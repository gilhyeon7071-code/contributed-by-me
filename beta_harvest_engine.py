# -*- coding: utf-8 -*-
"""Beta harvesting engine prototype.

Generates ETF rebalancing orders based on market regime and target allocation.
Designed to run either standalone or as part of the daily batch pipeline.

Configuration lives in paper_engine_config.json under the "beta_harvest" key.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

BASE_DIR = Path(os.environ.get("STOC_BASE_DIR") or str(Path(__file__).resolve().parent))
CONFIG_PATH = BASE_DIR / "paper" / "paper_engine_config.json"
CACHE_DIR = BASE_DIR / "_cache"


def _jload(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _jsave(path: Path, obj: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def load_config(cfg_path: Path | None = None) -> dict:
    return _jload(cfg_path or CONFIG_PATH)


def _load_etf_cache(ticker: str, start: str, end: str) -> pd.DataFrame | None:
    """Load ETF price cache. Accepts exact filename or any matching ticker cache."""
    exact = CACHE_DIR / f"etf_{ticker}_{start}_{end}.csv"
    if exact.exists():
        df = pd.read_csv(exact, parse_dates=["date"])
        df = df.rename(columns={"종가": "close"})
        return df
    # fallback: any cache for this ticker
    candidates = sorted(CACHE_DIR.glob(f"etf_{ticker}_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    for cand in candidates:
        df = pd.read_csv(cand, parse_dates=["date"])
        df = df.rename(columns={"종가": "close"})
        return df
    return None


def get_latest_etf_price(ticker: str, as_of: date) -> float:
    """Return the latest available close price up to as_of."""
    start = "2020-01-03"
    end = as_of.strftime("%Y-%m-%d")
    df = _load_etf_cache(ticker, start, end)
    if df is None or df.empty:
        raise FileNotFoundError(f"no ETF cache for {ticker}")
    df = df[df["date"] <= pd.Timestamp(as_of)]
    if df.empty:
        raise ValueError(f"no ETF price available for {ticker} up to {as_of}")
    return float(df.iloc[-1]["close"])


def _load_largecap_prices(as_of: date, base_dir: Path) -> Dict[str, float]:
    """Load latest close prices for large-cap stocks from paper ohlcv parquet."""
    path = base_dir / "paper" / "prices" / "ohlcv_paper.parquet"
    if not path.exists():
        return {}
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] <= pd.Timestamp(as_of)]
    if df.empty:
        return {}
    latest = df.groupby("code")["date"].max().reset_index()
    df = df.merge(latest, on=["code", "date"])
    df["code"] = df["code"].astype(str).str.zfill(6)
    return {
        row["code"]: float(row["close"])
        for _, row in df.iterrows()
        if pd.notna(row["close"])
    }


def _load_market_data_for_regime(base_dir: Path, as_of: date) -> pd.DataFrame:
    """Lightweight market data load + regime computation.

    Mirrors optimize_params_v41_1.compute_factors but only for regime.
    """
    import sys
    sys.path.insert(0, str(base_dir))
    import optimize_params_v41_1 as opt

    df = opt.load_data(base_dir)
    df = df[(df["date"] >= pd.Timestamp("2020-01-03")) & (df["date"] <= pd.Timestamp(as_of))].copy()
    df = opt.compute_factors(df)
    return df


def resolve_regime(df: pd.DataFrame, as_of: date, fallback: str = "NORMAL") -> str:
    """Get the market regime for the latest available date."""
    latest = df[df["date"] <= pd.Timestamp(as_of)]
    if latest.empty:
        return fallback
    regimes = latest.groupby("date")["market_regime"].first()
    return str(regimes.iloc[-1]).upper() if not regimes.empty else fallback


def get_regime_exposure(cfg: dict, regime: str) -> float:
    """Read gross exposure for a regime from config."""
    overrides = cfg.get("regime_overrides") or {}
    sub = overrides.get(regime) or {}
    cap = sub.get("capital_budget_policy") or {}
    v = cap.get("gross_exposure_pct")
    if v is None:
        v = sub.get("max_gross_exposure_pct")
    if v is None:
        defaults = {"RALLY": 0.60, "NORMAL": 0.55, "BEAR": 0.35, "CRASH": 0.10}
        v = defaults.get(regime, 0.55)
    return float(v)


def load_positions(path: Path) -> Dict[str, int]:
    """Load current ETF position ledger."""
    data = _jload(path)
    return {str(k): int(v) for k, v in data.get("positions", {}).items()}


def save_positions(path: Path, positions: Dict[str, int], as_of: date) -> None:
    obj = {
        "as_of": as_of.strftime("%Y-%m-%d"),
        "positions": {str(k): int(v) for k, v in positions.items()},
    }
    _jsave(path, obj)


def _normalize_codes(codes) -> List[str]:
    if isinstance(codes, str):
        return [str(codes).zfill(6)]
    return [str(c).zfill(6) for c in (codes or []) if c]


def generate_orders(
    cfg: dict,
    as_of: date,
    positions: Dict[str, int],
    prices: Dict[str, float],
    regime: str,
) -> List[Dict[str, Any]]:
    """Compute BUY/SELL orders to align positions with target allocation."""
    beta = cfg.get("beta_harvest") or {}
    if not beta.get("enabled"):
        return []

    capital_total = float(cfg.get("capital_total", 100_000_000))
    mode = str(beta.get("mode", "etf")).lower()
    if mode == "largecap":
        universe = beta.get("largecaps") or {}
    else:
        universe = beta.get("etfs") or {}

    allocation = beta.get("allocation") or {}
    min_amount = float(beta.get("min_order_amount_krw", 100_000))
    max_weight = float(beta.get("max_single_etf_weight", 0.6))
    use_scaling = bool(beta.get("use_regime_scaling", True))

    exposure = get_regime_exposure(cfg, regime) if use_scaling else 1.0
    target_total_notional = capital_total * exposure

    orders = []
    for market, weight in allocation.items():
        codes = _normalize_codes(universe.get(market))
        if not codes:
            continue

        available = {c: prices.get(c) for c in codes if prices.get(c)}
        available = {
            c: p for c, p in available.items()
            if p is not None and p > 0 and np.isfinite(p)
        }
        if not available:
            continue

        per_stock_weight = float(weight) / len(available)
        for code, price in available.items():
            target_notional = target_total_notional * per_stock_weight
            target_notional = min(target_notional, capital_total * max_weight)

            current_qty = int(positions.get(code, 0))
            current_notional = current_qty * price
            diff = target_notional - current_notional

            if abs(diff) < min_amount:
                continue

            side = "BUY" if diff > 0 else "SELL"
            qty = int(abs(diff) / price)
            if qty < 1:
                continue

            orders.append({
                "date": as_of.strftime("%Y%m%d"),
                "side": side,
                "code": code,
                "qty": qty,
                "price": round(price, 2),
                "market": market,
                "regime": regime,
                "target_exposure": round(exposure, 4),
                "target_notional": round(target_notional, 2),
                "current_notional": round(current_notional, 2),
            })

    return orders


def write_orders(orders: List[Dict[str, Any]], output_dir: Path, as_of: date, beta_cfg: dict) -> Path:
    """Write orders to CSV and dispatch-ready exec xlsx."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"orders_{as_of.strftime('%Y%m%d')}_beta_harvest.csv"
    xlsx_path = output_dir / f"orders_{as_of.strftime('%Y%m%d')}_beta_exec.xlsx"

    order_type = str(beta_cfg.get("order_type", "limit")).lower()

    if orders:
        df = pd.DataFrame(orders)
        df = df[["date", "side", "code", "qty", "price", "market", "regime", "target_exposure", "target_notional", "current_notional"]]
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

        # Dispatch-compatible exec xlsx
        exec_rows = []
        for _, r in df.iterrows():
            note = (
                f"beta_harvest;market={r['market']};regime={r['regime']};"
                f"target_exposure={r['target_exposure']};target_notional={r['target_notional']};"
                f"current_notional={r['current_notional']}"
            )
            exec_rows.append({
                "exec_date": str(r["date"]),
                "side": r["side"],
                "code": str(r["code"]).zfill(6),
                "fill_qty": int(r["qty"]),
                "fill_price": float(r["price"]),
                "order_type": order_type,
                "stop_level": None,
                "signal_date": str(r["date"]),
                "signal_ts": "",
                "is_stop": False,
                "note": note,
                "reason": "beta_harvest",
                "entry_blocked": False,
                "entry_block_reason": "",
                "posthoc_policy_violation": False,
                "posthoc_policy_reason": "",
                "execution_blocked": False,
                "execution_block_reason": "",
                "disclosure_policy_available": False,
                "intent_id": "",
                "order_id": "",
                "trace_id": "",
                "trader_instruction": "",
            })
        exec_df = pd.DataFrame(exec_rows)
        exec_df.to_excel(xlsx_path, index=False, engine="openpyxl")
    else:
        # write empty file with header for idempotency
        cols = ["date", "side", "code", "qty", "price", "market", "regime", "target_exposure", "target_notional", "current_notional"]
        pd.DataFrame(columns=cols).to_csv(csv_path, index=False, encoding="utf-8-sig")
        exec_cols = [
            "exec_date", "side", "code", "fill_qty", "fill_price", "order_type", "stop_level",
            "signal_date", "signal_ts", "is_stop", "note", "reason", "entry_blocked",
            "entry_block_reason", "posthoc_policy_violation", "posthoc_policy_reason",
            "execution_blocked", "execution_block_reason", "disclosure_policy_available",
            "intent_id", "order_id", "trace_id", "trader_instruction",
        ]
        pd.DataFrame(columns=exec_cols).to_excel(xlsx_path, index=False, engine="openpyxl")
    return csv_path


def record_paper_fills(orders: List[Dict[str, Any]], base_dir: Path, as_of: date) -> Path:
    """Append virtual fills to the beta_harvest ledger.

    2026-08-20: this used to append to ``paper/fills.csv`` -- the authoritative
    ledger that every strategy statistic reads.  beta_harvest is regime-based
    beta exposure management, not a v41.1 strategy signal, so its rows were
    mixing a non-strategy path into strategy evidence and every downstream
    breakdown had to reconstruct the distinction by string-matching ``note``.
    On 2026-07-28 this engine produced 12 real fills into that ledger while
    every config snapshot of the day recorded it as disabled, and no record
    explains how.  Writes now go to a dedicated ledger instead.

    The 12 rows already in ``paper/fills.csv`` are left untouched: they are
    real, fully closed trades.  This only changes where future rows land.
    Schema is identical, so the two can be concatenated deliberately.
    See .agent/PLANS.md 2026-08-20.
    """
    fills_path = base_dir / "paper" / "fills_beta_harvest.csv"

    # Fail-closed: never let this engine target the authoritative ledger, even
    # if a future edit or an env override points it back there.
    authority = (base_dir / "paper" / "fills.csv").resolve()
    if fills_path.resolve() == authority:
        raise RuntimeError(
            "beta_harvest must not write the authoritative ledger paper/fills.csv"
        )

    header = "datetime,code,side,qty,price,order_id,note\n"
    if not fills_path.exists():
        fills_path.write_text(header, encoding="utf-8-sig")

    rows = []
    for o in orders:
        dt = as_of.strftime("%Y%m%dT15:20:00")
        code = str(o["code"]).zfill(6)
        side = o["side"]
        qty = int(o["qty"])
        price = float(o["price"])
        order_id = f"PAPER_{side}_{code}_{o['date']}"
        note = (
            f"beta_harvest;market={o['market']};regime={o['regime']};"
            f"target_exposure={o['target_exposure']};target_notional={o['target_notional']};"
            f"current_notional={o['current_notional']}"
        )
        rows.append(f"{dt},{code},{side},{qty},{price},{order_id},{note}")

    if rows:
        with open(fills_path, "a", encoding="utf-8-sig") as f:
            f.write("\n".join(rows) + "\n")
    return fills_path


def update_positions(positions: Dict[str, int], orders: List[Dict[str, Any]]) -> Dict[str, int]:
    """Update position ledger after generating orders (assumes full fill)."""
    out = dict(positions)
    for o in orders:
        code = str(o["code"])
        qty = int(o["qty"])
        if o["side"] == "BUY":
            out[code] = out.get(code, 0) + qty
        elif o["side"] == "SELL":
            out[code] = max(0, out.get(code, 0) - qty)
    return out


def main(as_of: date | None = None, dry_run: bool = False) -> int:
    as_of = as_of or date.today()
    cfg = load_config()
    beta = cfg.get("beta_harvest") or {}

    if not beta.get("enabled"):
        print(f"[BETA_HARVEST] {as_of} disabled in config")
        return 0

    print(f"[BETA_HARVEST] {as_of} starting...")

    # Determine regime
    df = _load_market_data_for_regime(BASE_DIR, as_of)
    regime = resolve_regime(df, as_of, fallback=beta.get("regime_fallback", "NORMAL"))
    exposure = get_regime_exposure(cfg, regime)
    print(f"[BETA_HARVEST] regime={regime} target_exposure={exposure:.2%}")

    # Load target prices
    mode = str(beta.get("mode", "etf")).lower()
    prices = {}
    if mode == "largecap":
        prices = _load_largecap_prices(as_of, BASE_DIR)
        largecaps = beta.get("largecaps") or {}
        target_codes = set()
        for codes in largecaps.values():
            target_codes.update(_normalize_codes(codes))
        for code in sorted(target_codes):
            p = prices.get(code)
            if p:
                print(f"[BETA_HARVEST] largecap({code}) price={p:,.0f}")
            else:
                print(f"[BETA_HARVEST] WARN no price for largecap {code}")
    else:
        etfs = beta.get("etfs") or {}
        for market, ticker in etfs.items():
            try:
                prices[str(ticker).zfill(6)] = get_latest_etf_price(ticker, as_of)
                print(f"[BETA_HARVEST] {market}({ticker}) price={prices[str(ticker).zfill(6)]:,.0f}")
            except Exception as exc:
                print(f"[BETA_HARVEST] WARN cannot price {market}({ticker}): {exc}")

    if not prices:
        print("[BETA_HARVEST] ERROR no prices available")
        return 1

    # Current positions
    ledger_path = Path(beta.get("position_ledger_path") or str(BASE_DIR / "paper" / "beta_harvest_positions.json"))
    positions = load_positions(ledger_path)
    print(f"[BETA_HARVEST] current positions={positions}")

    # Generate orders
    orders = generate_orders(cfg, as_of, positions, prices, regime)
    print(f"[BETA_HARVEST] generated {len(orders)} orders")
    for o in orders:
        print(f"  {o['side']} {o['code']} qty={o['qty']} price={o['price']:.0f} target={o['target_notional']:,.0f}")

    # Persist orders
    paper_only = bool(beta.get("paper_only", False))
    if paper_only:
        if not dry_run:
            fills_path = record_paper_fills(orders, BASE_DIR, as_of)
            print(f"[BETA_HARVEST] paper-only: recorded {len(orders)} fills to {fills_path}")
            new_positions = update_positions(positions, orders)
            save_positions(ledger_path, new_positions, as_of)
            print(f"[BETA_HARVEST] positions updated: {new_positions}")
        else:
            print("[BETA_HARVEST] dry-run: fills not recorded, positions not updated")
    else:
        output_dir = Path(beta.get("order_output_dir") or str(BASE_DIR / "paper"))
        order_path = write_orders(orders, output_dir, as_of, beta)
        print(f"[BETA_HARVEST] orders written: {order_path}")

        if not dry_run:
            new_positions = update_positions(positions, orders)
            save_positions(ledger_path, new_positions, as_of)
            print(f"[BETA_HARVEST] positions updated: {new_positions}")
        else:
            print("[BETA_HARVEST] dry-run: positions not updated")

    return 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Beta harvest ETF order generator")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: today)")
    parser.add_argument("--dry-run", action="store_true", help="do not update position ledger")
    parser.add_argument("--config", type=str, default=None, help="path to paper_engine_config.json")
    args = parser.parse_args()
    if args.config:
        CONFIG_PATH = Path(args.config)
        print(f"[BETA_HARVEST] using config: {CONFIG_PATH}")
    as_of = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
    raise SystemExit(main(as_of=as_of, dry_run=args.dry_run))
