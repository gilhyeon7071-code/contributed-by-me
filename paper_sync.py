from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

# Ensure utils import works regardless of cwd
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils.pipeline_audit import log_pipeline_event, count_rows

BASE_DIR = Path(__file__).resolve().parent
PAPER_DIR = BASE_DIR / "paper"
LOG_DIR = BASE_DIR / "2_Logs"

FILLS_PATH = PAPER_DIR / "fills.csv"

# Generate normalized fills and calculated trades without touching paper/trades.csv.
FILLS_NORM_PATH = PAPER_DIR / "fills_norm.csv"
TRADES_CALC_PATH = PAPER_DIR / "trades_calc.csv"
LINEAGE_AUDIT_LATEST_PATH = LOG_DIR / "paper_sync_lineage_audit_latest.json"

CONFIG_PATH = PAPER_DIR / "paper_config.json"
ENGINE_CONFIG_PATH = PAPER_DIR / "paper_engine_config.json"


@dataclass
class Config:
    fee_rate: float = 0.0         # Base fee rate applied per side.
    slippage_rate: float = 0.0    # Base slippage rate applied per side.
    sell_tax_rate: float = 0.0
    matching: str = "FIFO"        # FIFO or LIFO.


def load_config() -> Config:
    cfg_path = CONFIG_PATH if CONFIG_PATH.exists() else ENGINE_CONFIG_PATH
    if not cfg_path.exists():
        return Config()
    try:
        j = json.loads(cfg_path.read_text(encoding="utf-8"))
        return Config(
            fee_rate=float(j.get("fee_rate", j.get("fee_pct", 0.0))),
            slippage_rate=float(j.get("slippage_rate", j.get("slippage_pct", 0.0))),
            sell_tax_rate=float(j.get("sell_tax_rate", j.get("sell_tax_pct", 0.0))),
            matching=str(j.get("matching", "FIFO")).upper(),
        )
    except Exception:
        # On config parse failure, continue with defaults for scheduled runs.
        return Config()


def _norm_cols(cols) -> List[str]:
    # Remove BOM and surrounding whitespace from column names.
    return [str(c).lstrip("\ufeff").strip() for c in cols]


def _parse_ts(series: pd.Series) -> pd.Series:
    # Accept multiple timestamp string formats.
    ts = pd.to_datetime(series, errors="coerce")
    return ts


def _is_surge_note(note: object) -> bool:
    text = str(note or "")
    if "surge_immediate=1" in text:
        return True
    m = re.search(r"(?:^|[;|])surge_type=([^;|]+)", text)
    return bool(m and str(m.group(1)).strip())


def _preserve_exit_ts_note(note: object) -> bool:
    text = str(note or "")
    if _is_surge_note(text):
        return True
    entry_timing = _note_value(text, "entry_timing").strip().lower()
    return entry_timing == "intraday_realtime"


def _note_value(note: object, key: str) -> str:
    m = re.search(rf"(?:^|[;|]){re.escape(key)}=([^;|]+)", str(note or ""))
    return str(m.group(1)).strip() if m else ""


def _date_only_ts_text(value: object) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]} 00:00:00"
    return str(value or "")


def _as_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)


def _as_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)


def _resolve_as_of_ymd(trades: pd.DataFrame) -> str:
    if trades is None or trades.empty:
        return ""
    date_col = "exit_ts" if "exit_ts" in trades.columns else ("entry_ts" if "entry_ts" in trades.columns else None)
    if date_col is None:
        return ""
    s = trades[date_col].astype(str).str.replace(r"\D", "", regex=True).str[:8]
    s = s[s.str.len() == 8]
    if s.empty:
        return ""
    return str(s.max())


def write_lineage_audit(fills: pd.DataFrame) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    out = LOG_DIR / f"paper_sync_lineage_audit_{stamp}.json"
    rows = []
    issues = []

    if fills is None or fills.empty:
        payload = {
            "generated_at": now.isoformat(timespec="seconds"),
            "status": "NO_FILLS",
            "rows": [],
            "issues": [],
        }
        text = json.dumps(payload, ensure_ascii=False, indent=2)
        out.write_text(text, encoding="utf-8")
        LINEAGE_AUDIT_LATEST_PATH.write_text(text, encoding="utf-8")
        return out

    work = fills.copy()
    work["code"] = work["code"].astype(str).str.strip().str.zfill(6)
    work["side"] = work["side"].astype(str).str.strip().str.upper()
    work["qty_n"] = pd.to_numeric(work["qty"], errors="coerce").fillna(0).astype(int)
    work["entry_order_id"] = ""
    buy_mask = work["side"].eq("BUY")
    sell_mask = work["side"].eq("SELL")
    work.loc[buy_mask, "entry_order_id"] = work.loc[buy_mask, "order_id"].astype(str).str.strip()
    work.loc[sell_mask, "entry_order_id"] = work.loc[sell_mask, "note"].map(
        lambda x: _note_value(x, "entry_order_id") or _note_value(x, "source_order_id")
    )
    work = work[work["entry_order_id"].astype(str).str.strip().ne("")]

    for (code, entry_order_id), g in work.groupby(["code", "entry_order_id"], sort=True):
        buy_qty = int(g.loc[g["side"].eq("BUY"), "qty_n"].sum())
        sell_qty = int(g.loc[g["side"].eq("SELL"), "qty_n"].sum())
        net_qty = int(buy_qty - sell_qty)
        issue = ""
        if buy_qty <= 0 and sell_qty > 0:
            issue = "SELL_WITHOUT_BUY_LINEAGE"
        elif sell_qty > buy_qty:
            issue = "LINEAGE_OVER_SELL"
        row = {
            "code": code,
            "entry_order_id": str(entry_order_id),
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "net_qty": net_qty,
            "issue": issue,
        }
        rows.append(row)
        if issue:
            issues.append(row)

    status = "PASS" if not issues else "FAIL"
    payload = {
        "generated_at": now.isoformat(timespec="seconds"),
        "status": status,
        "lineages": len(rows),
        "issue_count": len(issues),
        "issues": issues,
        "rows": rows,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    out.write_text(text, encoding="utf-8")
    LINEAGE_AUDIT_LATEST_PATH.write_text(text, encoding="utf-8")
    print(f"[PAPER_SYNC_LINEAGE] status={status} issue_count={len(issues)} -> {LINEAGE_AUDIT_LATEST_PATH}")
    return out


def load_fills_any_schema(path: Path) -> pd.DataFrame:
    """
    ?덉슜 ?ㅽ궎留?
    A) 理쒖냼: datetime,code,side,qty,price,(order_id),(note)
    B) ?뺤옣: ts,date,code,name,side,qty,price,fee,slippage,order_id,note
    """
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str)
    df.columns = _norm_cols(df.columns)

    cols = set(df.columns)

    # ?쒓컙 而щ읆 寃곗젙
    if "ts" in cols:
        ts_raw = df["ts"]
    elif "datetime" in cols:
        ts_raw = df["datetime"]
    else:
    # Derive date from ts when date is missing.
        if "date" in cols:
            ts_raw = df["date"]
        else:
            ts_raw = pd.Series([None] * len(df))

    ts = _parse_ts(ts_raw)
    note_raw = df["note"] if "note" in cols else pd.Series([""] * len(df), index=df.index)
    order_id_raw = df["order_id"] if "order_id" in cols else pd.Series([""] * len(df), index=df.index)
    is_surge_direct = note_raw.apply(_is_surge_note)
    surge_order_ids = set(order_id_raw[is_surge_direct].astype(str).str.strip())
    surge_order_ids.discard("")
    linked_entry_ids = note_raw.apply(lambda x: _note_value(x, "entry_order_id") or _note_value(x, "source_order_id"))
    is_surge = is_surge_direct | linked_entry_ids.astype(str).str.strip().isin(surge_order_ids)
    preserve_ts_direct = note_raw.apply(_preserve_exit_ts_note)
    preserve_ts_order_ids = set(order_id_raw[preserve_ts_direct].astype(str).str.strip())
    preserve_ts_order_ids.discard("")
    preserve_ts = preserve_ts_direct | linked_entry_ids.astype(str).str.strip().isin(preserve_ts_order_ids)
    ts_date_only = pd.to_datetime(ts.dt.strftime("%Y%m%d"), format="%Y%m%d", errors="coerce")
    effective_ts = ts.where(preserve_ts, ts_date_only)

    out = pd.DataFrame()
    out["ts"] = effective_ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    out["date"] = effective_ts.dt.strftime("%Y%m%d")

    # ?꾩닔 而щ읆
    out["code"] = df["code"] if "code" in cols else ""
    out["name"] = df["name"] if "name" in cols else ""
    out["side"] = (df["side"] if "side" in cols else "").astype(str).str.upper().str.strip()
    out["qty"] = _as_int(df["qty"] if "qty" in cols else pd.Series([0] * len(df)))
    out["price"] = _as_float(df["price"] if "price" in cols else pd.Series([0.0] * len(df)))
    # Compatibility aliases for downstream checks expecting explicit fill_* names
    out["fill_qty"] = out["qty"]
    out["fill_price"] = out["price"]

    # ?좏깮 而щ읆
    out["order_id"] = order_id_raw
    out["note"] = note_raw
    out["_is_surge_signal"] = is_surge.astype(bool)
    out["_preserve_exit_ts"] = preserve_ts.astype(bool)

    # ?뺣젹/?뺣━
    out = out.dropna(subset=["date"])  # ts ?뚯떛 ?ㅽ뙣(NaT) ?쒓굅
    out = out[out["code"].astype(str).str.len() > 0]
    out = out[out["side"].isin(["BUY", "SELL"])]
    out = out[out["qty"] > 0]
    out = out[out["price"] > 0]

    out["_sort_ts"] = effective_ts
    out["_side_order"] = out["side"].map({"BUY": 0, "SELL": 1}).fillna(9).astype(int)
    out = (
        out.sort_values(["_sort_ts", "code", "_side_order"], kind="mergesort")
        .drop(columns=["_sort_ts", "_side_order"])
        .reset_index(drop=True)
    )
    return out


@dataclass
class Lot:
    side: str              # LONG or SHORT
    qty: int
    entry_price: float
    entry_ts: str
    name: str
    note: str
    entry_order_id: str
    preserve_exit_ts: bool


def _pop_lot(lots: List[Lot], matching: str) -> Lot:
    if matching == "LIFO":
        return lots.pop(-1)
    return lots.pop(0)  # FIFO


def _prioritize_lots_by_entry_order(lots: List[Lot], target_order_id: str) -> None:
    target = str(target_order_id or "").strip()
    if not target:
        return
    matched = [lot for lot in lots if str(lot.entry_order_id or "").strip() == target]
    if not matched:
        return
    others = [lot for lot in lots if str(lot.entry_order_id or "").strip() != target]
    lots[:] = matched + others


def build_trades_from_fills(fills: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    Match fills with FIFO/LIFO lots.
    - BUY first opens LONG, then SELL closes it.
    - SELL first opens SHORT, then BUY closes it.
    """
    open_lots: Dict[str, List[Lot]] = {}
    trades_rows = []
    trade_seq = 1

    for _, r in fills.iterrows():
        code = str(r["code"]).strip()
        name = str(r.get("name", "")).strip()
        side = str(r["side"]).strip().upper()
        qty = int(r["qty"])
        price = float(r["price"])
        ts = str(r["ts"])
        note = str(r.get("note", "")).strip()
        oid = str(r.get("order_id", "")).strip()
        fill_note = f"order_id={oid}" if oid else ""
        if note:
            fill_note = (fill_note + " | " if fill_note else "") + note
        preserve_exit_ts = bool(r.get("_preserve_exit_ts", False))
        entry_order_id = _note_value(note, "entry_order_id") or _note_value(note, "source_order_id")

        lots = open_lots.setdefault(code, [])

        def close_against(existing_side: str, close_qty: int, exit_price: float, exit_ts: str) -> int:
            nonlocal trade_seq
            nonlocal trades_rows

            remaining = close_qty
            _prioritize_lots_by_entry_order(lots, entry_order_id)
            while remaining > 0 and lots:
                lot = _pop_lot(lots, cfg.matching)
                if lot.side != existing_side:
                    # ?ㅻⅨ 諛⑺뼢 lot?대㈃ ?ㅼ떆 ?ｊ퀬 醫낅즺(?뺣젹 ?쇱꽑 諛⑹?)
                    lots.insert(0, lot)
                    break

                m = min(remaining, lot.qty)
                remaining -= m

                entry_price = lot.entry_price
                exit_price2 = exit_price

                if existing_side == "LONG":
                    gross_ret = (exit_price2 / entry_price) - 1.0
                else:  # SHORT
                    gross_ret = (entry_price / exit_price2) - 1.0

                cost_rate = ((float(entry_price) + float(exit_price2)) / float(entry_price)) * (
                    float(cfg.fee_rate) + float(cfg.slippage_rate)
                )
                net_ret = gross_ret - cost_rate - float(cfg.sell_tax_rate)

                trades_rows.append(
                    {
                        "trade_id": str(trade_seq),
                        "entry_ts": lot.entry_ts,
                        "exit_ts": exit_ts if lot.preserve_exit_ts else _date_only_ts_text(exit_ts),
                        "code": code,
                        "name": lot.name or name,
                        "side": existing_side,
                        "qty": int(m),
                        "entry_price": float(entry_price),
                        "exit_price": float(exit_price2),
                        "gross_ret": float(gross_ret),
                        "net_ret": float(net_ret),
                        "fee_rate": float(cfg.fee_rate),
                        "slippage_rate": float(cfg.slippage_rate),
                        "sell_tax_rate": float(cfg.sell_tax_rate),
                        "note": lot.note,
                    }
                )
                trade_seq += 1

                # Update remaining lot quantity.
                if lot.qty > m:
                    lot.qty -= m
                    # Put partially closed lot back at the front for deterministic matching.
                    lots.insert(0, lot)

            return int(remaining)

        if side == "BUY":
            # SHORT 청산 우선, 남은 수량만 LONG 오픈
            if any(l.side == "SHORT" for l in lots):
                remaining_qty = close_against("SHORT", qty, price, ts)
                if remaining_qty > 0:
                    lots.append(Lot(side="LONG", qty=remaining_qty, entry_price=price, entry_ts=ts, name=name, note=fill_note, entry_order_id=oid, preserve_exit_ts=preserve_exit_ts))
            else:
                lots.append(Lot(side="LONG", qty=qty, entry_price=price, entry_ts=ts, name=name, note=fill_note, entry_order_id=oid, preserve_exit_ts=preserve_exit_ts))

        elif side == "SELL":
            # LONG 청산 우선, 남은 수량만 SHORT 오픈
            if any(l.side == "LONG" for l in lots):
                remaining_qty = close_against("LONG", qty, price, ts)
                if remaining_qty > 0:
                    lots.append(Lot(side="SHORT", qty=remaining_qty, entry_price=price, entry_ts=ts, name=name, note=fill_note, entry_order_id=oid, preserve_exit_ts=preserve_exit_ts))
            else:
                lots.append(Lot(side="SHORT", qty=qty, entry_price=price, entry_ts=ts, name=name, note=fill_note, entry_order_id=oid, preserve_exit_ts=preserve_exit_ts))

    trades = pd.DataFrame(trades_rows)
    if trades.empty:
        # ?ㅻ뜑留??좎?
        trades = pd.DataFrame(columns=[
            "trade_id","entry_ts","exit_ts","code","name","side","qty",
            "entry_price","exit_price","gross_ret","net_ret","fee_rate","slippage_rate","sell_tax_rate","note"
        ])
    return trades


def write_pnl_summary(trades: pd.DataFrame, source_path: str = "") -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    run_id = str(os.getenv("RUN_ID", "")).strip() or f"paper_sync_pnl_{stamp}"
    out = LOG_DIR / f"paper_sync_pnl_summary_{stamp}.json"
    as_of = _resolve_as_of_ymd(trades)
    rows_total = int(len(trades))
    rows_as_of = int(
        trades["exit_ts"].astype(str).str.replace(r"\D", "", regex=True).str[:8].eq(as_of).sum()
    ) if ("exit_ts" in trades.columns and as_of) else 0

    if trades.empty:
        payload = {
            "generated_at": now.isoformat(timespec="seconds"),
            "run_id": run_id,
            "as_of": as_of,
            "source": str(source_path or TRADES_CALC_PATH),
            "rows_total": rows_total,
            "rows_as_of": rows_as_of,
            "trades_used": 0,
            "status": "no_trades",
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[PAPER_PNL] no trades (rows=0)")
        print(f"[OUT] {out}")
        return out

    r = pd.to_numeric(trades["net_ret"], errors="coerce").dropna()
    n = int(len(r))
    wins = int((r > 0).sum())
    losses = int((r < 0).sum())
    win_rate = (wins / n) if n else 0.0

    payload = {
        "generated_at": now.isoformat(timespec="seconds"),
        "run_id": run_id,
        "as_of": as_of,
        "source": str(source_path or TRADES_CALC_PATH),
        "rows_total": rows_total,
        "rows_as_of": rows_as_of,
        "trades_used": n,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_ret": float(r.mean()) if n else 0.0,
        "sum_ret": float(r.sum()) if n else 0.0,
        "comp_ret": float((1.0 + r).prod() - 1.0) if n else 0.0,
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[PAPER_PNL] trades_used={n}")
    print(f"[OUT] {out}")
    return out


def main() -> int:
    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    _audit_ymd = datetime.now().strftime("%Y%m%d")
    log_pipeline_event(
        stage="paper_sync",
        batch_label="[13/14]",
        event="START",
        date=_audit_ymd,
        input_files={
            "fills": str(FILLS_PATH),
        },
    )

    cfg = load_config()

    fills = load_fills_any_schema(FILLS_PATH)
    if fills.empty:
        print(f"[PAPER_SYNC] no fills -> {FILLS_PATH}")
        # Exit quietly without writing summary files when there are no fills.
        return 0

    fills.drop(columns=[c for c in fills.columns if str(c).startswith("_")], errors="ignore").to_csv(
        FILLS_NORM_PATH,
        index=False,
        encoding="utf-8-sig",
    )
    print(f"[PAPER_SYNC] fills_norm rows={len(fills)} -> {FILLS_NORM_PATH}")

    trades = build_trades_from_fills(fills, cfg)
    trades.to_csv(TRADES_CALC_PATH, index=False, encoding="utf-8-sig")
    print(f"[PAPER_SYNC] trades_calc rows={len(trades)} -> {TRADES_CALC_PATH}")

    write_lineage_audit(fills)
    pnl_summary_path = write_pnl_summary(trades, source_path=str(TRADES_CALC_PATH))
    log_pipeline_event(
        stage="paper_sync",
        batch_label="[13/14]",
        event="END",
        date=_audit_ymd,
        output_files={
            "fills_norm": {"path": str(FILLS_NORM_PATH), "rows": int(len(fills))},
            "trades_calc": {"path": str(TRADES_CALC_PATH), "rows": int(len(trades))},
            "pnl_summary": {"path": str(pnl_summary_path)},
        },
        metrics={
            "fills_norm_rows": int(len(fills)),
            "trades_calc_rows": int(len(trades)),
        },
        status="PASS",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
