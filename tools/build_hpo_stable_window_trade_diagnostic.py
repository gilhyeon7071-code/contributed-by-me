# -*- coding: utf-8 -*-
"""Build read-only trade diagnostics for a stable HPO window.

This script replays the current optimize_params_v41_1.py entry/exit logic for
one stable_params_v41_1.json window and writes diagnostic-only CSV/JSON files.
It does not modify best/stable params, orders, fills, gates, or policy.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
STABLE_PATH = ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import optimize_params_v41_1 as opt  # noqa: E402


def _json_default(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if np.isfinite(out) else float(default)
    except Exception:
        return float(default)


def _profit_factor(values: pd.Series) -> float:
    x = pd.to_numeric(values, errors="coerce").dropna()
    if x.empty:
        return 0.0
    pos = float(x[x > 0].sum())
    neg = abs(float(x[x < 0].sum()))
    return float(pos / (neg + 1e-9))


def _loss_group(df: pd.DataFrame, by: str) -> list[dict[str, Any]]:
    if df.empty or by not in df.columns:
        return []
    out = (
        df[df["ret"] < 0]
        .groupby(by, dropna=False)["ret"]
        .agg(["count", "sum", "mean"])
        .sort_values("sum")
        .reset_index()
    )
    return out.to_dict(orient="records")


def _factor_bins(df: pd.DataFrame, col: str) -> list[dict[str, Any]]:
    if df.empty or col not in df.columns:
        return []
    vals = pd.to_numeric(df[col], errors="coerce")
    valid = df[vals.notna()].copy()
    if valid.empty:
        return []
    valid["_bin"] = pd.qcut(pd.to_numeric(valid[col], errors="coerce"), q=min(4, len(valid)), duplicates="drop")
    out = (
        valid.groupby("_bin", observed=False)
        .agg(rows=("ret", "size"), pf=("ret", _profit_factor), mean_ret=("ret", "mean"), loss_sum=("ret", lambda s: float(s[s < 0].sum())))
        .reset_index()
    )
    out["_bin"] = out["_bin"].astype(str)
    return out.to_dict(orient="records")


def _setup_bucket(row: pd.Series) -> str:
    flags: list[str] = []
    if _safe_float(row.get("rs")) >= 3.0:
        flags.append("high_rs")
    if _safe_float(row.get("v_accel")) >= 5.0:
        flags.append("high_v_accel")
    if _safe_float(row.get("atr14_pct")) <= 0.05:
        flags.append("low_atr")
    if _safe_float(row.get("high_52w_gap")) <= 0.05:
        flags.append("near_52w_high")
    if _safe_float(row.get("value")) >= 100_000_000_000.0:
        flags.append("large_value")
    return "|".join(flags) if flags else "optimizer_unclassified"


def _replay_window_trades(df: pd.DataFrame, params: dict[str, Any], start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    wdf = df[(df["date"] >= start) & (df["date"] <= end)].copy()
    if wdf.empty:
        return pd.DataFrame()

    opt._PRICE_CACHE = None
    pc = opt._build_price_cache(wdf)
    slices = pc["slices"]
    date_s = pc["date_s"]
    o_s = pc["open_s"]
    h_s = pc["high_s"]
    l_s = pc["low_s"]
    c_s = pc["close_s"]

    p0 = {
        "rs_lim": _safe_float(params.get("rs_lim"), 1.0),
        "v_accel_lim": _safe_float(params.get("v_accel_lim"), 1.0),
        "stretch_max": _safe_float(params.get("stretch_max"), 9.9),
        "value_min": _safe_float(params.get("value_min"), 0.0),
        "atr_max": _safe_float(params.get("atr_max"), 9.9),
        "rsi_max": _safe_float(params.get("rsi_max"), 70.0),
        "require_macd_golden": 1.0 if _safe_float(params.get("require_macd_golden"), 0.0) >= 0.5 else 0.0,
        "vol_close_corr_min": _safe_float(params.get("vol_close_corr_min"), 0.0),
        "near_52w_high_gap_max": _safe_float(params.get("near_52w_high_gap_max"), 0.05),
        "min_listing_days": _safe_float(params.get("min_listing_days"), 126.0),
    }
    ladder = opt._relax_ladder_operational(p0) if _safe_float(params.get("use_relax_ladder"), 1.0) >= 0.5 else [("L0", p0)]

    selected_daily: list[pd.DataFrame] = []
    for _, gday in wdf.groupby("date", sort=True):
        chosen = pd.DataFrame()
        chosen_level = ""
        for level, p_try in ladder:
            cand = opt._select_day_candidates_operational(gday, p_try)
            if not cand.empty:
                chosen = cand.copy()
                chosen_level = level
                break
        if not chosen.empty:
            chosen["relax_level"] = chosen_level
            selected_daily.append(chosen)
    if not selected_daily:
        return pd.DataFrame()

    sig = pd.concat(selected_daily, ignore_index=True)
    sig["code"] = sig["code"].astype(str)
    sig["date"] = pd.to_datetime(sig["date"], errors="coerce")
    sig = sig.dropna(subset=["date", "code"])
    g = sig.groupby("date", sort=False)
    sig["score"] = (
        g["rs"].rank(pct=True) * _safe_float(params.get("w_rs"), 0.2)
        + g["rs_slope"].rank(pct=True) * _safe_float(params.get("w_rs_slope"), 0.55)
        + g["v_accel"].rank(pct=True) * _safe_float(params.get("w_v_accel"), 0.25)
    )
    sig = sig.sort_values(["date", "score"], ascending=[True, False], kind="mergesort")

    hold_days = int(params.get("hold", params.get("hold_days", opt.DEFAULT_FROZEN.get("hold", 10))))
    max_pos = int(params.get("max_pos", params.get("max_positions", opt.DEFAULT_FROZEN.get("max_pos", 20))))
    fee = _safe_float(params.get("fee"), opt.DEFAULT_FEE)
    stop_loss_raw = params.get("stop_loss", opt.DEFAULT_FROZEN.get("stop_loss", -0.05))
    stop_loss = float(stop_loss_raw) if stop_loss_raw is not None else None
    take_profit_raw = params.get("take_profit", opt.DEFAULT_FROZEN.get("take_profit", None))
    take_profit = float(take_profit_raw) if take_profit_raw is not None else None
    trail_raw = params.get("trail_pct", opt.DEFAULT_FROZEN.get("trail_pct", None))
    trail_pct = float(trail_raw) if trail_raw is not None else None
    gap_up_max_pct = _safe_float(params.get("gap_up_max_pct"), 0.0)
    entry_gap_down_stop_pct = _safe_float(params.get("entry_gap_down_stop_pct"), 0.0)
    gap_limit = _safe_float(params.get("gap_limit"), 0.0)

    sig_dates = sig["date"].to_numpy(dtype="datetime64[ns]")
    sig_codes = sig["code"].to_numpy(dtype=object)
    active: list[tuple[np.datetime64, str]] = []
    rows: list[dict[str, Any]] = []
    i = 0
    n = len(sig)
    w_start64 = np.datetime64(start.to_datetime64())
    w_end64 = np.datetime64(end.to_datetime64())

    while i < n:
        d = sig_dates[i]
        if active:
            active = [t for t in active if t[0] > d]
        if len(active) >= max_pos:
            j = i + 1
            while j < n and sig_dates[j] == d:
                j += 1
            i = j
            continue

        active_codes = {t[1] for t in active}
        j = i
        while j < n and sig_dates[j] == d:
            if len(active) >= max_pos:
                break
            sig_row = sig.iloc[j]
            code = str(sig_codes[j])
            j += 1
            if code in active_codes:
                continue
            sl = slices.get(code)
            if not sl:
                continue
            a, b = sl
            dates_code = date_s[a:b]
            if len(dates_code) < 2:
                continue
            lo = int(a + np.searchsorted(dates_code, w_start64, side="left"))
            hi_excl = int(a + np.searchsorted(dates_code, w_end64, side="right"))
            if hi_excl - lo < 2:
                continue
            dates_w = date_s[lo:hi_excl]
            idx = int(np.searchsorted(dates_w, d))
            if idx >= len(dates_w) or dates_w[idx] != d:
                continue
            entry_i = lo + idx + 1
            if entry_i >= hi_excl:
                continue
            entry_p = float(o_s[entry_i])
            if not np.isfinite(entry_p) or entry_p <= 0:
                continue

            sig_close = float(c_s[lo + idx])
            gap = (entry_p - sig_close) / sig_close if np.isfinite(sig_close) and sig_close > 0 else np.nan
            if np.isfinite(gap):
                if gap_up_max_pct > 0 and gap > gap_up_max_pct:
                    continue
                if entry_gap_down_stop_pct > 0 and gap <= -abs(entry_gap_down_stop_pct):
                    continue
                if gap_up_max_pct <= 0 and entry_gap_down_stop_pct <= 0 and gap_limit > 0 and abs(gap) > gap_limit:
                    continue

            end_i = min(entry_i + hold_days - 1, hi_excl - 1)
            exit_p = float(c_s[end_i])
            exit_d = date_s[end_i]
            exit_reason = "TIME"
            stop_p = entry_p * (1.0 + stop_loss) if stop_loss is not None else None
            tp_p = entry_p * (1.0 + take_profit) if take_profit is not None else None
            max_high = entry_p
            trail_p = entry_p * (1.0 - trail_pct) if trail_pct is not None else None

            for k in range(entry_i, end_i + 1):
                hi = float(h_s[k])
                lo_p = float(l_s[k])
                if np.isfinite(hi) and hi > max_high:
                    max_high = hi
                    if trail_p is not None:
                        trail_p = max_high * (1.0 - trail_pct)
                if stop_p is not None and np.isfinite(lo_p) and lo_p <= stop_p:
                    exit_p = float(stop_p)
                    exit_d = date_s[k]
                    exit_reason = "STOP"
                    break
                if tp_p is not None and np.isfinite(hi) and hi >= tp_p:
                    exit_p = float(tp_p)
                    exit_d = date_s[k]
                    exit_reason = "TAKE_PROFIT"
                    break
                if trail_p is not None and np.isfinite(lo_p) and lo_p <= trail_p:
                    exit_p = float(trail_p)
                    exit_d = date_s[k]
                    exit_reason = "TRAIL"
                    break

            ret = (exit_p / entry_p) - 1.0 - fee
            if np.isfinite(ret):
                rec = {
                    "signal_date": pd.Timestamp(d).strftime("%Y-%m-%d"),
                    "entry_date": pd.Timestamp(date_s[entry_i]).strftime("%Y-%m-%d"),
                    "exit_date": pd.Timestamp(exit_d).strftime("%Y-%m-%d"),
                    "code": code,
                    "entry_px": entry_p,
                    "exit_px": exit_p,
                    "ret": float(ret),
                    "exit_reason": exit_reason,
                    "score": float(sig_row.get("score", np.nan)),
                    "relax_level": str(sig_row.get("relax_level", "")),
                    "entry_gap_pct": float(gap) if np.isfinite(gap) else None,
                    "rs": _safe_float(sig_row.get("rs"), np.nan),
                    "rs_slope": _safe_float(sig_row.get("rs_slope"), np.nan),
                    "v_accel": _safe_float(sig_row.get("v_accel"), np.nan),
                    "stretch": _safe_float(sig_row.get("stretch"), np.nan),
                    "atr14_pct": _safe_float(sig_row.get("atr14_pct"), np.nan),
                    "rsi14": _safe_float(sig_row.get("rsi14"), np.nan),
                    "vol_close_corr20": _safe_float(sig_row.get("vol_close_corr20"), np.nan),
                    "high_52w_gap": _safe_float(sig_row.get("high_52w_gap"), np.nan),
                    "value": _safe_float(sig_row.get("value"), np.nan),
                    "listing_days": _safe_float(sig_row.get("listing_days"), np.nan),
                    "macd_golden": bool(sig_row.get("macd_golden", False)),
                }
                rec["setup_bucket"] = _setup_bucket(pd.Series(rec))
                rows.append(rec)
            active.append((exit_d, code))
            active_codes.add(code)

        while j < n and sig_dates[j] == d:
            j += 1
        i = j

    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-start", default="2023-04-29")
    ap.add_argument("--window-end", default="2024-04-28")
    ap.add_argument("--stable-path", default=str(STABLE_PATH))
    ap.add_argument("--base-dir", default=str(ROOT))
    ap.add_argument("--out-prefix", default="hpo_stable_val_2024_trade_diagnostic")
    args = ap.parse_args()

    stable_path = Path(args.stable_path)
    base_dir = Path(args.base_dir)
    out_prefix = str(args.out_prefix or "hpo_stable_val_2024_trade_diagnostic").strip()
    stable = json.loads(stable_path.read_text(encoding="utf-8"))
    start = pd.Timestamp(args.window_start)
    end = pd.Timestamp(args.window_end)
    stable_window = next(
        (
            row for row in stable.get("windows", [])
            if str(row.get("start")) == args.window_start and str(row.get("end")) == args.window_end
        ),
        {},
    )

    df = opt.load_data(base_dir)
    df = df[df["date"] >= pd.Timestamp("2020-01-01")].copy()
    df = opt.compute_factors(df)
    df = df.dropna(
        subset=[
            "rs",
            "rs_slope",
            "v_accel",
            "stretch",
            "atr14_pct",
            "gap_next",
            "rsi14",
            "vol_close_corr20",
            "high_52w_gap",
            "listing_days",
        ]
    ).copy()

    trades = _replay_window_trades(df, stable, start, end)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = LOG_DIR / f"{out_prefix}_latest.csv"
    json_path = LOG_DIR / f"{out_prefix}_latest.json"
    trades.to_csv(csv_path, index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "hpo_stable_val_2024_trade_diagnostic",
        "policy_change": False,
        "trading_effect": False,
        "stable_path": str(stable_path),
        "base_dir": str(base_dir),
        "window": {"start": args.window_start, "end": args.window_end},
        "stable_window_reference": stable_window,
        "replayed": {
            "rows": int(len(trades)),
            "pf": _profit_factor(trades["ret"]) if "ret" in trades else 0.0,
            "mean_ret": float(pd.to_numeric(trades.get("ret", pd.Series(dtype=float)), errors="coerce").mean()) if len(trades) else 0.0,
            "win_rate": float((pd.to_numeric(trades.get("ret", pd.Series(dtype=float)), errors="coerce") > 0).mean()) if len(trades) else 0.0,
            "exit_reason_counts": dict(Counter(trades.get("exit_reason", pd.Series(dtype=str)).fillna(""))),
            "fundamental_critical_applicable": False,
            "fundamental_critical_note": "optimize_params_v41_1.py stable HPO replay has no fundamental exit layer; FUNDAMENTAL_CRITICAL is available in report_backtest_v41_1.py outputs only.",
        },
        "reference_match": {
            "rows_match": int(len(trades)) == int(stable_window.get("n_trades", -1) or -1),
            "pf_match_rounded_6": round(_profit_factor(trades["ret"]) if "ret" in trades else 0.0, 6)
            == round(_safe_float(stable_window.get("pf"), -1.0), 6),
            "note": "If false, current code/data replay does not reproduce the historical stable_params window aggregate; use replay decomposition as current reproducible diagnostic, not exact historical trade list.",
        },
        "loss_by_exit_reason": _loss_group(trades, "exit_reason"),
        "loss_by_setup_bucket": _loss_group(trades, "setup_bucket"),
        "factor_bins": {
            col: _factor_bins(trades, col)
            for col in ["rs", "rs_slope", "v_accel", "stretch", "atr14_pct", "rsi14", "high_52w_gap", "value", "entry_gap_pct"]
        },
        "top_losses": (
            trades.sort_values("ret")
            .head(20)
            .to_dict(orient="records")
            if not trades.empty
            else []
        ),
        "outputs": {"csv": str(csv_path), "json": str(json_path)},
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2, default=_json_default), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "csv": str(csv_path), "rows": len(trades), "pf": summary["replayed"]["pf"]}, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
