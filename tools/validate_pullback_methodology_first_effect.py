from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_pullback_methodology_first_screener import _load_daily_history


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

OUT_JSON = LOG_DIR / "pullback_methodology_first_effect_validation_latest.json"
OUT_CSV = LOG_DIR / "pullback_methodology_first_effect_validation_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _pct(num: float, den: float) -> float:
    return num / den if den > 0 else 0.0


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _rolling_signals_for_code(group: pd.DataFrame) -> list[dict[str, Any]]:
    group = group.sort_values("date").reset_index(drop=True).copy()
    if len(group) < 66:
        return []

    close_s = group["close"].astype(float)
    high_s = group["high"].astype(float)
    low_s = group["low"].astype(float)
    volume_s = group["volume"].fillna(0).astype(float)
    value_s = group["value"].fillna(0).astype(float)

    group["sma20"] = close_s.rolling(20).mean()
    group["sma50"] = close_s.rolling(50).mean()
    group["sma20_prev"] = close_s.shift(5).rolling(20).mean()
    group["high20"] = high_s.rolling(20).max()
    group["low60"] = low_s.rolling(60).min()
    group["avg_value20"] = value_s.rolling(20).mean()
    group["avg_volume5"] = volume_s.rolling(5).mean()
    group["prior_rally_volume"] = volume_s.shift(5).rolling(10).mean()
    group["prev_close"] = close_s.shift(1)
    group["prev_high"] = high_s.shift(1)
    rolling10_high = high_s.rolling(10).max()
    dip_flag = ((rolling10_high - close_s) / rolling10_high >= 0.03).astype(float)
    group["pullback_dips_proxy"] = dip_flag.rolling(20).sum()
    prev_close_shift = close_s.shift(1)
    tr = pd.concat(
        [
            high_s - low_s,
            (high_s - prev_close_shift).abs(),
            (low_s - prev_close_shift).abs(),
        ],
        axis=1,
    ).max(axis=1)
    group["atr14"] = tr.rolling(14).mean()
    group["low5"] = low_s.rolling(5).min()
    group["close_1d"] = close_s.shift(-1)
    group["close_3d"] = close_s.shift(-3)
    group["close_5d"] = close_s.shift(-5)
    group["max_high_5d"] = high_s.shift(-1).rolling(5).max().shift(-4)
    group["min_low_5d"] = low_s.shift(-1).rolling(5).min().shift(-4)

    high20_idx: list[int | None] = []
    for idx in range(len(group)):
        if idx < 19:
            high20_idx.append(None)
            continue
        window = high_s.iloc[idx - 19 : idx + 1]
        high20_idx.append(int(window.idxmax()))
    group["days_from_high20"] = [idx - hidx if hidx is not None else None for idx, hidx in enumerate(high20_idx)]

    out: list[dict[str, Any]] = []
    for idx, row in group.iloc[60:-5].iterrows():
        close = float(row["close"])
        high = float(row["high"])
        low = float(row["low"])
        high20 = float(row["high20"])
        low60 = float(row["low60"])
        avg_value20 = float(row["avg_value20"])
        avg_volume5 = float(row["avg_volume5"])
        prior_rally_volume = float(row["prior_rally_volume"])
        prev_close = float(row["prev_close"])
        prev_high = float(row["prev_high"])
        sma20 = float(row["sma20"])
        sma50 = float(row["sma50"])
        sma20_prev = float(row["sma20_prev"])
        days_from_high = int(row["days_from_high20"])
        pullback_dips = int(row["pullback_dips_proxy"])
        if close <= 0 or pd.isna(row["close_5d"]):
            continue

        pullback_from_high = _pct(close - high20, high20)
        rally_from_low60 = _pct(high20 - low60, low60)
        day_return = _pct(close - prev_close, prev_close)
        range_position = _pct(close - low, high - low) if high > low else 0.0
        volume_contraction_ratio = _pct(avg_volume5, prior_rally_volume)
        value_liquidity_ok = avg_value20 >= 500_000_000
        atr14 = float(row["atr14"])
        low5 = float(row["low5"])
        structure_stop = min(low5, close - atr14) if atr14 > 0 else low5
        risk = max(close - structure_stop, 0.0)
        reward = max(high20 - close, 0.0)
        rr = _pct(reward, risk)
        risk_pct = _pct(risk, close)

        trend_axis = close >= sma50 and sma20 >= sma50 and sma20 >= sma20_prev and rally_from_low60 >= 0.12
        pullback_axis = -0.14 <= pullback_from_high <= -0.03 and close >= sma20 * 0.96
        volume_axis = 0.75 <= volume_contraction_ratio <= 1.8 and row["volume"] > 0 and value_liquidity_ok
        reaccel_axis = day_return > 0 and range_position >= 0.55 and (row["volume"] >= avg_volume5 or close >= prev_high)
        first_pullback_axis = 1 <= days_from_high <= 10 and pullback_dips <= 8
        rr_axis = rr >= 1.3 and risk > 0
        loss_risk_axis = 0 < risk_pct <= 0.06
        axis_score = sum([trend_axis, pullback_axis, volume_axis, reaccel_axis, first_pullback_axis, rr_axis])
        hard_risk = bool(row.get("tradability_blocked", False)) or not value_liquidity_ok or close < 1000
        if hard_risk or not (axis_score >= 4 and trend_axis and pullback_axis):
            continue

        max_high_5d = float(row["max_high_5d"])
        min_low_5d = float(row["min_low_5d"])
        out.append(
            {
                "asof": str(row["date"]),
                "code": str(row["code"]).zfill(6),
                "market": "" if pd.isna(row.get("market", "")) else str(row.get("market", "")),
                "methodology_status": "PULLBACK_FIRST_WATCH",
                "axis_score": axis_score,
                "trend_axis": "PASS" if trend_axis else "FAIL",
                "pullback_axis": "PASS" if pullback_axis else "FAIL",
                "volume_axis": "PASS" if volume_axis else "FAIL",
                "reaccel_axis": "PASS" if reaccel_axis else "FAIL",
                "first_pullback_axis": "PASS" if first_pullback_axis else "FAIL",
                "rr_axis": "PASS" if rr_axis else "FAIL",
                "loss_risk_axis": "PASS" if loss_risk_axis else "FAIL",
                "close": round(close, 4),
                "pullback_from_high_pct": round(pullback_from_high, 6),
                "volume_contraction_ratio": round(volume_contraction_ratio, 6),
                "rr_to_high20": round(rr, 6),
                "risk_pct": round(risk_pct, 6),
                "day_return_pct": round(day_return, 6),
                "range_position": round(range_position, 6),
                "days_from_high20": days_from_high,
                "pullback_dips_proxy": pullback_dips,
                "next_1d_return": round(_pct(float(row["close_1d"]) - close, close), 6),
                "next_3d_return": round(_pct(float(row["close_3d"]) - close, close), 6),
                "next_5d_return": round(_pct(float(row["close_5d"]) - close, close), 6),
                "max_up_5d": round(_pct(max_high_5d - close, close), 6),
                "max_down_5d": round(_pct(min_low_5d - close, close), 6),
                "followthrough_5d": max_high_5d > close * 1.03,
                "stop_risk_5d": min_low_5d < close * 0.95,
                "connection_level": "SIGNAL_QUALITY_ONLY",
                "trading_connection": False,
                "signal_connection": False,
                "execution_connection": False,
                "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
            }
        )
    return out


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "sample_count": 0,
            "avg_1d": 0.0,
            "avg_3d": 0.0,
            "avg_5d": 0.0,
            "median_5d": 0.0,
            "win_rate_5d": 0.0,
            "followthrough_rate_5d": 0.0,
            "stop_risk_rate_5d": 0.0,
            "avg_max_down_5d": 0.0,
        }
    returns_5d = [float(row["next_5d_return"]) for row in rows]
    return {
        "sample_count": len(rows),
        "avg_1d": round(_avg([float(row["next_1d_return"]) for row in rows]), 6),
        "avg_3d": round(_avg([float(row["next_3d_return"]) for row in rows]), 6),
        "avg_5d": round(_avg(returns_5d), 6),
        "median_5d": round(_median(returns_5d), 6),
        "win_rate_5d": round(_avg([1.0 if float(row["next_5d_return"]) > 0 else 0.0 for row in rows]), 6),
        "followthrough_rate_5d": round(_avg([1.0 if row["followthrough_5d"] else 0.0 for row in rows]), 6),
        "stop_risk_rate_5d": round(_avg([1.0 if row["stop_risk_5d"] else 0.0 for row in rows]), 6),
        "avg_max_down_5d": round(_avg([float(row["max_down_5d"]) for row in rows]), 6),
    }


def build() -> dict[str, Any]:
    daily = _load_daily_history()
    signal_rows: list[dict[str, Any]] = []
    for _, group in daily.groupby("code", sort=False):
        signal_rows.extend(_rolling_signals_for_code(group))
    dates = sorted(daily["date"].unique())
    tested_dates = sorted(set(row["asof"] for row in signal_rows))

    fields = [
        "asof",
        "code",
        "market",
        "methodology_status",
        "axis_score",
        "trend_axis",
        "pullback_axis",
        "volume_axis",
        "reaccel_axis",
        "first_pullback_axis",
        "rr_axis",
        "loss_risk_axis",
        "close",
        "pullback_from_high_pct",
        "volume_contraction_ratio",
        "rr_to_high20",
        "risk_pct",
        "day_return_pct",
        "range_position",
        "days_from_high20",
        "pullback_dips_proxy",
        "next_1d_return",
        "next_3d_return",
        "next_5d_return",
        "max_up_5d",
        "max_down_5d",
        "followthrough_5d",
        "stop_risk_5d",
        "connection_level",
        "trading_connection",
        "signal_connection",
        "execution_connection",
        "connection_note",
    ]
    _write_csv(OUT_CSV, signal_rows, fields)

    both_quality = [row for row in signal_rows if int(row["axis_score"]) >= 5]
    watch_quality = [row for row in signal_rows if int(row["axis_score"]) == 4]
    first_pullback_required = [row for row in signal_rows if row["first_pullback_axis"] == "PASS"]
    first_pullback_required_rr = [
        row for row in first_pullback_required if row["rr_axis"] == "PASS"
    ]
    first_pullback_required_reaccel = [
        row for row in first_pullback_required if row["reaccel_axis"] == "PASS"
    ]
    scenario_rows = {
        "first_pullback_rr": first_pullback_required_rr,
        "first_pullback_rr_volume_v2": [
            row for row in first_pullback_required_rr if row["volume_axis"] == "PASS"
        ],
        "first_pullback_rr_loss_risk": [
            row for row in first_pullback_required_rr if row["loss_risk_axis"] == "PASS"
        ],
        "first_pullback_rr_volume_v2_loss_risk": [
            row
            for row in first_pullback_required_rr
            if row["volume_axis"] == "PASS" and row["loss_risk_axis"] == "PASS"
        ],
        "first_pullback_rr_risk_pct_lte_05": [
            row for row in first_pullback_required_rr if float(row["risk_pct"]) <= 0.05
        ],
        "first_pullback_rr_risk_pct_lte_08": [
            row for row in first_pullback_required_rr if float(row["risk_pct"]) <= 0.08
        ],
        "first_pullback_rr_day_nonnegative": [
            row for row in first_pullback_required_rr if float(row["day_return_pct"]) >= 0
        ],
        "first_pullback_rr_day_nonnegative_volume_mid": [
            row
            for row in first_pullback_required_rr
            if float(row["day_return_pct"]) >= 0 and 0.75 <= float(row["volume_contraction_ratio"]) <= 1.8
        ],
        "first_pullback_rr_range_upper_half": [
            row for row in first_pullback_required_rr if float(row["range_position"]) >= 0.5
        ],
        "first_pullback_rr_clean_pullback": [
            row
            for row in first_pullback_required_rr
            if int(row["pullback_dips_proxy"]) <= 5 and int(row["days_from_high20"]) <= 7
        ],
    }
    summary = {
        "generated_at": _now_ts(),
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "connection_level": "SIGNAL_QUALITY_ONLY",
        "trading_connection": False,
        "signal_connection": False,
        "execution_connection": False,
        "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        "source": str(ROOT / "krx_daily_archive"),
        "date_range_loaded": [str(dates[0]) if dates else "", str(dates[-1]) if dates else ""],
        "signal_date_range_tested": [str(tested_dates[0]) if tested_dates else "", str(tested_dates[-1]) if tested_dates else ""],
        "unique_codes_loaded": int(daily["code"].nunique()) if not daily.empty else 0,
        "signal_count": len(signal_rows),
        "signal_date_count": len(set(row["asof"] for row in signal_rows)),
        "axis_score_counts": dict(Counter(str(row["axis_score"]) for row in signal_rows)),
        "all_watch_summary": _summarize(signal_rows),
        "axis_score_5_plus_summary": _summarize(both_quality),
        "axis_score_4_summary": _summarize(watch_quality),
        "first_pullback_required_summary": _summarize(first_pullback_required),
        "first_pullback_required_rr_summary": _summarize(first_pullback_required_rr),
        "first_pullback_required_reaccel_summary": _summarize(first_pullback_required_reaccel),
        "refinement_scenario_summaries": {
            name: _summarize(rows) for name, rows in scenario_rows.items()
        },
        "top_positive_5d": sorted(signal_rows, key=lambda r: float(r["next_5d_return"]), reverse=True)[:10],
        "top_negative_5d": sorted(signal_rows, key=lambda r: float(r["next_5d_return"]))[:10],
    }
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def main() -> int:
    summary = build()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
