from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
ARCHIVE_DIR = ROOT / "krx_daily_archive"

CURRENT_POOL_FILES = [
    LOG_DIR / "market_rising_latest.csv",
    LOG_DIR / "surge_sanity_labeled_latest.csv",
    LOG_DIR / "candidates_latest_data.with_final_score.csv",
    LOG_DIR / "news_candidates_latest.csv",
    LOG_DIR / "surge_event_money_pullback_screener_latest.csv",
]

OUT_JSON = LOG_DIR / "pullback_methodology_first_screener_latest.json"
OUT_CSV = LOG_DIR / "pullback_methodology_first_screener_latest.csv"
OUT_COMPARE_CSV = LOG_DIR / "pullback_methodology_first_vs_current_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _ratio(num: float, den: float) -> float:
    return num / den if den > 0 else 0.0


def _archive_start(path: Path) -> str:
    return path.name[10:18]


def _latest_daily_files() -> list[Path]:
    files = sorted(ARCHIVE_DIR.glob("krx_daily_????????_????????_clean.parquet"))
    return [path for path in files if _archive_start(path) >= "20251201"]


def _load_daily_history() -> pd.DataFrame:
    files = _latest_daily_files()
    if not files:
        return pd.DataFrame()
    frames = []
    for path in files:
        frame = pd.read_parquet(path)
        if not frame.empty:
            frames.append(frame.dropna(axis=1, how="all"))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["date"] = df["date"].astype(str)
    for col in ("open", "high", "low", "close", "volume", "value"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["code", "date", "open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["code", "date"], keep="last")
    df = df.sort_values(["code", "date"])
    return df


def _current_pool() -> tuple[set[str], dict[str, str]]:
    codes: set[str] = set()
    sources: dict[str, list[str]] = {}
    for path in CURRENT_POOL_FILES:
        for row in _read_csv(path):
            code = _code(row.get("code"))
            if not code:
                continue
            codes.add(code)
            sources.setdefault(code, []).append(path.name)
    return codes, {code: "|".join(sorted(set(srcs))) for code, srcs in sources.items()}


def _axis_label(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _safe_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def _score_code(
    code: str,
    rows: pd.DataFrame,
    latest_asof: str,
    current_codes: set[str],
    current_sources: dict[str, str],
) -> dict[str, Any] | None:
    rows = rows.sort_values("date").tail(90).reset_index(drop=True)
    if len(rows) < 45:
        return None

    latest = rows.iloc[-1]
    if str(latest["date"]) != latest_asof:
        return None

    close = _safe_float(latest["close"])
    high = _safe_float(latest["high"])
    low = _safe_float(latest["low"])
    volume = _safe_float(latest["volume"])
    value = _safe_float(latest["value"])
    if close <= 0:
        return None

    close_s = rows["close"].astype(float)
    high_s = rows["high"].astype(float)
    low_s = rows["low"].astype(float)
    volume_s = rows["volume"].fillna(0).astype(float)
    value_s = rows["value"].fillna(0).astype(float)

    sma20 = _safe_float(close_s.tail(20).mean())
    sma50 = _safe_float(close_s.tail(50).mean())
    sma20_prev = _safe_float(close_s.iloc[-25:-5].mean()) if len(close_s) >= 50 else 0.0
    high20 = _safe_float(high_s.tail(20).max())
    low60 = _safe_float(low_s.tail(60).min())
    avg_value20 = _safe_float(value_s.tail(20).mean())
    avg_volume20 = _safe_float(volume_s.tail(20).mean())
    avg_volume5 = _safe_float(volume_s.tail(5).mean())
    prior_rally_volume = _safe_float(volume_s.iloc[-15:-5].mean()) if len(volume_s) >= 20 else 0.0
    prev_close = _safe_float(close_s.iloc[-2]) if len(close_s) >= 2 else close
    prev_high = _safe_float(high_s.iloc[-2]) if len(high_s) >= 2 else high

    high_index = int(high_s.tail(20).idxmax())
    days_from_high = int(rows.index[-1] - high_index)
    pullback_from_high = _ratio(close - high20, high20)
    rally_from_low60 = _ratio(high20 - low60, low60)
    day_return = _ratio(close - prev_close, prev_close)
    range_position = _ratio(close - low, high - low) if high > low else 0.0
    volume_contraction_ratio = _ratio(avg_volume5, prior_rally_volume)
    value_liquidity_ok = avg_value20 >= 500_000_000

    prev_close_shift = close_s.shift(1)
    tr = pd.concat(
        [
            high_s - low_s,
            (high_s - prev_close_shift).abs(),
            (low_s - prev_close_shift).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr14 = _safe_float(tr.tail(14).mean())
    low5 = _safe_float(low_s.tail(5).min())
    structure_stop = min(low5, close - atr14) if atr14 > 0 else low5
    risk = max(close - structure_stop, 0.0)
    reward = max(high20 - close, 0.0)
    rr = _ratio(reward, risk)
    risk_pct = _ratio(risk, close)

    pullback_dips = int(((high_s.rolling(10).max() - close_s) / high_s.rolling(10).max() >= 0.03).tail(20).sum())
    trend_axis = close >= sma50 and sma20 >= sma50 and sma20 >= sma20_prev and rally_from_low60 >= 0.12
    pullback_axis = -0.14 <= pullback_from_high <= -0.03 and close >= sma20 * 0.96
    volume_axis = 0.75 <= volume_contraction_ratio <= 1.8 and volume > 0 and value_liquidity_ok
    reaccel_axis = day_return > 0 and range_position >= 0.55 and (volume >= avg_volume5 or close >= prev_high)
    first_pullback_axis = 1 <= days_from_high <= 10 and pullback_dips <= 8
    rr_axis = rr >= 1.3 and risk > 0
    loss_risk_axis = 0 < risk_pct <= 0.06

    axis_score = sum([trend_axis, pullback_axis, volume_axis, reaccel_axis, first_pullback_axis, rr_axis])
    refined_pullback_watch = trend_axis and pullback_axis and first_pullback_axis and rr_axis and volume_axis and loss_risk_axis
    hard_risk = bool(latest.get("tradability_blocked", False)) or not value_liquidity_ok or close < 1000
    if hard_risk:
        methodology_status = "PULLBACK_FIRST_EXCLUDE"
    elif refined_pullback_watch:
        methodology_status = "PULLBACK_FIRST_REFINED_WATCH"
    elif axis_score >= 5:
        methodology_status = "PULLBACK_FIRST_STRONG"
    elif axis_score >= 4 and trend_axis and pullback_axis:
        methodology_status = "PULLBACK_FIRST_WATCH"
    else:
        methodology_status = "PULLBACK_FIRST_EXCLUDE"

    if code in current_codes and methodology_status != "PULLBACK_FIRST_EXCLUDE":
        compare_status = "BOTH"
    elif methodology_status != "PULLBACK_FIRST_EXCLUDE":
        compare_status = "METHODOLOGY_FIRST_ONLY"
    elif code in current_codes:
        compare_status = "CURRENT_POOL_ONLY"
    else:
        compare_status = "NEITHER"

    return {
        "asof": str(latest["date"]),
        "code": code,
        "market": _safe_text(latest.get("market", "")),
        "methodology_status": methodology_status,
        "compare_status": compare_status,
        "axis_score": axis_score,
        "trend_axis": _axis_label(trend_axis),
        "pullback_axis": _axis_label(pullback_axis),
        "volume_axis": _axis_label(volume_axis),
        "reaccel_axis": _axis_label(reaccel_axis),
        "first_pullback_axis": _axis_label(first_pullback_axis),
        "rr_axis": _axis_label(rr_axis),
        "loss_risk_axis": _axis_label(loss_risk_axis),
        "refined_pullback_watch": refined_pullback_watch,
        "close": round(close, 4),
        "pullback_from_high_pct": round(pullback_from_high, 6),
        "rally_from_low60_pct": round(rally_from_low60, 6),
        "days_from_high20": days_from_high,
        "pullback_dips_proxy": pullback_dips,
        "volume_contraction_ratio": round(volume_contraction_ratio, 6),
        "avg_value20": round(avg_value20, 2),
        "day_return_pct": round(day_return, 6),
        "range_position": round(range_position, 6),
        "rr_to_high20": round(rr, 6),
        "risk_pct": round(risk_pct, 6),
        "structure_stop": round(structure_stop, 4),
        "hard_risk": hard_risk,
        "tradability_blocked": bool(latest.get("tradability_blocked", False)),
        "current_pool": code in current_codes,
        "current_pool_sources": current_sources.get(code, ""),
        "connection_level": "SIGNAL_QUALITY_ONLY",
        "trading_connection": False,
        "signal_connection": False,
        "execution_connection": False,
        "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        "evidence": ";".join(
            [
                f"trend={trend_axis}",
                f"pullback={pullback_axis}",
                f"volume={volume_axis}",
                f"reaccel={reaccel_axis}",
                f"first_pullback={first_pullback_axis}",
                f"rr={rr_axis}",
                f"loss_risk={loss_risk_axis}",
            ]
        ),
    }


def build() -> dict[str, Any]:
    daily = _load_daily_history()
    current_codes, current_sources = _current_pool()

    rows: list[dict[str, Any]] = []
    latest_asof = str(daily["date"].max()) if not daily.empty else ""
    if not daily.empty:
        for code, group in daily.groupby("code", sort=False):
            scored = _score_code(code, group, latest_asof, current_codes, current_sources)
            if scored is not None:
                rows.append(scored)

    rows.sort(key=lambda r: (r["methodology_status"], -int(r["axis_score"]), -float(r["avg_value20"])))
    report_rows = [row for row in rows if row["methodology_status"] != "PULLBACK_FIRST_EXCLUDE" or row["current_pool"]]
    compare_rows = [row for row in rows if row["compare_status"] in {"BOTH", "METHODOLOGY_FIRST_ONLY", "CURRENT_POOL_ONLY"}]
    scored_codes = {row["code"] for row in rows}

    fields = [
        "asof",
        "code",
        "market",
        "methodology_status",
        "compare_status",
        "axis_score",
        "trend_axis",
        "pullback_axis",
        "volume_axis",
        "reaccel_axis",
        "first_pullback_axis",
        "rr_axis",
        "loss_risk_axis",
        "refined_pullback_watch",
        "close",
        "pullback_from_high_pct",
        "rally_from_low60_pct",
        "days_from_high20",
        "pullback_dips_proxy",
        "volume_contraction_ratio",
        "avg_value20",
        "day_return_pct",
        "range_position",
        "rr_to_high20",
        "risk_pct",
        "structure_stop",
        "hard_risk",
        "tradability_blocked",
        "current_pool",
        "current_pool_sources",
        "connection_level",
        "trading_connection",
        "signal_connection",
        "execution_connection",
        "connection_note",
        "evidence",
    ]
    _write_csv(OUT_CSV, report_rows, fields)
    _write_csv(OUT_COMPARE_CSV, compare_rows, fields)

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
        "source_files": {
            "krx_daily_archive_dir": str(ARCHIVE_DIR),
            "current_pool_files": [str(path) for path in CURRENT_POOL_FILES],
        },
        "daily_rows_loaded": int(len(daily)),
        "archive_files_loaded": len(_latest_daily_files()),
        "unique_codes_screened": int(daily["code"].nunique()) if not daily.empty else 0,
        "latest_asof": latest_asof,
        "current_pool_codes": len(current_codes),
        "current_pool_not_screened": sorted(current_codes - scored_codes),
        "output_rows": len(report_rows),
        "compare_rows": len(compare_rows),
        "methodology_status_counts": dict(Counter(row["methodology_status"] for row in rows)),
        "compare_status_counts": dict(Counter(row["compare_status"] for row in rows)),
        "refined_pullback_watch_count": sum(1 for row in rows if row["refined_pullback_watch"]),
        "axis_pass_counts": {
            axis: sum(1 for row in rows if row[axis] == "PASS")
            for axis in (
                "trend_axis",
                "pullback_axis",
                "volume_axis",
                "reaccel_axis",
                "first_pullback_axis",
                "rr_axis",
                "loss_risk_axis",
            )
        },
        "top_refined_pullback": [
            row for row in report_rows if row["refined_pullback_watch"]
        ][:30],
        "top_methodology_first": [
            row for row in report_rows if row["methodology_status"] != "PULLBACK_FIRST_EXCLUDE"
        ][:30],
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
