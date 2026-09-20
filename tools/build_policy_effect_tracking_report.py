from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(r"E:\1_Data")
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
TRADES_CALC = PAPER_DIR / "trades_calc.csv"
FILLS = PAPER_DIR / "fills.csv"
LATEST_JSON = LOG_DIR / "policy_effect_tracking_latest.json"
LATEST_CSV = LOG_DIR / "policy_effect_tracking_segments_latest.csv"

POLICY_START_YMD = "20260428"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _dated_json_path() -> Path:
    return LOG_DIR / f"policy_effect_tracking_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"


def _dated_csv_path() -> Path:
    return LOG_DIR / f"policy_effect_tracking_segments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig")


def _ymd(value: Any) -> str:
    text = str(value or "")
    digits = re.sub(r"[^0-9]", "", text)
    return digits[:8] if len(digits) >= 8 else ""


def _num(series: pd.Series | Any, default: float = 0.0) -> pd.Series:
    if isinstance(series, pd.Series):
        return pd.to_numeric(series, errors="coerce").fillna(default)
    return pd.Series(dtype=float)


def _flag(note: pd.Series, pattern: str) -> pd.Series:
    return note.astype(str).str.contains(pattern, case=False, regex=True, na=False)


def _safe_sum(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return float(pd.to_numeric(series, errors="coerce").fillna(0.0).sum())


def _closed_trades() -> pd.DataFrame:
    df = _read_csv(TRADES_CALC)
    if df.empty:
        return df
    for col in ["trade_id", "entry_ts", "exit_ts", "code", "qty", "entry_price", "exit_price", "net_ret", "note"]:
        if col not in df.columns:
            df[col] = ""
    df["entry_ymd"] = df["entry_ts"].map(_ymd)
    df["exit_ymd"] = df["exit_ts"].map(_ymd)
    df["qty_num"] = _num(df["qty"])
    df["entry_price_num"] = _num(df["entry_price"])
    df["notional_krw"] = df["qty_num"] * df["entry_price_num"]
    df["net_ret_num"] = _num(df["net_ret"])
    df["pnl_krw"] = df["notional_krw"] * df["net_ret_num"]
    note = df["note"].astype(str)
    df["policy_window"] = df["entry_ymd"].ge(POLICY_START_YMD)
    df["surge_immediate"] = _flag(note, r"surge_immediate=1")
    df["overheat_reduced"] = _flag(note, r"overheat_reduce=|overheat_qty=")
    df["split_first"] = _flag(note, r"split_entry=1st")
    df["split_second"] = _flag(note, r"split_entry=2nd")
    df["split_second_linked"] = _flag(note, r"split_first_order_id=.+")
    df["surge_type_reduced"] = _flag(note, r"surge_type_qty=|surge_type_mult=")
    return df


def _buy_fills() -> pd.DataFrame:
    df = _read_csv(FILLS)
    if df.empty:
        return df
    for col in ["datetime", "code", "side", "qty", "price", "order_id", "note"]:
        if col not in df.columns:
            df[col] = ""
    df = df[df["side"].astype(str).str.upper().eq("BUY")].copy()
    if df.empty:
        return df
    df["entry_ymd"] = df["datetime"].map(_ymd)
    df["qty_num"] = _num(df["qty"])
    df["price_num"] = _num(df["price"])
    df["notional_krw"] = df["qty_num"] * df["price_num"]
    note = df["note"].astype(str)
    df["policy_window"] = df["entry_ymd"].ge(POLICY_START_YMD)
    df["surge_immediate"] = _flag(note, r"surge_immediate=1")
    df["overheat_reduced"] = _flag(note, r"overheat_reduce=|overheat_qty=")
    df["split_first"] = _flag(note, r"split_entry=1st")
    df["split_second"] = _flag(note, r"split_entry=2nd")
    df["split_second_linked"] = _flag(note, r"split_first_order_id=.+")
    df["surge_type_reduced"] = _flag(note, r"surge_type_qty=|surge_type_mult=")
    return df


def _segment_closed(df: pd.DataFrame, name: str, mask: pd.Series) -> dict[str, Any]:
    sub = df.loc[mask].copy()
    pnl = _safe_sum(sub["pnl_krw"]) if "pnl_krw" in sub.columns else 0.0
    wins = int((pd.to_numeric(sub.get("pnl_krw", pd.Series(dtype=float)), errors="coerce").fillna(0.0) > 0).sum())
    losses_abs = abs(_safe_sum(sub.loc[pd.to_numeric(sub.get("pnl_krw", pd.Series(dtype=float)), errors="coerce").fillna(0.0) < 0, "pnl_krw"])) if not sub.empty else 0.0
    profits = _safe_sum(sub.loc[pd.to_numeric(sub.get("pnl_krw", pd.Series(dtype=float)), errors="coerce").fillna(0.0) > 0, "pnl_krw"]) if not sub.empty else 0.0
    return {
        "segment": name,
        "closed_trades": int(len(sub)),
        "closed_notional_krw": round(_safe_sum(sub.get("notional_krw", pd.Series(dtype=float))), 2),
        "closed_pnl_krw": round(pnl, 2),
        "win_rate_pct": round((wins / len(sub) * 100.0), 4) if len(sub) else 0.0,
        "profit_factor": round((profits / losses_abs), 6) if losses_abs > 0 else (999.0 if profits > 0 else 0.0),
    }


def _segment_buys(df: pd.DataFrame, name: str, mask: pd.Series) -> dict[str, Any]:
    sub = df.loc[mask].copy()
    return {
        "segment": name,
        "buy_fills": int(len(sub)),
        "buy_notional_krw": round(_safe_sum(sub.get("notional_krw", pd.Series(dtype=float))), 2),
        "unique_codes": int(sub["code"].astype(str).str.zfill(6).nunique()) if "code" in sub.columns and len(sub) else 0,
    }


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    closed = _closed_trades()
    buys = _buy_fills()

    segments: list[dict[str, Any]] = []
    buy_segments: list[dict[str, Any]] = []

    segment_defs = [
        ("전체", lambda d: pd.Series(True, index=d.index)),
        ("정책 적용일 이후", lambda d: d["policy_window"]),
        ("급등 즉시진입", lambda d: d["surge_immediate"]),
        ("정책 이후 급등 즉시진입", lambda d: d["policy_window"] & d["surge_immediate"]),
        ("과열 감액 적용", lambda d: d["overheat_reduced"]),
        ("정책 이후 과열 감액 적용", lambda d: d["policy_window"] & d["overheat_reduced"]),
        ("2차 분할진입", lambda d: d["split_second"]),
        ("2차 분할진입 링크 확인", lambda d: d["split_second_linked"]),
        ("급등 유형 감액", lambda d: d["surge_type_reduced"]),
    ]

    if not closed.empty:
        for name, fn in segment_defs:
            segments.append(_segment_closed(closed, name, fn(closed)))
    if not buys.empty:
        for name, fn in segment_defs:
            buy_segments.append(_segment_buys(buys, name, fn(buys)))

    closed_seg_df = pd.DataFrame(segments)
    buy_seg_df = pd.DataFrame(buy_segments)
    merged = closed_seg_df.merge(buy_seg_df, on="segment", how="outer") if not closed_seg_df.empty or not buy_seg_df.empty else pd.DataFrame()
    merged.to_csv(LATEST_CSV, index=False, encoding="utf-8-sig")
    dated_csv = _dated_csv_path()
    merged.to_csv(dated_csv, index=False, encoding="utf-8-sig")

    result = {
        "generated_at": _now(),
        "policy_start_ymd": POLICY_START_YMD,
        "inputs": {
            "trades_calc": str(TRADES_CALC),
            "fills": str(FILLS),
        },
        "summary": {
            "closed_trades_total": int(len(closed)),
            "buy_fills_total": int(len(buys)),
            "policy_window_closed_trades": int(closed["policy_window"].sum()) if not closed.empty else 0,
            "policy_window_buy_fills": int(buys["policy_window"].sum()) if not buys.empty else 0,
            "policy_window_overheat_reduced_buys": int((buys["policy_window"] & buys["overheat_reduced"]).sum()) if not buys.empty else 0,
            "policy_window_split_second_linked_buys": int((buys["policy_window"] & buys["split_second_linked"]).sum()) if not buys.empty else 0,
        },
        "segments": segments,
        "buy_segments": buy_segments,
        "outputs": {
            "latest_json": str(LATEST_JSON),
            "latest_csv": str(LATEST_CSV),
            "dated_csv": str(dated_csv),
        },
        "interpretation": [
            "closed_trades는 실제 청산된 거래 손익 기준이다.",
            "buy_fills는 아직 청산 전인 정책 적용 건수를 추적한다.",
            "정책 적용 직후에는 buy_fills가 먼저 늘고 closed_trades 평가는 지연된다.",
        ],
    }
    text = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    LATEST_JSON.write_text(text, encoding="utf-8")
    dated_json = _dated_json_path()
    dated_json.write_text(text, encoding="utf-8")
    print(json.dumps({"latest": str(LATEST_JSON), "csv": str(LATEST_CSV), "dated": str(dated_json)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
