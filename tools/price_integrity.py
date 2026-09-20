from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import pandas as pd


@dataclass(frozen=True)
class CorporateActionDecision:
    apply_adjusted: bool
    raw_move_abs: float
    adjusted_move_abs: float
    reason: str


def _num(v: Any) -> float | None:
    try:
        x = float(v)
    except Exception:
        return None
    if not math.isfinite(x):
        return None
    return x


def norm_ymd8(v: Any) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s[:8]


def build_prev_close_map(
    df: pd.DataFrame,
    *,
    code_col: str = "code",
    date_col: str = "date",
    close_col: str = "close",
) -> Dict[str, Dict[str, Any]]:
    if df is None or df.empty:
        return {}
    work = df.copy()
    if code_col not in work.columns or date_col not in work.columns or close_col not in work.columns:
        return {}
    work[code_col] = work[code_col].astype(str).str.zfill(6)
    work[date_col] = work[date_col].map(norm_ymd8)
    work[close_col] = pd.to_numeric(work[close_col], errors="coerce")
    work = work.dropna(subset=[close_col])
    work = work[(work[code_col].str.len() == 6) & (work[date_col].str.len() == 8)]
    if work.empty:
        return {}
    work = work.sort_values([code_col, date_col], kind="mergesort")
    last = work.groupby(code_col, as_index=False).tail(1)
    out: Dict[str, Dict[str, Any]] = {}
    for row in last.itertuples(index=False):
        code = str(getattr(row, code_col)).zfill(6)
        out[code] = {
            "date": norm_ymd8(getattr(row, date_col)),
            "close": float(getattr(row, close_col)),
        }
    return out


def detect_day_price_issues(
    day_df: pd.DataFrame,
    prev_close_map: Dict[str, Dict[str, Any]],
    *,
    spike_threshold_abs: float = 0.45,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict[str, Any]]]:
    if day_df is None or day_df.empty:
        empty = pd.DataFrame(columns=list(day_df.columns) if isinstance(day_df, pd.DataFrame) else [])
        return empty.copy(), empty.copy(), []

    work = day_df.copy()
    work["code"] = work["code"].astype(str).str.zfill(6)
    work["date"] = work["date"].map(norm_ymd8)
    for col in ["open", "high", "low", "close", "volume"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    blocked_rows: List[Dict[str, Any]] = []
    spike_rows: List[Dict[str, Any]] = []
    keep_mask = pd.Series(True, index=work.index)

    for idx, row in work.iterrows():
        code = str(row.get("code") or "").zfill(6)
        ymd = norm_ymd8(row.get("date"))
        open_px = _num(row.get("open"))
        high_px = _num(row.get("high"))
        low_px = _num(row.get("low"))
        close_px = _num(row.get("close"))
        volume = _num(row.get("volume"))

        reasons: List[str] = []
        if not ymd or len(code) != 6:
            reasons.append("bad_key")
        if any(v is None for v in [open_px, high_px, low_px, close_px]):
            reasons.append("nan_price")
        if not reasons:
            if min(open_px, high_px, low_px, close_px) <= 0:
                reasons.append("non_positive_price")
            if high_px < low_px:
                reasons.append("high_lt_low")
            if open_px < low_px or open_px > high_px:
                reasons.append("open_outside_range")
            if close_px < low_px or close_px > high_px:
                reasons.append("close_outside_range")
        if volume is not None and volume < 0:
            reasons.append("negative_volume")

        prev_ctx = prev_close_map.get(code, {})
        prev_close = _num(prev_ctx.get("close"))
        prev_date = norm_ymd8(prev_ctx.get("date"))
        if not reasons and prev_close is not None and prev_close > 0:
            move_abs = abs((close_px / prev_close) - 1.0)
            if volume == 0 and not math.isclose(close_px, prev_close, rel_tol=0.0, abs_tol=1e-12):
                reasons.append("zero_volume_price_move")
            if move_abs >= float(spike_threshold_abs):
                spike_rows.append(
                    {
                        "code": code,
                        "date": ymd,
                        "prev_date": prev_date,
                        "prev_close": prev_close,
                        "raw_close": close_px,
                        "raw_move_abs": move_abs,
                    }
                )

        if reasons:
            keep_mask.loc[idx] = False
            blocked_rows.append(
                {
                    "code": code,
                    "date": ymd,
                    "reasons": reasons,
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "volume": row.get("volume"),
                }
            )

    clean = work.loc[keep_mask].copy()
    blocked = pd.DataFrame(blocked_rows)
    return clean, blocked, spike_rows


def decide_corporate_action_repair(
    *,
    prev_close_raw: Any,
    close_raw: Any,
    prev_close_adjusted: Any,
    close_adjusted: Any,
    raw_move_trigger_abs: float = 0.45,
    adjusted_move_accept_abs: float = 0.30,
) -> CorporateActionDecision:
    prev_raw = _num(prev_close_raw)
    close_raw_num = _num(close_raw)
    prev_adj = _num(prev_close_adjusted)
    close_adj = _num(close_adjusted)
    if any(v is None or v <= 0 for v in [prev_raw, close_raw_num, prev_adj, close_adj]):
        return CorporateActionDecision(False, 0.0, 0.0, "insufficient_adjusted_window")

    raw_move_abs = abs((close_raw_num / prev_raw) - 1.0)
    adjusted_move_abs = abs((close_adj / prev_adj) - 1.0)
    if raw_move_abs < float(raw_move_trigger_abs):
        return CorporateActionDecision(False, raw_move_abs, adjusted_move_abs, "raw_move_below_trigger")
    if adjusted_move_abs >= raw_move_abs:
        return CorporateActionDecision(False, raw_move_abs, adjusted_move_abs, "adjusted_not_better")
    if adjusted_move_abs > float(adjusted_move_accept_abs):
        return CorporateActionDecision(False, raw_move_abs, adjusted_move_abs, "adjusted_move_still_too_large")
    return CorporateActionDecision(True, raw_move_abs, adjusted_move_abs, "adjusted_window_repair")
