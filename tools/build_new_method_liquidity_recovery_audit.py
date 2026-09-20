#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Audit whether liquidity-gap new-method candidates recover tradable liquidity.

This is read-only research validation. It uses existing new-method history and
KRX daily archive prices; it does not change operational gates or orders.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
KRX_ARCHIVE = ROOT / "krx_daily_archive"

INPUT_HISTORY_TYPE = LOG_DIR / "new_method_history_type_outcome_audit_latest.csv"
INPUT_HISTORY = LOG_DIR / "new_method_candidates_history.csv"

OUT_DETAIL_CSV = LOG_DIR / "new_method_liquidity_recovery_audit_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "new_method_liquidity_recovery_summary_latest.csv"
OUT_JSON = LOG_DIR / "new_method_liquidity_recovery_audit_latest.json"
OUT_MD = LOG_DIR / "new_method_liquidity_recovery_audit_latest.md"

TRADABLE_VALUE_KRW = 5_000_000_000.0
ROBUST_VALUE_KRW = 15_000_000_000.0


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _norm_code_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)


def _load_krx_dates(start_ymd: str, end_ymd: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(KRX_ARCHIVE.glob("krx_daily_*_clean.parquet")):
        name = path.name
        parts = name.replace("krx_daily_", "").replace("_clean.parquet", "").split("_")
        if len(parts) < 2:
            continue
        p_start, p_end = parts[0], parts[1]
        if p_end < start_ymd or p_start > end_ymd:
            continue
        df = pd.read_parquet(path, columns=["date", "code", "open", "high", "low", "close", "value"])
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["date", "code", "open", "high", "low", "close", "value"])
    out = pd.concat(frames, ignore_index=True)
    out["date"] = out["date"].astype(str).str.replace("-", "", regex=False)
    out["code"] = _norm_code_series(out["code"])
    for col in ["open", "high", "low", "close", "value"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.drop_duplicates(["date", "code"], keep="last")
    return out


def _setup_type_fallback(row: pd.Series) -> str:
    trade_value = _float(row.get("trade_value"))
    if trade_value < TRADABLE_VALUE_KRW:
        return "SETUP_LIQUIDITY_GAP"
    return "NON_LIQUIDITY_GAP"


def _summarize(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for key, g in df.groupby(keys, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        closed = g[g["forward_return_status"].astype(str).str.upper().eq("CLOSED")].copy()
        ret_all = pd.to_numeric(closed["forward_return"], errors="coerce").dropna()
        recovered = g[g["recovered_to_tradable"].astype(bool)]
        recovered_closed = recovered[recovered["forward_return_status"].astype(str).str.upper().eq("CLOSED")]
        ret_recovered = pd.to_numeric(recovered_closed["forward_return"], errors="coerce").dropna()
        not_recovered = g[~g["recovered_to_tradable"].astype(bool)]
        not_recovered_closed = not_recovered[not_recovered["forward_return_status"].astype(str).str.upper().eq("CLOSED")]
        ret_not_recovered = pd.to_numeric(not_recovered_closed["forward_return"], errors="coerce").dropna()
        row = {k: v for k, v in zip(keys, key)}
        row.update(
            {
                "rows": int(len(g)),
                "closed_rows": int(len(ret_all)),
                "pending_rows": int((g["forward_return_status"].astype(str).str.upper() != "CLOSED").sum()),
                "recovered_to_tradable_rows": int(len(recovered)),
                "recovered_to_robust_rows": int(g["recovered_to_robust"].astype(bool).sum()),
                "recovery_rate_tradable": round(float(len(recovered)) / float(len(g)), 6) if len(g) else None,
                "recovered_closed_rows": int(len(ret_recovered)),
                "recovered_win_rate": round(float((ret_recovered > 0).mean()), 6) if len(ret_recovered) else None,
                "recovered_avg_forward_return": round(float(ret_recovered.mean()), 6) if len(ret_recovered) else None,
                "not_recovered_closed_rows": int(len(ret_not_recovered)),
                "not_recovered_win_rate": round(float((ret_not_recovered > 0).mean()), 6) if len(ret_not_recovered) else None,
                "not_recovered_avg_forward_return": round(float(ret_not_recovered.mean()), 6) if len(ret_not_recovered) else None,
            }
        )
        if len(ret_recovered) >= 5 and (row["recovered_avg_forward_return"] or 0) > 0:
            row["evidence_layer"] = "RECOVERY_LAYER_PROMISING"
        elif len(ret_recovered) > 0:
            row["evidence_layer"] = "RECOVERY_LAYER_SMALL_SAMPLE"
        else:
            row["evidence_layer"] = "NO_RECOVERY_CLOSED_SAMPLE"
        rows.append(row)
    return pd.DataFrame(rows).sort_values(keys).reset_index(drop=True)


def main() -> int:
    if not INPUT_HISTORY.exists():
        raise SystemExit(f"missing input: {INPUT_HISTORY}")
    hist = pd.read_csv(INPUT_HISTORY, dtype={"code": str}, encoding="utf-8-sig")
    hist["code"] = _norm_code_series(hist["code"])
    hist["date_ymd"] = hist["date"].astype(str).str.replace("-", "", regex=False)
    for col in ["trade_value", "forward_return", "expected_holding_days", "close"]:
        hist[col] = pd.to_numeric(hist[col], errors="coerce")
    if INPUT_HISTORY_TYPE.exists():
        typed = pd.read_csv(INPUT_HISTORY_TYPE, dtype={"code": str}, encoding="utf-8-sig")
        typed["code"] = _norm_code_series(typed["code"])
        typed["date_ymd"] = typed["date"].astype(str).str.replace("-", "", regex=False)
        hist = hist.merge(
            typed[["date_ymd", "code", "setup_type", "liquidity_state"]],
            on=["date_ymd", "code"],
            how="left",
            suffixes=("", "_typed"),
        )
    if "setup_type" not in hist.columns:
        hist["setup_type"] = hist.apply(_setup_type_fallback, axis=1)
    hist["setup_type"] = hist["setup_type"].fillna(hist.apply(_setup_type_fallback, axis=1))
    gap = hist[hist["setup_type"].eq("SETUP_LIQUIDITY_GAP")].copy()

    if gap.empty:
        raise SystemExit("no SETUP_LIQUIDITY_GAP rows")

    start_ymd = str(gap["date_ymd"].min())
    end_ymd = "20260714"
    daily = _load_krx_dates(start_ymd, end_ymd)
    trading_dates = sorted(daily["date"].dropna().astype(str).unique().tolist())
    date_pos = {d: i for i, d in enumerate(trading_dates)}

    by_code = {code: g.sort_values("date").reset_index(drop=True) for code, g in daily.groupby("code")}
    out_rows: list[dict[str, Any]] = []
    for _, row in gap.iterrows():
        signal_date = str(row["date_ymd"])
        code = str(row["code"])
        holding = int(_float(row.get("expected_holding_days"), 0))
        pos = date_pos.get(signal_date)
        candidate_dates: list[str] = []
        if pos is not None:
            candidate_dates = trading_dates[pos + 1 : pos + 1 + max(0, holding)]
        px = by_code.get(code, pd.DataFrame())
        window = (px[px["date"].isin(candidate_dates)].copy() if not px.empty and candidate_dates else pd.DataFrame(columns=["date", "code", "open", "high", "low", "close", "value"]))
        recovered_tradable = window[window["value"] >= TRADABLE_VALUE_KRW].sort_values("date")
        recovered_robust = window[window["value"] >= ROBUST_VALUE_KRW].sort_values("date")
        first_tradable = recovered_tradable.iloc[0].to_dict() if not recovered_tradable.empty else {}
        first_robust = recovered_robust.iloc[0].to_dict() if not recovered_robust.empty else {}
        signal_close = _float(row.get("close"))
        first_tradable_close = _float(first_tradable.get("close")) if first_tradable else 0.0
        ret_to_first_tradable = (first_tradable_close / signal_close - 1.0) if signal_close > 0 and first_tradable_close > 0 else None
        max_value = float(window["value"].max()) if not window.empty and window["value"].notna().any() else None
        out = row.to_dict()
        out.update(
            {
                "liquidity_recovery_window_dates": ",".join(candidate_dates),
                "liquidity_recovery_window_rows": int(len(window)),
                "max_value_in_window": max_value,
                "recovered_to_tradable": bool(first_tradable),
                "first_tradable_date": str(first_tradable.get("date", "")) if first_tradable else "",
                "first_tradable_value": float(first_tradable.get("value", 0.0)) if first_tradable else 0.0,
                "first_tradable_close": first_tradable_close if first_tradable else 0.0,
                "ret_to_first_tradable": ret_to_first_tradable,
                "recovered_to_robust": bool(first_robust),
                "first_robust_date": str(first_robust.get("date", "")) if first_robust else "",
                "first_robust_value": float(first_robust.get("value", 0.0)) if first_robust else 0.0,
                "recovery_interpretation": (
                    "TRADABLE_RECOVERED"
                    if first_tradable
                    else ("NO_WINDOW_DATA" if window.empty else "NO_TRADABLE_RECOVERY")
                ),
            }
        )
        out_rows.append(out)

    detail = pd.DataFrame(out_rows)
    summary_branch = _summarize(detail, ["regime", "method_branch", "horizon"])
    summary_status = _summarize(detail, ["forward_return_status"])
    summary = pd.concat(
        [
            summary_branch.assign(summary_level="branch"),
            summary_status.assign(regime="ALL", method_branch="ALL", horizon="ALL", summary_level="forward_status"),
        ],
        ignore_index=True,
        sort=False,
    )
    detail_cols = [
        "date",
        "code",
        "regime",
        "method_branch",
        "horizon",
        "expected_holding_days",
        "rank_in_branch",
        "trade_value",
        "setup_type",
        "forward_return",
        "forward_return_status",
        "liquidity_recovery_window_dates",
        "liquidity_recovery_window_rows",
        "max_value_in_window",
        "recovered_to_tradable",
        "first_tradable_date",
        "first_tradable_value",
        "ret_to_first_tradable",
        "recovered_to_robust",
        "first_robust_date",
        "first_robust_value",
        "recovery_interpretation",
        "promotion_blocker",
    ]
    for col in detail_cols:
        if col not in detail.columns:
            detail[col] = ""
    detail[detail_cols].to_csv(OUT_DETAIL_CSV, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_new_method_liquidity_recovery_audit",
        "input_history": str(INPUT_HISTORY),
        "input_history_type": str(INPUT_HISTORY_TYPE),
        "krx_archive": str(KRX_ARCHIVE),
        "rows": int(len(detail)),
        "unique_codes": int(detail["code"].nunique()),
        "date_min": str(detail["date"].min()),
        "date_max": str(detail["date"].max()),
        "recovered_to_tradable_rows": int(detail["recovered_to_tradable"].astype(bool).sum()),
        "recovered_to_robust_rows": int(detail["recovered_to_robust"].astype(bool).sum()),
        "summary_records": summary.to_dict(orient="records"),
        "limitations": [
            "Recovery is measured on daily KRX value after signal date within expected_holding_days.",
            "No intraday order book, slippage, or fill model is applied.",
            "Rows with pending forward_return are not counted as closed wins or losses.",
            "This is a research observation layer, not an operational gate change.",
        ],
        "operational_change": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# New Method Liquidity Recovery Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- rows: {payload['rows']}",
        f"- unique_codes: {payload['unique_codes']}",
        f"- date_range: {payload['date_min']} to {payload['date_max']}",
        f"- recovered_to_tradable_rows: {payload['recovered_to_tradable_rows']}",
        f"- recovered_to_robust_rows: {payload['recovered_to_robust_rows']}",
        "",
        "## Branch summary",
    ]
    for _, r in summary_branch.iterrows():
        avg = r.get("recovered_avg_forward_return")
        win = r.get("recovered_win_rate")
        avg_s = "-" if pd.isna(avg) else f"{float(avg):.4f}"
        win_s = "-" if pd.isna(win) else f"{float(win):.3f}"
        lines.append(
            f"- {r['regime']} / {r['method_branch']} / {r['horizon']}: "
            f"rows={int(r['rows'])}, recovered={int(r['recovered_to_tradable_rows'])}, "
            f"closed_recovered={int(r['recovered_closed_rows'])}, recovered_win_rate={win_s}, "
            f"recovered_avg_ret={avg_s}, layer={r['evidence_layer']}"
        )
    lines.extend(["", "## Caveats"])
    for item in payload["limitations"]:
        lines.append(f"- {item}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "detail": str(OUT_DETAIL_CSV), "summary": str(OUT_SUMMARY_CSV), "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
