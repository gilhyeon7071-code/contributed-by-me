#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Apply candidate-type grammar to new-method history and summarize forward outcomes.

This is read-only research validation. It does not change operational gates,
scores, order flow, or candidate generation.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

INPUT_HISTORY = LOG_DIR / "new_method_candidates_history.csv"
OUT_DETAIL_CSV = LOG_DIR / "new_method_history_type_outcome_audit_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "new_method_history_type_outcome_summary_latest.csv"
OUT_JSON = LOG_DIR / "new_method_history_type_outcome_audit_latest.json"
OUT_MD = LOG_DIR / "new_method_history_type_outcome_audit_latest.md"

LIQUIDITY_MIN = 5_000_000_000.0
ROBUST_LIQUIDITY_MIN = 15_000_000_000.0
LOW_ATR_MAX = 0.04
VERY_LOW_RSI_MAX = 30.0
LOW_RSI_MAX = 40.0
STRONG_RS_MIN = 1.0


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _status(value: Any) -> str:
    return str(value or "").strip().upper()


def _liquidity_state(row: pd.Series) -> str:
    trade_value = _float(row.get("trade_value"))
    if trade_value >= ROBUST_LIQUIDITY_MIN:
        return "ROBUST_LIQUID"
    if trade_value >= LIQUIDITY_MIN:
        return "TRADABLE_LIQUID"
    return "LIQUIDITY_GAP"


def _volatility_state(row: pd.Series) -> str:
    atr = _float(row.get("atr_pct"))
    if atr <= 0:
        return "ATR_UNKNOWN"
    if atr <= LOW_ATR_MAX:
        return "LOW_ATR"
    return "NORMAL_OR_HIGH_ATR"


def _rsi_state(row: pd.Series) -> str:
    rsi = _float(row.get("rsi14"))
    if rsi <= 0:
        return "RSI_UNKNOWN"
    if rsi <= VERY_LOW_RSI_MAX:
        return "VERY_OVERSOLD"
    if rsi <= LOW_RSI_MAX:
        return "OVERSOLD"
    return "NOT_OVERSOLD"


def _rs_state(row: pd.Series) -> str:
    rs = _float(row.get("rs"))
    if rs >= STRONG_RS_MIN:
        return "STRONG_RS"
    if rs > 0:
        return "WEAK_OR_NEUTRAL_RS"
    return "RS_UNKNOWN"


def _rank_bucket(row: pd.Series) -> str:
    rank = int(_float(row.get("rank_in_branch"), 999))
    if rank <= 3:
        return "TOP3"
    if rank <= 7:
        return "MID4_7"
    return "TAIL8_PLUS"


def _setup_type(row: pd.Series) -> str:
    branch = str(row.get("method_branch") or "").upper()
    liquidity = str(row.get("liquidity_state"))
    volatility = str(row.get("volatility_state"))
    rsi = str(row.get("rsi_state"))
    rs = str(row.get("rs_state"))

    if liquidity == "LIQUIDITY_GAP":
        return "SETUP_LIQUIDITY_GAP"
    if "STRESS" in branch and rsi in {"VERY_OVERSOLD", "OVERSOLD"} and rs == "STRONG_RS":
        return "STRESS_OVERSOLD_STRONG_RS"
    if "BEAR" in branch and volatility == "LOW_ATR" and rsi in {"VERY_OVERSOLD", "OVERSOLD"}:
        return "BEAR_LOW_ATR_OVERSOLD"
    if "TRANSITION" in branch and volatility == "LOW_ATR" and rs == "STRONG_RS":
        return "TRANSITION_LOW_ATR_STRONG_RS"
    if volatility == "LOW_ATR" and rs == "STRONG_RS":
        return "LOW_ATR_STRONG_RS_OTHER"
    return "METHOD_BRANCH_DEFAULT"


def _outcome_bucket(row: pd.Series) -> str:
    status = _status(row.get("forward_return_status"))
    if status != "CLOSED":
        return "PENDING_OR_NOT_CLOSED"
    ret = _float(row.get("forward_return"))
    if ret > 0:
        return "CLOSED_WIN"
    if ret < 0:
        return "CLOSED_LOSS"
    return "CLOSED_FLAT"


def _summary(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_key, g in df.groupby(keys, dropna=False):
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        closed = g[g["forward_return_status"].astype(str).str.upper().eq("CLOSED")].copy()
        returns = pd.to_numeric(closed["forward_return"], errors="coerce").dropna()
        row = {k: v for k, v in zip(keys, group_key)}
        closed_n = int(len(returns))
        row.update(
            {
                "rows": int(len(g)),
                "closed_rows": closed_n,
                "pending_rows": int((g["forward_return_status"].astype(str).str.upper() != "CLOSED").sum()),
                "win_rows": int((returns > 0).sum()) if closed_n else 0,
                "loss_rows": int((returns < 0).sum()) if closed_n else 0,
                "win_rate": round(float((returns > 0).mean()), 6) if closed_n else None,
                "avg_forward_return": round(float(returns.mean()), 6) if closed_n else None,
                "median_forward_return": round(float(returns.median()), 6) if closed_n else None,
                "min_forward_return": round(float(returns.min()), 6) if closed_n else None,
                "max_forward_return": round(float(returns.max()), 6) if closed_n else None,
            }
        )
        if closed_n >= 5 and row["avg_forward_return"] is not None and row["avg_forward_return"] > 0 and (row["win_rate"] or 0) >= 0.5:
            row["evidence_layer"] = "PROMISING_CLOSED_SAMPLE"
        elif closed_n >= 5 and row["avg_forward_return"] is not None and row["avg_forward_return"] <= 0:
            row["evidence_layer"] = "WEAK_CLOSED_SAMPLE"
        else:
            row["evidence_layer"] = "INSUFFICIENT_OR_PENDING_SAMPLE"
        rows.append(row)
    return pd.DataFrame(rows).sort_values(keys).reset_index(drop=True)


def main() -> int:
    if not INPUT_HISTORY.exists():
        raise SystemExit(f"missing input: {INPUT_HISTORY}")
    df = pd.read_csv(INPUT_HISTORY, dtype={"code": str}, encoding="utf-8-sig")
    df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    for col in ["candidate_score", "rank_in_branch", "close", "trade_value", "forward_return", "rs", "rsi14", "stretch", "atr_pct"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["date", "regime", "method_branch", "horizon", "forward_return_status", "status", "promotion_blocker"]:
        if col not in df.columns:
            df[col] = ""

    df["liquidity_state"] = df.apply(_liquidity_state, axis=1)
    df["volatility_state"] = df.apply(_volatility_state, axis=1)
    df["rsi_state"] = df.apply(_rsi_state, axis=1)
    df["rs_state"] = df.apply(_rs_state, axis=1)
    df["rank_bucket"] = df.apply(_rank_bucket, axis=1)
    df["setup_type"] = df.apply(_setup_type, axis=1)
    df["outcome_bucket"] = df.apply(_outcome_bucket, axis=1)

    detail_cols = [
        "date",
        "code",
        "regime",
        "method_branch",
        "horizon",
        "rank_in_branch",
        "rank_bucket",
        "candidate_score",
        "trade_value",
        "liquidity_state",
        "atr_pct",
        "volatility_state",
        "rsi14",
        "rsi_state",
        "rs",
        "rs_state",
        "stretch",
        "setup_type",
        "forward_return",
        "forward_return_status",
        "outcome_bucket",
        "status",
        "promotion_blocker",
    ]
    df[detail_cols].to_csv(OUT_DETAIL_CSV, index=False, encoding="utf-8-sig")

    summary_branch_type = _summary(df, ["regime", "method_branch", "horizon", "setup_type"])
    summary_type = _summary(df, ["setup_type"])
    summary_branch = _summary(df, ["regime", "method_branch", "horizon"])
    summary = pd.concat(
        [
            summary_branch_type.assign(summary_level="branch_setup_type"),
            summary_type.assign(regime="ALL", method_branch="ALL", horizon="ALL", summary_level="setup_type"),
            summary_branch.assign(setup_type="ALL", summary_level="branch"),
        ],
        ignore_index=True,
        sort=False,
    )
    ordered = [
        "summary_level",
        "regime",
        "method_branch",
        "horizon",
        "setup_type",
        "rows",
        "closed_rows",
        "pending_rows",
        "win_rows",
        "loss_rows",
        "win_rate",
        "avg_forward_return",
        "median_forward_return",
        "min_forward_return",
        "max_forward_return",
        "evidence_layer",
    ]
    summary[ordered].to_csv(OUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_new_method_history_type_outcome_audit",
        "input_history": str(INPUT_HISTORY),
        "rows": int(len(df)),
        "unique_codes": int(df["code"].nunique()),
        "date_min": str(df["date"].min()),
        "date_max": str(df["date"].max()),
        "forward_return_status_counts": {str(k): int(v) for k, v in df["forward_return_status"].astype(str).value_counts().to_dict().items()},
        "setup_type_counts": {str(k): int(v) for k, v in df["setup_type"].value_counts().to_dict().items()},
        "outcome_bucket_counts": {str(k): int(v) for k, v in df["outcome_bucket"].value_counts().to_dict().items()},
        "summary_records": summary[ordered].to_dict(orient="records"),
        "methodology_note": "This summarizes existing observe-only new-method candidates by descriptive setup type and realized forward_return when CLOSED.",
        "limitations": [
            "This uses only rows already present in new_method_candidates_history.csv.",
            "Pending rows are not treated as wins or losses.",
            "No intraday execution, slippage, or order fill model is applied.",
            "Setup type cutoffs are descriptive audit grammar, not approved trading parameters.",
        ],
        "operational_change": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# New Method History Type Outcome Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- rows: {payload['rows']}",
        f"- unique_codes: {payload['unique_codes']}",
        f"- date_range: {payload['date_min']} to {payload['date_max']}",
        f"- forward_return_status_counts: {payload['forward_return_status_counts']}",
        "",
        "## Setup type counts",
    ]
    for key, value in payload["setup_type_counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Branch/type outcome summary"])
    branch_type = summary[summary["summary_level"].eq("branch_setup_type")].copy()
    for _, row in branch_type.iterrows():
        avg = row["avg_forward_return"]
        win = row["win_rate"]
        avg_s = "-" if pd.isna(avg) else f"{float(avg):.4f}"
        win_s = "-" if pd.isna(win) else f"{float(win):.3f}"
        lines.append(
            f"- {row['regime']} / {row['method_branch']} / {row['setup_type']}: "
            f"rows={int(row['rows'])}, closed={int(row['closed_rows'])}, pending={int(row['pending_rows'])}, "
            f"win_rate={win_s}, avg_ret={avg_s}, layer={row['evidence_layer']}"
        )
    lines.extend(["", "## Caveats"])
    for item in payload["limitations"]:
        lines.append(f"- {item}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "detail": str(OUT_DETAIL_CSV), "summary": str(OUT_SUMMARY_CSV), "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
