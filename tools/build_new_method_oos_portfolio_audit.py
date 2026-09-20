"""Read-only OOS concentration and cost-sensitivity audit for new-method candidates."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_DETAIL = LOG_DIR / "new_method_representative_oos_candidates_latest.csv"
INPUT_SUMMARY = LOG_DIR / "new_method_representative_oos_candidate_summary_latest.csv"
OUT_DAILY = LOG_DIR / "new_method_oos_portfolio_daily_latest.csv"
OUT_SUMMARY = LOG_DIR / "new_method_oos_portfolio_audit_summary_latest.csv"
OUT_JSON = LOG_DIR / "new_method_oos_portfolio_audit_latest.json"
OUT_MD = LOG_DIR / "new_method_oos_portfolio_audit_latest.md"

# Sensitivity only: this is not an operating cost policy or approval threshold.
ROUND_TRIP_COST_BPS = (10, 25, 50)
REQUIRED_COLUMNS = {
    "date", "code", "research_hypothesis_id", "research_regime", "horizon",
    "path_status", "path_forward_return", "oos_excess_return",
}


def _safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _as_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def main() -> int:
    if not INPUT_DETAIL.exists() or not INPUT_SUMMARY.exists():
        raise SystemExit("missing OOS representative candidate outputs")
    detail = pd.read_csv(INPUT_DETAIL, encoding="utf-8-sig")
    missing = sorted(REQUIRED_COLUMNS.difference(detail.columns))
    if missing:
        raise SystemExit(f"OOS candidate detail missing required columns: {missing}")
    source_summary = pd.read_csv(INPUT_SUMMARY, encoding="utf-8-sig")
    detail["date"] = pd.to_datetime(detail["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    detail["path_forward_return"] = _as_float(detail["path_forward_return"])
    detail["oos_excess_return"] = _as_float(detail["oos_excess_return"])
    closed = detail[
        detail["path_status"].eq("CLOSED_EXACT")
        & detail["path_forward_return"].notna()
        & detail["oos_excess_return"].notna()
    ].copy()
    if closed.duplicated(["date", "code", "research_hypothesis_id"]).any():
        raise SystemExit("duplicate closed candidate keys")

    daily_rows: list[dict[str, object]] = []
    for key, group in closed.groupby(
        ["research_hypothesis_id", "research_regime", "horizon", "date"], dropna=False
    ):
        absolute = group["oos_excess_return"].abs()
        total_absolute = float(absolute.sum())
        sorted_absolute = absolute.sort_values(ascending=False)
        shares = absolute / total_absolute if total_absolute > 0 else pd.Series(dtype=float)
        record: dict[str, object] = {
            "research_hypothesis_id": key[0],
            "research_regime": key[1],
            "horizon": key[2],
            "date": key[3],
            "closed_candidate_rows": int(len(group)),
            "unique_codes": int(group["code"].nunique()),
            "daily_equal_weight_raw_return": float(group["path_forward_return"].mean()),
            "daily_equal_weight_excess_return": float(group["oos_excess_return"].mean()),
            "daily_median_excess_return": float(group["oos_excess_return"].median()),
            "positive_excess_share": float((group["oos_excess_return"] > 0).mean()),
            "largest_abs_excess_share": float(sorted_absolute.iloc[0] / total_absolute) if total_absolute > 0 else 0.0,
            "top10_abs_excess_share": float(sorted_absolute.head(10).sum() / total_absolute) if total_absolute > 0 else 0.0,
            "abs_excess_hhi": float((shares ** 2).sum()) if total_absolute > 0 else 0.0,
        }
        for bps in ROUND_TRIP_COST_BPS:
            cost = bps / 10_000.0
            record[f"daily_net_excess_after_{bps}bps"] = record["daily_equal_weight_excess_return"] - cost
        daily_rows.append(record)
    daily = pd.DataFrame(daily_rows)
    if not daily.empty:
        daily = daily.sort_values(["research_hypothesis_id", "date"]).reset_index(drop=True)

    summary_rows: list[dict[str, object]] = []
    for key, group in daily.groupby(["research_hypothesis_id", "research_regime", "horizon"], dropna=False):
        record = {
            "research_hypothesis_id": key[0],
            "research_regime": key[1],
            "horizon": key[2],
            "closed_signal_dates": int(group["date"].nunique()),
            "closed_candidate_rows": int(group["closed_candidate_rows"].sum()),
            "avg_daily_equal_weight_raw_return": float(group["daily_equal_weight_raw_return"].mean()),
            "avg_daily_equal_weight_excess_return": float(group["daily_equal_weight_excess_return"].mean()),
            "median_daily_equal_weight_excess_return": float(group["daily_equal_weight_excess_return"].median()),
            "positive_excess_days": int((group["daily_equal_weight_excess_return"] > 0).sum()),
            "avg_positive_excess_share": float(group["positive_excess_share"].mean()),
            "max_largest_abs_excess_share": float(group["largest_abs_excess_share"].max()),
            "avg_top10_abs_excess_share": float(group["top10_abs_excess_share"].mean()),
            "max_abs_excess_hhi": float(group["abs_excess_hhi"].max()),
            "assessment": "INSUFFICIENT_DATES" if group["date"].nunique() < 5 else "DESCRIPTIVE_ONLY",
        }
        for bps in ROUND_TRIP_COST_BPS:
            record[f"avg_daily_net_excess_after_{bps}bps"] = float(group[f"daily_net_excess_after_{bps}bps"].mean())
        summary_rows.append(record)
    summary = pd.DataFrame(summary_rows)
    if not summary.empty:
        summary = summary.sort_values("research_hypothesis_id").reset_index(drop=True)

    source_closed = int(pd.to_numeric(source_summary.get("closed_rows"), errors="coerce").fillna(0).sum())
    if source_closed != int(len(closed)):
        raise SystemExit(f"closed-row mismatch: source={source_closed}, audit={len(closed)}")
    if not daily.empty and int(daily["closed_candidate_rows"].sum()) != int(len(closed)):
        raise SystemExit("daily portfolio rows do not reconcile to closed candidate rows")

    daily.to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_new_method_oos_portfolio_audit",
        "input_detail": str(INPUT_DETAIL),
        "input_summary": str(INPUT_SUMMARY),
        "method": {
            "portfolio": "same-signal-date equal-weight closed candidates within each representative hypothesis",
            "performance": "candidate path return minus same-date/same-research-regime/same-horizon universe baseline",
            "concentration": "absolute excess-return contribution; not portfolio holdings or realized allocation",
            "cost_sensitivity": {"round_trip_bps": list(ROUND_TRIP_COST_BPS), "policy_change": False},
        },
        "row_counts": {
            "input_candidate_rows": int(len(detail)),
            "closed_candidate_rows": int(len(closed)),
            "daily_portfolio_rows": int(len(daily)),
            "hypothesis_rows": int(len(summary)),
        },
        "summary": _safe_records(summary),
        "limitations": [
            "This is a sensitivity calculation, not an approved transaction-cost assumption or operating threshold.",
            "The cost haircut is applied once per completed candidate path and does not model fill quality, liquidity capacity, or portfolio overlap.",
            "Small numbers of signal dates remain descriptive only and cannot support an operating decision.",
        ],
        "operational_change": False,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    lines = [
        "# New Method OOS Portfolio Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- input_candidate_rows: {payload['row_counts']['input_candidate_rows']}",
        f"- closed_candidate_rows: {payload['row_counts']['closed_candidate_rows']}",
        f"- daily_portfolio_rows: {payload['row_counts']['daily_portfolio_rows']}",
        "- scope: read-only; existing operating logic fields excluded upstream",
        "",
        "## Hypothesis summary",
    ]
    if summary.empty:
        lines.append("- no closed OOS candidate paths")
    else:
        for _, row in summary.iterrows():
            lines.append(
                f"- {row['research_hypothesis_id']}: dates={int(row['closed_signal_dates'])}, "
                f"avg_daily_excess={float(row['avg_daily_equal_weight_excess_return']):.6f}, "
                f"net_10bps={float(row['avg_daily_net_excess_after_10bps']):.6f}, "
                f"net_25bps={float(row['avg_daily_net_excess_after_25bps']):.6f}, "
                f"net_50bps={float(row['avg_daily_net_excess_after_50bps']):.6f}, "
                f"max_top1_abs_share={float(row['max_largest_abs_excess_share']):.4f}, "
                f"assessment={row['assessment']}"
            )
    lines.extend(["", "## Caveats", *[f"- {item}" for item in payload["limitations"]]])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "summary": str(OUT_SUMMARY), "daily": str(OUT_DAILY), "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
