from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"

OUT_JSON = LOG_DIR / "recent_regime_distribution_202606_202607_latest.json"
OUT_DAILY = LOG_DIR / "recent_regime_distribution_202606_202607_daily_latest.csv"
OUT_SUMMARY = LOG_DIR / "recent_regime_distribution_202606_202607_summary_latest.csv"
OUT_MD = LOG_DIR / "recent_regime_distribution_202606_202607_latest.md"

WINDOW_START = pd.Timestamp("2026-06-01")
WINDOW_END_REQUESTED = pd.Timestamp("2026-07-31")
CURRENT_START = pd.Timestamp("2026-07-01")


def load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_recent_regime_distribution", REPORT_BACKTEST)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {REPORT_BACKTEST}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def clean_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def main() -> int:
    report = load_report_module()
    data = report.load_data()
    factors = report.compute_factors(data)
    factors = factors.copy()
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    regime_map = report._assign_report_research_regime(factors)
    factors["market_regime"] = factors["date"].map(regime_map).fillna("UNKNOWN")

    data_min = factors["date"].min()
    data_max = factors["date"].max()
    window_end = min(WINDOW_END_REQUESTED, data_max)
    window = factors[(factors["date"].ge(WINDOW_START)) & (factors["date"].le(window_end))].copy()

    daily = (
        window.groupby(["date", "market_regime"], dropna=False)
        .agg(row_count=("code", "size"), unique_codes=("code", "nunique"))
        .reset_index()
        .sort_values(["date", "market_regime"])
    )
    daily["ym"] = daily["date"].dt.strftime("%Y-%m")
    daily["date"] = daily["date"].dt.strftime("%Y-%m-%d")

    daily_date_totals = daily.groupby("date", as_index=False)["row_count"].sum().rename(columns={"row_count": "date_row_total"})
    daily_with_share = daily.merge(daily_date_totals, on="date", how="left")
    daily_with_share["row_share"] = daily_with_share["row_count"] / daily_with_share["date_row_total"]

    date_primary = (
        daily_with_share.sort_values(["date", "row_count", "market_regime"], ascending=[True, False, True])
        .groupby("date", as_index=False)
        .first()[["date", "market_regime", "row_count", "unique_codes", "row_share"]]
        .rename(columns={
            "market_regime": "primary_regime",
            "row_count": "primary_row_count",
            "unique_codes": "primary_unique_codes",
            "row_share": "primary_row_share",
        })
    )

    summary = (
        daily_with_share.groupby(["ym", "market_regime"], dropna=False)
        .agg(
            active_dates=("date", "nunique"),
            row_count=("row_count", "sum"),
            avg_daily_row_share=("row_share", "mean"),
            unique_codes_sum=("unique_codes", "sum"),
        )
        .reset_index()
        .sort_values(["ym", "row_count", "market_regime"], ascending=[True, False, True])
    )

    primary_counts = (
        date_primary.groupby("primary_regime", as_index=False)
        .agg(primary_dates=("date", "nunique"))
        .sort_values(["primary_dates", "primary_regime"], ascending=[False, True])
    )

    current_window = window[window["date"].ge(CURRENT_START)]
    current_regime_counts = current_window["market_regime"].astype(str).value_counts().to_dict()
    current_bull_rows = int(current_regime_counts.get("BULL", 0))
    current_sideways_rows = int(current_regime_counts.get("SIDEWAYS", 0))
    if current_bull_rows == 0 and current_sideways_rows == 0:
        conclusion = "CURRENT_WINDOW_HAS_NO_BULL_OR_SIDEWAYS_ROWS"
    else:
        conclusion = "CURRENT_WINDOW_HAS_BULL_OR_SIDEWAYS_ROWS"

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "recent_regime_distribution_202606_202607_read_only",
        "classification": "RECENT_REGIME_DISTRIBUTION_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "data_window": {
            "min_date": data_min.strftime("%Y-%m-%d"),
            "max_date": data_max.strftime("%Y-%m-%d"),
            "audit_start": WINDOW_START.strftime("%Y-%m-%d"),
            "audit_end_requested": WINDOW_END_REQUESTED.strftime("%Y-%m-%d"),
            "audit_end_effective": window_end.strftime("%Y-%m-%d"),
            "current_start": CURRENT_START.strftime("%Y-%m-%d"),
        },
        "row_counts": {
            "data_rows": int(len(data)),
            "factor_rows": int(len(factors)),
            "audit_rows": int(len(window)),
            "audit_dates": int(window["date"].nunique()),
            "daily_rows": int(len(daily_with_share)),
            "summary_rows": int(len(summary)),
            "current_rows": int(len(current_window)),
            "current_bull_rows": current_bull_rows,
            "current_sideways_rows": current_sideways_rows,
        },
        "primary_regime_date_counts": clean_records(primary_counts),
        "monthly_regime_summary": clean_records(summary),
        "current_regime_row_counts": current_regime_counts,
        "operation_effect": {
            "candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
        },
        "validation": [
            "regime_assignment_loaded_from_report_backtest: PASS",
            "audit_window_non_empty: PASS" if len(window) else "audit_window_non_empty: FAIL",
            "daily_distribution_non_empty: PASS" if len(daily_with_share) else "daily_distribution_non_empty: FAIL",
            "operation_effect_no_changes: PASS",
        ],
    }
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    daily_with_share.to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# Recent Regime Distribution Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- data_window: {payload['data_window']['min_date']} ~ {payload['data_window']['max_date']}",
        f"- audit_window: {payload['data_window']['audit_start']} ~ {payload['data_window']['audit_end_effective']}",
        f"- current_start: {payload['data_window']['current_start']}",
        "",
        "## Row Counts",
        "",
        *[f"- {k}: {v}" for k, v in payload["row_counts"].items()],
        "",
        "## Primary Regime Date Counts",
        "",
    ]
    for row in payload["primary_regime_date_counts"]:
        md.append(f"- {row['primary_regime']}: {row['primary_dates']} dates")
    md += [
        "",
        "## Current Regime Row Counts",
        "",
    ]
    for regime, count in sorted(payload["current_regime_row_counts"].items(), key=lambda item: (-item[1], item[0])):
        md.append(f"- {regime}: {count}")
    md += [
        "",
        "## Operation Effect",
        "",
        "- NOT_APPLIED to candidate generation, official backtest, HPO, diagnostics, paper/live, gates, thresholds, or parameters.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_DAILY}")
    print(f"[OK] wrote {OUT_SUMMARY}")
    print(f"[OK] wrote {OUT_MD}")
    print(f"[CONCLUSION] {conclusion}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
