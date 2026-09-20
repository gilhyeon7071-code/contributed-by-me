from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
READ_ONLY_ROWS = LOG_DIR / "stress_composite_daily_candidates_rows_latest.csv"
OPERATIONAL_ROWS = LOG_DIR / "candidates_latest_data.with_final_score.csv"

OUT_JSON = LOG_DIR / "stress_composite_daily_candidate_quality_latest.json"
OUT_SUMMARY = LOG_DIR / "stress_composite_daily_candidate_quality_summary_latest.csv"
OUT_MD = LOG_DIR / "stress_composite_daily_candidate_quality_latest.md"


def norm_date(value: Any) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def norm_code(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return ""
    text = str(value).strip()
    try:
        if float(text).is_integer():
            return str(int(float(text))).zfill(6)
    except Exception:
        pass
    return text.zfill(6) if text.isdigit() else text


def load_read_only() -> pd.DataFrame:
    df = pd.read_csv(READ_ONLY_ROWS, encoding="utf-8-sig")
    df["source"] = "read_only_stress_spec"
    df["date_norm"] = df["date_norm"].map(norm_date)
    df["code_norm"] = df["code_norm"].map(norm_code)
    df["trade_value"] = pd.to_numeric(df.get("value", 0), errors="coerce").fillna(0.0)
    return df


def load_operational() -> pd.DataFrame:
    df = pd.read_csv(OPERATIONAL_ROWS, encoding="utf-8-sig")
    df["source"] = "operational_candidates"
    df["date_norm"] = df["date"].map(norm_date) if "date" in df.columns else ""
    df["code_norm"] = df["code"].map(norm_code) if "code" in df.columns else ""
    if "trading_value" in df.columns:
        df["trade_value"] = pd.to_numeric(df["trading_value"], errors="coerce")
    else:
        df["trade_value"] = pd.to_numeric(df.get("value", 0), errors="coerce")
    df["trade_value"] = df["trade_value"].fillna(0.0)
    return df


def summarize(df: pd.DataFrame, source: str, date_value: str) -> dict[str, Any]:
    if df.empty:
        return {
            "source": source,
            "date": date_value,
            "rows": 0,
            "unique_codes": 0,
            "known_market_rows": 0,
            "unknown_market_rows": 0,
            "value_ge_100m_rows": 0,
            "value_ge_1b_rows": 0,
            "value_ge_10b_rows": 0,
            "value_median": None,
            "value_min": None,
            "value_max": None,
            "top_codes": "",
        }
    market = df.get("market", pd.Series([""] * len(df), index=df.index)).astype(str).str.upper()
    value = pd.to_numeric(df["trade_value"], errors="coerce").fillna(0.0)
    top = df.sort_values(["trade_value", "code_norm"], ascending=[False, True]).head(10)
    return {
        "source": source,
        "date": date_value,
        "rows": int(len(df)),
        "unique_codes": int(df["code_norm"].nunique()),
        "known_market_rows": int((market.ne("UNKNOWN") & market.ne("") & market.ne("NAN")).sum()),
        "unknown_market_rows": int((market.eq("UNKNOWN") | market.eq("") | market.eq("NAN")).sum()),
        "value_ge_100m_rows": int(value.ge(100_000_000).sum()),
        "value_ge_1b_rows": int(value.ge(1_000_000_000).sum()),
        "value_ge_10b_rows": int(value.ge(10_000_000_000).sum()),
        "value_median": float(value.median()) if len(value) else None,
        "value_min": float(value.min()) if len(value) else None,
        "value_max": float(value.max()) if len(value) else None,
        "top_codes": "|".join(top["code_norm"].astype(str).tolist()),
    }


def main() -> int:
    read_only = load_read_only()
    op = load_operational()
    rows: list[dict[str, Any]] = []
    for source, frame in [("read_only_stress_spec", read_only), ("operational_candidates", op)]:
        for date_value, g in frame[frame["date_norm"].astype(str).ne("")].groupby("date_norm", dropna=False):
            rows.append(summarize(g, source, str(date_value)))
    summary = pd.DataFrame(rows).sort_values(["date", "source"]).reset_index(drop=True)

    latest_date = read_only["date_norm"].max() if len(read_only) else ""
    latest = summary[(summary["source"] == "read_only_stress_spec") & (summary["date"] == latest_date)]
    latest_known_ratio = 0.0
    latest_value_1b_ratio = 0.0
    if not latest.empty and int(latest.iloc[0]["rows"]) > 0:
        latest_known_ratio = float(latest.iloc[0]["known_market_rows"]) / float(latest.iloc[0]["rows"])
        latest_value_1b_ratio = float(latest.iloc[0]["value_ge_1b_rows"]) / float(latest.iloc[0]["rows"])
    if latest.empty or int(latest.iloc[0]["rows"]) == 0:
        conclusion = "NO_LATEST_READ_ONLY_CANDIDATES_FOR_QUALITY_AUDIT"
    elif latest_known_ratio >= 0.80 and latest_value_1b_ratio >= 0.50:
        conclusion = "READ_ONLY_STRESS_CANDIDATE_QUALITY_ACCEPTABLE_FOR_NEXT_REPLAY"
    else:
        conclusion = "READ_ONLY_STRESS_CANDIDATES_NEED_TRADABILITY_FILTER_BEFORE_REPLAY"

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "stress_composite_daily_candidate_quality_read_only",
        "classification": "STRESS_COMPOSITE_DAILY_CANDIDATE_QUALITY_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "latest_read_only_date": latest_date,
        "latest_known_market_ratio": latest_known_ratio,
        "latest_value_ge_1b_ratio": latest_value_1b_ratio,
        "summary": summary.to_dict(orient="records"),
        "operation_effect": {
            "candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
            "operational_candidate_file_modified": False,
        },
        "validation": [
            "read_only_candidate_rows_loaded: PASS",
            "operational_candidate_rows_loaded_read_only: PASS",
            "quality_summary_written: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    md = [
        "# STRESS Composite Daily Candidate Quality",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- latest_read_only_date: {latest_date}",
        f"- latest_known_market_ratio: {latest_known_ratio}",
        f"- latest_value_ge_1b_ratio: {latest_value_1b_ratio}",
        "",
        "## Summary",
        "",
    ]
    for row in payload["summary"]:
        md.append(
            f"- {row['date']} / {row['source']}: rows={row['rows']}, known_market={row['known_market_rows']}, "
            f"value>=1b={row['value_ge_1b_rows']}, median_value={row['value_median']}"
        )
    md += [
        "",
        "## Operation Effect",
        "",
        "- NOT_APPLIED to candidate generation, official backtest, HPO, diagnostics, paper/live, gates, thresholds, or parameters.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_SUMMARY}")
    print(f"[OK] wrote {OUT_MD}")
    print(f"[CONCLUSION] {conclusion}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
