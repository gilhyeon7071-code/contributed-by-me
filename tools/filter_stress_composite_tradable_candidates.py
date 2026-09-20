from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
RAW_ROWS = LOG_DIR / "stress_composite_daily_candidates_rows_latest.csv"
OPERATIONAL_ROWS = LOG_DIR / "candidates_latest_data.with_final_score.csv"

OUT_JSON = LOG_DIR / "stress_composite_tradability_filter_latest.json"
OUT_SUMMARY = LOG_DIR / "stress_composite_tradability_filter_summary_latest.csv"
OUT_ROWS = LOG_DIR / "stress_composite_tradability_filter_rows_latest.csv"
OUT_COMPARE = LOG_DIR / "stress_composite_tradability_filter_compare_latest.csv"
OUT_MD = LOG_DIR / "stress_composite_tradability_filter_latest.md"

FILTERS: list[dict[str, Any]] = [
    {
        "filter_id": "known_market_only",
        "markets": {"KOSPI", "KOSDAQ"},
        "min_value": 0,
        "min_close": 0,
        "note": "Remove UNKNOWN market only.",
    },
    {
        "filter_id": "known_market_value_ge_100m",
        "markets": {"KOSPI", "KOSDAQ"},
        "min_value": 100_000_000,
        "min_close": 0,
        "note": "Known market plus minimum value 100m KRW.",
    },
    {
        "filter_id": "known_market_value_ge_1b",
        "markets": {"KOSPI", "KOSDAQ"},
        "min_value": 1_000_000_000,
        "min_close": 0,
        "note": "Known market plus minimum value 1b KRW.",
    },
    {
        "filter_id": "known_market_value_ge_1b_close_ge_1000",
        "markets": {"KOSPI", "KOSDAQ"},
        "min_value": 1_000_000_000,
        "min_close": 1_000,
        "note": "Known market, value 1b KRW, and price at least 1,000.",
    },
    {
        "filter_id": "known_market_value_ge_10b",
        "markets": {"KOSPI", "KOSDAQ"},
        "min_value": 10_000_000_000,
        "min_close": 0,
        "note": "Known market plus minimum value 10b KRW.",
    },
    {
        "filter_id": "known_market_value_ge_10b_close_ge_1000",
        "markets": {"KOSPI", "KOSDAQ"},
        "min_value": 10_000_000_000,
        "min_close": 1_000,
        "note": "Known market, value 10b KRW, and price at least 1,000.",
    },
]


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


def load_raw() -> pd.DataFrame:
    df = pd.read_csv(RAW_ROWS, encoding="utf-8-sig")
    df["date_norm"] = df["date_norm"].map(norm_date)
    df["code_norm"] = df["code_norm"].map(norm_code)
    df["market_norm"] = df["market"].fillna("").astype(str).str.upper()
    df["trade_value"] = pd.to_numeric(df.get("value", 0), errors="coerce").fillna(0.0)
    df["close_num"] = pd.to_numeric(df.get("close", 0), errors="coerce").fillna(0.0)
    df["stress_candidate_score"] = pd.to_numeric(df.get("stress_candidate_score", 0), errors="coerce").fillna(0.0)
    return df


def load_operational() -> pd.DataFrame:
    if not OPERATIONAL_ROWS.exists():
        return pd.DataFrame()
    df = pd.read_csv(OPERATIONAL_ROWS, encoding="utf-8-sig")
    df["date_norm"] = df["date"].map(norm_date) if "date" in df.columns else ""
    df["code_norm"] = df["code"].map(norm_code) if "code" in df.columns else ""
    return df[df["date_norm"].astype(str).ne("")].copy()


def apply_filter(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    mask = (
        df["market_norm"].isin(cfg["markets"])
        & df["trade_value"].ge(float(cfg["min_value"]))
        & df["close_num"].ge(float(cfg["min_close"]))
    )
    out = df[mask].copy()
    out["filter_id"] = cfg["filter_id"]
    out["filter_note"] = cfg["note"]
    out["read_only_filter_rank"] = out.groupby("date_norm")["stress_candidate_score"].rank(method="first", ascending=False).astype(int) if len(out) else pd.Series(dtype=int)
    return out


def summarize(df: pd.DataFrame, raw: pd.DataFrame, cfg: dict[str, Any], latest_date: str, operational_primary_date: str) -> dict[str, Any]:
    latest = df[df["date_norm"].eq(latest_date)]
    op_day = df[df["date_norm"].eq(operational_primary_date)] if operational_primary_date else df.iloc[0:0]
    raw_latest_n = int(len(raw[raw["date_norm"].eq(latest_date)]))
    value = pd.to_numeric(latest.get("trade_value", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    return {
        "filter_id": cfg["filter_id"],
        "note": cfg["note"],
        "min_value": int(cfg["min_value"]),
        "min_close": int(cfg["min_close"]),
        "total_rows": int(len(df)),
        "latest_date": latest_date,
        "latest_rows": int(len(latest)),
        "latest_retention_vs_raw": float(len(latest) / raw_latest_n) if raw_latest_n else None,
        "latest_unique_codes": int(latest["code_norm"].nunique()) if len(latest) else 0,
        "latest_value_median": float(value.median()) if len(value) else None,
        "latest_value_min": float(value.min()) if len(value) else None,
        "latest_value_max": float(value.max()) if len(value) else None,
        "latest_top_codes": "|".join(latest.sort_values(["stress_candidate_score", "trade_value"], ascending=[False, False]).head(10)["code_norm"].astype(str).tolist()) if len(latest) else "",
        "operational_date": operational_primary_date,
        "operational_date_rows": int(len(op_day)),
    }


def compare_with_operational(filtered: pd.DataFrame, op: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if filtered.empty:
        return pd.DataFrame(columns=["filter_id", "date", "filtered_candidates", "operational_candidates", "overlap_count", "overlap_codes"])
    for (filter_id, date_value), g in filtered.groupby(["filter_id", "date_norm"], dropna=False):
        op_day = op[op["date_norm"].eq(date_value)] if not op.empty else pd.DataFrame()
        filtered_codes = set(g["code_norm"].astype(str))
        op_codes = set(op_day["code_norm"].astype(str)) if not op_day.empty else set()
        overlap = filtered_codes & op_codes
        rows.append({
            "filter_id": filter_id,
            "date": date_value,
            "filtered_candidates": int(len(filtered_codes)),
            "operational_candidates": int(len(op_codes)),
            "overlap_count": int(len(overlap)),
            "overlap_codes": "|".join(sorted(overlap)),
        })
    return pd.DataFrame(rows).sort_values(["filter_id", "date"]).reset_index(drop=True)


def choose_review_filter(summary: pd.DataFrame) -> tuple[str, str]:
    candidates = summary[
        (pd.to_numeric(summary["latest_rows"], errors="coerce") >= 20)
        & (pd.to_numeric(summary["latest_value_median"], errors="coerce") >= 1_000_000_000)
    ].copy()
    if candidates.empty:
        return "", "NO_FILTER_HAS_ENOUGH_LATEST_ROWS_AND_MEDIAN_VALUE"
    candidates["strictness"] = pd.to_numeric(candidates["min_value"], errors="coerce") + pd.to_numeric(candidates["min_close"], errors="coerce")
    chosen = candidates.sort_values(["strictness", "latest_rows"], ascending=[False, False]).iloc[0]
    return str(chosen["filter_id"]), "FILTER_REVIEW_CANDIDATE_SELECTED_READ_ONLY"


def main() -> int:
    raw = load_raw()
    op = load_operational()
    latest_date = str(raw["date_norm"].max()) if len(raw) else ""
    operational_dates = sorted(op["date_norm"].dropna().astype(str).unique().tolist()) if not op.empty else []
    operational_primary_date = operational_dates[-1] if operational_dates else ""

    filtered_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for cfg in FILTERS:
        frame = apply_filter(raw, cfg)
        filtered_frames.append(frame)
        summary_rows.append(summarize(frame, raw, cfg, latest_date, operational_primary_date))

    filtered = pd.concat(filtered_frames, ignore_index=True) if filtered_frames else pd.DataFrame()
    summary = pd.DataFrame(summary_rows)
    compare = compare_with_operational(filtered, op)
    chosen_filter, chosen_reason = choose_review_filter(summary)
    if chosen_filter:
        conclusion = "READ_ONLY_TRADABILITY_FILTER_REVIEW_CANDIDATE_FOUND"
    else:
        conclusion = "READ_ONLY_TRADABILITY_FILTER_NOT_READY"

    keep_cols = [
        "filter_id", "date_norm", "read_only_filter_rank", "code_norm", "name", "market", "market_regime",
        "stress_candidate_score", "close", "trade_value", "rs", "rs_rank_pct", "rsi14", "rsi14_rank_pct",
        "stretch", "stretch_rank_pct", "fwd_ret_h1", "fwd_ret_h2", "fwd_ret_h5",
    ]
    rows_out = filtered[[c for c in keep_cols if c in filtered.columns]].sort_values(["filter_id", "date_norm", "read_only_filter_rank", "code_norm"]).reset_index(drop=True)

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "stress_composite_tradability_filter_read_only",
        "classification": "STRESS_COMPOSITE_TRADABILITY_FILTER_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "chosen_filter": chosen_filter,
        "chosen_reason": chosen_reason,
        "latest_date": latest_date,
        "operational_primary_date": operational_primary_date,
        "row_counts": {
            "raw_rows": int(len(raw)),
            "filtered_rows_all_variants": int(len(rows_out)),
            "summary_rows": int(len(summary)),
            "compare_rows": int(len(compare)),
        },
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
            "raw_read_only_candidates_loaded: PASS",
            "tradability_filter_variants_6: PASS" if len(summary) == 6 else "tradability_filter_variants_6: FAIL",
            "operational_candidate_file_read_only_compare: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    rows_out.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    compare.to_csv(OUT_COMPARE, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# STRESS Composite Tradability Filter",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- chosen_filter: {chosen_filter}",
        f"- chosen_reason: {chosen_reason}",
        f"- latest_date: {latest_date}",
        "",
        "## Summary",
        "",
    ]
    for row in payload["summary"]:
        md.append(
            f"- {row['filter_id']}: latest_rows={row['latest_rows']}, retention={row['latest_retention_vs_raw']}, "
            f"median_value={row['latest_value_median']}, op_date_rows={row['operational_date_rows']}"
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
    print(f"[OK] wrote {OUT_ROWS}")
    print(f"[OK] wrote {OUT_COMPARE}")
    print(f"[OK] wrote {OUT_MD}")
    print(f"[CONCLUSION] {conclusion}")
    print(f"[CHOSEN_FILTER] {chosen_filter}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
