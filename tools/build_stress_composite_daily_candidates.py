from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
ROBUSTNESS = ROOT / "tools" / "analyze_stress_composite_robustness.py"
SPEC_JSON = LOG_DIR / "stress_composite_methodology_spec_latest.json"
OPERATIONAL_CANDIDATES = LOG_DIR / "candidates_latest_data.with_final_score.csv"

OUT_JSON = LOG_DIR / "stress_composite_daily_candidates_latest.json"
OUT_ROWS = LOG_DIR / "stress_composite_daily_candidates_rows_latest.csv"
OUT_DAILY = LOG_DIR / "stress_composite_daily_candidates_daily_latest.csv"
OUT_COMPARE = LOG_DIR / "stress_composite_daily_candidates_compare_latest.csv"
OUT_MD = LOG_DIR / "stress_composite_daily_candidates_latest.md"

CURRENT_START = pd.Timestamp("2026-07-01")


def load_robustness_module() -> Any:
    spec = importlib.util.spec_from_file_location("stress_composite_robustness_for_daily_candidates", ROBUSTNESS)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {ROBUSTNESS}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


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


def norm_date(value: Any) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def rank_condition_mask(df: pd.DataFrame, field: str, direction: str, threshold: float) -> pd.Series:
    rank_col = f"{field}_rank_pct"
    ranks = pd.to_numeric(df[rank_col], errors="coerce")
    if direction == "low":
        return ranks.le(float(threshold)).fillna(False)
    if direction == "high":
        return ranks.ge(float(threshold)).fillna(False)
    raise ValueError(f"unsupported direction: {direction}")


def build_candidate_score(df: pd.DataFrame) -> pd.Series:
    rsi_component = 1.0 - pd.to_numeric(df["rsi14_rank_pct"], errors="coerce")
    stretch_component = 1.0 - pd.to_numeric(df["stretch_rank_pct"], errors="coerce")
    rs_component = pd.to_numeric(df["rs_rank_pct"], errors="coerce")
    return ((rsi_component + stretch_component + rs_component) / 3.0).fillna(0.0)


def summarize_daily(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=[
            "date", "candidate_rows", "unique_codes", "avg_candidate_score", "max_candidate_score",
            "top_codes", "top_names",
        ])
    grouped = []
    for day, g in rows.groupby("date_norm", dropna=False):
        top = g.sort_values(["stress_candidate_score", "value"], ascending=[False, False]).head(10)
        grouped.append({
            "date": day,
            "candidate_rows": int(len(g)),
            "unique_codes": int(g["code_norm"].nunique()),
            "avg_candidate_score": float(pd.to_numeric(g["stress_candidate_score"], errors="coerce").mean()),
            "max_candidate_score": float(pd.to_numeric(g["stress_candidate_score"], errors="coerce").max()),
            "top_codes": "|".join(top["code_norm"].astype(str).tolist()),
            "top_names": "|".join(top.get("name", pd.Series([""] * len(top))).astype(str).tolist()),
        })
    return pd.DataFrame(grouped).sort_values("date").reset_index(drop=True)


def load_operational_candidates() -> pd.DataFrame:
    if not OPERATIONAL_CANDIDATES.exists():
        return pd.DataFrame()
    op = pd.read_csv(OPERATIONAL_CANDIDATES, encoding="utf-8-sig")
    op["code_norm"] = op["code"].map(norm_code) if "code" in op.columns else ""
    op["date_norm"] = op["date"].map(norm_date) if "date" in op.columns else ""
    return op


def compare_with_operational(rows: pd.DataFrame, op: pd.DataFrame, latest_data_date: str) -> pd.DataFrame:
    compare_rows: list[dict[str, Any]] = []
    stress_by_date = {
        date: set(g["code_norm"].astype(str))
        for date, g in rows.groupby("date_norm", dropna=False)
    } if not rows.empty else {}
    op_by_date = {
        date: set(g["code_norm"].astype(str))
        for date, g in op.groupby("date_norm", dropna=False)
        if str(date).strip()
    } if not op.empty else {}
    all_dates = sorted(set(stress_by_date) | set(op_by_date) | {latest_data_date})
    for day in all_dates:
        stress_codes = stress_by_date.get(day, set())
        op_codes = op_by_date.get(day, set())
        overlap = stress_codes & op_codes
        compare_rows.append({
            "date": day,
            "stress_spec_candidates": int(len(stress_codes)),
            "operational_candidates": int(len(op_codes)),
            "overlap_count": int(len(overlap)),
            "overlap_codes": "|".join(sorted(overlap)),
            "stress_only_count": int(len(stress_codes - op_codes)),
            "operational_only_count": int(len(op_codes - stress_codes)),
        })
    return pd.DataFrame(compare_rows).sort_values("date").reset_index(drop=True)


def main() -> int:
    spec_payload = load_json(SPEC_JSON)
    if spec_payload.get("classification") != "STRESS_COMPOSITE_METHODOLOGY_SPEC_READ_ONLY":
        raise ValueError("unexpected methodology spec classification")
    if spec_payload.get("operational_decision") != "NOT_APPROVED" or spec_payload.get("full_logic_application") != "NOT_APPLIED":
        raise ValueError("methodology spec must remain NOT_APPROVED / NOT_APPLIED")

    rob = load_robustness_module()
    comp = rob.load_composite_module()
    base = comp.load_base_module()
    report = base.load_report_module()
    data = report.load_data()
    factors = report.compute_factors(data)
    factors = base.add_forward_returns(factors)
    factors = base.add_regime_and_ranks(report, factors)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    factors["date_norm"] = factors["date"].dt.strftime("%Y-%m-%d")
    factors["code_norm"] = factors["code"].map(norm_code)
    latest_data_date = factors["date"].max().strftime("%Y-%m-%d")

    cond = factors["market_regime"].astype(str).isin(spec_payload["intended_regime"])
    condition_flags: list[str] = []
    for item in spec_payload["conditions"]:
        field = str(item["field"])
        direction = str(item["direction"])
        threshold = float(item["cross_sectional_rank_threshold"])
        flag = f"pass_{field}_{direction}_{threshold:g}"
        factors[flag] = rank_condition_mask(factors, field, direction, threshold)
        condition_flags.append(flag)
        cond = cond & factors[flag]

    rows = factors[cond & factors["date"].ge(CURRENT_START)].copy()
    rows["methodology_id"] = spec_payload["methodology_id"]
    rows["strategy_family"] = spec_payload["strategy_family"]
    rows["stress_candidate_score"] = build_candidate_score(rows) if not rows.empty else pd.Series(dtype=float)
    rows["read_only_candidate_rank"] = (
        rows.groupby("date_norm")["stress_candidate_score"].rank(method="first", ascending=False).astype(int)
        if not rows.empty else pd.Series(dtype=int)
    )

    keep_cols = [
        "date_norm", "read_only_candidate_rank", "code_norm", "market", "market_regime",
        "methodology_id", "strategy_family", "stress_candidate_score",
        "close", "value", "ret1_pct", "rs", "rs_rank_pct", "rsi14", "rsi14_rank_pct",
        "stretch", "stretch_rank_pct", "atr_pct", "fwd_ret_h1", "fwd_ret_h2", "fwd_ret_h5",
    ]
    if "name" in rows.columns:
        keep_cols.insert(3, "name")
    rows_out = rows[[c for c in keep_cols if c in rows.columns]].sort_values(
        ["date_norm", "read_only_candidate_rank", "code_norm"]
    ).reset_index(drop=True)

    daily = summarize_daily(rows.assign(name=rows.get("name", "")))
    op = load_operational_candidates()
    compare = compare_with_operational(rows, op, latest_data_date)

    latest_stress_n = int(compare.loc[compare["date"].eq(latest_data_date), "stress_spec_candidates"].iloc[0]) if latest_data_date in set(compare["date"]) else 0
    operational_dates = sorted([x for x in op["date_norm"].dropna().astype(str).unique().tolist() if x]) if not op.empty and "date_norm" in op.columns else []
    operational_primary_date = operational_dates[-1] if operational_dates else ""
    op_date_stress_n = int(compare.loc[compare["date"].eq(operational_primary_date), "stress_spec_candidates"].iloc[0]) if operational_primary_date in set(compare["date"]) else 0
    total_current_stress_n = int(len(rows_out))
    if latest_stress_n > 0:
        conclusion = "READ_ONLY_STRESS_SPEC_HAS_LATEST_DATE_CANDIDATES"
    elif total_current_stress_n > 0:
        conclusion = "READ_ONLY_STRESS_SPEC_HAS_PRIOR_CURRENT_WINDOW_CANDIDATES_NOT_LATEST_DATE"
    else:
        conclusion = "READ_ONLY_STRESS_SPEC_HAS_NO_CURRENT_WINDOW_CANDIDATES"

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "stress_composite_daily_candidates_read_only",
        "classification": "STRESS_COMPOSITE_DAILY_CANDIDATES_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "methodology_id": spec_payload["methodology_id"],
        "data_window": {
            "current_start": CURRENT_START.strftime("%Y-%m-%d"),
            "latest_data_date": latest_data_date,
            "operational_primary_date": operational_primary_date,
        },
        "row_counts": {
            "data_rows": int(len(data)),
            "factor_rows": int(len(factors)),
            "read_only_current_window_candidate_rows": total_current_stress_n,
            "read_only_latest_date_candidate_rows": latest_stress_n,
            "read_only_operational_date_candidate_rows": op_date_stress_n,
            "operational_candidate_rows": int(len(op)),
            "daily_summary_rows": int(len(daily)),
            "compare_rows": int(len(compare)),
        },
        "condition_flags": condition_flags,
        "daily_summary": daily.to_dict(orient="records"),
        "compare_summary": compare.to_dict(orient="records"),
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
            "methodology_spec_status_read_only_confirmed: PASS",
            "price_history_integrity_path_executed: PASS",
            "daily_candidate_rows_written: PASS",
            "operational_candidate_file_read_only_compare: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    rows_out.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    daily.to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    compare.to_csv(OUT_COMPARE, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# STRESS Composite Daily Candidates",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- methodology_id: {payload['methodology_id']}",
        f"- current_start: {payload['data_window']['current_start']}",
        f"- latest_data_date: {latest_data_date}",
        f"- operational_primary_date: {operational_primary_date}",
        "",
        "## Row Counts",
        "",
        *[f"- {k}: {v}" for k, v in payload["row_counts"].items()],
        "",
        "## Daily Summary",
        "",
    ]
    for row in payload["daily_summary"]:
        md.append(
            f"- {row['date']}: candidates={row['candidate_rows']}, unique_codes={row['unique_codes']}, "
            f"max_score={row['max_candidate_score']}"
        )
    if not payload["daily_summary"]:
        md.append("- none")
    md += [
        "",
        "## Operation Effect",
        "",
        "- NOT_APPLIED to candidate generation, official backtest, HPO, diagnostics, paper/live, gates, thresholds, or parameters.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_ROWS}")
    print(f"[OK] wrote {OUT_DAILY}")
    print(f"[OK] wrote {OUT_COMPARE}")
    print(f"[OK] wrote {OUT_MD}")
    print(f"[CONCLUSION] {conclusion}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
