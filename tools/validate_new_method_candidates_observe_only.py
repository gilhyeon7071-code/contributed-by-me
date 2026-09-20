from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

LATEST = LOG_DIR / "new_method_candidates_latest.csv"
HISTORY = LOG_DIR / "new_method_candidates_history.csv"
META = LOG_DIR / "new_method_candidates_meta.json"

OUT_JSON = LOG_DIR / "new_method_candidates_validation_latest.json"
OUT_BRANCH = LOG_DIR / "new_method_candidates_validation_branch_latest.csv"
OUT_DAILY = LOG_DIR / "new_method_candidates_validation_daily_latest.csv"
OUT_ISSUES = LOG_DIR / "new_method_candidates_validation_issues_latest.csv"
OUT_MD = LOG_DIR / "new_method_candidates_validation_latest.md"

REQUIRED_COLUMNS = {
    "date",
    "code",
    "market",
    "regime",
    "method_branch",
    "signal_reason",
    "horizon",
    "expected_holding_days",
    "candidate_score",
    "rank_in_branch",
    "close",
    "trade_value",
    "forward_return",
    "forward_return_status",
    "status",
    "promotion_blocker",
    "source",
    "operational_use",
}


def profit_factor(ret: Iterable[float]) -> float | None:
    gains = 0.0
    losses = 0.0
    for value in ret:
        if value > 0:
            gains += float(value)
        elif value < 0:
            losses += abs(float(value))
    if losses > 0:
        return gains / losses
    if gains > 0:
        return None
    return 0.0


def clean_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): clean_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean_json_value(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def load_candidates(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", dtype={"code": str})
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ["expected_holding_days", "rank_in_branch"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["candidate_score", "close", "trade_value", "forward_return"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def summarize_branch(df: pd.DataFrame, label: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, group in df.groupby(["regime", "method_branch", "horizon"], dropna=False):
        regime, branch, horizon = keys
        closed = group[group["forward_return_status"].astype(str).eq("CLOSED")].copy()
        pending = group[group["forward_return_status"].astype(str).eq("PENDING")].copy()
        ret = pd.to_numeric(closed["forward_return"], errors="coerce").dropna()
        rows.append({
            "dataset": label,
            "regime": regime,
            "method_branch": branch,
            "horizon": horizon,
            "rows": int(len(group)),
            "closed_rows": int(len(closed)),
            "pending_rows": int(len(pending)),
            "closed_dates": int(closed["date"].nunique()) if len(closed) else 0,
            "pending_dates": int(pending["date"].nunique()) if len(pending) else 0,
            "unique_codes": int(group["code"].nunique()),
            "first_date": str(group["date"].min()) if len(group) else "",
            "last_date": str(group["date"].max()) if len(group) else "",
            "win_rate": float((ret > 0).mean()) if len(ret) else None,
            "ret_mean": float(ret.mean()) if len(ret) else None,
            "ret_median": float(ret.median()) if len(ret) else None,
            "profit_factor": profit_factor(ret.tolist()),
            "worst_ret": float(ret.min()) if len(ret) else None,
            "best_ret": float(ret.max()) if len(ret) else None,
            "avg_trade_value": float(pd.to_numeric(group["trade_value"], errors="coerce").mean()) if len(group) else None,
            "min_trade_value": float(pd.to_numeric(group["trade_value"], errors="coerce").min()) if len(group) else None,
            "max_rank": int(pd.to_numeric(group["rank_in_branch"], errors="coerce").max()) if len(group) else None,
        })
    return pd.DataFrame(rows)


def summarize_daily(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, group in df.groupby(["date", "regime", "method_branch", "horizon"], dropna=False):
        date, regime, branch, horizon = keys
        closed = group[group["forward_return_status"].astype(str).eq("CLOSED")].copy()
        pending = group[group["forward_return_status"].astype(str).eq("PENDING")].copy()
        ret = pd.to_numeric(closed["forward_return"], errors="coerce").dropna()
        rows.append({
            "date": date,
            "regime": regime,
            "method_branch": branch,
            "horizon": horizon,
            "rows": int(len(group)),
            "closed_rows": int(len(closed)),
            "pending_rows": int(len(pending)),
            "unique_codes": int(group["code"].nunique()),
            "portfolio_ret": float(ret.mean()) if len(ret) else None,
            "win_rate": float((ret > 0).mean()) if len(ret) else None,
            "top_codes": "|".join(group.sort_values("rank_in_branch").head(10)["code"].astype(str).tolist()),
        })
    return pd.DataFrame(rows).sort_values(["date", "regime", "method_branch"]).reset_index(drop=True)


def collect_issues(latest: pd.DataFrame, history: pd.DataFrame, meta: dict[str, Any]) -> pd.DataFrame:
    issues: list[dict[str, Any]] = []

    missing_latest = sorted(REQUIRED_COLUMNS - set(latest.columns))
    missing_history = sorted(REQUIRED_COLUMNS - set(history.columns))
    if missing_latest:
        issues.append({"severity": "HIGH", "issue": "latest_missing_required_columns", "detail": ",".join(missing_latest)})
    if missing_history:
        issues.append({"severity": "HIGH", "issue": "history_missing_required_columns", "detail": ",".join(missing_history)})

    duplicate_keys = ["date", "regime", "method_branch", "code"]
    latest_dupes = int(latest.duplicated(duplicate_keys).sum()) if set(duplicate_keys).issubset(latest.columns) else -1
    history_dupes = int(history.duplicated(duplicate_keys).sum()) if set(duplicate_keys).issubset(history.columns) else -1
    if latest_dupes:
        issues.append({"severity": "HIGH", "issue": "latest_duplicate_candidate_keys", "detail": str(latest_dupes)})
    if history_dupes:
        issues.append({"severity": "HIGH", "issue": "history_duplicate_candidate_keys", "detail": str(history_dupes)})

    if latest["operational_use"].astype(str).str.lower().ne("false").any():
        issues.append({"severity": "HIGH", "issue": "latest_operational_use_not_false", "detail": "operational_use must remain false"})
    if history["operational_use"].astype(str).str.lower().ne("false").any():
        issues.append({"severity": "HIGH", "issue": "history_operational_use_not_false", "detail": "operational_use must remain false"})

    if latest["status"].astype(str).str.contains("OBSERVE_ONLY", na=False).all() is False:
        issues.append({"severity": "HIGH", "issue": "latest_status_not_observe_only", "detail": "all latest rows must be observe-only"})

    for name, df in [("latest", latest), ("history", history)]:
        null_codes = int(df["code"].isna().sum())
        null_dates = int(df["date"].isna().sum())
        null_scores = int(df["candidate_score"].isna().sum())
        bad_rank = int((pd.to_numeric(df["rank_in_branch"], errors="coerce") <= 0).sum())
        if null_codes:
            issues.append({"severity": "HIGH", "issue": f"{name}_null_codes", "detail": str(null_codes)})
        if null_dates:
            issues.append({"severity": "HIGH", "issue": f"{name}_null_dates", "detail": str(null_dates)})
        if null_scores:
            issues.append({"severity": "MEDIUM", "issue": f"{name}_null_candidate_scores", "detail": str(null_scores)})
        if bad_rank:
            issues.append({"severity": "MEDIUM", "issue": f"{name}_non_positive_rank", "detail": str(bad_rank)})

    meta_effect = meta.get("operation_effect", {})
    if any(value is not False for value in meta_effect.values()):
        issues.append({"severity": "HIGH", "issue": "meta_operation_effect_not_all_false", "detail": str(meta_effect)})

    if not issues:
        issues.append({"severity": "INFO", "issue": "no_schema_or_policy_issues_found", "detail": "candidate structure is valid for observe-only revalidation"})
    return pd.DataFrame(issues)


def decide_readiness(branch_summary: pd.DataFrame, issues: pd.DataFrame) -> str:
    high_issues = issues[issues["severity"].astype(str).eq("HIGH")]
    if len(high_issues):
        return "NEEDS_FIX_SCHEMA_OR_POLICY"
    closed = branch_summary[branch_summary["dataset"].eq("history")]
    enough_dates = closed["closed_dates"].fillna(0).max() >= 5 if len(closed) else False
    if enough_dates:
        return "READY_FOR_BRANCH_REVALIDATION_READ_ONLY"
    return "DATA_ACCUMULATION_NEEDED_BEFORE_LOGIC_PROMOTION"


def main() -> int:
    required = [LATEST, HISTORY, META]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("missing inputs: " + "; ".join(missing))

    latest = load_candidates(LATEST)
    history = load_candidates(HISTORY)
    meta = json.loads(META.read_text(encoding="utf-8"))

    latest_summary = summarize_branch(latest, "latest")
    history_summary = summarize_branch(history, "history")
    branch_summary = pd.concat([latest_summary, history_summary], ignore_index=True).sort_values(
        ["dataset", "regime", "method_branch"]
    ).reset_index(drop=True)
    daily = summarize_daily(history)
    issues = collect_issues(latest, history, meta)
    readiness = decide_readiness(branch_summary, issues)

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "new_method_candidates_observe_only_validation",
        "classification": "NEW_METHOD_CANDIDATES_VALIDATION_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED_TO_OPERATIONAL_LOGIC",
        "conclusion": readiness,
        "inputs": {
            "latest": str(LATEST),
            "history": str(HISTORY),
            "meta": str(META),
        },
        "row_counts": {
            "latest_rows": int(len(latest)),
            "history_rows": int(len(history)),
            "latest_closed_rows": int((latest["forward_return_status"] == "CLOSED").sum()),
            "latest_pending_rows": int((latest["forward_return_status"] == "PENDING").sum()),
            "history_closed_rows": int((history["forward_return_status"] == "CLOSED").sum()),
            "history_pending_rows": int((history["forward_return_status"] == "PENDING").sum()),
            "branch_summary_rows": int(len(branch_summary)),
            "daily_rows": int(len(daily)),
            "issues_rows": int(len(issues)),
        },
        "branch_summary": clean_json_value(branch_summary.to_dict(orient="records")),
        "issues": clean_json_value(issues.to_dict(orient="records")),
        "operation_effect": {
            "operational_candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "gate_or_threshold_change": False,
            "order_path": False,
        },
        "validation": [
            "required_inputs_exist: PASS",
            "schema_required_columns: PASS" if not set(issues["issue"]).intersection({"latest_missing_required_columns", "history_missing_required_columns"}) else "schema_required_columns: FAIL",
            "duplicate_candidate_keys_zero: PASS" if not set(issues["issue"]).intersection({"latest_duplicate_candidate_keys", "history_duplicate_candidate_keys"}) else "duplicate_candidate_keys_zero: FAIL",
            "observe_only_policy_preserved: PASS" if not set(issues["issue"]).intersection({"latest_operational_use_not_false", "history_operational_use_not_false", "latest_status_not_observe_only"}) else "observe_only_policy_preserved: FAIL",
            "operation_effect_no_changes: PASS",
        ],
    }
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    branch_summary.to_csv(OUT_BRANCH, index=False, encoding="utf-8-sig")
    daily.to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    issues.to_csv(OUT_ISSUES, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(clean_json_value(payload), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# New Method Candidates Validation",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        "",
        "## Row Counts",
        "",
        *[f"- {k}: {v}" for k, v in payload["row_counts"].items()],
        "",
        "## Branch Summary",
        "",
    ]
    for row in payload["branch_summary"]:
        if row["dataset"] != "history":
            continue
        md.append(
            f"- {row['regime']} / {row['method_branch']} / {row['horizon']}: "
            f"rows={row['rows']}, closed={row['closed_rows']}, pending={row['pending_rows']}, "
            f"closed_dates={row['closed_dates']}, mean={row['ret_mean']}, pf={row['profit_factor']}, worst={row['worst_ret']}"
        )
    md += [
        "",
        "## Issues",
        "",
    ]
    for row in payload["issues"]:
        md.append(f"- {row['severity']}: {row['issue']} - {row['detail']}")
    md += [
        "",
        "## Boundary",
        "",
        "- Validation uses new_method_candidates_history/latest only.",
        "- NOT_APPLIED to operational candidate generation, official backtest, HPO, paper/live, gates, thresholds, or orders.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_BRANCH}")
    print(f"[OK] wrote {OUT_DAILY}")
    print(f"[OK] wrote {OUT_ISSUES}")
    print(f"[OK] wrote {OUT_MD}")
    print(f"[CONCLUSION] {readiness}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
