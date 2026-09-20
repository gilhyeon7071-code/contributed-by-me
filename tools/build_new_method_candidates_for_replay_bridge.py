from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

NEW_METHOD_LATEST = LOG_DIR / "new_method_candidates_latest.csv"
NEW_METHOD_META = LOG_DIR / "new_method_candidates_meta.json"
APPLICATION_CONTRACT = LOG_DIR / "new_method_logic_application_contract_latest.json"

OUT_CSV = LOG_DIR / "new_method_candidates_for_replay_latest.csv"
OUT_META = LOG_DIR / "new_method_candidates_for_replay_meta.json"
OUT_MD = LOG_DIR / "new_method_candidates_for_replay_latest.md"

REPLAY_COLUMNS = [
    "date",
    "code",
    "name",
    "market",
    "market_regime",
    "close",
    "value",
    "score",
    "final_score",
    "stretch",
    "atr14_pct",
    "rsi14",
    "high_52w_gap",
    "relax_level",
    "candidate_origin",
    "method_branch",
    "signal_reason",
    "horizon",
    "expected_holding_days",
    "observe_status",
    "promotion_blocker",
    "forward_return_status",
    "source",
]


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def clean_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): clean_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean_json_value(v) for v in value]
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def normalize_code(value: Any) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6) if text.isdigit() else text


def numeric_series(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def main() -> int:
    required = [NEW_METHOD_LATEST, NEW_METHOD_META, APPLICATION_CONTRACT]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("missing inputs: " + "; ".join(missing))

    src = pd.read_csv(NEW_METHOD_LATEST, encoding="utf-8-sig", dtype={"code": str})
    method_meta = read_json(NEW_METHOD_META)
    contract = read_json(APPLICATION_CONTRACT)

    if src.empty:
        raise SystemExit("new_method_candidates_latest.csv is empty")

    out = pd.DataFrame(index=src.index)
    out["date"] = pd.to_datetime(src["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["code"] = src["code"].map(normalize_code)
    out["name"] = ""
    out["market"] = src.get("market", "").astype(str)
    out["market_regime"] = src.get("regime", "").astype(str)
    out["close"] = numeric_series(src, "close")
    out["value"] = numeric_series(src, "trade_value")
    out["score"] = numeric_series(src, "candidate_score").clip(0.0, 1.0).round(6)
    out["final_score"] = out["score"]
    out["stretch"] = numeric_series(src, "stretch", 0.0)
    out["atr14_pct"] = numeric_series(src, "atr_pct", 0.0)
    out["rsi14"] = numeric_series(src, "rsi14", 0.0)
    out["high_52w_gap"] = numeric_series(src, "high_52w_gap", 0.0)
    out["relax_level"] = src.get("method_branch", "").astype(str)
    out["candidate_origin"] = "NEW_METHOD_OBSERVE_ONLY"
    out["method_branch"] = src.get("method_branch", "").astype(str)
    out["signal_reason"] = src.get("signal_reason", "").astype(str)
    out["horizon"] = src.get("horizon", "").astype(str)
    out["expected_holding_days"] = numeric_series(src, "expected_holding_days").astype(int)
    out["observe_status"] = src.get("status", "").astype(str)
    out["promotion_blocker"] = src.get("promotion_blocker", "").astype(str)
    out["forward_return_status"] = src.get("forward_return_status", "").astype(str)
    out["source"] = src.get("source", "NEW_METHOD_OBSERVE_ONLY").astype(str)
    out["new_method_rank_in_branch"] = numeric_series(src, "rank_in_branch").astype(int)
    out["new_method_forward_return"] = numeric_series(src, "forward_return", default=float("nan"))

    for col in REPLAY_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    ordered_cols = REPLAY_COLUMNS + [c for c in out.columns if c not in REPLAY_COLUMNS]
    out = out[ordered_cols].sort_values(["date", "market_regime", "method_branch", "new_method_rank_in_branch", "code"]).reset_index(drop=True)

    branch_counts = (
        out.groupby(["market_regime", "method_branch", "horizon", "observe_status", "promotion_blocker"], dropna=False)
        .agg(
            rows=("code", "size"),
            latest_date=("date", "max"),
            closed_rows=("forward_return_status", lambda s: int((s == "CLOSED").sum())),
            pending_rows=("forward_return_status", lambda s: int((s == "PENDING").sum())),
            unique_codes=("code", "nunique"),
        )
        .reset_index()
        .sort_values(["market_regime", "method_branch"])
    )

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "new_method_candidates_for_replay_bridge",
        "classification": "NEW_METHOD_CANDIDATES_FOR_REPLAY_BRIDGE",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED_TO_OPERATIONAL_LOGIC",
        "logic_application_stage": "PARALLEL_CANDIDATE_BRIDGE_APPLIED",
        "input": str(NEW_METHOD_LATEST),
        "contract": str(APPLICATION_CONTRACT),
        "candidate_origin": "NEW_METHOD_OBSERVE_ONLY",
        "methodology_meta_stage": method_meta.get("logic_application_stage"),
        "row_counts": {
            "input_rows": int(len(src)),
            "output_rows": int(len(out)),
            "closed_rows": int((out["forward_return_status"] == "CLOSED").sum()),
            "pending_rows": int((out["forward_return_status"] == "PENDING").sum()),
            "branch_count": int(out[["market_regime", "method_branch"]].drop_duplicates().shape[0]),
        },
        "branch_counts": branch_counts.astype(object).where(pd.notna(branch_counts), None).to_dict(orient="records"),
        "schema": {
            "replay_columns": REPLAY_COLUMNS,
            "extra_columns": [c for c in out.columns if c not in REPLAY_COLUMNS],
        },
        "must_not_write_confirmed": [
            "2_Logs/candidates_latest_data.csv",
            "2_Logs/candidates_latest.csv",
            "2_Logs/candidates_latest_meta.json",
        ],
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
            "bridge_rows_generated: PASS" if len(out) else "bridge_rows_generated: FAIL",
            "candidate_origin_fixed: PASS" if set(out["candidate_origin"]) == {"NEW_METHOD_OBSERVE_ONLY"} else "candidate_origin_fixed: FAIL",
            "required_replay_columns_present: PASS" if set(REPLAY_COLUMNS).issubset(out.columns) else "required_replay_columns_present: FAIL",
            "operation_effect_no_changes: PASS",
        ],
    }
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    OUT_META.write_text(json.dumps(clean_json_value(payload), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# New Method Candidates For Replay Bridge",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- logic_application_stage: {payload['logic_application_stage']}",
        f"- candidate_origin: {payload['candidate_origin']}",
        "",
        "## Row Counts",
        "",
        *[f"- {k}: {v}" for k, v in payload["row_counts"].items()],
        "",
        "## Branch Counts",
        "",
    ]
    for row in payload["branch_counts"]:
        md.append(
            f"- {row['market_regime']} / {row['method_branch']} / {row['horizon']}: "
            f"rows={row['rows']}, closed={row['closed_rows']}, pending={row['pending_rows']}, blocker={row['promotion_blocker']}"
        )
    md += [
        "",
        "## Boundary",
        "",
        "- This bridge is for replay/followthrough validation input only.",
        "- NOT_APPLIED to candidates_latest_data.csv, final_score, official backtest, HPO, paper/live, gates, thresholds, or orders.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_CSV}")
    print(f"[OK] wrote {OUT_META}")
    print(f"[OK] wrote {OUT_MD}")
    print("[CONCLUSION] PARALLEL_CANDIDATE_BRIDGE_APPLIED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
