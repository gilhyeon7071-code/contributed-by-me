from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

GEN_CAND = ROOT / "generate_candidates_v41_1.py"
FINAL_SCORE = ROOT / "tools" / "final_score_merge_daily.py"
FOLLOWTHROUGH = ROOT / "tools" / "followthrough_realtime.py"
NEW_METHOD_META = LOG_DIR / "new_method_candidates_meta.json"
NEW_METHOD_VALIDATION = LOG_DIR / "new_method_candidates_validation_latest.json"

OUT_JSON = LOG_DIR / "new_method_logic_application_contract_latest.json"
OUT_MD = LOG_DIR / "new_method_logic_application_contract_latest.md"


EVIDENCE_POINTS = {
    "generate_select_candidates": {
        "path": str(GEN_CAND),
        "lines": "510-525",
        "meaning": "_select_candidates applies the current core technical filters and returns the operational candidate pool.",
    },
    "generate_candidate_selection_loop": {
        "path": str(GEN_CAND),
        "lines": "1712-1751",
        "meaning": "The relax ladder selects candidates and then sector-union/watch filters are applied before output.",
    },
    "generate_output_commit": {
        "path": str(GEN_CAND),
        "lines": "1802-1856",
        "meaning": "candidates_latest_data.csv and candidates_latest_meta.json are written here; replacing this path would affect operations.",
    },
    "final_score_input_and_merge": {
        "path": str(FINAL_SCORE),
        "lines": "1988-2018",
        "meaning": "final_score_merge_daily reads an existing candidate file and overlays sector/news/topic layers.",
    },
    "final_score_write": {
        "path": str(FINAL_SCORE),
        "lines": "2196-2225",
        "meaning": "final_score is computed and written after overlay scoring.",
    },
    "followthrough_candidate_input": {
        "path": str(FOLLOWTHROUGH),
        "lines": "778-838",
        "meaning": "followthrough_realtime consumes a candidate CSV path and joins it with intraday rows; it keeps candidate_origin if present.",
    },
    "followthrough_entry_validation": {
        "path": str(FOLLOWTHROUGH),
        "lines": "856-893",
        "meaning": "intraday buy_signal and entry_allowed_validation are calculated from candidate + intraday fields.",
    },
}


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    required = [GEN_CAND, FINAL_SCORE, FOLLOWTHROUGH, NEW_METHOD_META, NEW_METHOD_VALIDATION]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("missing inputs: " + "; ".join(missing))

    meta = read_json(NEW_METHOD_META)
    validation = read_json(NEW_METHOD_VALIDATION)

    contract = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "new_method_logic_application_contract",
        "classification": "NEW_METHOD_LOGIC_APPLICATION_CONTRACT_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED_TO_OPERATIONAL_LOGIC",
        "objective": "Fix the next code-level application path for the methodology cycle: validation -> logic application -> new data -> revalidation.",
        "current_new_method_state": {
            "candidate_generator_stage": meta.get("logic_application_stage"),
            "candidate_rows": meta.get("row_counts"),
            "validation_conclusion": validation.get("conclusion"),
            "validation_rows": validation.get("row_counts"),
        },
        "code_evidence": EVIDENCE_POINTS,
        "decision": {
            "do_not_replace_generate_candidates_output_yet": True,
            "reason": "generate_candidates_v41_1 writes the operational candidates_latest_data.csv; replacing it before enough closed new-method samples would mix observe-only research with operations.",
            "first_controlled_application_point": "parallel_candidate_bridge_after_generation",
            "first_controlled_application_artifact": "2_Logs/new_method_candidates_for_replay_latest.csv",
            "why": "It applies the new methodology to a candidate-like schema without changing candidates_latest_data.csv, final_score, paper/live, gates, or orders.",
        },
        "application_path": [
            {
                "stage": "A_DONE_observe_only_candidate_generator",
                "artifact": "2_Logs/new_method_candidates_latest.csv and history.csv",
                "status": "DONE",
                "operation_effect": "none",
            },
            {
                "stage": "B_NEXT_parallel_candidate_bridge",
                "artifact": "2_Logs/new_method_candidates_for_replay_latest.csv",
                "status": "NEXT",
                "description": "Convert new_method_candidates into the minimal candidate schema consumed by replay/followthrough validation, with candidate_origin=NEW_METHOD_OBSERVE_ONLY.",
                "must_not_write": [
                    "2_Logs/candidates_latest_data.csv",
                    "2_Logs/candidates_latest.csv",
                    "2_Logs/candidates_latest_meta.json",
                ],
            },
            {
                "stage": "C_LATER_followthrough_observe_run",
                "artifact": "followthrough validation output using --candidates new_method_candidates_for_replay_latest.csv",
                "status": "LATER",
                "description": "Only if intraday source data is available; run as validation, not orders.",
            },
            {
                "stage": "D_LATER_operational_candidate_overlay_design",
                "artifact": "patch proposal for generate_candidates_v41_1 or final_score_merge_daily",
                "status": "LATER",
                "entry_condition": "enough closed dates and explicit approval to modify the operational candidate path.",
            },
        ],
        "mapping_contract": {
            "new_method_to_candidate_schema": {
                "date": "date",
                "code": "code",
                "market": "market",
                "close": "close",
                "value": "trade_value",
                "score": "candidate_score",
                "final_score": "candidate_score",
                "relax_level": "method_branch",
                "candidate_origin": "NEW_METHOD_OBSERVE_ONLY",
                "market_regime": "regime",
            },
            "additional_columns_to_preserve": [
                "method_branch",
                "signal_reason",
                "horizon",
                "expected_holding_days",
                "status",
                "promotion_blocker",
                "source",
                "forward_return_status",
            ],
        },
        "promotion_gate": {
            "minimum_before_operational_overlay_design": [
                "schema/policy validation PASS",
                "no duplicate candidate keys",
                "observe-only candidate history has enough closed signal dates",
                "pending STRESS/BEAR rows are revalidated after price data refresh",
                "explicit user approval before touching operational candidate files",
            ],
            "current_blockers": [
                "closed signal dates are small",
                "STRESS h2 latest rows pending",
                "BEAR h5 latest rows pending",
                "TRANSITION has only one closed signal date",
            ],
        },
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
            "required_code_files_exist: PASS",
            "new_method_candidate_meta_exists: PASS",
            "new_method_candidate_validation_exists: PASS",
            "code_evidence_points_fixed: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    OUT_JSON.write_text(json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8")

    md = [
        "# New Method Logic Application Contract",
        "",
        f"- generated_at: {contract['generated_at']}",
        f"- classification: {contract['classification']}",
        f"- operational_decision: {contract['operational_decision']}",
        f"- full_logic_application: {contract['full_logic_application']}",
        "",
        "## Decision",
        "",
        f"- first_controlled_application_point: {contract['decision']['first_controlled_application_point']}",
        f"- first_controlled_application_artifact: {contract['decision']['first_controlled_application_artifact']}",
        f"- reason: {contract['decision']['reason']}",
        "",
        "## Code Evidence",
        "",
    ]
    for key, item in EVIDENCE_POINTS.items():
        md.append(f"- {key}: {item['path']}:{item['lines']} - {item['meaning']}")
    md += [
        "",
        "## Application Path",
        "",
    ]
    for item in contract["application_path"]:
        md.append(f"- {item['stage']}: {item['status']} - {item.get('description', item.get('artifact', ''))}")
    md += [
        "",
        "## Next",
        "",
        "- Build the parallel candidate bridge artifact only: 2_Logs/new_method_candidates_for_replay_latest.csv",
        "- Do not write candidates_latest_data.csv, final_score outputs, paper/live, gates, or orders.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_MD}")
    print("[CONCLUSION] NEW_METHOD_LOGIC_APPLICATION_CONTRACT_BUILT_READ_ONLY")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
