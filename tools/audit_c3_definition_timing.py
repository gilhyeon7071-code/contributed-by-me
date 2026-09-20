from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"

CONTRACT_PATH = LOG_DIR / "c3_e_definition_contract_latest.json"
COMPARISON_PATH = LOG_DIR / "c3_strategy_definition_comparison_summary_latest.csv"
OFFICIAL_CONTRACT_SOURCE = ROOT / "tools" / "build_c3_official_replay_contract.py"
STRATEGY_REPLAY_SOURCE = ROOT / "tools" / "run_c3_strategy_param_replay.py"

LATEST_JSON = LOG_DIR / "c3_definition_timing_audit_latest.json"
LATEST_CSV = LOG_DIR / "c3_definition_timing_audit_latest.csv"
LATEST_MD = LOG_DIR / "c3_definition_timing_audit_latest.md"

STATUS = "READ_ONLY_C3_DEFINITION_TIMING_AUDIT_NOT_OPERATIONAL"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _as_int(value: Any) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def _as_float(value: Any) -> float:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def _metric_map(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("definition", "")).strip()
        if not name:
            continue
        out[name] = {
            "n": _as_int(row.get("n")),
            "win_rate": _as_float(row.get("win_rate")),
            "ret_sum": _as_float(row.get("ret_sum")),
            "ret_mean": _as_float(row.get("ret_mean")),
            "profit_factor": _as_float(row.get("profit_factor")),
        }
    return out


def _source_evidence() -> dict[str, Any]:
    official = OFFICIAL_CONTRACT_SOURCE.read_text(encoding="utf-8-sig")
    replay = STRATEGY_REPLAY_SOURCE.read_text(encoding="utf-8-sig")
    return {
        "official_contract_source": str(OFFICIAL_CONTRACT_SOURCE),
        "strategy_replay_source": str(STRATEGY_REPLAY_SOURCE),
        "official_contract_mentions_report_after_entry": "report_trade_csv_after_entry" in official,
        "official_contract_mentions_cross_recheck": "cross_variant_recheck" in official,
        "official_contract_native_support_false": '"native_report_support": False' in official,
        "strategy_replay_joins_market_after_recheck": 'join_cols = join_keys + ["c3_after_recheck", "market_after_recheck", "market_raw"]' in replay,
        "strategy_replay_filters_c3_after_recheck": 'joined["c3_after_recheck"].eq(1)' in replay,
        "strategy_replay_filters_market_after_recheck": 'joined["market_after_recheck"].str.strip().ne("")' in replay,
    }


def _field_timing_rows() -> list[dict[str, Any]]:
    return [
        {
            "field": "native_selection_contract",
            "source": "c3_e_definition_contract_latest.json/native_selection_contract",
            "timing_bucket": "pre_entry_research_contract",
            "decision_time_status": "usable_as_research_tradable_proxy_after_param_approval",
            "reason": "signal-date filters, entry trigger, exit override contract are native replay inputs, not post-replay joins.",
        },
        {
            "field": "signal_v_accel",
            "source": "native trade field / native numeric filter at >=0.8",
            "timing_bucket": "signal_date_feature",
            "decision_time_status": "available_as_signal_feature",
            "reason": "already present in native selection contract at >=0.8; stricter >=0.9 was diagnostic in current evidence.",
        },
        {
            "field": "score",
            "source": "report_trade_csv",
            "timing_bucket": "report_selection_score",
            "decision_time_status": "not_selected_as_mandatory",
            "reason": "adapter breakdown showed score>=1 removes many rows when used with other filters; E contract kept it diagnostic.",
        },
        {
            "field": "followthrough_1d",
            "source": "report_trade_csv_after_entry",
            "timing_bucket": "post_entry_day_confirmation",
            "decision_time_status": "not_usable_for_original_entry_without_delayed_entry_design",
            "reason": "official contract source labels it after-entry; it can support a delayed-entry or confirmation study, not same original entry.",
        },
        {
            "field": "market_after_recheck",
            "source": "cross_variant_validation_join",
            "timing_bucket": "post_replay_recheck_join",
            "decision_time_status": "not_decision_time_proven",
            "reason": "strategy replay joins this field from the cross validation artifact after native trades are produced.",
        },
        {
            "field": "c3_after_recheck",
            "source": "cross_variant_recheck",
            "timing_bucket": "post_replay_recheck_filter",
            "decision_time_status": "not_usable_as_native_preselection",
            "reason": "official contract states c3_after_recheck is a post-replay/recheck field and native report support is false.",
        },
    ]


def _definition_rows(metrics: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "definition": "A_native_c3_contract",
            "timing_class": "tradable_proxy_candidate",
            "operational_status": "research_only_not_approved",
            "decision": "keep_as_C3_TRADABLE_PROXY_V0_input",
            "blocker": "strategy-scoped parameter approval and later-window validation still required",
            **metrics.get("A_native_c3_contract", {}),
        },
        {
            "definition": "B_native_plus_followthrough",
            "timing_class": "delayed_entry_or_confirmation_candidate",
            "operational_status": "not_original_entry_rule",
            "decision": "study_separately_if_entry_is_after_followthrough_confirmation",
            "blocker": "followthrough_1d is after-entry for the current original entry timing",
            **metrics.get("B_native_plus_followthrough", {}),
        },
        {
            "definition": "C_native_plus_market_after_recheck",
            "timing_class": "diagnostic_or_requires_market_source_mapping",
            "operational_status": "not_decision_time_proven",
            "decision": "do_not_use_until_market_after_recheck_is_mapped_to_pre_entry_source",
            "blocker": "market_after_recheck comes from cross validation join in current evidence",
            **metrics.get("C_native_plus_market_after_recheck", {}),
        },
        {
            "definition": "E_native_plus_followthrough_and_market",
            "timing_class": "diagnostic_high_quality_subset",
            "operational_status": "not_original_entry_rule",
            "decision": "preserve_as_C3_DIAGNOSTIC_E_not_operational_entry_condition",
            "blocker": "requires both after-entry followthrough and post-replay market_after_recheck",
            **metrics.get("E_native_plus_followthrough_and_market", {}),
        },
        {
            "definition": "F_full_current_adapter",
            "timing_class": "post_replay_adapter",
            "operational_status": "too_narrow_and_not_native",
            "decision": "do_not_promote_as_current_c3_entry_rule",
            "blocker": "mandatory c3_after_recheck collapses sample to 3 rows",
            **metrics.get("F_full_current_adapter", {}),
        },
    ]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "record_type",
        "name",
        "timing_class",
        "decision_time_status",
        "operational_status",
        "decision",
        "blocker",
        "n",
        "win_rate",
        "ret_sum",
        "ret_mean",
        "profit_factor",
        "source",
        "reason",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# C3 Definition Timing Audit",
        "",
        f"- status: `{payload['status']}`",
        f"- created_at: `{payload['created_at']}`",
        f"- conclusion: `{payload['conclusion']}`",
        f"- full_logic_application: `{payload['operation_effect']['full_logic_application']}`",
        "",
        "## Definition Timing Decisions",
        "",
        "| definition | timing_class | n | ret_sum | profit_factor | decision |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for row in payload["definition_timing"]:
        lines.append(
            "| {definition} | {timing_class} | {n} | {ret_sum} | {profit_factor} | {decision} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Field Timing",
            "",
            "| field | timing_bucket | decision_time_status | source |",
            "|---|---|---|---|",
        ]
    )
    for row in payload["field_timing"]:
        lines.append(
            f"| {row['field']} | {row['timing_bucket']} | {row['decision_time_status']} | {row['source']} |"
        )
    lines.extend(
        [
            "",
            "## Required Split",
            "",
            "- `C3_TRADABLE_PROXY_V0`: native C3 contract only; research-only until parameter approval and later-window replay.",
            "- `C3_DELAYED_CONFIRMATION_STUDY`: native C3 plus `followthrough_1d`; only valid if entry timing is redesigned after confirmation.",
            "- `C3_DIAGNOSTIC_E`: native C3 plus `followthrough_1d` plus `market_after_recheck`; preserve as diagnostic subset, not original entry condition.",
            "",
            "## Not Changed",
            "",
            "- candidate generation",
            "- official backtest policy",
            "- HPO",
            "- paper/live order path",
            "- stable parameter promotion",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    if not CONTRACT_PATH.exists():
        raise FileNotFoundError(CONTRACT_PATH)
    if not COMPARISON_PATH.exists():
        raise FileNotFoundError(COMPARISON_PATH)

    contract = _read_json(CONTRACT_PATH)
    metrics = _metric_map(_read_csv(COMPARISON_PATH))
    field_rows = _field_timing_rows()
    definition_rows = _definition_rows(metrics)
    source_evidence = _source_evidence()

    conclusion = (
        "C3 E has strong research outcome metrics but is not a valid original-entry rule in current evidence; "
        "split native C3 as tradable proxy and E as diagnostic/delayed-confirmation evidence."
    )
    payload = {
        "status": STATUS,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_artifacts": {
            "contract": str(CONTRACT_PATH),
            "comparison_summary": str(COMPARISON_PATH),
        },
        "contract_status": contract.get("contract_status"),
        "definition_id": contract.get("definition_id"),
        "conclusion": conclusion,
        "field_timing": field_rows,
        "definition_timing": definition_rows,
        "source_evidence": source_evidence,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "next_required_before_integration": [
            "Map market_after_recheck to a pre-entry market/regime source or remove it from original-entry rules.",
            "If followthrough_1d is desired, define a delayed-entry timing contract and replay it separately.",
            "Replay C3_TRADABLE_PROXY_V0 on later/OOS windows before any parameter approval proposal.",
        ],
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    csv_rows: list[dict[str, Any]] = []
    for row in definition_rows:
        csv_rows.append({"record_type": "definition", "name": row["definition"], **row})
    for row in field_rows:
        csv_rows.append({"record_type": "field", "name": row["field"], **row})
    _write_csv(LATEST_CSV, csv_rows)
    _write_md(LATEST_MD, payload)

    print(json.dumps({"status": STATUS, "json": str(LATEST_JSON), "csv": str(LATEST_CSV), "md": str(LATEST_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
