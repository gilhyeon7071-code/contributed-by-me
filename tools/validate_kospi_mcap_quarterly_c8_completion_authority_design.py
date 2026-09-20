from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_completion_authority_design_contract_v1.json"
)
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class DesignValidationError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DesignValidationError("JSON_READ_FAILED", str(path)) from exc
    if not isinstance(payload, dict):
        raise DesignValidationError("JSON_OBJECT_REQUIRED", str(path))
    return payload


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def load_design_contract(
    root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    path = contract_path or root / CONTRACT_RELATIVE
    contract = _load_json(path)
    if contract.get("strategy_id") != STRATEGY_ID or contract.get("mode") != "VALIDATION_ONLY":
        raise DesignValidationError("DESIGN_CONTRACT_INVALID")
    permissions = contract.get("execution_permissions")
    if not isinstance(permissions, Mapping) or any(value is not False for value in permissions.values()):
        raise DesignValidationError("DESIGN_PERMISSIONS_INVALID")
    return contract


def inspect_current_adapter_interface(source: str) -> dict[str, Any]:
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise DesignValidationError("ADAPTER_SOURCE_SYNTAX_INVALID", str(exc)) from exc
    target = next(
        (
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "build_corporate_action_adapter_output"
        ),
        None,
    )
    if target is None:
        raise DesignValidationError("ADAPTER_ENTRYPOINT_MISSING")
    parameters = [argument.arg for argument in target.args.args + target.args.kwonlyargs]
    context_names = {
        "completion_source_context_by_receipt",
        "document_source_by_receipt",
        "viewer_landing_payload_by_receipt",
        "viewer_landing_payloads",
    }
    has_context_input = bool(context_names & set(parameters))
    has_table_structure_parser = "HTMLParser" in source or "parse_table_rows" in source
    has_viewer_tree_parser = "NODE_BLOCK_PATTERN" in source or "parse_viewer_node_titles" in source
    flattens_document = "_document_text(payload)" in source
    return {
        "entrypoint_parameters": parameters,
        "has_completion_source_context_input": has_context_input,
        "has_structured_table_parser": has_table_structure_parser,
        "has_viewer_tree_parser": has_viewer_tree_parser,
        "flattens_document_before_date_parse": flattens_document,
        "interface_supports_proposed_authority": bool(
            has_context_input and has_table_structure_parser and has_viewer_tree_parser
        ),
    }


def validate_candidate_receipt(item: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    classification = str(item.get("classification") or "")
    document_hash = str(item.get("document_sha256") or "")
    if not SHA256_PATTERN.fullmatch(document_hash):
        reasons.append("DOCUMENT_HASH_INVALID")
    if item.get("promotion_allowed") is not False:
        reasons.append("PROMOTION_PERMISSION_CHANGED")
    if classification == "SCHEDULE_ROW_EXACT_DATE_CANDIDATE":
        if item.get("source") != "OPENDART_PASS":
            reasons.append("SCHEDULE_ROW_SOURCE_NOT_OPENDART")
        if len(item.get("schedule_row_dates") or []) != 1:
            reasons.append("SCHEDULE_ROW_DATE_NOT_UNIQUE")
    elif classification == "OFFICIAL_SCHEDULE_NODE_EXACT_DATE_CANDIDATE":
        landing_hash = str(item.get("landing_sha256") or "")
        node_ids = item.get("schedule_node_ele_ids") or []
        node_dates = item.get("schedule_node_dates") or []
        if item.get("source") != "VIEWER_FALLBACK":
            reasons.append("VIEWER_NODE_SOURCE_INVALID")
        if not SHA256_PATTERN.fullmatch(landing_hash):
            reasons.append("VIEWER_LANDING_HASH_INVALID")
        if len(node_ids) != 1:
            reasons.append("VIEWER_SCHEDULE_NODE_NOT_UNIQUE")
        if len(node_dates) != 1:
            reasons.append("VIEWER_SCHEDULE_DATE_NOT_UNIQUE")
        linked_dates = {
            date
            for evidence in item.get("schedule_row_evidence") or []
            if evidence.get("member_ele_id") in node_ids
            for date in evidence.get("full_dates") or []
        }
        if linked_dates != set(node_dates):
            reasons.append("VIEWER_NODE_MEMBER_DATE_MISMATCH")
    else:
        reasons.append("UNEXPECTED_CANDIDATE_CLASS")
    if not str(item.get("candidate_actual_effective_date") or ""):
        reasons.append("CANDIDATE_DATE_MISSING")
    return sorted(set(reasons))


def validate_blocked_receipt(item: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    classification = str(item.get("classification") or "")
    if item.get("candidate_actual_effective_date"):
        reasons.append("BLOCKED_RECEIPT_HAS_CANDIDATE_DATE")
    if classification == "PARTIAL_DATE_YEAR_REQUIRED_BLOCKED":
        if not item.get("partial_dates") or item.get("schedule_row_dates"):
            reasons.append("PARTIAL_DATE_BLOCK_CONTRACT_MISMATCH")
    elif classification == "NON_MERGER_CONTENT_BLOCKED":
        if item.get("non_merger_content") is not True:
            reasons.append("NON_MERGER_BLOCK_CONTRACT_MISMATCH")
    else:
        reasons.append("UNEXPECTED_BLOCKED_CLASS")
    return sorted(set(reasons))


def validate_design(
    *, root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        root = root.resolve()
        contract = load_design_contract(root, contract_path)
        inputs = contract["inputs"]
        policy = _load_json(_resolve(root, inputs["policy_scope_report_path"]))
        residual = _load_json(_resolve(root, inputs["residual_scope_report_path"]))
        adapter_path = _resolve(root, inputs["adapter_source_path"])
        adapter_bytes = adapter_path.read_bytes()
        adapter_source = adapter_bytes.decode("utf-8")
        interface = inspect_current_adapter_interface(adapter_source)
        expected = contract["expected_baseline"]
        reasons: list[str] = []

        if policy.get("status") != "PASS" or residual.get("status") != "PASS":
            reasons.append("INPUT_ANALYSIS_NOT_PASS")
        if policy.get("selection_as_of") != residual.get("selection_as_of"):
            reasons.append("INPUT_SELECTION_AS_OF_MISMATCH")
        if policy.get("capture_status_before") != "BLOCKED":
            reasons.append("CAPTURE_STATUS_CHANGED")
        if policy.get("capture_full_capture_complete_before") is not False:
            reasons.append("FULL_CAPTURE_STATE_CHANGED")

        baseline = policy.get("current_adapter_baseline") or {}
        opendart = baseline.get("OPENDART_PASS") or {}
        viewer = baseline.get("VIEWER_FALLBACK") or {}
        checks = {
            "opendart_completion_count": opendart.get("completion_count"),
            "opendart_current_date_count": opendart.get("completion_date_current_count"),
            "viewer_completion_count": viewer.get("completion_count"),
            "viewer_current_date_count": viewer.get("completion_date_current_count"),
            "residual_count": residual.get("residual_receipt_count"),
            "schedule_row_candidates": (residual.get("classification_counts") or {}).get(
                "SCHEDULE_ROW_EXACT_DATE_CANDIDATE", 0
            ),
            "viewer_schedule_node_candidates": (residual.get("classification_counts") or {}).get(
                "OFFICIAL_SCHEDULE_NODE_EXACT_DATE_CANDIDATE", 0
            ),
            "partial_date_blocked": (residual.get("classification_counts") or {}).get(
                "PARTIAL_DATE_YEAR_REQUIRED_BLOCKED", 0
            ),
            "non_merger_blocked": (residual.get("classification_counts") or {}).get(
                "NON_MERGER_CONTENT_BLOCKED", 0
            ),
            "opendart_non_initial_lineage_required": opendart.get(
                "explicit_lineage_required_count"
            ),
            "opendart_non_initial_lineage_resolved": opendart.get(
                "explicit_lineage_resolved_count"
            ),
            "viewer_non_initial_lineage_required": viewer.get("explicit_lineage_required_count"),
            "viewer_non_initial_lineage_resolved": viewer.get("explicit_lineage_resolved_count"),
        }
        for name, expected_value in expected.items():
            if checks.get(name) != expected_value:
                reasons.append(f"BASELINE_{name.upper()}_MISMATCH")

        receipts = residual.get("receipts") or []
        candidate_classes = {
            "SCHEDULE_ROW_EXACT_DATE_CANDIDATE",
            "OFFICIAL_SCHEDULE_NODE_EXACT_DATE_CANDIDATE",
        }
        candidates = [item for item in receipts if item.get("classification") in candidate_classes]
        blocked = [item for item in receipts if item.get("classification") not in candidate_classes]
        candidate_errors = {
            str(item.get("receipt_no")): validate_candidate_receipt(item)
            for item in candidates
            if validate_candidate_receipt(item)
        }
        blocked_errors = {
            str(item.get("receipt_no")): validate_blocked_receipt(item)
            for item in blocked
            if validate_blocked_receipt(item)
        }
        if candidate_errors:
            reasons.append("CANDIDATE_EVIDENCE_INVALID")
        if blocked_errors:
            reasons.append("BLOCKED_EVIDENCE_INVALID")

        current_total = int(checks["opendart_current_date_count"]) + int(
            checks["viewer_current_date_count"]
        )
        completion_total = int(checks["opendart_completion_count"]) + int(
            checks["viewer_completion_count"]
        )
        added_count = len(candidates)
        proposed_total = current_total + added_count
        if completion_total - current_total != len(receipts):
            reasons.append("RESIDUAL_NOT_EXACT_CURRENT_DATE_COMPLEMENT")
        if proposed_total + len(blocked) != completion_total:
            reasons.append("PROPOSED_COUNT_RECONCILIATION_FAILED")

        source_candidate_counts = Counter(str(item.get("source") or "") for item in candidates)
        if source_candidate_counts != Counter({"OPENDART_PASS": 14, "VIEWER_FALLBACK": 2}):
            reasons.append("CANDIDATE_SOURCE_COUNTS_MISMATCH")

        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_COMPLETION_AUTHORITY_DESIGN_VALIDATION",
            "status": "PASS" if not reasons else "FAIL",
            "verdict": (
                "DESIGN_VALID_INTERFACE_EXTENSION_REQUIRED_ADAPTER_UNCHANGED"
                if not reasons
                else "FAIL_COMPLETION_AUTHORITY_DESIGN_VALIDATION"
            ),
            "reason_codes": sorted(set(reasons)),
            "generated_at": generated_at,
            "selection_as_of": policy.get("selection_as_of"),
            "adapter_source_sha256": hashlib.sha256(adapter_bytes).hexdigest(),
            "current_adapter_interface": interface,
            "completion_simulation": {
                "completion_total": completion_total,
                "current_date_count": current_total,
                "existing_date_override_count": 0,
                "structured_fallback_added_count": added_count,
                "proposed_date_count": proposed_total,
                "still_blocked_count": len(blocked),
                "candidate_source_counts": dict(sorted(source_candidate_counts.items())),
            },
            "candidate_receipts": [
                {
                    "receipt_no": item["receipt_no"],
                    "source": item["source"],
                    "candidate_actual_effective_date": item[
                        "candidate_actual_effective_date"
                    ],
                    "candidate_authority": item["candidate_authority"],
                }
                for item in candidates
            ],
            "blocked_receipts": [
                {
                    "receipt_no": item["receipt_no"],
                    "classification": item["classification"],
                }
                for item in blocked
            ],
            "candidate_validation_errors": candidate_errors,
            "blocked_validation_errors": blocked_errors,
            "implementation_scope": {
                "payload_only_schedule_row_candidates": 14,
                "viewer_candidates_requiring_hash_verified_landing_context": 2,
                "required_interface_extension": [
                    "completion_source_context_by_receipt",
                    "viewer_landing_payload_by_receipt",
                ],
                "legacy_nonempty_date_precedence": "PRESERVE",
                "lineage_gate_precedence": "UNCHANGED_AND_STILL_REQUIRED",
            },
            "lineage_invariance": {
                "opendart_required": checks["opendart_non_initial_lineage_required"],
                "opendart_resolved": checks["opendart_non_initial_lineage_resolved"],
                "viewer_required": checks["viewer_non_initial_lineage_required"],
                "viewer_resolved": checks["viewer_non_initial_lineage_resolved"],
                "date_authority_bypasses_lineage": False,
            },
            "capture_status_before": policy.get("capture_status_before"),
            "capture_full_capture_complete_before": policy.get(
                "capture_full_capture_complete_before"
            ),
            "adapter_changed": False,
            "adapter_contract_changed": False,
            "implementation_allowed": False,
            "network_requests": 0,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
        }
    except (DesignValidationError, OSError, UnicodeDecodeError, ValueError, TypeError) as exc:
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_COMPLETION_AUTHORITY_DESIGN_VALIDATION",
            "status": "FAIL",
            "verdict": "FAIL_COMPLETION_AUTHORITY_DESIGN_VALIDATION",
            "reason_codes": [getattr(exc, "code", "DESIGN_VALIDATION_FAILED")],
            "failure_detail": getattr(exc, "detail", str(exc)),
            "generated_at": generated_at,
            "adapter_changed": False,
            "adapter_contract_changed": False,
            "implementation_allowed": False,
            "network_requests": 0,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
        }


def write_design_report(
    report: Mapping[str, Any], *, root: Path = ROOT, contract_path: Path | None = None
) -> tuple[Path, Path]:
    contract = load_design_contract(root.resolve(), contract_path)
    generated_at = str(report.get("generated_at") or "")
    stamp = "".join(character for character in generated_at if character.isdigit())[:14]
    if len(stamp) != 14:
        raise DesignValidationError("REPORT_TIMESTAMP_INVALID")
    output = contract["output"]
    report_dir = root.resolve() / REPORT_DIR_RELATIVE
    versioned = report_dir / f"{output['versioned_report_prefix']}{stamp}.json"
    latest = _resolve(root.resolve(), output["latest_report_path"])
    payload = _json_bytes(report)
    _atomic_write(versioned, payload)
    _atomic_write(latest, payload)
    return versioned, latest


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the design boundary for C8 structured completion-date authority."
    )
    parser.add_argument("--contract", type=Path, default=ROOT / CONTRACT_RELATIVE)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    report = validate_design(root=ROOT, contract_path=args.contract)
    if args.write_report:
        paths = write_design_report(report, root=ROOT, contract_path=args.contract)
        report = {**report, "report_paths": [str(path) for path in paths]}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
