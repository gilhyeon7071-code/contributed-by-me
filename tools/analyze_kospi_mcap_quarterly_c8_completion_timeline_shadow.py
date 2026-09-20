from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    _date_after_label,
    _document_text,
    load_corporate_action_adapter_contract,
)
from tools import analyze_kospi_mcap_quarterly_c8_dart_viewer_policy_scope as policy_scope
from tools import analyze_kospi_mcap_quarterly_c8_event_identity_shadow as event_shadow
from tools import validate_kospi_mcap_quarterly_c8_dart_viewer_fallback as fallback


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_completion_timeline_shadow_validation_contract_v1.json"
)
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")


class CompletionTimelineShadowError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def load_contract(
    root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    path = contract_path or root / CONTRACT_RELATIVE
    contract = event_shadow._load_json(path)
    if (
        contract.get("strategy_id") != STRATEGY_ID
        or contract.get("mode") != "VALIDATION_ONLY"
    ):
        raise CompletionTimelineShadowError("COMPLETION_TIMELINE_CONTRACT_INVALID")
    permissions = contract.get("execution_permissions")
    if not isinstance(permissions, Mapping) or any(
        value is not False for value in permissions.values()
    ):
        raise CompletionTimelineShadowError("COMPLETION_TIMELINE_PERMISSIONS_INVALID")
    matcher = contract.get("matcher")
    if not isinstance(matcher, Mapping):
        raise CompletionTimelineShadowError("COMPLETION_TIMELINE_MATCHER_INVALID")
    required_false = (
        "actual_effective_date_input_allowed",
        "planned_effective_date_input_allowed",
        "receipt_link_input_allowed",
        "official_family_input_allowed",
        "list_order_input_allowed",
        "nearest_date_input_allowed",
    )
    if any(matcher.get(key) is not False for key in required_false):
        raise CompletionTimelineShadowError("FORBIDDEN_MATCHER_INPUT_ENABLED")
    required_true = (
        "same_stock_only",
        "prior_receipt_only",
        "both_fields_required",
        "exact_one_candidate_required",
    )
    if any(matcher.get(key) is not True for key in required_true):
        raise CompletionTimelineShadowError("FAIL_CLOSED_MATCHER_RULE_MISSING")
    if matcher.get("fields") != ["board_resolution_date", "merger_contract_date"]:
        raise CompletionTimelineShadowError("TIMELINE_FIELDS_CHANGED")
    if int(matcher.get("required_exact_fields") or 0) != 2:
        raise CompletionTimelineShadowError("TIMELINE_EXACT_FIELD_COUNT_INVALID")
    return contract


def extract_timeline_fields(
    payload: bytes,
    *,
    date_patterns: Sequence[str],
    board_labels: Sequence[str],
    contract_labels: Sequence[str],
) -> tuple[dict[str, str], list[str]]:
    text, reasons = _document_text(payload)
    if reasons:
        return {}, list(reasons)
    return {
        "board_resolution_date": _date_after_label(
            text, list(board_labels), list(date_patterns)
        ),
        "merger_contract_date": _date_after_label(
            text, list(contract_labels), list(date_patterns)
        ),
    }, []


def select_timeline_candidate(
    *,
    event_receipt_no: str,
    event_stock_code: str,
    event_receipt_date: str,
    event_fields: Mapping[str, str],
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    field_names = ("board_resolution_date", "merger_contract_date")
    missing_event_fields = [field for field in field_names if not event_fields.get(field)]
    evaluated: list[dict[str, Any]] = []
    for candidate in candidates:
        if str(candidate.get("stock_code") or "") != event_stock_code:
            continue
        candidate_receipt = str(candidate.get("receipt_no") or "")
        candidate_date = str(candidate.get("receipt_date") or "")
        if (candidate_date, candidate_receipt) >= (
            event_receipt_date,
            event_receipt_no,
        ):
            continue
        candidate_fields = candidate.get("fields") or {}
        missing_candidate_fields = [
            field for field in field_names if not candidate_fields.get(field)
        ]
        matched_fields = [
            field
            for field in field_names
            if event_fields.get(field)
            and candidate_fields.get(field)
            and event_fields.get(field) == candidate_fields.get(field)
        ]
        conflict_fields = [
            field
            for field in field_names
            if event_fields.get(field)
            and candidate_fields.get(field)
            and event_fields.get(field) != candidate_fields.get(field)
        ]
        passed = (
            not missing_event_fields
            and not missing_candidate_fields
            and not conflict_fields
            and len(matched_fields) == len(field_names)
        )
        evaluated.append(
            {
                "receipt_no": candidate_receipt,
                "passed": passed,
                "matched_fields": matched_fields,
                "conflict_fields": conflict_fields,
                "missing_candidate_fields": missing_candidate_fields,
            }
        )
    passed_rows = [row for row in evaluated if row["passed"]]
    if missing_event_fields:
        classification = "EVENT_TIMELINE_FIELDS_MISSING"
        selected = ""
    elif len(passed_rows) == 1:
        classification = "UNIQUE_TIMELINE_MATCH"
        selected = passed_rows[0]["receipt_no"]
    elif len(passed_rows) > 1:
        classification = "AMBIGUOUS_TIMELINE_MATCH"
        selected = ""
    else:
        classification = "NO_TIMELINE_MATCH"
        selected = ""
    return {
        "classification": classification,
        "selected_initial_receipt": selected,
        "passing_candidate_receipts": [row["receipt_no"] for row in passed_rows],
        "candidate_count": len(evaluated),
        "missing_event_fields": missing_event_fields,
        "candidate_evaluations": evaluated,
    }


def _verify_inputs(root: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    source = contract["input"]
    evidence: dict[str, Any] = {}
    for key in (
        "lineage_report",
        "completion_scope_report",
        "broad_b_supplement_report",
        "fallback_validation_report",
    ):
        path = event_shadow._resolve(root, source[f"{key}_path"])
        actual = event_shadow._sha256(path)
        expected = str(source[f"{key}_sha256"])
        if actual != expected:
            raise CompletionTimelineShadowError(
                "INPUT_HASH_MISMATCH", f"{key}: expected={expected}, actual={actual}"
            )
        evidence[key] = {"path": str(path), "sha256": actual}
    return evidence


def analyze_completion_timeline_shadow(
    *, root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    root = root.resolve()
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        contract = load_contract(root, contract_path)
        input_evidence = _verify_inputs(root, contract)
        source = contract["input"]
        lineage = event_shadow._load_json(
            event_shadow._resolve(root, source["lineage_report_path"])
        )
        completion_scope = event_shadow._load_json(
            event_shadow._resolve(root, source["completion_scope_report_path"])
        )
        fallback_report = event_shadow._load_json(
            event_shadow._resolve(root, source["fallback_validation_report_path"])
        )
        if lineage.get("status") != "PASS" or completion_scope.get("status") != "PASS":
            raise CompletionTimelineShadowError("INPUT_ANALYSIS_NOT_PASS")
        if fallback_report.get("status") != "FAIL":
            raise CompletionTimelineShadowError("FALLBACK_FAIL_CLOSED_STATE_CHANGED")

        fallback_contract_path = event_shadow._resolve(
            root, source["fallback_validation_contract_path"]
        )
        fallback_contract = fallback.load_validation_contract(
            root, fallback_contract_path
        )
        manifest, capture, status_by_receipt = fallback._validate_capture_context(
            root=root, contract=fallback_contract
        )
        _, rows_by_receipt, list_evidence = fallback._load_list_pages(
            root=root, contract=fallback_contract, manifest=manifest
        )
        document_payloads, _, document_evidence = fallback._load_document_payloads(
            root=root,
            contract=fallback_contract,
            manifest=manifest,
            status_by_receipt=status_by_receipt,
        )
        supplement_rows, supplement_documents, _, supplement_evidence = (
            fallback._load_broad_b_supplement(root=root, contract=fallback_contract)
        )
        adapter_contract = load_corporate_action_adapter_contract(root)
        diagnostics, _ = policy_scope._document_diagnostics(
            rows_by_receipt=rows_by_receipt,
            document_payloads=document_payloads,
            status_by_receipt=status_by_receipt,
            adapter_contract=adapter_contract,
        )
        lineage_rows = {
            str(row["receipt_no"]): row for row in lineage.get("rows", [])
        }
        matcher = contract["matcher"]
        date_patterns = adapter_contract["document_rules"]["date_patterns"]
        feature_cache: dict[str, dict[str, str]] = {}
        feature_errors: dict[str, list[str]] = {}

        def features(receipt: str, payload: bytes) -> dict[str, str]:
            if receipt not in feature_cache:
                extracted, reasons = extract_timeline_fields(
                    payload,
                    date_patterns=date_patterns,
                    board_labels=matcher["board_resolution_date_labels"],
                    contract_labels=matcher["merger_contract_date_labels"],
                )
                feature_cache[receipt] = extracted
                if reasons:
                    feature_errors[receipt] = reasons
            return feature_cache[receipt]

        candidates: list[dict[str, Any]] = []
        for receipt, diagnostic in sorted(diagnostics.items()):
            lineage_row = lineage_rows.get(receipt, {})
            event_kind = str(
                lineage_row.get("opendart_event_kind_override")
                or diagnostic["event_kind"]
            )
            if event_kind != "INITIAL_DECISION":
                continue
            candidates.append(
                {
                    "receipt_no": receipt,
                    "stock_code": diagnostic["stock_code"],
                    "receipt_date": diagnostic["receipt_date"],
                    "fields": features(receipt, document_payloads[receipt]),
                    "source": diagnostic["source"],
                }
            )

        revision_prefixes = tuple(matcher["supplement_revision_prefixes"])
        for row in supplement_rows:
            report_name = str(row.get("report_nm") or "").strip()
            if report_name.startswith(revision_prefixes):
                continue
            receipt = str(row["rcept_no"])
            candidates.append(
                {
                    "receipt_no": receipt,
                    "stock_code": str(row["stock_code"]),
                    "receipt_date": str(row["rcept_dt"]),
                    "fields": features(receipt, supplement_documents[receipt]),
                    "source": "BROAD_B_LEGACY_SUPPLEMENT",
                }
            )
        candidate_receipts = [row["receipt_no"] for row in candidates]
        if len(candidate_receipts) != len(set(candidate_receipts)):
            raise CompletionTimelineShadowError("INITIAL_CANDIDATE_DUPLICATE")

        population = contract["population"]
        resolved_completion_rows = [
            row
            for row in completion_scope.get("rows", [])
            if row.get("authority_resolved")
        ]
        if len(resolved_completion_rows) != int(
            population["required_resolved_completion_gold_count_before_exclusion"]
        ):
            raise CompletionTimelineShadowError(
                "RESOLVED_COMPLETION_GOLD_COUNT_MISMATCH",
                str(len(resolved_completion_rows)),
            )
        prior_event_identity_receipts = {
            str(row["receipt_no"])
            for row in lineage.get("rows", [])
            if row.get("authority_resolved")
        }
        completion_gold_receipts = {
            str(row["receipt_no"]) for row in resolved_completion_rows
        }
        overlap = prior_event_identity_receipts & completion_gold_receipts
        if len(overlap) != int(
            population["required_overlap_with_prior_event_identity_gold"]
        ):
            raise CompletionTimelineShadowError(
                "PRIOR_GOLD_OVERLAP_MISMATCH", str(len(overlap))
            )
        excluded = set(population["exploration_receipts_excluded"])
        gold_rows = [
            row
            for row in resolved_completion_rows
            if str(row["receipt_no"]) not in excluded
        ]
        if len(gold_rows) != int(population["required_blind_gold_count"]):
            raise CompletionTimelineShadowError(
                "BLIND_GOLD_COUNT_MISMATCH", str(len(gold_rows))
            )

        outcome_counts: Counter[str] = Counter()
        classification_counts: Counter[str] = Counter()
        gold_results: list[dict[str, Any]] = []
        for row in gold_rows:
            receipt = str(row["receipt_no"])
            diagnostic = diagnostics.get(receipt)
            if not diagnostic:
                raise CompletionTimelineShadowError(
                    "GOLD_DIAGNOSTIC_MISSING", receipt
                )
            selected = select_timeline_candidate(
                event_receipt_no=receipt,
                event_stock_code=str(row["stock_code"]),
                event_receipt_date=str(row["receipt_date"]),
                event_fields=features(receipt, document_payloads[receipt]),
                candidates=candidates,
            )
            predicted = selected["selected_initial_receipt"]
            expected = str(row["initial_receipt"])
            outcome = (
                "CORRECT"
                if predicted == expected
                else "WRONG"
                if predicted
                else "UNRESOLVED"
            )
            outcome_counts[outcome] += 1
            classification_counts[selected["classification"]] += 1
            gold_results.append(
                {
                    "receipt_no": receipt,
                    "stock_code": str(row["stock_code"]),
                    "official_initial_receipt": expected,
                    "outcome": outcome,
                    **selected,
                }
            )

        correct = outcome_counts["CORRECT"]
        wrong = outcome_counts["WRONG"]
        resolved = correct + wrong
        precision = correct / resolved if resolved else 0.0
        coverage = correct / len(gold_rows) if gold_rows else 0.0

        unresolved_completion_rows = [
            row
            for row in completion_scope.get("rows", [])
            if not row.get("authority_resolved")
        ]
        if len(unresolved_completion_rows) != int(
            population["required_unresolved_completion_count"]
        ):
            raise CompletionTimelineShadowError(
                "UNRESOLVED_COMPLETION_COUNT_MISMATCH",
                str(len(unresolved_completion_rows)),
            )
        projection_results: list[dict[str, Any]] = []
        projection_classes: Counter[str] = Counter()
        for row in unresolved_completion_rows:
            receipt = str(row["receipt_no"])
            diagnostic = diagnostics.get(receipt)
            if not diagnostic:
                raise CompletionTimelineShadowError(
                    "COMPLETION_DIAGNOSTIC_MISSING", receipt
                )
            selected = select_timeline_candidate(
                event_receipt_no=receipt,
                event_stock_code=str(row["stock_code"]),
                event_receipt_date=str(row["receipt_date"]),
                event_fields=features(receipt, document_payloads[receipt]),
                candidates=candidates,
            )
            projection_classes[selected["classification"]] += 1
            projection_results.append(
                {
                    "receipt_no": receipt,
                    "stock_code": str(row["stock_code"]),
                    "authority_resolved": False,
                    "promotion_allowed": False,
                    **selected,
                }
            )

        acceptance = contract["acceptance"]
        reason_codes: list[str] = []
        if wrong != int(acceptance["required_wrong_link_count"]):
            reason_codes.append("WRONG_LINK_COUNT_NOT_ZERO")
        if precision != float(acceptance["required_resolved_precision"]):
            reason_codes.append("RESOLVED_PRECISION_BELOW_REQUIRED")
        if coverage < float(acceptance["minimum_blind_gold_coverage"]):
            reason_codes.append("BLIND_GOLD_COVERAGE_BELOW_MINIMUM")
        if correct < int(acceptance["minimum_correct_link_count"]):
            reason_codes.append("CORRECT_LINK_COUNT_BELOW_MINIMUM")
        if not resolved:
            reason_codes.append("NO_RESOLVED_BLIND_ROWS")
        reason_codes = sorted(set(reason_codes))
        status = "PASS" if not reason_codes else "FAIL"
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_COMPLETION_TIMELINE_BLIND_SHADOW_VALIDATION",
            "status": status,
            "verdict": (
                "COMPLETION_TIMELINE_MATCHER_SUPPORTED_NO_POLICY_CHANGE"
                if status == "PASS"
                else "COMPLETION_TIMELINE_MATCHER_NOT_SUPPORTED_NO_POLICY_CHANGE"
            ),
            "reason_codes": reason_codes,
            "generated_at": generated_at,
            "selection_as_of": manifest["selection_as_of"],
            "input_integrity": {
                **input_evidence,
                **list_evidence,
                **document_evidence,
                "broad_b_supplement": supplement_evidence,
            },
            "population": {
                "resolved_completion_gold_count_before_exclusion": len(
                    resolved_completion_rows
                ),
                "exploration_receipts_excluded": sorted(excluded),
                "blind_gold_count": len(gold_rows),
                "prior_event_identity_gold_overlap_count": len(overlap),
                "unresolved_completion_count": len(unresolved_completion_rows),
                "initial_candidate_count": len(candidates),
                "feature_error_count": len(feature_errors),
                "feature_errors": feature_errors,
            },
            "blind_gold": {
                "correct_link_count": correct,
                "wrong_link_count": wrong,
                "unresolved_count": outcome_counts["UNRESOLVED"],
                "resolved_count": resolved,
                "precision": precision,
                "coverage": coverage,
                "classification_counts": dict(sorted(classification_counts.items())),
                "rows": gold_results,
            },
            "unresolved_completion_projection": {
                "classification_counts": dict(sorted(projection_classes.items())),
                "rows": projection_results,
                "authority_granted_count": 0,
                "intervals_created": 0,
            },
            "policy_change_allowed": False,
            "adapter_changed": False,
            "capture_status_before": capture.get("status"),
            "capture_status_changed": False,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
            "network_requests": 0,
        }
    except (
        CompletionTimelineShadowError,
        fallback.FallbackValidationError,
        OSError,
        ValueError,
        TypeError,
        KeyError,
    ) as exc:
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_COMPLETION_TIMELINE_BLIND_SHADOW_VALIDATION",
            "status": "FAIL",
            "verdict": "FAIL_COMPLETION_TIMELINE_SHADOW_VALIDATION",
            "reason_codes": [getattr(exc, "code", type(exc).__name__)],
            "failure_detail": getattr(exc, "detail", str(exc)),
            "generated_at": generated_at,
            "policy_change_allowed": False,
            "adapter_changed": False,
            "capture_status_changed": False,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
            "network_requests": 0,
        }


def write_report(
    report: Mapping[str, Any],
    *,
    root: Path = ROOT,
    contract_path: Path | None = None,
) -> tuple[Path, Path]:
    contract = load_contract(root.resolve(), contract_path)
    generated_at = str(report.get("generated_at") or "")
    stamp = "".join(character for character in generated_at if character.isdigit())[:14]
    if len(stamp) != 14:
        raise CompletionTimelineShadowError("REPORT_TIMESTAMP_INVALID")
    output = contract["output"]
    report_dir = root.resolve() / REPORT_DIR_RELATIVE
    versioned = report_dir / f"{output['versioned_report_prefix']}{stamp}.json"
    latest = event_shadow._resolve(root.resolve(), output["latest_report_path"])
    payload = event_shadow._json_bytes(report)
    event_shadow._atomic_write(versioned, payload)
    event_shadow._atomic_write(latest, payload)
    return versioned, latest


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate completion timeline identity without policy changes."
    )
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--contract", type=Path)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    report = analyze_completion_timeline_shadow(
        root=args.root,
        contract_path=args.contract,
    )
    if args.write_report:
        versioned, latest = write_report(
            report,
            root=args.root,
            contract_path=args.contract,
        )
        report = {**report, "report_paths": [str(versioned), str(latest)]}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
