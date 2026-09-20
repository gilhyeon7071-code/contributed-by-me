from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    load_corporate_action_adapter_contract,
)
from tools import analyze_kospi_mcap_quarterly_c8_dart_viewer_policy_scope as policy_scope
from tools import analyze_kospi_mcap_quarterly_c8_lineage_landing_scope as landing_scope
from tools import validate_kospi_mcap_quarterly_c8_dart_viewer_fallback as fallback


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_lineage_authority_validation_contract_v1.json"
)
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")


class LineageAuthorityError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LineageAuthorityError("JSON_READ_FAILED", str(path)) from exc
    if not isinstance(payload, dict):
        raise LineageAuthorityError("JSON_OBJECT_REQUIRED", str(path))
    return payload


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def load_lineage_authority_contract(
    root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    path = contract_path or root / CONTRACT_RELATIVE
    contract = _load_json(path)
    if contract.get("strategy_id") != STRATEGY_ID or contract.get("mode") != "VALIDATION_ONLY":
        raise LineageAuthorityError("LINEAGE_AUTHORITY_CONTRACT_INVALID")
    permissions = contract.get("execution_permissions")
    if not isinstance(permissions, Mapping) or any(
        value is not False for value in permissions.values()
    ):
        raise LineageAuthorityError("LINEAGE_AUTHORITY_PERMISSIONS_INVALID")
    rules = contract.get("authority_rules")
    if not isinstance(rules, Mapping):
        raise LineageAuthorityError("LINEAGE_AUTHORITY_RULES_INVALID")
    if rules.get("list_history_is_authority") is not False:
        raise LineageAuthorityError("LIST_HISTORY_AUTHORITY_FORBIDDEN")
    if rules.get("completion_family_establishes_initial_decision_lineage") is not False:
        raise LineageAuthorityError("COMPLETION_FAMILY_AUTHORITY_FORBIDDEN")
    return contract


def classify_lineage_authority(
    *,
    source: str,
    event_kind: str,
    direct_initial_receipts: Sequence[str],
    viewer_family_root_receipt: str = "",
    viewer_family_root_is_initial_decision: bool = False,
    viewer_family_valid: bool = False,
    opendart_family_initial_receipt: str = "",
    opendart_family_valid: bool = False,
    prior_initial_receipts: Sequence[str] = (),
    viewer_family_allowed_event_kinds: Sequence[str] = ("REVISION", "CANCELLATION"),
    opendart_family_allowed_event_kinds: Sequence[str] = ("REVISION", "CANCELLATION"),
) -> dict[str, Any]:
    direct = sorted(set(str(item) for item in direct_initial_receipts if item))
    prior = sorted(set(str(item) for item in prior_initial_receipts if item))
    if len(direct) == 1:
        return {
            "classification": "DOCUMENT_EXPLICIT_INITIAL_RECEIPT",
            "authority_resolved": True,
            "initial_receipt": direct[0],
        }
    viewer_allowed = event_kind in set(viewer_family_allowed_event_kinds)
    if (
        source == "VIEWER_FALLBACK"
        and viewer_allowed
        and viewer_family_valid
        and viewer_family_root_is_initial_decision
        and viewer_family_root_receipt
    ):
        return {
            "classification": "DART_VIEWER_OFFICIAL_FAMILY",
            "authority_resolved": True,
            "initial_receipt": viewer_family_root_receipt,
        }
    if (
        source == "OPENDART_PASS"
        and event_kind in set(opendart_family_allowed_event_kinds)
        and opendart_family_valid
        and opendart_family_initial_receipt
    ):
        return {
            "classification": "DART_OPENDART_OFFICIAL_FAMILY",
            "authority_resolved": True,
            "initial_receipt": opendart_family_initial_receipt,
        }
    if len(prior) == 1:
        return {
            "classification": "LIST_HISTORY_UNIQUE_INFERENCE_ONLY",
            "authority_resolved": False,
            "initial_receipt": "",
        }
    if not prior:
        return {
            "classification": "NO_PRIOR_INITIAL_CANDIDATE",
            "authority_resolved": False,
            "initial_receipt": "",
        }
    return {
        "classification": "LIST_HISTORY_AMBIGUOUS_MULTIPLE",
        "authority_resolved": False,
        "initial_receipt": "",
    }


def _assert_expected(actual: Any, expected: Any, code: str) -> None:
    if actual != expected:
        raise LineageAuthorityError(code, f"expected={expected!r}, actual={actual!r}")


def analyze_lineage_authority(
    *, root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    root = root.resolve()
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        contract = load_lineage_authority_contract(root, contract_path)
        fallback_contract_path = fallback._resolve(
            root, contract["input"]["fallback_validation_contract_path"]
        )
        fallback_contract = fallback.load_validation_contract(root, fallback_contract_path)
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
        adapter_contract = load_corporate_action_adapter_contract(root)
        diagnostics, _ = policy_scope._document_diagnostics(
            rows_by_receipt=rows_by_receipt,
            document_payloads=document_payloads,
            status_by_receipt=status_by_receipt,
            adapter_contract=adapter_contract,
        )
        policy_contract_path = fallback._resolve(
            root, contract["input"]["viewer_policy_scope_contract_path"]
        )
        family_report = policy_scope.analyze_policy_scope(
            root=root, contract_path=policy_contract_path
        )
        if family_report.get("status") != "PASS":
            raise LineageAuthorityError("VIEWER_FAMILY_ANALYSIS_NOT_PASS")
        family_rows = {
            row["receipt_no"]: row
            for row in family_report["viewer_official_family_scope"]["receipts"]
        }
        opendart_scope = landing_scope.analyze_scope(root=root)
        if opendart_scope.get("status") != "PASS":
            raise LineageAuthorityError("OPENDART_LANDING_SCOPE_NOT_PASS")
        if opendart_scope.get("selection_as_of") != manifest["selection_as_of"]:
            raise LineageAuthorityError("OPENDART_LANDING_SCOPE_AS_OF_MISMATCH")
        expected = contract["acceptance"]
        for field, expected_field in (
            ("target_receipt_count", "required_opendart_landing_target_count"),
            ("resolved_count", "required_opendart_landing_resolved_count"),
            ("unresolved_count", "required_opendart_landing_unresolved_count"),
        ):
            _assert_expected(
                opendart_scope.get(field),
                expected[expected_field],
                f"OPENDART_LANDING_{field.upper()}_MISMATCH",
            )
        if opendart_scope.get("landing_integrity_pass") is not True:
            raise LineageAuthorityError("OPENDART_LANDING_INTEGRITY_NOT_PASS")
        opendart_family_rows = {
            row["receipt_no"]: row for row in opendart_scope.get("rows", [])
        }

        initial_by_code: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for receipt, diagnostic in diagnostics.items():
            if diagnostic["event_kind"] == "INITIAL_DECISION":
                initial_by_code[diagnostic["stock_code"]].append(
                    (diagnostic["receipt_date"], receipt)
                )
        for values in initial_by_code.values():
            values.sort()

        allowed_kinds = contract["authority_rules"]["viewer_family_allowed_event_kinds"]
        source_counts: Counter[str] = Counter()
        resolved_counts: Counter[str] = Counter()
        unresolved_counts: Counter[str] = Counter()
        classification_counts: dict[str, Counter[str]] = defaultdict(Counter)
        event_kind_counts: dict[str, Counter[str]] = defaultdict(Counter)
        result_rows: list[dict[str, Any]] = []
        viewer_family_root_not_initial_decision = 0
        viewer_completion_unresolved = 0
        non_merger_attachment_noop = 0

        for receipt, diagnostic in sorted(diagnostics.items()):
            event_kind = str(diagnostic["event_kind"])
            if event_kind == "INITIAL_DECISION":
                continue
            source = str(diagnostic["source"])
            prior = [
                candidate
                for receipt_date, candidate in initial_by_code.get(
                    diagnostic["stock_code"], []
                )
                if receipt_date <= diagnostic["receipt_date"]
            ]
            family = family_rows.get(receipt, {})
            opendart_family = opendart_family_rows.get(receipt, {})
            root_receipt = str(family.get("official_family_root_receipt") or "")
            root_diagnostic = diagnostics.get(root_receipt)
            root_landing_family = opendart_family_rows.get(root_receipt, {})
            family_valid = bool(family.get("family_root_valid"))
            family_class = str(family.get("policy_scope_classification") or "")
            family_valid = family_valid and family_class in {
                "DIRECT_FAMILY_STATE_CANDIDATE",
                "ATTACHMENT_NOOP_CANDIDATE",
            }
            root_is_initial_decision = bool(
                root_diagnostic
                and (
                    root_diagnostic["event_kind"] == "INITIAL_DECISION"
                    or root_landing_family.get("event_kind_override")
                    == "INITIAL_DECISION"
                )
            )
            viewer_family_root_not_initial_decision += int(
                source == "VIEWER_FALLBACK"
                and event_kind in set(allowed_kinds)
                and family_valid
                and not root_is_initial_decision
            )
            classified = classify_lineage_authority(
                source=source,
                event_kind=event_kind,
                direct_initial_receipts=diagnostic["explicit_initial_candidates"],
                viewer_family_root_receipt=root_receipt,
                viewer_family_root_is_initial_decision=root_is_initial_decision,
                viewer_family_valid=family_valid,
                opendart_family_initial_receipt=str(
                    opendart_family.get("initial_receipt") or ""
                ),
                opendart_family_valid=bool(
                    opendart_family.get("authority_resolved")
                ),
                prior_initial_receipts=prior,
                viewer_family_allowed_event_kinds=allowed_kinds,
                opendart_family_allowed_event_kinds=contract["authority_rules"][
                    "opendart_landing_family_allowed_event_kinds"
                ],
            )
            classification = classified["classification"]
            source_counts[source] += 1
            classification_counts[source][classification] += 1
            event_kind_counts[source][event_kind] += 1
            if classified["authority_resolved"]:
                resolved_counts[source] += 1
            else:
                unresolved_counts[source] += 1
            if source == "VIEWER_FALLBACK" and event_kind == "COMPLETION":
                viewer_completion_unresolved += int(
                    not classified["authority_resolved"]
                )
            is_non_merger_attachment_noop = bool(
                classified["authority_resolved"]
                and family_class == "ATTACHMENT_NOOP_CANDIDATE"
                and not diagnostic["contains_merger"]
            )
            non_merger_attachment_noop += int(is_non_merger_attachment_noop)
            result_rows.append(
                {
                    "receipt_no": receipt,
                    "source": source,
                    "stock_code": diagnostic["stock_code"],
                    "receipt_date": diagnostic["receipt_date"],
                    "event_kind": event_kind,
                    "contains_merger": diagnostic["contains_merger"],
                    "direct_initial_receipts": diagnostic[
                        "explicit_initial_candidates"
                    ],
                    "viewer_family_root_receipt": root_receipt,
                    "viewer_family_root_event_kind": (
                        root_diagnostic["event_kind"] if root_diagnostic else ""
                    ),
                    "viewer_family_classification": family_class,
                    "opendart_landing_family_classification": str(
                        opendart_family.get("classification") or ""
                    ),
                    "opendart_landing_sha256": str(
                        opendart_family.get("landing_sha256") or ""
                    ),
                    "opendart_landing_reason_codes": list(
                        opendart_family.get("reason_codes") or []
                    ),
                    "opendart_event_kind_override": str(
                        opendart_family.get("event_kind_override") or ""
                    ),
                    "prior_initial_candidate_count": len(prior),
                    "prior_initial_candidates": prior,
                    "classification": classification,
                    "authority_resolved": classified["authority_resolved"],
                    "initial_receipt": classified["initial_receipt"],
                    "non_merger_attachment_noop": is_non_merger_attachment_noop,
                }
            )

        normalized_class_counts = {
            source: dict(sorted(counter.items()))
            for source, counter in sorted(classification_counts.items())
        }
        normalized_event_counts = {
            source: dict(sorted(counter.items()))
            for source, counter in sorted(event_kind_counts.items())
        }
        _assert_expected(
            len(result_rows),
            expected["required_non_initial_count"],
            "NON_INITIAL_COUNT_MISMATCH",
        )
        _assert_expected(
            dict(sorted(source_counts.items())),
            expected["required_source_counts"],
            "SOURCE_COUNTS_MISMATCH",
        )
        _assert_expected(
            {source: resolved_counts[source] for source in sorted(source_counts)},
            expected["required_authority_resolved_counts"],
            "RESOLVED_COUNTS_MISMATCH",
        )
        _assert_expected(
            {source: unresolved_counts[source] for source in sorted(source_counts)},
            expected["required_unresolved_counts"],
            "UNRESOLVED_COUNTS_MISMATCH",
        )
        _assert_expected(
            normalized_class_counts,
            expected["required_classification_counts"],
            "CLASSIFICATION_COUNTS_MISMATCH",
        )
        _assert_expected(
            viewer_family_root_not_initial_decision,
            expected["required_viewer_family_root_not_initial_decision_count"],
            "VIEWER_FAMILY_ROOT_NOT_INITIAL_DECISION_COUNT_MISMATCH",
        )
        _assert_expected(
            viewer_completion_unresolved,
            expected["required_viewer_completion_unresolved_count"],
            "VIEWER_COMPLETION_UNRESOLVED_COUNT_MISMATCH",
        )
        _assert_expected(
            non_merger_attachment_noop,
            expected["required_non_merger_attachment_noop_count"],
            "NON_MERGER_ATTACHMENT_NOOP_COUNT_MISMATCH",
        )

        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_INITIAL_DECISION_LINEAGE_AUTHORITY_VALIDATION",
            "status": "PASS",
            "verdict": "LINEAGE_AUTHORITY_PARTIAL_C8_REMAINS_BLOCKED",
            "reason_codes": [
                "OPENDART_INITIAL_DECISION_LINEAGE_UNRESOLVED",
                "VIEWER_COMPLETION_INITIAL_DECISION_LINEAGE_UNRESOLVED",
            ],
            "generated_at": generated_at,
            "selection_as_of": manifest["selection_as_of"],
            "capture_status_before": capture.get("status"),
            "capture_full_capture_complete_before": capture.get(
                "full_capture_complete"
            ),
            "input_integrity": {**list_evidence, **document_evidence},
            "non_initial_count": len(result_rows),
            "source_counts": dict(sorted(source_counts.items())),
            "authority_resolved_counts": {
                source: resolved_counts[source] for source in sorted(source_counts)
            },
            "unresolved_counts": {
                source: unresolved_counts[source] for source in sorted(source_counts)
            },
            "classification_counts": normalized_class_counts,
            "event_kind_counts": normalized_event_counts,
            "viewer_family_root_not_initial_decision_count": (
                viewer_family_root_not_initial_decision
            ),
            "viewer_completion_unresolved_count": viewer_completion_unresolved,
            "non_merger_attachment_noop_count": non_merger_attachment_noop,
            "opendart_landing_scope": {
                "target_receipt_count": opendart_scope["target_receipt_count"],
                "resolved_count": opendart_scope["resolved_count"],
                "unresolved_count": opendart_scope["unresolved_count"],
                "landing_integrity_pass": opendart_scope["landing_integrity_pass"],
            },
            "rows": result_rows,
            "policy_change_allowed": False,
            "capture_status_changed": False,
            "network_requests": 0,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
        }
    except (
        LineageAuthorityError,
        fallback.FallbackValidationError,
        policy_scope.PolicyScopeError,
    ) as exc:
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_INITIAL_DECISION_LINEAGE_AUTHORITY_VALIDATION",
            "status": "FAIL",
            "verdict": "FAIL_LINEAGE_AUTHORITY_VALIDATION",
            "reason_codes": [getattr(exc, "code", "LINEAGE_AUTHORITY_VALIDATION_FAILED")],
            "failure_detail": getattr(exc, "detail", str(exc)),
            "generated_at": generated_at,
            "policy_change_allowed": False,
            "capture_status_changed": False,
            "network_requests": 0,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
        }


def write_lineage_authority_report(
    report: Mapping[str, Any],
    *,
    root: Path = ROOT,
    contract_path: Path | None = None,
) -> tuple[Path, Path]:
    contract = load_lineage_authority_contract(root.resolve(), contract_path)
    generated_at = str(report.get("generated_at") or "")
    stamp = "".join(character for character in generated_at if character.isdigit())[:14]
    if len(stamp) != 14:
        raise LineageAuthorityError("REPORT_TIMESTAMP_INVALID")
    report_dir = root.resolve() / REPORT_DIR_RELATIVE
    output = contract["output"]
    versioned = report_dir / f"{output['versioned_report_prefix']}{stamp}.json"
    latest = fallback._resolve(root.resolve(), output["latest_report_path"])
    payload = _json_bytes(report)
    _atomic_write(versioned, payload)
    _atomic_write(latest, payload)
    return versioned, latest


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate explicit C8 initial-decision lineage authority without policy changes."
    )
    parser.add_argument("--contract", type=Path, default=ROOT / CONTRACT_RELATIVE)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    report = analyze_lineage_authority(root=ROOT, contract_path=args.contract)
    if args.write_report:
        paths = write_lineage_authority_report(
            report, root=ROOT, contract_path=args.contract
        )
        print(
            json.dumps(
                {"report": report, "report_paths": [str(path) for path in paths]},
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
