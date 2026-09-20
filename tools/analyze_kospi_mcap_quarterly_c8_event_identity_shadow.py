from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
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
from tools import validate_kospi_mcap_quarterly_c8_dart_viewer_fallback as fallback


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_event_identity_shadow_validation_contract_v1.json"
)
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")


class EventIdentityShadowError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EventIdentityShadowError("JSON_READ_FAILED", str(path)) from exc
    if not isinstance(payload, dict):
        raise EventIdentityShadowError("JSON_OBJECT_REQUIRED", str(path))
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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve(root: Path, value: object) -> Path:
    path = Path(str(value or ""))
    return path if path.is_absolute() else root / path


def load_contract(
    root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    path = contract_path or root / CONTRACT_RELATIVE
    contract = _load_json(path)
    if (
        contract.get("strategy_id") != STRATEGY_ID
        or contract.get("mode") != "VALIDATION_ONLY"
    ):
        raise EventIdentityShadowError("EVENT_IDENTITY_CONTRACT_INVALID")
    permissions = contract.get("execution_permissions")
    if not isinstance(permissions, Mapping) or any(
        value is not False for value in permissions.values()
    ):
        raise EventIdentityShadowError("EVENT_IDENTITY_PERMISSIONS_INVALID")
    matcher = contract.get("matcher")
    if not isinstance(matcher, Mapping):
        raise EventIdentityShadowError("EVENT_IDENTITY_MATCHER_INVALID")
    required_false = (
        "receipt_link_input_allowed",
        "official_family_input_allowed",
        "list_order_input_allowed",
        "nearest_date_input_allowed",
    )
    if any(matcher.get(key) is not False for key in required_false):
        raise EventIdentityShadowError("FORBIDDEN_MATCHER_INPUT_ENABLED")
    if (
        matcher.get("same_stock_only") is not True
        or matcher.get("prior_receipt_only") is not True
        or matcher.get("exact_one_candidate_required") is not True
    ):
        raise EventIdentityShadowError("FAIL_CLOSED_MATCHER_RULE_MISSING")
    return contract


def _normalize_value(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower()
    return re.sub(r"[^0-9a-z가-힣]", "", text)


def _extract_between(text: str, start: str, end_patterns: Sequence[str]) -> str:
    start_index = text.find(start)
    if start_index < 0:
        return ""
    value_start = start_index + len(start)
    tail = text[value_start : value_start + 2500]
    end_indexes = []
    for pattern in end_patterns:
        match = re.search(pattern, tail)
        if match:
            end_indexes.append(match.start())
    value = tail[: min(end_indexes)] if end_indexes else tail[:400]
    return _normalize_value(value)


def extract_event_fields(
    payload: bytes, *, date_patterns: Sequence[str]
) -> tuple[dict[str, str], list[str]]:
    text, reasons = _document_text(payload)
    if reasons:
        return {}, list(reasons)
    counterparty = ""
    counterparty_match = re.search(
        r"합병상대회사.{0,600}?회사명\s*(.{1,160}?)\s*주요사업", text
    )
    if counterparty_match:
        counterparty = _normalize_value(counterparty_match.group(1))
    fields = {
        "counterparty_company": counterparty,
        "merger_method": _extract_between(
            text, "합병방법", (r"\s*2[.]?\s*합병목적",)
        ),
        "merger_ratio": _extract_between(
            text,
            "합병비율",
            (r"\s*4[.]?\s*합병비율\s*산출근거", r"\s*4[.]?\s*산출근거"),
        ),
        "planned_effective_date": _date_after_label(
            text, ("합병기일",), date_patterns
        ),
        "board_resolution_date": _date_after_label(
            text, ("이사회결의일(결정일)", "이사회결의일"), date_patterns
        ),
    }
    return fields, []


def evaluate_candidate(
    event_fields: Mapping[str, str],
    candidate_fields: Mapping[str, str],
    *,
    identity_fields: Sequence[str],
    conflict_fields: Sequence[str],
    minimum_exact_matches: int,
    minimum_identity_matches: int,
) -> dict[str, Any]:
    comparable = [
        field
        for field in event_fields
        if event_fields.get(field) and candidate_fields.get(field)
    ]
    matches = [
        field
        for field in comparable
        if event_fields.get(field) == candidate_fields.get(field)
    ]
    conflicts = [
        field
        for field in conflict_fields
        if event_fields.get(field)
        and candidate_fields.get(field)
        and event_fields.get(field) != candidate_fields.get(field)
    ]
    identity_matches = [field for field in matches if field in set(identity_fields)]
    passed = (
        not conflicts
        and len(matches) >= minimum_exact_matches
        and len(identity_matches) >= minimum_identity_matches
    )
    return {
        "passed": passed,
        "comparable_fields": comparable,
        "matched_fields": matches,
        "identity_matched_fields": identity_matches,
        "conflict_fields": conflicts,
    }


def select_shadow_candidate(
    *,
    event_receipt_no: str,
    event_stock_code: str,
    event_receipt_date: str,
    event_fields: Mapping[str, str],
    candidates: Sequence[Mapping[str, Any]],
    matcher: Mapping[str, Any],
) -> dict[str, Any]:
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
        result = evaluate_candidate(
            event_fields,
            candidate.get("fields") or {},
            identity_fields=matcher["identity_fields"],
            conflict_fields=matcher["conflict_fields"],
            minimum_exact_matches=int(matcher["minimum_exact_matches"]),
            minimum_identity_matches=int(matcher["minimum_identity_matches"]),
        )
        evaluated.append(
            {
                "receipt_no": candidate_receipt,
                **result,
            }
        )
    passed = [row for row in evaluated if row["passed"]]
    if len(passed) == 1:
        classification = "UNIQUE_STRUCTURED_MATCH"
        selected = passed[0]["receipt_no"]
    elif len(passed) > 1:
        classification = "AMBIGUOUS_STRUCTURED_MATCH"
        selected = ""
    else:
        classification = "NO_STRUCTURED_MATCH"
        selected = ""
    return {
        "classification": classification,
        "selected_initial_receipt": selected,
        "passing_candidate_receipts": [row["receipt_no"] for row in passed],
        "candidate_count": len(evaluated),
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
        path = _resolve(root, source[f"{key}_path"])
        actual = _sha256(path)
        expected = str(source[f"{key}_sha256"])
        if actual != expected:
            raise EventIdentityShadowError(
                "INPUT_HASH_MISMATCH", f"{key}: expected={expected}, actual={actual}"
            )
        evidence[key] = {"path": str(path), "sha256": actual}
    return evidence


def analyze_event_identity_shadow(
    *, root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    root = root.resolve()
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        contract = load_contract(root, contract_path)
        input_evidence = _verify_inputs(root, contract)
        source = contract["input"]
        lineage = _load_json(_resolve(root, source["lineage_report_path"]))
        completion_scope = _load_json(
            _resolve(root, source["completion_scope_report_path"])
        )
        fallback_report = _load_json(
            _resolve(root, source["fallback_validation_report_path"])
        )
        if lineage.get("status") != "PASS" or completion_scope.get("status") != "PASS":
            raise EventIdentityShadowError("INPUT_ANALYSIS_NOT_PASS")
        if fallback_report.get("status") != "FAIL":
            raise EventIdentityShadowError("FALLBACK_FAIL_CLOSED_STATE_CHANGED")

        fallback_contract_path = _resolve(
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
                extracted, reasons = extract_event_fields(
                    payload, date_patterns=date_patterns
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
            raise EventIdentityShadowError("INITIAL_CANDIDATE_DUPLICATE")

        population = contract["population"]
        gold_all = [
            row for row in lineage.get("rows", []) if row.get("authority_resolved")
        ]
        if len(gold_all) != int(population["required_official_gold_count_before_exclusion"]):
            raise EventIdentityShadowError(
                "OFFICIAL_GOLD_COUNT_MISMATCH", str(len(gold_all))
            )
        excluded = set(population["exploration_receipts_excluded"])
        gold_rows = [row for row in gold_all if row["receipt_no"] not in excluded]
        if len(gold_rows) != int(population["required_blind_gold_count"]):
            raise EventIdentityShadowError("BLIND_GOLD_COUNT_MISMATCH", str(len(gold_rows)))

        gold_results: list[dict[str, Any]] = []
        outcome_counts: Counter[str] = Counter()
        class_counts: Counter[str] = Counter()
        for row in gold_rows:
            receipt = str(row["receipt_no"])
            diagnostic = diagnostics.get(receipt)
            if not diagnostic:
                raise EventIdentityShadowError("GOLD_DIAGNOSTIC_MISSING", receipt)
            selected = select_shadow_candidate(
                event_receipt_no=receipt,
                event_stock_code=str(row["stock_code"]),
                event_receipt_date=str(row["receipt_date"]),
                event_fields=features(receipt, document_payloads[receipt]),
                candidates=candidates,
                matcher=matcher,
            )
            predicted = selected["selected_initial_receipt"]
            expected = str(row["initial_receipt"])
            outcome = "CORRECT" if predicted == expected else "WRONG" if predicted else "UNRESOLVED"
            outcome_counts[outcome] += 1
            class_counts[selected["classification"]] += 1
            gold_results.append(
                {
                    "receipt_no": receipt,
                    "stock_code": str(row["stock_code"]),
                    "event_kind": str(row["event_kind"]),
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
            raise EventIdentityShadowError(
                "UNRESOLVED_COMPLETION_COUNT_MISMATCH",
                str(len(unresolved_completion_rows)),
            )
        completion_results: list[dict[str, Any]] = []
        completion_classes: Counter[str] = Counter()
        for row in unresolved_completion_rows:
            receipt = str(row["receipt_no"])
            diagnostic = diagnostics.get(receipt)
            if not diagnostic:
                raise EventIdentityShadowError(
                    "COMPLETION_DIAGNOSTIC_MISSING", receipt
                )
            selected = select_shadow_candidate(
                event_receipt_no=receipt,
                event_stock_code=str(row["stock_code"]),
                event_receipt_date=str(row["receipt_date"]),
                event_fields=features(receipt, document_payloads[receipt]),
                candidates=candidates,
                matcher=matcher,
            )
            completion_classes[selected["classification"]] += 1
            completion_results.append(
                {
                    "receipt_no": receipt,
                    "stock_code": str(row["stock_code"]),
                    "authority_resolved": False,
                    "promotion_allowed": False,
                    **selected,
                }
            )

        acceptance = contract["acceptance"]
        acceptance_reasons: list[str] = []
        if wrong != int(acceptance["required_wrong_link_count"]):
            acceptance_reasons.append("WRONG_LINK_COUNT_NOT_ZERO")
        if precision != float(acceptance["required_resolved_precision"]):
            acceptance_reasons.append("RESOLVED_PRECISION_BELOW_REQUIRED")
        if coverage < float(acceptance["minimum_blind_gold_coverage"]):
            acceptance_reasons.append("BLIND_GOLD_COVERAGE_BELOW_MINIMUM")
        if correct < int(acceptance["minimum_correct_link_count"]):
            acceptance_reasons.append("CORRECT_LINK_COUNT_BELOW_MINIMUM")
        if not resolved:
            acceptance_reasons.append("NO_RESOLVED_BLIND_ROWS")
        acceptance_reasons = sorted(set(acceptance_reasons))
        status = "PASS" if not acceptance_reasons else "FAIL"
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_EVENT_IDENTITY_BLIND_SHADOW_VALIDATION",
            "status": status,
            "verdict": (
                "SHADOW_MATCHER_SUPPORTED_NO_POLICY_CHANGE"
                if status == "PASS"
                else "SHADOW_MATCHER_NOT_SUPPORTED_NO_POLICY_CHANGE"
            ),
            "reason_codes": acceptance_reasons,
            "generated_at": generated_at,
            "selection_as_of": manifest["selection_as_of"],
            "input_integrity": {
                **input_evidence,
                **list_evidence,
                **document_evidence,
                "broad_b_supplement": supplement_evidence,
            },
            "population": {
                "official_gold_count_before_exclusion": len(gold_all),
                "exploration_receipts_excluded": sorted(excluded),
                "blind_gold_count": len(gold_rows),
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
                "classification_counts": dict(sorted(class_counts.items())),
                "rows": gold_results,
            },
            "unresolved_completion_projection": {
                "classification_counts": dict(sorted(completion_classes.items())),
                "rows": completion_results,
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
        EventIdentityShadowError,
        fallback.FallbackValidationError,
        OSError,
        ValueError,
        TypeError,
        KeyError,
    ) as exc:
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_EVENT_IDENTITY_BLIND_SHADOW_VALIDATION",
            "status": "FAIL",
            "verdict": "FAIL_EVENT_IDENTITY_SHADOW_VALIDATION",
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
        raise EventIdentityShadowError("REPORT_TIMESTAMP_INVALID")
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
        description="Validate a structured-field C8 event identity matcher without policy changes."
    )
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--contract", type=Path)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    report = analyze_event_identity_shadow(
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
