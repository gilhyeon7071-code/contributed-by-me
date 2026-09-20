from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timedelta
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
from tools import capture_kospi_mcap_quarterly_c8_kind_stock_issues as capture
from tools import validate_kospi_mcap_quarterly_c8_dart_viewer_fallback as fallback


CONTRACT_RELATIVE = capture.CONTRACT_RELATIVE
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")


class KindShadowError(RuntimeError):
    pass


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise KindShadowError(f"JSON_OBJECT_REQUIRED:{path}")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_contract(root: Path = ROOT) -> dict[str, Any]:
    contract = capture.load_contract(root)
    matcher = contract["matcher"]
    forbidden = (
        "actual_effective_date_input_allowed",
        "planned_effective_date_input_allowed",
        "receipt_link_input_allowed",
        "official_family_input_allowed",
        "list_order_input_allowed",
        "nearest_date_input_allowed",
        "fuzzy_company_name_allowed",
    )
    if any(matcher.get(key) is not False for key in forbidden):
        raise KindShadowError("KIND_FORBIDDEN_MATCHER_INPUT_ENABLED")
    required = (
        "same_stock_only",
        "prior_receipt_only",
        "exact_one_kind_row_required",
        "exact_one_initial_decision_required",
        "exact_date_match_required",
    )
    if any(matcher.get(key) is not True for key in required):
        raise KindShadowError("KIND_FAIL_CLOSED_MATCHER_RULE_MISSING")
    return contract


def extract_scheduled_listing_date(
    payload: bytes, *, labels: Sequence[str], date_patterns: Sequence[str]
) -> str:
    text, reasons = _document_text(payload)
    if reasons:
        return ""
    return _date_after_label(text, list(labels), list(date_patterns))


def select_kind_candidate(
    *,
    event_receipt_no: str,
    stock_code: str,
    candidate_receipts: Sequence[str],
    candidate_listing_dates: Mapping[str, str],
    kind_rows: Sequence[Mapping[str, Any]],
    max_days: int,
    merger_substring: str,
) -> dict[str, Any]:
    start = datetime.strptime(event_receipt_no[:8], "%Y%m%d").date()
    end = start + timedelta(days=max_days)
    eligible_kind = []
    for row in kind_rows:
        if str(row.get("stock_code") or "") != stock_code:
            continue
        if merger_substring not in str(row.get("issue_reason") or ""):
            continue
        try:
            listing = datetime.strptime(str(row.get("listing_date") or ""), "%Y%m%d").date()
        except ValueError:
            continue
        if start <= listing <= end:
            eligible_kind.append(dict(row))
    if not eligible_kind:
        return {
            "classification": "NO_KIND_MERGER_ROW",
            "selected_initial_receipt": "",
            "eligible_kind_rows": [],
            "matching_initial_receipts": [],
        }
    if len(eligible_kind) > 1:
        return {
            "classification": "AMBIGUOUS_KIND_MERGER_ROWS",
            "selected_initial_receipt": "",
            "eligible_kind_rows": eligible_kind,
            "matching_initial_receipts": [],
        }
    listing_date = str(eligible_kind[0]["listing_date"])
    matches = [
        receipt
        for receipt in candidate_receipts
        if candidate_listing_dates.get(receipt) == listing_date
    ]
    if len(matches) == 1:
        classification = "UNIQUE_KIND_LISTING_MATCH"
        selected = matches[0]
    elif len(matches) > 1:
        classification = "AMBIGUOUS_INITIAL_DECISIONS"
        selected = ""
    elif any(candidate_listing_dates.get(receipt) for receipt in candidate_receipts):
        classification = "NO_INITIAL_LISTING_DATE_MATCH"
        selected = ""
    else:
        classification = "INITIAL_LISTING_DATE_MISSING"
        selected = ""
    return {
        "classification": classification,
        "selected_initial_receipt": selected,
        "eligible_kind_rows": eligible_kind,
        "matching_initial_receipts": matches,
    }


def diagnose_kind_merger_availability(
    *,
    event_receipt_no: str,
    stock_code: str,
    kind_rows: Sequence[Mapping[str, Any]],
    max_days: int,
    merger_substring: str,
) -> dict[str, Any]:
    event_date = datetime.strptime(event_receipt_no[:8], "%Y%m%d").date()
    signed_day_offsets: list[int] = []
    for row in kind_rows:
        if str(row.get("stock_code") or "") != stock_code:
            continue
        if merger_substring not in str(row.get("issue_reason") or ""):
            continue
        try:
            listing_date = datetime.strptime(
                str(row.get("listing_date") or ""), "%Y%m%d"
            ).date()
        except ValueError:
            continue
        signed_day_offsets.append((listing_date - event_date).days)

    if not signed_day_offsets:
        classification = "NO_MERGER_ROW_ANYWHERE"
        nearest_signed_days = None
    else:
        nearest_signed_days = min(
            signed_day_offsets, key=lambda value: (abs(value), value)
        )
        if any(0 <= value <= max_days for value in signed_day_offsets):
            classification = "FORWARD_WINDOW_MERGER_ROW_PRESENT"
        elif any(-max_days <= value < 0 for value in signed_day_offsets):
            classification = "MERGER_ROW_BEFORE_COMPLETION_WITHIN_WINDOW"
        else:
            classification = "NO_MERGER_ROW_WITHIN_ABSOLUTE_WINDOW"

    return {
        "classification": classification,
        "valid_merger_row_count": len(signed_day_offsets),
        "nearest_signed_days": nearest_signed_days,
        "window_days": max_days,
    }


def _load_kind_rows(
    root: Path, contract: Mapping[str, Any]
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    manifest_path = root / contract["acquisition"]["latest_manifest_path"]
    manifest = _load_json(manifest_path)
    if manifest.get("status") != "PASS":
        raise KindShadowError("KIND_CAPTURE_MANIFEST_NOT_PASS")
    if manifest.get("contract_sha256") != _sha256(root / CONTRACT_RELATIVE):
        raise KindShadowError("KIND_CAPTURE_CONTRACT_HASH_MISMATCH")
    expected_codes = capture.frozen_stock_codes(root, contract)
    entries = manifest.get("captures") or []
    if [row.get("stock_code") for row in entries] != expected_codes:
        raise KindShadowError("KIND_CAPTURE_CODE_SET_MISMATCH")
    rows_by_code: dict[str, list[dict[str, Any]]] = {}
    merger_total = 0
    for entry in entries:
        code = str(entry["stock_code"])
        raw_path = root / entry["raw_path"]
        if _sha256(raw_path) != entry["raw_sha256"]:
            raise KindShadowError(f"KIND_RAW_HASH_MISMATCH:{code}")
        rows = capture.parse_issue_rows(raw_path.read_bytes())
        if len(rows) != int(entry["row_count"]):
            raise KindShadowError(f"KIND_RAW_ROW_COUNT_MISMATCH:{code}")
        for row in rows:
            row["stock_code"] = code
        rows_by_code[code] = rows
        merger_total += sum(
            contract["matcher"]["issue_reason_required_substring"] in row["issue_reason"]
            for row in rows
        )
    if merger_total != int(manifest["merger_row_count"]):
        raise KindShadowError("KIND_MERGER_ROW_COUNT_MISMATCH")
    return rows_by_code, {
        "manifest_path": str(manifest_path),
        "manifest_sha256": _sha256(manifest_path),
        "captured_code_count": len(entries),
        "total_row_count": manifest["total_row_count"],
        "merger_row_count": merger_total,
        "network_request_count": manifest["network_request_count"],
    }


def _load_document_payloads(
    root: Path, contract: Mapping[str, Any]
) -> tuple[dict[str, bytes], dict[str, Any]]:
    source = contract["input"]
    for key in (
        "timeline_report",
        "lineage_report",
        "completion_scope_report",
        "broad_b_supplement_report",
        "fallback_validation_report",
    ):
        path = root / source[f"{key}_path"]
        if _sha256(path) != source[f"{key}_sha256"]:
            raise KindShadowError(f"KIND_INPUT_HASH_MISMATCH:{key}")
    fallback_contract = fallback.load_validation_contract(
        root, root / source["fallback_validation_contract_path"]
    )
    manifest, _, status_by_receipt = fallback._validate_capture_context(
        root=root, contract=fallback_contract
    )
    document_payloads, _, main_evidence = fallback._load_document_payloads(
        root=root,
        contract=fallback_contract,
        manifest=manifest,
        status_by_receipt=status_by_receipt,
    )
    _, supplement_documents, _, supplement_evidence = fallback._load_broad_b_supplement(
        root=root, contract=fallback_contract
    )
    payloads = dict(document_payloads)
    payloads.update(supplement_documents)
    return payloads, {
        "main_document_count": len(document_payloads),
        "supplement_document_count": len(supplement_documents),
        "main_evidence": main_evidence,
        "supplement_evidence": supplement_evidence,
    }


def assess_source_ceiling(
    *, population_count: int, source_ceiling_count: int, minimum_correct_link_count: int
) -> dict[str, Any]:
    shortfall = max(0, minimum_correct_link_count - source_ceiling_count)
    status = "PASS" if shortfall == 0 else "FAIL"
    return {
        "status": status,
        "verdict": (
            "KIND_PRE_CAPTURE_SOURCE_CEILING_SUFFICIENT"
            if status == "PASS"
            else "KIND_PRE_CAPTURE_SOURCE_CEILING_BELOW_ACCEPTANCE_MINIMUM"
        ),
        "reason_codes": (
            []
            if status == "PASS"
            else ["DART_INITIAL_LISTING_DATE_CEILING_BELOW_MINIMUM_CORRECT_LINK_COUNT"]
        ),
        "population_count": population_count,
        "source_ceiling_count": source_ceiling_count,
        "source_ceiling_coverage": (
            source_ceiling_count / population_count if population_count else 0.0
        ),
        "minimum_correct_link_count": minimum_correct_link_count,
        "shortfall_count": shortfall,
    }


def required_input_shortfall_attribution(
    *, dart_available_count: int, kind_available_count: int, minimum_correct_count: int
) -> str:
    dart_short = dart_available_count < minimum_correct_count
    kind_short = kind_available_count < minimum_correct_count
    if dart_short and kind_short:
        return "DUAL_REQUIRED_INPUT_SHORTFALL"
    if dart_short:
        return "DART_REQUIRED_INPUT_SHORTFALL"
    if kind_short:
        return "KIND_REQUIRED_INPUT_SHORTFALL"
    return "NO_REQUIRED_INPUT_SHORTFALL"


def evaluate_pre_capture_feasibility(*, root: Path = ROOT) -> dict[str, Any]:
    root = root.resolve()
    contract = load_contract(root)
    timeline_path = root / contract["input"]["timeline_report_path"]
    if _sha256(timeline_path) != contract["input"]["timeline_report_sha256"]:
        raise KindShadowError("KIND_INPUT_HASH_MISMATCH:timeline_report")
    timeline = _load_json(timeline_path)
    blind_rows = timeline["blind_gold"]["rows"]
    required_population = int(contract["population"]["required_blind_gold_count"])
    if len(blind_rows) != required_population:
        raise KindShadowError("KIND_BLIND_POPULATION_MISMATCH")

    payloads, document_evidence = _load_document_payloads(root, contract)
    adapter_contract = load_corporate_action_adapter_contract(root)
    matcher = contract["matcher"]
    date_patterns = adapter_contract["document_rules"]["date_patterns"]
    listing_cache: dict[str, str] = {}

    for row in blind_rows:
        receipt = str(row["official_initial_receipt"])
        if receipt in listing_cache:
            continue
        payload = payloads.get(receipt)
        listing_cache[receipt] = (
            extract_scheduled_listing_date(
                payload,
                labels=matcher["scheduled_listing_date_labels"],
                date_patterns=date_patterns,
            )
            if payload
            else ""
        )

    source_ceiling_count = sum(
        bool(listing_cache.get(str(row["official_initial_receipt"])))
        for row in blind_rows
    )
    assessment = assess_source_ceiling(
        population_count=len(blind_rows),
        source_ceiling_count=source_ceiling_count,
        minimum_correct_link_count=int(contract["acceptance"]["minimum_correct_link_count"]),
    )
    return {
        "strategy_id": contract["strategy_id"],
        "scope": "C8_KIND_STOCK_ISSUE_PRE_CAPTURE_FEASIBILITY",
        **assessment,
        "source_ceiling_basis": "official_initial_decision_with_scheduled_listing_date",
        "input_integrity": {
            "contract_sha256": _sha256(root / CONTRACT_RELATIVE),
            "timeline_report_sha256": _sha256(timeline_path),
            "documents": document_evidence,
        },
        "network_requests": 0,
        "artifact_writes": 0,
        "adapter_changed": False,
        "policy_change_allowed": False,
        "canonical_publish_allowed": False,
        "m2_allowed": False,
        "candidate_selection_allowed": False,
        "orders_allowed": False,
        "operational_change": False,
    }


def analyze(*, root: Path = ROOT) -> dict[str, Any]:
    root = root.resolve()
    contract = load_contract(root)
    timeline = _load_json(root / contract["input"]["timeline_report_path"])
    if len(timeline["blind_gold"]["rows"]) != contract["population"]["required_blind_gold_count"]:
        raise KindShadowError("KIND_BLIND_POPULATION_MISMATCH")
    if len(timeline["unresolved_completion_projection"]["rows"]) != contract["population"]["required_unresolved_completion_count"]:
        raise KindShadowError("KIND_PROJECTION_POPULATION_MISMATCH")
    rows_by_code, kind_evidence = _load_kind_rows(root, contract)
    payloads, document_evidence = _load_document_payloads(root, contract)
    adapter_contract = load_corporate_action_adapter_contract(root)
    matcher = contract["matcher"]
    date_patterns = adapter_contract["document_rules"]["date_patterns"]
    listing_cache: dict[str, str] = {}

    def listing_date(receipt: str) -> str:
        if receipt not in listing_cache:
            payload = payloads.get(receipt)
            listing_cache[receipt] = (
                extract_scheduled_listing_date(
                    payload,
                    labels=matcher["scheduled_listing_date_labels"],
                    date_patterns=date_patterns,
                )
                if payload
                else ""
            )
        return listing_cache[receipt]

    def score(row: Mapping[str, Any]) -> dict[str, Any]:
        candidates = [
            str(value["receipt_no"])
            for value in row.get("candidate_evaluations", [])
        ]
        candidate_dates = {receipt: listing_date(receipt) for receipt in candidates}
        selected = select_kind_candidate(
            event_receipt_no=str(row["receipt_no"]),
            stock_code=str(row["stock_code"]),
            candidate_receipts=candidates,
            candidate_listing_dates=candidate_dates,
            kind_rows=rows_by_code.get(str(row["stock_code"]), []),
            max_days=int(matcher["completion_to_listing_max_calendar_days"]),
            merger_substring=str(matcher["issue_reason_required_substring"]),
        )
        availability = diagnose_kind_merger_availability(
            event_receipt_no=str(row["receipt_no"]),
            stock_code=str(row["stock_code"]),
            kind_rows=rows_by_code.get(str(row["stock_code"]), []),
            max_days=int(matcher["completion_to_listing_max_calendar_days"]),
            merger_substring=str(matcher["issue_reason_required_substring"]),
        )
        return {
            "receipt_no": str(row["receipt_no"]),
            "stock_code": str(row["stock_code"]),
            "candidate_listing_dates": candidate_dates,
            "kind_merger_availability": availability,
            **selected,
        }

    counts: Counter[str] = Counter()
    classes: Counter[str] = Counter()
    no_kind_decomposition: Counter[str] = Counter()
    official_initial_listing_date_available = 0
    exact_one_forward_kind_row_count = 0
    gold_rows: list[dict[str, Any]] = []
    for row in timeline["blind_gold"]["rows"]:
        result = score(row)
        predicted = result["selected_initial_receipt"]
        expected = str(row["official_initial_receipt"])
        official_initial_date_available = bool(listing_date(expected))
        outcome = "CORRECT" if predicted == expected else "WRONG" if predicted else "UNRESOLVED"
        counts[outcome] += 1
        classes[result["classification"]] += 1
        official_initial_listing_date_available += int(official_initial_date_available)
        exact_one_forward_kind_row_count += int(len(result["eligible_kind_rows"]) == 1)
        if result["classification"] == "NO_KIND_MERGER_ROW":
            no_kind_decomposition[
                result["kind_merger_availability"]["classification"]
            ] += 1
        gold_rows.append(
            {
                "official_initial_receipt": expected,
                "official_initial_listing_date_available": official_initial_date_available,
                "outcome": outcome,
                **result,
            }
        )
    correct = counts["CORRECT"]
    wrong = counts["WRONG"]
    resolved = correct + wrong
    precision = correct / resolved if resolved else 0.0
    coverage = correct / len(gold_rows) if gold_rows else 0.0
    acceptance = contract["acceptance"]
    reasons = []
    if wrong != int(acceptance["required_wrong_link_count"]):
        reasons.append("KIND_WRONG_LINK_COUNT_NOT_ZERO")
    if precision < float(acceptance["required_resolved_precision"]):
        reasons.append("KIND_RESOLVED_PRECISION_BELOW_MINIMUM")
    if coverage < float(acceptance["minimum_blind_gold_coverage"]):
        reasons.append("KIND_BLIND_GOLD_COVERAGE_BELOW_MINIMUM")
    if correct < int(acceptance["minimum_correct_link_count"]):
        reasons.append("KIND_CORRECT_LINK_COUNT_BELOW_MINIMUM")
    projection_rows = [score(row) for row in timeline["unresolved_completion_projection"]["rows"]]
    projection_classes = Counter(row["classification"] for row in projection_rows)
    status = "PASS" if not reasons else "FAIL"
    minimum_correct = int(acceptance["minimum_correct_link_count"])
    return {
        "strategy_id": contract["strategy_id"],
        "scope": "C8_KIND_STOCK_ISSUE_SHADOW_VALIDATION",
        "status": status,
        "verdict": "KIND_STOCK_ISSUE_MATCHER_SUPPORTED_SHADOW_ONLY" if status == "PASS" else "KIND_STOCK_ISSUE_MATCHER_NOT_SUPPORTED_NO_POLICY_CHANGE",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "reason_codes": reasons,
        "input_integrity": {
            "contract_sha256": _sha256(root / CONTRACT_RELATIVE),
            "kind_capture": kind_evidence,
            "documents": document_evidence,
        },
        "blind_gold": {
            "population_count": len(gold_rows),
            "correct_link_count": correct,
            "wrong_link_count": wrong,
            "unresolved_count": counts["UNRESOLVED"],
            "resolved_count": resolved,
            "precision": precision,
            "coverage": coverage,
            "classification_counts": dict(sorted(classes.items())),
            "rows": gold_rows,
        },
        "bottleneck_diagnostics": {
            "attribution": required_input_shortfall_attribution(
                dart_available_count=official_initial_listing_date_available,
                kind_available_count=exact_one_forward_kind_row_count,
                minimum_correct_count=minimum_correct,
            ),
            "dart_official_initial_listing_date": {
                "available_count": official_initial_listing_date_available,
                "missing_count": len(gold_rows) - official_initial_listing_date_available,
                "minimum_correct_link_count": minimum_correct,
                "shortfall_count": max(
                    0, minimum_correct - official_initial_listing_date_available
                ),
            },
            "kind_exact_one_forward_window_merger_row": {
                "available_count": exact_one_forward_kind_row_count,
                "missing_count": len(gold_rows) - exact_one_forward_kind_row_count,
                "minimum_correct_link_count": minimum_correct,
                "shortfall_count": max(
                    0, minimum_correct - exact_one_forward_kind_row_count
                ),
            },
            "resolved_exact_date_match": {
                "count": resolved,
                "correct_count": correct,
                "wrong_count": wrong,
                "minimum_correct_link_count": minimum_correct,
                "shortfall_count": max(0, minimum_correct - resolved),
            },
            "no_kind_merger_row_decomposition": {
                "population_count": classes["NO_KIND_MERGER_ROW"],
                "classification_counts": dict(sorted(no_kind_decomposition.items())),
            },
        },
        "unresolved_completion_projection": {
            "classification_counts": dict(sorted(projection_classes.items())),
            "authority_granted_count": 0,
            "intervals_created": 0,
            "rows": projection_rows,
        },
        "network_requests": 0,
        "adapter_changed": False,
        "policy_change_allowed": False,
        "capture_status_changed": False,
        "canonical_publish_allowed": False,
        "m2_allowed": False,
        "candidate_selection_allowed": False,
        "orders_allowed": False,
        "operational_change": False,
    }


def save_report(report: Mapping[str, Any], *, root: Path = ROOT) -> tuple[Path, Path]:
    contract = load_contract(root)
    output = contract["output"]
    report_dir = root / REPORT_DIR_RELATIVE
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    versioned = report_dir / f"{output['versioned_report_prefix']}{stamp}.json"
    latest = root / output["latest_report_path"]
    encoded = (json.dumps(report, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    versioned.write_bytes(encoded)
    latest.write_bytes(encoded)
    return versioned, latest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args()
    report = analyze()
    if not args.no_write:
        versioned, latest = save_report(report)
        report["written_paths"] = [str(versioned), str(latest)]
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
