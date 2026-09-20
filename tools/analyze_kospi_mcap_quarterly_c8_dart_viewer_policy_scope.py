from __future__ import annotations

import argparse
import html
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    _date_after_label,
    _document_receipt_references,
    _document_text,
    load_corporate_action_adapter_contract,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.contracts import ContractError, normalize_ymd
from tools import validate_kospi_mcap_quarterly_c8_dart_viewer_fallback as fallback


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_dart_viewer_policy_scope_contract_v1.json"
)
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")
FAMILY_SELECT_PATTERN = re.compile(
    r"<select\b[^>]*\bid\s*=\s*['\"]family['\"][^>]*>(?P<body>.*?)</select>",
    re.IGNORECASE | re.DOTALL,
)
OPTION_PATTERN = re.compile(
    r"<option\b(?P<attrs>[^>]*)>(?P<label>.*?)</option>",
    re.IGNORECASE | re.DOTALL,
)
VALUE_PATTERN = re.compile(r"\bvalue\s*=\s*['\"](?P<value>[^'\"]+)['\"]", re.IGNORECASE)
TITLE_PATTERN = re.compile(r"\btitle\s*=\s*['\"](?P<title>[^'\"]*)['\"]", re.IGNORECASE)
RECEIPT_PATTERN = re.compile(r"^[0-9]{14}$")


class PolicyScopeError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyScopeError("JSON_READ_FAILED", str(path)) from exc
    if not isinstance(payload, dict):
        raise PolicyScopeError("JSON_OBJECT_REQUIRED", str(path))
    return payload


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def load_policy_scope_contract(
    root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    path = contract_path or root / CONTRACT_RELATIVE
    contract = _load_json(path)
    if contract.get("strategy_id") != STRATEGY_ID or contract.get("mode") != "VALIDATION_ONLY":
        raise PolicyScopeError("POLICY_SCOPE_CONTRACT_INVALID")
    permissions = contract.get("execution_permissions")
    if not isinstance(permissions, Mapping) or any(value is not False for value in permissions.values()):
        raise PolicyScopeError("POLICY_SCOPE_PERMISSIONS_INVALID")
    return contract


def _decode_html(payload: bytes) -> str:
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", errors="replace")


def parse_official_family_references(
    payload: bytes, *, correction_markers: list[str] | None = None
) -> list[dict[str, Any]]:
    markers = correction_markers or ["정정", "첨부추가"]
    text = _decode_html(payload)
    match = FAMILY_SELECT_PATTERN.search(text)
    if not match:
        return []
    references: list[dict[str, Any]] = []
    seen: set[str] = set()
    for option in OPTION_PATTERN.finditer(match.group("body")):
        value_match = VALUE_PATTERN.search(option.group("attrs"))
        if not value_match:
            continue
        query = parse_qs(html.unescape(value_match.group("value")), keep_blank_values=True)
        receipt = str(query.get("rcpNo", [""])[0])
        if not RECEIPT_PATTERN.fullmatch(receipt) or receipt in seen:
            continue
        seen.add(receipt)
        label = re.sub(r"<[^>]+>", " ", option.group("label"))
        label = " ".join(html.unescape(label).split())
        title_match = TITLE_PATTERN.search(option.group(0))
        title = html.unescape(title_match.group("title")).strip() if title_match else ""
        references.append(
            {
                "receipt_no": receipt,
                "label": label,
                "title": title,
                "selected": bool(re.search(r"\bselected\b", option.group("attrs"), re.IGNORECASE)),
                "is_correction": any(marker in label for marker in markers),
            }
        )
    return references


def nearest_date_after_label(text: str, labels: list[str], patterns: list[str]) -> str:
    nearest: list[tuple[int, str]] = []
    for label in labels:
        start = 0
        while True:
            index = text.find(label, start)
            if index < 0:
                break
            window = text[index + len(label) : index + len(label) + 120]
            for pattern in patterns:
                for match in re.finditer(pattern, window):
                    try:
                        normalized = normalize_ymd(
                            f"{int(match.group(1)):04d}{int(match.group(2)):02d}{int(match.group(3)):02d}"
                        )
                    except (ContractError, IndexError, ValueError):
                        continue
                    nearest.append((match.start(), normalized))
            start = index + len(label)
    if not nearest:
        return ""
    minimum_distance = min(distance for distance, _ in nearest)
    dates = {date for distance, date in nearest if distance == minimum_distance}
    return next(iter(dates)) if len(dates) == 1 else ""


def _event_kind(
    row: Mapping[str, str], text: str, adapter_contract: Mapping[str, Any]
) -> str:
    prefixes = adapter_contract["document_rules"]["revision_report_prefixes"]
    is_revision = any(row["report_nm"].strip().startswith(prefix) for prefix in prefixes)
    if row["series"] == "MERGER_COMPLETION_HISTORY":
        return "COMPLETION"
    if not is_revision:
        return "INITIAL_DECISION"
    if any(
        phrase in text for phrase in adapter_contract["document_rules"]["cancellation_phrases"]
    ):
        return "CANCELLATION"
    return "REVISION"


def is_attachment_correction(report_name: object) -> bool:
    return "[첨부정정]" in re.sub(r"\s+", "", str(report_name or ""))


def _document_diagnostics(
    *,
    rows_by_receipt: Mapping[str, Mapping[str, str]],
    document_payloads: Mapping[str, bytes],
    status_by_receipt: Mapping[str, str],
    adapter_contract: Mapping[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    rules = adapter_contract["document_rules"]
    prefixes = rules["revision_report_prefixes"]
    initial_by_code: dict[str, set[str]] = {}
    for receipt, row in rows_by_receipt.items():
        if row["series"] != "MERGER_DECISION_HISTORY":
            continue
        if not any(row["report_nm"].strip().startswith(prefix) for prefix in prefixes):
            initial_by_code.setdefault(row["stock_code"], set()).add(receipt)

    diagnostics: dict[str, dict[str, Any]] = {}
    summary_by_source: dict[str, Counter[str]] = {
        "OPENDART_PASS": Counter(),
        "VIEWER_FALLBACK": Counter(),
    }
    failure_receipts_by_source: dict[str, list[str]] = {
        "OPENDART_PASS": [],
        "VIEWER_FALLBACK": [],
    }
    for receipt, row in sorted(rows_by_receipt.items()):
        source = "OPENDART_PASS" if status_by_receipt[receipt] == "PASS" else "VIEWER_FALLBACK"
        text, text_reasons = _document_text(document_payloads[receipt])
        kind = _event_kind(row, text, adapter_contract)
        references = _document_receipt_references(
            document_payloads[receipt], rules["explicit_lineage_patterns"]
        )
        references.discard(receipt)
        candidates = sorted(references & initial_by_code.get(row["stock_code"], set()))
        current_date = _date_after_label(text, rules["actual_effective_date_labels"], rules["date_patterns"])
        nearest_date = nearest_date_after_label(
            text, rules["actual_effective_date_labels"], rules["date_patterns"]
        )
        reasons = list(text_reasons)
        if "합병" not in text:
            reasons.append("DART_DOCUMENT_NOT_COMPANY_MERGER")
        if kind != "INITIAL_DECISION" and len(candidates) != 1:
            reasons.append("EVENT_IDENTITY_UNRESOLVED")
        if kind == "COMPLETION" and not current_date:
            reasons.append("COMPLETION_ACTUAL_DATE_MISSING_CURRENT_PARSER")
        reasons = sorted(set(reasons))
        counter = summary_by_source[source]
        counter["document_count"] += 1
        counter[f"event_kind_{kind}"] += 1
        counter["contains_merger_count"] += int("합병" in text)
        counter["explicit_lineage_required_count"] += int(kind != "INITIAL_DECISION")
        counter["explicit_lineage_resolved_count"] += int(
            kind != "INITIAL_DECISION" and len(candidates) == 1
        )
        counter["initial_self_count"] += int(kind == "INITIAL_DECISION")
        counter["completion_count"] += int(kind == "COMPLETION")
        counter["completion_date_current_count"] += int(kind == "COMPLETION" and bool(current_date))
        counter["completion_date_nearest_count"] += int(kind == "COMPLETION" and bool(nearest_date))
        counter["completion_date_parser_false_missing_count"] += int(
            kind == "COMPLETION" and not current_date and bool(nearest_date)
        )
        counter["current_contract_fail_count"] += int(bool(reasons))
        if reasons:
            failure_receipts_by_source[source].append(receipt)
        diagnostics[receipt] = {
            "receipt_no": receipt,
            "source": source,
            "stock_code": row["stock_code"],
            "series": row["series"],
            "report_name": row["report_nm"],
            "receipt_date": row["rcept_dt"],
            "event_kind": kind,
            "text_nonempty": bool(text),
            "contains_merger": "합병" in text,
            "explicit_initial_candidates": candidates,
            "current_actual_date": current_date if kind == "COMPLETION" else "",
            "nearest_actual_date": nearest_date if kind == "COMPLETION" else "",
            "current_reason_codes": reasons,
        }
    summary = {
        source: {
            **dict(sorted(counter.items())),
            "failure_receipts": failure_receipts_by_source[source],
        }
        for source, counter in summary_by_source.items()
    }
    return diagnostics, summary


def _family_scope_analysis(
    *,
    root: Path,
    policy_contract: Mapping[str, Any],
    fallback_contract: Mapping[str, Any],
    rows_by_receipt: Mapping[str, Mapping[str, str]],
    document_payloads: Mapping[str, bytes],
    status_by_receipt: Mapping[str, str],
    diagnostics: Mapping[str, Mapping[str, Any]],
    adapter_contract: Mapping[str, Any],
) -> dict[str, Any]:
    shadow_dir = fallback._resolve(root, fallback_contract["input"]["shadow_pass_journal_dir"])
    markers = [str(item) for item in policy_contract["family_rules"]["correction_markers"]]
    rules = adapter_contract["document_rules"]
    rows: list[dict[str, Any]] = []
    class_counts: Counter[str] = Counter()
    overlap_counts: Counter[str] = Counter()
    root_source_counts: Counter[str] = Counter()
    for receipt, source_status in sorted(status_by_receipt.items()):
        if source_status != "UNAVAILABLE":
            continue
        current = diagnostics[receipt]
        shadow = _load_json(shadow_dir / f"{receipt}.json")
        landing = shadow.get("landing")
        if not isinstance(landing, Mapping):
            raise PolicyScopeError("PRIMARY_LANDING_MISSING", receipt)
        landing_payload = fallback._read_verified_artifact(
            root=root,
            path_value=landing.get("path"),
            expected_sha256=landing.get("sha256"),
            expected_bytes=landing.get("bytes"),
        )
        family = parse_official_family_references(
            landing_payload, correction_markers=markers
        )
        selected = [item["receipt_no"] for item in family if item["selected"]]
        originals = [item["receipt_no"] for item in family if not item["is_correction"]]
        root_candidates = []
        for candidate in originals:
            row = rows_by_receipt.get(candidate)
            if (
                row
                and row["stock_code"] == current["stock_code"]
                and row["series"] == current["series"]
            ):
                root_candidates.append(candidate)
        root_candidates = sorted(set(root_candidates))
        root_receipt = root_candidates[0] if len(root_candidates) == 1 else ""
        root_diag = diagnostics.get(root_receipt) if root_receipt else None
        family_evidence_reasons: list[str] = []
        if len(originals) != 1:
            family_evidence_reasons.append("FAMILY_ORIGINAL_OPTION_NOT_EXACT_ONE")
        if len(root_candidates) != 1:
            family_evidence_reasons.append("FAMILY_ROOT_NOT_ACCEPTED_SAME_SERIES")
        if root_diag is None:
            family_evidence_reasons.append("FAMILY_ROOT_DIAGNOSTIC_MISSING")
        elif root_diag["receipt_date"] > current["receipt_date"]:
            family_evidence_reasons.append("FAMILY_ROOT_AFTER_CURRENT_RECEIPT")
        family_root_valid = (
            len(originals) == 1
            and len(root_candidates) == 1
            and root_diag is not None
            and root_diag["receipt_date"] <= current["receipt_date"]
        )
        selected_current_match = len(selected) == 1 and selected[0] == receipt
        attachment_correction = is_attachment_correction(current["report_name"])
        if not attachment_correction and not selected_current_match:
            family_evidence_reasons.append("FAMILY_SELECTED_RECEIPT_NOT_CURRENT")

        if (
            attachment_correction
            and family_root_valid
            and root_diag.get("contains_merger")
        ):
            classification = "ATTACHMENT_NOOP_CANDIDATE"
        elif (
            current["event_kind"] == "INITIAL_DECISION"
            and family_root_valid
            and selected_current_match
            and root_receipt == receipt
        ):
            classification = "INITIAL_SELF_CANDIDATE"
        elif current["event_kind"] == "COMPLETION":
            root_date = root_diag.get("nearest_actual_date", "") if root_diag else ""
            if (
                family_root_valid
                and selected_current_match
                and root_diag.get("contains_merger")
                and root_date
            ):
                classification = "COMPLETION_FAMILY_ONLY_BLOCKED"
            else:
                classification = "FAMILY_EVIDENCE_INVALID_BLOCKED"
        elif (
            family_root_valid
            and selected_current_match
            and current["contains_merger"]
            and root_diag.get("contains_merger")
        ):
            classification = "DIRECT_FAMILY_STATE_CANDIDATE"
        else:
            classification = "FAMILY_EVIDENCE_INVALID_BLOCKED"
        class_counts[classification] += 1
        if root_receipt:
            root_source_counts[diagnostics[root_receipt]["source"]] += 1
        for code in current["current_reason_codes"]:
            overlap_counts[code] += 1
        rows.append(
            {
                **current,
                "official_family_receipts": [item["receipt_no"] for item in family],
                "official_family_selected_receipts": selected,
                "official_family_original_receipts": originals,
                "accepted_same_series_root_candidates": root_candidates,
                "official_family_root_receipt": root_receipt,
                "official_family_root_source": (
                    diagnostics[root_receipt]["source"] if root_receipt else ""
                ),
                "official_family_root_contains_merger": (
                    bool(root_diag.get("contains_merger")) if root_diag else False
                ),
                "official_family_root_nearest_actual_date": (
                    str(root_diag.get("nearest_actual_date") or "") if root_diag else ""
                ),
                "family_root_valid": family_root_valid,
                "selected_current_match": selected_current_match,
                "attachment_correction": attachment_correction,
                "family_evidence_reason_codes": sorted(set(family_evidence_reasons)),
                "policy_scope_classification": classification,
                "promotion_allowed": False,
            }
        )
    candidate_classes = {
        "INITIAL_SELF_CANDIDATE",
        "DIRECT_FAMILY_STATE_CANDIDATE",
        "ATTACHMENT_NOOP_CANDIDATE",
    }
    return {
        "receipt_count": len(rows),
        "classification_counts": dict(sorted(class_counts.items())),
        "current_failure_overlap_counts": dict(sorted(overlap_counts.items())),
        "official_family_root_source_counts": dict(sorted(root_source_counts.items())),
        "without_inference_policy_candidate_count": sum(
            count for name, count in class_counts.items() if name in candidate_classes
        ),
        "still_blocked_count": sum(
            count for name, count in class_counts.items() if name not in candidate_classes
        ),
        "receipts": rows,
    }


def analyze_policy_scope(
    *, root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    root = root.resolve()
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        policy_contract = load_policy_scope_contract(root, contract_path)
        fallback_contract_path = fallback._resolve(
            root, policy_contract["fallback_validation_contract_path"]
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
        if set(document_payloads) != set(rows_by_receipt):
            raise PolicyScopeError("DOCUMENT_RECEIPT_SET_MISMATCH")
        adapter_contract = load_corporate_action_adapter_contract(root)
        diagnostics, baseline = _document_diagnostics(
            rows_by_receipt=rows_by_receipt,
            document_payloads=document_payloads,
            status_by_receipt=status_by_receipt,
            adapter_contract=adapter_contract,
        )
        family_scope = _family_scope_analysis(
            root=root,
            policy_contract=policy_contract,
            fallback_contract=fallback_contract,
            rows_by_receipt=rows_by_receipt,
            document_payloads=document_payloads,
            status_by_receipt=status_by_receipt,
            diagnostics=diagnostics,
            adapter_contract=adapter_contract,
        )
        opendart_fail = int(baseline["OPENDART_PASS"].get("current_contract_fail_count", 0))
        still_blocked = int(family_scope["still_blocked_count"])
        reasons: list[str] = []
        if opendart_fail:
            reasons.append("EXISTING_OPENDART_DOCUMENTS_FAIL_CURRENT_ADAPTER")
        if still_blocked:
            reasons.append("VIEWER_FALLBACK_SCOPE_REMAINS_BLOCKED")
        if baseline["OPENDART_PASS"].get("completion_date_parser_false_missing_count", 0) or baseline[
            "VIEWER_FALLBACK"
        ].get("completion_date_parser_false_missing_count", 0):
            reasons.append("COMPLETION_DATE_PARSER_FALSE_MISSING_CONFIRMED")
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_DART_VIEWER_OFFICIAL_FAMILY_POLICY_SCOPE_ANALYSIS",
            "status": "PASS",
            "verdict": "POLICY_SCOPE_ANALYSIS_COMPLETE_POLICY_CHANGE_NOT_ALLOWED",
            "reason_codes": sorted(reasons),
            "generated_at": generated_at,
            "selection_as_of": manifest["selection_as_of"],
            "capture_status_before": capture.get("status"),
            "capture_full_capture_complete_before": capture.get("full_capture_complete"),
            "input_integrity": {**list_evidence, **document_evidence},
            "current_adapter_baseline": baseline,
            "viewer_official_family_scope": family_scope,
            "policy_change_allowed": False,
            "capture_status_changed": False,
            "network_requests": 0,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
        }
    except (PolicyScopeError, fallback.FallbackValidationError) as exc:
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_DART_VIEWER_OFFICIAL_FAMILY_POLICY_SCOPE_ANALYSIS",
            "status": "FAIL",
            "verdict": "FAIL_POLICY_SCOPE_ANALYSIS",
            "reason_codes": [getattr(exc, "code", "POLICY_SCOPE_ANALYSIS_FAILED")],
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


def write_policy_scope_report(
    report: Mapping[str, Any], *, root: Path = ROOT, contract_path: Path | None = None
) -> tuple[Path, Path]:
    contract = load_policy_scope_contract(root.resolve(), contract_path)
    generated_at = str(report.get("generated_at") or "")
    stamp = "".join(character for character in generated_at if character.isdigit())[:14]
    if len(stamp) != 14:
        raise PolicyScopeError("REPORT_TIMESTAMP_INVALID")
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
        description="Analyze DART viewer official family evidence without changing C8 policy."
    )
    parser.add_argument("--contract", type=Path, default=ROOT / CONTRACT_RELATIVE)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    report = analyze_policy_scope(root=ROOT, contract_path=args.contract)
    if args.write_report:
        paths = write_policy_scope_report(report, root=ROOT, contract_path=args.contract)
        print(json.dumps({"report": report, "report_paths": [str(path) for path in paths]}, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
