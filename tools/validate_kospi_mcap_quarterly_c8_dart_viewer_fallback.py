from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import sys
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    _date_after_label,
    _document_receipt_references,
    _document_text,
    _structured_completion_date,
    build_corporate_action_adapter_output,
    build_list_request_spec,
    load_corporate_action_adapter_contract,
    parse_disclosure_list_page,
)


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_dart_viewer_fallback_validation_contract_v1.json"
)
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")
RECEIPT_PATTERN = re.compile(r"^[0-9]{14}$")


class FallbackValidationError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FallbackValidationError("JSON_READ_FAILED", str(path)) from exc
    if not isinstance(payload, dict):
        raise FallbackValidationError("JSON_OBJECT_REQUIRED", str(path))
    return payload


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise FallbackValidationError("PATH_OUTSIDE_ROOT", str(resolved)) from exc
    return resolved


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def _latest_attempt(record: object, *, identity: str) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise FallbackValidationError("MANIFEST_RECORD_INVALID", identity)
    attempts = record.get("attempts")
    if not isinstance(attempts, list) or not attempts or not isinstance(attempts[-1], Mapping):
        raise FallbackValidationError("MANIFEST_ATTEMPT_MISSING", identity)
    return dict(attempts[-1])


def _normalized_receipts(value: object) -> list[str]:
    if isinstance(value, list):
        receipts = [str(item).strip() for item in value]
    elif isinstance(value, str):
        receipts = value.split()
    else:
        raise FallbackValidationError("ACCEPTED_RECEIPTS_INVALID")
    if len(receipts) != len(set(receipts)) or any(
        not RECEIPT_PATTERN.fullmatch(receipt) for receipt in receipts
    ):
        raise FallbackValidationError("ACCEPTED_RECEIPTS_INVALID")
    return sorted(receipts)


def load_validation_contract(
    root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    path = contract_path or root / CONTRACT_RELATIVE
    contract = _load_json(path)
    if (
        contract.get("strategy_id") != STRATEGY_ID
        or contract.get("mode") != "VALIDATION_ONLY"
    ):
        raise FallbackValidationError("FALLBACK_VALIDATION_CONTRACT_INVALID")
    permissions = contract.get("execution_permissions")
    if not isinstance(permissions, Mapping) or any(value is not False for value in permissions.values()):
        raise FallbackValidationError("FALLBACK_VALIDATION_PERMISSIONS_INVALID")
    return contract


def _read_verified_artifact(
    *, root: Path, path_value: object, expected_sha256: object, expected_bytes: object | None = None
) -> bytes:
    path = _resolve(root, str(path_value or ""))
    if not path.is_file():
        raise FallbackValidationError("RAW_ARTIFACT_MISSING", str(path))
    payload = path.read_bytes()
    if expected_bytes is not None and len(payload) != int(expected_bytes):
        raise FallbackValidationError("RAW_ARTIFACT_LENGTH_MISMATCH", str(path))
    if _sha256(payload) != str(expected_sha256 or ""):
        raise FallbackValidationError("RAW_ARTIFACT_HASH_MISMATCH", str(path))
    return payload


def build_deterministic_viewer_package(
    *, root: Path, receipt: str, shadow_journal: Mapping[str, Any]
) -> tuple[bytes, dict[str, Any]]:
    if not RECEIPT_PATTERN.fullmatch(receipt):
        raise FallbackValidationError("FALLBACK_RECEIPT_INVALID", receipt)
    if shadow_journal.get("receipt_no") != receipt:
        raise FallbackValidationError("SHADOW_RECEIPT_MISMATCH", receipt)
    documents = shadow_journal.get("documents")
    if not isinstance(documents, list) or not documents:
        raise FallbackValidationError("SHADOW_DOCUMENTS_MISSING", receipt)

    normalized: list[tuple[int, str, str, bytes, str]] = []
    seen_indexes: set[int] = set()
    for record in documents:
        if not isinstance(record, Mapping) or record.get("kind") != "VIEWER_DOCUMENT":
            raise FallbackValidationError("SHADOW_DOCUMENT_RECORD_INVALID", receipt)
        request = record.get("request")
        if not isinstance(request, Mapping) or request.get("rcpNo") != receipt:
            raise FallbackValidationError("SHADOW_DOCUMENT_RECEIPT_MISMATCH", receipt)
        try:
            index = int(record.get("index"))
        except (TypeError, ValueError) as exc:
            raise FallbackValidationError("SHADOW_DOCUMENT_INDEX_INVALID", receipt) from exc
        dcm_no = str(request.get("dcmNo") or "")
        ele_id = str(request.get("eleId") or "")
        if index < 1 or index in seen_indexes or not dcm_no.isdigit() or not ele_id.isdigit():
            raise FallbackValidationError("SHADOW_DOCUMENT_IDENTITY_INVALID", receipt)
        seen_indexes.add(index)
        payload = _read_verified_artifact(
            root=root,
            path_value=record.get("path"),
            expected_sha256=record.get("sha256"),
            expected_bytes=record.get("bytes"),
        )
        if int(record.get("http_status", 0)) != 200:
            raise FallbackValidationError("SHADOW_DOCUMENT_HTTP_NOT_200", receipt)
        normalized.append((index, dcm_no, ele_id, payload, str(record.get("sha256"))))

    output = io.BytesIO()
    member_names: list[str] = []
    document_hashes: list[str] = []
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_STORED) as archive:
        for index, dcm_no, ele_id, payload, payload_hash in sorted(normalized):
            name = f"{index:03d}_{dcm_no}_{ele_id}.html"
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            info.create_system = 3
            info.external_attr = 0o600 << 16
            archive.writestr(info, payload)
            member_names.append(name)
            document_hashes.append(payload_hash)
    package = output.getvalue()
    return package, {
        "receipt_no": receipt,
        "member_count": len(member_names),
        "member_names": member_names,
        "document_sha256s": document_hashes,
        "package_bytes": len(package),
        "package_sha256": _sha256(package),
        "included_artifact_kind": "VIEWER_DOCUMENT",
        "landing_artifacts_included": False,
        "persisted": False,
    }


def _validate_capture_context(
    *, root: Path, contract: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    source = contract["input"]
    manifest = _load_json(_resolve(root, source["manifest_path"]))
    capture = _load_json(_resolve(root, source["capture_status_path"]))
    if capture.get("status") != source["required_capture_status"]:
        raise FallbackValidationError("CAPTURE_STATUS_NOT_BLOCKED")
    if source["required_capture_reason"] not in (capture.get("reason_codes") or []):
        raise FallbackValidationError("CAPTURE_UNAVAILABLE_REASON_MISSING")
    if manifest.get("full_capture_complete") is not source["required_manifest_full_capture_complete"]:
        raise FallbackValidationError("MANIFEST_FULL_CAPTURE_STATE_MISMATCH")

    checkpoint = manifest.get("checkpoint")
    documents = checkpoint.get("documents") if isinstance(checkpoint, Mapping) else None
    if not isinstance(documents, Mapping):
        raise FallbackValidationError("MANIFEST_DOCUMENTS_MISSING")
    status_by_receipt: dict[str, str] = {}
    for receipt, record in documents.items():
        if not RECEIPT_PATTERN.fullmatch(str(receipt)):
            raise FallbackValidationError("MANIFEST_DOCUMENT_RECEIPT_INVALID", str(receipt))
        status_by_receipt[str(receipt)] = str(
            _latest_attempt(record, identity=str(receipt)).get("status") or ""
        )
    counts = Counter(status_by_receipt.values())
    expected = {
        "PASS": int(source["expected_opendart_pass_count"]),
        "UNAVAILABLE": int(source["expected_opendart_unavailable_count"]),
    }
    if len(status_by_receipt) != int(source["expected_document_count"]) or counts != expected:
        raise FallbackValidationError(
            "MANIFEST_DOCUMENT_STATUS_COUNT_MISMATCH", f"actual={dict(counts)}"
        )
    unavailable = sorted(
        receipt for receipt, status in status_by_receipt.items() if status == "UNAVAILABLE"
    )
    capture_unavailable = [str(item) for item in (capture.get("unavailable_receipts") or [])]
    if unavailable != capture_unavailable or int(capture.get("unavailable_document_count", -1)) != len(
        unavailable
    ):
        raise FallbackValidationError("CAPTURE_MANIFEST_UNAVAILABLE_SET_MISMATCH")
    return manifest, capture, status_by_receipt


def _load_corp_package(
    *, root: Path, contract: Mapping[str, Any]
) -> tuple[bytes, dict[str, Any]]:
    report = _load_json(_resolve(root, contract["input"]["corp_code_report_path"]))
    if report.get("status") != "PASS" or report.get("package_parse_status") != "PASS":
        raise FallbackValidationError("CORP_CODE_PACKAGE_REPORT_NOT_PASS")
    payload = _read_verified_artifact(
        root=root,
        path_value=report.get("raw_path"),
        expected_sha256=report.get("raw_sha256"),
        expected_bytes=report.get("raw_size_bytes"),
    )
    return payload, report


def _load_list_pages(
    *, root: Path, contract: Mapping[str, Any], manifest: Mapping[str, Any]
) -> tuple[dict[str, bytes], dict[str, dict[str, str]], dict[str, Any]]:
    checkpoint = manifest["checkpoint"]
    list_records = checkpoint.get("list_pages")
    if not isinstance(list_records, Mapping):
        raise FallbackValidationError("MANIFEST_LIST_PAGES_MISSING")
    journal_dir = _resolve(root, contract["input"]["list_journal_dir"])
    query_start = manifest.get("query_start")
    query_end = manifest.get("query_end")
    payloads: dict[str, bytes] = {}
    rows_by_receipt: dict[str, dict[str, str]] = {}
    raw_hashes: list[str] = []
    for manifest_key, record in sorted(list_records.items()):
        parts = str(manifest_key).split(":")
        if len(parts) != 3 or not parts[2].isdigit():
            raise FallbackValidationError("MANIFEST_LIST_KEY_INVALID", str(manifest_key))
        code, series, page_text = parts
        page_no = int(page_text)
        latest = _latest_attempt(record, identity=str(manifest_key))
        journal_path = journal_dir / f"{code}_{series}_p{page_no:04d}.json"
        journal = _load_json(journal_path)
        request = journal.get("request")
        if not isinstance(request, Mapping):
            raise FallbackValidationError("LIST_JOURNAL_REQUEST_MISSING", str(journal_path))
        expected_identity = (code, series, page_no)
        actual_identity = (
            str(request.get("code") or ""),
            str(request.get("series") or ""),
            int(request.get("page_no", 0)),
        )
        if actual_identity != expected_identity or journal.get("status") != "PASS":
            raise FallbackValidationError("LIST_JOURNAL_IDENTITY_MISMATCH", str(journal_path))
        if any(
            str(actual) != str(latest.get(field))
            for field, actual in (
                ("status", journal.get("status")),
                ("corp_code", request.get("corp_code")),
                ("series", request.get("series")),
                ("page_no", request.get("page_no")),
                ("payload_sha256", journal.get("payload_sha256")),
            )
        ):
            raise FallbackValidationError("LIST_JOURNAL_MANIFEST_MISMATCH", str(journal_path))
        journal_receipts = _normalized_receipts(journal.get("accepted_receipts"))
        manifest_receipts = _normalized_receipts(latest.get("accepted_receipts"))
        if journal_receipts != manifest_receipts:
            raise FallbackValidationError("LIST_JOURNAL_RECEIPTS_MISMATCH", str(journal_path))
        payload = _read_verified_artifact(
            root=root,
            path_value=journal.get("raw_path"),
            expected_sha256=journal.get("payload_sha256"),
        )
        corp_code = str(request.get("corp_code") or "")
        adapter_key = f"{corp_code}:{series}:{page_no}"
        if adapter_key in payloads:
            raise FallbackValidationError("LIST_ADAPTER_KEY_DUPLICATE", adapter_key)
        spec = build_list_request_spec(
            corp_code=corp_code,
            query_start=query_start,
            query_end=query_end,
            series=series,
            page_no=page_no,
            repo_root=root,
        )
        parsed = parse_disclosure_list_page(
            payload, request_spec=spec, expected_stock_code=code, repo_root=root
        )
        if parsed["status"] != "PASS":
            raise FallbackValidationError(
                "LIST_RAW_PARSE_FAILED", f"{adapter_key}:{','.join(parsed['reason_codes'])}"
            )
        parsed_receipts = sorted(row["rcept_no"] for row in parsed["accepted_rows"])
        if parsed_receipts != journal_receipts:
            raise FallbackValidationError("LIST_RAW_JOURNAL_RECEIPTS_MISMATCH", adapter_key)
        for row in parsed["accepted_rows"]:
            receipt = row["rcept_no"]
            if receipt in rows_by_receipt:
                raise FallbackValidationError("DUPLICATE_RECEIPT_NUMBER", receipt)
            rows_by_receipt[receipt] = row
        payloads[adapter_key] = payload
        raw_hashes.append(str(journal.get("payload_sha256")))
    return payloads, rows_by_receipt, {
        "manifest_list_page_count": len(list_records),
        "verified_list_page_count": len(payloads),
        "accepted_receipt_count": len(rows_by_receipt),
        "unique_raw_sha256_count": len(set(raw_hashes)),
    }


def _load_document_payloads(
    *,
    root: Path,
    contract: Mapping[str, Any],
    manifest: Mapping[str, Any],
    status_by_receipt: Mapping[str, str],
) -> tuple[dict[str, bytes], dict[str, dict[str, Any]], dict[str, Any]]:
    source = contract["input"]
    pass_dir = _resolve(root, source["document_pass_journal_dir"])
    unavailable_dir = _resolve(root, source["document_unavailable_journal_dir"])
    shadow_dir = _resolve(root, source["shadow_pass_journal_dir"])
    document_records = manifest["checkpoint"]["documents"]
    payloads: dict[str, bytes] = {}
    package_evidence: dict[str, dict[str, Any]] = {}
    pass_count = 0
    fallback_count = 0
    for receipt, status in sorted(status_by_receipt.items()):
        latest = _latest_attempt(document_records[receipt], identity=receipt)
        if latest.get("receipt_no") != receipt:
            raise FallbackValidationError("DOCUMENT_MANIFEST_RECEIPT_MISMATCH", receipt)
        if status == "PASS":
            journal = _load_json(pass_dir / f"{receipt}.json")
            request = journal.get("request")
            if (
                journal.get("status") != "PASS"
                or not isinstance(request, Mapping)
                or request.get("receipt_no") != receipt
                or journal.get("payload_sha256") != latest.get("payload_sha256")
            ):
                raise FallbackValidationError("DOCUMENT_PASS_JOURNAL_MISMATCH", receipt)
            payloads[receipt] = _read_verified_artifact(
                root=root,
                path_value=journal.get("raw_path"),
                expected_sha256=journal.get("payload_sha256"),
            )
            pass_count += 1
            continue

        unavailable = _load_json(unavailable_dir / f"{receipt}.json")
        unavailable_request = unavailable.get("request")
        if (
            unavailable.get("status") != "UNAVAILABLE"
            or int(unavailable.get("http_status", 0)) != int(source["required_unavailable_http_status"])
            or str(unavailable.get("dart_status")) != source["required_unavailable_dart_status"]
            or unavailable.get("error_code") != source["required_unavailable_error_code"]
            or not isinstance(unavailable_request, Mapping)
            or unavailable_request.get("receipt_no") != receipt
            or unavailable.get("payload_sha256") != latest.get("payload_sha256")
        ):
            raise FallbackValidationError("DOCUMENT_UNAVAILABLE_JOURNAL_MISMATCH", receipt)
        _read_verified_artifact(
            root=root,
            path_value=unavailable.get("raw_path"),
            expected_sha256=unavailable.get("payload_sha256"),
        )
        shadow = _load_json(shadow_dir / f"{receipt}.json")
        if shadow.get("status") != source["required_shadow_status"]:
            raise FallbackValidationError("SHADOW_JOURNAL_NOT_PASS", receipt)
        package, evidence = build_deterministic_viewer_package(
            root=root, receipt=receipt, shadow_journal=shadow
        )
        payloads[receipt] = package
        package_evidence[receipt] = evidence
        fallback_count += 1
    return payloads, package_evidence, {
        "opendart_package_count": pass_count,
        "viewer_fallback_package_count": fallback_count,
        "combined_document_count": len(payloads),
        "fallback_package_bytes": sum(item["package_bytes"] for item in package_evidence.values()),
        "fallback_member_count": sum(item["member_count"] for item in package_evidence.values()),
        "fallback_package_sha256_set_sha256": _sha256(
            "\n".join(sorted(item["package_sha256"] for item in package_evidence.values())).encode(
                "ascii"
            )
        ),
    }


def _load_completion_authority_contexts(
    *,
    root: Path,
    contract: Mapping[str, Any],
    status_by_receipt: Mapping[str, str],
) -> tuple[dict[str, str], dict[str, bytes], dict[str, Any]]:
    source = contract["input"]
    shadow_dir = _resolve(root, source["shadow_pass_journal_dir"])
    source_contexts: dict[str, str] = {}
    viewer_landings: dict[str, bytes] = {}
    landing_hashes: list[str] = []
    for receipt, status in sorted(status_by_receipt.items()):
        if status == "PASS":
            source_contexts[receipt] = "OPENDART_PASS"
            continue
        if status != "UNAVAILABLE":
            raise FallbackValidationError("COMPLETION_SOURCE_STATUS_INVALID", receipt)
        shadow = _load_json(shadow_dir / f"{receipt}.json")
        landing = shadow.get("landing")
        if (
            shadow.get("status") != source["required_shadow_status"]
            or shadow.get("receipt_no") != receipt
            or not isinstance(landing, Mapping)
            or landing.get("kind") != "LANDING"
            or int(landing.get("http_status", 0)) != 200
        ):
            raise FallbackValidationError("VIEWER_LANDING_CONTEXT_INVALID", receipt)
        payload = _read_verified_artifact(
            root=root,
            path_value=landing.get("path"),
            expected_sha256=landing.get("sha256"),
            expected_bytes=landing.get("bytes"),
        )
        source_contexts[receipt] = "VIEWER_FALLBACK"
        viewer_landings[receipt] = payload
        landing_hashes.append(str(landing.get("sha256")))
    return source_contexts, viewer_landings, {
        "completion_source_context_count": len(source_contexts),
        "viewer_landing_context_count": len(viewer_landings),
        "viewer_landing_sha256_set_sha256": _sha256(
            "\n".join(sorted(landing_hashes)).encode("ascii")
        ),
    }


def _load_broad_b_supplement(
    *, root: Path, contract: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, bytes], list[str], dict[str, Any]]:
    source = contract["input"]
    report_path = _resolve(root, source["broad_b_supplement_report_path"])
    report_payload = report_path.read_bytes()
    report_hash = _sha256(report_payload)
    if report_hash != source["broad_b_supplement_report_sha256"]:
        raise FallbackValidationError("BROAD_B_SUPPLEMENT_REPORT_HASH_MISMATCH")
    try:
        report = json.loads(report_payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FallbackValidationError("BROAD_B_SUPPLEMENT_REPORT_INVALID") from exc
    if (
        not isinstance(report, Mapping)
        or report.get("status") != source["required_broad_b_supplement_status"]
        or report.get("integrity_pass") is not True
        or int(report.get("target_stock_count", -1))
        != int(source["required_broad_b_target_stock_count"])
        or int(report.get("new_merger_decision_count", -1))
        != int(source["required_broad_b_new_decision_count"])
        or int(report.get("verified_new_document_count", -1))
        != int(source["required_broad_b_direct_document_count"])
        or int(report.get("verified_viewer_fallback_count", -1))
        != int(source["required_broad_b_viewer_document_count"])
        or int(report.get("pending_list_request_count", -1)) != 0
        or int(report.get("pending_document_request_count", -1)) != 0
        or int(report.get("pending_viewer_request_count", -1)) != 0
    ):
        raise FallbackValidationError("BROAD_B_SUPPLEMENT_REPORT_NOT_COMPLETE")
    rows = report.get("new_rows")
    direct = report.get("documents")
    unavailable = report.get("unavailable_documents")
    viewer = report.get("viewer_documents")
    if not all(isinstance(value, list) for value in (rows, direct, unavailable, viewer)):
        raise FallbackValidationError("BROAD_B_SUPPLEMENT_COLLECTIONS_INVALID")
    rows_by_receipt: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise FallbackValidationError("BROAD_B_SUPPLEMENT_ROW_INVALID")
        receipt = str(row.get("rcept_no") or "")
        if (
            not RECEIPT_PATTERN.fullmatch(receipt)
            or row.get("series") != "MERGER_DECISION_HISTORY"
            or row.get("source_series") != "BROAD_B_LEGACY_SUPPLEMENT"
            or receipt in rows_by_receipt
        ):
            raise FallbackValidationError("BROAD_B_SUPPLEMENT_ROW_INVALID", receipt)
        rows_by_receipt[receipt] = dict(row)
    payloads: dict[str, bytes] = {}
    source_hashes = [report_hash]
    direct_receipts: set[str] = set()
    for journal in direct:
        if not isinstance(journal, Mapping):
            raise FallbackValidationError("BROAD_B_DIRECT_DOCUMENT_INVALID")
        receipt = str(journal.get("receipt_no") or "")
        if journal.get("status") != "SHADOW_PASS" or receipt in direct_receipts:
            raise FallbackValidationError("BROAD_B_DIRECT_DOCUMENT_INVALID", receipt)
        payload = _read_verified_artifact(
            root=root,
            path_value=journal.get("raw_path"),
            expected_sha256=journal.get("payload_sha256"),
            expected_bytes=journal.get("size_bytes"),
        )
        payloads[receipt] = payload
        direct_receipts.add(receipt)
        source_hashes.append(str(journal.get("payload_sha256")))
    unavailable_receipts = {
        str(item.get("receipt_no") or "")
        for item in unavailable
        if isinstance(item, Mapping) and item.get("status") == "SHADOW_UNAVAILABLE"
    }
    viewer_receipts: set[str] = set()
    for journal in viewer:
        if not isinstance(journal, Mapping):
            raise FallbackValidationError("BROAD_B_VIEWER_DOCUMENT_INVALID")
        receipt = str(journal.get("receipt_no") or "")
        package, evidence = build_deterministic_viewer_package(
            root=root, receipt=receipt, shadow_journal=journal
        )
        if receipt in viewer_receipts:
            raise FallbackValidationError("BROAD_B_VIEWER_DOCUMENT_DUPLICATE", receipt)
        payloads[receipt] = package
        viewer_receipts.add(receipt)
        source_hashes.append(evidence["package_sha256"])
    if unavailable_receipts != viewer_receipts:
        raise FallbackValidationError("BROAD_B_VIEWER_UNAVAILABLE_SET_MISMATCH")
    if direct_receipts & viewer_receipts or set(payloads) != set(rows_by_receipt):
        raise FallbackValidationError("BROAD_B_SUPPLEMENT_DOCUMENT_SET_MISMATCH")
    return (
        [rows_by_receipt[key] for key in sorted(rows_by_receipt)],
        payloads,
        sorted(set(source_hashes)),
        {
            "report_sha256": report_hash,
            "target_stock_count": int(report["target_stock_count"]),
            "new_decision_count": len(rows_by_receipt),
            "direct_document_count": len(direct_receipts),
            "viewer_document_count": len(viewer_receipts),
            "full_universe_coverage": bool(source["broad_b_full_universe_coverage"]),
        },
    )


def _fallback_diagnostics(
    *,
    root: Path,
    rows_by_receipt: Mapping[str, Mapping[str, str]],
    document_payloads: Mapping[str, bytes],
    fallback_receipts: set[str],
    viewer_landing_payloads: Mapping[str, bytes],
) -> dict[str, Any]:
    adapter_contract = load_corporate_action_adapter_contract(root)
    rules = adapter_contract["document_rules"]
    prefixes = rules["revision_report_prefixes"]
    initial_by_code: dict[str, set[str]] = {}
    for receipt, row in rows_by_receipt.items():
        if row["series"] != "MERGER_DECISION_HISTORY":
            continue
        if not any(row["report_nm"].strip().startswith(prefix) for prefix in prefixes):
            initial_by_code.setdefault(row["stock_code"], set()).add(receipt)

    rows: list[dict[str, Any]] = []
    failure_counts: Counter[str] = Counter()
    for receipt in sorted(fallback_receipts):
        row = rows_by_receipt.get(receipt)
        if row is None:
            rows.append({"receipt_no": receipt, "status": "FAIL", "reason_codes": ["RECEIPT_NOT_IN_LIST"]})
            failure_counts["RECEIPT_NOT_IN_LIST"] += 1
            continue
        text, text_reasons = _document_text(document_payloads[receipt])
        reasons = list(text_reasons)
        contains_merger = "합병" in text
        if not contains_merger:
            reasons.append("DART_DOCUMENT_NOT_COMPANY_MERGER")
        is_revision = any(row["report_nm"].strip().startswith(prefix) for prefix in prefixes)
        if row["series"] == "MERGER_COMPLETION_HISTORY":
            event_kind = "COMPLETION"
        elif not is_revision:
            event_kind = "INITIAL_DECISION"
        elif any(phrase in text for phrase in rules["cancellation_phrases"]):
            event_kind = "CANCELLATION"
        else:
            event_kind = "REVISION"
        references = _document_receipt_references(
            document_payloads[receipt], rules["explicit_lineage_patterns"]
        )
        references.discard(receipt)
        candidates = sorted(references & initial_by_code.get(row["stock_code"], set()))
        if event_kind != "INITIAL_DECISION" and len(candidates) != 1:
            reasons.append("EVENT_IDENTITY_UNRESOLVED")
        actual_date = _date_after_label(
            text, rules["actual_effective_date_labels"], rules["date_patterns"]
        )
        actual_date_authority = "DART_LABEL_NEAREST_DATE" if actual_date else ""
        if event_kind == "COMPLETION" and not actual_date:
            structured_date, structured_authority, structured_reasons = (
                _structured_completion_date(
                    document_payloads[receipt],
                    source="VIEWER_FALLBACK",
                    viewer_landing_payload=viewer_landing_payloads.get(receipt),
                    rules=rules["completion_structured_fallback"],
                )
            )
            reasons.extend(structured_reasons)
            if structured_date:
                actual_date = structured_date
                actual_date_authority = structured_authority
        if event_kind == "COMPLETION" and not actual_date:
            reasons.append("COMPLETION_ACTUAL_DATE_MISSING")
        reasons = sorted(set(reasons))
        failure_counts.update(reasons)
        rows.append(
            {
                "receipt_no": receipt,
                "stock_code": row["stock_code"],
                "series": row["series"],
                "report_name": row["report_nm"],
                "event_kind": event_kind,
                "text_nonempty": bool(text),
                "contains_company_merger_text": contains_merger,
                "explicit_receipt_reference_count": len(references),
                "initial_receipt_candidates": candidates,
                "actual_effective_date": actual_date if event_kind == "COMPLETION" else "",
                "actual_effective_date_authority": (
                    actual_date_authority if event_kind == "COMPLETION" else ""
                ),
                "status": "PASS" if not reasons else "FAIL",
                "reason_codes": reasons,
            }
        )
    failed = [row["receipt_no"] for row in rows if row["status"] != "PASS"]
    return {
        "receipt_count": len(rows),
        "pass_count": len(rows) - len(failed),
        "fail_count": len(failed),
        "failure_reason_counts": dict(sorted(failure_counts.items())),
        "failed_receipts": failed,
        "receipts": rows,
    }


def validate_fallback_authority(
    *, root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    root = root.resolve()
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        contract = load_validation_contract(root, contract_path)
        manifest, capture, status_by_receipt = _validate_capture_context(
            root=root, contract=contract
        )
        corp_payload, corp_report = _load_corp_package(root=root, contract=contract)
        list_payloads, rows_by_receipt, list_evidence = _load_list_pages(
            root=root, contract=contract, manifest=manifest
        )
        document_payloads, package_evidence, document_evidence = _load_document_payloads(
            root=root,
            contract=contract,
            manifest=manifest,
            status_by_receipt=status_by_receipt,
        )
        source_contexts, viewer_landings, authority_context_evidence = (
            _load_completion_authority_contexts(
                root=root,
                contract=contract,
                status_by_receipt=status_by_receipt,
            )
        )
        supplemental_rows, supplemental_documents, supplemental_hashes, supplement_evidence = (
            _load_broad_b_supplement(root=root, contract=contract)
        )
        if set(document_payloads) != set(rows_by_receipt):
            raise FallbackValidationError(
                "COMBINED_DOCUMENT_SET_MISMATCH",
                f"documents={len(document_payloads)}, receipts={len(rows_by_receipt)}",
            )
        diagnostic_rows = dict(rows_by_receipt)
        diagnostic_rows.update({row["rcept_no"]: row for row in supplemental_rows})
        diagnostic_documents = dict(document_payloads)
        diagnostic_documents.update(supplemental_documents)
        diagnostics = _fallback_diagnostics(
            root=root,
            rows_by_receipt=diagnostic_rows,
            document_payloads=diagnostic_documents,
            fallback_receipts=set(package_evidence),
            viewer_landing_payloads=viewer_landings,
        )
        adapter = build_corporate_action_adapter_output(
            selection_as_of=manifest["selection_as_of"],
            earliest_listed_date=manifest["query_start"],
            base_codes=manifest["universe"]["codes"],
            corp_code_payload=corp_payload,
            list_page_payloads=list_payloads,
            document_payloads=document_payloads,
            supplemental_decision_rows=supplemental_rows,
            supplemental_document_payloads=supplemental_documents,
            supplemental_source_sha256s=supplemental_hashes,
            completion_source_context_by_receipt=source_contexts,
            viewer_landing_payload_by_receipt=viewer_landings,
            repo_root=root,
        )
        reasons = list(adapter.get("reason_codes") or [])
        if not supplement_evidence["full_universe_coverage"]:
            reasons.append("BROAD_B_SUPPLEMENT_TARGETED_SCOPE_ONLY")
        if diagnostics["fail_count"]:
            reasons.append("VIEWER_FALLBACK_RECEIPT_DIAGNOSTICS_FAILED")
        acceptance = contract["acceptance"]
        if int(adapter.get("completion_event_count", -1)) != int(
            acceptance["required_completion_event_count"]
        ):
            reasons.append("COMPLETION_EVENT_COUNT_MISMATCH")
        if int(adapter.get("completion_actual_date_count", -1)) != int(
            acceptance["required_completion_actual_date_count"]
        ):
            reasons.append("COMPLETION_ACTUAL_DATE_COUNT_MISMATCH")
        if adapter.get("completion_actual_date_authority_counts") != acceptance.get(
            "required_completion_actual_date_authority_counts"
        ):
            reasons.append("COMPLETION_DATE_AUTHORITY_COUNTS_MISMATCH")
        if len(adapter.get("completion_structured_fallback_rows") or []) != int(
            acceptance["required_completion_structured_fallback_count"]
        ):
            reasons.append("COMPLETION_STRUCTURED_FALLBACK_COUNT_MISMATCH")
        if adapter.get("completion_missing_receipts") != acceptance.get(
            "required_completion_missing_receipts"
        ):
            reasons.append("COMPLETION_MISSING_RECEIPTS_MISMATCH")
        if adapter.get("status") != contract["acceptance"]["required_adapter_status"]:
            reasons.append("FULL_ADAPTER_LIFECYCLE_NOT_PASS")
        reasons = sorted(set(reasons))
        status = "PASS" if not reasons else "FAIL"
        coverage = adapter.get("coverage") or {}
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_DART_VIEWER_FALLBACK_AUTHORITY_VALIDATION",
            "status": status,
            "verdict": (
                "DART_VIEWER_FALLBACK_AUTHORITY_VALIDATION_PASS"
                if status == "PASS"
                else "FAIL_DART_VIEWER_FALLBACK_AUTHORITY_VALIDATION"
            ),
            "reason_codes": reasons,
            "generated_at": generated_at,
            "selection_as_of": manifest["selection_as_of"],
            "query_start": manifest["query_start"],
            "query_end": manifest["query_end"],
            "capture_status_before": capture.get("status"),
            "capture_full_capture_complete_before": capture.get("full_capture_complete"),
            "manifest_integrity": {
                "document_count": len(status_by_receipt),
                "document_status_counts": dict(sorted(Counter(status_by_receipt.values()).items())),
                "corp_package_sha256": corp_report.get("raw_sha256"),
                **list_evidence,
                **document_evidence,
                **authority_context_evidence,
                "broad_b_supplement": supplement_evidence,
            },
            "fallback_diagnostics": diagnostics,
            "adapter": {
                "status": adapter.get("status"),
                "verdict": adapter.get("verdict"),
                "reason_codes": adapter.get("reason_codes") or [],
                "base_code_count": adapter.get("base_code_count"),
                "mapped_code_count": adapter.get("mapped_code_count"),
                "expected_list_page_count": adapter.get("expected_list_page_count"),
                "received_list_page_count": adapter.get("received_list_page_count"),
                "accepted_disclosure_count": adapter.get("accepted_disclosure_count"),
                "supplemental_decision_count": adapter.get("supplemental_decision_count"),
                "document_count": adapter.get("document_count"),
                "event_count": adapter.get("event_count"),
                "completion_event_count": adapter.get("completion_event_count"),
                "completion_actual_date_count": adapter.get("completion_actual_date_count"),
                "completion_actual_date_authority_counts": adapter.get(
                    "completion_actual_date_authority_counts"
                ),
                "completion_structured_fallback_rows": adapter.get(
                    "completion_structured_fallback_rows"
                ),
                "completion_missing_receipts": adapter.get("completion_missing_receipts"),
                "interval_count": len(adapter.get("interval_rows") or []),
                "coverage_provenance_status": coverage.get("provenance_status"),
            },
            "policy_change_allowed": status == "PASS",
            "capture_status_changed": False,
            "fallback_packages_persisted": False,
            "network_requests": 0,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
        }
    except FallbackValidationError as exc:
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_DART_VIEWER_FALLBACK_AUTHORITY_VALIDATION",
            "status": "FAIL",
            "verdict": "FAIL_DART_VIEWER_FALLBACK_AUTHORITY_VALIDATION",
            "reason_codes": [exc.code],
            "failure_detail": exc.detail,
            "generated_at": generated_at,
            "policy_change_allowed": False,
            "capture_status_changed": False,
            "fallback_packages_persisted": False,
            "network_requests": 0,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
        }


def write_validation_report(
    report: Mapping[str, Any], *, root: Path = ROOT, contract_path: Path | None = None
) -> tuple[Path, Path]:
    contract = load_validation_contract(root.resolve(), contract_path)
    generated_at = str(report.get("generated_at") or "")
    stamp = "".join(character for character in generated_at if character.isdigit())[:14]
    if len(stamp) != 14:
        raise FallbackValidationError("REPORT_TIMESTAMP_INVALID")
    report_dir = root.resolve() / REPORT_DIR_RELATIVE
    output = contract["output"]
    versioned = report_dir / f"{output['versioned_report_prefix']}{stamp}.json"
    latest = _resolve(root.resolve(), output["latest_report_path"])
    payload = _json_bytes(report)
    _atomic_write(versioned, payload)
    _atomic_write(latest, payload)
    return versioned, latest


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate exact OpenDART 014 viewer fallback against the full C8 adapter lifecycle."
    )
    parser.add_argument("--contract", type=Path, default=ROOT / CONTRACT_RELATIVE)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    report = validate_fallback_authority(root=ROOT, contract_path=args.contract)
    if args.write_report:
        versioned, latest = write_validation_report(
            report, root=ROOT, contract_path=args.contract
        )
        report = {**report, "report_paths": [str(versioned), str(latest)]}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
