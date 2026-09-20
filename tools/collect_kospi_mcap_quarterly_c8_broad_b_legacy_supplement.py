from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    _extract_package_members,
)
from tools import collect_kospi_mcap_quarterly_c8_dart_viewer_shadow as shadow


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_broad_b_legacy_supplement_contract_v1.json"
)
RECEIPT_PATTERN = re.compile(r"^[0-9]{14}$")
Fetcher = shadow.Fetcher


class BroadBSupplementError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail or code


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BroadBSupplementError("JSON_READ_FAILED", str(path)) from exc
    if not isinstance(value, dict):
        raise BroadBSupplementError("JSON_OBJECT_REQUIRED", str(path))
    return value


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise BroadBSupplementError("PATH_OUTSIDE_ROOT", str(resolved)) from exc
    return resolved


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def _read_frozen_json(root: Path, path_value: object, expected_hash: object) -> dict[str, Any]:
    path = _resolve(root, str(path_value or ""))
    payload = path.read_bytes()
    actual = _sha256(payload)
    if actual != str(expected_hash or "").lower():
        raise BroadBSupplementError("FROZEN_INPUT_HASH_MISMATCH", f"{path}:{actual}")
    try:
        value = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BroadBSupplementError("FROZEN_INPUT_JSON_INVALID", str(path)) from exc
    if not isinstance(value, dict):
        raise BroadBSupplementError("FROZEN_INPUT_OBJECT_REQUIRED", str(path))
    return value


def _latest_corp_mapping(manifest: Mapping[str, Any]) -> dict[str, str]:
    attempts = (
        manifest.get("checkpoint", {}).get("corp_code_package", {}).get("attempts") or []
    )
    if not attempts or attempts[-1].get("status") != "PASS":
        raise BroadBSupplementError("CORP_MAPPING_NOT_PASS")
    mapping = dict(attempts[-1].get("mapping") or {})
    if not mapping:
        raise BroadBSupplementError("CORP_MAPPING_EMPTY")
    return mapping


def _existing_receipts(manifest: Mapping[str, Any]) -> set[str]:
    receipts: set[str] = set()
    records = manifest.get("checkpoint", {}).get("list_pages", {})
    if not isinstance(records, Mapping):
        raise BroadBSupplementError("MANIFEST_LIST_PAGES_INVALID")
    for record in records.values():
        attempts = record.get("attempts") if isinstance(record, Mapping) else None
        if attempts and attempts[-1].get("status") == "PASS":
            receipts.update(str(value) for value in attempts[-1].get("accepted_receipts") or [])
    if any(not RECEIPT_PATTERN.fullmatch(value) for value in receipts):
        raise BroadBSupplementError("MANIFEST_RECEIPT_INVALID")
    return receipts


def _target_windows(
    report: Mapping[str, Any], manifest: Mapping[str, Any], contract: Mapping[str, Any]
) -> list[dict[str, Any]]:
    source = contract["input"]
    if (
        report.get("status") != source["required_completion_scope_status"]
        or report.get("verdict") != source["required_completion_scope_verdict"]
        or report.get("selection_as_of") != source["required_selection_as_of"]
    ):
        raise BroadBSupplementError("COMPLETION_SCOPE_IDENTITY_MISMATCH")
    rows = [row for row in report.get("rows", []) if row.get("authority_resolved") is False]
    if len(rows) != int(source["required_unresolved_completion_count"]):
        raise BroadBSupplementError("UNRESOLVED_COMPLETION_COUNT_MISMATCH", str(len(rows)))
    mapping = _latest_corp_mapping(manifest)
    by_code: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        code = str(row.get("stock_code") or "")
        receipt = str(row.get("receipt_no") or "")
        receipt_date = str(row.get("receipt_date") or "")
        if not re.fullmatch(r"[0-9A-Z]{6}", code) or not RECEIPT_PATTERN.fullmatch(receipt):
            raise BroadBSupplementError("COMPLETION_SCOPE_ROW_IDENTITY_INVALID", receipt)
        if not re.fullmatch(r"[0-9]{8}", receipt_date):
            raise BroadBSupplementError("COMPLETION_SCOPE_ROW_DATE_INVALID", receipt)
        by_code.setdefault(code, []).append(row)
    if len(by_code) != int(source["required_target_stock_count"]):
        raise BroadBSupplementError("TARGET_STOCK_COUNT_MISMATCH", str(len(by_code)))
    query_start = str(manifest.get("query_start") or "")
    if not re.fullmatch(r"[0-9]{8}", query_start):
        raise BroadBSupplementError("MANIFEST_QUERY_START_INVALID")
    windows: list[dict[str, Any]] = []
    for code, code_rows in sorted(by_code.items()):
        corp_code = str(mapping.get(code) or "")
        if not re.fullmatch(r"[0-9]{8}", corp_code):
            raise BroadBSupplementError("TARGET_CORP_MAPPING_MISSING", code)
        windows.append(
            {
                "stock_code": code,
                "corp_code": corp_code,
                "query_start": query_start,
                "query_end": max(str(row["receipt_date"]) for row in code_rows),
                "completion_receipts": sorted(str(row["receipt_no"]) for row in code_rows),
                "classifications": sorted({str(row.get("classification") or "") for row in code_rows}),
            }
        )
    return windows


def _load_context(root: Path, contract_path: Path) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], set[str]]:
    contract = _load_json(contract_path)
    if contract.get("strategy_id") != STRATEGY_ID or contract.get("mode") != "SHADOW_ONLY":
        raise BroadBSupplementError("BROAD_B_CONTRACT_INVALID")
    source = contract.get("input")
    if not isinstance(source, Mapping):
        raise BroadBSupplementError("BROAD_B_INPUT_INVALID")
    report = _read_frozen_json(
        root, source.get("completion_scope_report_path"), source.get("completion_scope_report_sha256")
    )
    manifest = _read_frozen_json(
        root, source.get("capture_manifest_path"), source.get("capture_manifest_sha256")
    )
    if manifest.get("full_capture_complete") is not source.get("required_manifest_full_capture_complete"):
        raise BroadBSupplementError("MANIFEST_FULL_CAPTURE_STATE_MISMATCH")
    if manifest.get("selection_as_of") != source.get("required_selection_as_of"):
        raise BroadBSupplementError("MANIFEST_SELECTION_AS_OF_MISMATCH")
    return contract, manifest, _target_windows(report, manifest, contract), _existing_receipts(manifest)


def _paths(root: Path, contract: Mapping[str, Any]) -> dict[str, Path]:
    output = contract["output"]
    output_root = _resolve(root, output["root"])
    return {
        "root": output_root,
        "list_raw": output_root / output["list_raw_dir"],
        "document_raw": output_root / output["document_raw_dir"],
        "viewer_raw": output_root / output["viewer_raw_dir"],
        "list_pass": output_root / output["list_pass_journal_dir"],
        "document_pass": output_root / output["document_pass_journal_dir"],
        "document_unavailable": output_root / output["document_unavailable_journal_dir"],
        "viewer_pass": output_root / output["viewer_pass_journal_dir"],
        "viewer_fail": output_root / output["viewer_failure_journal_dir"],
        "fail": output_root / output["failure_journal_dir"],
        "checkpoint": output_root / output["checkpoint_path"],
        "report": _resolve(root, output["report_path"]),
    }


def _normalized_report_name(value: object) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def _is_merger_decision(report_name: object, contract: Mapping[str, Any]) -> bool:
    normalized = _normalized_report_name(report_name)
    rule = contract["decision_filter"]
    return str(rule["required_normalized_fragment"]) in normalized and not any(
        str(fragment) in normalized for fragment in rule["forbidden_normalized_fragments"]
    )


def _parse_list_payload(
    payload: bytes,
    *,
    target: Mapping[str, Any],
    page_no: int,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        body = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BroadBSupplementError("BROAD_B_LIST_JSON_INVALID") from exc
    if not isinstance(body, Mapping):
        raise BroadBSupplementError("BROAD_B_LIST_OBJECT_REQUIRED")
    status = str(body.get("status") or "")
    if status == "013":
        return {"dart_status": status, "page_no": page_no, "total_page": 0, "rows": []}
    if status != "000":
        raise BroadBSupplementError("BROAD_B_LIST_DART_STATUS_INVALID", status)
    try:
        actual_page = int(body.get("page_no"))
        total_page = int(body.get("total_page"))
    except (TypeError, ValueError) as exc:
        raise BroadBSupplementError("BROAD_B_LIST_PAGINATION_INVALID") from exc
    if actual_page != page_no or total_page < page_no:
        raise BroadBSupplementError("BROAD_B_LIST_PAGINATION_MISMATCH")
    source_rows = body.get("list")
    if not isinstance(source_rows, list):
        raise BroadBSupplementError("BROAD_B_LIST_ROWS_INVALID")
    rows: list[dict[str, str]] = []
    for source_row in source_rows:
        if not isinstance(source_row, Mapping):
            raise BroadBSupplementError("BROAD_B_LIST_ROW_INVALID")
        if not _is_merger_decision(source_row.get("report_nm"), contract):
            continue
        corp_code = str(source_row.get("corp_code") or "").strip()
        stock_code = str(source_row.get("stock_code") or "").strip()
        receipt = str(source_row.get("rcept_no") or "").strip()
        receipt_date = str(source_row.get("rcept_dt") or "").strip()
        if (
            corp_code != target["corp_code"]
            or stock_code != target["stock_code"]
            or not RECEIPT_PATTERN.fullmatch(receipt)
            or not re.fullmatch(r"[0-9]{8}", receipt_date)
            or receipt_date < target["query_start"]
            or receipt_date > target["query_end"]
        ):
            raise BroadBSupplementError("BROAD_B_LIST_ROW_IDENTITY_MISMATCH", receipt)
        rows.append(
            {
                "corp_code": corp_code,
                "stock_code": stock_code,
                "report_nm": str(source_row.get("report_nm") or "").strip(),
                "rcept_no": receipt,
                "rcept_dt": receipt_date,
                "rm": str(source_row.get("rm") or "").strip(),
                "series": "MERGER_DECISION_HISTORY",
                "source_series": "BROAD_B_LEGACY_SUPPLEMENT",
            }
        )
    receipts = [row["rcept_no"] for row in rows]
    if len(receipts) != len(set(receipts)):
        raise BroadBSupplementError("BROAD_B_LIST_DUPLICATE_RECEIPT")
    return {"dart_status": status, "page_no": page_no, "total_page": total_page, "rows": rows}


def _read_key(path: Path) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8-sig").strip()


def _fetch(
    *,
    endpoint: str,
    params: dict[str, str],
    contract: Mapping[str, Any],
    fetcher: Fetcher,
    sleep_fn: Callable[[float], None],
    timeout: float,
    retries: int,
    remaining_budget: int,
) -> tuple[int, bytes, Mapping[str, str], int]:
    execution = contract["execution"]
    return shadow._fetch_with_retry(
        endpoint=endpoint,
        params=params,
        headers={"User-Agent": str(execution["user_agent"]), "Accept": "application/json,application/xml"},
        timeout=timeout,
        retries=min(retries, max(0, remaining_budget - 1)),
        backoff=float(execution["retry_backoff_seconds"]),
        fetcher=fetcher,
        sleep_fn=sleep_fn,
    )


def _list_journal_path(paths: Mapping[str, Path], code: str, page_no: int) -> Path:
    return paths["list_pass"] / f"{code}_p{page_no:04d}.json"


def _verify_list_journal(
    root: Path,
    path: Path,
    *,
    target: Mapping[str, Any],
    page_no: int,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    journal = _load_json(path)
    if (
        journal.get("status") != "SHADOW_PASS"
        or journal.get("promotion_allowed") is not False
        or journal.get("request", {}).get("stock_code") != target["stock_code"]
        or int(journal.get("request", {}).get("page_no", 0)) != page_no
    ):
        raise BroadBSupplementError("BROAD_B_LIST_JOURNAL_INVALID", str(path))
    raw = _resolve(root, journal.get("raw_path", "")).read_bytes()
    if _sha256(raw) != journal.get("payload_sha256") or len(raw) != int(journal.get("size_bytes", -1)):
        raise BroadBSupplementError("BROAD_B_LIST_JOURNAL_ARTIFACT_MISMATCH", str(path))
    parsed = _parse_list_payload(raw, target=target, page_no=page_no, contract=contract)
    if parsed["dart_status"] != journal.get("dart_status") or parsed["total_page"] != journal.get("total_page") or parsed["rows"] != journal.get("accepted_rows"):
        raise BroadBSupplementError("BROAD_B_LIST_JOURNAL_REPLAY_MISMATCH", str(path))
    return journal


def _collect_list_page(
    *,
    root: Path,
    paths: Mapping[str, Path],
    target: Mapping[str, Any],
    page_no: int,
    api_key: str,
    contract: Mapping[str, Any],
    fetcher: Fetcher,
    sleep_fn: Callable[[float], None],
    timeout: float,
    retries: int,
    remaining_budget: int,
    clock: Callable[[], datetime],
) -> tuple[dict[str, Any], int]:
    route = contract["official_route"]
    params = {
        "crtfc_key": api_key,
        "corp_code": target["corp_code"],
        "bgn_de": target["query_start"],
        "end_de": target["query_end"],
        "pblntf_ty": route["pblntf_ty"],
        "last_reprt_at": route["last_reprt_at"],
        "sort": route["sort"],
        "sort_mth": route["sort_mth"],
        "page_count": route["page_count"],
        "page_no": str(page_no),
    }
    status, payload, headers, attempts = _fetch(
        endpoint=route["list_endpoint"], params=params, contract=contract, fetcher=fetcher,
        sleep_fn=sleep_fn, timeout=timeout, retries=retries, remaining_budget=remaining_budget,
    )
    if status != 200:
        raise BroadBSupplementError("BROAD_B_LIST_HTTP_STATUS_INVALID", str(status))
    parsed = _parse_list_payload(payload, target=target, page_no=page_no, contract=contract)
    digest = _sha256(payload)
    raw_path = paths["list_raw"] / target["stock_code"] / f"p{page_no:04d}_{digest[:16]}.json"
    shadow._write_immutable(raw_path, payload)
    journal = {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_BROAD_B_LEGACY_LIST_PAGE_SHADOW",
        "status": "SHADOW_PASS",
        "promotion_allowed": False,
        "captured_at": clock().astimezone(timezone.utc).isoformat(),
        "request": {key: value for key, value in target.items() if key != "completion_receipts"} | {"page_no": page_no},
        "http_status": status,
        "dart_status": parsed["dart_status"],
        "total_page": parsed["total_page"],
        "accepted_rows": parsed["rows"],
        "accepted_receipt_count": len(parsed["rows"]),
        "payload_sha256": digest,
        "size_bytes": len(payload),
        "raw_path": raw_path.relative_to(root).as_posix(),
        "server_date_header": str(headers.get("Date", "")),
        "network_requests": attempts,
    }
    shadow._write_immutable(_list_journal_path(paths, target["stock_code"], page_no), _json_bytes(journal))
    return journal, attempts


def _document_status(payload: bytes) -> str:
    if payload.startswith(b"PK") and _extract_package_members(payload):
        return "PASS"
    try:
        root = ET.fromstring(payload)
    except ET.ParseError:
        return "INVALID"
    for node in root.iter():
        if node.tag.rsplit("}", 1)[-1].lower() == "status":
            return str(node.text or "").strip()
    return "INVALID"


def _verify_document_journal(root: Path, path: Path, receipt: str) -> dict[str, Any]:
    journal = _load_json(path)
    if journal.get("status") != "SHADOW_PASS" or journal.get("receipt_no") != receipt:
        raise BroadBSupplementError("BROAD_B_DOCUMENT_JOURNAL_INVALID", receipt)
    raw = _resolve(root, journal.get("raw_path", "")).read_bytes()
    if _sha256(raw) != journal.get("payload_sha256") or len(raw) != int(journal.get("size_bytes", -1)):
        raise BroadBSupplementError("BROAD_B_DOCUMENT_ARTIFACT_MISMATCH", receipt)
    members = _extract_package_members(raw)
    if not members or len(members) != int(journal.get("package_member_count", -1)):
        raise BroadBSupplementError("BROAD_B_DOCUMENT_PACKAGE_REPLAY_MISMATCH", receipt)
    return journal


def _verify_unavailable_journal(root: Path, path: Path, receipt: str) -> dict[str, Any]:
    journal = _load_json(path)
    if (
        journal.get("status") != "SHADOW_UNAVAILABLE"
        or journal.get("receipt_no") != receipt
        or journal.get("dart_status") != "014"
    ):
        raise BroadBSupplementError("BROAD_B_DOCUMENT_UNAVAILABLE_JOURNAL_INVALID", receipt)
    raw = _resolve(root, journal.get("raw_path", "")).read_bytes()
    if _sha256(raw) != journal.get("payload_sha256") or len(raw) != int(journal.get("size_bytes", -1)):
        raise BroadBSupplementError("BROAD_B_DOCUMENT_UNAVAILABLE_ARTIFACT_MISMATCH", receipt)
    if _document_status(raw) != "014":
        raise BroadBSupplementError("BROAD_B_DOCUMENT_UNAVAILABLE_REPLAY_MISMATCH", receipt)
    return journal


def _collect_document(
    *,
    root: Path,
    paths: Mapping[str, Path],
    row: Mapping[str, str],
    api_key: str,
    contract: Mapping[str, Any],
    fetcher: Fetcher,
    sleep_fn: Callable[[float], None],
    timeout: float,
    retries: int,
    remaining_budget: int,
    clock: Callable[[], datetime],
) -> tuple[dict[str, Any], int]:
    receipt = row["rcept_no"]
    status, payload, headers, attempts = _fetch(
        endpoint=contract["official_route"]["document_endpoint"],
        params={"crtfc_key": api_key, "rcept_no": receipt}, contract=contract,
        fetcher=fetcher, sleep_fn=sleep_fn, timeout=timeout, retries=retries,
        remaining_budget=remaining_budget,
    )
    document_status = _document_status(payload)
    if status != 200:
        raise BroadBSupplementError("BROAD_B_DOCUMENT_HTTP_STATUS_INVALID", f"{receipt}:{status}")
    if document_status == "014":
        digest = _sha256(payload)
        raw_path = paths["document_raw"] / receipt / f"unavailable_{digest[:16]}.xml"
        shadow._write_immutable(raw_path, payload)
        journal = {
            "strategy_id": STRATEGY_ID,
            "scope": "M1_C8_BROAD_B_LEGACY_DOCUMENT_UNAVAILABLE_SHADOW",
            "status": "SHADOW_UNAVAILABLE",
            "promotion_allowed": False,
            "receipt_no": receipt,
            "stock_code": row["stock_code"],
            "corp_code": row["corp_code"],
            "captured_at": clock().astimezone(timezone.utc).isoformat(),
            "http_status": status,
            "dart_status": "014",
            "payload_sha256": digest,
            "size_bytes": len(payload),
            "raw_path": raw_path.relative_to(root).as_posix(),
            "server_date_header": str(headers.get("Date", "")),
            "network_requests": attempts,
        }
        shadow._write_immutable(paths["document_unavailable"] / f"{receipt}.json", _json_bytes(journal))
        return journal, attempts
    if document_status != "PASS":
        raise BroadBSupplementError("BROAD_B_DOCUMENT_RESPONSE_INVALID", f"{receipt}:{status}:{document_status}")
    members = _extract_package_members(payload)
    digest = _sha256(payload)
    raw_path = paths["document_raw"] / receipt / f"document_{digest[:16]}.zip"
    shadow._write_immutable(raw_path, payload)
    journal = {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_BROAD_B_LEGACY_DOCUMENT_SHADOW",
        "status": "SHADOW_PASS",
        "promotion_allowed": False,
        "receipt_no": receipt,
        "stock_code": row["stock_code"],
        "corp_code": row["corp_code"],
        "captured_at": clock().astimezone(timezone.utc).isoformat(),
        "http_status": status,
        "payload_sha256": digest,
        "size_bytes": len(payload),
        "package_member_count": len(members),
        "raw_path": raw_path.relative_to(root).as_posix(),
        "server_date_header": str(headers.get("Date", "")),
        "network_requests": attempts,
    }
    shadow._write_immutable(paths["document_pass"] / f"{receipt}.json", _json_bytes(journal))
    return journal, attempts


def _replay(
    *, root: Path, paths: Mapping[str, Path], targets: list[dict[str, Any]],
    existing_receipts: set[str], contract: Mapping[str, Any],
) -> dict[str, Any]:
    list_rows: dict[str, dict[str, str]] = {}
    list_pending: list[dict[str, Any]] = []
    list_page_count = 0
    for target in targets:
        first_path = _list_journal_path(paths, target["stock_code"], 1)
        if not first_path.is_file():
            list_pending.append({"kind": "LIST", "target": target, "page_no": 1})
            continue
        first = _verify_list_journal(root, first_path, target=target, page_no=1, contract=contract)
        total_page = int(first["total_page"])
        pages = range(1, max(1, total_page) + 1)
        for page_no in pages:
            path = _list_journal_path(paths, target["stock_code"], page_no)
            if not path.is_file():
                list_pending.append({"kind": "LIST", "target": target, "page_no": page_no})
                continue
            journal = first if page_no == 1 else _verify_list_journal(
                root, path, target=target, page_no=page_no, contract=contract
            )
            list_page_count += 1
            for row in journal["accepted_rows"]:
                receipt = row["rcept_no"]
                previous = list_rows.get(receipt)
                if previous is not None and previous != row:
                    raise BroadBSupplementError("BROAD_B_RECEIPT_METADATA_CONFLICT", receipt)
                list_rows[receipt] = row
    new_rows = {receipt: row for receipt, row in list_rows.items() if receipt not in existing_receipts}
    document_pending: list[dict[str, Any]] = []
    documents: dict[str, dict[str, Any]] = {}
    unavailable_documents: dict[str, dict[str, Any]] = {}
    viewer_pending: list[dict[str, Any]] = []
    viewer_documents: dict[str, dict[str, Any]] = {}
    if not list_pending:
        for receipt, row in sorted(new_rows.items()):
            pass_path = paths["document_pass"] / f"{receipt}.json"
            unavailable_path = paths["document_unavailable"] / f"{receipt}.json"
            if pass_path.is_file() and unavailable_path.is_file():
                raise BroadBSupplementError("BROAD_B_DOCUMENT_TERMINAL_STATE_CONFLICT", receipt)
            if pass_path.is_file():
                documents[receipt] = _verify_document_journal(root, pass_path, receipt)
            elif unavailable_path.is_file():
                unavailable_documents[receipt] = _verify_unavailable_journal(
                    root, unavailable_path, receipt
                )
                completed, issues = shadow._verify_completed(root, paths["viewer_pass"], [receipt])
                if issues:
                    raise BroadBSupplementError(
                        "BROAD_B_VIEWER_ARTIFACT_INTEGRITY_FAILED", ";".join(issues)
                    )
                if completed:
                    viewer_documents[receipt] = _load_json(paths["viewer_pass"] / f"{receipt}.json")
                else:
                    viewer_pending.append({"kind": "VIEWER", "row": row})
            else:
                document_pending.append({"kind": "DOCUMENT", "row": row})
    return {
        "list_page_count": list_page_count,
        "list_pending": list_pending,
        "accepted_rows": list_rows,
        "existing_duplicate_receipts": sorted(set(list_rows) & existing_receipts),
        "new_rows": new_rows,
        "document_pending": document_pending,
        "documents": documents,
        "unavailable_documents": unavailable_documents,
        "viewer_pending": viewer_pending,
        "viewer_documents": viewer_documents,
    }


def _summary(
    *, contract: Mapping[str, Any], targets: list[dict[str, Any]], replay: Mapping[str, Any],
    mode: str, generated_at: str, run_requests: list[dict[str, Any]], network_requests: int,
    integrity_issues: list[str] | None = None,
) -> dict[str, Any]:
    issues = list(integrity_issues or [])
    list_pending = replay["list_pending"]
    document_pending = replay["document_pending"]
    viewer_pending = replay["viewer_pending"]
    complete = not list_pending and not document_pending and not viewer_pending and not issues
    positive = contract["positive_control"]
    positive_present = positive["required_receipt_no"] in replay["accepted_rows"]
    if not list_pending and not positive_present:
        issues.append("POSITIVE_CONTROL_RECEIPT_MISSING")
        complete = False
    if issues:
        status = "SHADOW_FAIL"
    elif complete:
        status = "SHADOW_COMPLETE"
    elif list_pending:
        status = "SHADOW_LIST_PENDING"
    elif document_pending:
        status = "SHADOW_DOCUMENT_PENDING"
    else:
        status = "SHADOW_VIEWER_PENDING"
    return {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_BROAD_B_LEGACY_SUPPLEMENT_STATUS",
        "contract_version": contract["contract_version"],
        "generated_at": generated_at,
        "mode": mode,
        "status": status,
        "promotion_allowed": False,
        "target_stock_count": len(targets),
        "target_completion_count": sum(len(target["completion_receipts"]) for target in targets),
        "verified_list_page_count": replay["list_page_count"],
        "pending_list_request_count": len(list_pending),
        "accepted_merger_decision_count": len(replay["accepted_rows"]),
        "existing_duplicate_receipt_count": len(replay["existing_duplicate_receipts"]),
        "new_merger_decision_count": len(replay["new_rows"]),
        "verified_new_document_count": len(replay["documents"]),
        "unavailable_new_document_count": len(replay["unavailable_documents"]),
        "verified_viewer_fallback_count": len(replay["viewer_documents"]),
        "pending_document_request_count": len(document_pending),
        "pending_viewer_request_count": len(viewer_pending),
        "positive_control_pass": positive_present,
        "positive_control_receipt": positive["required_receipt_no"],
        "integrity_pass": not issues,
        "integrity_issues": issues,
        "target_windows": targets,
        "accepted_rows": [replay["accepted_rows"][key] for key in sorted(replay["accepted_rows"])],
        "existing_duplicate_receipts": replay["existing_duplicate_receipts"],
        "new_rows": [replay["new_rows"][key] for key in sorted(replay["new_rows"])],
        "documents": [replay["documents"][key] for key in sorted(replay["documents"])],
        "unavailable_documents": [
            replay["unavailable_documents"][key]
            for key in sorted(replay["unavailable_documents"])
        ],
        "viewer_documents": [
            replay["viewer_documents"][key]
            for key in sorted(replay["viewer_documents"])
        ],
        "next_requests": [
            ({"kind": "LIST", "stock_code": item["target"]["stock_code"], "page_no": item["page_no"]}
             if item["kind"] == "LIST" else {"kind": item["kind"], "receipt_no": item["row"]["rcept_no"]})
            for item in (list_pending + document_pending + viewer_pending)[:100]
        ],
        "run_requests": run_requests,
        "network_requests": network_requests,
        "capture_status_modified": False,
        "adapter_modified": False,
        "downstream_opened": False,
    }


def run_collection(
    *, root: Path = ROOT, contract_path: Path | None = None, apply: bool = False,
    confirm_shadow_only: bool = False, max_network_requests: int | None = None,
    timeout: float | None = None, delay: float | None = None, retries: int | None = None,
    fetcher: Fetcher = shadow._default_fetcher,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, Any]:
    root = root.resolve()
    contract_path = contract_path or root / CONTRACT_RELATIVE
    contract, _manifest, targets, existing_receipts = _load_context(root, contract_path)
    paths = _paths(root, contract)
    execution = contract["execution"]
    budget = int(execution["default_max_network_requests"] if max_network_requests is None else max_network_requests)
    timeout = float(execution["default_timeout_seconds"] if timeout is None else timeout)
    delay = float(execution["default_delay_seconds"] if delay is None else delay)
    retries = int(execution["default_retries"] if retries is None else retries)
    if budget < 1 or timeout <= 0 or delay < 0 or retries < 0:
        raise BroadBSupplementError("EXECUTION_ARGUMENT_INVALID")
    if apply and not confirm_shadow_only:
        raise BroadBSupplementError("SHADOW_CONFIRMATION_REQUIRED")
    api_key = _read_key(_resolve(root, contract["input"]["api_key_file"])) if apply else ""
    if apply and not api_key:
        raise BroadBSupplementError("DART_API_KEY_MISSING")
    replay = _replay(root=root, paths=paths, targets=targets, existing_receipts=existing_receipts, contract=contract)
    if not apply:
        return _summary(
            contract=contract, targets=targets, replay=replay, mode="DRY_RUN",
            generated_at=clock().isoformat(), run_requests=[], network_requests=0,
        )
    network_requests = 0
    run_requests: list[dict[str, Any]] = []
    while network_requests < budget:
        replay = _replay(root=root, paths=paths, targets=targets, existing_receipts=existing_receipts, contract=contract)
        pending = replay["list_pending"] or replay["document_pending"] or replay["viewer_pending"]
        if not pending:
            break
        item = pending[0]
        if run_requests and delay:
            sleep_fn(delay)
        try:
            if item["kind"] == "LIST":
                journal, used = _collect_list_page(
                    root=root, paths=paths, target=item["target"], page_no=item["page_no"],
                    api_key=api_key, contract=contract, fetcher=fetcher, sleep_fn=sleep_fn,
                    timeout=timeout, retries=retries, remaining_budget=budget-network_requests,
                    clock=clock,
                )
                run_requests.append({"kind": "LIST", "stock_code": item["target"]["stock_code"], "page_no": item["page_no"], "accepted_receipt_count": journal["accepted_receipt_count"]})
            elif item["kind"] == "DOCUMENT":
                journal, used = _collect_document(
                    root=root, paths=paths, row=item["row"], api_key=api_key,
                    contract=contract, fetcher=fetcher, sleep_fn=sleep_fn, timeout=timeout,
                    retries=retries, remaining_budget=budget-network_requests, clock=clock,
                )
                run_requests.append(
                    {
                        "kind": "DOCUMENT",
                        "receipt_no": item["row"]["rcept_no"],
                        "result_status": journal["status"],
                        "package_member_count": journal.get("package_member_count", 0),
                    }
                )
            else:
                viewer_paths = {
                    "raw": paths["viewer_raw"],
                    "pass": paths["viewer_pass"],
                    "fail": paths["viewer_fail"],
                }
                journal, used = shadow._collect_one(
                    root=root,
                    receipt=item["row"]["rcept_no"],
                    contract=contract,
                    paths=viewer_paths,
                    fetcher=fetcher,
                    sleep_fn=sleep_fn,
                    timeout=timeout,
                    retries=min(retries, max(0, budget - network_requests - 1)),
                    backoff=float(execution["retry_backoff_seconds"]),
                    delay=delay,
                    request_budget=budget - network_requests,
                    clock=clock,
                )
                run_requests.append(
                    {
                        "kind": "VIEWER",
                        "receipt_no": item["row"]["rcept_no"],
                        "document_count": journal["document_count"],
                    }
                )
            network_requests += used
        except (BroadBSupplementError, shadow.ShadowCollectionError) as exc:
            failure = {
                "strategy_id": STRATEGY_ID,
                "scope": "M1_C8_BROAD_B_LEGACY_SUPPLEMENT_FAILURE",
                "status": "SHADOW_FAIL",
                "promotion_allowed": False,
                "failed_at": clock().astimezone(timezone.utc).isoformat(),
                "reason_code": exc.code,
                "detail": exc.detail,
                "request": {
                    "kind": item["kind"],
                    "receipt_no": str(item.get("row", {}).get("rcept_no") or ""),
                },
            }
            stamp = clock().strftime("%Y%m%dT%H%M%S%fZ")
            shadow._write_immutable(paths["fail"] / f"{stamp}.json", _json_bytes(failure))
            replay = _replay(root=root, paths=paths, targets=targets, existing_receipts=existing_receipts, contract=contract)
            result = _summary(
                contract=contract, targets=targets, replay=replay, mode="APPLY_SHADOW_ONLY",
                generated_at=clock().isoformat(), run_requests=run_requests,
                network_requests=network_requests, integrity_issues=[exc.code],
            )
            _atomic_write(paths["checkpoint"], _json_bytes(result))
            _atomic_write(paths["report"], _json_bytes(result))
            return result
    replay = _replay(root=root, paths=paths, targets=targets, existing_receipts=existing_receipts, contract=contract)
    result = _summary(
        contract=contract, targets=targets, replay=replay, mode="APPLY_SHADOW_ONLY",
        generated_at=clock().isoformat(), run_requests=run_requests,
        network_requests=network_requests,
    )
    _atomic_write(paths["checkpoint"], _json_bytes(result))
    _atomic_write(paths["report"], _json_bytes(result))
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect C8 broad-B legacy merger-decision supplement")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--contract", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-shadow-only", action="store_true")
    parser.add_argument("--max-network-requests", type=int)
    parser.add_argument("--timeout", type=float)
    parser.add_argument("--delay", type=float)
    parser.add_argument("--retries", type=int)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        result = run_collection(
            root=args.root, contract_path=args.contract, apply=args.apply,
            confirm_shadow_only=args.confirm_shadow_only,
            max_network_requests=args.max_network_requests, timeout=args.timeout,
            delay=args.delay, retries=args.retries,
        )
    except (BroadBSupplementError, shadow.ShadowCollectionError, OSError, ValueError) as exc:
        print(json.dumps({"status": "FAIL", "reason_code": getattr(exc, "code", type(exc).__name__), "detail": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] != "SHADOW_FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
