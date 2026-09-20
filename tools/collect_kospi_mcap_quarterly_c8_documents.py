from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    _extract_package_members,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_capture import (
    evaluate_capture_manifest,
    load_corporate_action_adapter_contract,
    load_corporate_action_capture_contract,
    next_pending_requests,
    record_document,
    write_capture_manifest,
    write_capture_report,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_source_builder import (
    _atomic_write_bytes,
    _json_bytes,
    _write_immutable,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.contracts import ContractError


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
MANIFEST_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/data/acquisition/checkpoints/"
    "c8_corporate_action_capture_manifest_latest.json"
)
RAW_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/data/inbox/raw")
JOURNAL_DIR_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/data/acquisition/checkpoints/"
    "c8_document_response_journal"
)
UNAVAILABLE_JOURNAL_DIR_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/data/acquisition/checkpoints/"
    "c8_document_unavailable_response_journal"
)
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")
REPORT_LATEST_RELATIVE = REPORT_DIR_RELATIVE / "c8_document_collection_latest.json"
DEFAULT_KEY_FILE = Path("_cache/dart_api_key.txt")
RECEIPT_PATTERN = re.compile(r"^[0-9]{14}$")
Fetcher = Callable[[str, dict[str, str], float], tuple[int, bytes, Mapping[str, str]]]
Evaluator = Callable[..., dict[str, Any]]


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _resolve(root: Path, path: Path) -> Path:
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _read_key(path: Path) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8").replace("\ufeff", "").strip()


def _default_fetcher(
    endpoint: str, params: dict[str, str], timeout: float
) -> tuple[int, bytes, Mapping[str, str]]:
    response = requests.get(
        endpoint,
        params=params,
        headers={"User-Agent": "RootA-C8-document-collector/1.0"},
        timeout=timeout,
        allow_redirects=False,
    )
    return int(response.status_code), bytes(response.content), dict(response.headers)


def _journal_path(root: Path, receipt_no: str) -> Path:
    return root / JOURNAL_DIR_RELATIVE / f"{receipt_no}.json"


def _unavailable_journal_path(root: Path, receipt_no: str) -> Path:
    return root / UNAVAILABLE_JOURNAL_DIR_RELATIVE / f"{receipt_no}.json"


def _raw_relative(receipt_no: str, payload: bytes, captured: datetime) -> Path:
    digest = _sha256(payload)[:16]
    stamp = captured.strftime("%Y%m%dT%H%M%S%f%z")
    suffix = ".zip" if payload.startswith(b"PK") else (".xml" if payload.lstrip().startswith(b"<") else ".bin")
    return RAW_DIR_RELATIVE / f"opendart_document_{receipt_no}_{stamp}_{digest}{suffix}"


def _xml_status(payload: bytes) -> tuple[str, str]:
    if not payload.lstrip().startswith(b"<"):
        return "", ""
    try:
        root = ET.fromstring(payload)
    except ET.ParseError:
        return "", ""
    status = ""
    message = ""
    for node in root.iter():
        name = node.tag.rsplit("}", 1)[-1].lower()
        if name == "status" and not status:
            status = str(node.text or "").strip()
        elif name == "message" and not message:
            message = str(node.text or "").strip()
    return status, message


def _inspect_package(payload: bytes) -> dict[str, Any]:
    dart_status, dart_message = _xml_status(payload)
    if dart_status and dart_status != "000":
        return {
            "status": "FAIL",
            "reason_codes": [f"DART_DOCUMENT_STATUS_{dart_status}"],
            "dart_status": dart_status,
            "dart_message": dart_message,
            "package_member_count": 0,
        }
    members = _extract_package_members(payload)
    if not members:
        return {
            "status": "FAIL",
            "reason_codes": ["DART_DOCUMENT_PACKAGE_INVALID"],
            "dart_status": dart_status,
            "dart_message": dart_message,
            "package_member_count": 0,
        }
    return {
        "status": "PASS",
        "reason_codes": [],
        "dart_status": dart_status or "000",
        "dart_message": dart_message,
        "package_member_count": len(members),
    }


def _journal_record(
    *,
    receipt_no: str,
    inspection: Mapping[str, Any],
    raw_relative: Path,
    captured_at: str,
    http_status: int,
    server_date_header: str,
) -> dict[str, Any]:
    return {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_DOCUMENT_PASS_RESPONSE_JOURNAL",
        "request": {"kind": "DOCUMENT", "receipt_no": receipt_no},
        "status": "PASS",
        "http_status": http_status,
        "dart_status": inspection["dart_status"],
        "payload_sha256": inspection["payload_sha256"],
        "package_member_count": int(inspection["package_member_count"]),
        "captured_at": captured_at,
        "server_date_header": server_date_header,
        "raw_path": raw_relative.as_posix(),
    }


def _apply_pass_record(
    manifest: Mapping[str, Any], journal: Mapping[str, Any]
) -> dict[str, Any]:
    return record_document(
        manifest,
        receipt_no=str(journal["request"]["receipt_no"]),
        status="PASS",
        payload_sha256=journal["payload_sha256"],
        package_member_count=int(journal["package_member_count"]),
        captured_at=str(journal["captured_at"]),
    )


def _unavailable_journal_record(
    *,
    receipt_no: str,
    inspection: Mapping[str, Any],
    raw_relative: Path,
    captured_at: str,
    http_status: int,
    server_date_header: str,
) -> dict[str, Any]:
    return {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_DOCUMENT_UNAVAILABLE_RESPONSE_JOURNAL",
        "request": {"kind": "DOCUMENT", "receipt_no": receipt_no},
        "status": "UNAVAILABLE",
        "http_status": http_status,
        "dart_status": inspection["dart_status"],
        "dart_message": inspection["dart_message"],
        "payload_sha256": inspection["payload_sha256"],
        "package_member_count": 0,
        "captured_at": captured_at,
        "server_date_header": server_date_header,
        "raw_path": raw_relative.as_posix(),
        "error_code": "DART_DOCUMENT_STATUS_014",
    }


def _apply_unavailable_record(
    manifest: Mapping[str, Any], journal: Mapping[str, Any]
) -> dict[str, Any]:
    return record_document(
        manifest,
        receipt_no=str(journal["request"]["receipt_no"]),
        status="UNAVAILABLE",
        payload_sha256=journal["payload_sha256"],
        package_member_count=0,
        captured_at=str(journal["captured_at"]),
        error_code="DART_DOCUMENT_STATUS_014",
    )


def _recover_pass_journal(
    manifest: Mapping[str, Any], receipt_no: str, root: Path
) -> dict[str, Any] | None:
    path = _journal_path(root, receipt_no)
    if not path.is_file():
        return None
    journal = _load_json_object(path)
    expected_request = {"kind": "DOCUMENT", "receipt_no": receipt_no}
    if (
        journal.get("strategy_id") != STRATEGY_ID
        or journal.get("scope") != "M1_C8_DOCUMENT_PASS_RESPONSE_JOURNAL"
        or journal.get("status") != "PASS"
        or journal.get("request") != expected_request
        or journal.get("http_status") != 200
    ):
        raise ContractError("document recovery journal identity invalid")
    try:
        captured = datetime.fromisoformat(str(journal.get("captured_at") or ""))
    except ValueError as exc:
        raise ContractError("document recovery timestamp invalid") from exc
    if captured.tzinfo is None:
        raise ContractError("document recovery timestamp timezone missing")
    raw_value = str(journal.get("raw_path") or "")
    raw_path = _resolve(root, Path(raw_value))
    allowed = (root / RAW_DIR_RELATIVE).resolve()
    try:
        raw_path.relative_to(allowed)
    except ValueError as exc:
        raise ContractError("document recovery raw path invalid") from exc
    payload = raw_path.read_bytes()
    if _sha256(payload) != journal.get("payload_sha256"):
        raise ContractError("document recovery raw hash mismatch")
    inspection = _inspect_package(payload)
    inspection["payload_sha256"] = _sha256(payload)
    rebuilt = _journal_record(
        receipt_no=receipt_no,
        inspection=inspection,
        raw_relative=Path(raw_value),
        captured_at=str(journal.get("captured_at") or ""),
        http_status=200,
        server_date_header=str(journal.get("server_date_header") or ""),
    )
    if inspection.get("status") != "PASS" or rebuilt != journal:
        raise ContractError("document recovery journal content invalid")
    return _apply_pass_record(manifest, journal)


def _recover_unavailable_journal(
    manifest: Mapping[str, Any], receipt_no: str, root: Path
) -> dict[str, Any] | None:
    path = _unavailable_journal_path(root, receipt_no)
    if not path.is_file():
        return None
    journal = _load_json_object(path)
    expected_request = {"kind": "DOCUMENT", "receipt_no": receipt_no}
    if (
        journal.get("strategy_id") != STRATEGY_ID
        or journal.get("scope") != "M1_C8_DOCUMENT_UNAVAILABLE_RESPONSE_JOURNAL"
        or journal.get("status") != "UNAVAILABLE"
        or journal.get("request") != expected_request
        or journal.get("http_status") != 200
        or journal.get("dart_status") != "014"
        or journal.get("package_member_count") != 0
        or journal.get("error_code") != "DART_DOCUMENT_STATUS_014"
    ):
        raise ContractError("document unavailable journal identity invalid")
    try:
        captured = datetime.fromisoformat(str(journal.get("captured_at") or ""))
    except ValueError as exc:
        raise ContractError("document unavailable journal timestamp invalid") from exc
    if captured.tzinfo is None:
        raise ContractError("document unavailable journal timestamp timezone missing")
    raw_value = str(journal.get("raw_path") or "")
    raw_path = _resolve(root, Path(raw_value))
    allowed = (root / RAW_DIR_RELATIVE).resolve()
    try:
        raw_path.relative_to(allowed)
    except ValueError as exc:
        raise ContractError("document unavailable raw path invalid") from exc
    payload = raw_path.read_bytes()
    if _sha256(payload) != journal.get("payload_sha256"):
        raise ContractError("document unavailable raw hash mismatch")
    inspection = _inspect_package(payload)
    inspection["payload_sha256"] = _sha256(payload)
    rebuilt = _unavailable_journal_record(
        receipt_no=receipt_no,
        inspection=inspection,
        raw_relative=Path(raw_value),
        captured_at=str(journal.get("captured_at") or ""),
        http_status=200,
        server_date_header=str(journal.get("server_date_header") or ""),
    )
    if (
        inspection.get("status") != "FAIL"
        or inspection.get("reason_codes") != ["DART_DOCUMENT_STATUS_014"]
        or rebuilt != journal
    ):
        raise ContractError("document unavailable journal content invalid")
    return _apply_unavailable_record(manifest, journal)


def _recover_failed_014(
    manifest: Mapping[str, Any], receipt_no: str, root: Path
) -> dict[str, Any] | None:
    recovered = _recover_unavailable_journal(manifest, receipt_no, root)
    if recovered is not None:
        return recovered
    attempts = (
        manifest.get("checkpoint", {})
        .get("documents", {})
        .get(receipt_no, {})
        .get("attempts")
        or []
    )
    if not attempts:
        return None
    latest = attempts[-1]
    if (
        latest.get("status") != "FAIL"
        or latest.get("error_code") != "DART_DOCUMENT_STATUS_014"
        or latest.get("package_member_count") != 0
    ):
        return None
    digest = str(latest.get("payload_sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ContractError("failed 014 payload hash invalid")
    candidates = sorted(
        path
        for path in (root / RAW_DIR_RELATIVE).glob(
            f"opendart_document_{receipt_no}_*_{digest[:16]}.*"
        )
        if path.is_file() and _sha256(path.read_bytes()) == digest
    )
    if len(candidates) != 1:
        raise ContractError("failed 014 raw evidence must resolve uniquely")
    raw_path = candidates[0]
    payload = raw_path.read_bytes()
    inspection = _inspect_package(payload)
    inspection["payload_sha256"] = digest
    if (
        inspection.get("status") != "FAIL"
        or inspection.get("reason_codes") != ["DART_DOCUMENT_STATUS_014"]
    ):
        raise ContractError("failed 014 raw evidence content invalid")
    captured_at = str(latest.get("captured_at") or "")
    try:
        captured = datetime.fromisoformat(captured_at)
    except ValueError as exc:
        raise ContractError("failed 014 timestamp invalid") from exc
    if captured.tzinfo is None:
        raise ContractError("failed 014 timestamp timezone missing")
    journal = _unavailable_journal_record(
        receipt_no=receipt_no,
        inspection=inspection,
        raw_relative=raw_path.relative_to(root),
        captured_at=captured_at,
        http_status=200,
        server_date_header="",
    )
    _write_immutable(
        root,
        _unavailable_journal_path(root, receipt_no),
        _json_bytes(journal),
    )
    return _apply_unavailable_record(manifest, journal)


def _write_collection_report(root: Path, report: Mapping[str, Any]) -> list[str]:
    stamp = "".join(char for char in str(report["generated_at"]) if char.isdigit())[:20]
    versioned = root / REPORT_DIR_RELATIVE / f"c8_document_collection_{stamp}.json"
    latest = root / REPORT_LATEST_RELATIVE
    content = _json_bytes(dict(report))
    _write_immutable(root, versioned, content)
    _atomic_write_bytes(root, latest, content)
    return [str(versioned), str(latest)]


def run_collection(
    *,
    repo_root: Path,
    manifest_path: Path,
    api_key_file: Path,
    max_requests: int | None,
    apply: bool,
    confirmed: bool,
    timeout: float,
    fetcher: Fetcher | None = None,
    now: Callable[[], datetime] | None = None,
    evaluator: Evaluator | None = None,
) -> dict[str, Any]:
    root = repo_root.resolve()
    manifest_file = _resolve(root, manifest_path)
    key_file = _resolve(root, api_key_file)
    clock = now or (lambda: datetime.now().astimezone())
    generated_at = clock().isoformat(timespec="microseconds")
    reasons: list[str] = []
    endpoint = ""
    try:
        manifest = _load_json_object(manifest_file)
        capture_contract = load_corporate_action_capture_contract(root)
        adapter_contract = load_corporate_action_adapter_contract(root)
        endpoint = str(adapter_contract.get("endpoints", {}).get("document") or "")
    except (OSError, ValueError, json.JSONDecodeError, ContractError):
        manifest = {}
        capture_contract = {"request_budget": {"default_max_requests_per_run": 0}}
        adapter_contract = {}
        reasons.append("C8_DOCUMENT_COLLECTION_MANIFEST_OR_CONTRACT_INVALID")
    default_budget = int(
        capture_contract.get("request_budget", {}).get("default_max_requests_per_run") or 0
    )
    budget = default_budget if max_requests is None else int(max_requests)
    if budget < 1 or budget > default_budget:
        reasons.append("C8_DOCUMENT_COLLECTION_REQUEST_BUDGET_INVALID")
    if timeout <= 0:
        reasons.append("C8_DOCUMENT_COLLECTION_TIMEOUT_INVALID")
    if apply and not confirmed:
        reasons.append("C8_DOCUMENT_COLLECTION_CONFIRMATION_MISSING")
    key = _read_key(key_file)
    if not key:
        reasons.append("DART_API_KEY_MISSING")
    if manifest.get("strategy_id") != STRATEGY_ID:
        reasons.append("C8_DOCUMENT_COLLECTION_MANIFEST_STRATEGY_MISMATCH")
    if endpoint != "https://opendart.fss.or.kr/api/document.xml":
        reasons.append("C8_DOCUMENT_COLLECTION_ENDPOINT_INVALID")
    evaluate = evaluator or evaluate_capture_manifest
    current_assessment: dict[str, Any] = {}
    if not reasons:
        try:
            current_assessment = evaluate(manifest, repo_root=root, generated_at=generated_at)
        except (ContractError, OSError, ValueError, KeyError, TypeError):
            reasons.append("C8_DOCUMENT_COLLECTION_MANIFEST_EVALUATION_FAILED")
    if current_assessment and (
        current_assessment.get("manifest_integrity_pass") is not True
        or current_assessment.get("base_universe_authorized") is not True
        or current_assessment.get("bulk_requests_executed", 0) < 1
    ):
        reasons.append("C8_DOCUMENT_COLLECTION_MANIFEST_NOT_AUTHORIZED")

    first_pending: dict[str, Any] = {}
    if not reasons:
        pending = next_pending_requests(manifest, 1, repo_root=root)
        first_pending = dict(pending[0]) if pending else {}
        if first_pending and first_pending.get("kind") == "LIST_PAGE":
            reasons.append("C8_DOCUMENT_COLLECTION_LIST_STAGE_INCOMPLETE")
        elif first_pending and first_pending.get("kind") != "DOCUMENT":
            reasons.append("C8_DOCUMENT_COLLECTION_PENDING_KIND_INVALID")

    base: dict[str, Any] = {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_CORPORATE_ACTION_DOCUMENT_COLLECTION",
        "generated_at": generated_at,
        "status": "BLOCKED" if reasons else ("RUNNING" if apply else "READY"),
        "verdict": (
            "C8_DOCUMENT_COLLECTION_BLOCKED"
            if reasons
            else ("C8_DOCUMENT_COLLECTION_EXECUTING" if apply else "C8_DOCUMENT_COLLECTION_READY")
        ),
        "reason_codes": sorted(set(reasons)),
        "selection_as_of": manifest.get("selection_as_of"),
        "default_max_requests_per_run": default_budget,
        "max_requests_this_run": budget,
        "first_pending_request": first_pending,
        "network_request_count": 0,
        "recovered_pass_journal_count": 0,
        "recovered_unavailable_count": 0,
        "unavailable_count": 0,
        "pass_count": 0,
        "fail_count": 0,
        "hard_stop_triggered": False,
        "stop_reason": "",
        "request_results": [],
        "manifest_updated": False,
        "capture_status_updated": False,
        "full_capture_complete": False,
        "acquisition_evidence_published": False,
        "canonical_generated": False,
        "m2_allowed": False,
        "candidate_selection_calculated": False,
        "target_portfolio_calculated": False,
        "orders_generated": False,
        "operational_change": False,
        "outputs": [],
    }
    if reasons:
        return base
    if not first_pending:
        base["status"] = "PASS"
        base["verdict"] = "C8_DOCUMENT_COLLECTION_NO_PENDING_REQUESTS"
        base["full_capture_complete"] = bool(current_assessment.get("full_capture_complete"))
        return base
    if not apply:
        return base

    get = fetcher or _default_fetcher
    hard_stops = set(capture_contract.get("request_budget", {}).get("hard_stop_statuses") or [])
    current = manifest
    manifest_write_failed = False
    while base["network_request_count"] < budget:
        pending = next_pending_requests(current, 1, repo_root=root)
        if not pending:
            base["stop_reason"] = "NO_PENDING_REQUESTS"
            break
        request = dict(pending[0])
        if request.get("kind") != "DOCUMENT":
            base["status"] = "FAIL"
            base["verdict"] = "FAIL_C8_DOCUMENT_COLLECTION"
            base["reason_codes"] = ["C8_DOCUMENT_COLLECTION_PENDING_KIND_CHANGED"]
            base["hard_stop_triggered"] = True
            base["stop_reason"] = "PENDING_KIND_CHANGED"
            break
        receipt_no = str(request.get("receipt_no") or "")
        if not RECEIPT_PATTERN.fullmatch(receipt_no):
            base["status"] = "FAIL"
            base["verdict"] = "FAIL_C8_DOCUMENT_COLLECTION"
            base["reason_codes"] = ["C8_DOCUMENT_COLLECTION_RECEIPT_INVALID"]
            base["hard_stop_triggered"] = True
            base["stop_reason"] = "RECEIPT_IDENTITY_INVALID"
            break
        try:
            recovered_unavailable = _recover_failed_014(current, receipt_no, root)
        except (OSError, ValueError, json.JSONDecodeError, ContractError, KeyError, TypeError):
            base["status"] = "FAIL"
            base["verdict"] = "FAIL_C8_DOCUMENT_COLLECTION"
            base["reason_codes"] = ["C8_DOCUMENT_UNAVAILABLE_JOURNAL_RECOVERY_FAILED"]
            base["hard_stop_triggered"] = True
            base["stop_reason"] = "UNAVAILABLE_JOURNAL_INVALID"
            break
        if recovered_unavailable is not None:
            try:
                write_capture_manifest(recovered_unavailable, manifest_file, repo_root=root)
            except OSError:
                base["status"] = "FAIL"
                base["verdict"] = "FAIL_C8_DOCUMENT_COLLECTION"
                base["reason_codes"] = ["C8_DOCUMENT_MANIFEST_WRITE_FAILED"]
                base["hard_stop_triggered"] = True
                base["stop_reason"] = "MANIFEST_WRITE_FAILED"
                manifest_write_failed = True
                break
            current = recovered_unavailable
            base["recovered_unavailable_count"] += 1
            base["unavailable_count"] += 1
            base["manifest_updated"] = True
            continue
        try:
            recovered = _recover_pass_journal(current, receipt_no, root)
        except (OSError, ValueError, json.JSONDecodeError, ContractError, KeyError, TypeError):
            base["status"] = "FAIL"
            base["verdict"] = "FAIL_C8_DOCUMENT_COLLECTION"
            base["reason_codes"] = ["C8_DOCUMENT_PASS_JOURNAL_RECOVERY_FAILED"]
            base["hard_stop_triggered"] = True
            base["stop_reason"] = "PASS_JOURNAL_INVALID"
            break
        if recovered is not None:
            try:
                write_capture_manifest(recovered, manifest_file, repo_root=root)
            except OSError:
                base["status"] = "FAIL"
                base["verdict"] = "FAIL_C8_DOCUMENT_COLLECTION"
                base["reason_codes"] = ["C8_DOCUMENT_MANIFEST_WRITE_FAILED"]
                base["hard_stop_triggered"] = True
                base["stop_reason"] = "MANIFEST_WRITE_FAILED"
                manifest_write_failed = True
                break
            current = recovered
            base["recovered_pass_journal_count"] += 1
            base["manifest_updated"] = True
            continue

        params = {"crtfc_key": key, "rcept_no": receipt_no}
        try:
            http_status, payload, headers = get(endpoint, params, timeout)
            http_status = int(http_status)
            payload = bytes(payload)
            headers = dict(headers)
            fetch_error = ""
        except Exception:
            http_status, payload, headers = 0, b"", {}
            fetch_error = "DART_DOCUMENT_REQUEST_FAILED"
        base["network_request_count"] += 1
        captured = clock()
        captured_at = captured.isoformat(timespec="microseconds")
        raw_path: Path | None = None
        raw_relative: Path | None = None
        raw_write_error = ""
        if payload:
            raw_relative = _raw_relative(receipt_no, payload, captured)
            try:
                raw_path = _write_immutable(root, root / raw_relative, payload)
            except (OSError, ValueError):
                raw_write_error = "DART_DOCUMENT_RAW_WRITE_FAILED"

        inspection: dict[str, Any] = {}
        if not fetch_error and not raw_write_error and http_status == 200 and payload:
            inspection = _inspect_package(payload)
            inspection["payload_sha256"] = _sha256(payload)
        if fetch_error:
            error_codes = [fetch_error]
        elif http_status != 200:
            error_codes = [f"DART_DOCUMENT_HTTP_{http_status}"]
        elif not payload:
            error_codes = ["DART_DOCUMENT_RESPONSE_EMPTY"]
        elif raw_write_error:
            error_codes = [raw_write_error]
        else:
            error_codes = list(inspection.get("reason_codes") or [])
        passed = not error_codes and inspection.get("status") == "PASS"
        checkpointed_unavailable = False
        if passed and raw_relative is not None:
            journal = _journal_record(
                receipt_no=receipt_no,
                inspection=inspection,
                raw_relative=raw_relative,
                captured_at=captured_at,
                http_status=http_status,
                server_date_header=str(headers.get("Date") or headers.get("date") or ""),
            )
            try:
                _write_immutable(root, _journal_path(root, receipt_no), _json_bytes(journal))
                updated = _apply_pass_record(current, journal)
            except (OSError, ValueError, ContractError, KeyError, TypeError):
                error_codes = ["DART_DOCUMENT_PASS_JOURNAL_WRITE_FAILED"]
                passed = False
            else:
                base["pass_count"] += 1
        if (
            not passed
            and error_codes == ["DART_DOCUMENT_STATUS_014"]
            and raw_relative is not None
            and inspection.get("dart_status") == "014"
        ):
            journal = _unavailable_journal_record(
                receipt_no=receipt_no,
                inspection=inspection,
                raw_relative=raw_relative,
                captured_at=captured_at,
                http_status=http_status,
                server_date_header=str(headers.get("Date") or headers.get("date") or ""),
            )
            try:
                _write_immutable(
                    root,
                    _unavailable_journal_path(root, receipt_no),
                    _json_bytes(journal),
                )
                updated = _apply_unavailable_record(current, journal)
            except (OSError, ValueError, ContractError, KeyError, TypeError):
                error_codes = ["DART_DOCUMENT_UNAVAILABLE_JOURNAL_WRITE_FAILED"]
            else:
                checkpointed_unavailable = True
                base["unavailable_count"] += 1
        if not passed and not checkpointed_unavailable:
            updated = record_document(
                current,
                receipt_no=receipt_no,
                status="FAIL",
                payload_sha256=_sha256(payload),
                package_member_count=0,
                captured_at=captured_at,
                error_code="|".join(error_codes) or "DART_DOCUMENT_PACKAGE_INVALID",
            )
            base["fail_count"] += 1

        try:
            write_capture_manifest(updated, manifest_file, repo_root=root)
        except OSError:
            base["status"] = "FAIL"
            base["verdict"] = "FAIL_C8_DOCUMENT_COLLECTION"
            base["reason_codes"] = ["C8_DOCUMENT_MANIFEST_WRITE_FAILED"]
            base["hard_stop_triggered"] = True
            base["stop_reason"] = "MANIFEST_WRITE_FAILED"
            manifest_write_failed = True
        else:
            current = updated
            base["manifest_updated"] = True
        result = {
            "request": request,
            "http_status": http_status,
            "dart_status": inspection.get("dart_status"),
            "status": (
                "PASS" if passed else ("UNAVAILABLE" if checkpointed_unavailable else "FAIL")
            ),
            "reason_codes": error_codes,
            "payload_sha256": _sha256(payload),
            "raw_path": str(raw_path) if raw_path is not None else None,
            "package_member_count": int(inspection.get("package_member_count") or 0),
        }
        base["request_results"].append(result)
        if manifest_write_failed:
            break
        if checkpointed_unavailable:
            continue
        if not passed:
            dart_status = str(inspection.get("dart_status") or "")
            base["status"] = "FAIL"
            base["verdict"] = "FAIL_C8_DOCUMENT_COLLECTION"
            base["reason_codes"] = sorted(set(error_codes))
            base["hard_stop_triggered"] = True
            base["stop_reason"] = (
                "DART_REQUEST_LIMIT_EXCEEDED"
                if dart_status in hard_stops
                else "DOCUMENT_REQUEST_OR_RESPONSE_FAILED"
            )
            break

    persisted = _load_json_object(manifest_file) if manifest_write_failed else current
    final_assessment = evaluate(persisted, repo_root=root, generated_at=clock().isoformat())
    status_outputs: list[str] = []
    if base["manifest_updated"]:
        status_outputs = [
            str(path) for path in write_capture_report(final_assessment, repo_root=root)
        ]
        base["capture_status_updated"] = True
    remaining = next_pending_requests(persisted, 1, repo_root=root)
    base["full_capture_complete"] = bool(final_assessment.get("full_capture_complete"))
    base["next_pending_request"] = dict(remaining[0]) if remaining else {}
    if base["status"] != "FAIL":
        base["status"] = "PASS"
        if not remaining:
            base["stop_reason"] = "NO_PENDING_REQUESTS"
            base["verdict"] = (
                "C8_DOCUMENT_COLLECTION_FULL_CAPTURE_COMPLETE"
                if base["full_capture_complete"]
                else "C8_DOCUMENT_COLLECTION_NO_PENDING_INCOMPLETE"
            )
        else:
            base["verdict"] = "C8_DOCUMENT_BUDGET_EXHAUSTED_CHECKPOINT_SAVED"
            base["stop_reason"] = "REQUEST_BUDGET_REACHED"
    try:
        collection_outputs = _write_collection_report(root, base)
    except (OSError, ValueError):
        collection_outputs = []
        base["collection_report_write_failed"] = True
    base["outputs"] = (
        ([str(manifest_file)] if base["manifest_updated"] else [])
        + status_outputs
        + collection_outputs
    )
    return base


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect restartable OpenDART original document packages only; downstream remains closed."
    )
    parser.add_argument("--manifest", type=Path, default=MANIFEST_RELATIVE)
    parser.add_argument("--api-key-file", type=Path, default=DEFAULT_KEY_FILE)
    parser.add_argument("--max-requests", type=int)
    parser.add_argument("--timeout", type=float, default=40.0)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-documents-only", action="store_true")
    parser.add_argument("--expect-verdict", default="")
    args = parser.parse_args()
    report = run_collection(
        repo_root=ROOT,
        manifest_path=args.manifest,
        api_key_file=args.api_key_file,
        max_requests=args.max_requests,
        apply=args.apply,
        confirmed=args.confirm_documents_only,
        timeout=args.timeout,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.expect_verdict:
        return 0 if report["verdict"] == args.expect_verdict else 3
    return 0 if report["status"] in {"READY", "PASS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
