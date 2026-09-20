from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    build_list_request_spec,
    parse_disclosure_list_page,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_capture import (
    evaluate_capture_manifest,
    load_corporate_action_capture_contract,
    next_pending_requests,
    record_list_page,
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
    "c8_list_page_response_journal_merger_only_v1"
)
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")
REPORT_LATEST_RELATIVE = REPORT_DIR_RELATIVE / "c8_list_page_collection_latest.json"
DEFAULT_KEY_FILE = Path("_cache/dart_api_key.txt")
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
        headers={"User-Agent": "RootA-C8-list-page-collector/1.0"},
        timeout=timeout,
        allow_redirects=False,
    )
    return int(response.status_code), bytes(response.content), dict(response.headers)


def _request_token(request: Mapping[str, Any]) -> str:
    return "_".join(
        [
            str(request["code"]),
            str(request["series"]),
            f"p{int(request['page_no']):04d}",
        ]
    )


def _journal_path(root: Path, request: Mapping[str, Any]) -> Path:
    return root / JOURNAL_DIR_RELATIVE / f"{_request_token(request)}.json"


def _raw_relative(
    request: Mapping[str, Any], payload: bytes, captured: datetime
) -> Path:
    digest = _sha256(payload)[:16]
    stamp = captured.strftime("%Y%m%dT%H%M%S%f%z")
    suffix = ".json" if payload.lstrip().startswith((b"{", b"[")) else ".bin"
    return RAW_DIR_RELATIVE / (
        f"opendart_list_{_request_token(request)}_{stamp}_{digest}{suffix}"
    )


def _request_spec(
    manifest: Mapping[str, Any], request: Mapping[str, Any], root: Path
) -> dict[str, Any]:
    return build_list_request_spec(
        corp_code=request["corp_code"],
        query_start=manifest["query_start"],
        query_end=manifest["query_end"],
        series=str(request["series"]),
        page_no=int(request["page_no"]),
        repo_root=root,
    )


def _journal_record(
    *,
    request: Mapping[str, Any],
    parsed: Mapping[str, Any],
    raw_relative: Path,
    captured_at: str,
    http_status: int,
    server_date_header: str,
) -> dict[str, Any]:
    return {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_LIST_PAGE_PASS_RESPONSE_JOURNAL",
        "request": dict(request),
        "status": "PASS",
        "http_status": http_status,
        "dart_status": parsed["dart_status"],
        "payload_sha256": parsed["payload_sha256"],
        "row_count": int(parsed["accepted_row_count"]),
        "total_page": int(parsed["total_page"]),
        "accepted_receipts": sorted(
            {str(row["rcept_no"]) for row in parsed["accepted_rows"]}
        ),
        "captured_at": captured_at,
        "server_date_header": server_date_header,
        "raw_path": raw_relative.as_posix(),
    }


def _apply_pass_record(
    manifest: Mapping[str, Any], journal: Mapping[str, Any], root: Path
) -> dict[str, Any]:
    request = journal["request"]
    return record_list_page(
        manifest,
        code=str(request["code"]),
        series=str(request["series"]),
        page_no=int(request["page_no"]),
        status="PASS",
        dart_status=str(journal["dart_status"]),
        payload_sha256=journal["payload_sha256"],
        row_count=int(journal["row_count"]),
        total_page=int(journal["total_page"]),
        accepted_receipts=list(journal["accepted_receipts"]),
        captured_at=str(journal["captured_at"]),
        repo_root=root,
    )


def _recover_pass_journal(
    manifest: Mapping[str, Any], request: Mapping[str, Any], root: Path
) -> dict[str, Any] | None:
    path = _journal_path(root, request)
    if not path.is_file():
        return None
    journal = _load_json_object(path)
    if (
        journal.get("strategy_id") != STRATEGY_ID
        or journal.get("scope") != "M1_C8_LIST_PAGE_PASS_RESPONSE_JOURNAL"
        or journal.get("status") != "PASS"
        or journal.get("request") != dict(request)
        or journal.get("http_status") != 200
    ):
        raise ContractError("list page recovery journal identity invalid")
    try:
        captured = datetime.fromisoformat(str(journal.get("captured_at") or ""))
    except ValueError as exc:
        raise ContractError("list page recovery timestamp invalid") from exc
    if captured.tzinfo is None:
        raise ContractError("list page recovery timestamp timezone missing")
    raw_value = str(journal.get("raw_path") or "")
    raw_path = _resolve(root, Path(raw_value))
    allowed = (root / RAW_DIR_RELATIVE).resolve()
    try:
        raw_path.relative_to(allowed)
    except ValueError as exc:
        raise ContractError("list page recovery raw path invalid") from exc
    payload = raw_path.read_bytes()
    if _sha256(payload) != journal.get("payload_sha256"):
        raise ContractError("list page recovery raw hash mismatch")
    spec = _request_spec(manifest, request, root)
    parsed = parse_disclosure_list_page(
        payload,
        request_spec=spec,
        expected_stock_code=str(request["code"]),
        repo_root=root,
    )
    rebuilt = _journal_record(
        request=request,
        parsed=parsed,
        raw_relative=Path(raw_value),
        captured_at=str(journal.get("captured_at") or ""),
        http_status=200,
        server_date_header=str(journal.get("server_date_header") or ""),
    )
    if parsed.get("status") != "PASS" or rebuilt != journal:
        raise ContractError("list page recovery journal content invalid")
    return _apply_pass_record(manifest, journal, root)


def _write_collection_report(root: Path, report: Mapping[str, Any]) -> list[str]:
    stamp = "".join(char for char in str(report["generated_at"]) if char.isdigit())[:20]
    versioned = root / REPORT_DIR_RELATIVE / f"c8_list_page_collection_{stamp}.json"
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
    try:
        manifest = _load_json_object(manifest_file)
        contract = load_corporate_action_capture_contract(root)
    except (OSError, ValueError, json.JSONDecodeError, ContractError):
        manifest = {}
        contract = {"request_budget": {"default_max_requests_per_run": 0}}
        reasons.append("C8_LIST_COLLECTION_MANIFEST_OR_CONTRACT_INVALID")
    default_budget = int(
        contract.get("request_budget", {}).get("default_max_requests_per_run") or 0
    )
    budget = default_budget if max_requests is None else int(max_requests)
    if budget < 1 or budget > default_budget:
        reasons.append("C8_LIST_COLLECTION_REQUEST_BUDGET_INVALID")
    if timeout <= 0:
        reasons.append("C8_LIST_COLLECTION_TIMEOUT_INVALID")
    if apply and not confirmed:
        reasons.append("C8_LIST_COLLECTION_CONFIRMATION_MISSING")
    key = _read_key(key_file)
    if not key:
        reasons.append("DART_API_KEY_MISSING")
    if manifest.get("strategy_id") != STRATEGY_ID:
        reasons.append("C8_LIST_COLLECTION_MANIFEST_STRATEGY_MISMATCH")
    evaluate = evaluator or evaluate_capture_manifest
    current_assessment: dict[str, Any] = {}
    if not reasons:
        try:
            current_assessment = evaluate(
                manifest, repo_root=root, generated_at=generated_at
            )
        except (ContractError, OSError, ValueError, KeyError, TypeError):
            reasons.append("C8_LIST_COLLECTION_MANIFEST_EVALUATION_FAILED")
    if current_assessment and (
        current_assessment.get("manifest_integrity_pass") is not True
        or current_assessment.get("base_universe_authorized") is not True
        or current_assessment.get("bulk_requests_executed", 0) < 1
    ):
        reasons.append("C8_LIST_COLLECTION_MANIFEST_NOT_AUTHORIZED")

    first_pending: dict[str, Any] = {}
    if not reasons:
        pending = next_pending_requests(manifest, 1, repo_root=root)
        first_pending = dict(pending[0]) if pending else {}
        if first_pending and first_pending.get("kind") not in {"LIST_PAGE", "DOCUMENT"}:
            reasons.append("C8_LIST_COLLECTION_PENDING_KIND_INVALID")

    base = {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_CORPORATE_ACTION_LIST_PAGE_COLLECTION",
        "generated_at": generated_at,
        "status": "BLOCKED" if reasons else ("RUNNING" if apply else "READY"),
        "verdict": (
            "C8_LIST_PAGE_COLLECTION_BLOCKED"
            if reasons
            else (
                "C8_LIST_PAGE_COLLECTION_EXECUTING"
                if apply
                else "C8_LIST_PAGE_COLLECTION_READY"
            )
        ),
        "reason_codes": sorted(set(reasons)),
        "selection_as_of": manifest.get("selection_as_of"),
        "query_start": manifest.get("query_start"),
        "query_end": manifest.get("query_end"),
        "default_max_requests_per_run": default_budget,
        "max_requests_this_run": budget,
        "first_pending_request": first_pending,
        "network_request_count": 0,
        "recovered_pass_journal_count": 0,
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
        base["verdict"] = "C8_LIST_PAGE_COLLECTION_NO_PENDING_REQUESTS"
        return base
    if first_pending.get("kind") == "DOCUMENT":
        base["status"] = "PASS"
        base["verdict"] = "C8_LIST_PAGE_STAGE_COMPLETE_DOCUMENTS_PENDING"
        base["stop_reason"] = "DOCUMENT_REQUESTS_OUTSIDE_SCOPE"
        return base
    if not apply:
        return base

    get = fetcher or _default_fetcher
    current = manifest
    hard_stops = set(contract.get("request_budget", {}).get("hard_stop_statuses") or [])
    while base["network_request_count"] < budget:
        pending = next_pending_requests(current, 1, repo_root=root)
        if not pending:
            base["stop_reason"] = "NO_PENDING_REQUESTS"
            break
        request = dict(pending[0])
        if request.get("kind") != "LIST_PAGE":
            base["stop_reason"] = "DOCUMENT_REQUESTS_OUTSIDE_SCOPE"
            break
        try:
            recovered = _recover_pass_journal(current, request, root)
        except (OSError, ValueError, json.JSONDecodeError, ContractError, KeyError, TypeError):
            base["status"] = "FAIL"
            base["verdict"] = "FAIL_C8_LIST_PAGE_COLLECTION"
            base["reason_codes"] = ["C8_LIST_PASS_JOURNAL_RECOVERY_FAILED"]
            base["hard_stop_triggered"] = True
            base["stop_reason"] = "PASS_JOURNAL_INVALID"
            break
        if recovered is not None:
            current = recovered
            write_capture_manifest(current, manifest_file, repo_root=root)
            base["recovered_pass_journal_count"] += 1
            base["manifest_updated"] = True
            continue

        spec = _request_spec(current, request, root)
        params = {"crtfc_key": key, **spec["params"]}
        try:
            http_status, payload, headers = get(spec["endpoint"], params, timeout)
            http_status = int(http_status)
            payload = bytes(payload)
            headers = dict(headers)
            fetch_error = ""
        except Exception:
            http_status, payload, headers = 0, b"", {}
            fetch_error = "DART_LIST_REQUEST_FAILED"
        base["network_request_count"] += 1
        captured = clock()
        captured_at = captured.isoformat(timespec="microseconds")
        raw_path: Path | None = None
        raw_relative: Path | None = None
        if payload:
            raw_relative = _raw_relative(request, payload, captured)
            raw_path = _write_immutable(root, root / raw_relative, payload)

        parsed: dict[str, Any] = {}
        if not fetch_error and http_status == 200 and payload:
            parsed = parse_disclosure_list_page(
                payload,
                request_spec=spec,
                expected_stock_code=str(request["code"]),
                repo_root=root,
            )
        if fetch_error:
            error_codes = [fetch_error]
        elif http_status != 200:
            error_codes = [f"DART_LIST_HTTP_{http_status}"]
        elif not payload:
            error_codes = ["DART_LIST_RESPONSE_EMPTY"]
        else:
            error_codes = list(parsed.get("reason_codes") or [])
        passed = not error_codes and parsed.get("status") == "PASS"
        if passed and raw_relative is not None:
            journal = _journal_record(
                request=request,
                parsed=parsed,
                raw_relative=raw_relative,
                captured_at=captured_at,
                http_status=http_status,
                server_date_header=str(headers.get("Date") or headers.get("date") or ""),
            )
            _write_immutable(root, _journal_path(root, request), _json_bytes(journal))
            current = _apply_pass_record(current, journal, root)
            base["pass_count"] += 1
        else:
            dart_status = str(parsed.get("dart_status") or "")
            payload_hash = _sha256(payload)
            current = record_list_page(
                current,
                code=str(request["code"]),
                series=str(request["series"]),
                page_no=int(request["page_no"]),
                status="FAIL",
                dart_status=dart_status,
                payload_sha256=payload_hash,
                row_count=0,
                total_page=0,
                accepted_receipts=[],
                captured_at=captured_at,
                error_code="|".join(error_codes) or "DART_LIST_PARSE_FAILED",
                repo_root=root,
            )
            base["fail_count"] += 1

        write_capture_manifest(current, manifest_file, repo_root=root)
        base["manifest_updated"] = True
        result = {
            "request": request,
            "http_status": http_status,
            "dart_status": parsed.get("dart_status"),
            "status": "PASS" if passed else "FAIL",
            "reason_codes": error_codes,
            "payload_sha256": _sha256(payload),
            "raw_path": str(raw_path) if raw_path is not None else None,
            "accepted_row_count": int(parsed.get("accepted_row_count") or 0),
            "total_page": int(parsed.get("total_page") or 0),
        }
        base["request_results"].append(result)
        if not passed:
            base["status"] = "FAIL"
            base["verdict"] = "FAIL_C8_LIST_PAGE_COLLECTION"
            base["reason_codes"] = sorted(set(error_codes))
            base["hard_stop_triggered"] = True
            base["stop_reason"] = (
                "DART_REQUEST_LIMIT_EXCEEDED"
                if str(parsed.get("dart_status") or "") in hard_stops
                else "LIST_PAGE_REQUEST_OR_RESPONSE_FAILED"
            )
            break

    final_assessment = evaluate(current, repo_root=root, generated_at=clock().isoformat())
    status_outputs: list[str] = []
    if base["manifest_updated"]:
        status_outputs = [
            str(path) for path in write_capture_report(final_assessment, repo_root=root)
        ]
        base["capture_status_updated"] = True
    remaining = next_pending_requests(current, 1, repo_root=root)
    base["full_capture_complete"] = bool(final_assessment.get("full_capture_complete"))
    base["next_pending_request"] = dict(remaining[0]) if remaining else {}
    if base["status"] != "FAIL":
        base["status"] = "PASS"
        if not remaining:
            base["verdict"] = "C8_LIST_PAGE_COLLECTION_ALL_CAPTURE_REQUESTS_COMPLETE"
        elif remaining[0].get("kind") == "DOCUMENT":
            base["verdict"] = "C8_LIST_PAGE_STAGE_COMPLETE_DOCUMENTS_PENDING"
            base["stop_reason"] = "DOCUMENT_REQUESTS_OUTSIDE_SCOPE"
        else:
            base["verdict"] = "C8_LIST_PAGE_BUDGET_EXHAUSTED_CHECKPOINT_SAVED"
            base["stop_reason"] = "REQUEST_BUDGET_REACHED"
    base["outputs"] = (
        ([str(manifest_file)] if base["manifest_updated"] else [])
        + status_outputs
        + _write_collection_report(root, base)
    )
    return base


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect restartable OpenDART list pages only; documents and downstream remain closed."
    )
    parser.add_argument("--manifest", type=Path, default=MANIFEST_RELATIVE)
    parser.add_argument("--api-key-file", type=Path, default=DEFAULT_KEY_FILE)
    parser.add_argument("--max-requests", type=int)
    parser.add_argument("--timeout", type=float, default=40.0)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-list-pages-only", action="store_true")
    parser.add_argument("--expect-verdict", default="")
    args = parser.parse_args()
    report = run_collection(
        repo_root=ROOT,
        manifest_path=args.manifest,
        api_key_file=args.api_key_file,
        max_requests=args.max_requests,
        apply=args.apply,
        confirmed=args.confirm_list_pages_only,
        timeout=args.timeout,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.expect_verdict:
        return 0 if report["verdict"] == args.expect_verdict else 3
    return 0 if report["status"] in {"READY", "PASS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
