from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import parse_qs, urlparse

import requests


ROOT = Path(__file__).resolve().parents[1]
STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_dart_viewer_shadow_contract_v1.json"
)
RECEIPT_PATTERN = re.compile(r"^[0-9]{14}$")
TITLE_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
VIEW_DOC_PATTERN = re.compile(
    r"viewDoc\s*\(\s*['\"](?P<rcpNo>[0-9]{14})['\"]\s*,\s*"
    r"['\"](?P<dcmNo>[0-9]+)['\"]\s*,\s*['\"](?P<eleId>[0-9]+)['\"]\s*,\s*"
    r"['\"](?P<offset>[0-9]+)['\"]\s*,\s*['\"](?P<length>[0-9]+)['\"]\s*,\s*"
    r"['\"](?P<dtd>[A-Za-z0-9_.-]+)['\"]\s*\)",
    re.IGNORECASE,
)
VIEWER_URL_PATTERN = re.compile(
    r"(?:https?://dart\.fss\.or\.kr)?/report/viewer\.do\?[^\s'\"<>]+",
    re.IGNORECASE,
)
NODE_BLOCK_PATTERN = re.compile(
    r"var\s+(?P<name>node[0-9]+)\s*=\s*\{\}\s*;(?P<body>.*?)"
    r"treeData\.push\(\s*(?P=name)\s*\)\s*;",
    re.IGNORECASE | re.DOTALL,
)
ATTACHMENT_SELECT_PATTERN = re.compile(
    r"<select\b[^>]*\bid\s*=\s*['\"]att['\"][^>]*>(?P<body>.*?)</select>",
    re.IGNORECASE | re.DOTALL,
)
OPTION_PATTERN = re.compile(
    r"<option\b[^>]*\bvalue\s*=\s*['\"](?P<value>[^'\"]+)['\"][^>]*>"
    r"(?P<label>.*?)</option>",
    re.IGNORECASE | re.DOTALL,
)
Fetcher = Callable[
    [str, dict[str, str], Mapping[str, str], float],
    tuple[int, bytes, Mapping[str, str]],
]


class ShadowCollectionError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail or code


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ShadowCollectionError("JSON_OBJECT_REQUIRED", str(path))
    return payload


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp")
    temp.write_bytes(payload)
    temp.replace(path)


def _write_immutable(path: Path, payload: bytes) -> None:
    if path.exists():
        if path.read_bytes() != payload:
            raise ShadowCollectionError("IMMUTABLE_WRITE_CONFLICT", str(path))
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _decode_html(payload: bytes) -> str:
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", errors="replace")


def _clean_title(text: str) -> str:
    match = TITLE_PATTERN.search(text)
    if not match:
        return ""
    without_tags = re.sub(r"<[^>]+>", " ", match.group(1))
    return " ".join(html.unescape(without_tags).split())


def _reference_key(ref: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(ref[key] for key in ("rcpNo", "dcmNo", "eleId", "offset", "length", "dtd"))


def parse_viewer_references(payload: bytes) -> list[dict[str, str]]:
    text = html.unescape(_decode_html(payload))
    refs: dict[tuple[str, ...], dict[str, str]] = {}
    for match in VIEW_DOC_PATTERN.finditer(text):
        ref = {key: value for key, value in match.groupdict().items()}
        refs[_reference_key(ref)] = ref
    for match in VIEWER_URL_PATTERN.finditer(text):
        query = parse_qs(urlparse(match.group(0)).query, keep_blank_values=True)
        required = ("rcpNo", "dcmNo", "eleId", "offset", "length", "dtd")
        if not all(query.get(key, [""])[0] for key in required):
            continue
        ref = {key: query[key][0] for key in required}
        if (
            RECEIPT_PATTERN.fullmatch(ref["rcpNo"])
            and all(ref[key].isdigit() for key in ("dcmNo", "eleId", "offset", "length"))
            and re.fullmatch(r"[A-Za-z0-9_.-]+", ref["dtd"])
        ):
            refs[_reference_key(ref)] = ref
    required = ("rcpNo", "dcmNo", "eleId", "offset", "length", "dtd")
    for block in NODE_BLOCK_PATTERN.finditer(text):
        name = re.escape(block.group("name"))
        body = block.group("body")
        ref: dict[str, str] = {}
        for key in required:
            field = re.search(
                rf"{name}\[['\"]{key}['\"]\]\s*=\s*['\"](?P<value>[^'\"]+)['\"]",
                body,
                re.IGNORECASE,
            )
            if field:
                ref[key] = field.group("value")
        if (
            set(ref) == set(required)
            and RECEIPT_PATTERN.fullmatch(ref["rcpNo"])
            and all(ref[key].isdigit() for key in ("dcmNo", "eleId", "offset", "length"))
            and re.fullmatch(r"[A-Za-z0-9_.-]+", ref["dtd"])
        ):
            refs[_reference_key(ref)] = ref
    return [refs[key] for key in sorted(refs)]


def _option_reference(match: re.Match[str]) -> dict[str, str] | None:
    query = parse_qs(html.unescape(match.group("value")), keep_blank_values=True)
    receipt = query.get("rcpNo", [""])[0]
    dcm_no = query.get("dcmNo", [""])[0]
    if not RECEIPT_PATTERN.fullmatch(receipt) or not dcm_no.isdigit():
        return None
    label = re.sub(r"<[^>]+>", " ", match.group("label"))
    return {
        "rcpNo": receipt,
        "dcmNo": dcm_no,
        "label": " ".join(html.unescape(label).split()),
    }


def parse_attachment_references(payload: bytes, target_receipt: str) -> list[dict[str, str]]:
    text = _decode_html(payload)
    refs: dict[tuple[str, str], dict[str, str]] = {}
    for select in ATTACHMENT_SELECT_PATTERN.finditer(text):
        for option in OPTION_PATTERN.finditer(select.group("body")):
            ref = _option_reference(option)
            if ref and ref["rcpNo"] == target_receipt:
                refs[(ref["rcpNo"], ref["dcmNo"])] = ref
    return [refs[key] for key in sorted(refs)]


def parse_related_receipt_links(payload: bytes) -> list[dict[str, str]]:
    text = _decode_html(payload)
    refs: dict[tuple[str, str], dict[str, str]] = {}
    for option in OPTION_PATTERN.finditer(text):
        ref = _option_reference(option)
        if ref:
            refs[(ref["rcpNo"], ref["dcmNo"])] = ref
    return [refs[key] for key in sorted(refs)]


def _default_fetcher(
    endpoint: str,
    params: dict[str, str],
    headers: Mapping[str, str],
    timeout: float,
) -> tuple[int, bytes, Mapping[str, str]]:
    response = requests.get(
        endpoint,
        params=params,
        headers=dict(headers),
        timeout=timeout,
        allow_redirects=False,
    )
    return int(response.status_code), bytes(response.content), dict(response.headers)


def _fetch_with_retry(
    *,
    endpoint: str,
    params: dict[str, str],
    headers: Mapping[str, str],
    timeout: float,
    retries: int,
    backoff: float,
    fetcher: Fetcher,
    sleep_fn: Callable[[float], None],
) -> tuple[int, bytes, Mapping[str, str], int]:
    last_error = ""
    for attempt in range(retries + 1):
        try:
            status, payload, response_headers = fetcher(endpoint, params, headers, timeout)
            if status in {429, 502, 503, 504} and attempt < retries:
                sleep_fn(backoff * (attempt + 1))
                continue
            return status, payload, response_headers, attempt + 1
        except (requests.RequestException, OSError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < retries:
                sleep_fn(backoff * (attempt + 1))
                continue
    raise ShadowCollectionError("NA_CONNECTION_LIMIT", last_error or "request failed")


def _load_context(root: Path, contract_path: Path) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    contract = _load_json_object(contract_path)
    if contract.get("strategy_id") != STRATEGY_ID or contract.get("mode") != "SHADOW_ONLY":
        raise ShadowCollectionError("SHADOW_CONTRACT_INVALID")
    input_contract = contract.get("input") or {}
    status_path = _resolve(root, str(input_contract.get("capture_status_path", "")))
    status = _load_json_object(status_path)
    reasons = [str(item) for item in (status.get("reason_codes") or [])]
    if status.get("status") != input_contract.get("required_capture_status"):
        raise ShadowCollectionError("CAPTURE_STATUS_NOT_BLOCKED")
    if input_contract.get("required_reason_code") not in reasons:
        raise ShadowCollectionError("UNAVAILABLE_REASON_NOT_PRESENT")
    receipts = [str(item) for item in (status.get(input_contract.get("receipt_field")) or [])]
    if len(receipts) != len(set(receipts)) or receipts != sorted(receipts):
        raise ShadowCollectionError("UNAVAILABLE_RECEIPTS_NOT_UNIQUE_SORTED")
    expected_count = int(input_contract.get("expected_receipt_count", -1))
    if len(receipts) != expected_count:
        raise ShadowCollectionError(
            "UNAVAILABLE_RECEIPT_COUNT_MISMATCH", f"expected={expected_count}, actual={len(receipts)}"
        )
    if not receipts or any(not RECEIPT_PATTERN.fullmatch(item) for item in receipts):
        raise ShadowCollectionError("UNAVAILABLE_RECEIPT_INVALID")
    if int(status.get("unavailable_document_count", -1)) != len(receipts):
        raise ShadowCollectionError("UNAVAILABLE_COUNT_FIELD_MISMATCH")
    return contract, status, receipts


def _output_paths(root: Path, contract: Mapping[str, Any]) -> dict[str, Path]:
    output = contract["output"]
    shadow_root = _resolve(root, output["root"])
    return {
        "root": shadow_root,
        "raw": shadow_root / output["raw_dir"],
        "pass": shadow_root / output["pass_journal_dir"],
        "fail": shadow_root / output["failure_journal_dir"],
        "checkpoint": shadow_root / output["checkpoint_path"],
        "report": _resolve(root, output["report_path"]),
    }


def _pass_journal(path: Path, receipt: str) -> dict[str, Any] | None:
    target = path / f"{receipt}.json"
    if not target.is_file():
        return None
    payload = _load_json_object(target)
    if payload.get("receipt_no") != receipt or payload.get("status") != "SHADOW_PASS":
        raise ShadowCollectionError("PASS_JOURNAL_INVALID", str(target))
    return payload


def _artifact_record(root: Path, path: Path, payload: bytes, kind: str) -> dict[str, Any]:
    return {
        "kind": kind,
        "path": path.relative_to(root).as_posix(),
        "bytes": len(payload),
        "sha256": _sha256(payload),
    }


def _verify_completed(root: Path, pass_dir: Path, receipts: list[str]) -> tuple[list[str], list[str]]:
    completed: list[str] = []
    issues: list[str] = []
    for receipt in receipts:
        journal = _pass_journal(pass_dir, receipt)
        if journal is None:
            continue
        for artifact in journal.get("artifacts") or []:
            path = _resolve(root, str(artifact.get("path", "")))
            if not path.is_file():
                issues.append(f"RAW_MISSING:{receipt}:{path}")
                continue
            payload = path.read_bytes()
            if len(payload) != int(artifact.get("bytes", -1)) or _sha256(payload) != artifact.get("sha256"):
                issues.append(f"RAW_HASH_MISMATCH:{receipt}:{path}")
        completed.append(receipt)
    return completed, issues


def _failure_code(status: int, payload: bytes) -> str:
    if status in {429, 502, 503, 504}:
        return "NA_CONNECTION_LIMIT"
    if status != 200:
        return f"HTTP_{status}"
    if not payload:
        return "EMPTY_RESPONSE"
    return ""


def _collect_one(
    *,
    root: Path,
    receipt: str,
    contract: Mapping[str, Any],
    paths: Mapping[str, Path],
    fetcher: Fetcher,
    sleep_fn: Callable[[float], None],
    timeout: float,
    retries: int,
    backoff: float,
    delay: float,
    request_budget: int,
    clock: Callable[[], datetime],
) -> tuple[dict[str, Any], int]:
    route = contract["official_route"]
    user_agent = "RootA-C8-DART-viewer-shadow/1.0"
    landing_url = str(route["landing_endpoint"])
    viewer_url = str(route["viewer_endpoint"])
    requests_used = 0

    def fetch(endpoint: str, params: dict[str, str], headers: Mapping[str, str]):
        nonlocal requests_used
        if requests_used >= request_budget:
            raise ShadowCollectionError("NETWORK_REQUEST_BUDGET_EXHAUSTED")
        status, payload, response_headers, attempts = _fetch_with_retry(
            endpoint=endpoint,
            params=params,
            headers=headers,
            timeout=timeout,
            retries=retries,
            backoff=backoff,
            fetcher=fetcher,
            sleep_fn=sleep_fn,
        )
        requests_used += attempts
        return status, payload, response_headers

    landing_status, landing, landing_headers = fetch(
        landing_url,
        {str(route["landing_query_key"]): receipt},
        {"User-Agent": user_agent, "Accept": "text/html,application/xhtml+xml"},
    )
    code = _failure_code(landing_status, landing)
    if code:
        raise ShadowCollectionError(code, f"landing receipt={receipt}")
    title = _clean_title(_decode_html(landing))
    if not title:
        raise ShadowCollectionError("LANDING_TITLE_MISSING", receipt)
    landing_hash = _sha256(landing)
    landing_path = paths["raw"] / receipt / f"landing_{landing_hash[:16]}.html"
    _write_immutable(landing_path, landing)
    artifacts = [_artifact_record(root, landing_path, landing, "LANDING")]
    landing_rows = [
        {
            "kind": "PRIMARY_LANDING",
            "request": {"rcpNo": receipt},
            "title": title,
            "http_status": landing_status,
            "server_date_header": str(landing_headers.get("Date", "")),
            **artifacts[0],
        }
    ]
    refs_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    for ref in parse_viewer_references(landing):
        refs_by_key[_reference_key(ref)] = {**ref, "source_landing": "PRIMARY"}
    related_links = parse_related_receipt_links(landing)
    attachment_refs = parse_attachment_references(landing, receipt)

    primary_dcms = {ref["dcmNo"] for ref in refs_by_key.values() if ref["rcpNo"] == receipt}
    attachment_refs = [ref for ref in attachment_refs if ref["dcmNo"] not in primary_dcms]
    for attachment in attachment_refs:
        if delay > 0:
            sleep_fn(delay)
        attachment_status, attachment_payload, attachment_headers = fetch(
            landing_url,
            {"rcpNo": attachment["rcpNo"], "dcmNo": attachment["dcmNo"]},
            {
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml",
                "Referer": f"{landing_url}?rcpNo={receipt}",
            },
        )
        code = _failure_code(attachment_status, attachment_payload)
        if code:
            raise ShadowCollectionError(
                code, f"attachment landing receipt={receipt}, dcmNo={attachment['dcmNo']}"
            )
        attachment_title = _clean_title(_decode_html(attachment_payload))
        if not attachment_title:
            raise ShadowCollectionError(
                "ATTACHMENT_LANDING_TITLE_MISSING", f"{receipt}:{attachment['dcmNo']}"
            )
        attachment_hash = _sha256(attachment_payload)
        attachment_path = (
            paths["raw"]
            / receipt
            / f"attachment_landing_{attachment['dcmNo']}_{attachment_hash[:16]}.html"
        )
        _write_immutable(attachment_path, attachment_payload)
        artifact = _artifact_record(root, attachment_path, attachment_payload, "ATTACHMENT_LANDING")
        artifacts.append(artifact)
        landing_rows.append(
            {
                "kind": "ATTACHMENT_LANDING",
                "request": attachment,
                "title": attachment_title,
                "http_status": attachment_status,
                "server_date_header": str(attachment_headers.get("Date", "")),
                **artifact,
            }
        )
        attachment_document_refs = parse_viewer_references(attachment_payload)
        if not any(
            ref["rcpNo"] == receipt and ref["dcmNo"] == attachment["dcmNo"]
            for ref in attachment_document_refs
        ):
            raise ShadowCollectionError(
                "ATTACHMENT_DOCUMENT_REFERENCES_MISSING", f"{receipt}:{attachment['dcmNo']}"
            )
        for ref in attachment_document_refs:
            refs_by_key[_reference_key(ref)] = {
                **ref,
                "source_landing": f"ATTACHMENT:{attachment['dcmNo']}",
            }

    refs = [refs_by_key[key] for key in sorted(refs_by_key)]
    if not refs:
        raise ShadowCollectionError("VIEWER_DOCUMENT_REFERENCES_MISSING", receipt)
    if not any(ref["rcpNo"] == receipt for ref in refs):
        raise ShadowCollectionError("EXACT_RECEIPT_REFERENCE_MISSING", receipt)
    document_rows: list[dict[str, Any]] = []
    for index, ref in enumerate(refs, start=1):
        if delay > 0:
            sleep_fn(delay)
        status, payload, response_headers = fetch(
            viewer_url,
            {key: ref[key] for key in ("rcpNo", "dcmNo", "eleId", "offset", "length", "dtd")},
            {
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml",
                "Referer": f"{landing_url}?rcpNo={receipt}",
            },
        )
        code = _failure_code(status, payload)
        if code:
            raise ShadowCollectionError(code, f"viewer receipt={receipt}, index={index}")
        digest = _sha256(payload)
        raw_path = (
            paths["raw"]
            / receipt
            / f"document_{index:03d}_{ref['rcpNo']}_{ref['dcmNo']}_{ref['eleId']}_{digest[:16]}.html"
        )
        _write_immutable(raw_path, payload)
        artifact = _artifact_record(root, raw_path, payload, "VIEWER_DOCUMENT")
        artifacts.append(artifact)
        document_rows.append(
            {
                "index": index,
                "request": ref,
                "http_status": status,
                "server_date_header": str(response_headers.get("Date", "")),
                **artifact,
            }
        )

    captured_at = clock().astimezone(timezone.utc).isoformat()
    journal = {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_DART_VIEWER_EXACT_RECEIPT_SHADOW",
        "status": "SHADOW_PASS",
        "promotion_allowed": False,
        "receipt_no": receipt,
        "captured_at": captured_at,
        "landing": {
            "title": title,
            "http_status": landing_status,
            "server_date_header": str(landing_headers.get("Date", "")),
            **artifacts[0],
        },
        "landing_count": len(landing_rows),
        "landings": landing_rows,
        "attachment_landing_count": len(attachment_refs),
        "related_receipt_links": related_links,
        "lineage_status": "EXTRACTED_CANDIDATES_NOT_POLICY_ACCEPTED",
        "document_count": len(document_rows),
        "documents": document_rows,
        "artifacts": artifacts,
        "network_requests": requests_used,
    }
    _write_immutable(paths["pass"] / f"{receipt}.json", _json_bytes(journal))
    return journal, requests_used


def _summary(
    *,
    root: Path,
    contract: Mapping[str, Any],
    receipts: list[str],
    paths: Mapping[str, Path],
    generated_at: str,
    mode: str,
    network_requests: int,
    run_receipts: list[str],
    stop_reason: str,
) -> dict[str, Any]:
    completed, integrity_issues = _verify_completed(root, paths["pass"], receipts)
    completed_set = set(completed)
    pending = [receipt for receipt in receipts if receipt not in completed_set]
    return {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_DART_VIEWER_EXACT_RECEIPT_SHADOW_STATUS",
        "generated_at": generated_at,
        "mode": mode,
        "status": "SHADOW_COMPLETE" if not pending and not integrity_issues else "SHADOW_INCOMPLETE",
        "promotion_allowed": False,
        "target_receipt_count": len(receipts),
        "completed_receipt_count": len(completed),
        "pending_receipt_count": len(pending),
        "completed_receipts": completed,
        "pending_receipts": pending,
        "integrity_pass": not integrity_issues,
        "integrity_issues": integrity_issues,
        "network_requests": network_requests,
        "run_receipts": run_receipts,
        "stop_reason": stop_reason,
        "capture_status_modified": False,
        "downstream_opened": False,
        "contract_version": contract.get("contract_version"),
    }


def run_collection(
    *,
    root: Path = ROOT,
    contract_path: Path | None = None,
    apply: bool = False,
    confirm_shadow_only: bool = False,
    max_receipts: int | None = None,
    max_network_requests: int | None = None,
    timeout: float | None = None,
    delay: float | None = None,
    retries: int | None = None,
    fetcher: Fetcher = _default_fetcher,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, Any]:
    root = root.resolve()
    contract_path = contract_path or (root / CONTRACT_RELATIVE)
    contract, _capture_status, receipts = _load_context(root, contract_path)
    paths = _output_paths(root, contract)
    execution = contract["execution"]
    max_receipts = int(
        execution["default_max_receipts"] if max_receipts is None else max_receipts
    )
    max_network_requests = int(
        execution["default_max_network_requests"]
        if max_network_requests is None
        else max_network_requests
    )
    timeout = float(execution["default_timeout_seconds"] if timeout is None else timeout)
    delay = float(execution["default_delay_seconds"] if delay is None else delay)
    retries = int(execution["default_retries"] if retries is None else retries)
    backoff = float(execution["retry_backoff_seconds"])
    if min(max_receipts, max_network_requests) <= 0 or timeout <= 0 or delay < 0 or retries < 0:
        raise ShadowCollectionError("EXECUTION_ARGUMENT_INVALID")
    if apply and not confirm_shadow_only:
        raise ShadowCollectionError("SHADOW_CONFIRMATION_REQUIRED")

    completed, integrity_issues = _verify_completed(root, paths["pass"], receipts)
    if integrity_issues:
        raise ShadowCollectionError("EXISTING_SHADOW_INTEGRITY_FAILED", ";".join(integrity_issues))
    pending = [receipt for receipt in receipts if receipt not in set(completed)]
    selected = pending[:max_receipts]
    generated_at = clock().astimezone(timezone.utc).isoformat()
    if not apply:
        return _summary(
            root=root,
            contract=contract,
            receipts=receipts,
            paths=paths,
            generated_at=generated_at,
            mode="DRY_RUN",
            network_requests=0,
            run_receipts=selected,
            stop_reason="DRY_RUN_NO_NETWORK_NO_WRITE",
        )

    total_requests = 0
    run_receipts: list[str] = []
    stop_reason = "MAX_RECEIPTS_REACHED" if selected else "NO_PENDING_RECEIPTS"
    for receipt in selected:
        remaining_budget = max_network_requests - total_requests
        if remaining_budget <= 0:
            stop_reason = "NETWORK_REQUEST_BUDGET_EXHAUSTED"
            break
        try:
            _journal, used = _collect_one(
                root=root,
                receipt=receipt,
                contract=contract,
                paths=paths,
                fetcher=fetcher,
                sleep_fn=sleep_fn,
                timeout=timeout,
                retries=retries,
                backoff=backoff,
                delay=delay,
                request_budget=remaining_budget,
                clock=clock,
            )
            total_requests += used
            run_receipts.append(receipt)
        except ShadowCollectionError as exc:
            stop_reason = exc.code
            failure = {
                "strategy_id": STRATEGY_ID,
                "scope": "M1_C8_DART_VIEWER_SHADOW_FAILURE",
                "status": "SHADOW_FAIL",
                "promotion_allowed": False,
                "receipt_no": receipt,
                "failed_at": clock().astimezone(timezone.utc).isoformat(),
                "reason_code": exc.code,
                "detail": exc.detail,
            }
            stamp = clock().strftime("%Y%m%dT%H%M%S%fZ")
            _write_immutable(paths["fail"] / receipt / f"{stamp}.json", _json_bytes(failure))
            break

    report = _summary(
        root=root,
        contract=contract,
        receipts=receipts,
        paths=paths,
        generated_at=clock().astimezone(timezone.utc).isoformat(),
        mode="APPLY_SHADOW_ONLY",
        network_requests=total_requests,
        run_receipts=run_receipts,
        stop_reason=stop_reason,
    )
    _atomic_write(paths["checkpoint"], _json_bytes(report))
    _atomic_write(paths["report"], _json_bytes(report))
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect exact DART viewer receipts in shadow mode")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--contract", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-shadow-only", action="store_true")
    parser.add_argument("--max-receipts", type=int)
    parser.add_argument("--max-network-requests", type=int)
    parser.add_argument("--timeout", type=float)
    parser.add_argument("--delay", type=float)
    parser.add_argument("--retries", type=int)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        result = run_collection(
            root=args.root,
            contract_path=args.contract,
            apply=args.apply,
            confirm_shadow_only=args.confirm_shadow_only,
            max_receipts=args.max_receipts,
            max_network_requests=args.max_network_requests,
            timeout=args.timeout,
            delay=args.delay,
            retries=args.retries,
        )
    except (ShadowCollectionError, OSError, ValueError, json.JSONDecodeError) as exc:
        code = exc.code if isinstance(exc, ShadowCollectionError) else type(exc).__name__
        print(json.dumps({"status": "FAIL", "reason_code": code, "detail": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
