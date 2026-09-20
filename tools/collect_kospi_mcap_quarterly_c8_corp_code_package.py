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
    load_corporate_action_adapter_contract,
    parse_corp_code_package,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_source_builder import (
    _atomic_write_bytes,
    _json_bytes,
    _write_immutable,
)


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
MANIFEST_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/data/acquisition/checkpoints/"
    "c8_corporate_action_capture_manifest_latest.json"
)
RAW_DIR_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/data/inbox/raw"
)
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")
REPORT_LATEST_RELATIVE = REPORT_DIR_RELATIVE / "c8_corp_code_package_capture_latest.json"
DEFAULT_KEY_FILE = Path("_cache/dart_api_key.txt")
CODE_PATTERN = re.compile(r"^[0-9A-Z]{6}$")
Fetcher = Callable[[str, dict[str, str], float], tuple[int, bytes, Mapping[str, str]]]


def _load_manifest(path: Path) -> tuple[dict[str, Any], list[str]]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    reasons: list[str] = []
    if manifest.get("strategy_id") != STRATEGY_ID:
        reasons.append("CORP_CODE_CAPTURE_MANIFEST_STRATEGY_MISMATCH")
    if manifest.get("scope") != "M1_C8_CORPORATE_ACTION_FULL_CAPTURE_CHECKPOINT":
        reasons.append("CORP_CODE_CAPTURE_MANIFEST_SCOPE_MISMATCH")
    universe = manifest.get("universe") or {}
    codes = universe.get("codes") or []
    if (
        not isinstance(codes, list)
        or not codes
        or any(not isinstance(code, str) or not CODE_PATTERN.fullmatch(code) for code in codes)
        or len(codes) != len(set(codes))
        or universe.get("code_count") != len(codes)
    ):
        reasons.append("CORP_CODE_CAPTURE_MANIFEST_UNIVERSE_INVALID")
    request_plan = manifest.get("request_plan") or {}
    if request_plan.get("corp_code_package_requests") != 1:
        reasons.append("CORP_CODE_CAPTURE_REQUEST_PLAN_INVALID")
    attempts = (
        manifest.get("checkpoint", {}).get("corp_code_package", {}).get("attempts") or []
    )
    if attempts:
        reasons.append("CORP_CODE_CAPTURE_ALREADY_ATTEMPTED")
    for field in (
        "full_capture_complete",
        "acquisition_evidence_published",
        "adapter_components_generated",
        "canonical_generated",
        "m2_allowed",
        "candidate_selection_calculated",
        "target_portfolio_calculated",
        "orders_generated",
        "operational_change",
    ):
        if manifest.get(field) is not False:
            reasons.append("CORP_CODE_CAPTURE_DOWNSTREAM_STATE_NOT_CLOSED")
            break
    return manifest, sorted(set(reasons))


def _read_key(path: Path) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8").replace("\ufeff", "").strip()


def _prior_capture_reason(root: Path, selection_as_of: object) -> str:
    latest = root / REPORT_LATEST_RELATIVE
    if not latest.is_file():
        return ""
    try:
        report = json.loads(latest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "CORP_CODE_CAPTURE_PRIOR_REPORT_INVALID"
    if (
        report.get("scope") == "M1_C8_CORP_CODE_PACKAGE_ONE_CALL_CAPTURE"
        and int(report.get("request_count") or 0) >= 1
        and report.get("selection_as_of") in {None, "", selection_as_of}
    ):
        return "CORP_CODE_CAPTURE_ALREADY_EXECUTED_FOR_SELECTION"
    return ""


def _default_fetcher(
    endpoint: str, params: dict[str, str], timeout: float
) -> tuple[int, bytes, Mapping[str, str]]:
    response = requests.get(
        endpoint,
        params=params,
        headers={"User-Agent": "RootA-C8-corp-code-capture/1.0"},
        timeout=timeout,
        allow_redirects=False,
    )
    return int(response.status_code), bytes(response.content), dict(response.headers)


def _suffix(payload: bytes) -> str:
    if payload.startswith(b"PK"):
        return ".zip"
    if payload.lstrip().startswith(b"<"):
        return ".xml"
    return ".bin"


def _write_report(root: Path, report: dict[str, Any]) -> list[str]:
    stamp = "".join(char for char in report["captured_at"] if char.isdigit())[:14]
    versioned = root / REPORT_DIR_RELATIVE / f"c8_corp_code_package_capture_{stamp}.json"
    latest = root / REPORT_LATEST_RELATIVE
    content = _json_bytes(report)
    _write_immutable(root, versioned, content)
    _atomic_write_bytes(root, latest, content)
    return [str(versioned), str(latest)]


def run_capture(
    *,
    repo_root: Path,
    manifest_path: Path,
    api_key_file: Path,
    apply: bool,
    confirmed: bool,
    timeout: float,
    fetcher: Fetcher | None = None,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    root = repo_root.resolve()
    manifest_file = manifest_path if manifest_path.is_absolute() else root / manifest_path
    key_file = api_key_file if api_key_file.is_absolute() else root / api_key_file
    manifest, reasons = _load_manifest(manifest_file)
    prior_reason = _prior_capture_reason(root, manifest.get("selection_as_of"))
    if prior_reason:
        reasons.append(prior_reason)
    key = _read_key(key_file)
    if not key:
        reasons.append("DART_API_KEY_MISSING")
    if timeout <= 0:
        reasons.append("CORP_CODE_CAPTURE_TIMEOUT_INVALID")
    if apply and not confirmed:
        reasons.append("CORP_CODE_CAPTURE_CONFIRMATION_MISSING")
    reasons = sorted(set(reasons))
    if reasons:
        return {
            "status": "BLOCKED",
            "verdict": "C8_CORP_CODE_PACKAGE_CAPTURE_BLOCKED",
            "reason_codes": reasons,
            "request_count": 0,
            "network_execution_authorized": False,
            "manifest_updated": False,
            "outputs": [],
        }
    if not apply:
        return {
            "status": "READY",
            "verdict": "C8_CORP_CODE_PACKAGE_ONE_CALL_READY",
            "reason_codes": [],
            "manifest_code_count": manifest["universe"]["code_count"],
            "request_count": 0,
            "network_execution_authorized": False,
            "manifest_updated": False,
            "outputs": [],
        }

    clock = now or (lambda: datetime.now().astimezone())
    request_started_at = clock().isoformat(timespec="microseconds")
    adapter_contract = load_corporate_action_adapter_contract(root)
    endpoint = adapter_contract["endpoints"]["corp_code"]
    try:
        status, payload, headers = (fetcher or _default_fetcher)(
            endpoint, {"crtfc_key": key}, timeout
        )
        fetch_error = ""
    except Exception as exc:
        status, payload, headers = 0, b"", {}
        fetch_error = type(exc).__name__
    response_received_at = clock().isoformat(timespec="microseconds")
    captured = clock()
    captured_at = captured.isoformat(timespec="microseconds")
    stamp = captured.strftime("%Y%m%dT%H%M%S%z")
    outputs: list[str] = []
    raw_relative = RAW_DIR_RELATIVE / f"opendart_corp_code_{stamp}{_suffix(payload)}"
    if payload:
        raw_path = _write_immutable(root, root / raw_relative, payload)
        outputs.append(str(raw_path))

    parsed = parse_corp_code_package(payload) if status == 200 and payload else {
        "status": "FAIL",
        "reason_codes": ["DART_CORP_CODE_PACKAGE_INVALID"],
        "mapping": {},
        "mapping_count": 0,
        "payload_sha256": None,
    }
    codes = list(manifest["universe"]["codes"])
    mapping = dict(parsed.get("mapping") or {})
    matched = [code for code in codes if code in mapping]
    missing = [code for code in codes if code not in mapping]
    alpha_codes = [code for code in codes if re.search(r"[A-Z]", code)]
    alpha_mapping = {code: mapping.get(code) for code in alpha_codes}
    report_reasons = list(parsed.get("reason_codes") or [])
    if fetch_error:
        report_reasons.append("DART_CORP_CODE_REQUEST_FAILED")
    elif status != 200:
        report_reasons.append(f"DART_CORP_CODE_HTTP_{status}")
    if parsed.get("status") == "PASS" and missing:
        report_reasons.append("CORP_CODE_EXACT_UNIVERSE_COVERAGE_INCOMPLETE")
    report_reasons = sorted(set(report_reasons))
    if fetch_error or status != 200 or parsed.get("status") != "PASS":
        verdict = "FAIL_C8_CORP_CODE_PACKAGE_CAPTURE"
        report_status = "FAIL"
    elif missing:
        verdict = "C8_CORP_CODE_PACKAGE_CAPTURED_MAPPING_GAP"
        report_status = "BLOCKED"
    else:
        verdict = "C8_CORP_CODE_PACKAGE_CAPTURED_COVERAGE_PASS"
        report_status = "PASS"
    report = {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_CORP_CODE_PACKAGE_ONE_CALL_CAPTURE",
        "selection_as_of": manifest.get("selection_as_of"),
        "manifest_sha256": hashlib.sha256(manifest_file.read_bytes()).hexdigest(),
        "manifest_code_set_sha256": manifest.get("universe", {}).get("code_set_sha256"),
        "status": report_status,
        "verdict": verdict,
        "reason_codes": report_reasons,
        "request_started_at": request_started_at,
        "response_received_at": response_received_at,
        "captured_at": captured_at,
        "endpoint": endpoint,
        "http_status": status,
        "server_date_header": str(headers.get("Date") or headers.get("date") or ""),
        "fetch_error_type": fetch_error,
        "request_count": 1,
        "redirects_allowed": False,
        "retries_allowed": False,
        "raw_path": raw_relative.as_posix() if payload else None,
        "raw_size_bytes": len(payload),
        "raw_sha256": parsed.get("payload_sha256"),
        "package_parse_status": parsed.get("status"),
        "package_mapping_count": int(parsed.get("mapping_count") or 0),
        "manifest_code_count": len(codes),
        "matched_code_count": len(matched),
        "missing_code_count": len(missing),
        "missing_codes": missing,
        "alphanumeric_codes": alpha_codes,
        "alphanumeric_code_mapping": alpha_mapping,
        "network_execution_scope": "OPENDART_CORP_CODE_PACKAGE_ONLY",
        "network_execution_authorized": True,
        "manifest_updated": False,
        "full_capture_complete": False,
        "acquisition_evidence_published": False,
        "canonical_generated": False,
        "m2_allowed": False,
        "candidate_selection_calculated": False,
        "target_portfolio_calculated": False,
        "orders_generated": False,
        "operational_change": False,
    }
    outputs.extend(_write_report(root, report))
    report["outputs"] = outputs
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture exactly one OpenDART corpCode package without downstream calls."
    )
    parser.add_argument("--manifest", type=Path, default=MANIFEST_RELATIVE)
    parser.add_argument("--api-key-file", type=Path, default=DEFAULT_KEY_FILE)
    parser.add_argument("--timeout", type=float, default=40.0)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-corp-code-only", action="store_true")
    parser.add_argument("--expect-verdict", default="")
    args = parser.parse_args()
    report = run_capture(
        repo_root=ROOT,
        manifest_path=args.manifest,
        api_key_file=args.api_key_file,
        apply=args.apply,
        confirmed=args.confirm_corp_code_only,
        timeout=args.timeout,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.expect_verdict:
        return 0 if report["verdict"] == args.expect_verdict else 3
    return 0 if report["status"] in {"READY", "PASS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
