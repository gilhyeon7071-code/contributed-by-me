from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    parse_corp_code_package,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_capture import (
    evaluate_capture_manifest,
    record_corp_code_package,
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
CAPTURE_REPORT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/reports/"
    "c8_corp_code_package_capture_latest.json"
)
RAW_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/data/inbox/raw")
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")
REPORT_LATEST_RELATIVE = REPORT_DIR_RELATIVE / "c8_corp_code_checkpoint_apply_latest.json"
Evaluator = Callable[..., dict[str, Any]]


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object required: {path}")
    return payload


def _resolve_input(root: Path, path: Path) -> Path:
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _resolve_raw(root: Path, raw_value: object) -> tuple[Path | None, str]:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None, "CORP_CODE_CHECKPOINT_RAW_PATH_MISSING"
    candidate = _resolve_input(root, Path(raw_value.strip()))
    allowed = (root / RAW_DIR_RELATIVE).resolve()
    try:
        candidate.relative_to(allowed)
    except ValueError:
        return None, "CORP_CODE_CHECKPOINT_RAW_PATH_OUTSIDE_STRATEGY"
    if not candidate.is_file():
        return None, "CORP_CODE_CHECKPOINT_RAW_MISSING"
    return candidate, ""


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write_audit_report(root: Path, report: Mapping[str, Any]) -> list[str]:
    stamp = "".join(char for char in str(report["generated_at"]) if char.isdigit())[:14]
    versioned = root / REPORT_DIR_RELATIVE / f"c8_corp_code_checkpoint_apply_{stamp}.json"
    latest = root / REPORT_LATEST_RELATIVE
    content = _json_bytes(dict(report))
    _write_immutable(root, versioned, content)
    _atomic_write_bytes(root, latest, content)
    return [str(versioned), str(latest)]


def run_checkpoint(
    *,
    repo_root: Path,
    manifest_path: Path,
    capture_report_path: Path,
    apply: bool,
    confirmed: bool,
    now: Callable[[], datetime] | None = None,
    evaluator: Evaluator | None = None,
) -> dict[str, Any]:
    root = repo_root.resolve()
    manifest_file = _resolve_input(root, manifest_path)
    capture_report_file = _resolve_input(root, capture_report_path)
    generated_at = (now or (lambda: datetime.now().astimezone()))().isoformat(
        timespec="microseconds"
    )
    reasons: list[str] = []
    try:
        manifest = _load_json_object(manifest_file)
    except (OSError, ValueError, json.JSONDecodeError):
        manifest = {}
        reasons.append("CORP_CODE_CHECKPOINT_MANIFEST_INVALID")
    try:
        capture_report = _load_json_object(capture_report_file)
    except (OSError, ValueError, json.JSONDecodeError):
        capture_report = {}
        reasons.append("CORP_CODE_CHECKPOINT_CAPTURE_REPORT_INVALID")

    if manifest.get("strategy_id") != STRATEGY_ID:
        reasons.append("CORP_CODE_CHECKPOINT_MANIFEST_STRATEGY_MISMATCH")
    if manifest.get("scope") != "M1_C8_CORPORATE_ACTION_FULL_CAPTURE_CHECKPOINT":
        reasons.append("CORP_CODE_CHECKPOINT_MANIFEST_SCOPE_MISMATCH")
    codes = list((manifest.get("universe") or {}).get("codes") or [])
    if not codes or len(codes) != len(set(codes)) or (manifest.get("universe") or {}).get(
        "code_count"
    ) != len(codes):
        reasons.append("CORP_CODE_CHECKPOINT_MANIFEST_UNIVERSE_INVALID")

    required_report = {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_CORP_CODE_PACKAGE_ONE_CALL_CAPTURE",
        "status": "PASS",
        "verdict": "C8_CORP_CODE_PACKAGE_CAPTURED_COVERAGE_PASS",
        "http_status": 200,
        "request_count": 1,
        "package_parse_status": "PASS",
        "missing_code_count": 0,
        "manifest_updated": False,
    }
    if any(capture_report.get(key) != value for key, value in required_report.items()):
        reasons.append("CORP_CODE_CHECKPOINT_CAPTURE_REPORT_NOT_APPROVED_PASS")
    if capture_report.get("manifest_code_count") != len(codes):
        reasons.append("CORP_CODE_CHECKPOINT_CAPTURE_REPORT_CODE_COUNT_MISMATCH")
    if capture_report.get("matched_code_count") != len(codes):
        reasons.append("CORP_CODE_CHECKPOINT_CAPTURE_REPORT_COVERAGE_MISMATCH")
    captured_at = str(capture_report.get("captured_at") or "")
    try:
        captured_datetime = datetime.fromisoformat(captured_at)
    except ValueError:
        captured_datetime = None
    if captured_datetime is None or captured_datetime.tzinfo is None:
        reasons.append("CORP_CODE_CHECKPOINT_CAPTURE_TIMESTAMP_INVALID")
    for field in (
        "full_capture_complete",
        "acquisition_evidence_published",
        "canonical_generated",
        "m2_allowed",
        "candidate_selection_calculated",
        "target_portfolio_calculated",
        "orders_generated",
        "operational_change",
    ):
        if capture_report.get(field) is not False:
            reasons.append("CORP_CODE_CHECKPOINT_CAPTURE_REPORT_DOWNSTREAM_OPEN")
            break
    if apply and not confirmed:
        reasons.append("CORP_CODE_CHECKPOINT_CONFIRMATION_MISSING")

    raw_path, raw_reason = _resolve_raw(root, capture_report.get("raw_path"))
    if raw_reason:
        reasons.append(raw_reason)
    payload = b""
    parsed: dict[str, Any] = {}
    if raw_path is not None:
        payload = raw_path.read_bytes()
        if len(payload) != capture_report.get("raw_size_bytes"):
            reasons.append("CORP_CODE_CHECKPOINT_RAW_SIZE_MISMATCH")
        if _sha256(payload) != capture_report.get("raw_sha256"):
            reasons.append("CORP_CODE_CHECKPOINT_RAW_SHA256_MISMATCH")
        parsed = parse_corp_code_package(payload)
        if parsed.get("status") != "PASS":
            reasons.append("CORP_CODE_CHECKPOINT_RAW_PARSE_FAILED")
        if parsed.get("payload_sha256") != capture_report.get("raw_sha256"):
            reasons.append("CORP_CODE_CHECKPOINT_PARSED_SHA256_MISMATCH")
        if parsed.get("mapping_count") != capture_report.get("package_mapping_count"):
            reasons.append("CORP_CODE_CHECKPOINT_PACKAGE_ROW_COUNT_MISMATCH")

    package_mapping = dict(parsed.get("mapping") or {})
    missing_codes = [code for code in codes if code not in package_mapping]
    exact_mapping = {code: package_mapping[code] for code in codes if code in package_mapping}
    if missing_codes:
        reasons.append("CORP_CODE_CHECKPOINT_EXACT_COVERAGE_INCOMPLETE")
    if len(set(exact_mapping.values())) != len(exact_mapping):
        reasons.append("CORP_CODE_CHECKPOINT_MAPPING_NOT_ONE_TO_ONE")

    current_assessment: dict[str, Any] = {}
    prospective_assessment: dict[str, Any] = {}
    updated_manifest = manifest
    evaluate = evaluator or evaluate_capture_manifest
    if not reasons:
        try:
            current_assessment = evaluate(
                manifest, repo_root=root, generated_at=generated_at
            )
        except (ContractError, OSError, ValueError, KeyError, TypeError):
            reasons.append("CORP_CODE_CHECKPOINT_CURRENT_MANIFEST_EVALUATION_FAILED")
    if current_assessment and (
        current_assessment.get("manifest_integrity_pass") is not True
        or current_assessment.get("base_universe_authorized") is not True
    ):
        reasons.append("CORP_CODE_CHECKPOINT_CURRENT_MANIFEST_NOT_AUTHORIZED")
    if not reasons:
        try:
            updated_manifest = record_corp_code_package(
                manifest,
                status="PASS",
                payload_sha256=parsed["payload_sha256"],
                row_count=int(parsed["mapping_count"]),
                captured_at=captured_at,
                mapping=exact_mapping,
            )
            prospective_assessment = evaluate(
                updated_manifest, repo_root=root, generated_at=generated_at
            )
        except (ContractError, OSError, ValueError, KeyError, TypeError):
            reasons.append("CORP_CODE_CHECKPOINT_RECORD_REJECTED")
    if prospective_assessment and (
        prospective_assessment.get("verdict") != "C8_CORPORATE_ACTION_CAPTURE_PENDING"
        or "CORPORATE_ACTION_REQUESTS_PENDING"
        not in prospective_assessment.get("reason_codes", [])
        or prospective_assessment.get("bulk_requests_executed") != 1
        or prospective_assessment.get("full_capture_complete") is not False
        or prospective_assessment.get("m2_allowed") is not False
        or prospective_assessment.get("orders_generated") is not False
    ):
        reasons.append("CORP_CODE_CHECKPOINT_PROSPECTIVE_STATE_INVALID")

    reasons = sorted(set(reasons))
    base = {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_CORP_CODE_PACKAGE_OFFLINE_CHECKPOINT",
        "generated_at": generated_at,
        "status": "BLOCKED" if reasons else ("PASS" if apply else "READY"),
        "verdict": (
            "C8_CORP_CODE_CHECKPOINT_BLOCKED"
            if reasons
            else (
                "C8_CORP_CODE_CHECKPOINT_APPLIED"
                if apply
                else "C8_CORP_CODE_CHECKPOINT_READY"
            )
        ),
        "reason_codes": reasons,
        "network_request_count": 0,
        "network_execution_authorized": False,
        "selection_as_of": manifest.get("selection_as_of"),
        "manifest_code_count": len(codes),
        "package_mapping_count": int(parsed.get("mapping_count") or 0),
        "matched_code_count": len(exact_mapping),
        "missing_code_count": len(missing_codes),
        "missing_codes": missing_codes,
        "raw_path": str(raw_path) if raw_path is not None else None,
        "raw_size_bytes": len(payload),
        "raw_sha256": parsed.get("payload_sha256"),
        "checkpoint_attempt_count_before": len(
            (manifest.get("checkpoint", {}).get("corp_code_package", {}).get("attempts") or [])
        ),
        "checkpoint_attempt_count_after": len(
            (
                updated_manifest.get("checkpoint", {})
                .get("corp_code_package", {})
                .get("attempts")
                or []
            )
        ),
        "prospective_capture_verdict": prospective_assessment.get("verdict"),
        "prospective_reason_codes": prospective_assessment.get("reason_codes", []),
        "prospective_bulk_requests_executed": prospective_assessment.get(
            "bulk_requests_executed"
        ),
        "next_pending_requests": prospective_assessment.get("next_pending_requests", []),
        "minimum_request_count": (manifest.get("request_plan") or {}).get(
            "minimum_request_count"
        ),
        "minimum_requests_remaining": max(
            0,
            int((manifest.get("request_plan") or {}).get("minimum_request_count") or 0)
            - int(prospective_assessment.get("bulk_requests_executed") or 0),
        ),
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
    if reasons or not apply:
        return base

    changed = updated_manifest != manifest
    if not changed:
        base["verdict"] = "C8_CORP_CODE_CHECKPOINT_ALREADY_APPLIED"
        return base

    manifest_output = write_capture_manifest(updated_manifest, manifest_file, repo_root=root)
    status_outputs = write_capture_report(prospective_assessment, repo_root=root)
    base["manifest_updated"] = True
    base["capture_status_updated"] = True
    base["outputs"] = [str(manifest_output), *(str(path) for path in status_outputs)]
    audit_outputs = _write_audit_report(root, base)
    base["outputs"].extend(audit_outputs)
    return base


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply an existing corpCode raw package to the C8 manifest without network access."
    )
    parser.add_argument("--manifest", type=Path, default=MANIFEST_RELATIVE)
    parser.add_argument("--capture-report", type=Path, default=CAPTURE_REPORT_RELATIVE)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-offline-checkpoint", action="store_true")
    parser.add_argument("--expect-verdict", default="")
    args = parser.parse_args()
    report = run_checkpoint(
        repo_root=ROOT,
        manifest_path=args.manifest,
        capture_report_path=args.capture_report,
        apply=args.apply,
        confirmed=args.confirm_offline_checkpoint,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.expect_verdict:
        return 0 if report["verdict"] == args.expect_verdict else 3
    return 0 if report["status"] in {"READY", "PASS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
