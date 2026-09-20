from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    build_list_request_spec,
    parse_disclosure_list_page,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_capture import (
    accepted_report_filter_sha256,
    evaluate_capture_manifest,
    load_corporate_action_adapter_contract,
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
LEGACY_JOURNAL_DIR_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/data/acquisition/checkpoints/"
    "c8_list_page_response_journal"
)
MIGRATED_JOURNAL_DIR_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/data/acquisition/checkpoints/"
    "c8_list_page_response_journal_merger_only_v1"
)
RAW_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/data/inbox/raw")
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")
REPORT_LATEST_RELATIVE = REPORT_DIR_RELATIVE / "c8_merger_only_filter_migration_latest.json"
MIGRATION_ID = "C8_MERGER_COMPLETION_EXACT_REPORT_NAME_V1"


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ContractError(f"JSON object required: {path}")
    return payload


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_receipts(receipts: set[str]) -> str:
    return _sha256(("\n".join(sorted(receipts)) + "\n").encode("ascii"))


def _request_token(request: Mapping[str, Any]) -> str:
    return "_".join(
        [
            str(request["code"]),
            str(request["series"]),
            f"p{int(request['page_no']):04d}",
        ]
    )


def _request_from_attempt(attempt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "kind": "LIST_PAGE",
        "code": str(attempt["code"]),
        "corp_code": str(attempt["corp_code"]),
        "series": str(attempt["series"]),
        "page_no": int(attempt["page_no"]),
    }


def _attempt_from_journal(journal: Mapping[str, Any]) -> dict[str, Any]:
    request = journal["request"]
    return {
        "status": "PASS",
        "dart_status": str(journal["dart_status"]),
        "corp_code": str(request["corp_code"]),
        "code": str(request["code"]),
        "series": str(request["series"]),
        "page_no": int(request["page_no"]),
        "payload_sha256": str(journal["payload_sha256"]),
        "row_count": int(journal["row_count"]),
        "total_page": int(journal["total_page"]),
        "accepted_receipts": list(journal["accepted_receipts"]),
        "captured_at": str(journal["captured_at"]),
        "error_code": "",
    }


def _new_journal(
    old_journal: Mapping[str, Any], parsed: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_LIST_PAGE_PASS_RESPONSE_JOURNAL",
        "request": copy.deepcopy(old_journal["request"]),
        "status": "PASS",
        "http_status": 200,
        "dart_status": str(parsed["dart_status"]),
        "payload_sha256": str(parsed["payload_sha256"]),
        "row_count": int(parsed["accepted_row_count"]),
        "total_page": int(parsed["total_page"]),
        "accepted_receipts": sorted(
            {str(row["rcept_no"]) for row in parsed["accepted_rows"]}
        ),
        "captured_at": str(old_journal["captured_at"]),
        "server_date_header": str(old_journal.get("server_date_header") or ""),
        "raw_path": str(old_journal["raw_path"]),
    }


def replay_legacy_list_journals(
    root: Path, manifest: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    raw_root = (root / RAW_DIR_RELATIVE).resolve()
    pages = manifest.get("checkpoint", {}).get("list_pages", {})
    rebuilt: dict[str, dict[str, Any]] = {}
    expected_files: set[str] = set()
    for key, record in pages.items():
        attempts = record.get("attempts") or []
        if not attempts or attempts[-1].get("status") != "PASS":
            raise ContractError("legacy list manifest must contain latest PASS attempts only")
        attempt = attempts[-1]
        request = _request_from_attempt(attempt)
        token = _request_token(request)
        expected_files.add(f"{token}.json")
        journal_path = root / LEGACY_JOURNAL_DIR_RELATIVE / f"{token}.json"
        journal = _load_json_object(journal_path)
        if (
            journal.get("strategy_id") != STRATEGY_ID
            or journal.get("scope") != "M1_C8_LIST_PAGE_PASS_RESPONSE_JOURNAL"
            or journal.get("status") != "PASS"
            or journal.get("http_status") != 200
            or journal.get("request") != request
            or journal.get("payload_sha256") != attempt.get("payload_sha256")
        ):
            raise ContractError(f"legacy list journal identity mismatch: {key}")
        raw_path = (root / str(journal.get("raw_path") or "")).resolve()
        try:
            raw_path.relative_to(raw_root)
        except ValueError as exc:
            raise ContractError(f"legacy raw path outside strategy raw: {key}") from exc
        payload = raw_path.read_bytes()
        if _sha256(payload) != journal.get("payload_sha256"):
            raise ContractError(f"legacy list raw hash mismatch: {key}")
        spec = build_list_request_spec(
            corp_code=request["corp_code"],
            query_start=manifest["query_start"],
            query_end=manifest["query_end"],
            series=request["series"],
            page_no=request["page_no"],
            repo_root=root,
        )
        parsed = parse_disclosure_list_page(
            payload,
            request_spec=spec,
            expected_stock_code=request["code"],
            repo_root=root,
        )
        if parsed.get("status") != "PASS":
            raise ContractError(
                f"legacy list raw replay failed: {key}: {parsed.get('reason_codes')}"
            )
        if (
            parsed.get("payload_sha256") != journal.get("payload_sha256")
            or parsed.get("dart_status") != journal.get("dart_status")
            or int(parsed.get("total_page") or 0) != int(journal.get("total_page") or 0)
        ):
            raise ContractError(f"legacy list raw replay metadata mismatch: {key}")
        rebuilt[str(key)] = _new_journal(journal, parsed)
    actual_files = {path.name for path in (root / LEGACY_JOURNAL_DIR_RELATIVE).glob("*.json")}
    if actual_files != expected_files:
        raise ContractError("legacy list journal set does not match manifest list pages")
    return rebuilt


def build_migrated_manifest(
    manifest: Mapping[str, Any],
    rebuilt_journals: Mapping[str, Mapping[str, Any]],
    *,
    filter_sha256: str,
    generated_at: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    updated = copy.deepcopy(dict(manifest))
    pages = updated.get("checkpoint", {}).get("list_pages", {})
    if set(pages) != set(rebuilt_journals):
        raise ContractError("rebuilt journal keys do not match manifest list pages")
    old_receipts: set[str] = set()
    accepted: set[str] = set()
    for key, record in pages.items():
        previous = record.get("attempts") or []
        if previous:
            old_receipts.update(previous[-1].get("accepted_receipts") or [])
        journal = rebuilt_journals[key]
        record["attempts"] = [_attempt_from_journal(journal)]
        accepted.update(journal["accepted_receipts"])
    old_documents = updated.get("checkpoint", {}).get("documents", {})
    retained_documents = {
        receipt: copy.deepcopy(record)
        for receipt, record in old_documents.items()
        if receipt in accepted
    }
    retained_pass = {
        receipt
        for receipt, record in retained_documents.items()
        if record.get("attempts") and record["attempts"][-1].get("status") == "PASS"
    }
    removed_pass = {
        receipt
        for receipt, record in old_documents.items()
        if receipt not in accepted
        and record.get("attempts")
        and record["attempts"][-1].get("status") == "PASS"
    }
    removed_fail = {
        receipt
        for receipt, record in old_documents.items()
        if receipt not in accepted
        and record.get("attempts")
        and record["attempts"][-1].get("status") == "FAIL"
    }
    updated["checkpoint"]["documents"] = retained_documents
    updated["manifest_version"] = "1.0.1"
    updated["accepted_report_filter_sha256"] = filter_sha256
    updated["updated_at"] = generated_at
    updated["full_capture_complete"] = False
    updated["acquisition_evidence_published"] = False
    updated["adapter_components_generated"] = False
    updated["canonical_generated"] = False
    updated["m2_allowed"] = False
    updated["candidate_selection_calculated"] = False
    updated["target_portfolio_calculated"] = False
    updated["orders_generated"] = False
    updated["operational_change"] = False
    migration = {
        "migration_id": MIGRATION_ID,
        "generated_at": generated_at,
        "old_accepted_receipt_count": len(old_receipts),
        "new_accepted_receipt_count": len(accepted),
        "excluded_receipt_count": len(old_receipts - accepted),
        "old_accepted_receipts_sha256": _sha256_receipts(old_receipts),
        "new_accepted_receipts_sha256": _sha256_receipts(accepted),
        "retained_document_pass_count": len(retained_pass),
        "excluded_document_pass_count": len(removed_pass),
        "excluded_document_fail_count": len(removed_fail),
        "legacy_list_journal_dir": LEGACY_JOURNAL_DIR_RELATIVE.as_posix(),
        "migrated_list_journal_dir": MIGRATED_JOURNAL_DIR_RELATIVE.as_posix(),
        "network_request_count": 0,
    }
    history = [
        entry
        for entry in updated.get("migration_history", [])
        if entry.get("migration_id") != MIGRATION_ID
    ]
    updated["migration_history"] = history + [migration]
    return updated, migration


def verify_migrated_journals(
    root: Path, rebuilt_journals: Mapping[str, Mapping[str, Any]]
) -> None:
    journal_dir = root / MIGRATED_JOURNAL_DIR_RELATIVE
    expected_names = {
        f"{_request_token(journal['request'])}.json"
        for journal in rebuilt_journals.values()
    }
    actual_names = {path.name for path in journal_dir.glob("*.json")}
    if actual_names != expected_names:
        raise ContractError("migrated list journal set mismatch")
    for journal in rebuilt_journals.values():
        path = journal_dir / f"{_request_token(journal['request'])}.json"
        if _load_json_object(path) != journal:
            raise ContractError(f"migrated list journal content mismatch: {path.name}")


def verify_manifest_uses_rebuilt_journals(
    manifest: Mapping[str, Any], rebuilt_journals: Mapping[str, Mapping[str, Any]]
) -> None:
    pages = manifest.get("checkpoint", {}).get("list_pages", {})
    if set(pages) != set(rebuilt_journals):
        raise ContractError("current manifest list page set mismatch")
    for key, journal in rebuilt_journals.items():
        attempts = pages[key].get("attempts") or []
        if len(attempts) != 1 or attempts[0] != _attempt_from_journal(journal):
            raise ContractError(f"current manifest does not use rebuilt journal: {key}")


def _write_report(root: Path, report: Mapping[str, Any]) -> tuple[Path, Path]:
    stamp = "".join(char for char in str(report["generated_at"]) if char.isdigit())[:14]
    versioned = root / REPORT_DIR_RELATIVE / f"c8_merger_only_filter_migration_{stamp}.json"
    latest = root / REPORT_LATEST_RELATIVE
    content = _json_bytes(dict(report))
    _write_immutable(root, versioned, content)
    _atomic_write_bytes(root, latest, content)
    return versioned, latest


def run_migration(
    *,
    repo_root: Path,
    manifest_path: Path,
    apply: bool,
    confirmed: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    root = repo_root.resolve()
    manifest_file = manifest_path if manifest_path.is_absolute() else root / manifest_path
    generated_at = (now or datetime.now().astimezone()).isoformat(timespec="microseconds")
    report: dict[str, Any] = {
        "strategy_id": STRATEGY_ID,
        "scope": "M1_C8_MERGER_ONLY_FILTER_MIGRATION",
        "generated_at": generated_at,
        "status": "FAIL",
        "verdict": "FAIL_C8_MERGER_ONLY_FILTER_MIGRATION",
        "reason_codes": [],
        "network_request_count": 0,
        "manifest_updated": False,
        "new_journals_written": 0,
        "migrated_journal_integrity_pass": False,
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
    if apply and not confirmed:
        report["reason_codes"] = ["C8_MERGER_ONLY_FILTER_CONFIRMATION_MISSING"]
        return report
    try:
        manifest = _load_json_object(manifest_file)
        if manifest.get("strategy_id") != STRATEGY_ID:
            raise ContractError("capture manifest strategy mismatch")
        adapter_contract = load_corporate_action_adapter_contract(root)
        filter_hash = accepted_report_filter_sha256(adapter_contract)
        rebuilt = replay_legacy_list_journals(root, manifest)
        existing_migration = next(
            (
                entry
                for entry in manifest.get("migration_history", [])
                if entry.get("migration_id") == MIGRATION_ID
            ),
            None,
        )
        already_applied = bool(
            manifest.get("accepted_report_filter_sha256") == filter_hash
            and existing_migration
        )
        if already_applied:
            verify_manifest_uses_rebuilt_journals(manifest, rebuilt)
            migrated = copy.deepcopy(manifest)
            migration = copy.deepcopy(existing_migration)
        else:
            migrated, migration = build_migrated_manifest(
                manifest,
                rebuilt,
                filter_sha256=filter_hash,
                generated_at=generated_at,
            )
        assessment = evaluate_capture_manifest(
            migrated, repo_root=root, generated_at=generated_at
        )
        if assessment.get("manifest_integrity_pass") is not True:
            raise ContractError(
                "migrated manifest integrity failed: "
                + ",".join(assessment.get("reason_codes") or [])
            )
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError, ContractError) as exc:
        report["reason_codes"] = ["C8_MERGER_ONLY_FILTER_REPLAY_OR_INTEGRITY_FAILED"]
        report["error_type"] = type(exc).__name__
        return report
    report.update({key: value for key, value in migration.items() if key != "generated_at"})
    report["migration_generated_at"] = migration.get("generated_at")
    report["already_applied"] = already_applied
    report["list_journal_count"] = len(rebuilt)
    report["pending_document_count"] = (
        migration["new_accepted_receipt_count"] - migration["retained_document_pass_count"]
    )
    report["next_pending_requests"] = assessment.get("next_pending_requests", [])
    if not apply:
        report["status"] = "READY"
        report["verdict"] = (
            "C8_MERGER_ONLY_FILTER_ALREADY_APPLIED_READY"
            if already_applied
            else "C8_MERGER_ONLY_FILTER_MIGRATION_READY"
        )
        return report
    try:
        for journal in rebuilt.values():
            path = (
                root
                / MIGRATED_JOURNAL_DIR_RELATIVE
                / f"{_request_token(journal['request'])}.json"
            )
            _write_immutable(root, path, _json_bytes(dict(journal)))
        verify_migrated_journals(root, rebuilt)
        report["migrated_journal_integrity_pass"] = True
        status_paths: tuple[Path, Path] = ()
        if not already_applied:
            report["new_journals_written"] = len(rebuilt)
            write_capture_manifest(migrated, manifest_file, repo_root=root)
            report["manifest_updated"] = True
            status_paths = write_capture_report(assessment, repo_root=root)
        report["status"] = "PASS"
        report["verdict"] = (
            "C8_MERGER_ONLY_FILTER_ALREADY_APPLIED_PASS"
            if already_applied
            else "C8_MERGER_ONLY_FILTER_MIGRATION_PASS"
        )
        report["outputs"] = [
            str(manifest_file),
            *(str(path) for path in status_paths),
        ]
        report_paths = _write_report(root, report)
        report["outputs"].extend(str(path) for path in report_paths)
    except (OSError, ValueError, KeyError, TypeError, ContractError) as exc:
        report["status"] = "FAIL"
        report["verdict"] = "FAIL_C8_MERGER_ONLY_FILTER_MIGRATION"
        report["reason_codes"] = ["C8_MERGER_ONLY_FILTER_WRITE_FAILED"]
        report["error_type"] = type(exc).__name__
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay stored list responses into a merger-only checkpoint without network access."
    )
    parser.add_argument("--manifest", type=Path, default=MANIFEST_RELATIVE)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-merger-only-filter", action="store_true")
    parser.add_argument("--expect-verdict", default="")
    args = parser.parse_args()
    report = run_migration(
        repo_root=ROOT,
        manifest_path=args.manifest,
        apply=args.apply,
        confirmed=args.confirm_merger_only_filter,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.expect_verdict:
        return 0 if report["verdict"] == args.expect_verdict else 3
    return 0 if report["status"] in {"READY", "PASS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
