from __future__ import annotations

from tools.migrate_kospi_mcap_quarterly_c8_merger_only_filter import (
    MIGRATION_ID,
    build_migrated_manifest,
)


def _attempt(series: str, receipts: list[str]):
    return {
        "status": "PASS",
        "dart_status": "000",
        "corp_code": "00000001",
        "code": "000001",
        "series": series,
        "page_no": 1,
        "payload_sha256": "a" * 64,
        "row_count": len(receipts),
        "total_page": 1,
        "accepted_receipts": receipts,
        "captured_at": "2026-09-15T17:00:00+09:00",
        "error_code": "",
    }


def _journal(series: str, receipts: list[str]):
    attempt = _attempt(series, receipts)
    return {
        "request": {
            "kind": "LIST_PAGE",
            "code": attempt["code"],
            "corp_code": attempt["corp_code"],
            "series": series,
            "page_no": 1,
        },
        "dart_status": "000",
        "payload_sha256": attempt["payload_sha256"],
        "row_count": len(receipts),
        "total_page": 1,
        "accepted_receipts": receipts,
        "captured_at": attempt["captured_at"],
    }


def _document(receipt: str, status: str):
    return {
        "attempts": [
            {
                "status": status,
                "receipt_no": receipt,
                "payload_sha256": "b" * 64,
                "package_member_count": 1 if status == "PASS" else 0,
                "captured_at": "2026-09-15T18:00:00+09:00",
                "error_code": "" if status == "PASS" else "DART_DOCUMENT_STATUS_014",
            }
        ]
    }


def test_migration_retains_only_documents_in_rebuilt_accepted_set():
    decision = "20260101000001"
    merger_completion = "20260202000002"
    unrelated_pass = "20260303000003"
    unrelated_fail = "20260404000004"
    manifest = {
        "checkpoint": {
            "list_pages": {
                "000001:MERGER_DECISION_HISTORY:1": {
                    "attempts": [_attempt("MERGER_DECISION_HISTORY", [decision])]
                },
                "000001:MERGER_COMPLETION_HISTORY:1": {
                    "attempts": [
                        _attempt(
                            "MERGER_COMPLETION_HISTORY",
                            [merger_completion, unrelated_pass, unrelated_fail],
                        )
                    ]
                },
            },
            "documents": {
                decision: _document(decision, "PASS"),
                merger_completion: _document(merger_completion, "PASS"),
                unrelated_pass: _document(unrelated_pass, "PASS"),
                unrelated_fail: _document(unrelated_fail, "FAIL"),
            },
        }
    }
    rebuilt = {
        "000001:MERGER_DECISION_HISTORY:1": _journal(
            "MERGER_DECISION_HISTORY", [decision]
        ),
        "000001:MERGER_COMPLETION_HISTORY:1": _journal(
            "MERGER_COMPLETION_HISTORY", [merger_completion]
        ),
    }
    migrated, summary = build_migrated_manifest(
        manifest,
        rebuilt,
        filter_sha256="c" * 64,
        generated_at="2026-09-15T21:00:00+09:00",
    )
    assert set(migrated["checkpoint"]["documents"]) == {
        decision,
        merger_completion,
    }
    assert summary["old_accepted_receipt_count"] == 4
    assert summary["new_accepted_receipt_count"] == 2
    assert summary["excluded_receipt_count"] == 2
    assert summary["retained_document_pass_count"] == 2
    assert summary["excluded_document_pass_count"] == 1
    assert summary["excluded_document_fail_count"] == 1
    assert migrated["migration_history"][-1]["migration_id"] == MIGRATION_ID
    assert migrated["accepted_report_filter_sha256"] == "c" * 64
    assert migrated["orders_generated"] is False
