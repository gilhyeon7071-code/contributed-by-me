from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tools import collect_kospi_mcap_quarterly_c8_lineage_landings as collector


RECEIPTS = ["20200101000001", "20200102000002"]


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _fixture(root: Path) -> Path:
    report_path = root / "input.json"
    report = {
        "status": "PASS",
        "verdict": "LINEAGE_AUTHORITY_PARTIAL_C8_REMAINS_BLOCKED",
        "selection_as_of": "20260915",
        "rows": [
            {"receipt_no": receipt, "source": "OPENDART_PASS", "authority_resolved": False}
            for receipt in RECEIPTS
        ],
    }
    raw = (json.dumps(report, ensure_ascii=False) + "\n").encode("utf-8")
    report_path.write_bytes(raw)
    contract_path = root / "contract.json"
    _write_json(
        contract_path,
        {
            "strategy_id": collector.STRATEGY_ID,
            "contract_version": "test",
            "mode": "SHADOW_ONLY",
            "input": {
                "lineage_report_path": "input.json",
                "lineage_report_sha256": hashlib.sha256(raw).hexdigest(),
                "required_report_status": "PASS",
                "required_report_verdict": "LINEAGE_AUTHORITY_PARTIAL_C8_REMAINS_BLOCKED",
                "required_selection_as_of": "20260915",
                "required_source": "OPENDART_PASS",
                "required_unresolved_count": 2,
                "receipt_number_pattern": "^[0-9]{14}$",
            },
            "official_route": {
                "host": "dart.fss.or.kr",
                "landing_endpoint": "https://dart.fss.or.kr/dsaf001/main.do",
                "landing_query_key": "rcpNo",
            },
            "execution": {
                "default_max_receipts": 2,
                "default_max_network_requests": 2,
                "default_delay_seconds": 0,
                "default_timeout_seconds": 1,
                "default_retries": 0,
                "retry_backoff_seconds": 0,
                "user_agent": "test",
            },
            "output": {
                "root": "shadow",
                "raw_dir": "raw",
                "pass_journal_dir": "journals/pass",
                "failure_journal_dir": "journals/fail",
                "checkpoint_path": "checkpoints/latest.json",
                "report_path": "report.json",
            },
        },
    )
    return contract_path


def _html(receipt: str, root_receipt: str) -> bytes:
    return (
        "<html><head><title>official title</title></head><body>"
        '<select id="family">'
        f'<option value="rcpNo={root_receipt}">원본</option>'
        f'<option value="rcpNo={receipt}" selected>정정</option>'
        "</select></body></html>"
    ).encode("utf-8")


def test_dry_run_is_read_only_and_targets_exact_unresolved_set(tmp_path: Path) -> None:
    contract = _fixture(tmp_path)
    result = collector.run_collection(root=tmp_path, contract_path=contract)
    assert result["mode"] == "DRY_RUN"
    assert result["target_receipt_count"] == 2
    assert result["pending_receipt_count"] == 2
    assert result["network_requests"] == 0
    assert not (tmp_path / "shadow").exists()


def test_apply_preserves_landing_and_family_refs_then_replay_skips(tmp_path: Path) -> None:
    contract = _fixture(tmp_path)
    calls: list[str] = []

    def fetcher(endpoint, params, headers, timeout):
        calls.append(params["rcpNo"])
        return 200, _html(params["rcpNo"], "20191231000001"), {"Date": "official"}

    clock = lambda: datetime(2026, 9, 16, tzinfo=timezone.utc)
    result = collector.run_collection(
        root=tmp_path,
        contract_path=contract,
        apply=True,
        confirm_shadow_only=True,
        fetcher=fetcher,
        sleep_fn=lambda _: None,
        clock=clock,
    )
    assert result["status"] == "SHADOW_COMPLETE"
    assert result["completed_receipt_count"] == 2
    assert result["integrity_pass"] is True
    assert calls == RECEIPTS
    journal = json.loads(
        (tmp_path / "shadow/journals/pass/20200101000001.json").read_text(encoding="utf-8")
    )
    assert journal["official_family_reference_count"] == 2
    assert journal["promotion_allowed"] is False

    replay = collector.run_collection(
        root=tmp_path,
        contract_path=contract,
        apply=True,
        confirm_shadow_only=True,
        fetcher=lambda *args: (_ for _ in ()).throw(AssertionError("must skip")),
        sleep_fn=lambda _: None,
        clock=clock,
    )
    assert replay["status"] == "SHADOW_COMPLETE"
    assert replay["network_requests"] == 0


def test_apply_requires_explicit_shadow_confirmation(tmp_path: Path) -> None:
    contract = _fixture(tmp_path)
    with pytest.raises(collector.LineageLandingError, match="SHADOW_CONFIRMATION_REQUIRED"):
        collector.run_collection(root=tmp_path, contract_path=contract, apply=True)


def test_existing_raw_tamper_fails_closed(tmp_path: Path) -> None:
    contract = _fixture(tmp_path)

    def fetcher(endpoint, params, headers, timeout):
        return 200, _html(params["rcpNo"], "20191231000001"), {}

    collector.run_collection(
        root=tmp_path,
        contract_path=contract,
        apply=True,
        confirm_shadow_only=True,
        fetcher=fetcher,
        sleep_fn=lambda _: None,
    )
    journal_path = tmp_path / "shadow/journals/pass/20200101000001.json"
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    (tmp_path / journal["landing"]["relative_path"]).write_bytes(b"tampered")
    with pytest.raises(collector.LineageLandingError, match="EXISTING_SHADOW_INTEGRITY_FAILED"):
        collector.run_collection(root=tmp_path, contract_path=contract)


def test_input_hash_drift_is_rejected(tmp_path: Path) -> None:
    contract = _fixture(tmp_path)
    (tmp_path / "input.json").write_text("{}", encoding="utf-8")
    with pytest.raises(collector.LineageLandingError, match="LINEAGE_REPORT_HASH_MISMATCH"):
        collector.run_collection(root=tmp_path, contract_path=contract)


def test_retry_attempts_never_exceed_network_budget(tmp_path: Path) -> None:
    contract = _fixture(tmp_path)
    calls = 0

    def fetcher(endpoint, params, headers, timeout):
        nonlocal calls
        calls += 1
        return 503, b"busy", {}

    result = collector.run_collection(
        root=tmp_path,
        contract_path=contract,
        apply=True,
        confirm_shadow_only=True,
        max_network_requests=1,
        retries=5,
        fetcher=fetcher,
        sleep_fn=lambda _: None,
    )
    assert calls == 1
    assert result["completed_receipt_count"] == 0
    assert result["stop_reason"] == "LANDING_HTTP_STATUS_INVALID"
