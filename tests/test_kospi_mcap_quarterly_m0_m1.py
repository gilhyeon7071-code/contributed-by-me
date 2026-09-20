from __future__ import annotations

import csv
import json
import shutil
import uuid
from pathlib import Path

import pytest

from paper.strategies.kospi_mcap_quarterly_v1.src import contracts
from paper.strategies.kospi_mcap_quarterly_v1.src.c8_source_audit import (
    REQUIRED_COLUMNS,
    build_report,
    c8_source_contract_sha256,
    load_c8_source_contract,
)


def test_ids_are_deterministic_scoped_and_attempts_are_distinct():
    event = contracts.rebalance_event_id("20260930")
    assert event == contracts.rebalance_event_id("2026-09-30")
    intent = contracts.intent_id(event, "005930", "BUY", "T1")
    assert intent == contracts.intent_id(event, "005930", "buy", "T1")
    assert contracts.order_id(intent, 1) != contracts.order_id(intent, 2)
    with pytest.raises(contracts.ContractError):
        contracts.order_id(intent, 3)


def test_strategy_d_uses_buy_or_sell_actual_fills_only_inside_namespace():
    rows = [
        {"strategy_id": contracts.STRATEGY_ID, "side": "BUY", "filled_qty": 2, "exec_date": "20260901"},
        {"strategy_id": contracts.STRATEGY_ID, "side": "SELL", "filled_qty": 1, "exec_date": "20260903"},
        {"strategy_id": contracts.STRATEGY_ID, "side": "SELL", "filled_qty": 0, "exec_date": "20260910"},
        {"strategy_id": "O6", "side": "BUY", "filled_qty": 1, "exec_date": "20260912"},
    ]
    assert contracts.strategy_d_from_fills(rows) == "20260903"


def test_strategy_write_guard_blocks_existing_o6_paths():
    with pytest.raises(contracts.ContractError):
        contracts.atomic_write_json(contracts.REPO_ROOT / "paper" / "fills.csv", {"bad": True})
    with pytest.raises(contracts.ContractError):
        contracts.publish_validated_json(
            versioned_path=contracts.STRATEGY_ROOT / "data" / "snapshots" / "x.json",
            latest_path=contracts.STRATEGY_ROOT / "data" / "snapshots" / "x_latest.json",
            payload={"status": "FAIL"},
            validation_status="FAIL",
        )


def test_missing_c8_source_fails_closed_without_candidate_or_order():
    missing_root = contracts.STRATEGY_ROOT / "state" / "test_missing_root"
    report = build_report(repo_root=missing_root, generated_at="2026-09-15T06:00:00+09:00")
    assert report["verdict"] == "FAIL_C8_UNIVERSE_SOURCE"
    assert report["m2_allowed"] is False
    assert report["candidate_selection_calculated"] is False
    assert report["target_portfolio_calculated"] is False
    assert report["orders_generated"] is False
    assert report["worst_case"]["verdict"] == "PASS_FAIL_CLOSED"


def _write_valid_canonical(
    path: Path,
    repo_root: Path,
    as_of: str = "20260930",
    date_authority: str = "OFFICIAL_QUERY_PARAMETER",
) -> None:
    strategy_data = repo_root / "paper" / "strategies" / "kospi_mcap_quarterly_v1" / "data"
    snapshot_dir = strategy_data / "source" / "snapshots"
    manifest_dir = strategy_data / "manifests"
    snapshot_dir.mkdir(parents=True)
    manifest_dir.mkdir(parents=True)
    roles = [
        "listing_security_as_of",
        "trade_management_status_as_of",
        "corporate_actions_as_of",
        "close_market_cap_as_of",
    ]
    components = []
    for role in roles:
        component_path = snapshot_dir / f"{role}_{as_of}.csv"
        component_path.write_text(f"role,as_of_date\n{role},{as_of}\n", encoding="utf-8")
        components.append(
            {
                "role": role,
                "status": "PASS",
                "as_of_date": as_of,
                "date_authority": date_authority,
                "path": component_path.relative_to(repo_root).as_posix(),
                "sha256": contracts.sha256_file(component_path),
                "rows": 100,
            }
        )
    manifest_path = manifest_dir / f"c8_source_manifest_{as_of}.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_schema_version": "1.0.0",
                "contract_id": "C8_CANONICAL_SOURCE_V1",
                "strategy_id": contracts.STRATEGY_ID,
                "as_of_date": as_of,
                "source_components": components,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    source_reference = manifest_path.relative_to(repo_root).as_posix()
    source_sha256 = contracts.sha256_file(manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(REQUIRED_COLUMNS))
        writer.writeheader()
        for index in range(100):
            code = f"{index:06d}"
            writer.writerow(
                {
                    "as_of_date": as_of,
                    "code": code,
                    "name": f"TEST{index}",
                    "market": "KOSPI",
                    "security_type": "COMMON",
                    "security_type_rule": "OFFICIAL_FIELD",
                    "listed_date": "20000101",
                    "delisted_date": "",
                    "listing_status": "ACTIVE",
                    "trade_status": "TRADING",
                    "management_status": "NORMAL",
                    "delisting_procedure_status": "NONE",
                    "merger_status": "NONE",
                    "event_type": "NONE",
                    "event_effective_date": "",
                    "close": "10000",
                    "close_as_of": as_of,
                    "market_cap": str(100000000000 + index),
                    "market_cap_as_of": as_of,
                    "source": source_reference,
                    "source_sha256": source_sha256,
                }
            )


def test_integrated_official_c8_source_with_n100_can_open_m2():
    test_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_{uuid.uuid4().hex}"
    source = test_root / "paper" / "strategies" / "kospi_mcap_quarterly_v1" / "data" / "source" / "c8_universe_source_latest.csv"
    try:
        _write_valid_canonical(source, test_root)
        report = build_report(
            repo_root=test_root,
            canonical_source=source,
            selection_as_of="20260930",
            generated_at="2026-09-15T06:00:00+09:00",
        )
        assert report["status"] == "PASS"
        assert report["verdict"] == "C8_UNIVERSE_SOURCE_READY"
        assert report["canonical_source_validation"]["eligible_rows"] == 100
        assert report["m2_allowed"] is True
        assert all(item["status"] == "PASS" for item in report["component_checks"])
    finally:
        shutil.rmtree(test_root, ignore_errors=True)


def test_filename_only_manifest_date_authority_fails_closed():
    test_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_manifest_{uuid.uuid4().hex}"
    source = test_root / "paper" / "strategies" / "kospi_mcap_quarterly_v1" / "data" / "source" / "c8_universe_source_latest.csv"
    try:
        _write_valid_canonical(source, test_root, date_authority="FILENAME_ONLY_UNVERIFIED_ASOF")
        report = build_report(
            repo_root=test_root,
            canonical_source=source,
            selection_as_of="20260930",
            generated_at="2026-09-15T06:00:00+09:00",
        )
        canonical = report["canonical_source_validation"]
        component_reasons = {
            reason
            for manifest in canonical["source_manifest_checks"]
            for component in manifest["component_checks"]
            for reason in component["reason_codes"]
        }
        assert "SOURCE_MANIFEST_INVALID" in canonical["reason_codes"]
        assert "SOURCE_ROLE_DATE_AUTHORITY_INVALID" in component_reasons
        assert report["verdict"] == "FAIL_C8_UNIVERSE_SOURCE"
        assert report["m2_allowed"] is False
        assert report["orders_generated"] is False
    finally:
        shutil.rmtree(test_root, ignore_errors=True)


def test_manifest_component_tamper_fails_closed():
    test_root = contracts.STRATEGY_ROOT / "state" / f"test_c8_tamper_{uuid.uuid4().hex}"
    source = test_root / "paper" / "strategies" / "kospi_mcap_quarterly_v1" / "data" / "source" / "c8_universe_source_latest.csv"
    try:
        _write_valid_canonical(source, test_root)
        component = (
            test_root
            / "paper"
            / "strategies"
            / "kospi_mcap_quarterly_v1"
            / "data"
            / "source"
            / "snapshots"
            / "listing_security_as_of_20260930.csv"
        )
        component.write_text("tampered\n", encoding="utf-8")
        report = build_report(
            repo_root=test_root,
            canonical_source=source,
            selection_as_of="20260930",
            generated_at="2026-09-15T06:00:00+09:00",
        )
        canonical = report["canonical_source_validation"]
        component_reasons = {
            reason
            for manifest in canonical["source_manifest_checks"]
            for check in manifest["component_checks"]
            for reason in check["reason_codes"]
        }
        assert "SOURCE_COMPONENT_HASH_MISMATCH" in component_reasons
        assert "SOURCE_MANIFEST_INVALID" in canonical["reason_codes"]
        assert report["m2_allowed"] is False
        assert report["orders_generated"] is False
    finally:
        shutil.rmtree(test_root, ignore_errors=True)


def test_manual_status_flags_are_visible_but_do_not_open_m2():
    test_root = contracts.STRATEGY_ROOT / "state" / f"test_manual_c8_{uuid.uuid4().hex}"
    source = test_root / "_cache" / "manual_static_population_source" / "data_4446_20260715.csv"
    try:
        source.parent.mkdir(parents=True)
        with source.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "종목코드",
                    "종목명",
                    "매매거래정지",
                    "정리매매 종목",
                    "관리종목",
                    "투자주의환기종목",
                    "투자경고종목",
                    "투자위험종목",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "종목코드": "000001",
                    "종목명": "TEST",
                    "매매거래정지": "O",
                    "정리매매 종목": "X",
                    "관리종목": "O",
                    "투자주의환기종목": "X",
                    "투자경고종목": "X",
                    "투자위험종목": "X",
                }
            )

        report = build_report(repo_root=test_root, generated_at="2026-09-15T06:00:00+09:00")
        status_source = next(
            item for item in report["manual_raw_source_inventory"] if item["source_kind"] == "status_flags"
        )
        trade_status = next(item for item in report["c8_field_matrix"] if item["field"] == "trade_status")

        assert status_source["file_date_hint"] == "20260715"
        assert status_source["date_authority"] == "FILENAME_ONLY_UNVERIFIED_ASOF"
        assert status_source["positive_flag_counts"]["매매거래정지"] == 1
        assert status_source["positive_flag_counts"]["관리종목"] == 1
        assert trade_status["status"] == "PARTIAL"
        assert report["verdict"] == "FAIL_C8_UNIVERSE_SOURCE"
        assert report["m2_allowed"] is False
        assert report["worst_case"]["verdict"] == "PASS_FAIL_CLOSED"
    finally:
        shutil.rmtree(test_root, ignore_errors=True)


def test_contract_file_preserves_isolation_and_approved_constants():
    contract = contracts.load_contract()
    assert contract["execution_scope"] == "ISOLATED_PAPER_ONLY"
    assert contract["broker_dispatch"] == "FORBIDDEN"
    assert contract["universe"]["n"] == 100
    assert contract["weighting"]["basket_weight_cap"] == pytest.approx(0.2)
    assert contract["termination"]["nav_latch_at_or_below_krw"] == 75_000_000
    assert contract["lineage"]["o6_global_d"] == "UNCHANGED_LATEST_BUY_FILL_DATE"
    assert set(REQUIRED_COLUMNS) == set(contract["c8_required_columns"])
    c8_contract = load_c8_source_contract()
    assert c8_contract["canonical_csv"]["primary_key"] == ["as_of_date", "code"]
    assert c8_contract["join_contract"]["name_join_forbidden"] is True
    assert "FILENAME_ONLY_UNVERIFIED_ASOF" in c8_contract["manifest_contract"]["forbidden_date_authority"]
    assert len(c8_contract["manifest_contract"]["required_source_roles"]) == 4
    assert len(c8_source_contract_sha256()) == 64
