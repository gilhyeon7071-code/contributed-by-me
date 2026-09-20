# -*- coding: utf-8 -*-
"""--out-dir 로 격리한 실행이 공유 2_Logs 를 건드리지 않는지 검사한다.

2026-09-11 에 **하루에 두 번** 같은 결함을 만들었다 — 자산 원장과 account_basis 둘 다
`--out-dir` 을 무시하고 `2_Logs` 에 직접 썼다. 시험 실행이 공유 SSOT 에 행을 남겼다.

절차서로 막으려 했으나 그건 사람이 읽어야 작동한다. 이 테스트는 잊어도 작동한다.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import broker_ledger_reconcile as M  # noqa: E402


def _mtime(p: Path):
    return p.stat().st_mtime if p.exists() else None


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(M, "EQUITY_LEDGER", tmp_path / "account_equity_ledger.csv")
    return tmp_path


def test_append_equity_writes_only_under_out_dir(isolated):
    shared = M.LOG_DIR / "account_equity_ledger.csv"
    before = _mtime(shared)

    M.append_equity({
        "ts": "2026-01-01T00:00:00", "mode": "test", "equity": 1.0,
        "cash": 1.0, "positions_value": 0.0, "codes": 0, "source": "unit",
    })

    assert (isolated / "account_equity_ledger.csv").is_file()
    assert _mtime(shared) == before, "공유 2_Logs 원장이 변경됐다"


def test_write_account_basis_writes_only_under_out_dir(isolated):
    shared = M.LOG_DIR / "broker_account_basis_latest.json"
    before = _mtime(shared)

    out = M.write_account_basis({
        "status": "OK", "equity": 100.0, "peak": 100.0, "cash": 10.0,
        "positions_value": 90.0, "mdd_pct": 0.0, "capital_total": 100.0,
    }, dt.datetime(2026, 1, 1))

    assert out is not None and out.parent == isolated
    assert out.is_file()
    assert _mtime(shared) == before, "공유 2_Logs account_basis 가 변경됐다"


def test_write_account_basis_refuses_when_valuation_missing(isolated):
    """모르는 값을 0 으로 채우지 않는다 — 계산 자체를 안 한다."""
    assert M.write_account_basis({"status": "NO_VALUATION"}, dt.datetime(2026, 1, 1)) is None
