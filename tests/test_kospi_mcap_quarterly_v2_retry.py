"""아침 작업 재시도 — **하면 안 될 때 안 하는지**가 본체 (2026-09-21).

배경: 사용자 *"매수면 바로바로 가능한 것인데 왜 기다려서 확인하는가 / 그렇게 간 시간이 1년이다"*.
topn 때 이미 적어둔 교훈을 새 로직이 안 따르고 있었다 —
*"발주 시각은 시계가 아니라 **조건**이 정한다"*(PLANS 29274).

10:00 에 호가가 덜 들어와 막히면 그날이 끝났다. 10-01 첫 재구성이 그러면 50종목이 하루 날아간다.
그래서 **주문을 한 건도 못 보낸 채 일시적 사유로 멈춘 경우**에만 같은 날 다시 시도한다.
C9(미체결 -> 다음 거래일 1회 재주문)는 건드리지 않는다 — 그건 다른 상태다."""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from paper.strategies.kospi_mcap_quarterly_v2.src import daily_ops as D  # noqa: E402
from test_kospi_mcap_quarterly_v2_runner import CFG  # noqa: E402

NOW = datetime(2026, 10, 2, 11, 0, 0)
OPS = {"auto_submit": True, "retry_until": "14:30", "retry_max": 4}


def _clock(t=NOW):
    return lambda: t


def _state(tmp_path: Path, rows) -> Path:
    st = tmp_path / "state"
    st.mkdir(parents=True, exist_ok=True)
    (st / D.DAILY_LOG).write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows),
                                  encoding="utf-8")
    return st


def _morning_row(status="STOP", reasons=None, job="morning"):
    return {"job": job, "date": "20261002", "run_at": "2026-10-02T10:00:00",
            "status": status, "reasons": reasons if reasons is not None else []}


def _retry(st, ops=None, now=NOW, called=None):
    def fake_morning(client, **kw):
        if called is not None:
            called.append(kw)
        return {"status": "OK", "reasons": [], "action": "QUARTERLY"}
    D.morning, real = fake_morning, D.morning
    try:
        return D.retry(object(), state_dir=st, cfg=CFG, ops=ops or OPS, clock=_clock(now))
    finally:
        D.morning = real


# ---------------------------------------------------------------- 분류
@pytest.mark.parametrize("reasons,want", [
    (["FAIL_EXECUTION_COVERAGE:0.1500"], "TRANSIENT"),
    (["BUY_CASH_SHORT:need=1>available=0"], "TRANSIENT"),
    (["TARGET_EXPOSURE_MISMATCH:weights=0.9:exposure=1.0"], "PERMANENT"),
    (["LEDGER_EXCEEDS_BROKER_HOLDING:005930(ledger=5,broker=0)"], "PERMANENT"),
    # 영구가 하나라도 섞이면 영구다 — 같은 실패를 반복하지 않는다
    (["FAIL_EXECUTION_COVERAGE:0.2", "TARGET_EXPOSURE_MISMATCH:x"], "PERMANENT"),
    (["뭔가 처음 보는 사유"], "UNKNOWN"),
    ([], "UNKNOWN"),
])
def test_classify_stop(reasons, want):
    assert D.classify_stop(reasons) == want


# ---------------------------------------------------------------- 재시도해야 하는 경우
def test_transient_stop_is_retried(tmp_path):
    st = _state(tmp_path, [_morning_row(reasons=["FAIL_EXECUTION_COVERAGE:0.1500"])])
    called = []
    rep = _retry(st, called=called)
    assert rep.get("retried") is True and rep["attempt"] == 1 and len(called) == 1
    assert rep["status"] == "OK"


# ---------------------------------------------------------------- 재시도하면 안 되는 경우들
def test_orders_already_placed_is_not_retried(tmp_path):
    """pending 이 있으면 **주문이 이미 나갔다는 뜻** — 다시 보내면 중복 발주다."""
    st = _state(tmp_path, [_morning_row(reasons=["FAIL_EXECUTION_COVERAGE:0.2"])])
    (st / D.PENDING).write_text(json.dumps({"kind": "QUARTERLY"}), encoding="utf-8")
    called = []
    rep = _retry(st, called=called)
    assert rep["reasons"] == ["ORDERS_ALREADY_PLACED"] and called == []


def test_permanent_stop_is_not_retried(tmp_path):
    st = _state(tmp_path, [_morning_row(reasons=["TARGET_EXPOSURE_MISMATCH:x"])])
    called = []
    rep = _retry(st, called=called)
    assert rep["stop_kind"] == "PERMANENT" and called == []


def test_unknown_stop_is_not_retried(tmp_path):
    """모르는 사유는 사람이 본다 — 자동 재시도는 원인을 지운다."""
    st = _state(tmp_path, [_morning_row(reasons=["처음 보는 것"])])
    called = []
    assert _retry(st, called=called)["stop_kind"] == "UNKNOWN" and called == []


def test_ok_morning_is_not_retried(tmp_path):
    st = _state(tmp_path, [_morning_row(status="OK")])
    called = []
    rep = _retry(st, called=called)
    assert rep["reasons"][0].startswith("NOTHING_TO_RETRY") and called == []


def test_no_morning_run_is_standby(tmp_path):
    st = _state(tmp_path, [])
    called = []
    rep = _retry(st, called=called)
    assert rep["reasons"] == ["NO_MORNING_RUN_TODAY"] and called == []


def test_max_retries_is_enforced(tmp_path):
    rows = [_morning_row(reasons=["BUY_CASH_SHORT:x"])]
    rows += [{**_morning_row(job="morning_retry"), "retried": True} for _ in range(4)]
    rows.append(_morning_row(reasons=["BUY_CASH_SHORT:x"]))
    st = _state(tmp_path, rows)
    called = []
    rep = _retry(st, called=called)
    assert rep["reasons"][0].startswith("MAX_RETRIES") and called == []


def test_after_deadline_is_not_retried(tmp_path):
    st = _state(tmp_path, [_morning_row(reasons=["BUY_CASH_SHORT:x"])])
    called = []
    rep = _retry(st, now=datetime(2026, 10, 2, 15, 0, 0), called=called)
    assert rep["reasons"][0].startswith("AFTER_DEADLINE") and called == []


def test_every_attempt_is_logged(tmp_path):
    """무엇을 왜 다시 했는지 기록에 남아야 한다 — 재시도가 조용하면 원인 추적이 끊긴다."""
    st = _state(tmp_path, [_morning_row(reasons=["FAIL_EXECUTION_COVERAGE:0.2"])])
    _retry(st)
    rows = D._log_rows(st)
    last = rows[-1]
    assert last["job"] == "morning_retry" and last["retried"] is True
    assert "RETRY_AFTER_TRANSIENT_STOP" in last["reasons"][0]


def test_shipped_config_has_bounds():
    p = ROOT / "paper" / "strategies" / "kospi_mcap_quarterly_v2" / "config" / "daily_ops_v1.json"
    d = json.loads(p.read_text(encoding="utf-8"))
    assert d["retry_max"] >= 1 and ":" in str(d["retry_until"])


# ------------------------------------------------ 재시도 연쇄의 기록 (2026-09-22 실측)
def test_retry_chain_judges_the_morning_row_not_its_own(tmp_path):
    """아침 STOP -> 재시도 한도 소진 뒤에도 사유가 **아침 결과**를 가리켜야 한다.

    종전: morning_retry 줄까지 섞어 마지막 줄로 판정해 'NOTHING_TO_RETRY:last=STANDBY'
    가 나왔다. 아침은 STOP 이었는데 기록은 STANDBY 였다고 말한 것이다."""
    rows = [_morning_row(reasons=["BUY_CASH_SHORT:x"])]
    rows += [{**_morning_row(job="morning_retry", status="RETRYING"), "retried": True} for _ in range(4)]
    rows.append({**_morning_row(job="morning_retry"), "reasons": ["MAX_RETRIES:4>=4"]})
    st = _state(tmp_path, rows)
    called = []
    rep = _retry(st, called=called)
    assert rep["reasons"][0].startswith("MAX_RETRIES"), rep["reasons"]
    assert called == []


def test_fired_retry_is_not_recorded_as_standby(tmp_path):
    """발사한 재시도와 건너뛴 재시도가 기록에서 구분돼야 한다."""
    st = _state(tmp_path, [_morning_row(reasons=["FAIL_EXECUTION_COVERAGE:0.2"])])
    _retry(st)
    fired = D._log_rows(st)[-1]
    assert fired["status"] == "RETRYING" and fired["retried"] is True

    st2 = _state(tmp_path / "b", [_morning_row(status="OK")])
    _retry(st2)
    skipped = D._log_rows(st2)[-1]
    assert skipped["status"] == "STANDBY" and not skipped.get("retried")
