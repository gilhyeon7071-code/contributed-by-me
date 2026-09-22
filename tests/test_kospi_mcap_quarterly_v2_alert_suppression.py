"""예행·검증이 **사용자에게 가짜 경보를 보내지 않는지** (2026-09-22 실측 사고).

2026-09-22 13:44, 검증 배터리가 돌린 예행의 **가짜 래치 시나리오**가
"[V2 아침] 20260922 LIQUIDATE status=STOP" 을 사용자 휴대폰으로 보냈다.
진짜 청산으로 읽힌다. 토큰이 깨져 있던 34시간 동안 404 라 안 보이다가
토큰을 고치자 드러났다 — 확인하는 행위가 사람을 깨우면 안 된다.

**더 중요한 것은 그 반대다.** 억제 스위치가 실경보까지 막으면 최악이다.
그래서 여기서는 '안 보낸다' 와 '그래도 보낸다' 를 **둘 다** 고정한다.
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
V2 = ROOT / "paper" / "strategies" / "kospi_mcap_quarterly_v2"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(V2 / "src"))
sys.path.insert(0, str(ROOT / "tools"))

from paper.strategies.kospi_mcap_quarterly_v2.src import daily_ops as D  # noqa: E402


@pytest.fixture
def transport(monkeypatch):
    """전송 계층을 센다. `alert=` 기본 인자는 정의 시점에 묶여 바꿔치기가 안 먹는다."""
    import notify_channels as N
    sent = []
    monkeypatch.setattr(N, "send_alert",
                        lambda text, level="error", **k: (sent.append(level), {"ok": True})[1])
    return sent


def test_real_alert_still_goes_out(transport, monkeypatch):
    """기본은 **보낸다.** 이걸 놓치면 경보 두절이고, 그게 더 나쁜 고장이다."""
    monkeypatch.delenv("V2_SUPPRESS_ALERT", raising=False)
    D._send_alert("진짜 사고", level="error")
    assert transport == ["error"]


@pytest.mark.parametrize("val", ["1", "true", "TRUE"])
def test_switch_on_suppresses(transport, monkeypatch, capsys, val):
    monkeypatch.setenv("V2_SUPPRESS_ALERT", val)
    D._send_alert("예행", level="error")
    assert transport == []
    # 조용히 삼키지 않는다. 다만 **stderr 로** 낸다 — stdout 은 보고서(JSON) 통로다
    assert "ALERT_SUPPRESSED" in capsys.readouterr().err


@pytest.mark.parametrize("val", ["0", "", "no", "false"])
def test_other_values_do_not_suppress(transport, monkeypatch, val):
    """모호한 값으로 경보가 꺼지면 안 된다 — 끄는 쪽이 명시적이어야 한다."""
    monkeypatch.setenv("V2_SUPPRESS_ALERT", val)
    D._send_alert("진짜 사고", level="error")
    assert transport == ["error"]


def test_rehearsal_sends_nothing_and_says_so(transport, tmp_path):
    import morning_branch_rehearsal as R
    shutil.rmtree(tmp_path / "r", ignore_errors=True)
    rec = R.run(tmp_path / "r", "20260922")
    assert transport == [], "예행이 사용자에게 경보를 보냈다"
    assert rec.get("alerts_suppressed") is True          # 증거에 사실을 남긴다
    assert rec["verdict"] == "PASS"


def test_rehearsal_restores_the_switch(transport, tmp_path, monkeypatch):
    """예행이 끝난 뒤에도 꺼져 있으면 **그날의 실경보가 전부 사라진다.**"""
    monkeypatch.delenv("V2_SUPPRESS_ALERT", raising=False)
    import morning_branch_rehearsal as R
    R.run(tmp_path / "r2", "20260922")
    assert "V2_SUPPRESS_ALERT" not in os.environ
    D._send_alert("예행 뒤의 진짜 사고", level="error")
    assert transport == ["error"]


def test_preexisting_switch_value_is_restored(transport, tmp_path, monkeypatch):
    monkeypatch.setenv("V2_SUPPRESS_ALERT", "1")
    import morning_branch_rehearsal as R
    R.run(tmp_path / "r3", "20260922")
    assert os.environ.get("V2_SUPPRESS_ALERT") == "1"


# ---------------------------------------------------- 진단 출력은 보고서 통로를 오염시키지 않는다
def test_diagnostics_go_to_stderr_not_stdout(transport, monkeypatch, capsys):
    """**09-22 미스터리의 진범.**

    `_send_alert` 가 전송 실패를 stdout 에 찍었다. stdout 은 이 배치의 보고서(JSON) 통로다.
    그래서 예행이 `json.loads(stdout)` 에서 깨져 FAIL 이 났고, 분기 4개는 전부 정답이었다.
    쿨다운 600초 때문에 **10분에 한 번만** 찍혀 재현이 안 됐다(33회 연속 PASS).
    """
    import notify_channels as N
    monkeypatch.delenv("V2_SUPPRESS_ALERT", raising=False)
    monkeypatch.setattr(N, "send_alert", lambda *a, **k: {"ok": False, "why": "일부러 실패"})
    D._send_alert("진짜 사고", level="error")
    cap = capsys.readouterr()
    assert cap.out == "", "보고서 통로(stdout)가 오염됐다: %r" % cap.out
    assert "ALERT_FAILED" in cap.err          # 조용히 삼키지도 않는다


def test_suppression_notice_also_avoids_stdout(transport, monkeypatch, capsys):
    monkeypatch.setenv("V2_SUPPRESS_ALERT", "1")
    D._send_alert("예행", level="error")
    cap = capsys.readouterr()
    assert cap.out == "" and "ALERT_SUPPRESSED" in cap.err


def test_alert_failure_does_not_break_the_rehearsal(tmp_path, monkeypatch):
    """전송이 실패해도 예행 판정이 멀쩡해야 한다 — 그게 09-22 에 뒤집혔던 것이다."""
    import notify_channels as N
    monkeypatch.delenv("V2_SUPPRESS_ALERT", raising=False)
    monkeypatch.setattr(N, "send_alert", lambda *a, **k: {"ok": False, "why": "일부러 실패"})
    import morning_branch_rehearsal as R
    rec = R.run(tmp_path / "boom", "20260922")
    assert rec["verdict"] == "PASS"
    assert not [s for s in rec["scenarios"] if s.get("report_note")], "보고서 통로가 오염됐다"
