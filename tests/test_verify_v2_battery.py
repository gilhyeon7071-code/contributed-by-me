"""검증 배터리가 **고장에서 우는지** (2026-09-22 신설).

배터리는 초록을 내는 것이 일이 아니다. 고장을 빨갛게 만드는 것이 일이다.
정상에서 침묵하는 것만 확인하고 끝내면, 배터리 자신이 09-22 의 `craft_scan [4]`
(평소 호출로 한 번도 안 돌던 검사)와 같은 물건이 된다.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import verify_v2_battery as B  # noqa: E402


def _stub(monkeypatch, rc: int, out: str):
    monkeypatch.setattr(B, "_run", lambda *a, **k: (rc, out))


def _check(monkeypatch, fn, rc, out):
    it = B.Item("k", "t")
    _stub(monkeypatch, rc, out)
    fn(it)
    return it


# ---------------------------------------------------------------- 정상에서 침묵
@pytest.mark.parametrize("fn,rc,out,want", [
    (B.c_pytest, 0, "875 passed, 6 warnings in 92s", B.PASS),
    (B.c_craft_scan, 0, "[1] a 0건\n[2] b 0건\n[8] c 0건", B.PASS),
    (B.c_freshness, 0, "[FRESH_GUARD] total=77 fresh=52 stale=0 missing=0 x", B.PASS),
    (B.c_go_nogo, 0, "PASS 9 / FAIL 0 / UNKNOWN 0", B.PASS),
    (B.c_boundary, 0, "변화 없음", B.PASS),
    (B.c_claims_fire, 0, "전체 33건 중 안 우는 것 0건", B.PASS),
])
def test_healthy_is_pass(monkeypatch, fn, rc, out, want):
    assert _check(monkeypatch, fn, rc, out).status == want


# ---------------------------------------------------------------- 고장에서 울린다
@pytest.mark.parametrize("fn,rc,out", [
    (B.c_pytest, 1, "3 failed, 872 passed in 90s"),
    (B.c_craft_scan, 1, "[1] a 0건\n[4] b 2건\n[8] c 0건"),
    (B.c_freshness, 1, "[FRESH_GUARD] total=77 fresh=50 stale=2 missing=0 x"),
    (B.c_freshness, 1, "[FRESH_GUARD] total=77 fresh=50 stale=0 missing=1 x"),
    (B.c_go_nogo, 1, "PASS 8 / FAIL 1 / UNKNOWN 0"),
    (B.c_boundary, 1, "append-only 파일이 줄었다"),
    (B.c_claims_fire, 0, "전체 33건 중 안 우는 것 3건"),
])
def test_broken_is_fail(monkeypatch, fn, rc, out):
    assert _check(monkeypatch, fn, rc, out).status == B.FAIL


# ---------------------------------------------------------------- 모르면 모른다
@pytest.mark.parametrize("fn", [B.c_pytest, B.c_craft_scan, B.c_freshness,
                                B.c_go_nogo, B.c_claims_fire, B.c_rehearsal])
def test_unreadable_output_is_unknown_not_pass(monkeypatch, fn):
    """출력을 못 읽었으면 UNKNOWN 이다. PASS 로 접으면 감시가 없는 것보다 나쁘다."""
    assert _check(monkeypatch, fn, 0, "알 수 없는 출력").status == B.UNKNOWN


def test_timeout_is_not_swallowed(monkeypatch):
    """타임아웃이 rc 0 으로 접히면 원인이 지워진다 — 그 자리를 막는다."""
    def fake(cmd, **kw):
        import subprocess
        raise subprocess.TimeoutExpired(cmd, kw.get("timeout", 0))
    monkeypatch.setattr(B.subprocess, "run", fake)
    rc, out = B._run(["x"], Path("."), 5)
    assert rc == 124 and "TIMEOUT" in out


def test_go_nogo_unknown_is_not_a_pass(monkeypatch):
    """가부 규칙과 같다 — UNKNOWN 이 섞이면 통과가 아니다."""
    assert _check(monkeypatch, B.c_go_nogo, 0, "PASS 8 / FAIL 0 / UNKNOWN 1").status == B.FAIL


def test_rehearsal_reports_stdout_junk(monkeypatch):
    """예행이 버텨도 잡소리가 있었으면 **말한다** — 원인 미상인 현상이다."""
    import json as _j
    rec = {"verdict": "PASS", "scenarios": [{"label": "래치", "report_note": "잡소리"}]}
    it = _check(monkeypatch, B.c_rehearsal, 0, _j.dumps(rec, ensure_ascii=False))
    assert it.status == B.PASS and "잡소리" in it.detail


def test_schedule_catches_a_disabled_task(monkeypatch):
    _stub(monkeypatch, 0, "VIBE_V2_Daily_Morning_1000|Disabled|2026-09-23|run_v2_daily_ops.bat morning")
    it = B.Item("k", "t"); B.c_schedule(it)
    assert it.status == B.FAIL and "Disabled" in it.detail


def test_schedule_catches_a_changed_command(monkeypatch):
    """'Ready' 는 명령줄이 옳다는 뜻이 아니다."""
    _stub(monkeypatch, 0, "VIBE_V2_Daily_Morning_1000|Ready|2026-09-23|엉뚱한.bat morning")
    it = B.Item("k", "t"); B.c_schedule(it)
    assert it.status == B.FAIL and "명령줄" in it.detail


def test_schedule_catches_no_next_run(monkeypatch):
    """다음 실행이 없으면 검증일이 영원히 안 온다."""
    _stub(monkeypatch, 0, "VIBE_V2_Daily_Morning_1000|Ready||run_v2_daily_ops.bat morning")
    it = B.Item("k", "t"); B.c_schedule(it)
    assert it.status == B.FAIL and "다음실행없음" in it.detail


def test_skipped_items_are_unknown_not_pass(monkeypatch, tmp_path, capsys):
    """--quick 은 '빨리 통과' 가 아니라 '확인 안 함' 이다."""
    monkeypatch.setattr(B, "_run", lambda *a, **k: (0, "PASS 9 / FAIL 0 / UNKNOWN 0"))
    rc = B.main(["--quick", "--only", "pytest", "--json", str(tmp_path / "o.json")])
    out = capsys.readouterr().out
    assert rc == 1 and "UNKNOWN 1" in out and "건너뜀" in out


def test_runner_missing_is_unknown_not_fail(monkeypatch):
    """실행 자체가 안 된 것은 '고장' 이 아니라 '확인 못 함' 이다.

    [2026-09-22] `npx` 를 셸 없이 부르지 못해 FileNotFoundError 가 났고
    배터리가 그걸 FAIL 로 적었다 — 타입 오류가 있는 것처럼 읽힌다."""
    monkeypatch.setattr(B, "_run", lambda *a, **k: (-1, "FileNotFoundError: 없다"))
    monkeypatch.setattr(Path, "is_file", lambda self: True)
    it = B.Item("k", "t"); B.c_dashboard_types(it)
    assert it.status == B.UNKNOWN and "돌리지 못했다" in it.detail


# ------------------------------------------------ 경보 통로 (2026-09-22 실측 사고)
def _alert(monkeypatch, tok, cid="7176011988", getme=None, exc=None):
    import notify_channels as _N
    monkeypatch.setattr(_N, "_secret_or_env",
                        lambda k, f: tok if "TOKEN" in k else cid)
    import urllib.request as U

    class _R:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): 
            import json
            return json.dumps(getme).encode()

    def fake(url, timeout=0):
        if exc: raise exc
        return _R()
    monkeypatch.setattr(U, "urlopen", fake)
    it = B.Item("alert", "경보")
    B.c_alert_channel(it)
    return it


_GOOD = "8931399277:" + "A" * 35


def test_alert_channel_ok(monkeypatch):
    it = _alert(monkeypatch, _GOOD, getme={"ok": True, "result": {"username": "bot"}})
    assert it.status == B.PASS


def test_prose_in_token_file_is_caught(monkeypatch):
    """09-21 13:18 의 실제 사고 모양 — BotFather 메시지 전문이 통째로 들어갔다."""
    bad = "Your token was replaced with a new one. You can use this token: " + _GOOD
    it = _alert(monkeypatch, bad, getme={"ok": True})
    assert it.status == B.FAIL and "다른 것이 섞였다" in it.detail


def test_invalid_token_is_fail_not_unknown(monkeypatch):
    import urllib.error as E
    it = _alert(monkeypatch, _GOOD, exc=E.HTTPError("u", 401, "x", None, None))
    assert it.status == B.FAIL and "무효" in it.detail


def test_network_trouble_is_unknown_not_fail(monkeypatch):
    """망 문제와 토큰 무효는 다른 일이다 — 섞으면 엉뚱한 곳을 고친다."""
    it = _alert(monkeypatch, _GOOD, exc=OSError("timed out"))
    assert it.status == B.UNKNOWN


def test_missing_token_is_fail(monkeypatch):
    assert _alert(monkeypatch, "", getme={"ok": True}).status == B.FAIL


# ------------------------------------------------ 원장 대조: 대조 불가는 통과가 아니다
def test_reconcile_no_broker_is_unknown(monkeypatch):
    monkeypatch.setattr(Path, "is_file", lambda self: True)
    it = _check(monkeypatch, B.c_ledger, 0, "[RECON] status=NO_BROKER 계좌=mock")
    assert it.status == B.UNKNOWN and "대조 불가" in it.detail


def test_reconcile_ok_is_pass(monkeypatch):
    monkeypatch.setattr(Path, "is_file", lambda self: True)
    assert _check(monkeypatch, B.c_ledger, 0, "[RECON] status=OK 계좌=mock").status == B.PASS
