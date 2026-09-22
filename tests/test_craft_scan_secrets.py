"""반복 실수 스캔 [7] 코드에 박힌 자격증명 — 잡는지 본다 (2026-09-21).

배경: 텔레그램 봇 토큰이 2026-06-27 에 `tools/test_tg.py` 에 박혔고 **석 달 뒤에야** 나왔다.
추적 중이었고 GitHub 원격이 붙어 있어 push 한 번이면 공개되는 상태였다.
그래서 여기서 볼 것은 "조용한가" 가 아니라 **"넣으면 잡는가"** 다."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import craft_scan as C  # noqa: E402

# 스캐너가 **이 파일 자신을 잡지 않도록** 리터럴을 두지 않고 조립한다.
# 허용목록으로 덮으면 다음에 진짜를 놓친다 — 오탐이 남으면 스캔 전체가 무시된다.
_TOK = "8931399277" + ":" + "AA" + "GXaRL1meuMtgS5SC3kajES374xJ8WY-nY"
_SEC = "abcdefghijklmnopqrstuvwxyz0123456789+/="
_KEY = "PS" + "abcdefghijklmnopqrstuvwxyz0123"
_PW = "hunter2!"


def _scan(tmp_path: Path, monkeypatch, rel: str, body: str):
    p = tmp_path / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding="utf-8")
    monkeypatch.setattr(C, "ROOT", tmp_path)
    return C._plaintext_secrets([p])


# ---------------------------------------------------------------- 잡아야 하는 것
@pytest.mark.parametrize("body,label", [
    (f"TOKEN = '{_TOK}'", "텔레그램 봇 토큰"),
    (f'app_secret = "{_SEC}"', "API 시크릿"),
    (f'APP_KEY = "{_KEY}"', "API 키"),
    (f"password = '{_PW}'", "평문 비밀번호"),
])
def test_secret_is_caught(tmp_path, monkeypatch, body, label):
    hits = _scan(tmp_path, monkeypatch, "tools/x.py", body)
    assert len(hits) == 1 and hits[0]["text"] == label and hits[0]["line"] == 1


def test_real_incident_is_caught(tmp_path, monkeypatch):
    """2026-06-27 실제 사건의 파일 모양 그대로."""
    body = ("import urllib.request\n"
            f"TOKEN = '{_TOK}'\n"
            "CHAT_ID = '7176011988'\n")
    hits = _scan(tmp_path, monkeypatch, "tools/test_tg.py", body)
    assert hits and hits[0]["line"] == 2


# ---------------------------------------------------------------- 잡으면 안 되는 것
@pytest.mark.parametrize("body", [
    "cfg = KISConfig(app_key='dummy', app_secret='dummy')",      # 시험용 더미
    'APP_KEY = "YOUR_APP_KEY_HERE"',
    'password = "<your_password>"',
    "# 예: app_secret = 'example0123456789012345678901234567890'",
])
def test_placeholders_are_not_flagged(tmp_path, monkeypatch, body):
    assert _scan(tmp_path, monkeypatch, "tools/x.py", body) == []


def test_secrets_dir_is_not_scanned(tmp_path, monkeypatch):
    """`.secrets/` 는 정식 보관처다 — 거기 있는 건 결함이 아니다."""
    assert _scan(tmp_path, monkeypatch, ".secrets/telegram_bot_token.txt", _TOK) == []


def test_backup_dir_is_not_scanned(tmp_path, monkeypatch):
    assert _scan(tmp_path, monkeypatch, "backup/old/test_tg.py", f"TOKEN = '{_TOK}'") == []


def test_scan_reports_the_new_key(tmp_path, monkeypatch):
    """scan() 결과에 키가 실제로 실리는지 — 함수만 고치고 배선을 빠뜨리면 감시가 없는 것이다."""
    (tmp_path / "tools").mkdir(parents=True)
    (tmp_path / "tools" / "a.py").write_text("x = 1\n", encoding="utf-8")
    monkeypatch.setattr(C, "ROOT", tmp_path)
    monkeypatch.setattr(C, "_files", lambda: [tmp_path / "tools" / "a.py"])
    assert "plaintext_secrets" in C.scan()


# ---------------------------------------------------------------- [8] 왕복 비용 상수 불일치 (2026-09-21)
def _drift(tmp_path, monkeypatch, body, name="x.py"):
    p = tmp_path / "tools" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding="utf-8")
    monkeypatch.setattr(C, "ROOT", tmp_path)
    return C._cost_constant_drift([p])


def test_drift_catches_stale_assignment(tmp_path, monkeypatch):
    """쓰는 값이 지금 모델과 다르면 잡는다 (0.358 계열 -> 0.400)."""
    hits = _drift(tmp_path, monkeypatch, "COST_ROUND_TRIP = 0.00358\n")
    assert len(hits) == 1 and "0.358%" in hits[0]["text"]


def _current_model():
    """[2026-09-22] 모델값을 시험에 **박아 쓰면** 기준이 바뀔 때 시험이 깨진다.
    실제로 비용 기준이 0.400% -> 0.421% 로 바뀌자 이 시험 둘이 깨졌다. 지금 값을 물어본다."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
    from cost_model import model_round_trip
    return model_round_trip()["round_trip"]


def test_drift_is_silent_when_matching(tmp_path, monkeypatch):
    assert _drift(tmp_path, monkeypatch, "cost = %s\n" % _current_model()) == []


def test_drift_ignores_prose(tmp_path, monkeypatch):
    """과거 사건을 적은 주석은 잡지 않는다 — 첫 구현이 이걸 37건 냈고 전부 오탐이었다.
    오탐이 남으면 스캔 전체가 무시된다."""
    body = ("# 엔진은 왕복 1.400% 를 청구하고 있었다 (2026-07 사고)\n"
            '"""기존 프로파일 backtest 2/3/2   왕복 0.120%"""\n'
            "x = 1\n")
    assert _drift(tmp_path, monkeypatch, body) == []


def test_drift_ignores_trailing_comment_on_code(tmp_path, monkeypatch):
    assert _drift(tmp_path, monkeypatch, "cost = %s   # 예전엔 0.00358 이었다\n" % _current_model()) == []


# ---------------------------------------------------------------- 비용 상수 예외 등록 (2026-09-22)
def _exceptions(tmp_path, monkeypatch, doc):
    p = tmp_path / "cost_constant_exceptions.json"
    if doc is not None:
        p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(C, "COST_EXCEPTIONS", p)


_FULL = {"pinned": 0.00179, "why": "결론이 이 값 위에 있다", "before_reuse": "다시 쓰기 전 갱신"}


def test_pinned_exception_is_silent(tmp_path, monkeypatch):
    _exceptions(tmp_path, monkeypatch, {"exceptions": {"tools/x.py:1": _FULL}})
    assert _drift(tmp_path, monkeypatch, "cost = 0.00179\n") == []


def test_value_change_breaks_the_exception(tmp_path, monkeypatch):
    """등록값과 달라지면 **다시 운다** — 예외가 드리프트를 영구히 가리면 안 된다."""
    _exceptions(tmp_path, monkeypatch, {"exceptions": {"tools/x.py:1": _FULL}})
    hits = _drift(tmp_path, monkeypatch, "cost = 0.0025\n")
    assert len(hits) == 1


def test_exception_without_evidence_is_ignored(tmp_path, monkeypatch):
    """pinned·why·before_reuse 가 다 있어야 인정된다 — '그냥 빼기' 를 막는다."""
    for missing in ("pinned", "why", "before_reuse"):
        doc = {k: v for k, v in _FULL.items() if k != missing}
        _exceptions(tmp_path, monkeypatch, {"exceptions": {"tools/x.py:1": doc}})
        assert len(_drift(tmp_path, monkeypatch, "cost = 0.00179\n")) == 1, missing


def test_missing_or_broken_registry_does_not_silence(tmp_path, monkeypatch):
    _exceptions(tmp_path, monkeypatch, None)
    assert len(_drift(tmp_path, monkeypatch, "cost = 0.00179\n")) == 1
    (tmp_path / "cost_constant_exceptions.json").write_text("{깨진", encoding="utf-8")
    assert len(_drift(tmp_path, monkeypatch, "cost = 0.00179\n")) == 1


def test_shipped_exceptions_all_have_evidence():
    p = Path(r"E:\1_Data\docs\references\cost_constant_exceptions.json")
    doc = json.loads(p.read_text(encoding="utf-8-sig"))
    assert doc["exceptions"]
    for k, e in doc["exceptions"].items():
        assert e.get("pinned") is not None and e.get("why") and e.get("before_reuse"), k


# ---------------------------------------------------------------- [4] 죽은 패키지 참조 (2026-09-22)
def test_dead_ref_check_runs_in_script_mode(tmp_path):
    """`python tools/craft_scan.py` 로 부를 때 **검사가 실제로 돈다.**

    [2026-09-22 실측] sys.path[0] 이 스크립트 폴더(tools/)라 저장소 루트가 경로에 없었고,
    이 검사는 매번 'paper_engine import 실패 - 확인불가' 로 끝났다(rc=1).
    embed 실행 때만 통과한 것을 '0건' 으로 읽은 것이 지난 검증의 오류다."""
    import subprocess
    root = Path(__file__).resolve().parents[1]
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import sys\n"
        "sys.path.insert(0, r'%s')\n" % (root / "tools") +
        "import craft_scan as C\n"
        "hits = C._dead_pkg_refs([])\n"
        "print('CONFIRMED' if not hits else hits[0]['text'])\n", encoding="utf-8")
    out = subprocess.run([sys.executable, str(probe)], cwd=str(tmp_path),
                         capture_output=True, text=True, encoding="utf-8")
    assert "CONFIRMED" in out.stdout, out.stdout + out.stderr


def _dead(tmp_path, monkeypatch, body):
    p = tmp_path / "x.py"
    p.write_text(body, encoding="utf-8")
    monkeypatch.setattr(C, "ROOT", tmp_path)
    return C._dead_pkg_refs([p])


def test_dead_ref_is_caught(tmp_path, monkeypatch):
    # 스캐너가 **이 파일 자신을 잡지 않도록** 죽은 이름을 리터럴로 두지 않고 조립한다
    # (자격증명 시험과 같은 이유 — 오탐이 남으면 스캔 전체가 무시된다).
    gone = "없는" + "이름"
    assert len(_dead(tmp_path, monkeypatch, "import paper_engine as pe\ny = pe.%s\n" % gone)) == 1
    assert len(_dead(tmp_path, monkeypatch, "from paper_engine import %s\n" % gone)) == 1


def test_live_ref_is_silent(tmp_path, monkeypatch):
    assert _dead(tmp_path, monkeypatch, "import paper_engine as pe\nx = pe.config\n") == []
