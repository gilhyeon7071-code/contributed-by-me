"""반복 실수 스캔 [7] 코드에 박힌 자격증명 — 잡는지 본다 (2026-09-21).

배경: 텔레그램 봇 토큰이 2026-06-27 에 `tools/test_tg.py` 에 박혔고 **석 달 뒤에야** 나왔다.
추적 중이었고 GitHub 원격이 붙어 있어 push 한 번이면 공개되는 상태였다.
그래서 여기서 볼 것은 "조용한가" 가 아니라 **"넣으면 잡는가"** 다."""
from __future__ import annotations

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
