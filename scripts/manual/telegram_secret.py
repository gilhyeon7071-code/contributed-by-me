"""텔레그램 자격증명을 한 곳에서만 읽는다 (2026-09-21 신설).

왜: `tools/test_tg.py` 와 `tools/get_chat_id.py` 에 **봇 토큰이 평문으로 박혀 있었다.**
둘 다 git 에 추적 중이었고(da30b288), 이 저장소에는 GitHub 원격이 붙어 있다.
push 한 번이면 공개된다. 정식 보관처는 `.secrets/`(git 제외)다.
"""
from __future__ import annotations

import os
from pathlib import Path

SECRETS = Path(__file__).resolve().parents[2] / ".secrets"


def _read(name: str, env_key: str) -> str:
    v = str(os.getenv(env_key, "")).strip()
    if v:
        return v
    p = SECRETS / name
    if p.is_file():
        return p.read_text(encoding="utf-8").strip()
    raise SystemExit(
        f"[FAIL] 자격증명을 찾지 못했다: 환경변수 {env_key} 또는 {p}\n"
        f"       코드에 적어 넣지 않는다."
    )


def bot_token() -> str:
    return _read("telegram_bot_token.txt", "TELEGRAM_BOT_TOKEN")


def chat_id() -> str:
    return _read("telegram_chat_id.txt", "TELEGRAM_CHAT_ID")
