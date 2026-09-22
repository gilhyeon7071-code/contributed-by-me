"""반복 실수 스캔 [1][2][3][5][6] — **울리는지** 본다 (2026-09-22).

배경: 이 스캔은 [4][7][8] 만 시험이 있었다. 나머지 다섯은 "0건" 만 봤을 뿐
**결함을 넣어 우는지는 안 봤다.** 침묵을 통과로 읽는 것이 09-22 재검증에서
[4] 가 평소 호출로 한 번도 안 돌던 것을 놓친 이유다.
울지 않는 감지기는 없는 것과 같다 — 그래서 여기서는 각각 결함을 심는다.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import craft_scan as C  # noqa: E402

# 스캐너가 **이 파일 자신을 잡지 않도록** 결함 본문을 리터럴로 두지 않고 조립한다.
_CTRL = "x = 'a" + chr(8) + "b'\n"
_ORTRAP = "v = _to_" + "float(cfg, 1.5) or 1.5\n"
_REALPATH = 'p = "2_Logs' + "/" + 'real_state.json"\n'
_BATARROW = "echo 결과 -" + "> 파일\n"
_BATBOM = "﻿@echo off\n"

CASES = [
    ("control_chars", "c1.py", _CTRL, "x = 'ab'\n"),
    ("falsy_or_traps", "c2.py", _ORTRAP, "v = _to_float(cfg, 0) or 0\n"),
    ("tests_touching_real_artifacts", "tests/c3.py", _REALPATH, 'p = tmp_path / "x.json"\n'),
    ("bat_arrow_redirects", "c5.bat", _BATARROW, "echo 결과\n"),
    ("bat_bom", "c6.bat", _BATBOM, "@echo off\n"),
]


def _scan_one(tmp_path, monkeypatch, rel: str, body: str, key: str):
    f = tmp_path / rel
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(body, encoding="utf-8")
    monkeypatch.setattr(C, "ROOT", tmp_path)
    monkeypatch.setattr(C, "_files", lambda: [f])
    return C.scan()[key]


@pytest.mark.parametrize("key,rel,bad,_good", CASES)
def test_detector_fires_on_the_defect(tmp_path, monkeypatch, key, rel, bad, _good):
    assert _scan_one(tmp_path, monkeypatch, rel, bad, key), key


@pytest.mark.parametrize("key,rel,_bad,good", CASES)
def test_detector_is_silent_when_clean(tmp_path, monkeypatch, key, rel, _bad, good):
    """정상에서 울면 오탐이 쌓이고, 오탐이 쌓이면 스캔 전체가 무시된다."""
    assert _scan_one(tmp_path, monkeypatch, rel, good, key) == [], key


def test_every_check_key_is_reported(tmp_path, monkeypatch):
    """검사를 늘리고 배선을 빠뜨리면 감시가 없는 것이다 — 키가 전부 실리는지."""
    f = tmp_path / "a.py"
    f.write_text("x = 1\n", encoding="utf-8")
    monkeypatch.setattr(C, "ROOT", tmp_path)
    monkeypatch.setattr(C, "_files", lambda: [f])
    out = C.scan()
    for key in ("control_chars", "falsy_or_traps", "tests_touching_real_artifacts",
                "dead_package_refs", "bat_arrow_redirects", "bat_bom",
                "plaintext_secrets", "cost_constant_drift"):
        assert key in out, key
