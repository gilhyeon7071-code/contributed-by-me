"""경계 감시 — 정상에서 침묵하고 **이상에서 우는지** 본다 (2026-09-21).

배경: 09-14~09-20 에 6번 울었는데 5번은 매일 줄이 붙는 파일(index_daily_history.csv),
4번은 예약작업이 그 순간 Running 이었던 것. 둘 다 정상 동작이다.
매일 우는 감지기는 없는 것과 같아서, 그 사이 진짜 변경(tools/load_merged_panel.py 등)은 묻힌다."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import boundary_watch as B  # noqa: E402

REL = "2_Logs/index_daily_history.csv"


def snap(files=None, tasks=None):
    return {"files": files or {}, "tasks": tasks or {}}


def ao(head="aaaa", n=1000, exists=True):
    return {"exists": exists, "append_only": True, "head16": head, "bytes": n}


# ---------------------------------------------------------------- 침묵해야 하는 것
def test_append_only_growth_is_silent():
    assert B.diff(snap({REL: ao(n=1000)}), snap({REL: ao(n=1240)})) == []


def test_task_running_is_silent():
    a = {"T": {"exists": True, "state": "Ready", "start_hhmm": "20:30", "interval": ""}}
    c = {"T": {"exists": True, "state": "Ready", "start_hhmm": "20:30", "interval": ""}}
    assert B.diff(snap(tasks=a), snap(tasks=c)) == []


def test_running_is_normalized_to_ready(monkeypatch):
    """PowerShell 이 Running 을 돌려줘도 서명은 Ready 여야 한다."""
    class R:
        stdout, stderr = "Running|2026-09-21T20:30:00|\n", ""
    monkeypatch.setattr(B.subprocess, "run", lambda *a, **k: R())
    assert B._task_schedule("VIBE_X")["state"] == "Ready"


# ---------------------------------------------------------------- 울어야 하는 것
def test_append_only_shrink_alarms():
    out = B.diff(snap({REL: ao(n=1000)}), snap({REL: ao(n=400)}))
    assert len(out) == 1 and "줄었다" in out[0]


def test_append_only_head_rewrite_alarms():
    """앞부분이 바뀌면 덮어쓰기·재생성이다 — 크기가 늘어도 운다."""
    out = B.diff(snap({REL: ao(head="aaaa", n=1000)}), snap({REL: ao(head="bbbb", n=9999)}))
    assert len(out) == 1 and "앞부분" in out[0]


def test_append_only_disappearing_alarms():
    out = B.diff(snap({REL: ao()}), snap({REL: {"exists": False}}))
    assert len(out) == 1 and "사라졌다" in out[0]


def test_task_disabled_still_alarms():
    """Running 은 무시하지만 Disabled 는 진짜 경계 변화다."""
    a = {"T": {"exists": True, "state": "Ready", "start_hhmm": "20:30", "interval": ""}}
    c = {"T": {"exists": True, "state": "Disabled", "start_hhmm": "20:30", "interval": ""}}
    assert len(B.diff(snap(tasks=a), snap(tasks=c))) == 1


def test_task_time_change_alarms():
    a = {"T": {"exists": True, "state": "Ready", "start_hhmm": "20:30", "interval": ""}}
    c = {"T": {"exists": True, "state": "Ready", "start_hhmm": "16:30", "interval": ""}}
    assert len(B.diff(snap(tasks=a), snap(tasks=c))) == 1


def test_ordinary_file_change_still_alarms():
    """append-only 가 아닌 파일은 종전대로 전체 해시로 본다 — 이게 본래 목적이다."""
    a = {"tools/load_merged_panel.py": {"exists": True, "sha256_16": "aaaa", "bytes": 5158}}
    c = {"tools/load_merged_panel.py": {"exists": True, "sha256_16": "bbbb", "bytes": 5158}}
    assert len(B.diff(snap(a), snap(c))) == 1


@pytest.mark.parametrize("rel,expect", [(REL, True), ("tools/load_merged_panel.py", False)])
def test_signature_shape(tmp_path, monkeypatch, rel, expect):
    p = tmp_path / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"x" * 100)
    monkeypatch.setattr(B, "ROOT", tmp_path)
    assert bool(B._file_sig(rel).get("append_only")) is expect
