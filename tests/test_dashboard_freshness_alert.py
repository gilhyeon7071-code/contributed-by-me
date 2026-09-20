# -*- coding: utf-8 -*-
"""대시보드 빌더가 **신선도 감시 결과를 경보로 올리는지** 고정한다.

[2026-09-12] screen-claim 감사에서 나온 것:
  `artifact_freshness_guard` 는 매일 돌며 STALE/MISSING 을 찾아내는데
  **그 산출물을 읽는 코드가 자기 자신뿐이었다**(전수 grep).
  같은 시각 화면은 alerts=0 / PASS / "모든 파이프라인 및 주문 상태가 정상 범위입니다" 였다.
  탐지가 있는데 화면이 안 쓰면 탐지는 없는 것과 같다.

빌더는 다른 저장소(E:\\vibe\\buffett)에 있다. 경로로 불러온다 - 두 루트가 한 화면을 만든다.
**낡은 값·없는 값을 주입해 문장과 경보가 바뀌는 것**까지 본다.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path

import pytest

BUILDER = Path(r"E:\vibe\buffett\tools\build_dashboard_state.py")
pytestmark = pytest.mark.skipif(not BUILDER.is_file(), reason="RootB 빌더가 없다")


@pytest.fixture(scope="module")
def bds():
    spec = importlib.util.spec_from_file_location("bds_under_test", str(BUILDER))
    m = importlib.util.module_from_spec(spec)
    sys.modules["bds_under_test"] = m
    spec.loader.exec_module(m)
    return m


def _guard(tmp_path, monkeypatch, bds, payload):
    """감시 산출물을 격리 파일로 갈아끼운다. **진짜 산출물을 건드리지 않는다.**"""
    p = tmp_path / "artifact_freshness_guard_latest.json"
    if payload is not None:
        p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(bds, "FRESHNESS_GUARD_LATEST", p, raising=False)
    return p


def _today():
    return dt.datetime.now().strftime("%Y%m%d")


def test_missing_guard_is_not_silence(tmp_path, monkeypatch, bds):
    """감시가 안 돌았으면 '정상' 이 아니라 '모른다' 다. 그때가 가장 모르는 상태다."""
    _guard(tmp_path, monkeypatch, bds, None)
    alerts = []
    out = bds._freshness_section(alerts)
    assert out["present"] is False
    codes = [a["code"] for a in alerts]
    assert "FRESHNESS_GUARD_MISSING" in codes, codes


def test_decision_path_violation_is_critical(tmp_path, monkeypatch, bds):
    """결정 경로가 낡으면 낡은 값으로 매매 판단이 내려진다."""
    _guard(tmp_path, monkeypatch, bds, {
        "generated_at": "%s 22:00:00" % dt.datetime.now().strftime("%Y-%m-%d"),
        "counts": {"total": 10, "stale": 1},
        "decision_path_violations": [{"artifact": "crash_index_latest.json", "status": "STALE"}],
        "display_path_violations": [],
        "series_violations": [],
    })
    alerts = []
    bds._freshness_section(alerts)
    hit = [a for a in alerts if a["code"] == "ARTIFACT_STALE_DECISION_PATH"]
    assert hit and hit[0]["severity"] == "CRITICAL", alerts
    assert "crash_index_latest.json" in hit[0]["message"]


def test_display_path_violation_is_warn_not_critical(tmp_path, monkeypatch, bds):
    """표시 경로는 매매를 안 바꾸지만 **화면이 틀린 말을 하게 만든다** - 침묵도 안 된다."""
    _guard(tmp_path, monkeypatch, bds, {
        "generated_at": "%s 22:00:00" % dt.datetime.now().strftime("%Y-%m-%d"),
        "counts": {"total": 10},
        "decision_path_violations": [],
        "display_path_violations": [{"artifact": "code_name_cache_latest.json", "status": "STALE"}],
        "series_violations": [],
    })
    alerts = []
    bds._freshness_section(alerts)
    hit = [a for a in alerts if a["code"] == "ARTIFACT_STALE_DISPLAY_PATH"]
    assert hit and hit[0]["severity"] == "WARN", alerts


def test_clean_guard_stays_silent(tmp_path, monkeypatch, bds):
    """위반이 없으면 경보를 만들지 않는다. 매번 울리는 경보는 무시된다."""
    _guard(tmp_path, monkeypatch, bds, {
        "generated_at": "%s 22:00:00" % dt.datetime.now().strftime("%Y-%m-%d"),
        "counts": {"total": 10, "fresh": 10},
        "decision_path_violations": [],
        "display_path_violations": [],
        "series_violations": [],
    })
    alerts = []
    out = bds._freshness_section(alerts)
    assert out["present"] is True and out["guard_stale"] is False
    assert alerts == [], alerts


def test_stale_guard_itself_is_reported(tmp_path, monkeypatch, bds):
    """감시가 며칠 전 것이면 그 뒤의 산출물 나이는 모른다."""
    _guard(tmp_path, monkeypatch, bds, {
        "generated_at": "2026-08-01 22:00:00",
        "counts": {"total": 10},
        "decision_path_violations": [],
        "display_path_violations": [],
        "series_violations": [],
    })
    alerts = []
    out = bds._freshness_section(alerts)
    assert out["guard_stale"] is True
    assert "FRESHNESS_GUARD_STALE" in [a["code"] for a in alerts], alerts


def test_violations_escalate_overall_status(tmp_path, monkeypatch, bds):
    """경보가 목록에만 있고 판정에 안 닿으면 화면은 그대로 '정상' 이다."""
    assert bds._status_from_alerts([{"code": "X", "severity": "WARN"}]) == "WARN"
    assert bds._status_from_alerts([{"code": "X", "severity": "CRITICAL"}]) == "FAIL"
    assert bds._status_from_alerts([]) == "PASS"
