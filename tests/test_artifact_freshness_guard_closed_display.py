"""종결(CLOSED)을 경보처럼 내보내지 않는지 (2026-09-22).

배경: `topn/*` 은 라운드 종결이라 갱신이 멈춘 것이 **설계된 상태**인데
화면엔 `!! ... CLOSED latest=None ref=None lag=None limit=None` 으로 나왔다.
이유 없는 경고로 보이고, 집계도 `total=1 fresh=0` 이라 '하나도 신선하지 않다' 로 읽혔다.
설계된 상태는 경보하지 않는다 — 매일 우는 감지기는 없는 것과 같다.
"""
from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import artifact_freshness_guard as G  # noqa: E402

CLOSED_ROW = {"series": "probe/*", "kind": "series", "status": "CLOSED", "role": "decision",
              "consumers": ["라운드"], "reason": "라운드 종결(2026-09-16) — 갱신 멈춤이 설계된 상태"}
STALE_ROW = {"series": "other/*", "kind": "series", "status": "STALE", "role": "decision",
             "consumers": ["라운드"], "latest_ymd": "20260801", "reference_ymd": "20260922",
             "lag_td": 35, "limit_td": 1, "reason": "늦었다"}


def _render(rows, monkeypatch, tmp_path) -> str:
    """main() 의 출력부만 태운다 — 진짜 산출물을 건드리지 않는다."""
    counts = {"total": 0, "fresh": 0, "stale": 0, "missing": 0, "unspecified": 0,
              "unspecified_and_old": 0, "no_calendar": 0, "expected_stale": 0, "retired": 0}
    out = {"generated_at": "x", "calendar": "holiday_manager", "counts": counts,
           "series_counts": {"total": len(rows), "fresh": 0, "stale": 0, "missing": 0,
                             "unspecified": 0, "no_calendar": 0,
                             "closed": len([r for r in rows if r["status"] == "CLOSED"])},
           "series_rows": rows, "series_violations": [], "expected_stale": [], "retired": [],
           "decision_path_violations": [], "display_path_violations": [],
           "unspecified": [], "unspecified_and_old": []}
    monkeypatch.setattr(G, "evaluate", lambda refs, cal: out)
    monkeypatch.setattr(G, "collect_references", lambda: {})
    monkeypatch.setattr(G, "_load_calendar", lambda: object())
    monkeypatch.setattr(G, "build_alert_text", lambda o: None)
    # main() 은 sys.argv 를 직접 읽는다 — pytest 의 인자가 새어들지 않게 갈아끼운다
    monkeypatch.setattr(sys, "argv", ["artifact_freshness_guard.py", "--out-dir", str(tmp_path)])
    buf = io.StringIO()
    with redirect_stdout(buf):
        G.main()
    return buf.getvalue()


def test_closed_is_not_marked_as_an_alarm(tmp_path, monkeypatch):
    txt = _render([CLOSED_ROW], monkeypatch, tmp_path)
    line = [l for l in txt.splitlines() if "probe/*" in l][0]
    assert "!!" not in line, line          # 설계된 상태를 경고로 내지 않는다
    assert "설계된 상태" in line            # 이유를 같이 낸다


def test_stale_series_still_alarms(tmp_path, monkeypatch):
    """종결을 조용하게 만들면서 **진짜 낡음까지 조용해지면** 감시를 죽인 것이다."""
    line = [l for l in _render([STALE_ROW], monkeypatch, tmp_path).splitlines() if "other/*" in l][0]
    assert "!!" in line and "lag=35" in line


def test_closed_is_counted_in_its_own_bucket(tmp_path, monkeypatch):
    line = [l for l in _render([CLOSED_ROW], monkeypatch, tmp_path).splitlines()
            if "사전등록 시리즈" in l][0]
    assert "closed=1" in line
