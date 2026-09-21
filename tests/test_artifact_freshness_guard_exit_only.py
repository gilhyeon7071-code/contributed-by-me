"""신선도 가드 — v41.1 청산 전용일 때만 진입 입력의 STALE 을 '설계된 상태'로 내린다(2026-09-20).
설계된 상태는 경보하지 않고, 스위치가 풀리거나 증거가 낡으면 다시 경보해야 한다."""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import artifact_freshness_guard as G  # noqa: E402

TODAY = dt.date(2026, 9, 20)


class Cal:  # 주말만 휴장인 단순 달력
    def is_market_open(self, ymd):
        return dt.datetime.strptime(ymd, "%Y%m%d").weekday() < 5


def _evidence(tmp_path, monkeypatch, *, exit_only, raw, age_days):
    p = tmp_path / "intraday_loop_status_latest.json"
    p.write_text(json.dumps({"switches": {"exit_only_mode": exit_only, "raw": {"PAPER_EXIT_ONLY": raw}}}),
                 encoding="utf-8")
    ts = (dt.datetime(2026, 9, 20, 10) - dt.timedelta(days=age_days)).timestamp()
    os.utime(p, (ts, ts))
    monkeypatch.setattr(G, "EXIT_ONLY_EVIDENCE", p)
    return p


def test_exit_only_on_and_evidence_fresh_is_exempt(tmp_path, monkeypatch):
    _evidence(tmp_path, monkeypatch, exit_only=True, raw="1", age_days=1)
    active, why = G.exit_only_state(Cal(), TODAY)
    assert active is True and "exit_only_mode=True" in why


@pytest.mark.parametrize("exit_only,raw,age,expect", [
    (False, "0", 1, "EXIT_ONLY_OFF"),      # 스위치가 풀렸다 -> 다시 경보해야 한다
    (True, "1", 30, "EXIT_ONLY_EVIDENCE_STALE"),  # 증거가 낡았다 -> 모름을 통과로 바꾸지 않는다
])
def test_not_exempt_cases(tmp_path, monkeypatch, exit_only, raw, age, expect):
    _evidence(tmp_path, monkeypatch, exit_only=exit_only, raw=raw, age_days=age)
    active, why = G.exit_only_state(Cal(), TODAY)
    assert active is False and why.startswith(expect)


def test_missing_or_unreadable_evidence_is_not_exempt(tmp_path, monkeypatch):
    monkeypatch.setattr(G, "EXIT_ONLY_EVIDENCE", tmp_path / "none.json")
    assert G.exit_only_state(Cal(), TODAY) == (False, "EXIT_ONLY_EVIDENCE_MISSING")
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    monkeypatch.setattr(G, "EXIT_ONLY_EVIDENCE", bad)
    active, why = G.exit_only_state(Cal(), TODAY)
    assert active is False and why.startswith("EXIT_ONLY_EVIDENCE_UNREADABLE")


def test_alert_text_reports_expected_stale_without_decision_alarm():
    row = {"artifact": "adaptive_entry_condition_policy_design_latest.json", "status": "EXPECTED_STALE",
           "reason": "v41.1 청산 전용이라 ...", "role": "decision", "consumers": ["entry.py"]}
    out = {"generated_at": "2026-09-20 09:08:07", "decision_path_violations": [], "display_path_violations": [],
           "series_violations": [], "expected_stale": [row], "counts": {"unspecified": 0}, "unspecified_and_old": []}
    txt = G.build_alert_text(out)
    assert txt is None or ("설계된 상태" in txt and "결정경로" not in txt)


# ---------------- 고정 감시 목록: V2 일일 운용이 멈추면 다음 아침에 잡힌다 (2026-09-20)
def _run_guard(tmp_path, monkeypatch, *, exists=True, age_days=0, only_if_exists=False, now=None):
    """가드의 실제 평가 함수를 돌린다(참조 스캔은 빈 dict 로 두어 고정 감시만 본다)."""
    p = tmp_path / "daily_log.jsonl"
    if exists:
        p.write_text("{}\n", encoding="utf-8")
        ts = ((now or dt.datetime(2026, 10, 5, 8, 30)) - dt.timedelta(days=age_days)).timestamp()
        os.utime(p, (ts, ts))
    monkeypatch.setattr(G, "FIXED_WATCH", [{"path": p, "role": "decision", "max_age_trading_days": 1,
                                            "note": "V2 일일 운용 기록", "only_if_exists": only_if_exists}])
    monkeypatch.setattr(G, "_now", lambda: now or dt.datetime(2026, 10, 5, 8, 30))
    monkeypatch.setattr(G, "evaluate_dated_series", lambda cal: [])
    monkeypatch.setattr(G, "evaluate_rolling", lambda cal: [])
    out = G.evaluate({}, Cal())
    row = next(r for r in out["rows"] if r["artifact"] == "daily_log.jsonl")
    return out, row


def test_fixed_watch_fresh_is_quiet(tmp_path, monkeypatch):
    out, row = _run_guard(tmp_path, monkeypatch, age_days=0)
    assert row["status"] == "FRESH" and out["decision_path_violations"] == []


def test_fixed_watch_catches_stopped_daily_jobs(tmp_path, monkeypatch):
    out, row = _run_guard(tmp_path, monkeypatch, age_days=7)
    assert row["status"] == "STALE" and row["role"] == "decision"
    assert [r["artifact"] for r in out["decision_path_violations"]] == ["daily_log.jsonl"]
    assert "결정경로" in (G.build_alert_text(out) or "")


def test_fixed_watch_missing_is_loud(tmp_path, monkeypatch):
    out, row = _run_guard(tmp_path, monkeypatch, exists=False)
    assert row["status"] == "MISSING" and out["decision_path_violations"]


def test_fixed_watch_skips_not_started_yet(tmp_path, monkeypatch):
    p = tmp_path / "daily_log.jsonl"
    monkeypatch.setattr(G, "FIXED_WATCH", [{"path": p, "role": "decision", "max_age_trading_days": 1,
                                            "note": "아직 시작 전", "only_if_exists": True}])
    monkeypatch.setattr(G, "_now", lambda: dt.datetime(2026, 10, 5, 8, 30))
    monkeypatch.setattr(G, "evaluate_dated_series", lambda cal: [])
    monkeypatch.setattr(G, "evaluate_rolling", lambda cal: [])
    out = G.evaluate({}, Cal())
    assert not any(r["artifact"] == "daily_log.jsonl" for r in out["rows"])


# ---------------- 종결 등록부: 근거 있는 것만 내린다 (2026-09-21)
def _registry(tmp_path, monkeypatch, body):
    p = tmp_path / "retired_artifacts.json"
    if body is not None:
        p.write_text(body, encoding="utf-8")
    monkeypatch.setattr(G, "RETIRED_REGISTRY", p)
    return p


def test_registered_artifact_is_retired(tmp_path, monkeypatch):
    _registry(tmp_path, monkeypatch,
              json.dumps({"artifacts": {"x_latest.json": {"why": "트랙 종결", "retired_at": "2026-09-21"}}}))
    e = G.retired_entry("x_latest.json")
    assert e and e["why"] == "트랙 종결"


def test_unregistered_artifact_is_not_retired(tmp_path, monkeypatch):
    _registry(tmp_path, monkeypatch, json.dumps({"artifacts": {"x_latest.json": {"why": "종결"}}}))
    assert G.retired_entry("other_latest.json") is None


def test_missing_registry_does_not_retire_anything(tmp_path, monkeypatch):
    """등록부가 없다고 전부 종결로 바꾸면 감시가 사라진다."""
    _registry(tmp_path, monkeypatch, None)
    assert G.retired_entry("x_latest.json") is None


def test_unreadable_registry_does_not_retire(tmp_path, monkeypatch):
    _registry(tmp_path, monkeypatch, "{깨진")
    assert G.retired_entry("x_latest.json") is None


def test_entry_without_reason_is_ignored(tmp_path, monkeypatch):
    """근거 없이 이름만 올려두는 것으로는 안 내린다 — '안 쓰는 것 같다' 를 막는다."""
    _registry(tmp_path, monkeypatch, json.dumps({"artifacts": {"x_latest.json": {"retired_at": "2026-09-21"}}}))
    assert G.retired_entry("x_latest.json") is None


def test_shipped_registry_entries_all_have_evidence():
    """실제 등록부의 모든 항목에 근거와 되살리는 조건이 적혀 있어야 한다."""
    p = Path(r"E:\1_Data\docs\references\retired_artifacts.json")
    doc = json.loads(p.read_text(encoding="utf-8-sig"))
    assert doc["artifacts"], "빈 등록부"
    for name, e in doc["artifacts"].items():
        assert e.get("why") and e.get("evidence") and e.get("revive_if"), f"{name} 근거 부족"
