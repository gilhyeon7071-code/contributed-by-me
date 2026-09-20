"""가부 판정기 — '모름을 통과로 바꾸지 않는다' 가 진짜인지 본다 (2026-09-20).

이 도구가 지키려는 단 하나: 되돌릴 수 없는 결정(10-01 발주) 앞에서
확인 안 된 것이 '간다' 로 넘어가지 않게 하는 것."""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "paper" / "strategies" /
                       "kospi_mcap_quarterly_v2" / "src"))
import go_nogo as G  # noqa: E402

NOW = dt.datetime(2026, 9, 28, 9, 0)


def _write(p: Path, obj, *, age_days=0.0, now=NOW):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    ts = (now - dt.timedelta(days=age_days)).timestamp()
    os.utime(p, (ts, ts))
    return p


def _one(n, tmp_path, monkeypatch):
    """기준 하나만 돌린다."""
    monkeypatch.setattr(G, "V2", tmp_path)
    monkeypatch.setattr(G, "ROOT", tmp_path / "root")
    monkeypatch.setattr(G, "STATE", tmp_path / "data" / "state")
    monkeypatch.setattr(G, "PLAN_RUNS", tmp_path / "data" / "plan_runs")
    monkeypatch.setattr(G, "TEST_RUNS", tmp_path / "data" / "test_runs")
    fn = dict((c[0], c[2]) for c in G.CRITERIA)[n]
    r = G.Result(n, "t")
    fn(NOW, r)
    return r


# ---------------------------------------------------------------- 핵심 불변식
def test_unknown_is_never_a_go(tmp_path, monkeypatch):
    """전부 UNKNOWN 이면 판정은 '미룬다' 이고 rc 는 0 이 아니다."""
    monkeypatch.setattr(G, "evaluate", lambda now=None: [
        G.Result(i, f"기준{i}").set(G.UNKNOWN, "확인 안 함") for i in range(1, 10)])
    txt = G.render(G.evaluate())
    assert "미룬다" in txt and "간다 (GO)" not in txt
    assert G.main([]) == 1


def test_all_pass_is_the_only_go():
    rs = [G.Result(i, f"기준{i}").set(G.PASS, "ok") for i in range(1, 10)]
    assert "간다 (GO)" in G.render(rs)
    rs[4].set(G.UNKNOWN, "증거 없음")          # 하나만 모름이어도
    assert "미룬다" in G.render(rs)


def test_exception_becomes_unknown_not_pass(monkeypatch):
    def boom(now, r):
        raise RuntimeError("조회 실패")
    monkeypatch.setattr(G, "CRITERIA", [(1, "터지는 기준", boom)])
    r = G.evaluate(NOW)[0]
    assert r.status == G.UNKNOWN and "예외" in r.detail


# ---------------------------------------------------------------- 증거의 나이
def test_stale_account_report_is_unknown_not_pass(tmp_path, monkeypatch):
    """계좌 점검이 낡으면 '그때 값' 이다 — 지금 통과로 쓰지 않는다."""
    monkeypatch.setattr(G, "V2", tmp_path)
    monkeypatch.setattr(G, "ROOT", tmp_path / "root")
    _write(tmp_path / "data" / "check_account_20260901_120000.json",
           {"holdings": [], "nrcvb_buy_amt": 999_000_000}, age_days=27)
    for n in (5, 7):
        r = G.Result(n, "t")
        dict((c[0], c[2]) for c in G.CRITERIA)[n](NOW, r)
        assert r.status == G.UNKNOWN and "낡" in r.detail


def test_fresh_account_report_decides(tmp_path, monkeypatch):
    monkeypatch.setattr(G, "V2", tmp_path)
    monkeypatch.setattr(G, "ROOT", tmp_path / "root")
    _write(tmp_path / "data" / "check_account_20260928_080000.json",
           {"holdings": [], "nrcvb_buy_amt": 104_400_000}, age_days=0.2)
    r5, r7 = G.Result(5, "t"), G.Result(7, "t")
    dict((c[0], c[2]) for c in G.CRITERIA)[5](NOW, r5)
    dict((c[0], c[2]) for c in G.CRITERIA)[7](NOW, r7)
    assert r5.status == G.PASS and r7.status == G.PASS


@pytest.mark.parametrize("amt,expect", [(60_000_000, G.PASS), (59_999_999, G.FAIL)])
def test_buying_power_threshold_is_exact(tmp_path, monkeypatch, amt, expect):
    monkeypatch.setattr(G, "V2", tmp_path)
    monkeypatch.setattr(G, "ROOT", tmp_path / "root")
    _write(tmp_path / "data" / "check_account_x.json", {"holdings": [], "nrcvb_buy_amt": amt}, age_days=0.1)
    r = G.Result(7, "t")
    dict((c[0], c[2]) for c in G.CRITERIA)[7](NOW, r)
    assert r.status == expect


def test_holdings_present_is_fail(tmp_path, monkeypatch):
    monkeypatch.setattr(G, "V2", tmp_path)
    monkeypatch.setattr(G, "ROOT", tmp_path / "root")
    _write(tmp_path / "data" / "check_account_x.json",
           {"holdings": [{"code": "005930"}], "nrcvb_buy_amt": 1}, age_days=0.1)
    r = G.Result(5, "t")
    dict((c[0], c[2]) for c in G.CRITERIA)[5](NOW, r)
    assert r.status == G.FAIL and "1종목" in r.detail


# ---------------------------------------------------------------- 사람의 확인은 증거가 아니다
def test_handcalc_without_record_is_unknown(tmp_path, monkeypatch):
    run = tmp_path / "data" / "plan_runs" / "20260917_full"
    _write(run / "target_20260917_summary.json", {"status": "OK", "invest_amount": 28_232_400})
    r = _one(2, tmp_path, monkeypatch)
    assert r.status == G.UNKNOWN and "손계산 대조 기록" in r.detail


def test_handcalc_record_decides(tmp_path, monkeypatch):
    run = tmp_path / "data" / "plan_runs" / "20260917_full"
    _write(run / "target_20260917_summary.json", {"status": "OK", "invest_amount": 28_232_400})
    _write(run / "handcalc_20260917.json", {"expected": {"invest_amount": 28_232_400}})
    assert _one(2, tmp_path, monkeypatch).status == G.PASS
    _write(run / "handcalc_20260917.json", {"expected": {"invest_amount": 30_000_000}})
    r = _one(2, tmp_path, monkeypatch)
    assert r.status == G.FAIL and "invest_amount" in r.detail


# ---------------------------------------------------------------- 없는 증거
def test_missing_test_run_is_unknown_not_fail(tmp_path, monkeypatch):
    """아직 안 한 것은 '실패' 가 아니라 '모름' 이다 — 둘을 섞으면 원인을 못 가린다."""
    for n in (3, 6):
        assert _one(n, tmp_path, monkeypatch).status == G.UNKNOWN


def test_unreadable_json_is_unknown(tmp_path, monkeypatch):
    run = tmp_path / "data" / "plan_runs" / "20260917_full"
    run.mkdir(parents=True)
    (run / "input_20260917_check.json").write_text("{깨진", encoding="utf-8")
    (run / "universe_20260917_count.json").write_text("{}", encoding="utf-8")
    r = _one(1, tmp_path, monkeypatch)
    assert r.status in (G.UNKNOWN, G.FAIL) and r.status != G.PASS


# ---------------------------------------------------------------- 기준 9
def test_v41_positions_open_is_fail(tmp_path, monkeypatch):
    root = tmp_path / "root"
    (root / "2_Logs").mkdir(parents=True)
    (root / "paper").mkdir(parents=True)
    for b in ("run_intraday_paper.bat", "run_paper_daily.bat"):
        (root / b).write_text('if not defined PAPER_EXIT_ONLY set "PAPER_EXIT_ONLY=1"', encoding="utf-8")
    _write(root / "2_Logs" / "intraday_loop_status_latest.json",
           {"switches": {"exit_only_mode": True, "raw": {"PAPER_EXIT_ONLY": "1"}}}, age_days=0.1)
    _write(root / "paper" / "paper_state.json", {"open_positions": [{"code": "005930"}]})
    monkeypatch.setattr(G, "ROOT", root)
    monkeypatch.setenv("PAPER_EXIT_ONLY", "1")
    r = G.Result(9, "t")
    G.c9_v41_exit_only(NOW, r)
    assert r.status == G.FAIL and "보유 1종목" in r.detail


def test_v41_stale_status_file_is_unknown(tmp_path, monkeypatch):
    root = tmp_path / "root"
    (root / "2_Logs").mkdir(parents=True)
    (root / "paper").mkdir(parents=True)
    for b in ("run_intraday_paper.bat", "run_paper_daily.bat"):
        (root / b).write_text('set "PAPER_EXIT_ONLY=1"', encoding="utf-8")
    _write(root / "2_Logs" / "intraday_loop_status_latest.json",
           {"switches": {"exit_only_mode": True, "raw": {"PAPER_EXIT_ONLY": "1"}}}, age_days=30)
    _write(root / "paper" / "paper_state.json", {"open_positions": []})
    monkeypatch.setattr(G, "ROOT", root)
    monkeypatch.setenv("PAPER_EXIT_ONLY", "1")
    r = G.Result(9, "t")
    G.c9_v41_exit_only(NOW, r)
    assert r.status == G.UNKNOWN and "낡" in r.detail
