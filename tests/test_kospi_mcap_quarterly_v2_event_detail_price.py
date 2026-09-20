"""H1 수집기 2·3 — 공시 상세 수치 / 사건 뒤 가격. 네트워크·아카이브 없이 가짜 입력으로."""
from __future__ import annotations

import json
from datetime import datetime

import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import attach_event_prices as P
from paper.strategies.kospi_mcap_quarterly_v2.src import collect_event_details as D

NOW = datetime(2026, 9, 18, 19, 0, 0)


# ---------------- 수집기 2: 공시 상세

@pytest.mark.parametrize("raw,expect", [("40,000", 40000), ("183,000,000", 183000000), ("-", None), ("", None)])
def test_number_parse(raw, expect):
    assert D._num(raw) == expect


@pytest.mark.parametrize("raw,expect", [("2026년 07월 07일", "20260707"), ("20260707", "20260707"),
                                        ("2026-7-7", "20260707"), ("", None)])
def test_date_parse(raw, expect):
    assert D._ymd(raw) == expect


class FakeSession:
    def __init__(self, by_api):
        self.by_api = by_api
        self.calls = []

    def get(self, url, params, timeout):
        api = url.split("/api/")[1].split(".json")[0]
        self.calls.append(api)
        body = self.by_api.get(api, {"status": "013", "message": "없음"})

        class R:
            def json(self_inner):
                return body
        return R()


def _event(eid="R1|BUYBACK_DIRECT", rc="R1", et="BUYBACK_DIRECT"):
    return {"event_id": eid, "rcept_no": rc, "event_type": et, "stock_code": "005930", "corp_name": "테스트",
            "rcept_dt": "20260706", "direction": "BUY_PRESSURE"}


def _events_dir(tmp_path, events):
    (tmp_path / "h1_events.jsonl").write_text(
        "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n", encoding="utf-8")
    return tmp_path


def test_detail_normalizes_qty_amount_period(tmp_path):
    body = {"status": "000", "list": [{"rcept_no": "R1", "aqpln_stk_ostk": "40,000", "aqpln_prc_ostk": "183,000,000",
                                       "aqexpd_bgd": "2026년 07월 07일", "aqexpd_edd": "2026년 10월 06일",
                                       "aq_mth": "장내매수", "aq_pp": "주주가치 제고"}]}
    d = _events_dir(tmp_path, [_event()])
    r = D.run("k", d, {"R1": "0012345"}, NOW, session=FakeSession({"tsstkAqDecsn": body}))
    assert r["ok"] == 1 and r["status"] == "OK"
    row = json.loads((d / "event_details.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["planned_qty"] == 40000 and row["planned_amount"] == 183000000
    assert row["period_start"] == "20260707" and row["period_end"] == "20261006" and row["method"] == "장내매수"


def test_detail_rerun_skips_done(tmp_path):
    body = {"status": "000", "list": [{"rcept_no": "R1", "aqpln_stk_ostk": "10", "aqpln_prc_ostk": "100",
                                       "aqexpd_bgd": "2026년 07월 07일", "aqexpd_edd": "2026년 07월 08일"}]}
    d = _events_dir(tmp_path, [_event()])
    s = FakeSession({"tsstkAqDecsn": body})
    D.run("k", d, {"R1": "0012345"}, NOW, session=s)
    r2 = D.run("k", d, {"R1": "0012345"}, NOW, session=s)
    assert r2["candidates"] == 0 and len(s.calls) == 1


def test_detail_not_found_is_recorded_and_retried(tmp_path):
    d = _events_dir(tmp_path, [_event()])
    s = FakeSession({})
    r = D.run("k", d, {"R1": "0012345"}, NOW, session=s)
    assert r["not_found"] == 1
    row = json.loads((d / "event_details.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["status"] == "NOT_FOUND"
    r2 = D.run("k", d, {"R1": "0012345"}, NOW, session=s)  # 못 찾은 건은 다시 시도한다
    assert r2["candidates"] == 1


def test_detail_matches_by_receipt_number_only(tmp_path):
    body = {"status": "000", "list": [{"rcept_no": "OTHER", "aqpln_stk_ostk": "999"}]}
    d = _events_dir(tmp_path, [_event()])
    r = D.run("k", d, {"R1": "0012345"}, NOW, session=FakeSession({"tsstkAqDecsn": body}))
    assert r["ok"] == 0 and r["not_found"] == 1
    assert json.loads((d / "event_details.jsonl").read_text(encoding="utf-8").splitlines()[0])["status"] == "NOT_MATCHED"


def test_trust_uses_contract_fields(tmp_path):
    body = {"status": "000", "list": [{"rcept_no": "R2", "ctr_prc": "5,000,000,000",
                                       "ctr_pd_bgd": "2026년 07월 07일", "ctr_pd_edd": "2027년 01월 06일"}]}
    d = _events_dir(tmp_path, [_event("R2|BUYBACK_TRUST", "R2", "BUYBACK_TRUST")])
    D.run("k", d, {"R2": "0012345"}, NOW, session=FakeSession({"tsstkAqTrctrCnsDecsn": body}))
    row = json.loads((d / "event_details.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["planned_amount"] == 5000000000 and row["planned_qty"] is None and row["period_end"] == "20270106"


# ---------------- 수집기 3: 사건 뒤 가격

DAYS = [f"2026070{i}" for i in range(1, 10)] + [f"202607{i}" for i in range(10, 32)]


def _idx(days, start=100.0, step=0.0):
    return {d: {"close": start + i * step, "open": start + i * step} for i, d in enumerate(days)}


def _px(days, code="005930", start=1000.0, step=0.0):
    return {code: {d: {"close": start + i * step, "open": start + i * step} for i, d in enumerate(days)}}


def test_excess_return_is_stock_minus_index():
    days = DAYS[:15]
    idx = {d: {"close": 100.0 * (1.01 ** i), "open": 100.0 * (1.01 ** i)} for i, d in enumerate(days)}   # 지수 +1%/일
    px = {"A": {d: {"close": 1000.0 * (1.03 ** i), "open": 1000.0 * (1.03 ** i)} for i, d in enumerate(days)}}
    w = P.window_for("A", days[7], px, idx, days)
    r1 = next(r for r in w["rows"] if r["offset"] == 1)
    assert r1["excess"] == pytest.approx(0.03 - 0.01, abs=1e-9)
    # car_k 는 t0 당일(전일 종가 대비)부터 k 일까지 = k+1 일치
    assert w["car_0"] == pytest.approx(0.02, abs=1e-9)
    assert w["car_1"] == pytest.approx(0.04, abs=1e-9)


def test_car_accumulates_only_after_t0():
    days = DAYS[:15]
    idx = {d: {"close": 100.0, "open": 100.0} for d in days}
    px = {"A": {d: {"close": 1000.0 * (1.02 ** i), "open": 1000.0 * (1.02 ** i)} for i, d in enumerate(days)}}
    w = P.window_for("A", days[7], px, idx, days)
    pre = [r for r in w["rows"] if r["offset"] < 0]
    assert all(r["car"] is None for r in pre)
    assert w["car_5"] == pytest.approx(0.02 * 6, abs=1e-6)  # t0 포함 6일


def test_incomplete_when_window_not_filled_yet():
    days = DAYS[:12]
    w = P.window_for("A", days[9], _px(days, "A"), _idx(days), days)   # +20 일이 아직 없음
    assert w["status"] == "INCOMPLETE"


def test_missing_stock_price_marks_offset_not_zero():
    days = DAYS[:15]
    px = {"A": {d: {"close": 1000.0, "open": 1000.0} for d in days if d != days[9]}}
    w = P.window_for("A", days[7], px, _idx(days), days)
    assert 2 in w["missing_offsets"] and w["status"] == "INCOMPLETE"
    assert next(r for r in w["rows"] if r["offset"] == 2)["close"] is None


def test_t0_is_next_trading_day_after_receipt():
    assert P._next_trading_day(DAYS, "20260703") == "20260704"
    assert P._next_trading_day(DAYS, "20260731") is None


def test_no_pre_window_is_skipped():
    days = DAYS[:15]
    assert P.window_for("A", days[2], _px(days, "A"), _idx(days), days)["status"] == "NO_PRE_WINDOW"


# ---------------- 09-18 추가: 시가 진입 기준 / 중복 / 기업행위 의심

def test_open_entry_excludes_the_overnight_gap():
    """공시 뒤 갭은 우리가 못 먹는다 — 종가 기준과 시가 진입 기준이 갈려야 한다."""
    days = DAYS[:15]
    idx = {d: {"close": 100.0, "open": 100.0} for d in days}
    px = {"A": {}}
    for i, d in enumerate(days):
        px["A"][d] = {"open": 1000.0, "close": 1000.0}
    t0 = days[7]
    px["A"][t0] = {"open": 1100.0, "close": 1100.0}        # 갭 +10% 로 시작해 그대로 마감
    for d in days[8:]:
        px["A"][d] = {"open": 1100.0, "close": 1100.0}
    w = P.window_for("A", t0, px, idx, days)
    assert w["gap_excess"] == pytest.approx(0.10, abs=1e-9)
    assert w["car_0"] == pytest.approx(0.10, abs=1e-9)      # 종가 기준은 갭을 포함
    assert w["car_open_0"] == pytest.approx(0.0, abs=1e-9)  # 시가에 샀으면 남는 게 없다
    assert w["car_open_5"] == pytest.approx(0.0, abs=1e-9)


def test_ca_suspect_flag():
    days = DAYS[:15]
    idx = {d: {"close": 100.0, "open": 100.0} for d in days}
    px = {"A": {d: {"close": 1000.0, "open": 1000.0} for d in days}}
    px["A"][days[9]] = {"close": 400.0, "open": 400.0}      # 하루 -60% = 가격제한 밖
    w = P.window_for("A", days[7], px, idx, days)
    assert w["ca_suspect"] is True


def test_dedupe_same_code_same_t0():
    evs = [{"event_id": "a", "stock_code": "A", "rcept_dt": "20260701", "event_type": "T"},
           {"event_id": "b", "stock_code": "A", "rcept_dt": "20260701", "event_type": "T"},
           {"event_id": "c", "stock_code": "A", "rcept_dt": "20260705", "event_type": "T"}]
    out, dropped = P.dedupe(evs, lambda ymd: ymd)
    assert dropped == 1 and [e["event_id"] for e in out] == ["a", "c"]


def test_index_loader_reads_close_and_open(tmp_path):
    f = tmp_path / "idx.csv"
    f.write_text("20260916,2001,KOSPI200,1060.05,1039.81,1060.05,1037.90,86431,ts" + chr(10), encoding="utf-8")
    idx = P.load_index(f)
    assert idx["20260916"]["close"] == 1060.05 and idx["20260916"]["open"] == 1039.81
