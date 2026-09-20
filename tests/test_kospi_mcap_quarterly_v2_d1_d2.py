"""분기 시총(V2) D1 입력 · D2 모집단 — 정상에서 통과, 이상마다 멈추는지."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import krx_input as d1
from paper.strategies.kospi_mcap_quarterly_v2.src import universe as d2
from paper.strategies.kospi_mcap_quarterly_v2.src import run_d1_d2 as runner

TH = {
    "receipt_min_hhmm": "1600",
    "price_rows_abs_min": 1, "price_rows_abs_max": 100,
    "basic_rows_abs_min": 1, "basic_rows_abs_max": 100,
    "flags_rows_abs_min": 1, "flags_rows_abs_max": 100,
    "rows_rel_change_max": 0.08,
    "top_check_n": 3,
    "kospi_common_abs_min": 1, "kospi_common_abs_max": 100,
    "kospi_common_rel_change_max": 0.08,
    "universe_abs_min": 1,
    "index_code": "2001", "index_window": 3,
}

STOCKS = [
    # code,   name,   market,          group,           share,     close, shares, halt, liq, adm
    ("005930", "삼성", "KOSPI", "주권", "보통주", 70000, 1000, "X", "X", "X"),
    ("000660", "하닉", "KOSPI", "주권", "보통주", 200000, 300, "X", "X", "X"),
    ("0120G0", "영문", "KOSPI", "주권", "보통주", 5000, 2000, "X", "X", "X"),
    ("111110", "정지", "KOSPI", "주권", "보통주", 1000, 100, "O", "X", "X"),
    ("222220", "정리", "KOSPI", "주권", "보통주", 1000, 90, "X", "O", "X"),
    ("333330", "관리", "KOSPI", "주권", "보통주", 1000, 80, "X", "X", "O"),
    ("444440", "우선", "KOSPI", "주권", "구형우선주", 1000, 70, "X", "X", "X"),
    ("555550", "리츠", "KOSPI", "부동산투자회사", "보통주", 1000, 60, "X", "X", "X"),
    ("666660", "글로벌", "KOSDAQ GLOBAL", "주권", "보통주", 1000, 50, "X", "X", "X"),
    ("777770", "코닥", "KOSDAQ", "주권", "보통주", 1000, 40, "X", "X", "X"),
]


def frames(stocks=STOCKS):
    price = pd.DataFrame([{"종목코드": s[0], "종목명": s[1], "시장구분": s[2], "종가": str(s[5]),
                           "시가총액": str(s[5] * s[6]), "상장주식수": str(s[6])} for s in stocks])
    basic = pd.DataFrame([{"단축코드": s[0], "시장구분": s[2], "증권구분": s[3], "주식종류": s[4],
                           "상장일": "2000/01/01"} for s in stocks])
    flags = pd.DataFrame([{"종목코드": s[0], "종목명": s[1], "매매거래정지": s[7], "정리매매 종목": s[8],
                           "관리종목": s[9]} for s in stocks])
    return price, basic, flags


# ---------- D1 정상 ----------

def test_d1_normal_joins_and_keeps_alphanumeric_code():
    out, chk = d1.build_input(*frames(), thresholds=TH)
    assert chk["status"] == "OK", chk["reasons"]
    assert "0120G0" in set(out["code"])
    assert out["basic_joined"].all() and out["flags_joined"].all()
    assert set(out["mcap_check"]) == {"OK"}
    assert str(out["market_cap"].dtype) == "Int64"


def test_classify_by_columns():
    p, b, f = frames()
    assert d1.classify_krx_file(p) == "price"
    assert d1.classify_krx_file(b) == "basic"
    assert d1.classify_krx_file(f) == "flags"
    assert d1.classify_krx_file(pd.DataFrame({"x": [1]})) is None


# ---------- D1 이상마다 멈춤 ----------

def test_d1_stops_on_missing_column():
    p, b, f = frames()
    _, chk = d1.build_input(p.drop(columns=["시가총액"]), b, f, thresholds=TH)
    assert chk["status"] == "STOP" and any("INPUT_SCHEMA_CHANGED:price" in r for r in chk["reasons"])


def test_d1_stops_on_blank_code_before_normalization():
    p, b, f = frames()
    p.loc[0, "종목코드"] = ""
    _, chk = d1.build_input(p, b, f, thresholds=TH)
    assert any(r.startswith("INPUT_CODE_INVALID:price") for r in chk["reasons"])


def test_d1_stops_on_leading_zero_lost():
    p, b, f = frames()
    p.loc[0, "종목코드"] = "5930"  # 엑셀 저장 사고
    _, chk = d1.build_input(p, b, f, thresholds=TH)
    assert any(r.startswith("INPUT_CODE_INVALID:price") for r in chk["reasons"])


def test_d1_stops_when_top_stock_unjoined():
    p, b, f = frames()
    b = b[b["단축코드"] != "000660"]
    _, chk = d1.build_input(p, b, f, thresholds=TH)
    assert any("INPUT_UNJOINED_IN_TOP3" in r and "000660" in r for r in chk["reasons"])


def test_d1_unjoined_outside_top_is_recorded_not_stopped():
    p, b, f = frames()
    b = b[b["단축코드"] != "777770"]
    _, chk = d1.build_input(p, b, f, thresholds=TH)
    assert chk["status"] == "OK" and chk["unjoined_basic"] == 1


def test_d1_stops_on_zero_close_in_top():
    p, b, f = frames()
    p.loc[p["종목코드"] == "000660", "종가"] = "0"
    _, chk = d1.build_input(p, b, f, thresholds=TH)
    assert any("INPUT_PRICE_INVALID_IN_TOP3" in r for r in chk["reasons"])


def test_d1_stops_on_mcap_mismatch_in_top():
    p, b, f = frames()
    p.loc[p["종목코드"] == "005930", "시가총액"] = "1"
    _, chk = d1.build_input(p, b, f, thresholds=TH)
    assert any("INPUT_MCAP_CHECK_IN_TOP3" in r for r in chk["reasons"])


def test_d1_rowcount_out_of_range_and_jump():
    th = dict(TH, price_rows_abs_min=50)
    _, chk = d1.build_input(*frames(), thresholds=th)
    assert any(r.startswith("INPUT_ROWCOUNT_OUT_OF_RANGE:price") for r in chk["reasons"])
    _, chk2 = d1.build_input(*frames(), thresholds=TH, previous_rows={"price": 20})
    assert any(r.startswith("INPUT_ROWCOUNT_JUMP:price") for r in chk2["reasons"])
    _, chk3 = d1.build_input(*frames(), thresholds=TH, previous_rows={"price": 10})
    assert chk3["status"] == "OK"  # 변화 0


def test_receipt_check():
    assert d1.check_receipt(datetime(2026, 9, 30, 16, 5), "20260930", "1600") is None
    assert d1.check_receipt(datetime(2026, 9, 30, 15, 59), "20260930", "1600").startswith("INPUT_BEFORE_CLOSE")
    assert d1.check_receipt(datetime(2026, 9, 29, 17, 0), "20260930", "1600").startswith("INPUT_NOT_SELECTION_DATE")


# ---------- KOSPI200 ----------

def _index_csv(tmp_path: Path, dates, code="2001"):
    rows = [{"date": d, "index_code": code, "index_name": "KOSPI200", "close": "1000", "fetched_at": f"{d}T20:09"}
            for d in dates]
    p = tmp_path / "idx.csv"
    pd.DataFrame(rows).to_csv(p, index=False)
    return p


def test_index_ok(tmp_path):
    s, chk = d1.load_kospi200(_index_csv(tmp_path, ["20260926", "20260929", "20260930"]), "20260930",
                              index_code="2001", window=3)
    assert chk["status"] == "OK" and len(s) == 3


def test_index_stops_without_selection_row(tmp_path):
    _, chk = d1.load_kospi200(_index_csv(tmp_path, ["20260926", "20260929"]), "20260930", index_code="2001", window=2)
    assert any(r.startswith("INDEX_NOT_READY") for r in chk["reasons"])


def test_index_stops_short_window(tmp_path):
    _, chk = d1.load_kospi200(_index_csv(tmp_path, ["20260929", "20260930"]), "20260930", index_code="2001", window=3)
    assert any(r.startswith("INDEX_WINDOW_SHORT") for r in chk["reasons"])


# ---------- D2 ----------

def test_d2_filters():
    out, _ = d1.build_input(*frames(), thresholds=TH)
    uni, cnt = d2.build_universe(out, thresholds=TH)
    assert cnt["status"] == "OK", cnt["reasons"]
    assert set(uni["code"]) == {"005930", "000660", "0120G0"}
    assert cnt["kospi"] == 8 and cnt["kospi_stock"] == 7 and cnt["kospi_common"] == 6
    assert (cnt["excluded_trading_halt"], cnt["excluded_liquidation"], cnt["excluded_administrative"]) == (1, 1, 1)


def test_d2_stops_on_unknown_flag_value():
    p, b, f = frames()
    f.loc[f["종목코드"] == "005930", "관리종목"] = "Y"
    out, _ = d1.build_input(p, b, f, thresholds=TH)
    _, cnt = d2.build_universe(out, thresholds=TH)
    assert cnt["status"] == "STOP" and any(r.startswith("FLAG_VALUE_UNKNOWN") for r in cnt["reasons"])


def test_d2_stops_too_small_out_of_range_and_jump():
    out, _ = d1.build_input(*frames(), thresholds=TH)
    _, c1 = d2.build_universe(out, thresholds=dict(TH, universe_abs_min=4))
    assert any(r.startswith("UNIVERSE_TOO_SMALL") for r in c1["reasons"])
    _, c2 = d2.build_universe(out, thresholds=dict(TH, kospi_common_abs_min=10))
    assert any(r.startswith("KOSPI_COMMON_OUT_OF_RANGE") for r in c2["reasons"])
    _, c3 = d2.build_universe(out, thresholds=TH, previous_counts={"kospi_common": 10})
    assert any(r.startswith("UNIVERSE_COUNT_JUMP") for r in c3["reasons"])


# ---------- 실행기 ----------

def _write_cp949(df, path):
    df.to_csv(path, index=False, encoding="cp949")
    return path


def test_runner_refuses_format_test_into_production_dir(tmp_path):
    p, b, f = frames()
    files = [_write_cp949(p, tmp_path / "a.csv"), _write_cp949(b, tmp_path / "b.csv"), _write_cp949(f, tmp_path / "c.csv")]
    th = tmp_path / "th.json"
    th.write_text(json.dumps(TH), encoding="utf-8")
    with pytest.raises(SystemExit):
        runner.run("20260930", files, runner.PRODUCTION_DATA_DIR / "x", index_csv=_index_csv(tmp_path, ["20260930"]),
                   thresholds_path=th, format_test=True)


def test_runner_end_to_end_and_writes_only_out_dir(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    p, b, f = frames()
    files = [_write_cp949(f, src / "data_3.csv"), _write_cp949(p, src / "data_1.csv"), _write_cp949(b, src / "data_2.csv")]
    th = tmp_path / "th.json"
    th.write_text(json.dumps(dict(TH, index_window=1)), encoding="utf-8")
    idx = _index_csv(tmp_path, ["20260930"])
    out = tmp_path / "out"
    before = set(p_.name for p_ in tmp_path.iterdir())
    res = runner.run("20260930", files, out, index_csv=idx, thresholds_path=th, format_test=True)
    assert res["status"] == "OK" and res["stage"] == "D2"
    assert set(p_.name for p_ in tmp_path.iterdir()) - before == {"out"}
    names = {p_.name for p_ in out.iterdir()}
    assert {"input_20260930.csv", "kospi200_20260930.csv", "input_20260930_check.json", "universe_20260930.csv",
            "universe_20260930_count.json", "input_log.jsonl", "universe_log.jsonl"} <= names
    kinds = {m["kind"] for m in res["manifest"]}
    assert kinds == {"price", "basic", "flags"}


def test_runner_stops_on_receipt_date_when_not_format_test(tmp_path):
    p, b, f = frames()
    files = [_write_cp949(p, tmp_path / "a.csv"), _write_cp949(b, tmp_path / "b.csv"), _write_cp949(f, tmp_path / "c.csv")]
    th = tmp_path / "th.json"
    th.write_text(json.dumps(TH), encoding="utf-8")
    res = runner.run("19990101", files, tmp_path / "out", index_csv=_index_csv(tmp_path, ["19990101"]),
                     thresholds_path=th, format_test=False)
    assert res["status"] == "STOP" and any(r.startswith("INPUT_NOT_SELECTION_DATE") for r in res["reasons"])


def test_runner_stops_on_missing_kind(tmp_path):
    p, b, _ = frames()
    files = [_write_cp949(p, tmp_path / "a.csv"), _write_cp949(b, tmp_path / "b.csv"), _write_cp949(p, tmp_path / "c.csv")]
    th = tmp_path / "th.json"
    th.write_text(json.dumps(TH), encoding="utf-8")
    res = runner.run("20260930", files, tmp_path / "out", index_csv=_index_csv(tmp_path, ["20260930"]),
                     thresholds_path=th, format_test=True)
    assert res["status"] == "STOP"
    assert "INPUT_FILE_MISSING:flags" in res["reasons"] and "INPUT_FILE_DUPLICATE_KIND:price" in res["reasons"]
