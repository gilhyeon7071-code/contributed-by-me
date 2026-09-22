"""왕복 비용 단일 출처 — 모델값과 실현값을 **섞지 않는지** (2026-09-21).

배경: 아침 상태판이 `trade_costs.csv` 의 **마지막 행**(개별 체결 1건, 68건 중 **최솟값** 0.398%)을
"왕복" 이라고 띄웠다. 모든 검증이 서는 모델 상수와 지나간 체결 하나가 같은 글자를 쓰고 있었다.
게다가 '마지막 행' 은 '최근 청산' 이 아니었다 — 파일 순서였다(실제 최근은 20260917, 화면은 20260601).

그리고 같은 "왕복 비용" 이 코드마다 0.358% / 0.400% 로 갈려 있었다(09-10 갱신이 전수 반영 안 됨)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import cost_model as C  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate_measured(tmp_path, monkeypatch):
    """[2026-09-22] 시험이 **실제 운영 파일**(docs/references/measured_costs.json)을 읽어
    09-22 실측이 생기자 기존 시험 4건이 깨졌다. 기본은 '실측 없음' 으로 격리하고,
    실측을 보려는 시험만 명시적으로 넣는다."""
    monkeypatch.setattr(C, "MEASURED", tmp_path / "__no_measured__.json")


def _calc(tmp_path: Path, rows: str) -> Path:
    p = tmp_path / "trades_calc.csv"
    p.write_text("trade_id,fee_rate,slippage_rate,sell_tax_rate\n" + rows, encoding="utf-8")
    return p


# ---------------------------------------------------------------- 산식을 재현한다
def test_round_trip_is_recomputed_not_hardcoded(tmp_path):
    m = C.model_round_trip(_calc(tmp_path, "T1,0.00015,0.0007,0.002\n"))
    assert m["ok"]
    # 2*0.00015 + 2*0.0007 + 0.002 = 0.00370
    assert round(m["round_trip"], 6) == 0.0037
    assert m["formula"] == "2*fee + 2*slippage + sell_tax"


def test_current_authority_file_yields_400bp(tmp_path):
    """수수료 0 + 슬리피지 0.1% + 세금 0.2% -> 0.400% (2026-09-21 실측 구성)."""
    m = C.model_round_trip(_calc(tmp_path, "T1,0.0,0.001,0.002\n"))
    assert round(m["round_trip_pct"], 3) == 0.400


def test_zero_fee_is_warned_not_swallowed(tmp_path):
    """수수료 0 은 모의계좌라 그럴 수 있지만 **말은 해야 한다** — 실매매 전제면 과소계상이다."""
    m = C.model_round_trip(_calc(tmp_path, "T1,0.0,0.001,0.002\n"))
    assert any("fee_rate=0" in w for w in m["warnings"])
    m2 = C.model_round_trip(_calc(tmp_path, "T1,0.00015,0.001,0.002\n"))
    assert m2["warnings"] == []


def test_missing_authority_is_unknown_not_default(tmp_path):
    m = C.model_round_trip(tmp_path / "none.csv")
    assert m["ok"] is False and "없음" in m["reason"]
    assert "round_trip" not in m          # 모르면 숫자를 만들어내지 않는다


def test_missing_columns_is_unknown(tmp_path):
    p = tmp_path / "trades_calc.csv"
    p.write_text("trade_id,net_ret\nT1,0.01\n", encoding="utf-8")
    assert C.model_round_trip(p)["ok"] is False


# ---------------------------------------------------------------- 실현값은 따로
def _costs(tmp_path: Path, rows: str) -> Path:
    p = tmp_path / "trade_costs.csv"
    p.write_text("trade_id,code,exit_date,total_cost_pct\n" + rows, encoding="utf-8")
    return p


def test_latest_exit_is_by_date_not_file_order(tmp_path):
    """'마지막 행' 을 '최근 청산' 으로 읽던 결함 — 날짜로 고른다."""
    r = C.realized_costs(_costs(tmp_path, "T1,AAA,20260917,0.00548\nT2,BBB,20260601,0.00398\n"))
    assert r["last_exit_date"] == "20260917" and r["last_code"] == "AAA"


def test_realized_reports_distribution_not_one_row(tmp_path):
    """한 건만 띄우면 그게 최솟값인지 최댓값인지 알 수 없다."""
    r = C.realized_costs(_costs(tmp_path, "T1,A,20260101,0.004\nT2,B,20260102,0.008\nT3,C,20260103,0.012\n"))
    assert round(r["median_pct"], 3) == 0.800 and round(r["min_pct"], 3) == 0.400
    assert round(r["max_pct"], 3) == 1.200 and r["n"] == 3


def test_summary_keeps_model_and_realized_on_separate_lines(monkeypatch, tmp_path):
    monkeypatch.setattr(C, "TRADES_CALC", _calc(tmp_path, "T1,0.0,0.001,0.002\n"))
    monkeypatch.setattr(C, "TRADE_COSTS", _costs(tmp_path, "T1,A,20260917,0.00548\n"))
    lines = C.summary_lines()
    head = [ln for ln in lines if "왕복(모델)" in ln]
    real = [ln for ln in lines if "실현" in ln]
    assert len(head) == 1 and len(real) == 1
    assert "실현" not in head[0]          # 한 줄에 뭉치지 않는다
    assert "0.400%" in head[0]


@pytest.mark.parametrize("missing", ["model", "realized"])
def test_unknown_side_says_unknown(monkeypatch, tmp_path, missing):
    monkeypatch.setattr(C, "TRADES_CALC",
                        tmp_path / "none.csv" if missing == "model" else _calc(tmp_path, "T1,0.0,0.001,0.002\n"))
    monkeypatch.setattr(C, "TRADE_COSTS",
                        tmp_path / "none2.csv" if missing == "realized" else _costs(tmp_path, "T1,A,20260917,0.005\n"))
    assert any("모름" in ln for ln in C.summary_lines())


# ---------------------------------------------------------------- 실측 수수료 반영 (2026-09-22)
def _measured(tmp_path, monkeypatch, body):
    p = tmp_path / "measured_costs.json"
    if body is not None:
        p.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(C, "MEASURED", p)
    return p


_M = {"measurements": [{"measured_at": "2026-09-22T10:06:34",
                        "buy_fee_pct": 0.00010704727921498661,
                        "sell_fee_tax_pct": 0.002105263157894737,
                        "evidence": "test_one_20260922_100634.json"}]}


def test_measured_fee_is_used(tmp_path, monkeypatch):
    """실측이 있으면 왕복 = 매수수수료 + (매도수수료+세금) + 슬리피지x2. 세율을 따로 가정하지 않는다."""
    _measured(tmp_path, monkeypatch, _M)
    m = C.model_round_trip(_calc(tmp_path, "T1,0.0,0.001,0.002\n"))
    assert m["fee_source"] == "measured"
    assert round(m["round_trip_pct"], 4) == 0.4212
    assert "sell_tax_rate_unused" in m          # 권위 파일의 세율은 쓰지 않았음을 밝힌다


def test_no_measurement_falls_back_and_warns(tmp_path, monkeypatch):
    _measured(tmp_path, monkeypatch, None)
    m = C.model_round_trip(_calc(tmp_path, "T1,0.0,0.001,0.002\n"))
    assert m["fee_source"] == "trades_calc" and round(m["round_trip_pct"], 3) == 0.400
    assert any("fee_rate=0" in w for w in m["warnings"])


def test_measurement_without_evidence_is_ignored(tmp_path, monkeypatch):
    """근거 없는 숫자는 실측이 아니다."""
    bad = {"measurements": [{"measured_at": "x", "buy_fee_pct": 0.1, "sell_fee_tax_pct": 0.1}]}
    _measured(tmp_path, monkeypatch, bad)
    assert C.model_round_trip(_calc(tmp_path, "T1,0.0,0.001,0.002\n"))["fee_source"] == "trades_calc"


def test_latest_measurement_wins(tmp_path, monkeypatch):
    doc = {"measurements": [
        {"measured_at": "2026-09-22T10:06:34", "buy_fee_pct": 0.0001, "sell_fee_tax_pct": 0.0021, "evidence": "a"},
        {"measured_at": "2026-10-01T10:00:00", "buy_fee_pct": 0.0002, "sell_fee_tax_pct": 0.0022, "evidence": "b"}]}
    _measured(tmp_path, monkeypatch, doc)
    m = C.model_round_trip(_calc(tmp_path, "T1,0.0,0.001,0.002\n"))
    assert m["measured_at"].startswith("2026-10-01") and m["buy_fee_pct"] == 0.0002


def test_broken_measurement_file_says_so(tmp_path, monkeypatch, capsys):
    """조용히 빈 값으로 떨어지지 않는다 — 09-22 에 import 누락을 except 가 삼킨 자리다."""
    p = _measured(tmp_path, monkeypatch, None)
    p.write_text("{깨진", encoding="utf-8")
    assert C.measured_fees() == {}
    assert "못 읽었다" in capsys.readouterr().out


def test_shipped_measurement_has_evidence_and_caveat():
    p = Path(r"E:\1_Data\docs\references\measured_costs.json")
    d = json.loads(p.read_text(encoding="utf-8-sig"))
    assert d["measurements"]
    for m in d["measurements"]:
        assert m.get("evidence") and m.get("how") and m.get("caveat")
