"""왕복 비용 단일 출처 — 모델값과 실현값을 **섞지 않는지** (2026-09-21).

배경: 아침 상태판이 `trade_costs.csv` 의 **마지막 행**(개별 체결 1건, 68건 중 **최솟값** 0.398%)을
"왕복" 이라고 띄웠다. 모든 검증이 서는 모델 상수와 지나간 체결 하나가 같은 글자를 쓰고 있었다.
게다가 '마지막 행' 은 '최근 청산' 이 아니었다 — 파일 순서였다(실제 최근은 20260917, 화면은 20260601).

그리고 같은 "왕복 비용" 이 코드마다 0.358% / 0.400% 로 갈려 있었다(09-10 갱신이 전수 반영 안 됨)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import cost_model as C  # noqa: E402


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
