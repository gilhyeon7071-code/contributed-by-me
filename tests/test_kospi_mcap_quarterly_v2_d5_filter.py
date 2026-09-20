"""D5 후보 분류 — 층·범위·모름 값. 순수 판정이라 네트워크·파일 없음."""
from __future__ import annotations

import json

import pytest

from paper.strategies.kospi_mcap_quarterly_v2.src import candidate_filter as F

RULES = F.load_rules()
OK_FLAGS = {"trading_halt": "X", "administrative": "X", "liquidation": "X", "delisting": "X"}


def _one(scope="EVENT_SLEEVE", **kw):
    return F.classify_one("005930", scope, kw.pop("rules", RULES), **kw)


def test_clean_stock_passes():
    r = _one(flags=OK_FLAGS, daily_value=1e9, position_krw=3e6, as_of="20260918")
    assert r["status"] == "ALLOWED" and r["reasons"] == []


@pytest.mark.parametrize("flag,code", [("trading_halt", "TRADING_HALT"), ("administrative", "ADMINISTRATIVE"),
                                       ("liquidation", "LIQUIDATION"), ("delisting", "DELISTING_DECIDED")])
def test_regulatory_flags_exclude(flag, code):
    r = _one(flags={**OK_FLAGS, flag: "O"}, daily_value=1e9, position_krw=3e6, as_of="20260918")
    assert r["status"] == "EXCLUDED" and code in r["reasons"]


def test_missing_flags_is_unknown_not_allowed():
    """모르는 값을 통과로 치지 않는다 — 세 번째 값."""
    r = _one(flags=None, daily_value=1e9, position_krw=3e6, as_of="20260918")
    assert r["status"] == "UNKNOWN" and "REGULATORY_FLAGS_MISSING" in r["unknown"]


def test_low_liquidity_and_order_size():
    r = _one(flags=OK_FLAGS, daily_value=1.5e8, position_krw=3e6, as_of="20260918")
    assert any(x.startswith("LOW_LIQUIDITY") for x in r["reasons"])
    r2 = _one(flags=OK_FLAGS, daily_value=1e8 * 2.5, position_krw=3e6, as_of="20260918")   # 2.5억, 1.2%
    assert r2["status"] == "ALLOWED"
    r3 = _one(flags=OK_FLAGS, daily_value=1e8 * 2.1, position_krw=1e7, as_of="20260918")   # 4.8% > 2%
    assert any(x.startswith("ORDER_TOO_BIG") for x in r3["reasons"])


def test_execution_tier_does_not_apply_to_core_basket():
    """기본 바구니는 유동성으로 빼지 않는다 — 상위 50 은 애초에 유동성이 크고, 빼면 지수 추종이 깨진다."""
    r = F.classify_one("005930", F.CORE, RULES, flags=OK_FLAGS, daily_value=1e7, position_krw=3e6, as_of="20260918")
    assert r["status"] == "ALLOWED"


def test_observed_tier_is_off_by_default():
    ev = {"kind": "RIGHTS_OFFERING", "date": "20260916"}
    r = _one(flags=OK_FLAGS, daily_value=1e9, position_krw=3e6, as_of="20260918", recent_events=[ev])
    assert r["status"] == "ALLOWED"                      # 켜지 않았으니 제외되지 않는다


def test_observed_tier_when_enabled_excludes_within_window():
    rules = json.loads(json.dumps(RULES))
    rules["tiers"]["T3_OBSERVED"]["enabled"] = True
    ev = [{"kind": "RIGHTS_OFFERING", "date": "20260916"}]
    r = _one(rules=rules, flags=OK_FLAGS, daily_value=1e9, position_krw=3e6, as_of="20260918", recent_events=ev)
    assert r["status"] == "EXCLUDED" and r["reasons"][0].startswith("AFTER_RIGHTS_OFFERING")
    old = [{"kind": "RIGHTS_OFFERING", "date": "20260901"}]                                  # 창 밖
    r2 = _one(rules=rules, flags=OK_FLAGS, daily_value=1e9, position_krw=3e6, as_of="20260918", recent_events=old)
    assert r2["status"] == "ALLOWED"


def test_observed_tier_even_when_enabled_does_not_touch_core_basket():
    rules = json.loads(json.dumps(RULES))
    rules["tiers"]["T3_OBSERVED"]["enabled"] = True
    ev = [{"kind": "WARNING_DESIGNATED", "date": "20260917"}]
    r = F.classify_one("000660", F.CORE, rules, flags=OK_FLAGS, daily_value=1e10, position_krw=3e6,
                       as_of="20260918", recent_events=ev)
    assert r["status"] == "ALLOWED"


def test_classify_many_groups_results():
    out = F.classify(["A", "B", "C"], F.SLEEVE, RULES, as_of="20260918",
                     flags={"A": OK_FLAGS, "B": {**OK_FLAGS, "trading_halt": "O"}},
                     daily_value={"A": 1e9, "B": 1e9}, position_krw=3e6)
    assert out["allowed"] == ["A"]
    assert [x["code"] for x in out["excluded"]] == ["B"]
    assert [x["code"] for x in out["unknown"]] == ["C"]      # 자료가 없는 종목은 모름
