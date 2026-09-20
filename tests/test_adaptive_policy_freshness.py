# -*- coding: utf-8 -*-
"""낡은 적응 진입 정책이 오늘 진입을 바꾸지 못하게 고정한다.

[2026-09-13] 실측:
  `2_Logs/adaptive_entry_condition_policy_design_latest.json` 은 2026-07-15 에 한 번 쓰인 뒤
  **생산자가 없다**(전수 grep 0건). 그런데 `adaptive_good_stock_entry.enabled=True` 이고
  그 안의 `current_candidate_mapping` 7종목이 최근 10일 후보에 **전부** 들어 있어서,
  40거래일 전 품질 판정이 오늘 수량 배수(`_adaptive_good_stock_qty_mult`)와
  모멘텀 차단 완화를 계속 정하고 있었다.

이름이 "current_candidate_mapping" 이다 - 며칠만 지나도 근거가 사라지는 성질이다.
방향: 이 검사는 **특례 경로를 줄인다(차단 강화)**.
"""
from __future__ import annotations

import datetime as dt
import json

import pytest

entry = pytest.importorskip("paper_engine.entry")


def _cfg(path, **extra):
    return {"adaptive_good_stock_entry": dict(
        {"enabled": True, "policy_design_path": str(path)}, **extra)}


def _write(tmp_path, generated_at, codes=("009150", "028300")):
    p = tmp_path / "policy.json"
    p.write_text(json.dumps({
        "generated_at": generated_at,
        "current_candidate_mapping": [
            {"code": c, "name": "X", "policy_route": "QUALITY_PROBE_TIER1",
             "post_split_qty_multiplier": 0.5} for c in codes],
    }, ensure_ascii=False), encoding="utf-8")
    return p


def test_fresh_policy_is_applied(tmp_path):
    """오늘 것이면 그대로 쓴다. 매번 막히면 이 기능이 죽는다."""
    p = _write(tmp_path, dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    assert len(entry._adaptive_good_stock_route_map(_cfg(p))) == 2


def test_stale_policy_is_skipped(tmp_path):
    """40거래일 낡은 매핑(실제 파일의 상태)은 쓰지 않는다."""
    p = _write(tmp_path, "2026-07-15T12:59:37")
    assert entry._adaptive_good_stock_route_map(_cfg(p)) == {}


def test_missing_generated_at_is_skipped(tmp_path):
    """시각이 없으면 나이를 알 수 없다 - 통과로 흡수하지 않는다."""
    p = tmp_path / "policy.json"
    p.write_text(json.dumps({"current_candidate_mapping": [
        {"code": "009150", "policy_route": "QUALITY_PROBE_TIER1"}]}), encoding="utf-8")
    assert entry._adaptive_good_stock_route_map(_cfg(p)) == {}


def test_limit_can_be_relaxed_and_is_declared(tmp_path):
    """0 이면 검사를 끈다. 그리고 **설정에 선언돼 있어야 한다.**"""
    p = _write(tmp_path, "2026-07-15T12:59:37")
    assert len(entry._adaptive_good_stock_route_map(
        _cfg(p, policy_max_age_trading_days=0))) == 2

    from paper_engine.config import load_config
    a = load_config().get("adaptive_good_stock_entry") or {}
    assert a.get("policy_max_age_trading_days") == 5, \
        "설정에 선언돼 있지 않다 - 코드 기본값에만 있으면 조용히 바뀐다"


def test_real_artifact_is_currently_stale():
    """실제 산출물이 지금 낡았다는 사실 자체를 고정한다.
    누가 갱신 배선을 만들면 이 시험이 FAIL 하고, 그때 이 시험을 지우면 된다."""
    from pathlib import Path
    p = Path(entry.LOG_DIR) / "adaptive_entry_condition_policy_design_latest.json"
    if not p.is_file():
        pytest.skip("산출물이 없다")
    obj = json.loads(p.read_text(encoding="utf-8-sig"))
    gen = str(obj.get("generated_at") or "")[:10].replace("-", "")
    age = entry._business_day_gap(gen, dt.datetime.now().strftime("%Y%m%d"))
    assert age is not None and age > 5, "낡지 않았다면 갱신 배선이 생긴 것이다"
