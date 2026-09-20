# -*- coding: utf-8 -*-
"""레짐 오버라이드가 승인된 노출 상한을 **넓히지 못하게** 고정한다.

[2026-09-13] 실측으로 나온 구멍:
  2026-09-11 (362) 에서 사용자 승인으로 발주를 켜면서 상한을 걸었다.
    max_gross_exposure_pct = 0.01 / max_daily_new_exposure_pct = 0.01
  그런데 `paper_engine.py` 가 `_deep_merge_dict(cfg, regime_override)` 로
  레짐 오버라이드를 **통짜 덮어쓴다.** RALLY 에 0.6 / 0.15 가 들어 있었다.
  -> 레짐이 RALLY 로 바뀌는 순간 상한이 **60배**(62,589,222원)가 된다.

  (362) 기록에 이 사실이 없다. 상한을 걸면서 오버라이드를 확인하지 않았다.

`regime_entry_policy.rally_*` 는 `min()` 이라 조이기만 한다 - 그쪽은 안전하다.
위험한 것은 통짜 덮어쓰기뿐이다.
"""
from __future__ import annotations

import copy

import pytest

cfgmod = pytest.importorskip("paper_engine.config")

CAP_KEYS = ("max_gross_exposure_pct", "max_daily_new_exposure_pct")


def _merge(dst, src):
    """paper_engine.py 의 _deep_merge_dict 와 같은 의미."""
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _merge(dst[k], v)
        else:
            dst[k] = v


def test_no_regime_loosens_the_approved_cap():
    """**핵심.** 어느 레짐으로 병합해도 상한이 최상위 값보다 커지면 안 된다."""
    cfg = cfgmod.load_config()
    base = {k: float(cfg.get(k)) for k in CAP_KEYS}
    for rg, ov in (cfg.get("regime_overrides") or {}).items():
        d = copy.deepcopy(cfg)
        if isinstance(ov, dict):
            _merge(d, ov)
        for k in CAP_KEYS:
            got = float(d.get(k))
            assert got <= base[k] + 1e-12, \
                "%s 에서 %s 가 %s -> %s 로 **넓어졌다**" % (rg, k, base[k], got)


def test_approved_cap_is_still_the_362_value():
    """(362) 승인값이 조용히 바뀌면 이 시험이 알린다."""
    cfg = cfgmod.load_config()
    for k in CAP_KEYS:
        assert float(cfg.get(k)) == pytest.approx(0.01), (k, cfg.get(k))


def test_merge_semantics_are_overwrite_not_min():
    """덮어쓰기라는 **성질 자체**를 고정한다. min 이었다면 이 구멍이 없었다."""
    d = {"max_gross_exposure_pct": 0.01}
    _merge(d, {"max_gross_exposure_pct": 0.6})
    assert d["max_gross_exposure_pct"] == 0.6, \
        "병합이 min 으로 바뀌었다면 이 시험을 지우고 위 시험만 남겨도 된다"
