"""노출 변경(MA200 국면 전환) 목표 — 직전 목표의 바스켓 비중에 새 노출만 곱한다. 순수 계산, KRX 파일을 읽지 않는다.

exec plan 4-2 (2026-09-19 사용자 결정). 국면 전환은 종목 교체가 아니라 규모 조정이다.
수량은 여기서 정하지 않는다 — E1(plan_orders)이 원장 NAV 와 집행 호가로 D7 과 같은 정수화를 한다.
보유 수량 x0.5 / x2.0 은 쓰지 않는다: 1~3주 종목이 0주로 빠져 되돌아오지 못한다(09-17 목표로 7종목, 855만원).
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd

# 선정일 호가·수량으로 계산된 열 — 노출이 바뀌면 틀린 값이라 버린다(E1 이 다시 계산)
STALE_COLS = ("target_value", "price", "floor_qty", "added_qty", "target_qty", "actual_value", "actual_weight",
              "value_error")


def scale_event_tag(exposure: float, effective_date: str) -> str:
    """주문 ID 에 붙는 꼬리. 분기 재구성 ID(전략|선정일)와 겹치지 않게 한다."""
    return f"SCALE|{float(exposure):.2f}|{effective_date}"


def scale_current_target(target: pd.DataFrame, summary: Dict[str, Any], *, regime: Dict[str, Any],
                         effective_date: str, weight_sum_tolerance: float = 1e-6
                         ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    reasons = []
    if summary.get("status") != "OK":
        reasons.append(f"BASE_TARGET_NOT_OK:{summary.get('stage')}")
    if regime.get("status") != "OK":
        reasons.append(f"REGIME_NOT_OK:{regime.get('reasons')}")
    exposure = regime.get("exposure")
    if not isinstance(exposure, (int, float)) or not (0.0 < float(exposure) <= 1.0):
        reasons.append(f"EXPOSURE_INVALID:{exposure}")
    if "basket_weight" not in target.columns or "code" not in target.columns:
        reasons.append("BASE_TARGET_COLUMNS_MISSING")
    else:
        w = pd.to_numeric(target["basket_weight"], errors="coerce")
        if w.isna().any() or (w < 0).any() or abs(float(w.sum()) - 1.0) > weight_sum_tolerance:
            reasons.append(f"BASE_WEIGHTS_INVALID:sum={float(w.sum()):.8f}")
        if target["code"].duplicated().any():
            reasons.append("BASE_TARGET_DUPLICATE_CODE")
    if reasons:
        return None, {"status": "STOP", "stage": "SCALE", "reasons": reasons}

    t = target.drop(columns=[c for c in STALE_COLS if c in target.columns]).copy()
    t["account_target_weight"] = t["basket_weight"].astype(float) * float(exposure)
    out = {
        "status": "OK", "stage": "SCALE", "reasons": [],
        "selection_date": str(summary["selection_date"]),   # 바구니는 그대로 — 선정일 유지
        "effective_date": effective_date,
        "event_tag": scale_event_tag(float(exposure), effective_date),
        "d3": regime,                                        # rebalance_phase·post-exec-check 가 d3.exposure 를 읽는다
        "base_exposure": (summary.get("d3") or {}).get("exposure"),
        "base_event_tag": summary.get("event_tag"),
        "names": int(len(t)),
    }
    return t, out
