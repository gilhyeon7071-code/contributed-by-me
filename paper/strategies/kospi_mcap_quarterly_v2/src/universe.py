"""D2 모집단 — KOSPI · 주권 · 보통주 에서 제도 차단(매매거래정지·정리매매·관리종목)을 뺀다.

순위·선정은 하지 않는다. spec/D2_universe.md 참조.
투자경고 등 다른 지정은 판단에 쓰지 않는다 — 쓰면 신호가 된다(정의 6).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

ALLOWED_FLAG_VALUES = {"O", "X"}
BLOCK_COLUMNS = ["trading_halt", "liquidation", "administrative"]


def build_universe(
    input_df: pd.DataFrame,
    *,
    thresholds: Dict[str, Any],
    previous_counts: Optional[Dict[str, int]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """반환: (모집단 표, 단계별 수 + status/reasons)."""
    reasons: List[str] = []
    previous_counts = previous_counts or {}

    kospi = input_df[input_df["market"].eq("KOSPI")]
    stock = kospi[kospi["security_group"].eq("주권")]
    common = stock[stock["share_type"].eq("보통주")]

    # 모르는 값을 정상으로 두지 않는다
    unknown = common[~common[BLOCK_COLUMNS].isin(ALLOWED_FLAG_VALUES).all(axis=1)]
    if len(unknown):
        reasons.append(f"FLAG_VALUE_UNKNOWN:{len(unknown)}:{list(unknown['code'].head(5))}")

    halt = common["trading_halt"].eq("O")
    liq = common["liquidation"].eq("O")
    adm = common["administrative"].eq("O")
    universe = common[~(halt | liq | adm)].copy()
    universe["in_universe"] = True

    counts = {
        "input_total": int(len(input_df)),
        "kospi": int(len(kospi)),
        "kospi_stock": int(len(stock)),
        "kospi_common": int(len(common)),
        "excluded_trading_halt": int(halt.sum()),
        "excluded_liquidation": int(liq.sum()),
        "excluded_administrative": int(adm.sum()),
        "universe_count": int(len(universe)),
    }

    kc = counts["kospi_common"]
    lo, hi = thresholds["kospi_common_abs_min"], thresholds["kospi_common_abs_max"]
    if not (lo <= kc <= hi):
        reasons.append(f"KOSPI_COMMON_OUT_OF_RANGE:{kc} not in [{lo},{hi}]")
    prev_kc = previous_counts.get("kospi_common")
    if prev_kc:
        change = abs(kc - prev_kc) / prev_kc
        if change > thresholds["kospi_common_rel_change_max"]:
            reasons.append(f"UNIVERSE_COUNT_JUMP:kospi_common={kc} prev={prev_kc} change={change:.3f}")
    if counts["universe_count"] < thresholds["universe_abs_min"]:
        reasons.append(f"UNIVERSE_TOO_SMALL:{counts['universe_count']}<{thresholds['universe_abs_min']}")

    counts["status"] = "STOP" if reasons else "OK"
    counts["reasons"] = reasons
    return universe, counts
