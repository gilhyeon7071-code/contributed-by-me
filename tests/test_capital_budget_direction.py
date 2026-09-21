"""예산 정책 스위치의 **방향**을 고정한다 (2026-09-21).

배경: 현황판 `운영(Ops)` 의 [긴급 정지] 버튼이 `capital_budget_policy.enabled = false` 를 저장하고
      *"긴급 정지 완료! 신규 주문이 차단됩니다"* 라고 알렸다. **정반대였다.**

  paper_engine/entry.py:8648   `if not budget_policy_enabled: return int(qty_in)`  <- 상한 미적용
  paper_engine/risk_orchestration.py:109  `if capital_budget_enabled:` 안에서만 gross 를 조인다

즉 `enabled=false` 는 차단이 아니라 **한도를 푸는** 쪽이다. 차단하려면
**켠 채로 예산을 0 으로** 내려야 한다(cap_qty=0 -> `[BUDGET_BASIC_BLOCK]`).

여기서 보는 것은 값이 아니라 **방향**이다 — 안전 방향이 뒤집히면 문구가 아니라 사고가 된다."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _cap_basic_qty(qty_in: int, entry_px: float, *, enabled: bool,
                   capital_total: float, basic_per_symbol_pct: float) -> int:
    """entry.py `_cap_basic_qty` 와 **같은 규칙**. 중첩 함수라 직접 못 불러서 규칙만 옮겼다.
    아래 `test_rule_matches_engine_source` 가 원본과 어긋나면 잡는다."""
    if not enabled or qty_in <= 0 or entry_px <= 0 or float(capital_total) <= 0:
        return int(qty_in)
    cap_qty = int(math.floor(float(capital_total) * float(basic_per_symbol_pct) / float(entry_px)))
    if cap_qty <= 0:
        return 0
    return int(min(qty_in, cap_qty))


# ---------------------------------------------------------------- 방향
def test_disabled_does_not_block_it_releases_the_cap():
    """**이것이 사고의 핵심이었다.** 꺼면 상한이 사라져 원래 수량이 그대로 나간다."""
    assert _cap_basic_qty(100, 1000.0, enabled=False,
                          capital_total=10_000_000, basic_per_symbol_pct=0.0) == 100


def test_enabled_with_zero_budget_blocks():
    """진짜 차단은 **켠 채 예산 0**."""
    assert _cap_basic_qty(100, 1000.0, enabled=True,
                          capital_total=10_000_000, basic_per_symbol_pct=0.0) == 0


def test_enabled_with_small_budget_caps_not_blocks():
    # 1,000만 x 5% = 50만 / 1,000원 = 500주 -> 100주는 그대로
    assert _cap_basic_qty(100, 1000.0, enabled=True,
                          capital_total=10_000_000, basic_per_symbol_pct=0.05) == 100
    # 600주 요청이면 500주로 깎인다
    assert _cap_basic_qty(600, 1000.0, enabled=True,
                          capital_total=10_000_000, basic_per_symbol_pct=0.05) == 500


@pytest.mark.parametrize("enabled,pct,want", [(False, 0.0, 50), (True, 0.0, 0), (True, 1.0, 50)])
def test_direction_table(enabled, pct, want):
    assert _cap_basic_qty(50, 2000.0, enabled=enabled,
                          capital_total=10_000_000, basic_per_symbol_pct=pct) == want


# ---------------------------------------------------------------- 원본과 어긋나면 잡는다
def test_rule_matches_engine_source():
    """규칙을 옮겨 적었으므로 **원본이 바뀌면 이 시험이 먼저 깨져야** 한다.
    옮겨 적은 시험이 조용히 낡는 것이 이 저장소가 반복해 당한 형태다."""
    src = (ROOT / "paper_engine" / "entry.py").read_text(encoding="utf-8", errors="replace")
    assert "if not budget_policy_enabled or qty_in <= 0" in src, "상한 건너뛰기 조건이 바뀌었다"
    assert "BUDGET_BASIC_BLOCK" in src, "0 수량 차단 경로가 사라졌다"
    ro = (ROOT / "paper_engine" / "risk_orchestration.py").read_text(encoding="utf-8", errors="replace")
    assert "if capital_budget_enabled:" in ro, "gross 조이기가 enabled 안에 있는 구조가 바뀌었다"
