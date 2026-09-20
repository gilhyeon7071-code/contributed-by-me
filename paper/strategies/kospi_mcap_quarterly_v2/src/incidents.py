"""사고 4종 판정 (시스템 정의 R10, 2026-09-13).

사고 = 의도와 실제가 다른데 그 차이를 시스템이 스스로 알아채지 못한 경우.
주문 실패 자체는 사고가 아니다. 실패했는데 기록이 없거나 성공으로 적힌 것이 사고다.
  1 의도≠결과 (설명 없는 수량 차이)   2 조용한 실패 (정상 종료인데 산출물이 없거나 낡음)
  3 무단 정지 (매매해야 할 날 실행 기록 없음)   4 정의 위반 (허용 날짜 밖 주문, 래치 뒤 매수, 종목 상한 초과)

종목 상한 초과는 **집행 직후에만** 판정한다(사용자 2026-09-17 승인). 분기 사이에 가격이 올라 20% 를 넘는 것은
분기 재구성 설계상 정상이라, 매일 판정하면 매일 운다. 허용폭 1%p 는 Claude 설정.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def intent_vs_result(desired: Dict[str, int], positions: Dict[str, int], explained: Dict[str, str]) -> List[Dict[str, Any]]:
    out = []
    for code in sorted(set(desired) | set(positions)):
        want, have = int(desired.get(code, 0)), int(positions.get(code, 0))
        if want != have and code not in explained:
            out.append({"type": "1_INTENT_RESULT_MISMATCH", "code": code, "detail": {"desired": want, "actual": have}})
    return out


def silent_failures(artifacts: Iterable[Tuple[Path, datetime]]) -> List[Dict[str, Any]]:
    out = []
    for path, not_before in artifacts:
        p = Path(path)
        if not p.exists():
            out.append({"type": "2_SILENT_FAILURE", "path": str(p), "detail": "MISSING"})
        elif datetime.fromtimestamp(p.stat().st_mtime) < not_before:
            out.append({"type": "2_SILENT_FAILURE", "path": str(p), "detail": "STALE"})
    return out


def unauthorized_halts(expected_dates: Iterable[str], run_dates: Iterable[str]) -> List[Dict[str, Any]]:
    ran = set(run_dates)
    return [{"type": "3_UNAUTHORIZED_HALT", "date": d} for d in sorted(set(expected_dates)) if d not in ran]


def definition_violations(orders: Iterable[Dict[str, Any]], *, allowed_execution_dates: Iterable[str],
                          latch_active: bool, positions_value: Optional[Dict[str, float]] = None,
                          nav: Optional[float] = None, cap: float = 0.20, exposure: float = 1.0,
                          tolerance: float = 0.01) -> List[Dict[str, Any]]:
    allowed = set(allowed_execution_dates)
    out = []
    for o in orders:
        if o.get("execution_date") not in allowed:
            out.append({"type": "4_DEFINITION_VIOLATION", "order_id": o.get("order_id"), "detail": "DATE_NOT_ALLOWED"})
        if latch_active and o.get("side") == "BUY":
            out.append({"type": "4_DEFINITION_VIOLATION", "order_id": o.get("order_id"), "detail": "BUY_AFTER_LATCH"})
    if positions_value and nav:
        for code, value in positions_value.items():
            if value / nav > cap * exposure + tolerance:
                out.append({"type": "4_DEFINITION_VIOLATION", "code": code,
                            "detail": {"weight": value / nav, "limit": cap * exposure + tolerance}})
    return out
