"""D5 후보 분류 — **무엇을 사지 않을 것인가**. 순수 판정, 주문 없음.

뼈대 여섯 줄
  입력    후보 종목 목록 + 기준일 + (제도 상태·유동성·최근 사건) 자료
  판정    층별 제외 규칙. 자료가 없으면 PASS/FAIL 이 아니라 **UNKNOWN**(세 번째 값)
  출력    통과 / 제외(사유 코드) / 판정 불가
  경계    비중·수량·주문을 만들지 않는다(그건 D7·E1)
  안전장치 제도 자료가 없으면 그 종목은 통과가 아니라 판정 불가 → 사건 슬리브에서는 사지 않는다
  산출물  호출자가 받은 결과를 그대로 기록(이 모듈은 파일을 쓰지 않는다)

왜 층과 범위를 나누나 (2026-09-18)
  근거의 등급이 달라요. T1 제도 사실은 논란이 없고, T3 관측은 시행 36번 중 하나라 아직 검증 전이에요.
  범위도 달라요 — 상위 50 중 16종목이 H1 사건, 5종목이 H2 조치를 맞았어요(하이닉스 포함).
  **기본 바구니에서 대형주를 사건 때문에 빼면 지수 추종이 깨져요.** 그래서 관측 기반 제외는 사건 슬리브에만.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

STRATEGY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RULES = STRATEGY_ROOT / "config" / "d5_filter_rules_v1.json"

CORE, SLEEVE = "CORE_BASKET", "EVENT_SLEEVE"
FLAG_FIELDS = {"TRADING_HALT": "trading_halt", "ADMINISTRATIVE": "administrative",
               "LIQUIDATION": "liquidation", "DELISTING_DECIDED": "delisting"}


def _tier_applies(tier: Dict[str, Any], scope: str) -> bool:
    return bool(tier.get("enabled")) and scope in tier.get("scope", [])


def classify_one(code: str, scope: str, rules: Dict[str, Any], *, flags: Optional[Dict[str, str]] = None,
                 daily_value: Optional[float] = None, position_krw: Optional[float] = None,
                 recent_events: Optional[List[Dict[str, Any]]] = None, as_of: str = "") -> Dict[str, Any]:
    """한 종목 판정. 모르는 값은 통과로 치지 않는다."""
    tiers = rules["tiers"]
    reasons: List[str] = []
    unknown: List[str] = []

    t1 = tiers["T1_REGULATORY"]
    if _tier_applies(t1, scope):
        if flags is None:
            unknown.append("REGULATORY_FLAGS_MISSING")
        else:
            for r in t1["rules"]:
                v = flags.get(FLAG_FIELDS[r["code"]])
                if v is None:
                    unknown.append(f"FLAG_MISSING:{r['code']}")
                elif str(v).upper() in ("O", "Y", "TRUE", "1"):
                    reasons.append(r["code"])

    t2 = tiers["T2_EXECUTION"]
    if _tier_applies(t2, scope):
        for r in t2["rules"]:
            if r["code"] == "LOW_LIQUIDITY":
                if daily_value is None:
                    unknown.append("DAILY_VALUE_MISSING")
                elif daily_value < float(r["min_daily_value_krw"]):
                    reasons.append(f"LOW_LIQUIDITY:{daily_value/1e8:.2f}억")
            elif r["code"] == "ORDER_TOO_BIG":
                if daily_value is None or position_krw is None:
                    unknown.append("ORDER_SHARE_UNKNOWN")
                elif daily_value > 0 and position_krw / daily_value > float(r["max_share_of_daily_value"]):
                    reasons.append(f"ORDER_TOO_BIG:{position_krw/daily_value*100:.1f}%")

    t3 = tiers["T3_OBSERVED"]
    if _tier_applies(t3, scope):
        for r in t3["rules"]:
            kind = r["code"].replace("AFTER_", "")
            for e in (recent_events or []):
                if e.get("kind") != kind:
                    continue
                if _within_days(e.get("date", ""), as_of, int(r["days"])):
                    reasons.append(f"{r['code']}:{e.get('date')}")
                    break

    status = "EXCLUDED" if reasons else ("UNKNOWN" if unknown else "ALLOWED")
    return {"code": code, "scope": scope, "status": status, "reasons": reasons, "unknown": unknown}


def _within_days(event_date: str, as_of: str, days: int) -> bool:
    """거래일이 아니라 달력일 기준의 대략 창. 거래일 창이 필요하면 호출자가 날짜 목록을 준다."""
    if not event_date or not as_of:
        return False
    try:
        d0 = datetime.strptime(event_date, "%Y%m%d")
        d1 = datetime.strptime(as_of, "%Y%m%d")
    except ValueError:
        return False
    return 0 <= (d1 - d0).days <= days


def classify(codes: List[str], scope: str, rules: Dict[str, Any], *, as_of: str,
             flags: Optional[Dict[str, Dict[str, str]]] = None,
             daily_value: Optional[Dict[str, float]] = None,
             position_krw: Optional[float] = None,
             recent_events: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[str, Any]:
    rows = [classify_one(c, scope, rules, flags=(flags or {}).get(c) if flags is not None else None,
                         daily_value=(daily_value or {}).get(c), position_krw=position_krw,
                         recent_events=(recent_events or {}).get(c), as_of=as_of) for c in codes]
    return {"as_of": as_of, "scope": scope, "rules_version": rules["version"],
            "allowed": [r["code"] for r in rows if r["status"] == "ALLOWED"],
            "excluded": [r for r in rows if r["status"] == "EXCLUDED"],
            "unknown": [r for r in rows if r["status"] == "UNKNOWN"], "rows": rows}


def load_rules(path: Path = DEFAULT_RULES) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))
