"""왕복 비용의 **단일 출처** (2026-09-21 신설).

왜: 같은 "왕복 비용" 이 코드마다 달랐다.
  - `0.358%` 를 쓰는 파일 12개 (2026-08-26 호가 기준)
  - `0.400%` 를 쓰는 파일 8개 (2026-09-10 브로커 실측으로 갱신)
  09-10 에 갱신했는데 **전수 반영이 안 돼** 두 값이 공존했다.
  비용은 모든 손익 판정이 그 위에 서는 값이라, 갈라지면 결론이 갈린다.

그리고 아침 상태판은 `paper/trade_costs.csv` 의 **마지막 한 줄**(개별 체결 1건, 그것도 68건 중 최솟값)을
"왕복 0.398%" 로 띄우고 있었다 — 모델값과 실현값이 같은 글자를 쓰고 있었다.

여기서는 **재현해서** 낸다. 상수를 적어두지 않는다 — 산식을 다시 돌린다.
"""
from __future__ import annotations

import csv
import statistics as _st
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
TRADES_CALC = ROOT / "paper" / "trades_calc.csv"          # 권위(사용자 승인)
TRADE_COSTS = ROOT / "paper" / "trade_costs.csv"          # 실현된 개별 체결 비용


def _floats(rows: List[Dict[str, str]], col: str) -> List[float]:
    out = []
    for r in rows:
        v = r.get(col)
        if v in (None, "", "nan"):
            continue
        try:
            out.append(float(v))
        except ValueError:
            continue
    return out


def model_round_trip(path: Path | None = None) -> Dict[str, Any]:
    """권위 파일에서 왕복 비용을 **다시 계산**한다.

    왕복 = 수수료x2 + 슬리피지x2 + 매도세x1.
    구성요소를 따로 낸다 — 합계만 보면 무엇이 빠졌는지 안 보인다.
    """
    p = Path(path or TRADES_CALC)
    if not p.is_file():
        return {"ok": False, "reason": f"권위 파일 없음: {p}"}
    rows = list(csv.DictReader(p.open(encoding="utf-8-sig")))
    if not rows:
        return {"ok": False, "reason": "행 없음"}
    fee = _floats(rows, "fee_rate")
    slip = _floats(rows, "slippage_rate")
    tax = _floats(rows, "sell_tax_rate")
    if not (fee and slip and tax):
        return {"ok": False, "reason": "비용 열이 없다(fee_rate·slippage_rate·sell_tax_rate)"}
    f, s, t = _st.median(fee), _st.median(slip), _st.median(tax)
    rt = 2 * f + 2 * s + t
    warn = []
    # **수수료 0 은 조용히 넘길 값이 아니다.** 모의계좌라 0 일 수 있지만,
    #   실매매 전제의 연구에 그대로 쓰면 비용을 과소계상한다. 09-22 실발주로 실측 예정.
    if f == 0.0:
        warn.append("fee_rate=0 — 수수료가 빠져 있다. 실매매 기준이면 과소계상이다")
    return {"ok": True, "source": str(p), "n": len(rows),
            "fee_rate": f, "slippage_rate": s, "sell_tax_rate": t,
            "round_trip": rt, "round_trip_pct": 100 * rt,
            "formula": "2*fee + 2*slippage + sell_tax",
            "warnings": warn}


def realized_costs(path: Path | None = None) -> Dict[str, Any]:
    """실제로 청산된 건들의 비용 분포. **모델값과 다른 것**이고 다르게 불러야 한다."""
    p = Path(path or TRADE_COSTS)
    if not p.is_file():
        return {"ok": False, "reason": f"파일 없음: {p}"}
    rows = list(csv.DictReader(p.open(encoding="utf-8-sig")))
    v = _floats(rows, "total_cost_pct")
    if not v:
        return {"ok": False, "reason": "total_cost_pct 없음"}
    # 마지막 '행' 이 최근 청산이 아니다 — 날짜로 고른다(상태판이 행 순서를 날짜로 착각했다)
    dated = [r for r in rows if r.get("exit_date")]
    last = max(dated, key=lambda r: str(r.get("exit_date"))) if dated else None
    return {"ok": True, "source": str(p), "n": len(v),
            "mean_pct": 100 * _st.mean(v), "median_pct": 100 * _st.median(v),
            "min_pct": 100 * min(v), "max_pct": 100 * max(v),
            "last_exit_date": (last or {}).get("exit_date"),
            "last_code": (last or {}).get("code"),
            "last_pct": 100 * float((last or {}).get("total_cost_pct") or 0) if last else None}


def summary_lines() -> List[str]:
    """화면용 두 줄. 모델값과 실현값을 **절대 같은 줄에 뭉치지 않는다**."""
    m, r = model_round_trip(), realized_costs()
    out: List[str] = []
    if m.get("ok"):
        out.append("비용    왕복(모델) %.3f%%  = 수수료 %.3f%%x2 + 슬리피지 %.3f%%x2 + 매도세 %.3f%%  [%s n=%d]"
                   % (m["round_trip_pct"], 100 * m["fee_rate"], 100 * m["slippage_rate"],
                      100 * m["sell_tax_rate"], Path(m["source"]).name, m["n"]))
        for w in m.get("warnings", []):
            out.append("        [주의] %s" % w)
    else:
        out.append("비용    왕복(모델) 모름 — %s" % m.get("reason"))
    if r.get("ok"):
        out.append("        실현 %d건  중앙 %.3f%%  평균 %.3f%%  (최근 청산 %s %s %.3f%%)"
                   % (r["n"], r["median_pct"], r["mean_pct"],
                      r.get("last_exit_date"), r.get("last_code"), r.get("last_pct") or 0.0))
    else:
        out.append("        실현 비용 모름 — %s" % r.get("reason"))
    return out


if __name__ == "__main__":
    for ln in summary_lines():
        print(ln)
