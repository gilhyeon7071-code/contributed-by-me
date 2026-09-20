"""H1 관찰 단계 — 그림자 기록. 사건이 뜨면 "이 규칙대로 샀다면" 을 **미리** 적고, 가격이 나오면 채운다.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.shadow_record --events-dir <폴더>

왜 필요한가
  지금까지의 숫자(PLANS 507~509)는 **과거를 되짚어** 잰 거예요. 되짚기는 규칙을 나중에 고르게 돼요.
  앞으로 쌓을 표본은 **사건이 난 날 규칙이 먼저 정해져 있어야** 해요. 그게 이 기록기예요.
  주문은 내지 않아요(관찰 단계). 무장은 표본이 찬 뒤 사용자 결정.

규칙  config/h1_shadow_rules_v1.json (판단 근거가 아니라 **기록 기준**이에요)
  대상    자기주식 직접취득·신탁계약, 정정 공시 제외
  진입    t0 = 접수일 다음 거래일의 **시가**
  청산    t0+5 거래일 **종가**
  크기    한 종목 300만원, 단 일 거래대금 2억 미만이거나 주문이 거래대금의 2% 를 넘으면 **건너뜀(사유 기록)**
  비용    왕복 0.35% 를 뺀 값도 같이 적음 (이 로직 자신의 실측이 나오면 그 값으로 다시 계산)

기록  shadow_trades.jsonl (날짜 없는 append-only)
  사건이 나면 status=PLANNED 로 한 줄. 진입가가 생기면 OPEN, 청산가가 생기면 CLOSED 로 **새 줄**을 덧붙여요.
  줄을 고치지 않아요 — 마지막 줄이 현재 상태고, 이력이 그대로 남아요.
  건너뛴 것도 SKIPPED 로 남겨요. **안 산 것이 기록돼야 나중에 규칙을 판정할 수 있어요.**
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

STRATEGY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RULES = STRATEGY_ROOT / "config" / "h1_shadow_rules_v1.json"


def load_prices(path: Path) -> Dict[str, Dict[str, Dict[str, float]]]:
    df = pd.read_parquet(path, columns=["code", "date", "open", "close", "volume"])
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    for code, d, o, c, v in zip(df["code"].astype(str), df["date"].astype(str), df["open"], df["close"], df["volume"]):
        if c and c > 0:
            out.setdefault(code, {})[d] = {"open": float(o) if o and o > 0 else None, "close": float(c),
                                           "value": float(c) * float(v or 0)}
    return out


def _last_state(path: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if Path(path).exists():
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            if line.strip():
                r = json.loads(line)
                out[r["shadow_id"]] = r
    return out


def plan(events: List[Dict[str, Any]], rules: Dict[str, Any], state: Dict[str, Dict[str, Any]],
         now: datetime) -> List[Dict[str, Any]]:
    """새 사건을 PLANNED 로. 가격은 아직 안 봐요 — 사건이 난 시점에 할 수 있는 것만 적어요."""
    types = set(rules["event_types"])
    new = []
    for e in events:
        if e["event_type"] not in types or not e["stock_code"]:
            continue
        if rules.get("exclude_amendments", True) and e.get("is_amendment"):
            continue
        sid = f"{e['event_id']}|{rules['version']}"
        if sid in state:
            continue
        new.append({"shadow_id": sid, "event_id": e["event_id"], "status": "PLANNED", "event_type": e["event_type"],
                    "stock_code": e["stock_code"], "corp_name": e.get("corp_name"), "rcept_dt": e["rcept_dt"],
                    "rules_version": rules["version"], "hold_days": rules["hold_days"],
                    "position_krw": rules["position_krw"], "planned_at": now.isoformat(timespec="seconds")})
    return new


def advance(state: Dict[str, Dict[str, Any]], px, days: List[str], rules: Dict[str, Any],
            now: datetime) -> List[Dict[str, Any]]:
    """가격이 생긴 만큼 PLANNED → OPEN/SKIPPED → CLOSED 로 진행. 줄을 고치지 않고 새 줄을 만든다."""
    out = []
    hold = int(rules["hold_days"])
    for sid, r in sorted(state.items()):
        if r["status"] in ("CLOSED", "SKIPPED"):
            continue
        later = [d for d in days if d > r["rcept_dt"]]
        if not later:
            continue
        t0 = later[0]
        bar = (px.get(r["stock_code"], {}) or {}).get(t0)
        if r["status"] == "PLANNED":
            if not bar or not bar.get("open"):
                continue  # 아직 가격이 없음 — 다음에 다시
            value = bar.get("value") or 0
            qty = int(rules["position_krw"] // bar["open"]) if bar["open"] > 0 else 0
            reasons = []
            if value < float(rules["min_daily_value_krw"]):
                reasons.append(f"LOW_LIQUIDITY:{value/1e8:.2f}억")
            if value > 0 and rules["position_krw"] / value > float(rules["max_share_of_daily_value"]):
                reasons.append(f"ORDER_TOO_BIG:{rules['position_krw']/value*100:.1f}%")
            if qty <= 0:
                reasons.append("QTY_ZERO")
            if reasons:
                out.append({**r, "status": "SKIPPED", "t0": t0, "skip_reasons": reasons,
                            "daily_value": value, "updated_at": now.isoformat(timespec="seconds")})
                continue
            out.append({**r, "status": "OPEN", "t0": t0, "entry_price": bar["open"], "qty": qty,
                        "notional": qty * bar["open"], "daily_value": value,
                        "updated_at": now.isoformat(timespec="seconds")})
            continue
        # OPEN → CLOSED
        i0 = days.index(t0)
        if i0 + hold >= len(days):
            continue
        d_exit = days[i0 + hold]
        ex = (px.get(r["stock_code"], {}) or {}).get(d_exit)
        if not ex or not ex.get("close"):
            continue
        gross = ex["close"] / r["entry_price"] - 1
        net = gross - float(rules["round_trip_cost_pct"])
        out.append({**r, "status": "CLOSED", "exit_date": d_exit, "exit_price": ex["close"],
                    "gross_return": gross, "net_return": net,
                    "pnl_krw": r["qty"] * (ex["close"] - r["entry_price"]) - r["notional"] * float(rules["round_trip_cost_pct"]),
                    "updated_at": now.isoformat(timespec="seconds")})
    return out


def run(events_dir: Path, rules: Dict[str, Any], now: datetime, prices_name: str = "prices_krx.parquet") -> Dict[str, Any]:
    events_dir = Path(events_dir)
    path = events_dir / "shadow_trades.jsonl"
    events = [json.loads(x) for x in (events_dir / "h1_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    state = _last_state(path)
    new_rows = plan(events, rules, state, now)
    for r in new_rows:
        state[r["shadow_id"]] = r
    px = load_prices(events_dir / prices_name) if (events_dir / prices_name).exists() else {}
    days = sorted({d for s in px.values() for d in s})
    moved = advance(state, px, days, rules, now) if days else []
    rows = new_rows + moved
    if rows:
        with path.open("a", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    final = _last_state(path)
    counts: Dict[str, int] = {}
    for r in final.values():
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    closed = [r for r in final.values() if r["status"] == "CLOSED"]
    res = {"run_at": now.isoformat(timespec="seconds"), "planned_new": len(new_rows), "advanced": len(moved),
           "counts": counts, "rules_version": rules["version"],
           "closed_net_sum_krw": round(sum(r["pnl_krw"] for r in closed), 0) if closed else 0,
           "closed_net_median_pct": round(sorted(r["net_return"] for r in closed)[len(closed) // 2] * 100, 2)
           if closed else None}
    with (events_dir / "collect_log.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"tool": "shadow_record", **res}, ensure_ascii=False) + "\n")
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    ap.add_argument("--prices", default="prices_krx.parquet")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    res = run(args.events_dir, json.loads(args.rules.read_text(encoding="utf-8")), datetime.now(), args.prices)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
