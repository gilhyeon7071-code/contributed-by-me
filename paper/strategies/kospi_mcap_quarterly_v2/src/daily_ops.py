"""일일 운용 — 첫 재구성 뒤 매일 도는 세 작업 (exec plan 4-2, 2026-09-19).

    저녁 20:20  evening    sync -> ledger(NAV·-25% 래치) -> 국면(MA200) -> 다음 거래일 할 일 판정 -> next_action.json
    아침 10:00  morning    오늘 날짜의 할 일만: 래치 -> liquidate / 노출 변경 -> SELL -> 체결 대기 -> BUY
                           할 일이 없으면 전날 남은 수량 1회 재주문(C9)
    오후 15:25  afternoon  pending 이 있을 때만: sync -> cancel -> sync -> ledger -> (노출 변경이면) post-exec-check
                           + 노출 달성 확인 -> 아침 OK·사고 0·달성이면 현재 목표로 올림
    수동       set-current 분기 재구성(10-01) post-exec-check 뒤 한 번: 현재 목표 등록

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.daily_ops evening --state-dir <상태폴더>

할 일 판정 순서: 래치 > 활성 목표 없음 > NAV 불완전 > 국면 판정 불가 > 노출 불일치(SCALE) > 없음.
판정할 수 없으면 UNDETERMINED 이고 아무것도 하지 않는다(알림만) — 모름을 '할 일 없음' 으로 누르지 않는다.
자동 발주는 config/daily_ops_v1.json 의 auto_submit 이 true 일 때만(사용자 승인 사항). false 면 계획만 남긴다.
휴장일에는 세 작업 모두 STANDBY 로 끝난다. 쓰기 지점: --state-dir 안뿐.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from paper.strategies.kospi_mcap_quarterly_v2.src import execution as E
from paper.strategies.kospi_mcap_quarterly_v2.src import kis_adapter as K
from paper.strategies.kospi_mcap_quarterly_v2.src import run_execution_day as R
from paper.strategies.kospi_mcap_quarterly_v2.src import targets as T
from paper.strategies.kospi_mcap_quarterly_v2.src.krx_input import load_kospi200
from paper.strategies.kospi_mcap_quarterly_v2.src.run_d1_d2 import STRATEGY_ROOT
from paper.strategies.kospi_mcap_quarterly_v2.src.scale_target import scale_current_target

DEFAULT_OPS_CFG = STRATEGY_ROOT / "config" / "daily_ops_v1.json"
DEFAULT_THRESHOLDS = STRATEGY_ROOT / "config" / "thresholds_v1.json"
DEFAULT_INDEX_CSV = Path(r"E:\1_Data\2_Logs\index_daily_history.csv")

CURRENT = "current_target.json"
NEXT_ACTION = "next_action.json"
PENDING = "pending_target.json"
DAILY_LOG = "daily_log.jsonl"
TARGET_EVENTS = "target_events.jsonl"  # 현재 목표가 바뀐 사건(덮어쓰는 CURRENT 의 이력). 날짜 없는 누적


def _read(path: Path) -> Optional[Dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _send_alert(text: str, level: str = "error") -> None:
    """알림 실패가 작업 결과를 바꾸면 안 된다 — 전부 삼킨다."""
    try:
        tools = str(Path(__file__).resolve().parents[4] / "tools")
        if tools not in sys.path:
            sys.path.insert(0, tools)
        from notify_channels import send_alert  # type: ignore
        res = send_alert(text, level=level, cooldown_sec=600.0)
        if not (res or {}).get("ok"):
            # [2026-09-20] 알림 실패를 조용히 넘기지 않는다. 경보가 3.5개월간 안 간 이력이 있다.
            #   배치가 stdout 을 2_Logs/v2_daily_*_last.txt 로 받으므로 여기 남기면 남는다.
            print(f"[V2][ALERT_FAILED] level={level} res={res}")
    except Exception as exc:
        print(f"[V2][ALERT_FAILED] level={level} {type(exc).__name__}: {exc}")


# ---------------------------------------------------------------- 판정 (순수)

def decide_next_action(*, ledger: Dict[str, Any], latch: Dict[str, Any], regime: Dict[str, Any],
                       current: Optional[Dict[str, Any]], for_date: str) -> Dict[str, Any]:
    base = {"for_date": for_date, "reasons": []}
    has_pos = bool(ledger.get("positions"))
    if latch.get("active"):
        if has_pos:
            return {**base, "action": "LIQUIDATE", "reasons": [f"LATCH_ACTIVE:{latch.get('triggered_as_of')}"]}
        return {**base, "action": "NONE", "reasons": ["LATCH_ACTIVE_FLAT"]}
    if not current:
        return {**base, "action": "NONE", "reasons": ["NO_ACTIVE_TARGET"]}
    if ledger.get("status") != "COMPLETE":
        return {**base, "action": "UNDETERMINED", "reasons": [f"NAV_INCOMPLETE:{ledger.get('missing_prices')}"]}
    if regime.get("status") != "OK":
        return {**base, "action": "UNDETERMINED", "reasons": [f"REGIME_NOT_OK:{regime.get('reasons')}"]}
    cur, new = float(current["exposure"]), float(regime["exposure"])
    if abs(cur - new) > 1e-9:
        return {**base, "action": "SCALE", "exposure_from": cur, "exposure_to": new, "regime": regime,
                "reasons": [f"REGIME_{regime['regime'].upper()}:{cur:.2f}->{new:.2f}"]}
    return {**base, "action": "NONE", "reasons": ["EXPOSURE_MATCHES_REGIME"]}


def sync_incidents(sync: Dict[str, Any]) -> List[Dict[str, Any]]:
    """체결 동기화 결과에서 사고를 꺼낸다. 받아놓고 버리면 OVERFILL·CANCEL_FAILED 가 나도 OK 가 된다(독립 검토 4번).
    조회에 없는 우리 주문(missing)도 올린다 — 전날 끝나지 않은 주문은 영원히 대사되지 않는다(18번)."""
    f = sync["fills"]
    out = [{**i, "source": "sync"} for i in f.get("incidents", [])]
    if f.get("missing"):
        out.append({"type": "ORDER_NOT_IN_DAY_QUERY", "detail": f["missing"], "source": "sync"})
    return out


def plan_replays(book: E.OrderBook, today: str, next_trading_day: Callable[[str], str]) -> List[Dict[str, Any]]:
    """E2 명세 '남은 수량은 다음 거래일 1회 재주문'(C9) — 전날 1차 시도가 취소·거절·미접수로 끝나 남은 것만.
    래치 청산은 제외(liquidate 가 날마다 스스로 다시 낸다). 가격은 보내기 직전 호가로 정한다(limit_price 0)."""
    out = []
    for c in E.replay_candidates(book):
        first = next(o for o in book.orders.values() if o["intent_id"] == c["intent_id"] and int(o["attempt_no"]) == 1)
        rid = str(first.get("rebalance_id") or "")
        if "|LATCH|" in rid:
            continue
        day = str(first.get("last_ts") or "")[:10].replace("-", "")
        if not day or next_trading_day(day) != today:
            continue
        out.append({"order_id": f"{c['intent_id']}|{c['next_attempt_no']}", "intent_id": c["intent_id"],
                    "attempt_no": c["next_attempt_no"], "code": c["code"], "side": c["side"], "limit_price": 0,
                    "requested_qty": c["remaining_qty"], "rebalance_id": rid, "execution_date": today})
    return sorted(out, key=lambda o: (o["side"] != "SELL", o["code"]))  # 매도 먼저


def missing_evening_runs(log_rows: List[Dict[str, Any]], today: str, next_trading_day: Callable[[str], str]
                         ) -> List[str]:
    """직전 저녁 기록 다음 거래일부터 오늘 전까지 비어 있는 거래일 = 사고 3번(무단 정지) 후보."""
    runs = sorted(r["date"] for r in log_rows if r.get("job") == "evening" and r.get("status") != "STANDBY")
    if not runs:
        return []
    gaps, d = [], next_trading_day(runs[-1])
    while d < today:
        gaps.append(d)
        d = next_trading_day(d)
    return gaps


# ---------------------------------------------------------------- 작업

def _log(state_dir: Path, row: Dict[str, Any]) -> None:
    R._append(state_dir / DAILY_LOG, row)


def _log_rows(state_dir: Path) -> List[Dict[str, Any]]:
    p = state_dir / DAILY_LOG
    return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()] if p.exists() else []


def _target_event(state_dir: Path, event: str, rec: Dict[str, Any], ts: str, **extra) -> None:
    """[2026-09-19] 현재 목표 변경을 사건으로 남긴다. CURRENT 는 덮어쓰므로 노출이 언제 1.0->0.5->1.0 으로
    바뀌었는지 흐름이 사라진다 — 국면 전환 횟수·비용(휩소)을 나중에 재려면 이 이력이 필요하다(DDD 도메인 이벤트)."""
    prev = _read(Path(state_dir) / CURRENT)
    R._append(Path(state_dir) / TARGET_EVENTS, {"ts": ts, "event": event,
                                                "exposure_from": (prev or {}).get("exposure"),
                                                "exposure_to": rec.get("exposure"),
                                                "selection_date": rec.get("selection_date"),
                                                "target_csv": rec.get("target_csv"), **extra})


def _set_current(state_dir: Path, rec: Dict[str, Any], event: str) -> None:
    _target_event(state_dir, event, rec, rec["set_at"])
    R._write(Path(state_dir) / CURRENT, rec)


def evening(client: Any, *, state_dir: Path, cfg: Dict[str, Any], thresholds: Dict[str, Any], index_csv: Path,
            clock: Callable[[], datetime] = datetime.now, is_market_open: Callable[[str], bool],
            next_trading_day: Callable[[str], str], alert: Callable[[str, str], None] = _send_alert) -> Dict[str, Any]:
    now = clock()
    today = now.strftime("%Y%m%d")
    state_dir = Path(state_dir)
    rep: Dict[str, Any] = {"job": "evening", "date": today, "run_at": now.isoformat(timespec="seconds"),
                           "status": "OK", "reasons": [], "incidents": []}
    if not is_market_open(today):
        rep.update(status="STANDBY", reasons=["MARKET_CLOSED_TODAY"])
        _log(state_dir, rep)
        return rep
    gaps = missing_evening_runs(_log_rows(state_dir), today, next_trading_day)
    if gaps:
        rep["incidents"].append({"type": "3_UNAUTHORIZED_HALT", "detail": f"EVENING_MISSING:{gaps}"})
    sync = R.sync_state(client, state_dir, clock)
    rep["sync"] = {"new_fills": len(sync["fills"]["new_fills"]), "missing": sync["fills"]["missing"]}
    rep["incidents"] += sync_incidents(sync)
    led = R.ledger_state(client, state_dir, cfg, clock, next_trading_day)
    rep["ledger"] = {k: led.get(k) for k in ("status", "nav", "nav_return", "cash_economic", "market_value",
                                              "missing_prices")}
    rep["ledger"]["positions"] = len(led.get("positions") or [])
    rep["latch"] = led["latch"]
    series, idx = load_kospi200(Path(index_csv), today, index_code=thresholds["index_code"],
                                window=int(thresholds["index_window"]))
    regime = T.compute_regime(series, today, cfg) if idx["status"] == "OK" else {"status": "STOP",
                                                                                "reasons": idx["reasons"]}
    rep["regime"] = regime
    current = _read(state_dir / CURRENT)
    rep["current_exposure"] = (current or {}).get("exposure")
    action = decide_next_action(ledger=led, latch=led["latch"], regime=regime, current=current,
                                for_date=next_trading_day(today))
    action["decided_at"] = rep["run_at"]
    R._write(state_dir / NEXT_ACTION, action)
    rep["action"] = action["action"]
    rep["action_reasons"] = action["reasons"]
    if action["action"] == "UNDETERMINED" or rep["incidents"]:
        rep["status"] = "STOP"
        rep["reasons"] = action["reasons"] + [f"{i['type']}:{i.get('detail')}" for i in rep["incidents"]]
    _log(state_dir, rep)
    if rep["status"] != "OK" or action["action"] in ("SCALE", "LIQUIDATE") or (led["latch"] or {}).get("new"):
        alert(f"[V2 저녁] {today} {action['action']} -> {action['for_date']}\n사유: {rep['reasons'] or action['reasons']}\n"
              f"NAV {rep['ledger']['nav']} 래치 {bool((led['latch'] or {}).get('active'))}",
              "error" if rep["status"] != "OK" or action["action"] == "LIQUIDATE" else "warning")
    return rep


def _wait_terminal(client: Any, state_dir: Path, order_ids: List[str], cfg: Dict[str, Any], ops: Dict[str, Any],
                   clock: Callable[[], datetime], sleep: Callable[[float], None]) -> Dict[str, Any]:
    waited = 0.0
    while True:
        R.sync_state(client, state_dir, clock)
        book = E.OrderBook(state_dir / "orders.jsonl")
        live = [o for o in order_ids if book.state(o) not in E.TERMINAL]
        if not live or waited >= float(ops["sell_wait_seconds"]):
            return {"live": live, "waited_seconds": waited}
        sleep(float(ops["sell_poll_seconds"]))
        waited += float(ops["sell_poll_seconds"])


def morning(client: Any, *, state_dir: Path, cfg: Dict[str, Any], ops: Dict[str, Any],
            clock: Callable[[], datetime] = datetime.now, sleep: Callable[[float], None],
            is_market_open: Callable[[str], bool], next_trading_day: Callable[[str], str],
            alert: Callable[[str, str], None] = _send_alert) -> Dict[str, Any]:
    now = clock()
    today = now.strftime("%Y%m%d")
    state_dir = Path(state_dir)
    no_submit = not bool(ops.get("auto_submit"))
    rep: Dict[str, Any] = {"job": "morning", "date": today, "run_at": now.isoformat(timespec="seconds"),
                           "status": "OK", "reasons": [], "no_submit": no_submit}
    if not is_market_open(today):
        rep.update(status="STANDBY", reasons=["MARKET_CLOSED_TODAY"])
        _log(state_dir, rep)
        return rep
    na = _read(state_dir / NEXT_ACTION)
    if not na or na.get("for_date") != today:
        action = "NONE"
        rep["reasons"].append(f"NO_ACTION_FOR_TODAY:{(na or {}).get('for_date')}")
        if na and na.get("action") not in ("NONE", None):
            rep["status"] = "STOP"  # 할 일이 있었는데 날짜가 지났다 = 저녁 작업이 빠졌거나 늦었다
            rep["reasons"].append(f"STALE_ACTION_NOT_EXECUTED:{na.get('action')}")  # 알림은 끝에서 한 번
    else:
        action = na["action"]
    rep["action"] = action
    if action == "LIQUIDATE":
        res = R.liquidate_after_latch(client, state_dir=state_dir, cfg=cfg, no_submit=no_submit, clock=clock)
        rep["liquidate"] = {"status": res["status"], "reasons": res["reasons"], "orders": len(res["orders"])}
        if res["status"] != "OK":
            rep.update(status="STOP", reasons=rep["reasons"] + res["reasons"])
        if not no_submit:
            R._write(state_dir / PENDING, {"kind": "LIQUIDATE", "date": today, "morning_status": rep["status"]})
    elif action == "SCALE":
        current = _read(state_dir / CURRENT)
        base_t = pd.read_csv(current["target_csv"], dtype={"code": str})
        base_s = _read(Path(current["summary_json"]))
        t, s = scale_current_target(base_t, base_s, regime=na["regime"], effective_date=today)
        if s["status"] != "OK":
            rep.update(status="STOP", reasons=rep["reasons"] + s["reasons"])
        else:
            tdir = state_dir / "targets"
            tdir.mkdir(parents=True, exist_ok=True)
            t_csv, s_json = tdir / f"target_scale_{today}.csv", tdir / f"target_scale_{today}_summary.json"
            t.to_csv(t_csv, index=False, encoding="utf-8-sig")
            R._write(s_json, s)
            sell = R.rebalance_phase(client, phase="SELL", target_csv=t_csv, summary_json=s_json, state_dir=state_dir,
                                     cfg=cfg, no_submit=no_submit, clock=clock, next_trading_day=next_trading_day)
            rep["sell"] = {"status": sell["status"], "reasons": sell["reasons"], "orders": len(sell.get("orders", []))}
            if sell["status"] != "OK":
                rep.update(status="STOP", reasons=rep["reasons"] + sell["reasons"])
            else:
                if not no_submit and sell.get("orders"):
                    rep["sell_wait"] = _wait_terminal(client, state_dir, [o["order_id"] for o in sell["orders"]], cfg,
                                                      ops, clock, sleep)
                buy = R.rebalance_phase(client, phase="BUY", target_csv=t_csv, summary_json=s_json,
                                        state_dir=state_dir, cfg=cfg, no_submit=no_submit, clock=clock,
                                        next_trading_day=next_trading_day)
                rep["buy"] = {"status": buy["status"], "reasons": buy["reasons"], "orders": len(buy.get("orders", []))}
                if buy["status"] != "OK":
                    rep.update(status="STOP", reasons=rep["reasons"] + buy["reasons"])
            # 그림자 모드면 pending 을 쓰지 않는다 — 실제로 바뀐 게 없으니 저녁이 다시 판정한다.
            # [2026-09-19] 실제 모드에선 성패와 무관하게 쓰되 morning_status 를 남긴다: 오후가 보낸 주문을
            #   정리(체결 반영·취소)는 하되, 현재 목표로 올리는 건 아침이 OK 이고 노출이 실제로 달성됐을 때만(검토 2번).
            if not no_submit:
                R._write(state_dir / PENDING, {"kind": "SCALE", "date": today, "target_csv": str(t_csv),
                                               "summary_json": str(s_json), "exposure": s["d3"]["exposure"],
                                               "morning_status": rep["status"]})
    elif action == "NONE":
        rep["replay"] = _replay(client, state_dir=state_dir, cfg=cfg, today=today, clock=clock,
                                next_trading_day=next_trading_day, no_submit=no_submit)
        if rep["replay"]["status"] != "OK":
            rep.update(status="STOP", reasons=rep["reasons"] + rep["replay"]["reasons"])
        if rep["replay"].get("submitted") and not (state_dir / PENDING).exists():
            R._write(state_dir / PENDING, {"kind": "REPLAY", "date": today, "morning_status": rep["status"]})
    if action in ("SCALE", "LIQUIDATE"):
        rep["replay"] = {"status": "OK", "reasons": [f"REPLAY_SUPERSEDED_BY_{action}"], "planned": 0}
    if no_submit and (action in ("SCALE", "LIQUIDATE") or (rep.get("replay") or {}).get("planned")):
        rep["reasons"].append("NO_SUBMIT_SHADOW")
    _log(state_dir, rep)
    if action in ("SCALE", "LIQUIDATE") or rep["status"] != "OK" or (rep.get("replay") or {}).get("planned"):
        alert(f"[V2 아침] {today} {action} status={rep['status']} 발주={'안 함(그림자)' if no_submit else '함'}\n"
              f"재주문 {(rep.get('replay') or {}).get('planned', 0)}건\n{rep['reasons']}",
              "error" if rep["status"] != "OK" else "warning")
    return rep


def _replay(client: Any, *, state_dir: Path, cfg: Dict[str, Any], today: str, clock: Callable[[], datetime],
            next_trading_day: Callable[[str], str], no_submit: bool) -> Dict[str, Any]:
    """전날 남은 수량 1회 재주문(검토 6번). 래치가 켜져 있으면 매수 재주문은 하지 않는다.
    매수는 확인된 현금(nrcvb) 안에서만 — 모자라면 매수 재주문 전부 멈춤(C9: 조용히 줄이지 않는다)."""
    book = E.OrderBook(state_dir / "orders.jsonl")
    plan = plan_replays(book, today, next_trading_day)
    out: Dict[str, Any] = {"status": "OK", "reasons": [], "planned": len(plan), "orders": plan, "submitted": 0}
    if not plan:
        return out
    if (state_dir / "latch.json").exists():
        dropped = [o["order_id"] for o in plan if o["side"] == "BUY"]
        plan = [o for o in plan if o["side"] != "BUY"]
        if dropped:
            out["reasons"].append(f"LATCH_ACTIVE_NO_BUY_REPLAY:{len(dropped)}")
    buys = [o for o in plan if o["side"] == "BUY"]
    if buys:
        quotes = {o["code"]: K.fetch_quote(client, o["code"], clock) for o in buys}
        est = sum(int(o["requested_qty"]) * int((quotes[o["code"]] or {}).get("ask1") or 0) for o in buys)
        est *= 1 + float(cfg["buy_cost_estimate_pct"])
        cash = float(K.fetch_buying_power(client, buys[0]["code"], 0)["nrcvb_buy_amt"])
        out["buy_estimate"], out["cash_available"] = est, cash
        if any(not (quotes[o["code"]] or {}).get("ask1") for o in buys) or est > cash:
            plan = [o for o in plan if o["side"] != "BUY"]
            out["status"] = "STOP"
            out["reasons"].append(f"REPLAY_BUY_STOPPED:need={est:.0f}:available={cash:.0f}")
    out["orders"] = plan
    if no_submit or not plan:
        return out
    log: List[Dict[str, Any]] = []
    sub = E.submit_orders(plan, client, book, clock(), cfg, clock=clock,
                          pre_submit=R.refresh_price_hook(client, cfg, clock, log))
    out["submit"], out["submitted"] = sub, sub.get("submitted", 0)
    if sub["status"] != "OK" or sub.get("skipped_pre_submit"):
        out["status"] = "STOP"
        out["reasons"] += sub["reasons"] + [f"SKIPPED:{x['order_id']}:{x['reason']}" for x in sub.get("skipped_pre_submit", [])]
    return out


# [2026-09-21] **한 번 막히면 하루를 버리던 것.**
#   사용자 지적: "매수면 바로바로 가능한 것인데 왜 기다려서 확인하는가 / 그렇게 간 시간이 1년이다".
#   topn 때 이미 적어둔 교훈을 새 로직이 안 따르고 있었다 —
#   *"내가 '10:00 발주' 를 제안했던 것도 틀렸다. 발주 시각은 시계가 아니라 **조건**이 정한다"* (PLANS 29274).
#
#   주문 생성이 멈추는 사유 셋 중 **둘은 시간이 지나면 풀린다**:
#     FAIL_EXECUTION_COVERAGE  호가를 못 받은 비중이 한도 초과   -> 호가는 들어온다
#     BUY_CASH_SHORT           매도 체결이 늦어 현금이 아직 없다  -> 체결되면 생긴다
#     TARGET_EXPOSURE_MISMATCH 목표 파일이 잘못됐다              -> 재시도해도 같다(영구)
#
#   그런데 10:00 에 한 번 막히면 그날은 끝이고 C9 의 "다음 거래일 1회 재주문" 으로 넘어갔다.
#   10-01 첫 재구성이 이걸로 막히면 **50종목 전량 매수가 하루 날아간다.**
#
#   C9 는 그대로 둔다 — 그건 "**체결 안 된 주문**" 이야기다.
#   여기서 새로 다루는 것은 "**주문을 한 건도 못 보낸 채** 일시적 사유로 멈춘" 경우다. 둘은 다른 상태다.
TRANSIENT_STOPS = ("FAIL_EXECUTION_COVERAGE", "BUY_CASH_SHORT", "QUOTE_", "WINDOW_CLOSED_MID_RUN")
PERMANENT_STOPS = ("TARGET_EXPOSURE_MISMATCH", "STALE_ACTION_NOT_EXECUTED",
                   "LEDGER_EXCEEDS_BROKER_HOLDING", "NOT_MOCK_ACCOUNT")


def classify_stop(reasons) -> str:
    """영구 사유가 하나라도 있으면 재시도하지 않는다 — 같은 실패를 반복하는 것은 소음이다."""
    rs = [str(x) for x in (reasons or [])]
    if any(any(k in r for k in PERMANENT_STOPS) for r in rs):
        return "PERMANENT"
    if any(any(k in r for k in TRANSIENT_STOPS) for r in rs):
        return "TRANSIENT"
    return "UNKNOWN"          # 모르는 사유는 재시도하지 않는다. 사람이 본다


def retry(client, *, state_dir: Path, cfg: Dict[str, Any], ops: Dict[str, Any],
          clock: Callable[[], datetime] = datetime.now, **kw) -> Dict[str, Any]:
    """아침 작업이 **아무 주문도 못 보낸 채** 일시적 사유로 멈췄으면 같은 날 다시 시도한다.

    안전 장치:
      - `pending_target.json` 이 있으면 **주문이 이미 나갔다는 뜻**이라 재시도하지 않는다(오후 작업 몫)
      - 영구·미상 사유는 재시도하지 않는다
      - 하루 최대 횟수와 마감 시각을 넘지 않는다
    """
    now = clock()
    today = now.strftime("%Y%m%d")
    state_dir = Path(state_dir)
    rep: Dict[str, Any] = {"job": "morning_retry", "date": today,
                           "run_at": now.isoformat(timespec="seconds"),
                           "status": "STANDBY", "reasons": []}

    rows = [r for r in _log_rows(state_dir) if r.get("date") == today]
    # [2026-09-22 실측] 판정 대상은 **아침 작업의 결과**다. 재시도 자신의 줄을 섞으면
    #   사유 문장이 "NOTHING_TO_RETRY:last=STANDBY" 로 나와 아침이 STANDBY 였던 것처럼 읽힌다
    #   (실제 아침은 STOP 이었다). 동작은 같았지만 **기록이 사실과 달랐다.**
    morning_rows = [r for r in rows if r.get("job") == "morning"]
    if not morning_rows:
        rep["reasons"] = ["NO_MORNING_RUN_TODAY"]
        _log(state_dir, rep)
        return rep
    last = morning_rows[-1]
    if last.get("status") != "STOP":
        rep["reasons"] = [f"NOTHING_TO_RETRY:last={last.get('status')}"]
        _log(state_dir, rep)
        return rep
    if (state_dir / PENDING).exists():
        rep["reasons"] = ["ORDERS_ALREADY_PLACED"]      # 오후 작업이 마무리한다
        _log(state_dir, rep)
        return rep

    kind = classify_stop(last.get("reasons"))
    rep["stop_kind"] = kind
    if kind != "TRANSIENT":
        rep["reasons"] = [f"NOT_RETRYABLE:{kind}:{last.get('reasons')}"]
        _log(state_dir, rep)
        return rep

    tried = len([r for r in rows if r.get("job") == "morning_retry" and r.get("retried")])
    max_tries = int(ops.get("retry_max", 4) or 4)
    if tried >= max_tries:
        rep["reasons"] = [f"MAX_RETRIES:{tried}>={max_tries}"]
        _log(state_dir, rep)
        return rep
    until = str(ops.get("retry_until", "14:30"))
    if now.strftime("%H:%M") > until:
        rep["reasons"] = [f"AFTER_DEADLINE:{now:%H:%M}>{until}"]
        _log(state_dir, rep)
        return rep

    rep["retried"] = True
    rep["attempt"] = tried + 1
    # 결과를 아직 모르는 시점이다. STANDBY(아무것도 안 함)로 적으면 **발사한 재시도와
    #   건너뛴 재시도가 기록에서 같은 글자**가 된다. 결과는 바로 뒤 morning 줄에 남는다.
    rep["status"] = "RETRYING"
    rep["reasons"] = [f"RETRY_AFTER_TRANSIENT_STOP:{last.get('reasons')}"]
    _log(state_dir, rep)
    res = morning(client, state_dir=state_dir, cfg=cfg, ops=ops, clock=clock, **kw)
    return {**rep, "status": res["status"], "result": {k: res.get(k) for k in ("status", "reasons", "action")}}


def afternoon(client: Any, *, state_dir: Path, cfg: Dict[str, Any], clock: Callable[[], datetime] = datetime.now,
              is_market_open: Callable[[str], bool], next_trading_day: Callable[[str], str],
              alert: Callable[[str, str], None] = _send_alert) -> Dict[str, Any]:
    now = clock()
    today = now.strftime("%Y%m%d")
    state_dir = Path(state_dir)
    rep: Dict[str, Any] = {"job": "afternoon", "date": today, "run_at": now.isoformat(timespec="seconds"),
                           "status": "OK", "reasons": [], "incidents": []}
    if not is_market_open(today):
        rep.update(status="STANDBY", reasons=["MARKET_CLOSED_TODAY"])
        _log(state_dir, rep)
        return rep
    pending = _read(state_dir / PENDING)
    if not pending:
        rep["action"] = "NONE"
        _log(state_dir, rep)
        return rep
    rep["action"] = pending["kind"]
    # [2026-09-19] 체결 반영 -> 취소 -> 다시 반영. 취소를 먼저 하면 낡은 filled_qty 로 판정해
    #   체결된 주문을 CANCELLED_UNFILLED 로 적는다(순서 역전, 독립 검토 3번).
    rep["incidents"] += sync_incidents(R.sync_state(client, state_dir, clock))
    book = E.OrderBook(state_dir / "orders.jsonl")
    rep["cancel"] = E.cancel_remaining(book, client, clock(), cfg)
    if rep["cancel"]["status"] != "OK":
        rep["incidents"].append({"type": "CANCEL_STOP", "detail": rep["cancel"]["reasons"]})
    rep["incidents"] += sync_incidents(R.sync_state(client, state_dir, clock))
    led = R.ledger_state(client, state_dir, cfg, clock, next_trading_day)
    rep["nav"] = led.get("nav")
    reasons: List[str] = [f"{i['type']}:{i.get('detail')}" for i in rep["incidents"]]
    if pending.get("morning_status", "OK") != "OK":
        reasons.append(f"MORNING_NOT_OK:{pending.get('morning_status')}")
    if pending["kind"] == "SCALE":
        chk = R.post_execution_check(client, state_dir=state_dir, summary_json=Path(pending["summary_json"]), cfg=cfg,
                                     clock=clock, next_trading_day=next_trading_day)
        rep["post_exec_check"] = {"status": chk["status"], "reasons": chk["reasons"], "max_weight": chk.get("max_weight")}
        if chk["status"] != "OK":
            reasons += chk["reasons"]
        # 노출이 실제로 달성됐나 — 상한 검사만으로는 '아무것도 안 샀다' 를 못 잡는다(검토 2번)
        target = float(pending["exposure"])
        achieved = (led["market_value"] / led["nav"]) if (led.get("status") == "COMPLETE" and led.get("nav")) else None
        rep["exposure_target"], rep["exposure_achieved"] = target, achieved
        tol = float(cfg["execution_coverage_fail_pct"])
        if achieved is None or abs(achieved - target) > tol:
            reasons.append(f"EXPOSURE_NOT_ACHIEVED:target={target:.2f}:achieved={achieved}")
        rep["promoted"] = not reasons
        if rep["promoted"]:
            _set_current(state_dir, {"target_csv": pending["target_csv"], "summary_json": pending["summary_json"],
                                     "exposure": pending["exposure"], "source": "SCALE", "set_at": rep["run_at"]},
                         "SCALE_PROMOTED")
        else:
            _target_event(state_dir, "SCALE_NOT_PROMOTED", {"exposure": pending["exposure"],
                                                           "target_csv": pending["target_csv"]}, rep["run_at"],
                          reasons=reasons)
        # 안 올리면 저녁이 다시 SCALE 을 낸다(다음 날 새 효력일·새 주문 ID)
    if reasons:
        rep.update(status="STOP", reasons=reasons)
    done = state_dir / "pending_done"
    done.mkdir(parents=True, exist_ok=True)
    (state_dir / PENDING).replace(done / f"pending_{pending.get('date')}_{now:%H%M%S}.json")
    _log(state_dir, rep)
    alert(f"[V2 오후] {today} {pending['kind']} 마무리 status={rep['status']} NAV {rep['nav']}\n{rep['reasons']}",
          "error" if rep["status"] != "OK" else "info")
    return rep


def set_current(*, state_dir: Path, target_csv: Path, summary_json: Path, clock=datetime.now) -> Dict[str, Any]:
    s = json.loads(Path(summary_json).read_text(encoding="utf-8"))
    t = pd.read_csv(target_csv, dtype={"code": str})
    reasons = []
    if s.get("status") != "OK":
        reasons.append(f"TARGET_NOT_OK:{s.get('stage')}")
    if "basket_weight" not in t.columns or abs(float(t["basket_weight"].sum()) - 1.0) > 1e-6:
        reasons.append("BASKET_WEIGHTS_INVALID")
    if reasons:
        return {"status": "STOP", "reasons": reasons}
    rec = {"target_csv": str(Path(target_csv).resolve()), "summary_json": str(Path(summary_json).resolve()),
           "exposure": float(s["d3"]["exposure"]), "selection_date": str(s["selection_date"]), "source": "QUARTERLY",
           "set_at": clock().isoformat(timespec="seconds")}
    _set_current(state_dir, rec, "QUARTERLY_SET")
    return {"status": "OK", **rec}


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("evening", "morning", "afternoon", "retry"):
        p = sub.add_parser(name)
        p.add_argument("--state-dir", required=True, type=Path)
    sc = sub.add_parser("set-current")
    sc.add_argument("--state-dir", required=True, type=Path)
    sc.add_argument("--target", required=True, type=Path)
    sc.add_argument("--summary", required=True, type=Path)
    ap.add_argument("--strategy-cfg", type=Path, default=R.DEFAULT_STRATEGY_CFG)
    ap.add_argument("--ops-cfg", type=Path, default=DEFAULT_OPS_CFG)
    ap.add_argument("--index-csv", type=Path, default=DEFAULT_INDEX_CSV)
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    if args.cmd == "set-current":
        res = set_current(state_dir=args.state_dir, target_csv=args.target, summary_json=args.summary)
        print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
        return 0 if res["status"] == "OK" else 3
    cfg = json.loads(args.strategy_cfg.read_text(encoding="utf-8"))
    ops = json.loads(args.ops_cfg.read_text(encoding="utf-8"))
    from holiday_manager import HolidayManager
    h = HolidayManager()
    ntd = lambda d: h.next_trading_day(d)  # noqa: E731
    client = K.make_client()
    if args.cmd == "evening":
        thr = json.loads(DEFAULT_THRESHOLDS.read_text(encoding="utf-8"))
        res = evening(client, state_dir=args.state_dir, cfg=cfg, thresholds=thr, index_csv=args.index_csv,
                      is_market_open=h.is_market_open, next_trading_day=ntd)
    elif args.cmd == "morning":
        import time
        res = morning(client, state_dir=args.state_dir, cfg=cfg, ops=ops, sleep=time.sleep,
                      is_market_open=h.is_market_open, next_trading_day=ntd)
    elif args.cmd == "retry":
        import time
        res = retry(client, state_dir=args.state_dir, cfg=cfg, ops=ops, sleep=time.sleep,
                    is_market_open=h.is_market_open, next_trading_day=ntd)
    else:
        res = afternoon(client, state_dir=args.state_dir, cfg=cfg, is_market_open=h.is_market_open,
                        next_trading_day=ntd)
    print(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    return 0 if res["status"] in ("OK", "STANDBY") else 3


if __name__ == "__main__":
    raise SystemExit(main())
