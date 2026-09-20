# -*- coding: utf-8 -*-
"""브로커 실제 잔고를 **입력으로 넣어** 내부 원장과 대조한다.

2026-09-07 신규. PLANS (217)~(219) 에서 드러난 것:

```
브로커 모의계좌   39종목 5,178주 실재 (KIS API 직접 확인)
내부 원장         39/39 이 net=0.  "팔았다"고 기록돼 있다
실제 SELL 발주    ACCEPTED 0건. 전부 DRY_RUN 또는 프리체크 차단
5개월간 안 걸린 이유  tools/reconcile_paper_state_from_fills.py 가
                  **fills 로 paper_state 를 맞춘다** = 원장으로 원장을 검사한다
                  브로커가 입력에 없으니 이 격차를 구조적으로 못 본다
```

이 도구는 그 빠진 입력을 넣는다. **고치지 않는다 — 대조하고 알린다.**
원장을 브로커에 맞추는 것(과거 손익 재계산)은 별도 정책 결정이고 사용자 승인 사항이다.

    python tools/broker_ledger_reconcile.py              # 대조 + 경보
    python tools/broker_ledger_reconcile.py --no-alert   # 조회만
    python tools/broker_ledger_reconcile.py --strict     # 격차가 있으면 rc=2
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# 배치 콘솔이 cp949 라 em-dash 같은 문자에서 print 가 죽는다.
# 도구가 일을 다 끝내고 **출력에서** 죽으면 rc!=0 이 되어 성공이 실패로 기록된다.
def _force_utf8_stdout() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


_force_utf8_stdout()

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TOOLS_DIR = ROOT / "tools"
PAPER_DIR = ROOT / "paper"

FILLS = PAPER_DIR / "fills.csv"
PAPER_STATE = PAPER_DIR / "paper_state.json"
TOPN_POSITIONS = LOG_DIR / "topn" / "positions.csv"
# [2026-09-19] 새 로직(분기 시총 V2)도 같은 모의계좌를 쓴다(10-01 첫 재구성). 원장은 날짜 없는 누적 체결.
V2_FILLS = PAPER_DIR / "strategies" / "kospi_mcap_quarterly_v2" / "data" / "state" / "fills.jsonl"

# 격차가 이 수를 넘으면 error, 아니면 warn. 0 이 정상이므로 문턱은 존재 자체다.
ERROR_IF_GAP_CODES = 1

# ── 청산 책임 선언 ────────────────────────────────────────────────────────
# [2026-09-11] 이 계좌에는 **청산 규칙이 다른 두 엔진**의 포지션이 섞여 있다.
#   그런데 어느 포지션을 누가 청산하는지 선언된 곳이 없다. 그래서 계좌만 보면
#   "손절이 돌고 있다" 고 읽힌다 — 2026-09-11 에 실제로 그 오독이 났다.
#   (계좌 1억이 전부 topn 것이고 topn 에는 손절이 없는데, paper_engine 의
#    손절·트레일을 보고 보호되고 있다고 읽었다. paper 포지션은 0건이었다.)
#
#   이 표는 **구현을 바꾸지 않는다. 구현이 무엇인지 적을 뿐이다.**
#   그래서 규칙마다 근거 위치를 같이 적는다. 구현을 고치면 여기도 같이 고쳐야 한다.
#   -> feedback_reconstruct_arithmetic_not_labels: 라벨을 믿지 말고 근거로 가라.
EXIT_OWNERS: Dict[str, Dict[str, Any]] = {
    "topn_stage1": {
        "exit_rule": "보유 13거래일 경과분 전량 매도 (일 1회, 종가 기준)",
        "stop_loss": False,
        "trailing": False,
        "intraday_watch": False,
        "evidence": "tools/topn_build_orders.py:15 '손절/트레일은 1단계에서 쓰지 않는다'",
    },
    "paper_engine_v41_1": {
        "exit_rule": "손절 / 트레일링 / TP (장중 루프)",
        "stop_loss": True,
        "trailing": True,
        "intraday_watch": True,
        "evidence": "intraday_paper_loop.py:381 _in_krx_session() 0900<=hhmm<1530 안에서만 실행",
    },
    "kospi_mcap_quarterly_v2": {
        "exit_rule": "종목 손절 없음 — 분기 재구성 / MA200 노출 변경 / 전략 NAV -25% 래치 뒤 다음 거래일 전량 매도",
        "stop_loss": False,
        "trailing": False,
        "intraday_watch": False,
        "evidence": "paper/strategies/kospi_mcap_quarterly_v2/src/daily_ops.py decide_next_action, ledger.py evaluate_latch",
    },
    "UNATTRIBUTED": {
        "exit_rule": "없음 — 어떤 엔진도 이 포지션을 청산하지 않는다",
        "stop_loss": False,
        "trailing": False,
        "intraday_watch": False,
        "evidence": "브로커 잔고에 있으나 내부 원장 어디에도 없다",
    },
    "AMBIGUOUS": {
        "exit_rule": "불명 — 두 원장이 같은 종목을 동시에 주장한다",
        "stop_loss": False,
        "trailing": False,
        "intraday_watch": False,
        "evidence": "topn/positions.csv · paper_state.json · V2 fills.jsonl 중 둘 이상이 같은 코드를 주장한다",
    },
}


def _now() -> dt.datetime:
    return dt.datetime.now()


def _z6(v: Any) -> str:
    return str(v).strip().zfill(6)


# ── 브로커 (유일한 외부 입력) ──────────────────────────────────────────────
def read_broker(mock: bool) -> Dict[str, Any]:
    """브로커 보유를 읽는다. 실패는 **조용하면 안 된다** — 상태로 올려보낸다."""
    try:
        sys.path.insert(0, str(TOOLS_DIR))
        from kis_order_client import KISOrderClient  # type: ignore

        client = KISOrderClient.from_env(mock=mock)
        r = client.inquire_balance_positions(max_pages=10)
        if not r.get("ok"):
            return {"ok": False, "reason": "inquire_balance_positions returned not-ok"}
        holdings: Dict[str, int] = {}
        values: Dict[str, float] = {}
        for row in r.get("rows") or []:
            code = _z6(row.get("pdno") or row.get("code") or "")
            if not code.isdigit() or len(code) != 6:
                continue
            try:
                qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
            except (TypeError, ValueError):
                qty = 0
            if qty:
                holdings[code] = holdings.get(code, 0) + qty
            # [2026-09-11] 평가금액도 같이 받는다. 없으면 None 으로 두고
            #   아래 자산 계산에서 **추정하지 않는다** (모르는 값을 0 으로 쓰지 않는다).
            for _k in ("evlu_amt", "evlu_amt_smtl", "prpr_evlu_amt"):
                if _k in row:
                    try:
                        values[code] = values.get(code, 0.0) + float(str(row[_k]).replace(",", ""))
                    except (TypeError, ValueError):
                        pass
                    break
        cash = None
        for s in r.get("summary_rows") or []:
            if "dnca_tot_amt" in s:
                try:
                    cash = float(s["dnca_tot_amt"])
                except (TypeError, ValueError):
                    cash = None
                break
        return {"ok": True, "mode": "mock" if mock else "prod",
                "holdings": holdings, "cash": cash, "values": values}
    except Exception as exc:
        return {"ok": False, "reason": f"{type(exc).__name__}: {exc}"}


# ── 내부 원장 셋 ───────────────────────────────────────────────────────────
def read_fills_net() -> Dict[str, Any]:
    if not FILLS.is_file():
        return {"ok": False, "reason": "fills.csv 없음"}
    try:
        df = pd.read_csv(FILLS, dtype={"code": str}, encoding="utf-8-sig")
    except Exception as exc:
        return {"ok": False, "reason": f"{type(exc).__name__}: {exc}"}
    if df.empty or "side" not in df.columns:
        return {"ok": True, "net": {}, "rows": len(df)}
    df["code"] = df["code"].map(_z6)
    qty = pd.to_numeric(df["qty"], errors="coerce").fillna(0.0)
    sign = df["side"].astype(str).str.upper().str.strip().map({"BUY": 1.0, "SELL": -1.0}).fillna(0.0)
    net = (qty * sign).groupby(df["code"]).sum()
    return {"ok": True, "net": {c: int(v) for c, v in net.items() if int(v) != 0},
            "rows": len(df), "all_net": {c: int(v) for c, v in net.items()}}


def read_paper_state() -> Dict[str, Any]:
    if not PAPER_STATE.is_file():
        return {"ok": False, "reason": "paper_state.json 없음"}
    try:
        d = json.loads(PAPER_STATE.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {"ok": False, "reason": f"{type(exc).__name__}: {exc}"}
    pos: Dict[str, int] = {}
    for p in d.get("open_positions") or []:
        if not isinstance(p, dict):
            continue
        c = _z6(p.get("code") or "")
        try:
            q = int(float(p.get("qty") or 0))
        except (TypeError, ValueError):
            q = 0
        if q:
            pos[c] = pos.get(c, 0) + q
    return {"ok": True, "positions": pos, "settled_cash": d.get("settled_cash")}


def read_topn_positions() -> Dict[str, Any]:
    # [2026-09-19] 라운드가 종결되면 이 파일은 마지막 상태로 멈춘다(topn 은 09-15 자).
    #   청산은 브로커에서 이미 끝났고 보유 0 이 정본이다. 멈춘 파일을 계속 대조하면
    #   "원장에만 6종목" 이 매일 뜬다 — 설계된 상태를 결함으로 읽는 것이다.
    closed = TOPN_POSITIONS.parent / "ROUND_CLOSED.json"
    if closed.is_file():
        return {"ok": True, "positions": {}, "closed": True,
                "reason": f"라운드 종결 표시({closed.name}) — 대조 대상 아님"}
    if not TOPN_POSITIONS.is_file():
        return {"ok": False, "reason": "topn/positions.csv 없음"}
    try:
        df = pd.read_csv(TOPN_POSITIONS, dtype={"code": str}, encoding="utf-8-sig")
    except Exception as exc:
        return {"ok": False, "reason": f"{type(exc).__name__}: {exc}"}
    if df.empty or "code" not in df.columns:
        return {"ok": True, "positions": {}}
    df["code"] = df["code"].map(_z6)
    q = pd.to_numeric(df.get("qty"), errors="coerce").fillna(0.0)
    g = q.groupby(df["code"]).sum()
    return {"ok": True, "positions": {c: int(v) for c, v in g.items() if int(v) != 0}}


def read_v2_positions() -> Dict[str, Any]:
    """V2 체결 누적(fills.jsonl, 한 줄 = 증분 체결)의 순보유.
    파일이 없으면 첫 재구성 전 = 설계된 상태라 ok. 읽다 깨지면 조용히 넘기지 않고 ok=False."""
    if not V2_FILLS.is_file():
        return {"ok": True, "positions": {}, "reason": "V2 체결 없음(첫 재구성 전)"}
    pos: Dict[str, int] = {}
    try:
        for line in V2_FILLS.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            f = json.loads(line)
            side = str(f.get("side") or "").upper()
            q = int(f.get("qty") or 0)
            c = _z6(f.get("code") or "")
            pos[c] = pos.get(c, 0) + (q if side == "BUY" else -q if side == "SELL" else 0)
    except Exception as exc:
        return {"ok": False, "reason": f"{type(exc).__name__}: {exc}"}
    return {"ok": True, "positions": {c: v for c, v in pos.items() if v != 0}}


# ── 계좌 자산 원장 (append-only) ──────────────────────────────────────────
# [2026-09-11] 날짜가 붙은 산출물은 30일 뒤 사라진다. 낙폭은 **과거와 비교해야**
#   나오는 값이라 그런 파일 위에 세울 수 없다. 그래서 날짜 없는 append-only 원장이다.
#   -> feedback_dated_artifacts_get_deleted
EQUITY_LEDGER = LOG_DIR / "account_equity_ledger.csv"
_NL = chr(10)
ROW_HEADER = "ts,mode,equity,cash,positions_value,codes,source" + _NL
ROW_FMT = "%s,%s,%.0f,%.0f,%.0f,%d,%s" + _NL
EQUITY_LEDGER_COLS = ROW_HEADER


def append_equity(row: Dict[str, Any]) -> None:
    new = not EQUITY_LEDGER.exists()
    with EQUITY_LEDGER.open("a", encoding="utf-8", newline="") as fh:
        if new:
            fh.write(EQUITY_LEDGER_COLS)
        fh.write(ROW_FMT % (
            row["ts"], row["mode"], row["equity"], row["cash"],
            row["positions_value"], row["codes"], row["source"]))


def read_equity_peak() -> Optional[float]:
    if not EQUITY_LEDGER.is_file():
        return None
    try:
        df = pd.read_csv(EQUITY_LEDGER)
    except Exception:
        return None
    q = pd.to_numeric(df.get("equity"), errors="coerce").dropna()
    return float(q.max()) if len(q) else None


def read_prev_day_equity(today_ymd: str) -> Optional[float]:
    """원장에서 **오늘 이전 마지막 날**의 마지막 자산. 일중손실률의 분모가 된다."""
    if not EQUITY_LEDGER.is_file():
        return None
    try:
        df = pd.read_csv(EQUITY_LEDGER)
    except Exception:
        return None
    if df.empty or "ts" not in df.columns:
        return None
    df["_ymd"] = df["ts"].astype(str).str.slice(0, 10).str.replace("-", "", regex=False)
    prev = df[df["_ymd"] < str(today_ymd)]
    if prev.empty:
        return None
    q = pd.to_numeric(prev["equity"], errors="coerce").dropna()
    return float(q.iloc[-1]) if len(q) else None


def write_account_basis(shadow: Dict[str, Any], now: dt.datetime) -> Optional[Path]:
    """`account_basis` 가 요구하는 모양으로 **브로커 기준** 자산 지표를 내보낸다.

    [2026-09-11] 소비자(p0_daily_check `_resolve_account_kill_switch_metrics`)는
    아직 이 파일을 읽지 않는다. **생산만 먼저 한다** — 위험 경로를 건드리지 않고
    며칠치 값을 쌓아 두려는 것이다. 연결은 값이 맞는 걸 본 뒤에 한다.
    """
    if shadow.get("status") != "OK":
        return None
    ymd = now.strftime("%Y%m%d")
    equity = float(shadow["equity"])
    peak = float(shadow["peak"])
    prev = read_prev_day_equity(ymd)
    daily = ((equity - prev) / prev) if (prev and prev > 0) else None
    payload = {
        "status": "PASS",
        "basis": "broker_account_equity",
        "as_of_ymd": ymd,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
        "capital_total": float(shadow.get("capital_total") or 0.0) or None,
        "equity_est": equity,
        "mark_to_market_equity_est": equity,
        "position_value": float(shadow["positions_value"]),
        "cash": float(shadow["cash"]),
        "account_peak_equity": peak,
        "account_mdd_basis": "equity_ledger_peak_with_capital_floor",
        "max_drawdown_pct": float(shadow["mdd_pct"]),
        "last_day_ret": daily,
        "daily_loss_pct": daily,
        "daily_loss_basis": "broker_equity_vs_prev_session_close",
        "daily_loss_status": "OK" if daily is not None else "NEEDS_PREVIOUS_DAILY_EQUITY",
        "source": "KIS inquire_balance_positions",
        "notes": ["broker_is_the_only_external_truth", "producer_only_not_yet_consumed"],
    }
    # 원장과 같은 곳에 쓴다. --out-dir 로 격리한 시험이 공유 산출물을 건드리지 않게.
    out = EQUITY_LEDGER.parent / "broker_account_basis_latest.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def ddm_shadow(broker: Dict[str, Any], now: dt.datetime) -> Dict[str, Any]:
    """**계좌 전체**를 DDM 사다리에 넣으면 몇 단계인가. 관측 전용 — 아무것도 하지 않는다.

    엔진이 실제로 보는 값(paper_ddm_status_latest.json)과 나란히 적는다.
    둘의 차이가 곧 '차단기가 못 보는 범위' 다.
    """
    out: Dict[str, Any] = {"observe_only": True}
    vals = broker.get("values") or {}
    hold = broker.get("holdings") or {}
    cash = broker.get("cash")
    # 평가금액을 못 받은 종목이 하나라도 있으면 자산을 **계산하지 않는다.**
    missing = sorted(set(hold) - set(vals))
    if missing or cash is None:
        out["status"] = "NO_VALUATION"
        out["reason"] = ("평가금액 미수신 %d종목" % len(missing)) if missing else "예수금 미수신"
        out["missing_codes"] = missing[:10]
        return out

    pos_val = float(sum(vals.get(c, 0.0) for c in hold))
    equity = pos_val + float(cash)

    # 기준선(peak)은 **원장 최대값과 투입원금 중 큰 쪽**이다.
    #   원장만 쓰면 관측 시작일이 곧 peak 이 되어 낙폭이 항상 0 근처로 나온다.
    #   -> feedback_freshness_reference_must_be_calendar 와 같은 함정이다.
    capital = None
    try:
        _d = json.loads((LOG_DIR / "paper_ddm_status_latest.json").read_text(encoding="utf-8"))
        capital = float((((_d.get("metric_details") or {}).get("account_basis")) or {}).get("capital_total") or 0.0) or None
    except Exception:
        capital = None
    ledger_peak = read_equity_peak()
    peak = max([v for v in (capital, ledger_peak, equity) if v]) if any((capital, ledger_peak, equity)) else equity
    mdd = (equity - peak) / peak if peak else 0.0

    stages: List[Dict[str, Any]] = []
    try:
        sys.path.insert(0, str(ROOT))
        from paper_engine.config import load_config  # type: ignore
        stages = list((load_config().get("drawdown_manager") or {}).get("stages") or [])
    except Exception as exc:
        out["status"] = "NO_STAGES"
        out["reason"] = f"{type(exc).__name__}: {exc}"
        return out

    stage_idx, stage = -1, None
    for i, st in enumerate(stages):
        if abs(mdd) >= float(st.get("mdd", 0.0)):
            stage_idx, stage = i, st

    out.update({
        "status": "OK",
        "equity": round(equity),
        "cash": round(float(cash)),
        "positions_value": round(pos_val),
        "peak": round(peak),
        "peak_source": "capital_total" if capital and peak == capital else ("equity_ledger" if ledger_peak and peak == ledger_peak else "current"),
        "capital_total": capital,
        "mdd_pct": round(mdd, 6),
        "stage_idx": stage_idx,
        "stage_label": (stage or {}).get("label"),
        "liquidate_weakest_pct": float((stage or {}).get("liquidate_weakest_pct", 0.0) or 0.0),
        "next_stage_mdd": float(stages[stage_idx + 1]["mdd"]) if stage_idx + 1 < len(stages) else None,
    })
    append_equity({"ts": now.strftime("%Y-%m-%dT%H:%M:%S"), "mode": broker.get("mode") or "?",
                   "equity": equity, "cash": float(cash), "positions_value": pos_val,
                   "codes": len(hold), "source": "kis_balance"})

    # 엔진이 실제로 보는 값과 대조한다 — 여기가 핵심이다.
    try:
        _d = json.loads((LOG_DIR / "paper_ddm_status_latest.json").read_text(encoding="utf-8"))
        ab = ((_d.get("metric_details") or {}).get("account_basis")) or {}
        out["engine_sees"] = {
            "generated_at": _d.get("generated_at"),
            "equity_est": ab.get("equity_est"),
            "mdd_pct": _d.get("current_mdd_abs"),
            "stage_idx": _d.get("stage_idx"),
            "metric_basis": _d.get("metric_basis"),
            "liquidation_target_ledger": "paper_engine open_pos",
        }
    except Exception:
        out["engine_sees"] = None
    return out


def ddm_liquidation_preview(shadow: Dict[str, Any], ownership: List[Dict[str, Any]]) -> Dict[str, Any]:
    """DDM 단계가 발동하면 **계좌 전체에서** 무엇이 팔릴지 미리 본다. 관측 전용.

    [2026-09-11] EXIT_OWNERSHIP_RULES R2/R3 의 "선택" 부분이다.
      R2  위험 청산은 소유권을 넘어선다
      R3  선택은 **계좌 전체 보유**에서 한다. open_pos(엔진 원장)가 아니다

    선택 규칙은 지어내지 않고 엔진 구현을 그대로 미러링한다
    (paper_engine/drawdown.py:431 _ddm_select_liquidation_targets):
      pnl_pct 오름차순(손실 큰 것부터) -> n = int(보유수 x pct), pct>0 인데 n=0 이면 1
      부분 청산(0<pct<1)일 때 보유수 < min_positions 면 아무것도 고르지 않는다

    **아무것도 팔지 않는다.** 단계가 -1 이어도 순위는 항상 낸다 —
    실제로 닿았을 때 무엇이 먼저 나가는지 미리 알고 있어야 한다.
    """
    out: Dict[str, Any] = {"observe_only": True}
    if shadow.get("status") != "OK":
        out["status"] = "NO_SHADOW"
        return out

    snap_path = LOG_DIR / "kis_account_snapshot_latest.json"
    try:
        snap = json.loads(snap_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        out["status"] = "NO_SNAPSHOT"
        out["reason"] = f"{type(exc).__name__}"
        return out
    import time as _time
    age_h = (_time.time() - snap_path.stat().st_mtime) / 3600.0
    out["snapshot_age_hours"] = round(age_h, 2)
    if age_h > 30.0:
        out["status"] = "SNAPSHOT_STALE"
        return out

    pnl = {}
    for r in (snap.get("positions") or []):
        c = _z6(r.get("code") or "")
        v = r.get("unrealized_pct")
        if c and isinstance(v, (int, float)):
            pnl[c] = float(v)

    rows = []
    for o in ownership:
        c = o["code"]
        if c not in pnl:
            continue
        rows.append({"code": c, "qty": int(o["broker_qty"]), "pnl_pct": pnl[c], "owner": o["owner"]})
    missing = sorted({o["code"] for o in ownership} - set(pnl))
    if missing:
        # 모르는 값을 0 으로 채우지 않는다. 빠진 종목은 순위에서 제외하고 드러낸다
        out["missing_pnl_codes"] = missing
    if not rows:
        out["status"] = "NO_ROWS"
        return out

    rows.sort(key=lambda x: x["pnl_pct"])          # 손실 큰 것부터
    for i, r in enumerate(rows, 1):
        r["rank"] = i

    stages = []
    try:
        sys.path.insert(0, str(ROOT))
        from paper_engine.config import load_config  # type: ignore
        dm = (load_config().get("drawdown_manager") or {})
        stages = list(dm.get("stages") or [])
        min_pos = int(float(dm.get("min_positions_for_partial_liquidation", 1) or 1))
    except Exception:
        min_pos = 1

    def _select(pct: float) -> List[str]:
        if pct <= 0 or not rows:
            return []
        if 0.0 < pct < 1.0 and len(rows) < max(1, min_pos):
            return []
        n = int(len(rows) * pct)
        if pct > 0 and n <= 0:
            n = 1
        n = max(0, min(len(rows), n))
        return [r["code"] for r in rows[:n]]

    cur_pct = float(shadow.get("liquidate_weakest_pct", 0.0) or 0.0)
    sel = _select(cur_pct)
    out.update({
        "status": "OK",
        "stage_idx": shadow.get("stage_idx"),
        "stage_label": shadow.get("stage_label"),
        "liquidate_weakest_pct": cur_pct,
        "ranking": rows,
        "selected_now": sel,
        "selected_now_notional": None,
        "by_stage": [
            {"stage": i, "label": st.get("label"), "mdd": st.get("mdd"),
             "pct": float(st.get("liquidate_weakest_pct", 0.0) or 0.0),
             "would_select": _select(float(st.get("liquidate_weakest_pct", 0.0) or 0.0))}
            for i, st in enumerate(stages)
        ],
        "note": "선택만 한다. 집행은 전용 경로 (EXIT_OWNERSHIP_RULES R3). 지금은 집행 경로가 없다",
    })
    return out


def reconcile(mock: bool) -> Dict[str, Any]:
    now = _now()
    broker = read_broker(mock=mock)
    fills = read_fills_net()
    state = read_paper_state()
    topn = read_topn_positions()
    v2 = read_v2_positions()

    out: Dict[str, Any] = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "account_mode": broker.get("mode") or ("mock" if mock else "prod"),
        "sources": {
            "broker": {"ok": broker["ok"], "reason": broker.get("reason"),
                       "codes": len(broker.get("holdings") or {}),
                       "shares": int(sum((broker.get("holdings") or {}).values())),
                       "cash": broker.get("cash")},
            "fills_net": {"ok": fills["ok"], "reason": fills.get("reason"),
                          "codes": len(fills.get("net") or {})},
            "paper_state": {"ok": state["ok"], "reason": state.get("reason"),
                            "codes": len(state.get("positions") or {}),
                            "settled_cash": state.get("settled_cash")},
            "topn_positions": {"ok": topn["ok"], "reason": topn.get("reason"),
                               "codes": len(topn.get("positions") or {})},
            "v2_positions": {"ok": v2["ok"], "reason": v2.get("reason"),
                             "codes": len(v2.get("positions") or {})},
        },
    }

    # 브로커를 못 읽으면 대조 자체가 성립하지 않는다. PASS 로 넘기지 않는다.
    if not broker["ok"]:
        out["status"] = "NO_BROKER"
        out["reason"] = f"브로커 조회 실패 — 대조 불가: {broker.get('reason')}"
        out["gaps"] = []
        return out

    bh: Dict[str, int] = broker.get("holdings") or {}
    fn: Dict[str, int] = fills.get("net") or {}
    ps: Dict[str, int] = state.get("positions") or {}
    tp: Dict[str, int] = topn.get("positions") or {}
    v2p: Dict[str, int] = v2.get("positions") or {}

    codes = sorted(set(bh) | set(fn) | set(ps) | set(tp) | set(v2p))
    gaps: List[Dict[str, Any]] = []
    for c in codes:
        b = int(bh.get(c, 0))
        internal_best = int(fn.get(c, 0)) + int(ps.get(c, 0)) + int(tp.get(c, 0)) + int(v2p.get(c, 0))
        # 세 원장이 같은 포지션을 중복 계상하지 않는다는 보장이 없다.
        # 그래서 합이 아니라 **최대값**으로 "내부가 아는 최대 보유"를 잡는다(관대한 쪽).
        internal_max = max(int(fn.get(c, 0)), int(ps.get(c, 0)), int(tp.get(c, 0)))
        # [2026-09-19] V2 는 v41.1 원장(fills·paper_state)과 겹치지 않는 별도 전략 원장이라 **더한다**.
        #   (v41.1 셋은 서로 중복 계상 가능성 때문에 최대값. V2 는 청산 전용·보유 0 이 09-28 가부 조건 9번)
        internal_max += int(v2p.get(c, 0))
        if b != internal_max:
            gaps.append({
                "code": c, "broker_qty": b,
                "fills_net": int(fn.get(c, 0)),
                "paper_state": int(ps.get(c, 0)),
                "topn_positions": int(tp.get(c, 0)),
                "v2_positions": int(v2p.get(c, 0)),
                "internal_max": internal_max,
                "internal_sum": internal_best,
                "gap": b - internal_max,
            })

    only_broker = [g for g in gaps if g["broker_qty"] > 0 and g["internal_max"] == 0]
    only_internal = [g for g in gaps if g["broker_qty"] == 0 and g["internal_max"] > 0]
    qty_mismatch = [g for g in gaps if g["broker_qty"] > 0 and g["internal_max"] > 0]

    out["gaps"] = gaps
    out["summary"] = {
        "gap_codes": len(gaps),
        "only_in_broker": len(only_broker),
        "only_in_ledger": len(only_internal),
        "qty_mismatch": len(qty_mismatch),
        "broker_shares_unbacked": int(sum(g["broker_qty"] for g in only_broker)),
    }
    out["ddm_shadow"] = ddm_shadow(broker, now)
    _ab = write_account_basis(out["ddm_shadow"], now)
    if _ab:
        out["ddm_shadow"]["account_basis_artifact"] = str(_ab)

    # ── 브로커 보유에 청산 책임을 붙인다 ──
    #   대상은 **브로커 잔고**다. 내부 원장이 아니다. 실제로 존재하는 위험만 센다.
    ownership: List[Dict[str, Any]] = []
    for c in sorted(bh):
        claims = [name for name, held in (("topn_stage1", int(tp.get(c, 0)) > 0),
                                          ("paper_engine_v41_1", int(ps.get(c, 0)) > 0),
                                          ("kospi_mcap_quarterly_v2", int(v2p.get(c, 0)) > 0)) if held]
        if len(claims) > 1:
            owner = "AMBIGUOUS"
        elif claims:
            owner = claims[0]
        else:
            owner = "UNATTRIBUTED"
        rule = EXIT_OWNERS[owner]
        ownership.append({
            "code": c,
            "broker_qty": int(bh[c]),
            "owner": owner,
            "exit_rule": rule["exit_rule"],
            "stop_loss": rule["stop_loss"],
            "intraday_watch": rule["intraday_watch"],
            "evidence": rule["evidence"],
        })
    no_stop = [o for o in ownership if not o["stop_loss"]]
    out["ownership"] = ownership
    out["ddm_liquidation_preview"] = ddm_liquidation_preview(out.get("ddm_shadow") or {}, ownership)
    out["ownership_summary"] = {
        "positions": len(ownership),
        "by_owner": {k: sum(1 for o in ownership if o["owner"] == k)
                     for k in sorted({o["owner"] for o in ownership})},
        "no_stop_codes": len(no_stop),
        "no_stop_shares": int(sum(o["broker_qty"] for o in no_stop)),
        "unattributed_codes": sum(1 for o in ownership if o["owner"] == "UNATTRIBUTED"),
        "ambiguous_codes": sum(1 for o in ownership if o["owner"] == "AMBIGUOUS"),
    }

    out["status"] = "GAP" if len(gaps) >= ERROR_IF_GAP_CODES else "MATCH"
    return out


def build_alert_text(out: Dict[str, Any]) -> Optional[str]:
    st = out.get("status")
    own = out.get("ownership_summary") or {}
    # 청산 책임이 **불명**인 것만 경보한다.
    #   "손절이 없다"(topn_stage1) 는 이상이 아니라 현재 설계다. 매일 울리면
    #   그 경보는 무시되고, 진짜 미귀속이 났을 때 같이 묻힌다.
    #   -> 설계 사실은 JSON 과 stdout 에 남기고, 경보는 이상에만 쓴다.
    own_bad = int(own.get("unattributed_codes", 0)) + int(own.get("ambiguous_codes", 0))
    if st == "MATCH" and not own_bad:
        return None
    lines = [f"[원장 대조] {out['generated_at']}  계좌={out.get('account_mode')}"]
    if st == "NO_BROKER":
        lines.append("브로커 조회 실패 — **대조 자체가 성립하지 않는다**")
        lines.append(f"  {out.get('reason')}")
        return "\n".join(lines)
    s = out["summary"]
    lines.append(f"격차 {s['gap_codes']}종목 "
                 f"(브로커에만 {s['only_in_broker']} / 원장에만 {s['only_in_ledger']} / 수량불일치 {s['qty_mismatch']})")
    lines.append(f"원장이 뒷받침하지 못하는 브로커 보유 {s['broker_shares_unbacked']:,}주")
    if own_bad:
        lines.append(f"청산 책임 불명 {own_bad}종목 "
                     f"(미귀속 {own.get('unattributed_codes', 0)} / "
                     f"중복주장 {own.get('ambiguous_codes', 0)}) "
                     f"— 이 종목들은 아무도 청산하지 않는다")
        for o in out.get("ownership", []):
            if o["owner"] in ("UNATTRIBUTED", "AMBIGUOUS"):
                lines.append(f"  {o['code']} {o['broker_qty']:,}주  {o['owner']}")
    for g in out["gaps"][:8]:
        lines.append(f"  {g['code']} 브로커 {g['broker_qty']:,} vs 원장 {g['internal_max']:,} (차 {g['gap']:+,})")
    if len(out["gaps"]) > 8:
        lines.append(f"  ... 외 {len(out['gaps']) - 8}종목")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prod", action="store_true", help="실계좌를 본다 (기본은 모의)")
    ap.add_argument("--no-alert", action="store_true")
    ap.add_argument("--strict", action="store_true", help="격차·조회실패 시 rc=2")
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    # [2026-09-11] **자산 원장도 out_dir 을 따른다.**
    #   안 그러면 --out-dir 로 격리한 시험 실행이 공유 원장에 행을 남긴다.
    #   -> feedback_check_where_the_tool_writes
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    global EQUITY_LEDGER
    EQUITY_LEDGER = out_dir / "account_equity_ledger.csv"

    out = reconcile(mock=not args.prod)
    ts = _now().strftime("%Y%m%d_%H%M%S")
    body = json.dumps(out, ensure_ascii=False, indent=2)
    (out_dir / f"broker_ledger_reconcile_{ts}.json").write_text(body, encoding="utf-8")
    (out_dir / "broker_ledger_reconcile_latest.json").write_text(body, encoding="utf-8")

    src = out["sources"]
    print(f"[RECON] status={out['status']} 계좌={out.get('account_mode')}")
    print(f"        브로커 ok={src['broker']['ok']} 종목={src['broker']['codes']} "
          f"주식={src['broker']['shares']:,} 예수금={src['broker']['cash']}")
    print(f"        fills_net 종목={src['fills_net']['codes']} / "
          f"paper_state 종목={src['paper_state']['codes']} / topn 종목={src['topn_positions']['codes']} / "
          f"V2 종목={src['v2_positions']['codes']}")
    if out.get("summary"):
        s = out["summary"]
        print(f"        격차 {s['gap_codes']}종목  브로커에만 {s['only_in_broker']}  "
              f"원장에만 {s['only_in_ledger']}  수량불일치 {s['qty_mismatch']}  "
              f"미뒷받침 {s['broker_shares_unbacked']:,}주")
    sh = out.get("ddm_shadow") or {}
    if sh.get("status") == "OK":
        print(f"        [DDM그림자] 계좌자산 {sh['equity']:,}원 (보유 {sh['positions_value']:,} + 예수금 {sh['cash']:,})  "
              f"기준 {sh['peak']:,}({sh['peak_source']})  낙폭 {sh['mdd_pct']*100:.2f}%  "
              f"단계 {sh['stage_idx']} {sh.get('stage_label') or '-'}  청산비율 {sh['liquidate_weakest_pct']:.2f}")
        es = sh.get("engine_sees") or {}
        if es:
            print(f"        [DDM실제]   엔진이 보는 자산 {es.get('equity_est')}  낙폭 {es.get('mdd_pct')}  "
                  f"단계 {es.get('stage_idx')}  청산대상원장={es.get('liquidation_target_ledger')}")
    elif sh:
        print(f"        [DDM그림자] {sh.get('status')} {sh.get('reason','')}")

    lq = out.get("ddm_liquidation_preview") or {}
    if lq.get("status") == "OK":
        rk = lq.get("ranking") or []
        print(f"        [청산순위] 단계 {lq.get('stage_idx')} 청산비율 {lq.get('liquidate_weakest_pct'):.2f} "
              f"-> 지금 선택 {len(lq.get('selected_now') or [])}종목  (스냅샷 {lq.get('snapshot_age_hours')}h 전)")
        for r in rk[:3]:
            print(f"          {r['rank']}순위 {r['code']} {r['pnl_pct']*100:+.2f}%  {r['owner']}")
        for st in (lq.get("by_stage") or []):
            if st["pct"] > 0:
                print(f"          단계{st['stage']} {st['label']} (mdd {st['mdd']}) 청산 {st['pct']:.0%} "
                      f"-> {', '.join(st['would_select']) or '-'}")
    elif lq:
        print(f"        [청산순위] {lq.get('status')} {lq.get('reason','')}")

    own = out.get("ownership_summary")
    if own:
        print(f"        청산책임 {own['by_owner']}  "
              f"손절없음 {own['no_stop_codes']}종목 {own['no_stop_shares']:,}주  "
              f"미귀속 {own['unattributed_codes']}  중복주장 {own['ambiguous_codes']}")
        for o in out.get("ownership", []):
            print(f"          {o['code']} {o['broker_qty']:>6,}주  {o['owner']:<18} "
                  f"손절={'있음' if o['stop_loss'] else '없음'}  {o['exit_rule']}")
    if out["status"] == "NO_BROKER":
        print(f"        {out.get('reason')}")

    text = build_alert_text(out)
    if text and not args.no_alert:
        try:
            sys.path.insert(0, str(TOOLS_DIR))
            from notify_channels import send_alert  # type: ignore

            res = send_alert(text, level="error",
                             extra={"source": "broker_ledger_reconcile"},
                             cooldown_sec=6 * 3600)
            print(f"[RECON] alert ok={res.get('ok')} suppressed={res.get('suppressed')} "
                  f"channels={res.get('channels')}")
        except Exception as exc:
            print(f"[RECON] alert_failed {type(exc).__name__}: {exc}")

    if args.strict and out["status"] != "MATCH":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
