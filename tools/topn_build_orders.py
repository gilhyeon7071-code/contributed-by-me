# -*- coding: utf-8 -*-
"""RD_20260901_topn 1단계 - ARM_SCORE 상위 N 에서 orders_exec 를 만든다

사전등록  docs/references/PREREG_RD_20260901_TOPN.md
          가격 규칙과 보유 규칙은 이 파일 헤더가 사전등록 부록이다 (실행 전 확정)

집행 대상  ARM_SCORE 만. 계좌가 1개라 ARM 하나만 집행할 수 있다.
          ARM_RANDOM / ARM_LIQ 는 지면 기록으로만 남는다 (주문 없음)

포지션 규칙 - v41.1 집행 파라미터를 그대로 쓴다. 튜닝하지 않는다
  자본       100,000,000원 (모의)
  max_pos    6           stable_params_v41_1.json
  hold       13 거래일    stable_params_v41_1.json
  1포지션     자본 / max_pos = 16,666,666원
  매도       보유 13거래일 경과분. 손절/트레일은 1단계에서 쓰지 않는다
             (1단계는 매매 가능성 측정이다. 청산 규칙 비교는 2단계 문제다)

가격 규칙 - 사전등록서에 빠져 있던 것을 여기서 확정한다
  BUY  limit = 직전 종가 x 1.005, 호가단위 올림
  SELL limit = 직전 종가 x 0.995, 호가단위 내림
  근거  시장가는 가용현금의 77% 만 쓸 수 있는 증거금 제약이 있어 쓰지 않는다
        (project_1data_market_order_upper_limit_margin). 0.5% 폭은 왕복비용
        계약값 0.358% 와 같은 자릿수로 잡은 것이며, 결과를 보고 바꾸지 않는다

출력      2_Logs/topn/ 아래에만 쓴다. paper/orders_*_exec.xlsx 를 절대 건드리지 않는다
          orders_{D}_exec.xlsx      디스패처 입력 (동일 스키마)
          positions.csv             하네스 전용 보유 원장
주문 없음  이 도구는 파일만 만든다. 발주는 kis_order_dispatch_from_exec.py 가 한다
"""
from __future__ import annotations

import argparse
import json
import sys
import datetime as dt
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
OUT_DIR = ROOT / "2_Logs" / "topn"
POS_PATH = OUT_DIR / "positions.csv"

CAPITAL = 100_000_000.0
MAX_POS = 6
HOLD_DAYS = 13
BUY_BUFFER = 1.005
SELL_BUFFER = 0.995
EXEC_ARM = "ARM_SCORE"

ORDER_COLS = [
    "exec_date", "side", "code", "fill_qty", "fill_price", "stop_level",
    "signal_date", "fill_datetime", "signal_ts", "is_stop", "note", "reason",
    "entry_blocked", "entry_block_reason", "execution_blocked",
    "execution_block_reason", "intent_id",
]


# [2026-09-08] 호가단위 표는 utils/krx_tick.py 가 단일 출처다.
#   같은 표가 audit_daily.py 와 여기 두 벌 있었고 여기 것이 5단계로 틀려 있었다
#   (5만원 이상이 전부 100원 -> 003230 지정가 1,336,700 이 거래소에서 호가단위 오류로 거절).
#   두 곳을 utils 로 모았다. 근거와 시장 구분을 안 쓰는 이유는 그 파일 docstring 에 있다.
#   이름은 그대로 재노출한다 - 이 모듈의 tick_size / round_tick 를 참조하는 시험이 있다.
from utils.krx_tick import round_tick, tick_size  # noqa: E402,F401


def load_positions() -> pd.DataFrame:
    if POS_PATH.exists():
        df = pd.read_csv(POS_PATH, dtype={"code": str}, encoding="utf-8-sig")
        if len(df):
            return df
    return pd.DataFrame(columns=["code", "entry_date", "qty", "entry_price", "held_days"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="", help="YYYYMMDD 발주일(exec_date). 비우면 오늘")
    ap.add_argument("--signal-date", default="", help="YYYYMMDD 신호일. 비우면 후보 원장 최신일")
    ap.add_argument("--force", action="store_true", help="최신이어도 다시 만든다")
    ap.add_argument("--out-dir", default=str(OUT_DIR))
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    # [2026-09-03] --out-dir 가 보유 원장까지 지배해야 한다 (시험 격리)
    global POS_PATH
    POS_PATH = out_dir / "positions.csv"

    led = out_dir / "forward_ledger.csv"
    if not led.exists():
        print("[STOP] forward_ledger.csv 가 없다. topn_candidates.py --forward 를 먼저 돌려라")
        return 2
    cand = pd.read_csv(led, dtype={"code": str}, encoding="utf-8-sig")
    cand = cand[cand["arm"] == EXEC_ARM]
    if not len(cand):
        print("[STOP] %s 후보가 없다" % EXEC_ARM)
        return 2

    # [2026-09-03] **신호일과 발주일은 같을 수 없다.**
    #   가격 패널은 21:32 에 갱신되므로 D일 종가로 후보를 뽑는 시점에는 이미 장이 닫혀 있다.
    #   그 상태로 발주하면 브로커가 반려한다 - 실측: 장외 발주 76건 중 18건이
    #   dispatch_status=SKIP_MARKET_CLOSED, msg1="모의투자 장종료 입니다." 체결 0건.
    #   사전등록서의 가격 규칙("BUY limit = **직전 종가** x 1.005")이 이미 그 전제를 담고
    #   있었는데 구현만 따르지 않았다.
    #   -> signal_date = 후보일(D),  exec_date = **발주하는 날**(기본 오늘, 보통 D+1)
    sig_d = args.signal_date.strip() or pd.Timestamp(cand["date"].max()).strftime("%Y%m%d")
    d = args.date.strip() or pd.Timestamp.now().strftime("%Y%m%d")
    ymd = pd.Timestamp(sig_d).strftime("%Y-%m-%d")
    today = cand[cand["date"] == ymd].sort_values("rank")
    if not len(today):
        print("[STOP] %s 후보가 없다" % ymd)
        return 2
    if sig_d == d:
        print("[WARN] signal_date == exec_date (%s). 장중 발주라면 참조가가 당일 종가라는 뜻이라"
              " 선견 위험이 있다. 정상 운용은 D 신호 -> D+1 발주다" % d, flush=True)
    print("[DATE] signal=%s  exec=%s" % (sig_d, d), flush=True)

    # [2026-09-03] **10분마다 640만행 패널을 다시 올리면 안 된다.**
    #   load_data 실측 118.6초. 장중 배치가 10분 간격이므로 하루 38회 = 76분간
    #   전 종목 패널을 물고 있게 되고, 같은 시각 생산 장중 루프(2분 주기)와 경합한다.
    #   그 루프는 이미 TIMEOUT 이 나던 중이다.
    #   -> 오늘 주문 파일이 이미 있고, 그 뒤로 **보유 원장도 후보 원장도 안 바뀌었으면**
    #      다시 만들 이유가 없다. 체결이 나서 positions.csv 가 갱신되면 그때 다시 만든다.
    op_check = out_dir / ("orders_%s_exec.xlsx" % d)
    if not args.force and op_check.exists():
        try:
            om = op_check.stat().st_mtime
            deps = [q for q in (POS_PATH, led) if q.exists()]
            newer = [q.name for q in deps if q.stat().st_mtime > om]
            if not newer:
                print("[SKIP] %s 가 이미 최신이다 (보유·후보 원장 변경 없음). 패널 로딩 생략"
                      % op_check.name, flush=True)
                return 0
            print("[REBUILD] 변경 감지: %s" % ", ".join(newer), flush=True)
        except Exception as e:
            print("[WARN] 신선도 판정 실패, 그대로 재생성한다: %s" % e, flush=True)

    # 종가는 패널에서 가져온다 (직전 종가 = 후보일 종가)
    import optimize_params_v41_1 as O

    px_all = O.load_data(O.BASE_DIR)
    px_all = px_all[px_all["date"] <= pd.Timestamp(ymd)][["date", "code", "close"]].copy()
    px_all["code"] = px_all["code"].astype(str).str.zfill(6)
    px = px_all[px_all["date"] == pd.Timestamp(ymd)]
    close_of = dict(zip(px["code"], pd.to_numeric(px["close"], errors="coerce")))

    # [2026-09-03] 신호일 종가가 없는 종목의 **마지막 알려진 종가**. 매도 누락 방지용.
    #   종가가 없다고 매도를 건너뛰면 그 포지션은 영원히 안 팔리고 held_days 만 늘어난다
    #   (상장폐지 / 거래정지 / 패널 결측). 소리 없이 빠뜨리지 않는다.
    _last = px_all.sort_values("date").groupby("code")["close"].last()
    last_close_of = dict(zip(_last.index, pd.to_numeric(_last.values, errors="coerce")))

    pos = load_positions()
    pos["code"] = pos["code"].astype(str).str.zfill(6)
    rows = []

    # 1) 매도 - 보유 13거래일 경과분
    sells = pos[pd.to_numeric(pos["held_days"], errors="coerce").fillna(0) >= HOLD_DAYS].copy()
    # [2026-09-07] 수량 가드를 **상류 한 곳**에 둔다.
    #   매수에는 `if q <= 0: continue` 가 있는데 매도에는 없었다. qty 가 0/음수/문자면
    #   0주 주문이 만들어지고 그 행이 **A4(발주 대비 체결률)의 분모**에 들어간다.
    #   원인이 신호나 유동성으로 오귀인된다.
    #   그리고 sells 는 아래에서 두 번 순회한다(주문 생성 / decision 사이드카).
    #   각 자리에 가드를 넣으면 한 곳을 빠뜨린다 - 실제로 처음 수리에서 사이드카를 놓쳤다.
    #   그래서 여기서 한 번만 거른다.
    _q = pd.to_numeric(sells["qty"], errors="coerce").fillna(0)
    _bad = sells[_q <= 0]
    for _, _r in _bad.iterrows():
        print("[SKIP] SELL %s qty=%s 가 유효하지 않다. 주문을 만들지 않는다"
              % (str(_r["code"]).zfill(6), _r.get("qty")), flush=True)
    sells = sells[_q > 0].copy()
    sells["qty"] = pd.to_numeric(sells["qty"], errors="coerce").fillna(0).astype(int)
    for _, r in sells.iterrows():
        c = str(r["code"]).zfill(6)
        cp = close_of.get(c)
        src = "신호일 종가"
        if not cp or not np.isfinite(cp) or cp <= 0:
            cp = last_close_of.get(c)
            src = "마지막 알려진 종가"
        if not cp or not np.isfinite(cp) or cp <= 0:
            cp = float(pd.to_numeric(pd.Series([r.get("entry_price")]), errors="coerce").iloc[0] or 0)
            src = "진입가(최후 수단)"
        if not cp or not np.isfinite(cp) or cp <= 0:
            print("[FAIL] SELL %s 가격을 어떤 방법으로도 못 구했다. **수동 확인 필요**" % c, flush=True)
            continue
        if src != "신호일 종가":
            print("[FALLBACK] SELL %s 가격 출처=%s (%.0f)" % (c, src, cp), flush=True)
        rows.append({
            "exec_date": d, "side": "SELL", "code": c,
            "fill_qty": int(r["qty"]), "fill_price": round_tick(float(cp) * SELL_BUFFER, up=False),
            "signal_date": sig_d, "is_stop": False,
            "note": "RD_TOPN_STAGE1 hold_exit", "reason": "hold_%dd" % HOLD_DAYS,
            "entry_blocked": False, "entry_block_reason": "",
            "execution_blocked": False, "execution_block_reason": "",
            "intent_id": "TOPN_%s_S_%s" % (d, c),
        })

    # 2) 매수 - 남는 칸을 상위 랭크로 채운다
    # [2026-09-03] **매도 주문을 냈다고 자리가 나는 것이 아니다.**
    #   예전 구현은 매도 대상 종목을 보유에서 빼고 그만큼 새로 샀다. 매도가 부분 체결되면
    #   (실측: 6건 중 2건) 미매도분 + 신규분이 겹쳐 max_pos 를 초과하고 자금이 이중 투입된다.
    #   체결로 positions.csv 에서 실제로 빠진 뒤에 자리가 열린다. 하루 지연되지만 안전하다.
    held = set(pos["code"].tolist())
    slots = MAX_POS - len(held)
    if len(sells):
        print("[SLOT] 매도 주문 %d건은 체결 전이므로 자리로 세지 않는다 (보유 %d, 빈칸 %d)"
              % (len(sells), len(held), slots), flush=True)
    per = CAPITAL / MAX_POS
    buys = []
    for _, r in today.iterrows():
        if slots <= 0:
            break
        c = str(r["code"]).zfill(6)
        if c in held:
            continue
        cp = close_of.get(c)
        if not cp or not np.isfinite(cp) or cp <= 0:
            continue
        lp = round_tick(float(cp) * BUY_BUFFER, up=True)
        q = int(per // lp)
        if q <= 0:
            continue
        buys.append((c, q, lp, int(r["rank"])))
        rows.append({
            "exec_date": d, "side": "BUY", "code": c,
            "fill_qty": q, "fill_price": lp,
            "signal_date": sig_d, "is_stop": False,
            "note": "RD_TOPN_STAGE1 %s rank=%d" % (EXEC_ARM, int(r["rank"])),
            "reason": EXEC_ARM,
            "entry_blocked": False, "entry_block_reason": "",
            "execution_blocked": False, "execution_block_reason": "",
            "intent_id": "TOPN_%s_B_%s" % (d, c),
        })
        slots -= 1

    orders = pd.DataFrame(rows)
    for c in ORDER_COLS:
        if c not in orders.columns:
            orders[c] = ""
    orders = orders[ORDER_COLS] if len(orders) else pd.DataFrame(columns=ORDER_COLS)
    op = out_dir / ("orders_%s_exec.xlsx" % d)
    # [2026-09-10] **재작성 전에 기존 파일을 보존한다.**
    #   이 파일은 매 실행마다 다시 만들어진다. 그래서 같은 날 두 번 돌면
    #   "아침에 무엇을 발주하려 했는가" 가 사라진다.
    #   2026-09-08 이 그랬다: 오전에 4종목이 나가 체결됐는데 파일에는 잔여 2행만 남아,
    #   브로커에 주문번호 7건이 있는데도 우리 쪽에서 출처를 못 찾았다.
    #   주문 금액이 슬롯의 99.9~100% 라는 산술로 역산해서야 topn 것임을 알았다.
    if op.exists():
        try:
            prev = op.with_name(
                op.stem + ".prev_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + op.suffix
            )
            shutil.copy2(op, prev)
        except Exception as exc:
            print("[WARN] 기존 주문 파일 보존 실패 (%s: %s). 재작성은 진행한다"
                  % (type(exc).__name__, exc))
    orders.to_excel(op, index=False)

    # [2026-09-02] A5(슬리피지)에는 **판단 기준가**가 필요하다. 지정가는 기준가에
    # 버퍼를 얹은 값이라 그것만으로는 슬리피지를 못 잰다. 사이드카로 남긴다.
    dec = []
    for c, q, lp, rk in buys:
        dec.append({"date": d, "signal_date": sig_d, "side": "BUY", "code": c, "rank": rk,
                    "ref_close": float(close_of.get(c, float("nan"))),
                    "limit_price": lp, "qty": q, "arm": EXEC_ARM})
    for _, r in sells.iterrows():
        c = str(r["code"]).zfill(6)
        cp = close_of.get(c)
        if cp and np.isfinite(cp) and cp > 0:
            dec.append({"date": d, "signal_date": sig_d, "side": "SELL", "code": c, "rank": -1,
                        "ref_close": float(cp),
                        "limit_price": round_tick(float(cp) * SELL_BUFFER, up=False),
                        "qty": int(r["qty"]), "arm": EXEC_ARM})
    # [2026-09-09] 사이드카는 **덮어쓰지 않고 병합해요.**
    #   사전등록서 기준 이 파일은 "판단 기준가(ref_close) 사이드카. A5 재료" 예요.
    #   즉 작업 지시가 아니라 **그날의 판단 기록**인데 덮어쓰고 있었어요.
    #   실측 2026-09-09: 14:25 에 체결이 원장에 들어가 보유가 6/6 이 되자
    #     14:42 재빌드가 매수 0건을 내면서 이 파일을 헤더만 남기고 비웠고,
    #     그 결과 **오늘 유일한 매수의 A5(슬리피지)가 n/a 가 됐어요**
    #     (직전 사이클에서는 +0.6112% 로 나오고 있었어요).
    #   빌드 자체는 정상이에요 - 자리가 없으면 살 게 없으니까요.
    #   틀린 건 정상 결과가 기록을 지운다는 점이에요.
    #   (code, side) 로 중복을 없애되 **먼저 적힌 것을 남겨요.**
    #   ref_close 는 신호일 종가라 그날 안에서 바뀌지 않아야 하니까요.
    _dec_path = out_dir / ("decision_%s.csv" % d)
    _dec_new = pd.DataFrame(dec)
    if _dec_path.exists():
        try:
            _dec_old = pd.read_csv(_dec_path, encoding="utf-8-sig")
            if len(_dec_old):
                _n_new = len(_dec_new)
                _dec_new = pd.concat([_dec_old, _dec_new], ignore_index=True)
                _dec_new = _dec_new.drop_duplicates(subset=["code", "side"], keep="first")
                _kept = len(_dec_new) - _n_new
                if _kept > 0:
                    print("[DEC] 사이드카 병합: 기존 %d행 보존 (이번 빌드 %d행)"
                          % (_kept, _n_new), flush=True)
        except Exception as _e:
            print("[DEC][WARN] 기존 사이드카를 읽지 못해 그대로 씁니다: %s: %s"
                  % (type(_e).__name__, _e), flush=True)
    _dec_new.to_csv(_dec_path, index=False, encoding="utf-8-sig")

    summary = {
        "round": "RD_20260901_topn", "stage": 1, "date": d, "signal_date": sig_d, "arm": EXEC_ARM,
        "generated_at": pd.Timestamp.now().isoformat(),
        "held_before": int(len(pos)), "sells": int(len(sells)), "buys": int(len(buys)),
        "slots_free_before_buy": int(MAX_POS - len(held)),
        "orders_path": str(op), "capital": CAPITAL, "max_pos": MAX_POS, "hold_days": HOLD_DAYS,
        "note": "파일만 만든다. 발주는 kis_order_dispatch_from_exec.py 가 한다",
    }
    (out_dir / "orders_build_last.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")

    print("[BUILD] %s  보유 %d -> 매도 %d / 매수 %d  (빈칸 %d)"
          % (d, len(pos), len(sells), len(buys), MAX_POS - len(held)), flush=True)
    for c, q, lp, rk in buys:
        print("        BUY  %s qty=%-6d limit=%-8d rank=%d" % (c, q, lp, rk), flush=True)
    print("[OUT] %s  (행 %d)" % (op, len(orders)), flush=True)
    print("[NOTE] 이 도구는 주문을 내지 않는다", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
