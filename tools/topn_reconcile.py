# -*- coding: utf-8 -*-
"""RD_20260901_topn 1단계 - 체결 대조 + 보유 원장 갱신 + A3/A4/A5 산출

사전등록  docs/references/PREREG_RD_20260901_TOPN.md

왜 필요한가
  topn_build_orders.py 는 positions.csv 를 **읽기만** 했다. 쓰는 코드가 없었다.
  그대로 두면 매일 같은 6종목을 새로 사려 하고 held_days 가 안 늘어 매도가 영원히 안 나간다.
  (2026-09-02 발견, PLANS (191))

하는 일
  1) 우리 주문만 골라낸다        submit log 의 note 가 "RD_TOPN_STAGE1" 로 시작
  2) 체결을 붙인다              paper/fills.csv 를 (code, side, 날짜)로 매칭.
                              order_id 가 있으면 그것을 우선한다
  3) positions.csv 를 갱신한다   held_days +1 / 매도체결 제거 / 매수체결 추가
  4) A3/A4/A5 를 낸다

멱등성  같은 날짜로 두 번 돌려도 held_days 가 두 번 늘지 않는다
       (positions.csv 의 last_update 와 대조)

주문 없음. 읽고 원장만 쓴다. 2_Logs/topn/ 밖에는 쓰지 않는다.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
OUT_DIR = ROOT / "2_Logs" / "topn"
POS_PATH = OUT_DIR / "positions.csv"
FILLS_PATH = ROOT / "paper" / "fills.csv"

TAG = "RD_TOPN_STAGE1"
MAX_POS = 6
POS_COLS = ["code", "entry_date", "qty", "entry_price", "held_days", "last_update"]


def _norm_ymd(x) -> str:
    s = "".join(ch for ch in str(x) if ch.isdigit())
    return s[:8]


def _read_csv_safe(path: Path, **kw) -> pd.DataFrame:
    """빈 CSV / 파싱 실패를 빈 프레임으로 흡수한다.

    [2026-09-03] 빈 submit log 에서 pd.read_csv 가 EmptyDataError 를 던져
    reconcile 이 중단됐고, 그 탓에 positions.csv 가 갱신되지 않아
    **held_days 가 영구히 멈추고 13일째 매도가 영원히 안 나갔다.**
    발주 0건인 날이면 생산에서 그대로 재현된다. 조용히 넘어가지는 않는다.
    """
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, **kw)
    except Exception as e:
        print("[EMPTY] %s 를 읽지 못했다 (%s: %s). 빈 것으로 취급한다"
              % (path.name, type(e).__name__, e), flush=True)
        return pd.DataFrame()


def load_positions() -> pd.DataFrame:
    df = _read_csv_safe(POS_PATH, dtype={"code": str}, encoding="utf-8-sig")
    if len(df.columns):
        for c in POS_COLS:
            if c not in df.columns:
                df[c] = ""
        df["code"] = df["code"].astype(str).str.zfill(6)
        return df[POS_COLS]
    return pd.DataFrame(columns=POS_COLS)


def load_our_orders(d: str, mode: str) -> pd.DataFrame:
    p = OUT_DIR / ("topn_orders_%s_broker_submit_%s.csv" % (d, mode))
    if not p.exists():
        return pd.DataFrame()
    df = _read_csv_safe(p, dtype=str)
    if "note" not in df.columns:
        return pd.DataFrame()
    df = df[df["note"].astype(str).str.startswith(TAG)].copy()
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)

    # [2026-09-03] **시도가 아니라 의도 단위로 센다.**
    #   장중에 10분마다 재시도하므로 차단된 시도가 submit log 에 매 주기 쌓인다
    #   (실측: 4주기 만에 6건 -> 24건, 하루 38주기면 228건).
    #   A4 = 체결/발주 의 분모를 시도 수로 잡으면 **체결률이 구조적으로 0 에 수렴한다.**
    #   우리 설계상 하루에 (code, side) 당 의도는 하나다. 마지막 시도의 상태를 남긴다.
    if "dispatch_ts" in df.columns:
        df = df.sort_values("dispatch_ts")
    n_raw = len(df)
    df = df.drop_duplicates(subset=["code", "side"], keep="last")
    if n_raw != len(df):
        print("[INTENT] 시도 %d건 -> 의도 %d건 (재시도 누적 제거)" % (n_raw, len(df)), flush=True)
    return df


def load_our_order_ids(d: str, mode: str) -> set:
    """그날 우리가 받은 브로커 주문번호 전부.

    [2026-09-08] load_our_orders() 의 결과를 쓰면 안 된다. 그쪽은 (code, side) 로
      keep="last" 중복 제거를 하는데, 장중 재시도 때문에 마지막 행은 거의 항상
      SKIP_ALREADY_DISPATCHED 이고 **ord_no 가 비어 있다**.
      실측 20260908: 78행 중 주문번호가 실린 행은 4개(10:02 3건, 10:50 1건)이고
      전부 중간 행이라 dedup 후에는 하나도 남지 않는다.
      귀속은 원본 로그 전체에서 모아야 한다.
    """
    p = OUT_DIR / ("topn_orders_%s_broker_submit_%s.csv" % (d, mode))
    df = _read_csv_safe(p, dtype=str)
    if not len(df) or "ord_no" not in df.columns:
        return set()
    if "note" in df.columns:
        df = df[df["note"].astype(str).str.startswith(TAG)]
    out = set()
    for x in df["ord_no"].tolist():
        s = str(x).strip()
        if s and s.lower() != "nan":
            out.add(s.lstrip("0") or "0")
    return out


def load_fills_broker(d: str, mode: str) -> pd.DataFrame:
    """브로커 일별 체결조회에서 **우리 주문번호의 체결만** 돌려준다.

    [2026-09-08] 왜 브로커를 직접 보나. 기존 입력 paper/fills.csv 는 v41.1 페이퍼
      엔진이 쓰는 시뮬레이션 체결 원장이라, 디스패처가 브로커로 직접 보낸 topn 주문의
      체결은 **구조적으로 거기 들어갈 수가 없다.** 그 파일은 2026-08-25 이후 갱신도
      멈춰 있었다. 그래서 오늘 4건이 전부 체결됐는데도 산출물은 체결 0 / 보유 0 이었고,
      사전등록 지표 A3/A4/A5 가 통째로 무효였다. 판정 지표가 자기 체결을 못 보고 있었다.

    귀속은 **주문번호로만** 한다. 같은 모의계좌를 v41.1 경로도 쓰므로 (code, side) 로
      맞추면 남의 체결이 섞인다. 번호가 없는 체결은 우리 것이 아니다.
    """
    ids = load_our_order_ids(d, mode)
    if not ids:
        print("[ATTR] 그날 받은 주문번호가 0개다 -> 귀속 0건", flush=True)
        return pd.DataFrame(columns=["code", "side", "qty", "price"])

    sys.path.insert(0, str(ROOT / "tools"))
    from kis_order_client import KISOrderClient  # noqa: E402

    client = KISOrderClient.from_env(mock=("true" if mode == "mock" else "false"))
    rsp = client.inquire_daily_ccld(start_ymd=d, end_ymd=d, ccld_dvsn="01")
    rows = rsp.get("rows") or []
    recs = []
    for r in rows:
        odno = str(r.get("odno", "")).strip().lstrip("0") or "0"
        if odno not in ids:
            continue
        q = pd.to_numeric(r.get("tot_ccld_qty"), errors="coerce")
        if not np.isfinite(q) or int(q) <= 0:
            continue
        side = "BUY" if str(r.get("sll_buy_dvsn_cd", "")).strip() == "02" else "SELL"
        recs.append({
            "code": str(r.get("pdno", "")).zfill(6),
            "side": side,
            "qty": int(q),
            "price": float(pd.to_numeric(r.get("avg_prvs"), errors="coerce") or 0.0),
            "order_id": odno,
        })
    print("[ATTR] 브로커 체결 %d건 중 주문번호로 %d건 귀속 (우리 번호 %d개)"
          % (len(rows), len(recs), len(ids)), flush=True)
    return pd.DataFrame(recs, columns=["code", "side", "qty", "price", "order_id"])


def load_fills(d: str, orders: pd.DataFrame) -> pd.DataFrame:
    """그날 체결 중 **우리 주문에서 나온 것만** 돌려준다.

    [2026-09-03] 예전 구현은 note 태그가 없으면 그날 전체 체결을 반환했다.
    그런데 fills.csv 의 note 에는 우리 태그가 0건이고 v41.1 것만 있다
    (exit_reason=TIME / beta_harvest;... 1,011행 전수 확인).
    v41.1 이 같은 날 체결하면 **그 체결이 하네스 것으로 잡혀** positions.csv 와
    A4/A5 가 통째로 오염된다. 전체 폴백을 없앤다.
    """
    if not FILLS_PATH.exists():
        return pd.DataFrame()
    df = _read_csv_safe(FILLS_PATH, dtype=str)
    if "datetime" not in df.columns:
        return pd.DataFrame()
    df["_ymd"] = df["datetime"].apply(_norm_ymd)
    df = df[df["_ymd"] == d].copy()
    if df.empty:
        return df
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)

    # 1) 태그가 실려 있으면 그게 가장 확실하다
    if "note" in df.columns:
        tagged = df[df["note"].astype(str).str.contains(TAG, na=False)]
        if len(tagged):
            print("[ATTR] note 태그로 %d건 귀속" % len(tagged), flush=True)
            return tagged

    # 2) 브로커 주문번호로 맞춘다
    ords = set()
    if len(orders) and "ord_no" in orders.columns:
        ords = {str(x).strip() for x in orders["ord_no"].tolist()
                if str(x).strip() and str(x).strip().lower() != "nan"}
    if ords and "order_id" in df.columns:
        by_id = df[df["order_id"].astype(str).str.strip().isin(ords)]
        if len(by_id):
            print("[ATTR] order_id 로 %d건 귀속" % len(by_id), flush=True)
            return by_id

    # 3) 우리가 그날 낸 (code, side) 로만 한정한다. **전체 폴백은 하지 않는다**
    if not len(orders):
        print("[ATTR] 우리 주문이 없다 -> 귀속 0건 (전체 체결로 폴백하지 않는다)", flush=True)
        return df.iloc[0:0]
    pairs = set(zip(orders["code"].astype(str).str.zfill(6),
                    orders["side"].astype(str).str.upper().str.strip()))
    mine = df[[(c, s) in pairs for c, s in zip(df["code"], df["side"])]]
    print("[ATTR] (code,side) 로 %d건 귀속 / 그날 전체 %d건" % (len(mine), len(df)), flush=True)
    return mine


def load_broker_holdings(mode: str) -> dict:
    """브로커 실보유를 {code: qty} 로 돌려줘요. 조회 실패면 None 을 돌려요.

    [2026-09-09] 왜 필요한가.
      보유 원장을 **델타 산술**로 유지하고 있었어요 - 매수 체결분을 더하고 매도 체결분을 빼는 식이요.
      그런데 뺄셈은 멱등이 아니라서 같은 날 두 번 돌리면 두 번 빠져요. 그래서 `already`
      분기에서 매도 반영을 통째로 건너뛰었고, 그 결과가 이랬어요:
        - 부분매도가 행을 통째로 지움 (272)
        - 그날 첫 reconcile 이후의 체결이 영원히 안 들어옴 (281)
      델타를 고치는 대신 **상태를 브로커에 맞추면** 이 계열이 통째로 사라져요.

    다만 통째로 흡수하면 안 돼요. 이 모의계좌는 v41.1 경로도 같이 쓰거든요
    (체결 귀속을 주문번호로만 하는 이유가 그거예요). 그래서 호출부는 **이미 원장에 있는
    종목의 수량만** 이 값으로 맞추고, 새 종목은 우리 주문번호로 귀속된 체결로만 들여요.
    """
    try:
        sys.path.insert(0, str(ROOT / "tools"))
        from kis_order_client import KISOrderClient  # noqa: E402

        client = KISOrderClient.from_env(mock=("true" if mode == "mock" else "false"))
        rsp = client.inquire_balance_positions()
        rows = rsp.get("rows") or rsp.get("output1") or []
        out = {}
        for r in rows:
            code = str(r.get("pdno", "")).zfill(6)
            raw = str(r.get("hldg_qty") or r.get("qty") or 0).replace(",", "")
            try:
                q = int(float(raw))
            except Exception:
                continue
            if code and q > 0:
                out[code] = q
        return out
    except Exception as e:
        print("[HOLD] 브로커 잔고 조회 실패: %s: %s" % (type(e).__name__, e), flush=True)
        return None


def main() -> int:
    # [2026-09-03] --out-dir 는 **이 모듈의 모든 경로**를 지배해야 한다.
    #   POS_PATH 만 바꿨더니 load_our_orders 가 여전히 모듈 상수 OUT_DIR 을 읽어
    #   시험 실행이 생산 submit log 를 찾았다(예행에서 '주문 0건'으로 드러남).
    #   **부분적으로만 반영되는 격리는 격리가 아니다.**
    #   argparse default 가 OUT_DIR 를 먼저 읽으므로 선언은 반드시 함수 첫 줄이어야 한다.
    global POS_PATH, OUT_DIR, FILLS_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="", help="YYYYMMDD. 비우면 decision 파일의 최신일")
    ap.add_argument("--mode", default="mock", choices=["mock", "prod"])
    ap.add_argument("--out-dir", default=str(OUT_DIR))
    ap.add_argument("--fills-path", default="",
                    help="체결 원본 경로. 비우면 paper/fills.csv. 격리 시험용")
    ap.add_argument("--fills-source", default="broker", choices=["broker", "paper"],
                    help="체결 입력. broker=브로커 체결조회(기본), paper=paper/fills.csv(구 경로)")
    ap.add_argument("--positions-source", default="broker", choices=["broker", "delta"],
                    help="보유 수량. broker=브로커 잔고에 맞춘다(기본), delta=체결 델타 산술(구 경로)")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    OUT_DIR = out_dir
    POS_PATH = out_dir / "positions.csv"
    if args.fills_path.strip():
        FILLS_PATH = Path(args.fills_path.strip())
        print("[FILLS] 원본 지정: %s" % FILLS_PATH, flush=True)

    d = args.date.strip()
    if not d:
        cands = sorted(out_dir.glob("decision_*.csv"))
        if not cands:
            print("[STOP] decision_*.csv 가 없다. topn_build_orders.py 를 먼저 돌려라")
            return 2
        d = cands[-1].stem.split("_")[-1]

    dec_p = out_dir / ("decision_%s.csv" % d)
    dec = _read_csv_safe(dec_p, dtype={"code": str}, encoding="utf-8-sig")
    if len(dec):
        dec["code"] = dec["code"].astype(str).str.zfill(6)

    pos = load_positions()
    already = bool(len(pos)) and (pos["last_update"].astype(str) == d).all()
    orders = load_our_orders(d, args.mode)
    if args.fills_source == "broker" and not args.fills_path.strip():
        # 실패를 삼키지 않는다. 조용히 paper 로 폴백하면 오늘(2026-09-08)과 똑같이
        # "체결 0" 을 정상처럼 보고하게 된다. 그 침묵이 이 결함의 본체였다.
        try:
            fills = load_fills_broker(d, args.mode)
        except Exception as e:
            print("[FAIL] 브로커 체결조회 실패 (%s: %s). 원장을 갱신하지 않는다."
                  % (type(e).__name__, e), flush=True)
            return 2
    else:
        print("[FILLS] source=paper (%s)" % FILLS_PATH, flush=True)
        fills = load_fills(d, orders)

    # --- 체결 집계 (code, side) 단위 ---
    filled = {}
    if len(fills):
        for (c, s), g in fills.groupby(["code", "side"]):
            q = int(g["qty"].sum())
            px = float((g["qty"] * g["price"]).sum() / q) if q else 0.0
            filled[(c, s)] = (q, px)

    # --- 보유 원장 갱신 ---
    # [2026-09-09] 예전 구현은 `already` 일 때 **체결 반영까지 통째로 건너뛰었다.**
    #   의도는 "held_days 를 하루에 두 번 늘리지 않는다" 하나인데 구현이 넓었다.
    #   그래서 **그날 첫 reconcile 이후에 체결된 건은 원장에 영원히 들어오지 않았다.**
    #   실측 2026-09-09: 13:35 에 낸 017900 매수 2,025주가 14:1x 에 전량 체결됐는데
    #     브로커 6종목 / positions.csv 5종목 - 017900 이 원장에 없었다.
    #   아침에 고친 부분매도 결함과 같은 계열이다(의도는 좁고 구현은 넓다).
    #   원장이 틀리면 내일 빈자리 계산이 틀리고 그게 발주를 바꾼다.
    #
    #   그래서 둘을 분리한다. held_days 증가만 하루 한 번으로 막고,
    #   **매수 체결분 추가는 항상 적용한다** - 아래 로직이 이미 멱등이다
    #   (그 코드가 원장에 있으면 건너뛴다).
    #   매도 차감은 델타를 빼는 방식이라 멱등이 아니므로 `already` 일 때는 하지 않는다.
    #   그 한계는 아래 [남은 것] 주석에 적어 둔다.
    new_pos = pos.copy()
    if already:
        print("[IDEMPOTENT] positions 가 이미 %s 로 갱신돼 있다. held_days 를 다시 늘리지 않는다 "
              "(매수 체결분은 계속 반영한다)" % d, flush=True)
    else:
        if len(new_pos):
            new_pos["held_days"] = pd.to_numeric(new_pos["held_days"], errors="coerce").fillna(0).astype(int) + 1
        # 매도 체결분 차감
        # [2026-09-09] 예전 구현은 `isin(sold)` 로 **행을 통째로 지웠다.**
        #   하네스 전략은 항상 전량 매도라 그동안 드러나지 않았는데,
        #   09-09 교정 매도(003230 27주 중 15주)에서 실제로 틀렸다:
        #     브로커 12주 보유  vs  positions.csv 에서 003230 행 소멸
        #     -> 보유 5 가 4 로 읽혀 빈자리가 1 에서 2 로 늘고,
        #        주문 파일이 매수 1건에서 2건으로 다시 만들어졌다 (실측)
        #   원장 오류가 **그날의 매매 결정을 바꿨다.** 그래서 수량을 뺀다.
        sold_qty = {c: int(q) for (c, s), (q, _px) in filled.items() if s == "SELL" and int(q) > 0}
        if args.positions_source == "broker":
            sold_qty = {}   # 아래에서 브로커 잔고로 맞추므로 델타 뺄셈을 하지 않아요
        if sold_qty and len(new_pos):
            new_pos["qty"] = pd.to_numeric(new_pos["qty"], errors="coerce").fillna(0).astype(int)
            for _code, _q in sold_qty.items():
                _m = new_pos["code"].astype(str) == str(_code)
                if bool(_m.any()):
                    _before = int(new_pos.loc[_m, "qty"].iloc[0])
                    new_pos.loc[_m, "qty"] = _before - int(_q)
                    print("[SELL] %s %d주 체결 -> 보유 %d - %d = %d"
                          % (_code, int(_q), _before, int(_q), _before - int(_q)), flush=True)
            # 잔량이 남으면 보유를 유지한다. 0 이하일 때만 행이 사라진다.
            new_pos = new_pos[new_pos["qty"] > 0]

    # 매수 체결분 추가 - **`already` 여부와 무관하게 항상 적용해요.**
    #   이미 원장에 있는 코드는 건너뛰므로 여러 번 돌려도 결과가 같아요(멱등).
    add = []
    for (c, s), (q, px) in filled.items():
        if s != "BUY" or q <= 0:
            continue
        if len(new_pos) and c in set(new_pos["code"].astype(str)):
            continue
        print("[BUY] %s %d주 체결 -> 원장에 추가 (평단 %s)" % (c, int(q), px), flush=True)
        add.append({"code": c, "entry_date": d, "qty": q, "entry_price": px,
                    "held_days": 0, "last_update": d})
    if add:
        new_pos = pd.concat([new_pos, pd.DataFrame(add)], ignore_index=True)

    # --- 브로커 잔고에 수량을 맞춰요 (positions-source=broker) ---
    #   델타 산술을 상태 대조로 바꾸는 부분이에요. 뺄셈이 사라지므로 멱등해지고,
    #   부분매도·늦은 체결·수동 교정이 전부 자동으로 반영돼요.
    #   **원장에 있는 종목만 맞춰요.** 이 계좌는 v41.1 경로도 같이 쓰므로
    #   모르는 종목을 흡수하면 A3 와 빈자리 계산이 오염돼요.
    if args.positions_source == "broker" and len(new_pos):
        hold = load_broker_holdings(args.mode)
        if hold is None:
            print("[HOLD] 조회 실패 -> 수량을 그대로 둬요 (델타 결과 유지)", flush=True)
        else:
            new_pos["qty"] = pd.to_numeric(new_pos["qty"], errors="coerce").fillna(0).astype(int)
            for _i in list(new_pos.index):
                _c = str(new_pos.at[_i, "code"]).zfill(6)
                _cur = int(new_pos.at[_i, "qty"])
                _bq = int(hold.get(_c, 0))
                if _bq != _cur:
                    print("[HOLD] %s 원장 %d -> 브로커 %d 로 맞춰요" % (_c, _cur, _bq), flush=True)
                    new_pos.at[_i, "qty"] = _bq
            _gone = new_pos[new_pos["qty"] <= 0]["code"].astype(str).tolist()
            if _gone:
                print("[HOLD] 브로커에 없어 원장에서 제거해요: %s" % ", ".join(_gone), flush=True)
            new_pos = new_pos[new_pos["qty"] > 0]
            _unknown = sorted(set(hold) - set(new_pos["code"].astype(str).str.zfill(6)))
            if _unknown:
                print("[HOLD] 브로커에만 있는 종목 %d개 (흡수하지 않아요 - 우리 주문이 아니거나 "
                      "미귀속): %s" % (len(_unknown), ", ".join(_unknown[:10])), flush=True)

    if len(new_pos):
        new_pos["last_update"] = d
    new_pos = new_pos[POS_COLS] if len(new_pos) else pd.DataFrame(columns=POS_COLS)
    # [2026-09-03] **내용이 같으면 쓰지 않는다.**
    #   무조건 다시 쓰면 mtime 이 매번 갱신되고, 그러면 이 파일의 나이를 근거로 삼는
    #   다른 판정(topn_build_orders 의 신선도 SKIP)이 영원히 "변경됨"이 된다.
    #   실측: 그 탓에 10분마다 118초짜리 패널 로딩이 그대로 돌았다.
    #
    #   개행도 명시적으로 고정한다. to_csv 는 '\r\n' 을 돌려주는데 Path.write_text 가
    #   거기에 또 변환을 걸어 디스크에는 '\r\r\n' 이 되고, 읽으면 '\n\n' 이라
    #   **영원히 불일치**한다 (2026-09-03 실측).
    csv_new = new_pos.to_csv(index=False, lineterminator="\n")
    csv_old = None
    if POS_PATH.exists():
        with POS_PATH.open("r", encoding="utf-8-sig", newline="") as fh:
            csv_old = fh.read()
    if csv_old is None or csv_old != csv_new:
        with POS_PATH.open("w", encoding="utf-8-sig", newline="") as fh:
            fh.write(csv_new)
        print("[POS] positions.csv 갱신 (%d행)" % len(new_pos), flush=True)
    else:
        print("[POS] 변경 없음 - positions.csv 를 다시 쓰지 않는다", flush=True)

    # --- A3 / A4 / A5 ---
    n_pos = int(len(new_pos))
    sub_q = int(orders["qty"].sum()) if len(orders) else 0
    sub_n = int(len(orders))
    fil_q = int(sum(q for (_, s), (q, _) in filled.items() if s in ("BUY", "SELL")))
    fil_n = int(len(filled))
    a4 = (fil_q / sub_q) if sub_q else None

    # A5 (사전등록 정의) - 기준가는 **신호일 종가(ref_close)** 예요. 이 정의는 건드리지 않아요.
    slip = []
    if len(dec) and filled:
        ref = dict(zip(zip(dec["code"], dec["side"]), pd.to_numeric(dec["ref_close"], errors="coerce")))
        for (c, s), (q, px) in filled.items():
            r = ref.get((c, s))
            if r and np.isfinite(r) and r > 0 and px > 0:
                sgn = 1.0 if s == "BUY" else -1.0
                slip.append(sgn * (px - r) / r)
    a5 = float(np.median(slip)) if slip else None

    # [2026-09-09] **관측 추가. 판정은 바꾸지 않아요.**
    #   사전등록 A5 합격선은 <=0.50% 인데, 같은 문서의 매수 가격 규칙이
    #   `limit = 신호일 종가 x 1.005` + 호가단위 올림이에요.
    #   그러면 **지정가에 정확히 체결돼도 슬리피지가 정의상 +0.5% 이상**이에요.
    #   실측 7개 종가로 확인: 5개 구조적 초과, 2개 정확히 경계.
    #   즉 A5 는 신호 품질과 무관하게 통과할 수 없어요. 두 규칙이 서로 모순이에요.
    #
    #   사전등록서는 "실행 후 고치면 사전등록이 아니다" 라고 못박고 있어서 고치지 않아요.
    #   대신 **지정가 대비 체결 편차**를 별도 필드로 함께 내요.
    #   이 값이 재는 것은 "우리가 정한 가격을 집행이 지켰는가" 이고,
    #   신호일 종가 대비 비용은 A5 가 아니라 비용 모델이 이미 재고 있어요.
    #   상세: PLANS (284)
    slip_lim = []
    if len(dec) and filled and "limit_price" in dec.columns:
        lim = dict(zip(zip(dec["code"], dec["side"]),
                       pd.to_numeric(dec["limit_price"], errors="coerce")))
        for (c, s), (q, px) in filled.items():
            r = lim.get((c, s))
            if r and np.isfinite(r) and r > 0 and px > 0:
                sgn = 1.0 if s == "BUY" else -1.0
                slip_lim.append(sgn * (px - r) / r)
    a5_lim = float(np.median(slip_lim)) if slip_lim else None

    blocked = ""
    if len(orders) and "dispatch_status" in orders.columns:
        vc = orders["dispatch_status"].astype(str).value_counts()
        blocked = "; ".join("%s=%d" % (k, v) for k, v in vc.items())

    status = {
        "round": "RD_20260901_topn", "stage": 1, "date": d, "mode": args.mode,
        "generated_at": pd.Timestamp.now().isoformat(),
        "orders_submitted": sub_n, "orders_qty": sub_q,
        "fills_pairs": fil_n, "fills_qty": fil_q,
        "A3_positions": n_pos, "A3_max_pos": MAX_POS,
        "A3_full": bool(n_pos >= MAX_POS),
        "A4_fill_rate": a4,
        "A5_slippage_median": a5,
        # 관측 전용. 사전등록 판정에 쓰지 않아요 (PLANS (284))
        "A5_vs_limit_median": a5_lim,
        "A5_note": ("A5 기준가는 신호일 종가예요. 매수 지정가 규칙이 종가x1.005 + 호가단위 올림이라 "
                    "지정가에 정확히 체결돼도 +0.5% 이상이 나와요. 합격선 0.50% 와 구조적으로 "
                    "모순이며 신호 품질과 무관해요. A5_vs_limit_median 은 지정가 대비 편차예요."),
        "dispatch_status": blocked,
        "idempotent_skip": bool(already),
        "note": "A4/A5 는 실제 발주(--apply)가 있어야 의미가 있다. dry-run 에서는 0 이 정상이다",
    }
    (out_dir / ("exec_%s.json" % d)).write_text(
        json.dumps(status, ensure_ascii=False, indent=1), encoding="utf-8")

    print("[RECONCILE] %s mode=%s" % (d, args.mode), flush=True)
    print("  주문 %d건 %d주 / 체결 %d쌍 %d주" % (sub_n, sub_q, fil_n, fil_q), flush=True)
    print("  A3 보유 %d / %d  %s" % (n_pos, MAX_POS, "FULL" if n_pos >= MAX_POS else ""), flush=True)
    print("  A4 체결률 %s" % ("%.1f%%" % (100 * a4) if a4 is not None else "n/a (발주 없음)"), flush=True)
    print("  A5 슬리피지 중앙 %s  (신호일 종가 기준. 합격선 0.50%% 와 구조적 모순 - PLANS 284)"
          % ("%+.4f%%" % (100 * a5) if a5 is not None else "n/a"), flush=True)
    print("     지정가 대비 편차 %s  (관측 전용, 판정 아님)"
          % ("%+.4f%%" % (100 * a5_lim) if a5_lim is not None else "n/a"), flush=True)
    if blocked:
        print("  발주 상태: %s" % blocked, flush=True)
    print("[OUT] %s" % (out_dir / ("exec_%s.json" % d)), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
