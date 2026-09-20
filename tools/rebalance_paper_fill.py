# -*- coding: utf-8 -*-
"""리밸런싱 주문을 가상 체결시켜 상태와 원장을 갱신한다.

2026-08-24 신규. tools/rebalance_portfolio.py 가 만든 주문을 받아
가상매매 상태를 전진시킨다.

**상태를 분리한다**: paper/paper_state.json 에 쓰지 않는다.
거기에는 v41.1 의 포지션이 들어 있고, 섞으면 어느 전략이 무엇을 했는지 가릴 수 없다.
이 실행기는 2_Logs/rebalance/rebal_state.json 만 쓴다.

체결 가정(명시):
  체결가 = exec_date 종가.  목표 수량 계산에 쓴 가격과 같다.
  이유: 백테스트가 종가->종가 h10 수익으로 측정됐다. 가정을 맞춰야
        가상매매 결과와 백테스트를 비교할 수 있다.
  **이것은 낙관적 가정이다** - 실제로는 호가를 먹고 슬리피지가 붙는다.
  슬리피지는 2026-08-25 측정 실매매로 따로 잰다.
비용:
  매수 = 체결금액 x fee_pct
  매도 = 체결금액 x (fee_pct + sell_tax_pct)
  기본값은 승인된 비용 원장 기준 왕복 0.358% 에 맞춘다.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(r"E:\1_Data")
ARCHIVE = ROOT / "krx_daily_archive"
DEFAULT_DIR = ROOT / "2_Logs" / "rebalance"


def load_closes(as_of: str) -> pd.Series:
    parts = []
    for f in sorted(glob.glob(str(ARCHIVE / "*_clean.parquet"))):
        try:
            df = pd.read_parquet(f, columns=["date", "code", "close"])
        except Exception:
            continue
        df["date"] = df["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
        df = df[df["date"] == as_of]
        if len(df):
            parts.append(df)
    if not parts:
        raise RuntimeError("as_of=%s 종가를 찾지 못했다" % as_of)
    d = pd.concat(parts, ignore_index=True)
    d["code"] = d["code"].astype(str).str.zfill(6)
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d = d[d["close"] > 0].drop_duplicates(subset=["code"], keep="last")
    return d.set_index("code")["close"]


PRICE_HI, PRICE_LO = 1.305, 0.695     # 일간 가격제한 +-30% 밖 = 정상 등락이 아니다
LEVEL_HI, LEVEL_LO = 10.0, 0.1        # 1년 중앙 종가에서 10배 밖 = 데이터 결함


def load_price_basis(as_of: str) -> tuple[pd.Series, pd.Series]:
    """집행일 이전의 (직전 종가, 1년 중앙 종가).

    [2026-08-25] 처음엔 직전 종가만 봤는데 **못 잡았다.**
    000150 은 2026-05-21~06-15 내내 오염돼 있었고(5월 초 7,730원 / 정상 160만원대),
    오염이 며칠 이어지면 전일 대비는 멀쩡해 보인다. 오염된 구간 안에서 사고
    정상 구간에서 팔면 285배 가짜 차익이 원장에 박힌다.
    그래서 **수준**도 본다 - 1년 중앙 종가에서 10배 넘게 벗어난 체결가는 막는다.
    """
    parts = []
    for f in sorted(glob.glob(str(ARCHIVE / "*_clean.parquet"))):
        try:
            df = pd.read_parquet(f, columns=["date", "code", "close"])
        except Exception:
            continue
        df["date"] = df["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
        df = df[(df["date"] < as_of) & (df["date"] >= _minus_days(as_of, 400))]
        if len(df):
            parts.append(df)
    if not parts:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    d = pd.concat(parts, ignore_index=True)
    d["code"] = d["code"].astype(str).str.zfill(6)
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d = d[d["close"] > 0]
    med = d.groupby("code")["close"].median()
    prev = (d.sort_values("date").drop_duplicates(subset=["code"], keep="last")
              .set_index("code")["close"])
    return prev, med


def _minus_days(ymd: str, n: int) -> str:
    import datetime as _dt
    return (_dt.datetime.strptime(ymd, "%Y%m%d") - _dt.timedelta(days=n)).strftime("%Y%m%d")


def load_state(p: Path, capital: float) -> dict:
    if not p.exists():
        return {"cash": float(capital), "positions": {}, "history": [], "schema": "rebal_state_v1"}
    st = json.loads(p.read_text(encoding="utf-8-sig"))
    st.setdefault("cash", float(capital))
    st.setdefault("positions", {})
    st.setdefault("history", [])
    return st


def main() -> int:
    ap = argparse.ArgumentParser(description="리밸런싱 주문 가상 체결")
    ap.add_argument("--orders", required=True)
    ap.add_argument("--state", default=str(DEFAULT_DIR / "rebal_state.json"))
    ap.add_argument("--ledger", default=str(DEFAULT_DIR / "rebal_trades.csv"))
    ap.add_argument("--capital", type=float, default=100_000_000, help="상태가 없을 때 초기 현금")
    ap.add_argument("--fee-pct", type=float, default=0.00179, help="편도 수수료율. 왕복 0.358%%의 절반")
    ap.add_argument("--sell-tax-pct", type=float, default=0.0, help="매도세. 0 이면 fee 에 포함된 것으로 본다")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="이미 반영된 exec_date 를 다시 반영한다. 이중 체결을 만들 수 있다")
    args = ap.parse_args()

    od = pd.read_excel(args.orders, dtype=str)
    need = ["exec_date", "side", "code", "fill_qty"]
    miss = [c for c in need if c not in od.columns]
    if miss:
        print("[STOP] 주문 파일에 컬럼이 없다: %s" % miss)
        return 2
    od["code"] = od["code"].astype(str).str.zfill(6)
    od["side"] = od["side"].astype(str).str.upper().str.strip()
    od["qty"] = pd.to_numeric(od["fill_qty"], errors="coerce").fillna(0).astype(int)
    od = od[(od["side"].isin(["BUY", "SELL"])) & (od["qty"] > 0)]
    if od.empty:
        print("[REBAL_FILL] 체결할 주문이 없다")
        return 0

    exec_dates = sorted(od["exec_date"].astype(str).unique())
    if len(exec_dates) != 1:
        print("[STOP] exec_date 가 여러 개다: %s" % exec_dates)
        return 2
    d = exec_dates[0]

    # [2026-08-25] 종가가 아직 없는 것은 고장이 아니라 대기다.
    #   신호일 종가로 주문을 만들고 집행일 종가로 체결하므로(선견 없음),
    #   집행일 장 마감 전에 돌리면 반드시 여기에 걸린다. rc=3 으로 구분한다.
    try:
        closes = load_closes(d)
    except RuntimeError as e:
        print("[WAIT] %s" % e)
        print("  exec_date=%s 아카이브가 아직 안 들어왔다. 마감 후 다시 돌리면 된다." % d)
        return 3
    st = load_state(Path(args.state), args.capital)
    # [2026-08-25] 멱등성. 같은 집행일을 두 번 반영하면 그대로 이중 체결이다.
    #   재시도 루프(종가 대기 -> 재실행)에 걸 것이므로 반드시 필요하다.
    _done = {str(h.get("exec_date")) for h in (st.get("history") or [])}
    if args.apply and d in _done and not args.force:
        print("[SKIP] exec_date=%s 는 이미 반영돼 있다(history). --force 없이는 다시 넣지 않는다." % d)
        return 0

    pos: dict = dict(st["positions"])
    cash = float(st["cash"])

    # [2026-08-25] 체결가 정상성. 패널 결함이 원장에 들어오면 되돌릴 수 없다.
    #   시험 장부에서 000150 이 7,730 -> 2,203,000 (285배) 로 "팔려" 수익률 전체를 만들었다.
    prev, med = load_price_basis(d)

    filled = skipped = bad_price = 0
    buy_amt = sell_amt = fees = 0.0
    ledger_rows = []

    # 매도를 먼저 처리해 현금을 확보한다
    for side in ("SELL", "BUY"):
        for r in od[od["side"] == side].itertuples():
            px = float(closes.get(r.code, 0.0) or 0.0)
            if px <= 0:
                skipped += 1
                continue
            _bad = ""
            pp = float(prev.get(r.code, 0.0) or 0.0)
            if pp > 0:
                _r = px / pp
                if _r > PRICE_HI or _r < PRICE_LO:
                    _bad = "직전 %s (x%.2f)" % (format(int(pp), ","), _r)
            pm = float(med.get(r.code, 0.0) or 0.0)
            if not _bad and pm > 0:
                _r = px / pm
                if _r > LEVEL_HI or _r < LEVEL_LO:
                    _bad = "1년중앙 %s (x%.3f)" % (format(int(pm), ","), _r)
            if _bad:
                print("  [BAD_PRICE] %s %s  체결가 %s  vs %s  -> 체결하지 않는다"
                      % (side, r.code, format(int(px), ","), _bad))
                bad_price += 1
                skipped += 1
                continue
            qty = int(r.qty)
            if side == "SELL":
                have = int(pos.get(r.code, 0))
                qty = min(qty, have)
                if qty <= 0:
                    skipped += 1
                    continue
                gross = px * qty
                fee = gross * (args.fee_pct + args.sell_tax_pct)
                cash += gross - fee
                sell_amt += gross
                pos[r.code] = have - qty
                if pos[r.code] == 0:
                    pos.pop(r.code, None)
            else:
                gross = px * qty
                fee = gross * args.fee_pct
                if cash < gross + fee:
                    afford = int(cash // (px * (1 + args.fee_pct)))
                    if afford <= 0:
                        skipped += 1
                        continue
                    qty = afford
                    gross = px * qty
                    fee = gross * args.fee_pct
                cash -= gross + fee
                buy_amt += gross
                pos[r.code] = int(pos.get(r.code, 0)) + qty
            fees += fee
            filled += 1
            ledger_rows.append({
                "exec_date": d, "side": side, "code": r.code, "qty": qty,
                "price": round(px, 2), "gross": round(gross, 2), "fee": round(fee, 2),
                "note": getattr(r, "note", ""),
            })

    # [2026-08-25] 그날 호가가 없는 종목(거래정지 등)을 0원으로 세면 자산이 과소 계상된다.
    #   실측: 20260415 시험 장부에서 8종목 1,775,230원이 통째로 0 이 됐다.
    #   거래정지는 무가치가 아니다. 마지막 종가로 평가한다.
    def _mark(c: str) -> float:
        v = float(closes.get(c, 0.0) or 0.0)
        return v if v > 0 else float(prev.get(c, 0.0) or 0.0)

    mv = sum(int(q) * _mark(c) for c, q in pos.items())
    _nq = sum(1 for c in pos if not (float(closes.get(c, 0.0) or 0.0) > 0))
    equity = cash + mv
    print("[REBAL_FILL] exec_date=%s  주문 %d -> 체결 %d, 건너뜀 %d" % (d, len(od), filled, skipped))
    if bad_price:
        print("  [주의] 가격제한 밖 종가라 체결을 막은 종목 %d개 (분할·병합 또는 데이터 결함)" % bad_price)
    print("  매수 %s원   매도 %s원   수수료·세금 %s원"
          % (format(int(buy_amt), ","), format(int(sell_amt), ","), format(int(fees), ",")))
    if _nq:
        print("  [주의] 그날 종가가 없어 직전 종가로 평가한 보유 %d종목" % _nq)
    print("  보유 %d종목  평가액 %s원  현금 %s원  자산 %s원 (%.2f%%)"
          % (len(pos), format(int(mv), ","), format(int(cash), ","),
             format(int(equity), ","), 100.0 * equity / max(args.capital, 1)))

    if not args.apply:
        print("  dry-run. --apply 를 주면 상태와 원장을 갱신한다.")
        return 0

    st["cash"] = cash
    st["positions"] = pos
    st["history"] = (st.get("history") or []) + [{
        "exec_date": d, "filled": filled, "skipped": skipped,
        "buy": int(buy_amt), "sell": int(sell_amt), "fees": int(fees),
        "cash": int(cash), "mv": int(mv), "equity": int(equity),
    }]
    sp = Path(args.state)
    sp.parent.mkdir(parents=True, exist_ok=True)
    sp.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")

    lp = Path(args.ledger)
    new = not lp.exists()
    with lp.open("a", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=list(ledger_rows[0].keys()))
        if new:
            w.writeheader()
        w.writerows(ledger_rows)
    print("  wrote %s" % sp)
    print("  appended %s (%d행)" % (lp, len(ledger_rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
