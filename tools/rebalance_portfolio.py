# -*- coding: utf-8 -*-
"""자본 제약 동일가중 리밸런싱 목표 포트폴리오와 주문 목록 산출.

2026-08-24 신규. v41.1 의 후보 파이프라인(9축 점수 -> 상위 소수 선별)과 반대로,
**vol60 상위 X% 를 배제하고 나머지를 전부 동일가중으로 담는다.**
기존 paper_engine 은 max_positions=18 / max_new=15 / max_hold_days=8 이라
이 형태를 표현할 수 없어 옆에 새로 만든다.

집행층은 검증된 것을 그대로 쓴다 - 출력이 tools/kis_order_dispatch_from_exec.py 가
읽는 orders 스키마(exec_date, side, code, fill_qty)와 같다.

자본 제약 고정점:
    슬롯 = 자본 / 종목수,  종목 = 주가 <= 슬롯
    둘이 서로를 정하므로 수렴할 때까지 반복한다(보통 4회).
    이 필터가 없으면 주가가 슬롯보다 비싼 종목이 qty=0 으로 조용히 빠지고,
    2026-08-24 실측으로 그것이 11.6%(65종목), 현금 놀림 20% 였다.

**안전**: 기본 출력 경로는 2_Logs/rebalance 다. paper/orders_*_exec.xlsx 로 쓰지 않는다 -
tools/kis_canary_run.py 가 그 패턴의 최신 파일을 자동으로 집어 디스패치하기 때문이다.
--apply 없이는 어떤 파일도 쓰지 않는다.
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"E:\1_Data")
ARCHIVE = ROOT / "krx_daily_archive"
DEFAULT_STATE = ROOT / "paper" / "paper_state.json"
DEFAULT_OUT = ROOT / "2_Logs" / "rebalance"


def load_panel() -> pd.DataFrame:
    parts = []
    for f in sorted(glob.glob(str(ARCHIVE / "*_clean.parquet"))):
        try:
            parts.append(pd.read_parquet(f, columns=["date", "code", "close", "value"]))
        except Exception:
            pass
    if not parts:
        raise RuntimeError("krx_daily_archive 에서 읽은 것이 없다")
    d = pd.concat(parts, ignore_index=True)
    d["date"] = d["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    d["code"] = d["code"].astype(str).str.zfill(6)
    for c in ("close", "value"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d[d["close"] > 0].drop_duplicates(subset=["date", "code"], keep="last")
    return d.sort_values(["code", "date"]).reset_index(drop=True)


def add_vol60(d: pd.DataFrame) -> pd.DataFrame:
    g = d.groupby("code")
    r = d["close"] / g["close"].shift(1) - 1.0
    d["vol60"] = r.groupby(d["code"]).transform(
        lambda s: s.rolling(60, min_periods=45).std()
    ) * 100
    return d


def fixed_point_slot(close: pd.Series, capital: float, max_iter: int = 12) -> float:
    """슬롯과 종목수가 서로를 정하므로 수렴시킨다."""
    n = int(len(close))
    for _ in range(max_iter):
        if n <= 0:
            return float(capital)
        slot = capital / n
        n2 = int((close <= slot).sum())
        if n2 == n or n2 == 0:
            break
        n = n2
    return capital / max(n, 1)


def build_target(d: pd.DataFrame, as_of: str, capital: float, exclude_pct: float,
                 min_value: float) -> tuple[pd.DataFrame, dict]:
    s = d[d["date"] == as_of].dropna(subset=["vol60"]).copy()
    s = s[s["value"] >= min_value]
    if len(s) < 50:
        raise RuntimeError("as_of=%s 적격 종목이 %d개뿐이다" % (as_of, len(s)))
    n_eligible = len(s)
    s["pv"] = s["vol60"].rank(pct=True)
    base = s[s["pv"] <= 1.0 - exclude_pct].copy()
    slot = fixed_point_slot(base["close"], capital)
    keep = base[base["close"] <= slot].copy()
    keep["target_qty"] = (slot / keep["close"]).astype(int)
    keep = keep[keep["target_qty"] > 0]
    meta = {
        "as_of": as_of,
        "capital": int(capital),
        "exclude_pct": exclude_pct,
        "min_value": int(min_value),
        "eligible": n_eligible,
        "after_vol_exclude": int(len(base)),
        "excluded_high_price": int(len(base) - len(keep)),
        "target_count": int(len(keep)),
        "slot_krw": int(slot),
        "target_notional": int((keep["target_qty"] * keep["close"]).sum()),
    }
    meta["cash_left"] = int(capital - meta["target_notional"])
    return keep[["code", "close", "value", "vol60", "target_qty"]], meta


def load_holdings(state_path: Path) -> pd.DataFrame:
    if not state_path.exists():
        return pd.DataFrame(columns=["code", "qty"])
    try:
        st = json.loads(state_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise RuntimeError("상태 파일을 읽지 못했다: %s" % exc)
    rows = []
    for p in st.get("open_positions") or []:
        if not isinstance(p, dict):
            continue
        code = str(p.get("code") or "").zfill(6)
        try:
            qty = int(float(p.get("qty") or 0))
        except Exception:
            qty = 0
        if code and qty > 0:
            rows.append({"code": code, "qty": qty})
    return pd.DataFrame(rows, columns=["code", "qty"])


def make_orders(target: pd.DataFrame, held: pd.DataFrame, exec_date: str,
                signal_date: str = "") -> pd.DataFrame:
    """[2026-08-25] signal_date 가 exec_date 로 채워져 있었다.

    신호일과 집행일은 다르다 - 어제 종가로 정해 오늘 산다. 같은 값을 넣으면
    산출물이 스스로를 잘못 기술한다([[feedback_reconstruct_arithmetic_not_labels]]).
    비우면 예전대로 exec_date 를 쓴다.
    """
    t = target.set_index("code")["target_qty"]
    h = held.set_index("code")["qty"] if len(held) else pd.Series(dtype=int)
    codes = sorted(set(t.index) | set(h.index))
    px = target.set_index("code")["close"]
    rows = []
    for c in codes:
        want = int(t.get(c, 0))
        have = int(h.get(c, 0))
        diff = want - have
        if diff == 0:
            continue
        side = "BUY" if diff > 0 else "SELL"
        note = "rebalance"
        if want == 0:
            note = "exit_not_in_target"
        elif have == 0:
            note = "new_entry"
        rows.append({
            "exec_date": exec_date,
            "side": side,
            "code": c,
            "fill_qty": abs(diff),
            "fill_price": float(px.get(c, 0.0)) or "",
            "order_type": "limit",
            "signal_date": signal_date or exec_date,
            "is_stop": 0,
            "note": note,
            "reason": "vol60_exclusion_equal_weight",
        })
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="자본 제약 동일가중 리밸런싱 주문 산출")
    ap.add_argument("--as-of", default="", help="YYYYMMDD. 비우면 아카이브 최신일")
    ap.add_argument("--exec-date", default="", help="주문 실행일. 비우면 as-of 와 동일")
    ap.add_argument("--capital", type=float, default=100_000_000)
    ap.add_argument("--exclude-pct", type=float, default=0.20)
    ap.add_argument("--min-value", type=float, default=1e9)
    ap.add_argument("--state", default=str(DEFAULT_STATE))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT))
    ap.add_argument("--apply", action="store_true", help="없으면 계산만 하고 파일을 쓰지 않는다")
    args = ap.parse_args()

    d = add_vol60(load_panel())
    as_of = args.as_of or str(d["date"].max())
    exec_date = args.exec_date or as_of

    target, meta = build_target(d, as_of, args.capital, args.exclude_pct, args.min_value)
    held = load_holdings(Path(args.state))
    orders = make_orders(target, held, exec_date, as_of)

    print("[REBAL] as_of=%s exec_date=%s" % (as_of, exec_date))
    print("  적격 %d -> vol60 상위 %d%% 배제 후 %d -> 고가주 %d 제외 -> 목표 %d종목"
          % (meta["eligible"], args.exclude_pct * 100, meta["after_vol_exclude"],
             meta["excluded_high_price"], meta["target_count"]))
    print("  슬롯 %s원   목표 투자금 %s원 (%.1f%%)   현금 %s원"
          % (format(meta["slot_krw"], ","), format(meta["target_notional"], ","),
             100.0 * meta["target_notional"] / max(args.capital, 1),
             format(meta["cash_left"], ",")))
    print("  현재 보유 %d종목" % len(held))
    if len(orders):
        b = orders[orders["side"] == "BUY"]
        s = orders[orders["side"] == "SELL"]
        print("  주문 %d건  BUY %d (%s원)  SELL %d"
              % (len(orders), len(b),
                 format(int((b["fill_qty"] * pd.to_numeric(b["fill_price"], errors="coerce").fillna(0)).sum()), ","),
                 len(s)))
        print("  note 분포: %s" % orders["note"].value_counts().to_dict())
    else:
        print("  주문 없음 (이미 목표와 일치)")

    if not args.apply:
        print("  dry-run. --apply 를 주면 파일을 쓴다.")
        return 0

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    xlsx = out_dir / ("orders_%s_rebal.xlsx" % exec_date)
    orders.to_excel(xlsx, index=False)
    tgt_csv = out_dir / ("target_%s.csv" % as_of)
    target.to_csv(tgt_csv, index=False, encoding="utf-8-sig")
    meta_json = out_dir / ("rebalance_meta_%s.json" % as_of)
    meta["orders"] = int(len(orders))
    meta["held"] = int(len(held))
    meta_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print("  wrote %s" % xlsx)
    print("  wrote %s" % tgt_csv)
    print("  wrote %s" % meta_json)
    print("  주의: paper/orders_*_exec.xlsx 가 아니다. 디스패치하려면 명시적으로 옮겨야 한다.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
