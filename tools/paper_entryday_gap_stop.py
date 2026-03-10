# -*- coding: utf-8 -*-
"""
tools/paper_entryday_gap_stop.py

목적:
- "진입일(=entry_date) 당일" OHLC로 손절(STOP/STOP_GAP) 여부를 판정하여,
  다음 거래일 데이터가 없어 paper_engine가 평가를 못하는 구간에서도
  현실적인 '진입 당일 손절' 청산을 기록한다.

동작:
- paper/paper_state.json의 open_positions를 읽는다.
- paper/prices/ohlcv_paper.parquet에서 (code, entry_date) OHLC를 찾는다.
- 조건:
  - 시가(open) <= stop_price  -> STOP_GAP (exit_price=open)
  - 저가(low)  <= stop_price  -> STOP     (exit_price=stop_price)
- 체결/거래 기록:
  - paper/fills.csv: SELL 1건(legacy schema) 추가 (idempotent)
  - paper/trades.csv: 1건(legacy schema) 추가 (idempotent)
- paper_state.json:
  - open_positions에서 청산 종목 제거
  - next_trade_seq 증가

주의:
- 이 스크립트는 "진입 당일 intraday STOP"를 보강하는 용도다.
- 기존 paper_engine의 다음날(T+1 이후) STOP_GAP/STOP 로직과 충돌하지 않도록
  order_id 규칙(PAPER_SELL_{code}_{exit_day}_{exit_reason})을 동일하게 사용한다.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


LEGACY_FILLS_HEADER = ["datetime", "code", "side", "qty", "price", "order_id", "note"]
LEGACY_TRADES_HEADER = ["trade_id", "code", "entry_date", "entry_price", "exit_date", "exit_price", "pnl_pct", "exit_reason", "note"]


def _ymd_series(d: pd.Series) -> pd.Series:
    y = pd.to_datetime(d, errors="coerce")
    y2 = pd.to_datetime(
        d.astype(str).str.replace(r"[^0-9]", "", regex=True),
        format="%Y%m%d",
        errors="coerce",
    )
    y = y.fillna(y2)
    return y.dt.strftime("%Y%m%d")


def _sig_float(v: Any) -> str:
    try:
        f = float(v)
    except Exception:
        return ""
    s = f"{f:.8f}".rstrip("0").rstrip(".")
    return s


def _calc_net_ret(entry_price: float, exit_price: float, fee_pct: float, slip_pct: float) -> float:
    if entry_price <= 0:
        return 0.0
    gross = (exit_price - entry_price) / entry_price
    # paper_engine과 동일 계열(양방향 비용): (entry+exit)/entry * (fee+slip)
    cost = ((entry_price + exit_price) / entry_price) * (fee_pct + slip_pct)
    return gross - cost


def _ensure_csv(path: Path, header: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(header)


def _read_csv_dicts(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", newline="", encoding=enc, errors="replace") as f:
                r = csv.DictReader(f)
                return list(r)
        except Exception:
            continue
    return []


def _append_rows(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    if not rows:
        return
    _ensure_csv(path, header)
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerows(rows)


def main() -> int:
    base = Path(__file__).resolve().parents[1]

    state_path = base / "paper" / "paper_state.json"
    cfg_path = base / "paper" / "paper_engine_config.json"
    prices_path = base / "paper" / "prices" / "ohlcv_paper.parquet"
    fills_path = base / "paper" / "fills.csv"
    trades_path = base / "paper" / "trades.csv"

    if not state_path.exists():
        print(f"[FATAL] missing state: {state_path}")
        return 2
    if not prices_path.exists():
        print(f"[FATAL] missing prices: {prices_path}")
        return 2

    st = json.load(state_path.open("r", encoding="utf-8"))
    open_positions = st.get("open_positions", []) or []
    if not open_positions:
        print("open_positions=0 -> nothing to do")
        return 0

    cfg: Dict[str, Any] = {}
    if cfg_path.exists():
        try:
            cfg = json.load(cfg_path.open("r", encoding="utf-8"))
        except Exception:
            cfg = {}

    fee_pct = float(cfg.get("fee_pct", 0.005) or 0.005)
    slip_pct = float(cfg.get("slippage_pct", 0.001) or 0.001)

    # existing ids/signatures (idempotent)
    existing_fill_ids = set()
    for row in _read_csv_dicts(fills_path):
        oid = (row.get("order_id") or "").strip()
        if oid:
            existing_fill_ids.add(oid)

    existing_trade_sigs = set()
    for row in _read_csv_dicts(trades_path):
        code = (row.get("code") or "").strip().zfill(6)
        sig = "|".join(
            [
                code,
                (row.get("entry_date") or "").strip(),
                _sig_float(row.get("entry_price") or ""),
                (row.get("exit_date") or "").strip(),
                _sig_float(row.get("exit_price") or ""),
                _sig_float(row.get("pnl_pct") or ""),
                (row.get("exit_reason") or "").strip(),
                (row.get("note") or "").strip(),
            ]
        )
        existing_trade_sigs.add(sig)

    next_seq = int(st.get("next_trade_seq", 1) or 1)

    # prices load + normalize
    df = pd.read_parquet(prices_path)
    need_cols = {"code", "date", "open", "high", "low", "close"}
    missing = sorted(list(need_cols - set(df.columns)))
    if missing:
        print(f"[FATAL] prices missing columns: {missing} in {prices_path}")
        return 2

    df["code"] = df["code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    df["ymd"] = _ymd_series(df["date"])
    prices_date_max = df["ymd"].max()
    print(f"prices_date_max={prices_date_max}")

    # index map: (code, ymd) -> last row
    df = df.sort_values(["code", "ymd"])
    last_rows = df.groupby(["code", "ymd"]).tail(1)
    px_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for _, r in last_rows.iterrows():
        key = (str(r["code"]), str(r["ymd"]))
        px_map[key] = {
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"]),
        }

    fills_new: List[List[Any]] = []
    trades_new: List[List[Any]] = []

    still_open: List[Dict[str, Any]] = []
    exited: List[Dict[str, Any]] = []

    for pos in open_positions:
        code = str(pos.get("code", "")).zfill(6)
        entry_date = str(pos.get("entry_date", "")).strip()
        qty = int(pos.get("qty", 1) or 1)

        try:
            entry_price = float(pos.get("entry_price", 0) or 0)
        except Exception:
            entry_price = 0.0

        try:
            stop_loss = float(pos.get("stop_loss", -0.05) or -0.05)
        except Exception:
            stop_loss = -0.05

        stop_price = entry_price * (1.0 + stop_loss)

        ohlc = px_map.get((code, entry_date))
        if not ohlc:
            still_open.append(pos)
            continue

        o = float(ohlc["open"])
        l = float(ohlc["low"])

        exit_reason = None
        exit_price = None

        # entry_date 당일 기준: 시가 갭락(=STOP_GAP) / 장중 손절(=STOP)
        if o <= stop_price:
            exit_reason = "STOP_GAP"
            exit_price = o
        elif l <= stop_price:
            exit_reason = "STOP"
            exit_price = stop_price

        if exit_reason and exit_price is not None and exit_price > 0:
            sell_order_id = f"PAPER_SELL_{code}_{entry_date}_{exit_reason}"

            # fills (SELL)
            if sell_order_id not in existing_fill_ids:
                fills_new.append(
                    [
                        f"{entry_date}T15:20:00",
                        code,
                        "SELL",
                        qty,
                        float(exit_price),
                        sell_order_id,
                        f"entry_day_stop=1 exit_reason={exit_reason}",
                    ]
                )
                existing_fill_ids.add(sell_order_id)

            pnl_pct = _calc_net_ret(entry_price, float(exit_price), fee_pct, slip_pct)

            note = f"signal_date={pos.get('signal_date')} entry_day_stop=1"
            trade_sig = "|".join(
                [
                    code,
                    entry_date,
                    _sig_float(entry_price),
                    entry_date,
                    _sig_float(float(exit_price)),
                    _sig_float(round(pnl_pct, 8)),
                    str(exit_reason),
                    note,
                ]
            )

            if trade_sig not in existing_trade_sigs:
                trade_id = f"T{next_seq:06d}"
                trades_new.append(
                    [
                        trade_id,
                        code,
                        entry_date,
                        entry_price,
                        entry_date,
                        float(exit_price),
                        round(pnl_pct, 8),
                        exit_reason,
                        note,
                    ]
                )
                existing_trade_sigs.add(trade_sig)
                next_seq += 1

            exited.append(
                {
                    "code": code,
                    "entry_date": entry_date,
                    "entry_price": entry_price,
                    "exit_price": float(exit_price),
                    "exit_reason": exit_reason,
                    "stop_price": stop_price,
                    "open": o,
                    "low": l,
                }
            )
        else:
            still_open.append(pos)

    # write outputs
    _append_rows(fills_path, LEGACY_FILLS_HEADER, fills_new)
    _append_rows(trades_path, LEGACY_TRADES_HEADER, trades_new)

    st["open_positions"] = still_open
    st["next_trade_seq"] = next_seq

    state_path.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"open_positions_before={len(open_positions)} open_positions_after={len(still_open)}")
    print(f"entryday_stop_exits={len(exited)} fills_added={len(fills_new)} trades_added={len(trades_new)}")
    if exited:
        print("exited_codes=" + ",".join([x["code"] for x in exited]))
        for x in exited:
            print(
                f"- {x['code']} {x['exit_reason']} exit={x['exit_price']} stop={round(x['stop_price'],4)} open={x['open']} low={x['low']}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
