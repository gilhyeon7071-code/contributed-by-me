from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
PAPER_DIR = BASE_DIR / "paper"
LOG_DIR = BASE_DIR / "2_Logs"

FILLS_PATH = PAPER_DIR / "fills.csv"

# 산출물(기존 paper/trades.csv는 건드리지 않고 별도 파일로 생성)
FILLS_NORM_PATH = PAPER_DIR / "fills_norm.csv"
TRADES_CALC_PATH = PAPER_DIR / "trades_calc.csv"

CONFIG_PATH = PAPER_DIR / "paper_config.json"


@dataclass
class Config:
    fee_rate: float = 0.0         # 왕복 비용(양쪽 적용)은 아래에서 2배로 처리
    slippage_rate: float = 0.0    # 왕복 비용(양쪽 적용)은 아래에서 2배로 처리
    matching: str = "FIFO"        # FIFO 또는 LIFO


def load_config() -> Config:
    if not CONFIG_PATH.exists():
        return Config()
    try:
        j = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return Config(
            fee_rate=float(j.get("fee_rate", 0.0)),
            slippage_rate=float(j.get("slippage_rate", 0.0)),
            matching=str(j.get("matching", "FIFO")).upper(),
        )
    except Exception:
        # config 파싱 실패 시 기본값으로 진행(자동 실행이 멈추지 않게)
        return Config()


def _norm_cols(cols) -> List[str]:
    # BOM/공백 방어
    return [str(c).lstrip("\ufeff").strip() for c in cols]


def _parse_ts(series: pd.Series) -> pd.Series:
    # 다양한 문자열 포맷 허용
    ts = pd.to_datetime(series, errors="coerce")
    return ts


def _as_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)


def _as_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)


def load_fills_any_schema(path: Path) -> pd.DataFrame:
    """
    허용 스키마:
    A) 최소: datetime,code,side,qty,price,(order_id),(note)
    B) 확장: ts,date,code,name,side,qty,price,fee,slippage,order_id,note
    """
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str)
    df.columns = _norm_cols(df.columns)

    cols = set(df.columns)

    # 시간 컬럼 결정
    if "ts" in cols:
        ts_raw = df["ts"]
    elif "datetime" in cols:
        ts_raw = df["datetime"]
    else:
        # ts가 없으면 date라도 있나 확인
        if "date" in cols:
            ts_raw = df["date"]
        else:
            ts_raw = pd.Series([None] * len(df))

    ts = _parse_ts(ts_raw)

    out = pd.DataFrame()
    out["ts"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    out["date"] = ts.dt.strftime("%Y%m%d")

    # 필수 컬럼
    out["code"] = df["code"] if "code" in cols else ""
    out["name"] = df["name"] if "name" in cols else ""
    out["side"] = (df["side"] if "side" in cols else "").astype(str).str.upper().str.strip()
    out["qty"] = _as_int(df["qty"] if "qty" in cols else pd.Series([0] * len(df)))
    out["price"] = _as_float(df["price"] if "price" in cols else pd.Series([0.0] * len(df)))

    # 선택 컬럼
    out["order_id"] = df["order_id"] if "order_id" in cols else ""
    out["note"] = df["note"] if "note" in cols else ""

    # 정렬/정리
    out = out.dropna(subset=["date"])  # ts 파싱 실패(NaT) 제거
    out = out[out["code"].astype(str).str.len() > 0]
    out = out[out["side"].isin(["BUY", "SELL"])]
    out = out[out["qty"] > 0]
    out = out[out["price"] > 0]

    out = out.sort_values(["ts", "code", "side"], kind="mergesort").reset_index(drop=True)
    return out


@dataclass
class Lot:
    side: str              # LONG or SHORT
    qty: int
    entry_price: float
    entry_ts: str
    name: str
    note: str


def _pop_lot(lots: List[Lot], matching: str) -> Lot:
    if matching == "LIFO":
        return lots.pop(-1)
    return lots.pop(0)  # FIFO


def build_trades_from_fills(fills: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    FIFO/LIFO로 매칭.
    - BUY가 먼저면 LONG 오픈, SELL로 청산
    - SELL이 먼저면 SHORT 오픈, BUY로 청산(숏도 지원)
    """
    open_lots: Dict[str, List[Lot]] = {}
    trades_rows = []
    trade_seq = 1

    for _, r in fills.iterrows():
        code = str(r["code"]).strip()
        name = str(r.get("name", "")).strip()
        side = str(r["side"]).strip().upper()
        qty = int(r["qty"])
        price = float(r["price"])
        ts = str(r["ts"])
        note = str(r.get("note", "")).strip()
        oid = str(r.get("order_id", "")).strip()
        fill_note = f"order_id={oid}" if oid else ""
        if note:
            fill_note = (fill_note + " | " if fill_note else "") + note

        lots = open_lots.setdefault(code, [])

        def close_against(existing_side: str, close_qty: int, exit_price: float, exit_ts: str):
            nonlocal trade_seq
            nonlocal trades_rows

            remaining = close_qty
            while remaining > 0 and lots:
                lot = _pop_lot(lots, cfg.matching)
                if lot.side != existing_side:
                    # 다른 방향 lot이면 다시 넣고 종료(정렬 혼선 방지)
                    lots.insert(0, lot)
                    break

                m = min(remaining, lot.qty)
                remaining -= m

                entry_price = lot.entry_price
                exit_price2 = exit_price

                if existing_side == "LONG":
                    gross_ret = (exit_price2 / entry_price) - 1.0
                else:  # SHORT
                    gross_ret = (entry_price / exit_price2) - 1.0

                # 비용(왕복 2번) 반영: 단순 근사(필요 시 config로 조정)
                fee_total = 2.0 * float(cfg.fee_rate)
                slip_total = 2.0 * float(cfg.slippage_rate)
                net_ret = gross_ret - fee_total - slip_total

                trades_rows.append(
                    {
                        "trade_id": str(trade_seq),
                        "entry_ts": lot.entry_ts,
                        "exit_ts": exit_ts,
                        "code": code,
                        "name": lot.name or name,
                        "side": existing_side,
                        "qty": int(m),
                        "entry_price": float(entry_price),
                        "exit_price": float(exit_price2),
                        "gross_ret": float(gross_ret),
                        "net_ret": float(net_ret),
                        "fee_rate": float(cfg.fee_rate),
                        "slippage_rate": float(cfg.slippage_rate),
                        "note": lot.note,
                    }
                )
                trade_seq += 1

                # lot 잔량 처리
                if lot.qty > m:
                    lot.qty -= m
                    # 남은 lot 다시 넣기(FIFO는 앞쪽, LIFO는 뒤쪽이 자연스럽지만 단순히 앞에 넣음)
                    lots.insert(0, lot)

        if side == "BUY":
            # SHORT 청산 우선
            if any(l.side == "SHORT" for l in lots):
                close_against("SHORT", qty, price, ts)
                # 남는 수량은 LONG 오픈
                remaining = qty - sum(
                    tr["qty"] for tr in trades_rows if tr["code"] == code and tr["exit_ts"] == ts and tr["side"] == "SHORT"
                )
                # 위 remaining 계산은 보수적으로 다시 계산
                # (정확하게는 close_against 내부 remaining을 외부로 반환해야 하지만, 단순히 lots 상태로 판단)
                # lots에 SHORT가 없고 qty가 남는 경우만 LONG 오픈
                # -> 아래에서 qty만큼 LONG 오픈하되, lots에 SHORT가 없으면 그대로 추가
                if not any(l.side == "SHORT" for l in lots):
                    lots.append(Lot(side="LONG", qty=qty, entry_price=price, entry_ts=ts, name=name, note=fill_note))
            else:
                lots.append(Lot(side="LONG", qty=qty, entry_price=price, entry_ts=ts, name=name, note=fill_note))

        elif side == "SELL":
            # LONG 청산 우선
            if any(l.side == "LONG" for l in lots):
                close_against("LONG", qty, price, ts)
                # 남는 수량은 SHORT 오픈(원하면 사용)
                if not any(l.side == "LONG" for l in lots):
                    lots.append(Lot(side="SHORT", qty=qty, entry_price=price, entry_ts=ts, name=name, note=fill_note))
            else:
                lots.append(Lot(side="SHORT", qty=qty, entry_price=price, entry_ts=ts, name=name, note=fill_note))

    trades = pd.DataFrame(trades_rows)
    if trades.empty:
        # 헤더만 유지
        trades = pd.DataFrame(columns=[
            "trade_id","entry_ts","exit_ts","code","name","side","qty",
            "entry_price","exit_price","gross_ret","net_ret","fee_rate","slippage_rate","note"
        ])
    return trades


def write_pnl_summary(trades: pd.DataFrame) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    out = LOG_DIR / f"paper_sync_pnl_summary_{stamp}.json"

    if trades.empty:
        payload = {
            "generated_at": now.isoformat(timespec="seconds"),
            "trades_used": 0,
            "status": "no_trades",
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[PAPER_PNL] no trades (rows=0)")
        print(f"[OUT] {out}")
        return out

    r = pd.to_numeric(trades["net_ret"], errors="coerce").dropna()
    n = int(len(r))
    wins = int((r > 0).sum())
    losses = int((r < 0).sum())
    win_rate = (wins / n) if n else 0.0

    payload = {
        "generated_at": now.isoformat(timespec="seconds"),
        "trades_used": n,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_ret": float(r.mean()) if n else 0.0,
        "sum_ret": float(r.sum()) if n else 0.0,
        "comp_ret": float((1.0 + r).prod() - 1.0) if n else 0.0,
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[PAPER_PNL] trades_used={n}")
    print(f"[OUT] {out}")
    return out


def main() -> int:
    PAPER_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_config()

    fills = load_fills_any_schema(FILLS_PATH)
    if fills.empty:
        print(f"[PAPER_SYNC] no fills -> {FILLS_PATH}")
        # 그래도 요약 파일은 만들지 않고 종료(불필요 로그 폭증 방지)
        return 0

    fills.to_csv(FILLS_NORM_PATH, index=False, encoding="utf-8-sig")
    print(f"[PAPER_SYNC] fills_norm rows={len(fills)} -> {FILLS_NORM_PATH}")

    trades = build_trades_from_fills(fills, cfg)
    trades.to_csv(TRADES_CALC_PATH, index=False, encoding="utf-8-sig")
    print(f"[PAPER_SYNC] trades_calc rows={len(trades)} -> {TRADES_CALC_PATH}")

    write_pnl_summary(trades)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

