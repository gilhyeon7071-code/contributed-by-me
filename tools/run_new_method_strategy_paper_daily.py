"""Paper-only daily executor for the research strategy contract.

This never imports or writes the operating paper-engine orders, fills, state, or gates.
It deliberately reuses only mechanical constraints that are compatible with the
methodology: three new entries per day, one share per entry, no same-code reentry,
and the current paper-engine transaction-cost defaults.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CANDIDATES_LATEST = LOG_DIR / "new_method_research_candidates_latest.csv"
CANDIDATES_HISTORY = LOG_DIR / "new_method_research_candidates_history.csv"
STATUS_LEDGER = LOG_DIR / "new_method_research_strategy_status_latest.csv"
TRADES_PATH = LOG_DIR / "new_method_strategy_paper_trades.csv"
STATUS_PATH = LOG_DIR / "new_method_strategy_paper_daily_status_latest.json"

STRATEGY_NAME = "RESIDUAL_MOMENTUM"
HORIZON = "h5"
MAX_NEW_TRADES_PER_DAY = 3
FIXED_QTY = 1
# [2026-09-10] 라이브 설정에 맞춰 교정. 이 상수들은 **편도**이고
#   왕복 = 2*FEE + 2*SLIPPAGE + SELL_TAX.
#   **이 파일에는 SELL_TAX 항이 아예 없었다** - 오늘 매도 쪽에 추가했다.
#   종전 0.005 는 왕복 1.0~1.4% 로 실제의 2.5~3.5배였다(BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2 + 0.001*2 + 0.002 = 0.00400 (optimize_params_v41_1.DEFAULT_FEE 와 동일).
#   **이 파일의 과거 산출물은 더 비싼 세계의 값이라 새 실행과 직접 비교할 수 없다.**
FEE_PCT = 0.0
SLIPPAGE_PCT = 0.001
SELL_TAX_PCT = 0.002   # 브로커 실측 0.19723% (수수료는 0)

TRADE_COLUMNS = [
    "event_id", "event_ts", "trade_id", "event_date", "side", "code", "market",
    "strategy_name", "horizon", "qty", "raw_price", "exec_price", "gross_notional",
    "fee", "slippage_cost", "net_cash", "gross_ret", "net_ret", "exit_reason",
    "source_signal_date", "research_regime", "operational_use", "broker_order", "note",
]


def _read_csv(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns or [])
    return pd.read_csv(path, encoding="utf-8-sig", dtype={"code": str})


def _as_code(value: Any) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6)


def _trade_id(signal_date: str, code: str) -> str:
    return f"NM-{STRATEGY_NAME}-{HORIZON}-{signal_date.replace('-', '')}-{code}"


def _event_row(**values: Any) -> dict[str, Any]:
    row = {key: "" for key in TRADE_COLUMNS}
    row.update(values)
    return row


def _is_priority_track(statuses: pd.DataFrame) -> bool:
    required = statuses[
        (statuses["strategy_name"].astype(str) == STRATEGY_NAME)
        & (statuses["horizon"].astype(str) == HORIZON)
    ]
    return bool(
        len(required) == 1
        and str(required.iloc[0].get("research_state", "")) == "PRIORITY_TRACK"
        and not bool(required.iloc[0].get("operational_use", False))
    )


def _open_trade_ids(trades: pd.DataFrame) -> set[str]:
    buys = set(trades.loc[trades["side"].astype(str) == "BUY", "trade_id"].astype(str))
    sells = set(trades.loc[trades["side"].astype(str) == "SELL", "trade_id"].astype(str))
    return buys - sells


def _closed_or_seen_codes(trades: pd.DataFrame) -> set[str]:
    return {_as_code(code) for code in trades.loc[trades["side"].astype(str) == "BUY", "code"]}


def _write_outputs(trades: pd.DataFrame, status: dict[str, Any]) -> None:
    trades = trades.reindex(columns=TRADE_COLUMNS)
    trades.to_csv(TRADES_PATH, index=False, encoding="utf-8-sig")
    STATUS_PATH.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    latest = _read_csv(CANDIDATES_LATEST)
    history = _read_csv(CANDIDATES_HISTORY)
    statuses = _read_csv(STATUS_LEDGER)
    trades = _read_csv(TRADES_PATH, columns=TRADE_COLUMNS).reindex(columns=TRADE_COLUMNS)
    now = datetime.now().isoformat(timespec="seconds")
    if latest.empty or history.empty or statuses.empty:
        raise RuntimeError("required research candidate or status artifact is missing")
    priority_track = _is_priority_track(statuses)
    if not priority_track:
        status = {
            "generated_at": now,
            "status": "HOLD_NO_PRIORITY_TRACK",
            "strategy": f"{STRATEGY_NAME}_{HORIZON}",
            "new_buys": 0,
            "new_sells": 0,
            "operational_use": False,
            "broker_order": False,
        }
        _write_outputs(trades, status)
        print(json.dumps(status, ensure_ascii=False))
        return 0

    for frame in (latest, history):
        frame["code"] = frame["code"].map(_as_code)
        frame["signal_date"] = frame["signal_date"].astype(str)
    latest_date = str(latest["signal_date"].max())
    open_ids = _open_trade_ids(trades)
    buy_rows = trades[trades["side"].astype(str) == "BUY"].copy()
    new_events: list[dict[str, Any]] = []

    # Fixed five-global-session exit, read from the same integrity-controlled candidate history.
    for _, buy in buy_rows[buy_rows["trade_id"].astype(str).isin(open_ids)].iterrows():
        source_date = str(buy["source_signal_date"])
        code = _as_code(buy["code"])
        matched = history[
            (history["strategy_name"].astype(str) == STRATEGY_NAME)
            & (history["signal_date"] == source_date)
            & (history["code"] == code)
        ]
        if matched.empty:
            continue
        gross_ret = pd.to_numeric(matched.iloc[0].get("path_return_h5"), errors="coerce")
        if pd.isna(gross_ret):
            continue
        raw_exit = float(buy["raw_price"]) * (1.0 + float(gross_ret))
        qty = int(float(buy["qty"]))
        exit_exec = raw_exit * (1.0 - SLIPPAGE_PCT)
        proceeds = exit_exec * qty
        exit_fee = proceeds * FEE_PCT
        # [2026-09-10] 매도 거래세가 빠져 있었다. 매도 쪽에만 붙는다(브로커 실측 0.19723%).
        exit_tax = proceeds * SELL_TAX_PCT
        entry_net_cash = abs(float(buy["net_cash"]))
        net_ret = ((proceeds - exit_fee - exit_tax) / entry_net_cash) - 1.0 if entry_net_cash > 0 else None
        new_events.append(_event_row(
            event_id=f"SELL-{buy['trade_id']}", event_ts=now, trade_id=str(buy["trade_id"]),
            event_date=latest_date, side="SELL", code=code, market=buy["market"],
            strategy_name=STRATEGY_NAME, horizon=HORIZON, qty=qty, raw_price=raw_exit,
            exec_price=exit_exec, gross_notional=proceeds, fee=exit_fee,
            slippage_cost=raw_exit * qty - proceeds, net_cash=proceeds - exit_fee,
            gross_ret=float(gross_ret), net_ret=net_ret, exit_reason="FIXED_H5_GLOBAL_SESSION_EXIT",
            source_signal_date=source_date, research_regime=buy["research_regime"],
            operational_use=False, broker_order=False,
            note="research_only; no legacy gate/final_score/stop rule applied",
        ))

    if new_events:
        trades = pd.concat([trades, pd.DataFrame(new_events)], ignore_index=True)

    seen_codes = _closed_or_seen_codes(trades)
    today = latest[
        (latest["strategy_name"].astype(str) == STRATEGY_NAME)
        & (latest["signal_date"] == latest_date)
    ].copy()
    today["strategy_rank"] = pd.to_numeric(today["strategy_rank"], errors="coerce")
    today["close"] = pd.to_numeric(today["close"], errors="coerce")
    today_buys = trades[
        (trades["side"].astype(str) == "BUY") & (trades["event_date"].astype(str) == latest_date)
    ]
    remaining_slots = max(0, MAX_NEW_TRADES_PER_DAY - len(today_buys))
    picks = today[
        today["close"].gt(0) & ~today["code"].isin(seen_codes)
    ].sort_values(["strategy_rank", "code"]).head(remaining_slots)
    for _, candidate in picks.iterrows():
        code = _as_code(candidate["code"])
        raw_entry = float(candidate["close"])
        exec_entry = raw_entry * (1.0 + SLIPPAGE_PCT)
        notional = exec_entry * FIXED_QTY
        fee = notional * FEE_PCT
        trade_id = _trade_id(latest_date, code)
        new_events.append(_event_row(
            event_id=f"BUY-{trade_id}", event_ts=now, trade_id=trade_id, event_date=latest_date,
            side="BUY", code=code, market=candidate.get("market", ""), strategy_name=STRATEGY_NAME,
            horizon=HORIZON, qty=FIXED_QTY, raw_price=raw_entry, exec_price=exec_entry,
            gross_notional=notional, fee=fee, slippage_cost=notional - raw_entry * FIXED_QTY,
            net_cash=-(notional + fee), source_signal_date=latest_date,
            research_regime=candidate.get("research_regime", ""), operational_use=False,
            broker_order=False,
            note="research_only; same_close to match close-to-close h5 validation",
        ))
    if len(new_events) > 0:
        existing_events = trades if not trades.empty else pd.DataFrame(columns=TRADE_COLUMNS)
        appended = pd.DataFrame(new_events).reindex(columns=TRADE_COLUMNS)
        # The exit records were already inserted above; retain only buy records in this append.
        if not trades.empty and any(item["side"] == "SELL" for item in new_events):
            appended = appended[appended["side"] == "BUY"]
        trades = appended.copy() if existing_events.empty else pd.concat([existing_events, appended], ignore_index=True)

    status = {
        "generated_at": now,
        "status": "OK_RESEARCH_PAPER_ONLY",
        "strategy": f"{STRATEGY_NAME}_{HORIZON}",
        "latest_signal_date": latest_date,
        "new_buys": int(len(picks)),
        "new_sells": int(sum(1 for event in new_events if event["side"] == "SELL")),
        "open_positions": int(len(_open_trade_ids(trades))),
        "mechanical_policy_reused": {
            "max_new_trades_per_day": MAX_NEW_TRADES_PER_DAY,
            "fixed_qty": FIXED_QTY,
            "allow_same_code_reentry": False,
            "fee_pct": FEE_PCT,
            "slippage_pct": SLIPPAGE_PCT,
        },
        "policy_not_reused": ["legacy final_score", "entry gate", "generic stop loss", "next_open", "T+2 settled-cash control"],
        "deferred_realism_requirement": "T+2 cash can only be enforced after an explicit research capital/notional policy replaces fixed_qty=1.",
        "entry_contract": "same_close; matches the historical close-to-close h5 return definition",
        "exit_contract": "exact h5 global trading session return from price-integrity-controlled history",
        "operational_use": False,
        "broker_order": False,
    }
    _write_outputs(trades, status)
    print(json.dumps(status, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
