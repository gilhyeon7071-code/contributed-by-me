from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set


@dataclass
class PortfolioState:
    open_positions: List[Dict[str, Any]]
    next_trade_seq: int
    processed_signals: List[str]
    settled_cash: Any = None
    pending_settlement: List[Dict[str, Any]] | None = None
    cash_ledger: List[Dict[str, Any]] | None = None
    settlement_cash_meta: Dict[str, Any] | None = None

    @classmethod
    def from_raw(cls, raw: Any) -> "PortfolioState":
        if not isinstance(raw, dict):
            return cls(open_positions=[], next_trade_seq=1, processed_signals=[])
        open_positions = raw.get("open_positions")
        processed_signals = raw.get("processed_signals")
        next_trade_seq = raw.get("next_trade_seq", 1)
        if not isinstance(open_positions, list):
            open_positions = []
        if not isinstance(processed_signals, list):
            processed_signals = []
        try:
            next_trade_seq_i = int(next_trade_seq)
        except Exception:
            next_trade_seq_i = 1
        return cls(
            open_positions=open_positions,
            next_trade_seq=max(1, next_trade_seq_i),
            processed_signals=processed_signals,
            settled_cash=raw.get("settled_cash"),
            pending_settlement=raw.get("pending_settlement") if isinstance(raw.get("pending_settlement"), list) else [],
            cash_ledger=raw.get("cash_ledger") if isinstance(raw.get("cash_ledger"), list) else [],
            settlement_cash_meta=raw.get("settlement_cash_meta") if isinstance(raw.get("settlement_cash_meta"), dict) else {},
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "open_positions": self.open_positions,
            "next_trade_seq": max(1, int(self.next_trade_seq or 1)),
            "processed_signals": self.processed_signals,
            "settled_cash": self.settled_cash,
            "pending_settlement": list(self.pending_settlement or []),
            "cash_ledger": list(self.cash_ledger or []),
            "settlement_cash_meta": dict(self.settlement_cash_meta or {}),
        }


@dataclass
class SignalTrackingState:
    state: Dict[str, Any]
    processed_signals: Set[str]
    existing_fill_order_ids: Set[str]
    existing_trade_sigs: Set[str]
    committed_source_order_ids: Set[str]
    committed_signal_keys: Set[str]
    df_fills: Any
    df_trades: Any
    same_close_filled_today: int
    open_pos: List[Dict[str, Any]]
    open_codes: Set[str]
    replay_global_ok: bool

    def to_runtime_dict(self) -> Dict[str, Any]:
        return {
            "state": self.state,
            "processed_signals": set(self.processed_signals),
            "existing_fill_order_ids": set(self.existing_fill_order_ids),
            "existing_trade_sigs": set(self.existing_trade_sigs),
            "committed_source_order_ids": set(self.committed_source_order_ids),
            "committed_signal_keys": set(self.committed_signal_keys),
            "df_fills": self.df_fills,
            "df_trades": self.df_trades,
            "same_close_filled_today": int(self.same_close_filled_today or 0),
            "open_pos": list(self.open_pos or []),
            "open_codes": set(self.open_codes),
            "replay_global_ok": bool(self.replay_global_ok),
        }


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def read_json_text_with_fallback(path: Path) -> str:
    raw_bytes = path.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "cp949", "euc-kr"):
        try:
            return raw_bytes.decode(enc)
        except Exception:
            continue
    return raw_bytes.decode("utf-8", errors="replace")
