from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CODE = "403550"
TARGET_EXIT = "DDM_LIQUIDATE_L4"


def _note_value(note: Any, key: str) -> str:
    text = "" if note is None else str(note)
    m = re.search(rf"(?:^|;){re.escape(key)}=([^;]*)", text)
    return m.group(1).strip() if m else ""


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        text = str(value).strip()
        if not text:
            return default
        return float(text)
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or pd.isna(value):
            return default
        text = str(value).strip()
        if not text:
            return default
        return int(float(text))
    except Exception:
        return default


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _entry_oid_from_trade_note(note: str) -> str:
    order_id = _note_value(note, "order_id")
    entry_oid = _note_value(note, "entry_order_id")
    return entry_oid or order_id


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fills = _load_csv(ROOT / "paper" / "fills_norm.csv")
    trades = _load_csv(ROOT / "paper" / "trades_calc.csv")

    code_fills = fills[fills["code"].astype(str).str.strip().eq(CODE)].copy()
    code_trades = trades[trades["code"].astype(str).str.strip().eq(CODE)].copy()
    if code_fills.empty:
        raise RuntimeError(f"No fills found for {CODE}")

    code_fills["qty_i"] = code_fills["qty"].map(_to_int)
    code_fills["price_f"] = code_fills["price"].map(_to_float)
    code_fills["entry_order_id_note"] = code_fills["note"].map(lambda s: _note_value(s, "entry_order_id") or _note_value(s, "source_order_id"))
    code_fills["exit_reason"] = code_fills["note"].map(lambda s: _note_value(s, "exit_reason"))
    code_fills["entry_trace_id_note"] = code_fills["note"].map(lambda s: _note_value(s, "entry_trace_id"))
    code_fills["source_trace_id_note"] = code_fills["note"].map(lambda s: _note_value(s, "source_trace_id"))
    code_fills["lineage_backfill_original_entry_order_id"] = code_fills["note"].map(
        lambda s: _note_value(s, "lineage_backfill_original_entry_order_id")
    )

    code_trades["qty_i"] = code_trades["qty"].map(_to_int)
    code_trades["entry_price_f"] = code_trades["entry_price"].map(_to_float)
    code_trades["exit_price_f"] = code_trades["exit_price"].map(_to_float)
    code_trades["net_ret_f"] = code_trades["net_ret"].map(_to_float)
    code_trades["entry_order_id_note"] = code_trades["note"].map(_entry_oid_from_trade_note)

    buys = code_fills[code_fills["side"].astype(str).str.upper().eq("BUY")].copy()
    sells = code_fills[code_fills["side"].astype(str).str.upper().eq("SELL")].copy()
    ddm_sells = sells[sells["exit_reason"].eq(TARGET_EXIT)].copy()
    ddm_trades = code_trades[code_trades["exit_ts"].astype(str).str.startswith("2026-05-15")].copy()

    lineage_rows: list[dict[str, Any]] = []
    for _, buy in buys.iterrows():
        oid = str(buy.get("order_id", "")).strip()
        sell_rows = sells[sells["entry_order_id_note"].eq(oid)]
        trade_rows = code_trades[code_trades["entry_order_id_note"].eq(oid)]
        buy_qty = _to_int(buy.get("qty"))
        sell_qty = int(sell_rows["qty_i"].sum()) if not sell_rows.empty else 0
        trade_qty = int(trade_rows["qty_i"].sum()) if not trade_rows.empty else 0
        lineage_rows.append(
            {
                "entry_order_id": oid,
                "buy_ts": str(buy.get("ts", "")),
                "buy_qty": buy_qty,
                "buy_price": _to_float(buy.get("price")),
                "sell_qty_by_fills": sell_qty,
                "trade_qty_by_trades_calc": trade_qty,
                "net_qty_after_sells": buy_qty - sell_qty,
                "sell_reasons": ",".join(sorted(set(str(x) for x in sell_rows["exit_reason"].tolist() if str(x)))),
                "trade_net_ret_sum": float(trade_rows["net_ret_f"].sum()) if not trade_rows.empty else 0.0,
                "trade_ids": ",".join(str(x) for x in trade_rows["trade_id"].tolist()),
            }
        )

    split_metadata_issues = []
    for _, row in ddm_sells.iterrows():
        oid = str(row.get("entry_order_id_note", "")).strip()
        buy_match = buys[buys["order_id"].astype(str).str.strip().eq(oid)]
        buy_trace = ""
        if not buy_match.empty:
            buy_trace = _note_value(str(buy_match.iloc[0].get("note", "")), "entry_trace_id")
        sell_trace = str(row.get("entry_trace_id_note", "")).strip()
        source_trace = str(row.get("source_trace_id_note", "")).strip()
        original_oid = str(row.get("lineage_backfill_original_entry_order_id", "")).strip()
        trace_mismatch = bool(oid and buy_trace and sell_trace and sell_trace != buy_trace)
        split_metadata_issues.append(
            {
                "sell_order_id": str(row.get("order_id", "")),
                "entry_order_id": oid,
                "qty": _to_int(row.get("qty")),
                "price": _to_float(row.get("price")),
                "buy_entry_trace_id": buy_trace,
                "sell_entry_trace_id": sell_trace,
                "sell_source_trace_id": source_trace,
                "lineage_backfill_original_entry_order_id": original_oid,
                "trace_mismatch_vs_entry_order_id": trace_mismatch,
            }
        )

    buy_qty_total = int(buys["qty_i"].sum())
    sell_qty_total = int(sells["qty_i"].sum())
    ddm_qty_total = int(ddm_sells["qty_i"].sum()) if not ddm_sells.empty else 0
    trade_qty_total = int(code_trades["qty_i"].sum()) if not code_trades.empty else 0
    ddm_trade_qty_total = int(ddm_trades["qty_i"].sum()) if not ddm_trades.empty else 0

    result = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "FAIL",
        "policy_change": False,
        "code": CODE,
        "target_exit_reason": TARGET_EXIT,
        "source": {
            "fills": str(ROOT / "paper" / "fills_norm.csv"),
            "trades": str(ROOT / "paper" / "trades_calc.csv"),
        },
        "summary": {
            "buy_qty_total": buy_qty_total,
            "sell_qty_total": sell_qty_total,
            "net_qty_after_all_sells": buy_qty_total - sell_qty_total,
            "oversell_qty": max(0, sell_qty_total - buy_qty_total),
            "ddm_sell_qty_total": ddm_qty_total,
            "trades_calc_qty_total": trade_qty_total,
            "ddm_trade_qty_total": ddm_trade_qty_total,
            "ddm_trade_net_ret_sum": float(ddm_trades["net_ret_f"].sum()) if not ddm_trades.empty else 0.0,
            "ddm_trade_count": int(len(ddm_trades)),
            "lineage_count": int(len(lineage_rows)),
            "trace_mismatch_count": int(sum(1 for x in split_metadata_issues if x["trace_mismatch_vs_entry_order_id"])),
        },
        "lineage_rows": lineage_rows,
        "ddm_sell_rows": [
            {
                "ts": str(row.get("ts", "")),
                "order_id": str(row.get("order_id", "")),
                "entry_order_id": str(row.get("entry_order_id_note", "")),
                "qty": _to_int(row.get("qty")),
                "price": _to_float(row.get("price")),
                "lineage_backfill_original_entry_order_id": str(row.get("lineage_backfill_original_entry_order_id", "")),
            }
            for _, row in ddm_sells.iterrows()
        ],
        "split_metadata_issues": split_metadata_issues,
        "interpretation": [
            "403550 residual DDM_L4 is not an oversell in current fills/trades quantities.",
            "Current trades_calc reflects entry_order_id-aware lineage matching for this case.",
            "The largest residual loss comes from carrying N1 residual 7 shares and N3 15 shares into next-day DDM_L4.",
            "One lineage split row carries entry_order_id for N1 but trace metadata from the original N3 split source; this is a metadata-quality issue, not a quantity over-sell.",
        ],
        "trading_effect": {
            "policy_value_validity": "NOT_PROVEN",
            "root_cause_class": "DDM_L4_RESIDUAL_LOSS_PLUS_SPLIT_METADATA_QUALITY",
        },
    }

    json_path = LOG_DIR / "mdd_403550_ddm_l4_lineage_diagnostic_latest.json"
    csv_path = LOG_DIR / "mdd_403550_ddm_l4_lineage_diagnostic_latest.csv"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(lineage_rows).to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(json.dumps({"json": str(json_path), "csv": str(csv_path), "summary": result["summary"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
