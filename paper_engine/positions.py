"""Fills/positions management for paper_engine.

Split out from the legacy ``paper_engine.py`` as Phase 5 of the module refactor.
"""
from __future__ import annotations


__all__ = [
    '_write_empty_pending_signals_file',
    '_archive_entry_source_row',
    '_atomic_write_csv_with_backup',
    '_backfill_horizon_note_in_fills',
    '_reconcile_open_positions_with_fills',
    '_recovery_action_hint',
    '_lineage_from_row',
    '_recover_open_positions',
    '_partition_replay_orders_for_recovery',
    '_load_replay_orders_raw',
    '_load_pending_signals',
    '_filter_pending_by_trading_age',
    '_revalidate_pending_signals',
    '_load_replay_orders',
    '_save_pending_signals',
    '_write_pending_status',
    '_build_entry_exit_lifecycle',
    '_build_state_machine_summary',
    '_build_symbol_stop_summary',
    '_collect_persisted_today_runtime_metrics',
    '_collect_persisted_today_order_artifacts',
    '_next_available_legacy_trade_id',
    '_recover_missing_sell_trades_legacy',
    '_write_ops_alert',
    'append_rows',
    '_append_rows_atomic',
    '_restore_backup_file',
    '_sync_new_fills_to_live_bridge',
    '_compute_current_open_notional',
    '_count_open_position_slots',
    '_recalculate_open_notional_and_alert',
    '_apply_sector_rebalance',
    '_prioritize_open_positions_for_sell',
    '_process_position_rows',
    '_process_open_positions_and_rebalance',
]

import csv
import json
import math
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import pandas as pd

from pricing_engine import (
    calc_exit_fee,
    calc_roundtrip_fee,
    calc_roundtrip_slippage,
)
from utils.common import norm_code, now_ymd, read_csv_safe

from paper_engine.config import _path_from_env
from paper_engine.common import (
    RUN_LABEL,
    PAPER_SESSION_ID,
    _concat_drop_all_na_columns,
    _stable_digest,
    prev_trading_date,
    _to_float,
    _to_int,
    _truthy,
    _sig_float,
    _v411_trade_sig,
    _extract_ymd_from_ts_text,
    _norm_ymd_text,
    _parse_note_fields,
    _extract_note_field,
    _get_dict,
    _get_list,
    get_ohlc,
    resolve_slip_pct,
    calc_net_ret,
    now_ts,
)
from paper_engine.io import (
    FILLS,
    TRADES,
    LEGACY_TRADES_HEADER,
    CANDIDATES_META_PATH,
    ROOTB_REGEN_LIVE_FILLS,
    PENDING_SIGNALS_PATH,
    PENDING_SIGNALS_SCHEMA,
    PENDING_STATUS_PATH,
    OPS_ALERT_LATEST_PATH,
    ENTRY_SOURCE_ARCHIVE_HISTORY_PATH,
    ENTRY_SOURCE_ARCHIVE_FIELDS,
    REPLAY_ORDERS_PATH,
)
from paper_engine.exit import (
    _paper_exit_ts,
    _extract_signal_date,
    _sell_lifecycle_signature,
    _hydrate_position_exit_tags_from_fills,
    _process_single_position_exit,
    _collect_persisted_today_sell_artifacts,
)
from paper_engine.drawdown import _ddm_pos_key
from paper_engine.surge import _normalize_surge_position_policy_fields

def _write_empty_pending_signals_file() -> None:
    empty_df = pd.DataFrame(columns=PENDING_SIGNALS_SCHEMA)
    empty_df.to_csv(PENDING_SIGNALS_PATH, index=False, encoding="utf-8-sig")

def _archive_entry_source_row(row: Dict[str, Any], archive_row: Dict[str, Any]) -> None:
    try:
        ENTRY_SOURCE_ARCHIVE_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
        exists = ENTRY_SOURCE_ARCHIVE_HISTORY_PATH.exists() and ENTRY_SOURCE_ARCHIVE_HISTORY_PATH.stat().st_size > 0
        with ENTRY_SOURCE_ARCHIVE_HISTORY_PATH.open("a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=ENTRY_SOURCE_ARCHIVE_FIELDS, extrasaction="ignore")
            if not exists:
                writer.writeheader()
            writer.writerow(archive_row)
            f.flush()
            os.fsync(f.fileno())
    except Exception as exc:
        print(f"[ENTRY_SOURCE_ARCHIVE_WARN] failed code={row.get('code','')} error={type(exc).__name__}:{exc}")

def _atomic_write_csv_with_backup(path: Path, df: pd.DataFrame, header: List[str]) -> str:
    bak_dir = path.parent / "_bak"
    bak_dir.mkdir(parents=True, exist_ok=True)
    backup_path = ""
    if path.exists():
        backup_name = f"{path.name}.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        backup_file = bak_dir / backup_name
        shutil.copy2(path, backup_file)
        backup_path = str(backup_file)
    ordered_cols = list(header)
    for col in df.columns:
        if col not in ordered_cols:
            ordered_cols.append(col)
    out_df = df.reindex(columns=ordered_cols)
    if "code" in out_df.columns:
        out_df["code"] = (
            out_df["code"]
            .astype(str)
            .str.strip()
            .str.replace(".0", "", regex=False)
            .str.zfill(6)
        )
    if "is_surge" in out_df.columns:
        def _is_surge_flag(v: Any) -> int:
            s = str(v or "").strip().lower()
            return 1 if s in {"1", "1.0", "true", "yes", "y"} else 0

        out_df["is_surge"] = out_df["is_surge"].map(_is_surge_flag).astype(int)
    tmp = path.with_suffix(path.suffix + ".tmp")
    out_df.to_csv(tmp, index=False, encoding="utf-8-sig")
    tmp.replace(path)
    return backup_path

def _backfill_horizon_note_in_fills(
    df_fills: pd.DataFrame,
    open_positions: List[Dict[str, Any]],
) -> tuple[pd.DataFrame, int]:
    if not isinstance(df_fills, pd.DataFrame) or df_fills.empty:
        return df_fills, 0
    if "order_id" not in df_fills.columns or "side" not in df_fills.columns or "note" not in df_fills.columns:
        return df_fills, 0

    order_to_horizon: Dict[str, str] = {}
    for pos in list(open_positions or []):
        if not isinstance(pos, dict):
            continue
        order_id = str(pos.get("entry_order_id", "") or "").strip()
        hz = str(pos.get("horizon_label", "") or "").strip().upper()
        if order_id and hz:
            order_to_horizon[order_id] = hz
    if not order_to_horizon:
        return df_fills, 0

    out = df_fills.copy()
    changed = 0
    for idx, row in out.iterrows():
        if str(row.get("side", "")).strip().upper() != "BUY":
            continue
        order_id = str(row.get("order_id", "") or "").strip()
        hz = order_to_horizon.get(order_id, "")
        if not hz:
            continue
        note = str(row.get("note", "") or "").strip()
        if "horizon=" in note:
            continue
        if note and not note.endswith(";"):
            note = note + ";"
        note = note + f"horizon={hz}"
        out.at[idx, "note"] = note
        changed += 1
    return out, int(changed)

def _reconcile_open_positions_with_fills(
    open_positions: List[Dict[str, Any]],
    fills_df: pd.DataFrame,
    stop_loss: float,
    take_profit: Optional[float],
    trail_pct: Optional[float],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    summary: Dict[str, Any] = {
        "enabled": True,
        "fills_rows": int(len(fills_df)) if isinstance(fills_df, pd.DataFrame) else 0,
        "state_open_before": int(len(open_positions or [])),
        "state_open_after": int(len(open_positions or [])),
        "fills_open_codes": 0,
        "added_codes": [],
        "removed_codes": [],
        "qty_adjusted_codes": [],
        "lineage_adjusted_codes": [],
    }
    if not isinstance(fills_df, pd.DataFrame) or fills_df.empty:
        return list(open_positions or []), summary
    if "code" not in fills_df.columns or "side" not in fills_df.columns:
        return list(open_positions or []), summary

    work = fills_df.copy()
    if "datetime" in work.columns:
        work["_ts"] = work["datetime"].astype(str)
    elif "ts" in work.columns:
        work["_ts"] = work["ts"].astype(str)
    elif "date" in work.columns:
        work["_ts"] = work["date"].astype(str) + "T15:20:00"
    else:
        work["_ts"] = ""
    qty_col = "qty" if "qty" in work.columns else ("fill_qty" if "fill_qty" in work.columns else None)
    px_col = "price" if "price" in work.columns else ("fill_price" if "fill_price" in work.columns else None)
    if qty_col is None:
        return list(open_positions or []), summary

    work = work.sort_values(["_ts"], kind="mergesort")

    def _derive_exit_state(code: str, source_order_id: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {"stop_exit_count": 0, "last_exit_severity": ""}
        src = str(source_order_id or "").strip()
        if not code or not src or "note" not in work.columns:
            return out
        stop_reasons = {"STOP", "STOP_GAP", "STOP_PREEMPTIVE_CLOSE", "REVERSAL_NEXT_OPEN", "SURGE_INTRADAY_REVERSAL"}
        last_ts = ""
        for _, srow in work.iterrows():
            if norm_code(srow.get("code", "")) != code:
                continue
            if str(srow.get("side", "")).strip().upper() != "SELL":
                continue
            note = str(srow.get("note", "") or "")
            row_src = _extract_note_field(note, "source_order_id") or _extract_note_field(note, "entry_order_id")
            if row_src != src:
                continue
            exit_reason = (_extract_note_field(note, "exit_reason") or "").strip().upper()
            if exit_reason not in stop_reasons:
                continue
            if str(_extract_note_field(note, "partial_exit")).strip().lower() not in {"1", "true", "yes"}:
                continue
            out["stop_exit_count"] = int(out.get("stop_exit_count", 0) or 0) + 1
            row_ts = str(srow.get("_ts", "") or "")
            if row_ts >= last_ts:
                last_ts = row_ts
                out["last_exit_severity"] = _extract_note_field(note, "exit_severity")
        return out

    net_qty_by_code: Dict[str, int] = {}
    net_qty_by_lineage: Dict[Tuple[str, str], int] = {}
    last_buy_by_code: Dict[str, Dict[str, Any]] = {}
    last_buy_by_lineage: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for _, row in work.iterrows():
        code = norm_code(row.get("code", ""))
        side = str(row.get("side", "")).strip().upper()
        qty_val = _to_int(row.get(qty_col), None)
        if not code or qty_val is None or qty_val <= 0:
            continue
        net_qty_by_code[code] = int(net_qty_by_code.get(code, 0))
        note_text = str(row.get("note", "") or "")
        if side == "BUY":
            net_qty_by_code[code] += int(qty_val)
            signal_date = _extract_signal_date(row.get("note"))
            note_fields = _parse_note_fields(note_text)
            is_surge_fill = bool(re.search(r"(?:^|[;\s])surge_immediate=1(?:$|[;\s])", note_text, flags=re.IGNORECASE))
            fill_ts = str(row.get("_ts", "") or "").strip()
            fill_ymd = re.sub(r"[^0-9]", "", fill_ts)[:8]
            px = _to_float(row.get(px_col), None) if px_col else None
            buy_info = {
                "entry_ts": fill_ts if fill_ts else f"{fill_ymd}T09:00:00",
                "entry_date": fill_ymd,
                "signal_date": signal_date if signal_date else fill_ymd,
                "entry_price": float(px) if px is not None and px > 0 else None,
                "name": str(row.get("name", "") or "").strip(),
                "order_id": str(row.get("order_id", "") or "").strip(),
                "_surge_immediate": 1 if is_surge_fill else 0,
                "note_fields": note_fields,
            }
            last_buy_by_code[code] = buy_info
            entry_order_id = str(row.get("order_id", "") or "").strip()
            if entry_order_id:
                key = (code, entry_order_id)
                net_qty_by_lineage[key] = int(net_qty_by_lineage.get(key, 0)) + int(qty_val)
                last_buy_by_lineage[key] = buy_info
        elif side == "SELL":
            net_qty_by_code[code] -= int(qty_val)
            sell_source_order_id = (
                _extract_note_field(note_text, "source_order_id")
                or _extract_note_field(note_text, "entry_order_id")
                or ""
            ).strip()
            if sell_source_order_id:
                key = (code, sell_source_order_id)
                net_qty_by_lineage[key] = int(net_qty_by_lineage.get(key, 0)) - int(qty_val)

    fills_open_codes = sorted([c for c, q in net_qty_by_code.items() if int(q) > 0])
    summary["fills_open_codes"] = int(len(fills_open_codes))

    state_by_code: Dict[str, Dict[str, Any]] = {}
    state_by_lineage: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for pos in list(open_positions or []):
        if not isinstance(pos, dict):
            continue
        code = norm_code(pos.get("code", ""))
        if not code:
            continue
        source_order_id = str(pos.get("source_order_id") or pos.get("entry_order_id") or "").strip()
        if source_order_id and (code, source_order_id) not in state_by_lineage:
            state_by_lineage[(code, source_order_id)] = dict(pos)
        if code not in state_by_code:
            state_by_code[code] = dict(pos)

    reconciled: List[Dict[str, Any]] = []
    added_codes: List[str] = []
    qty_adjusted_codes: List[str] = []
    lineage_adjusted_codes: List[str] = []
    handled_lineage_keys: Set[Tuple[str, str]] = set()

    def _lineage_buy_sort_value(key: Tuple[str, str]) -> str:
        buy = last_buy_by_lineage.get(key, {})
        return str(buy.get("entry_ts") or buy.get("entry_date") or key[1])

    def _best_positive_lineage_for_code(code: str, exclude_source_order_id: str = "") -> Tuple[str, Dict[str, Any], int]:
        candidates = [
            key
            for key, qty in net_qty_by_lineage.items()
            if key[0] == code and int(qty) > 0 and key[1] != str(exclude_source_order_id or "").strip()
        ]
        if not candidates:
            return "", last_buy_by_code.get(code, {}), int(net_qty_by_code.get(code, 0))
        best_key = sorted(candidates, key=_lineage_buy_sort_value, reverse=True)[0]
        return best_key[1], last_buy_by_lineage.get(best_key, last_buy_by_code.get(code, {})), int(net_qty_by_lineage.get(best_key, 0))

    def _build_or_update_recovered_position(
        *,
        code: str,
        source_order_id: str,
        net_qty: int,
        buy: Dict[str, Any],
        existing_pos: Dict[str, Any],
    ) -> Dict[str, Any]:
        pos = dict(existing_pos or {})
        if not pos:
            entry_date = str(buy.get("entry_date", "") or now_ymd())
            signal_date = str(buy.get("signal_date", "") or entry_date)
            entry_ts = str(buy.get("entry_ts", "") or f"{entry_date}T09:00:00")
            entry_price = _to_float(buy.get("entry_price"), None)
            if entry_price is None or entry_price <= 0:
                entry_price = 1.0
            entry_order_id = str(source_order_id or buy.get("order_id", "") or f"RECOVERED_BUY_{code}_{entry_date}")
            note_fields = _get_dict(buy, "note_fields")
            pos = {
                "code": code,
                "name": str(buy.get("name", "") or ""),
                "qty": net_qty,
                "signal_date": signal_date,
                "entry_date": entry_date,
                "entry_ts": entry_ts,
                "entry_price": float(entry_price),
                "max_close": float(entry_price),
                "asset_type": "",
                "sector": "",
                "fundamentals": {},
                "tp_taken_levels": [],
                "executed_sell_tags": [],
                "stop_loss": float(stop_loss),
                "take_profit": take_profit,
                "trail_pct": trail_pct,
                "entry_order_id": entry_order_id,
                "entry_intent_id": f"RECOVERED_INTENT_{code}_{entry_date}",
                "entry_trace_id": _stable_digest("RECOVER_ENTRY", code, entry_date, entry_order_id, net_qty),
                "lineage_origin": "RECOVERED_FROM_FILLS",
                "source_order_id": entry_order_id,
                "source_intent_id": "",
                "source_trace_id": "",
                "replay_chain_id": _stable_digest("RECOVER_CHAIN", code, entry_order_id),
                "replay_depth": 0,
                "market_cap": 0.0,
                "_surge_immediate": int(_to_int(buy.get("_surge_immediate"), 0) or 0),
            }
            for key in [
                "entry_timing",
                "surge_type",
                "surge_score",
                "surge_score_final",
                "surge_rvol20",
                "surge_spread_bps",
                "surge_orderflow_tag",
                "surge_orderflow_risk_score",
                "surge_lob_slippage_pct",
                "surge_lob_slippage_source",
                "horizon",
                "split_entry",
                "run_label",
            ]:
                value = note_fields.get(key)
                if value is not None and str(value).strip() != "":
                    pos[key] = str(value).strip()
            if _truthy(note_fields.get("surge_immediate")):
                pos["_surge_immediate"] = 1
            exit_state = _derive_exit_state(code, entry_order_id)
            if int(exit_state.get("stop_exit_count", 0) or 0) > 0:
                pos["stop_exit_count"] = int(exit_state.get("stop_exit_count", 0) or 0)
                pos["last_exit_severity"] = str(exit_state.get("last_exit_severity", "") or "")
            added_codes.append(code)
        else:
            cur_qty = _to_int(pos.get("qty"), 0) or 0
            if int(cur_qty) != int(net_qty):
                pos["qty"] = int(net_qty)
                qty_adjusted_codes.append(code)
            if _to_float(pos.get("market_cap"), None) is None:
                pos["market_cap"] = 0.0
            if _to_int(pos.get("_surge_immediate"), None) is None:
                _last_buy = last_buy_by_code.get(code, {}) or {}
                pos["_surge_immediate"] = int(_to_int(_last_buy.get("_surge_immediate"), 0) or 0)
            note_fields = _get_dict(last_buy_by_code.get(code, {}), "note_fields")
            if isinstance(note_fields, dict):
                for key in [
                    "entry_timing",
                    "surge_type",
                    "surge_score",
                    "surge_score_final",
                    "surge_rvol20",
                    "surge_spread_bps",
                    "surge_orderflow_tag",
                    "surge_orderflow_risk_score",
                    "surge_lob_slippage_pct",
                    "surge_lob_slippage_source",
                    "horizon",
                    "split_entry",
                    "run_label",
                ]:
                    if str(pos.get(key) or "").strip():
                        continue
                    value = note_fields.get(key)
                    if value is not None and str(value).strip() != "":
                        pos[key] = str(value).strip()
                if not _truthy(pos.get("_surge_immediate")) and _truthy(note_fields.get("surge_immediate")):
                    pos["_surge_immediate"] = 1
            source_order_id = str(pos.get("source_order_id") or pos.get("entry_order_id") or "").strip()
            exit_state = _derive_exit_state(code, source_order_id)
            recovered_stop_count = int(exit_state.get("stop_exit_count", 0) or 0)
            current_stop_count = max(0, int(_to_int(pos.get("stop_exit_count"), 0) or 0))
            if recovered_stop_count > current_stop_count:
                pos["stop_exit_count"] = recovered_stop_count
                pos["last_exit_severity"] = str(exit_state.get("last_exit_severity", "") or "")
        return pos

    handled_codes: Set[str] = set()

    # Existing state rows are lot/order scoped. Keep them aligned to their own
    # source order before falling back to code-level netting; otherwise old
    # same-code round trips can incorrectly remove a current position.
    for key, existing_pos in sorted(state_by_lineage.items(), key=lambda item: item[0]):
        code, source_order_id = key
        if key not in net_qty_by_lineage:
            continue
        net_qty = int(net_qty_by_lineage.get(key, 0))
        if net_qty <= 0:
            continue
        buy = last_buy_by_lineage.get(key, last_buy_by_code.get(code, {}))
        pos = _build_or_update_recovered_position(
            code=code,
            source_order_id=source_order_id,
            net_qty=net_qty,
            buy=buy,
            existing_pos=existing_pos,
        )
        reconciled.append(pos)
        handled_codes.add(code)
        handled_lineage_keys.add(key)

    for code in fills_open_codes:
        code_net_qty = int(net_qty_by_code.get(code, 0))
        lineage_keys = sorted(
            [
                key
                for key, qty in net_qty_by_lineage.items()
                if key[0] == code and int(qty) > 0 and key not in handled_lineage_keys
            ],
            key=lambda item: item[1],
        )
        lineage_net_qty = sum(int(net_qty_by_lineage.get(key, 0)) for key in lineage_keys)
        if lineage_keys and lineage_net_qty != code_net_qty:
            lineage_keys = []
        if lineage_keys:
            handled_codes.add(code)
            for key in lineage_keys:
                _, source_order_id = key
                net_qty = int(net_qty_by_lineage.get(key, 0))
                buy = last_buy_by_lineage.get(key, last_buy_by_code.get(code, {}))
                pos = _build_or_update_recovered_position(
                    code=code,
                    source_order_id=source_order_id,
                    net_qty=net_qty,
                    buy=buy,
                    existing_pos=state_by_lineage.get(key, {}),
                )
                reconciled.append(pos)
                handled_lineage_keys.add(key)

    for code in fills_open_codes:
        if code in handled_codes:
            continue
        net_qty = int(net_qty_by_code.get(code, 0))
        if net_qty <= 0:
            continue
        existing_pos = state_by_code.get(code, {})
        source_order_id = str((last_buy_by_code.get(code, {}) or {}).get("order_id", "") or "")
        existing_source_order_id = ""
        if isinstance(existing_pos, dict):
            existing_source_order_id = str(existing_pos.get("source_order_id") or existing_pos.get("entry_order_id") or "").strip()
        if existing_source_order_id and existing_source_order_id in {key[1] for key in net_qty_by_lineage if key[0] == code}:
            if int(net_qty_by_lineage.get((code, existing_source_order_id), 0)) <= 0:
                source_order_id, fallback_buy, lineage_qty = _best_positive_lineage_for_code(
                    code,
                    exclude_source_order_id=existing_source_order_id,
                )
                if source_order_id:
                    net_qty = min(net_qty, int(lineage_qty))
                    existing_pos = {}
                    lineage_adjusted_codes.append(code)
                else:
                    fallback_buy = last_buy_by_code.get(code, {})
            else:
                fallback_buy = last_buy_by_code.get(code, {})
        else:
            fallback_buy = last_buy_by_code.get(code, {})
        pos = _build_or_update_recovered_position(
            code=code,
            source_order_id=source_order_id,
            net_qty=net_qty,
            buy=fallback_buy,
            existing_pos=existing_pos,
        )
        reconciled.append(pos)

    active_reconciled_codes = {norm_code(pos.get("code", "")) for pos in reconciled if isinstance(pos, dict)}
    removed_codes = sorted([c for c in state_by_code.keys() if c not in active_reconciled_codes])
    summary["added_codes"] = added_codes
    summary["removed_codes"] = removed_codes
    summary["qty_adjusted_codes"] = qty_adjusted_codes
    summary["lineage_adjusted_codes"] = sorted(set(lineage_adjusted_codes))
    summary["state_open_after"] = int(len(reconciled))
    return reconciled, summary


def _recovery_action_hint(reason: str) -> str:
    mapping = {
        "entry_or_signal_date_missing": "paper_state.json의 open_positions 생성 시 entry_date/signal_date 저장 경로를 점검",
        "source_order_id_missing": "replay_orders_latest.csv 생성 경로를 다시 실행해 source_order_id를 복구",
        "source_intent_id_missing": "make_orders_exec_from_fills.py를 재실행해 replay_source_intent_id를 복구",
        "source_trace_id_missing": "make_orders_exec_from_fills.py를 재실행해 replay_source_trace_id를 복구",
        "replay_chain_id_missing": "orders_exec 재생성 후 replay_chain_id 생성 경로를 확인",
        "trace_or_chain_missing": "replay_orders_latest.csv와 orders_exec를 재생성하고 trace/chain 컬럼을 점검",
        "active_position_conflict": "이미 보유 중인 종목이므로 replay를 폐기하고 open_positions 정합성만 확인",
        "resume_next_session": "다음 세션 주문 후보로 자동 재개",
        "normalized": "누락 필드를 자동 복구함. 다음 저장부터 동일 필드가 유지되는지 확인",
        "as_is": "추가 조치 없음",
    }
    return mapping.get(str(reason or "").strip(), "관련 source lineage와 replay queue를 점검")

def _lineage_from_row(row: Dict[str, Any], code: str, signal_date: str, entry_day: str, order_id: str, qty: int) -> Dict[str, Any]:
    is_open_order_replay = _truthy(row.get("_replay_order")) or str(row.get("carry_reason", "")).strip().upper() == "OPEN_ORDER_REPLAY"
    source_order_id = str(row.get("replay_source_order_id", "") or "").strip()
    source_intent_id = str(row.get("replay_source_intent_id", "") or "").strip()
    source_trace_id = str(row.get("replay_source_trace_id", "") or "").strip()
    prev_chain_id = str(row.get("replay_chain_id", "") or "").strip()
    prev_depth = _to_int(row.get("replay_depth")) or 0
    if is_open_order_replay and source_order_id and not source_intent_id:
        source_intent_id = f"REPLAY_SRC_INTENT_{code}_{source_order_id[-16:]}"
    if is_open_order_replay and source_order_id and not source_trace_id:
        source_trace_id = _stable_digest("REPLAY_SRC_TRACE", code, source_order_id, prev_chain_id, signal_date)
    lineage = {
        "entry_order_id": order_id,
        "entry_intent_id": "",
        "entry_trace_id": "",
        "lineage_origin": "FRESH_SIGNAL",
        "source_order_id": source_order_id,
        "source_intent_id": source_intent_id,
        "source_trace_id": source_trace_id,
        "replay_chain_id": "",
        "replay_depth": 0,
    }
    lineage["entry_intent_id"] = f"ENTRY_INTENT_{code}_{entry_day}_{signal_date}"
    lineage["entry_trace_id"] = _stable_digest("ENTRY", code, signal_date, entry_day, order_id, qty)
    if is_open_order_replay:
        lineage["lineage_origin"] = "OPEN_ORDER_REPLAY"
        lineage["replay_depth"] = int(prev_depth + 1)
        lineage["replay_chain_id"] = prev_chain_id or _stable_digest(
            "REPLAY_CHAIN",
            code,
            source_order_id or order_id,
            source_intent_id,
            source_trace_id,
        )
    else:
        lineage["replay_chain_id"] = _stable_digest("ENTRY_CHAIN", code, order_id, signal_date)
    return lineage

def _recover_open_positions(state: Dict[str, Any], cfg: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    raw_positions = _get_list(state, "open_positions")
    recovered: List[Dict[str, Any]] = []
    deduped = 0
    normalized = 0
    auto_recovered_positions = 0
    manual_review_positions = 0
    auto_recovered_details: List[Dict[str, Any]] = []
    manual_review_details: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()

    fills_for_exit_tags = read_csv_safe(FILLS)
    if fills_for_exit_tags is None:
        fills_for_exit_tags = pd.DataFrame()

    for idx, raw in enumerate(raw_positions):
        if not isinstance(raw, dict):
            continue
        pos = dict(raw)
        pos_normalized = False
        needs_manual_review = False
        code = norm_code(pos.get("code", ""))
        if not code:
            continue
        pos["code"] = code
        entry_date = re.sub(r"[^0-9]", "", str(pos.get("entry_date") or ""))[:8]
        signal_date = re.sub(r"[^0-9]", "", str(pos.get("signal_date") or ""))[:8]
        entry_ts = str(pos.get("entry_ts") or f"{entry_date}T09:00:00").strip()
        raw_origin = str(pos.get("lineage_origin") or "").strip().upper()
        has_source_order = bool(str(pos.get("source_order_id") or "").strip())
        has_source_intent = bool(str(pos.get("source_intent_id") or "").strip())
        has_source_trace = bool(str(pos.get("source_trace_id") or "").strip())
        has_replay_chain = bool(str(pos.get("replay_chain_id") or "").strip())
        replay_origin = raw_origin == "OPEN_ORDER_REPLAY" or has_source_order or has_source_intent or has_source_trace or has_replay_chain
        pos["entry_date"] = entry_date
        pos["signal_date"] = signal_date
        pos["entry_ts"] = entry_ts

        if not entry_date and not signal_date:
            needs_manual_review = True

        entry_order_id = str(pos.get("entry_order_id") or "").strip()
        if not entry_order_id:
            entry_order_id = f"RECOVERED_ENTRY_{code}_{entry_date or signal_date or idx}"
            pos["entry_order_id"] = entry_order_id
            normalized += 1
            pos_normalized = True
        if not str(pos.get("entry_intent_id") or "").strip():
            pos["entry_intent_id"] = f"RECOVERED_INTENT_{code}_{entry_date or signal_date or idx}"
            normalized += 1
            pos_normalized = True
        if not str(pos.get("entry_trace_id") or "").strip():
            pos["entry_trace_id"] = _stable_digest("RECOVER_TRACE", code, entry_ts, entry_order_id)
            normalized += 1
            pos_normalized = True
        if not str(pos.get("replay_chain_id") or "").strip():
            pos["replay_chain_id"] = _stable_digest("RECOVER_CHAIN", code, entry_order_id, pos.get("entry_trace_id"))
            normalized += 1
            pos_normalized = True
        if "replay_depth" not in pos or _to_int(pos.get("replay_depth")) is None:
            pos["replay_depth"] = 0
            normalized += 1
            pos_normalized = True
        if not str(pos.get("lineage_origin") or "").strip():
            pos["lineage_origin"] = "OPEN_ORDER_REPLAY" if replay_origin else "FRESH_SIGNAL"
            normalized += 1
            pos_normalized = True
        if isinstance(cfg, dict):
            surge_normalized = _normalize_surge_position_policy_fields(pos, cfg)
            if surge_normalized:
                normalized += int(surge_normalized)
                pos_normalized = True
        if fills_for_exit_tags is not None and not fills_for_exit_tags.empty:
            tag_normalized = _hydrate_position_exit_tags_from_fills(pos, fills_for_exit_tags)
            if tag_normalized:
                normalized += int(tag_normalized)
                pos_normalized = True

        if replay_origin:
            if not has_source_order and entry_order_id:
                pos["source_order_id"] = entry_order_id
                has_source_order = True
                normalized += 1
                pos_normalized = True
            if not has_source_intent and has_source_order:
                pos["source_intent_id"] = f"REPLAY_SRC_INTENT_{code}_{str(pos.get('source_order_id') or '')[-16:]}"
                has_source_intent = True
                normalized += 1
                pos_normalized = True
            if not has_source_trace and has_source_order:
                pos["source_trace_id"] = _stable_digest(
                    "REPLAY_SRC_TRACE",
                    code,
                    pos.get("source_order_id"),
                    pos.get("replay_chain_id"),
                    entry_ts,
                )
                has_source_trace = True
                normalized += 1
                pos_normalized = True
            if not has_source_order or (not has_source_intent and not has_source_trace):
                needs_manual_review = True
            if not str(pos.get("replay_chain_id") or "").strip():
                needs_manual_review = True

        detail = {
            "code": code,
            "entry_date": entry_date,
            "entry_order_id": str(pos.get("entry_order_id") or "").strip(),
            "lineage_origin": str(pos.get("lineage_origin") or "").strip(),
            "source_order_id": str(pos.get("source_order_id") or "").strip(),
        }
        reasons: List[str] = []
        if not entry_date and not signal_date:
            reasons.append("entry_or_signal_date_missing")
        if replay_origin and not str(pos.get("source_order_id") or "").strip():
            reasons.append("source_order_id_missing")
        if replay_origin and not str(pos.get("source_intent_id") or "").strip():
            reasons.append("source_intent_id_missing")
        if replay_origin and not str(pos.get("source_trace_id") or "").strip():
            reasons.append("source_trace_id_missing")
        if replay_origin and not str(pos.get("replay_chain_id") or "").strip():
            reasons.append("replay_chain_id_missing")
        reason_text = ",".join(reasons) if reasons else ("normalized" if pos_normalized else "as_is")
        detail["reason"] = reason_text
        detail["action_hint"] = _recovery_action_hint(reasons[0] if reasons else reason_text)

        dedupe_key = f"{code}|{entry_order_id}"
        if dedupe_key in seen_keys:
            deduped += 1
            continue
        seen_keys.add(dedupe_key)
        if pos_normalized:
            if needs_manual_review:
                manual_review_positions += 1
                if len(manual_review_details) < 20:
                    manual_review_details.append(detail)
            else:
                auto_recovered_positions += 1
                if len(auto_recovered_details) < 20:
                    auto_recovered_details.append(detail)
        recovered.append(pos)

    summary = {
        "generated_at": now_ts(),
        "open_positions_before": int(len(raw_positions)),
        "open_positions_after": int(len(recovered)),
        "normalized_fields": int(normalized),
        "auto_recovered_positions": int(auto_recovered_positions),
        "manual_review_positions": int(manual_review_positions),
        "deduped_positions": int(deduped),
        "auto_recovered_details": auto_recovered_details,
        "manual_review_details": manual_review_details,
    }
    return recovered, summary

def _partition_replay_orders_for_recovery(
    replay_df: pd.DataFrame,
    open_positions: List[Dict[str, Any]],
    queue_guard_reason: str = "",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if replay_df is None or replay_df.empty:
        return replay_df, {
            "eligible_rows": 0,
            "resume_rows": 0,
            "discard_rows": 0,
            "discard_codes": [],
            "discard_source_order_ids": [],
            "manual_review_rows": 0,
            "manual_review_codes": [],
            "queue_guard_reason": "",
            "queue_guard_rows": 0,
            "discard_details": [],
            "manual_review_details": [],
            "resume_details": [],
        }

    active_codes = {norm_code(pos.get("code", "")) for pos in open_positions if norm_code(pos.get("code", ""))}
    active_order_ids = {
        str(pos.get("entry_order_id") or "").strip()
        for pos in open_positions
        if str(pos.get("entry_order_id") or "").strip()
    }
    active_source_order_ids = {
        str(pos.get("source_order_id") or "").strip()
        for pos in open_positions
        if str(pos.get("source_order_id") or "").strip()
    }

    work = replay_df.copy()
    work["code"] = work["code"].astype(str).str.zfill(6)
    source_order = work["replay_source_order_id"].astype(str).str.strip() if "replay_source_order_id" in work.columns else pd.Series("", index=work.index)
    source_intent = work["replay_source_intent_id"].astype(str).str.strip() if "replay_source_intent_id" in work.columns else pd.Series("", index=work.index)
    source_trace = work["replay_source_trace_id"].astype(str).str.strip() if "replay_source_trace_id" in work.columns else pd.Series("", index=work.index)
    chain_id = work["replay_chain_id"].astype(str).str.strip() if "replay_chain_id" in work.columns else pd.Series("", index=work.index)

    blocked_mask = work["code"].isin(active_codes)
    if "replay_source_order_id" in replay_df.columns:
        blocked_mask = blocked_mask | source_order.isin(active_order_ids | active_source_order_ids)
    manual_mask = source_order.eq("") | ((source_intent.eq("")) & (source_trace.eq(""))) | chain_id.eq("")
    manual_mask = manual_mask & ~blocked_mask
    guard_reason = str(queue_guard_reason or "").strip()
    guarded_mask = pd.Series(False, index=work.index)
    if guard_reason:
        guarded_mask = ~blocked_mask
        manual_mask = manual_mask | guarded_mask

    blocked = work.loc[blocked_mask].copy()
    manual = work.loc[manual_mask].copy()
    kept = work.loc[~(blocked_mask | manual_mask)].copy()

    def _detail_rows(frame: pd.DataFrame, reason: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if frame is None or frame.empty:
            return rows
        for _, row in frame.head(20).iterrows():
            rows.append(
                {
                    "code": str(row.get("code") or "").zfill(6),
                    "next_session_ymd": str(row.get("next_session_ymd") or "").strip(),
                    "replay_source_order_id": str(row.get("replay_source_order_id") or "").strip(),
                    "replay_chain_id": str(row.get("replay_chain_id") or "").strip(),
                    "reason": reason,
                    "action_hint": _recovery_action_hint(reason),
                }
            )
        return rows

    def _snapshot_rows(frame: pd.DataFrame, reason: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if frame is None or frame.empty:
            return rows
        for _, row in frame.head(100).iterrows():
            payload: Dict[str, Any] = {}
            for col, val in row.items():
                if pd.isna(val):
                    payload[str(col)] = None
                elif isinstance(val, (str, int, float, bool)) or val is None:
                    payload[str(col)] = val
                else:
                    payload[str(col)] = str(val)
            payload["quarantine_reason"] = reason
            payload["quarantine_at"] = now_ts()
            rows.append(payload)
        return rows

    summary = {
        "eligible_rows": int(len(kept)),
        "resume_rows": int(len(kept)),
        "discard_rows": int(len(blocked)),
        "discard_codes": sorted({str(v).zfill(6) for v in blocked.get("code", pd.Series(dtype=str)).astype(str).tolist() if str(v).strip()}),
        "discard_source_order_ids": sorted({str(v).strip() for v in blocked.get("replay_source_order_id", pd.Series(dtype=str)).astype(str).tolist() if str(v).strip()}),
        "manual_review_rows": int(len(manual)),
        "manual_review_codes": sorted({str(v).zfill(6) for v in manual.get("code", pd.Series(dtype=str)).astype(str).tolist() if str(v).strip()}),
        "queue_guard_reason": guard_reason,
        "queue_guard_rows": int(guarded_mask.sum()) if guard_reason else 0,
        "discard_details": _detail_rows(blocked, "active_position_conflict"),
        "manual_review_details": _detail_rows(manual, guard_reason or "trace_or_chain_missing"),
        "manual_review_snapshot_rows": _snapshot_rows(manual, guard_reason or "trace_or_chain_missing"),
        "resume_details": _detail_rows(kept, "resume_next_session"),
    }
    return kept, summary

def _load_replay_orders_raw(max_age_days: int) -> pd.DataFrame:
    if not REPLAY_ORDERS_PATH.exists():
        return pd.DataFrame()
    try:
        q = pd.read_csv(REPLAY_ORDERS_PATH)
    except Exception:
        return pd.DataFrame()
    if q.empty:
        return q
    need = {"code", "side", "remaining_qty", "next_session_ymd"}
    if any(col not in q.columns for col in need):
        return pd.DataFrame()

    q["code"] = q["code"].astype(str).str.zfill(6)
    if "exec_date" in q.columns:
        q["exec_date"] = q["exec_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    else:
        q["exec_date"] = ""
    q["next_session_ymd"] = q["next_session_ymd"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    q["remaining_qty"] = pd.to_numeric(q["remaining_qty"], errors="coerce").fillna(0.0)
    q = q[
        (q["code"].str.len() == 6)
        & (q["next_session_ymd"].str.len() == 8)
        & (q["remaining_qty"] > 0)
    ].copy()
    if q.empty:
        return q

    if max_age_days > 0 and "exec_date" in q.columns:
        today_dt = datetime.strptime(now_ymd(), "%Y%m%d")
        min_day = (today_dt - timedelta(days=max_age_days)).strftime("%Y%m%d")
        q = q[(q["exec_date"] == "") | (q["exec_date"] >= min_day)].copy()
    return q

def _load_pending_signals(max_age_days: int) -> pd.DataFrame:
    if not PENDING_SIGNALS_PATH.exists():
        return pd.DataFrame()
    try:
        p = pd.read_csv(PENDING_SIGNALS_PATH)
    except Exception:
        return pd.DataFrame()
    if p.empty:
        return p
    if "signal_date" not in p.columns or "code" not in p.columns:
        return pd.DataFrame()

    p["signal_date"] = p["signal_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    p["code"] = p["code"].astype(str).str.zfill(6)
    p = p[(p["signal_date"].str.len() == 8) & (p["code"].str.len() == 6)].copy()

    if p.empty:
        return p
    p = p.sort_values(["signal_date", "code"], ascending=[False, True]).drop_duplicates(["code", "signal_date"], keep="first")
    return p

def _filter_pending_by_trading_age(p: pd.DataFrame, px: Optional[pd.DataFrame], max_age_days: int) -> pd.DataFrame:
    if p is None or p.empty or max_age_days <= 0:
        return p
    if px is None or px.empty or "code" not in px.columns or "date" not in px.columns:
        return p

    q = px.copy()
    q["code"] = q["code"].astype(str).str.zfill(6)
    q["date"] = q["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    if "close" in q.columns:
        q = q[q["close"].fillna(0).astype(float) > 0].copy()
    q = q[(q["code"].str.len() == 6) & (q["date"].str.len() == 8)].copy()
    if q.empty:
        return p

    date_map: Dict[str, List[str]] = (
        q.sort_values(["code", "date"])
        .drop_duplicates(["code", "date"], keep="last")
        .groupby("code")["date"]
        .agg(list)
        .to_dict()
    )

    keep_mask: List[bool] = []
    for _, row in p.iterrows():
        code = str(row.get("code", "")).zfill(6)
        signal_date = str(row.get("signal_date", ""))
        trading_dates = date_map.get(code, [])
        future_days = [d for d in trading_dates if d > signal_date]
        keep_mask.append(len(future_days) <= int(max_age_days))

    return p.loc[keep_mask].copy()

def _revalidate_pending_signals(
    pending: pd.DataFrame,
    current_df: pd.DataFrame,
    score_gap_max: float = 0.0,
    min_final_score: float = 0.0,
    market_gate_block: bool = False,
    sector_gate_enabled: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if pending is None or pending.empty:
        return pd.DataFrame(), {"loaded": 0, "revalidated": 0, "dropped": 0, "score_gap_failed": 0, "min_score_failed": 0, "failed_codes": [], "failed_reason_counts": {}}
    if current_df is None or current_df.empty or "code" not in current_df.columns:
        return pd.DataFrame(), {"loaded": int(len(pending)), "revalidated": 0, "dropped": int(len(pending)), "score_gap_failed": 0, "min_score_failed": 0, "failed_codes": [], "failed_reason_counts": {}}

    cur = current_df.copy()
    cur["code"] = cur["code"].astype(str).str.zfill(6)
    if "signal_date" in cur.columns:
        cur["signal_date"] = cur["signal_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    else:
        cur["signal_date"] = now_ymd()

    sort_cols = [c for c in ["signal_date", "final_score", "score"] if c in cur.columns]
    ascending = [False] * len(sort_cols)
    if sort_cols:
        cur = cur.sort_values(sort_cols, ascending=ascending)
    cur = cur.drop_duplicates(["code"], keep="first").copy()

    pending = pending.copy()
    pending["code"] = pending["code"].astype(str).str.zfill(6)
    split2_codes: set[str] = set()
    if "split_entry_2nd" in pending.columns:
        split2_mask = pending["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"})
        split2_codes = set(pending.loc[split2_mask, "code"].astype(str).str.zfill(6).tolist())
    merged = pending.merge(cur, on="code", how="inner", suffixes=("_pending", ""))
    if merged.empty and not split2_codes:
        return pd.DataFrame(), {"loaded": int(len(pending)), "revalidated": 0, "dropped": int(len(pending)), "score_gap_failed": 0, "min_score_failed": 0, "failed_codes": [], "failed_reason_counts": {}}

    score_gap_failed = 0
    min_score_failed = 0
    keep_codes: set[str] = set()
    failed_codes: List[str] = []
    failed_reason_counts: Dict[str, int] = {}
    score_gap_by_code: Dict[str, float] = {}
    prev_final_score_by_code: Dict[str, float] = {}
    prev_score_by_code: Dict[str, float] = {}

    for _, row in merged.iterrows():
        code = str(row.get("code", "")).zfill(6)
        if code in split2_codes:
            keep_codes.add(code)
            continue
        prev_final = pd.to_numeric(pd.Series([row.get("final_score_pending")]), errors="coerce").iloc[0]
        cur_final = pd.to_numeric(pd.Series([row.get("final_score")]), errors="coerce").iloc[0]
        prev_score = pd.to_numeric(pd.Series([row.get("score_pending")]), errors="coerce").iloc[0]
        cur_score = pd.to_numeric(pd.Series([row.get("score")]), errors="coerce").iloc[0]

        prev_final_score_by_code[code] = float(prev_final) if pd.notna(prev_final) else float("nan")
        prev_score_by_code[code] = float(prev_score) if pd.notna(prev_score) else float("nan")

        score_gap = None
        current_score_basis = None
        if pd.notna(prev_final) and pd.notna(cur_final):
            score_gap = abs(float(cur_final) - float(prev_final))
            current_score_basis = float(cur_final)
        elif pd.notna(prev_score) and pd.notna(cur_score):
            score_gap = abs(float(cur_score) - float(prev_score))
            current_score_basis = float(cur_score)

        if score_gap is not None:
            score_gap_by_code[code] = float(score_gap)
        if market_gate_block:
            failed_codes.append(code)
            failed_reason_counts["MARKET_GATE_FAILED"] = int(failed_reason_counts.get("MARKET_GATE_FAILED", 0)) + 1
            continue
        if sector_gate_enabled:
            sector_allowed = None
            if "sector_entry_allowed" in row.index:
                sector_allowed = str(row.get("sector_entry_allowed") or "").strip().upper() in {"TRUE", "1", "Y", "YES"}
            elif "sector_score" in row.index:
                sector_score = pd.to_numeric(pd.Series([row.get("sector_score")]), errors="coerce").iloc[0]
                if pd.notna(sector_score):
                    sector_allowed = float(sector_score) > 0
            if sector_allowed is False:
                failed_codes.append(code)
                failed_reason_counts["SECTOR_GATE_FAILED"] = int(failed_reason_counts.get("SECTOR_GATE_FAILED", 0)) + 1
                continue
        if min_final_score > 0 and current_score_basis is not None and current_score_basis < min_final_score:
            min_score_failed += 1
            failed_codes.append(code)
            failed_reason_counts["MIN_SCORE_FAILED"] = int(failed_reason_counts.get("MIN_SCORE_FAILED", 0)) + 1
            continue
        if score_gap_max > 0 and score_gap is not None and score_gap > score_gap_max:
            score_gap_failed += 1
            failed_codes.append(code)
            failed_reason_counts["SCORE_GAP_FAILED"] = int(failed_reason_counts.get("SCORE_GAP_FAILED", 0)) + 1
            continue
        keep_codes.add(code)

    if not keep_codes:
        loaded = int(len(pending))
        return pd.DataFrame(), {
            "loaded": loaded,
            "revalidated": 0,
            "dropped": loaded,
            "score_gap_failed": int(score_gap_failed),
            "min_score_failed": int(min_score_failed),
            "failed_codes": failed_codes,
            "failed_reason_counts": failed_reason_counts,
        }

    carry_meta = pending.drop_duplicates(["code"], keep="first").set_index("code")
    out = cur[cur["code"].isin(keep_codes)].copy()
    out_code_norm = out["code"].astype(str).str.zfill(6) if "code" in out.columns else pd.Series(dtype=str)

    # split-entry 2nd is residual execution of an already-open first entry.
    # It must not disappear just because the symbol is absent from today's
    # fresh candidate file; the actual safety checks still run later.
    split2_missing_codes = sorted(split2_codes - set(out["code"].astype(str).str.zfill(6).tolist()))
    if split2_missing_codes:
        split2_missing = pending[pending["code"].isin(split2_missing_codes)].copy()
        split2_missing["_carryover"] = 1
        split2_missing["carry_reason"] = "ENTRY_RETRY_READY"
        split2_missing["carry_origin_signal_date"] = split2_missing["signal_date"].astype(str)
        split2_missing["carry_revalidated_at"] = now_ts()
        split2_missing["carry_prev_final_score"] = pd.to_numeric(split2_missing.get("final_score"), errors="coerce")
        split2_missing["carry_prev_score"] = pd.to_numeric(split2_missing.get("score"), errors="coerce")
        split2_missing["carry_score_gap"] = pd.NA
        split2_missing["entry_day_override"] = now_ymd()
        split2_missing["fallback_stage"] = pd.to_numeric(split2_missing.get("fallback_stage"), errors="coerce").fillna(0).astype(int)
        split2_missing["execution_pool"] = True
        split2_missing["sector_entry_allowed"] = True
        if "final_score" not in split2_missing.columns:
            split2_missing["final_score"] = 1.0
        split2_missing["final_score"] = pd.to_numeric(split2_missing["final_score"], errors="coerce").fillna(1.0)
        if "score" not in split2_missing.columns:
            split2_missing["score"] = split2_missing["final_score"]
        split2_missing["score"] = pd.to_numeric(split2_missing["score"], errors="coerce").fillna(split2_missing["final_score"])
        out = _concat_drop_all_na_columns([out, split2_missing], ignore_index=True)
        out_code_norm = out["code"].astype(str).str.zfill(6) if "code" in out.columns else pd.Series(dtype=str, index=out.index)

    out["_carryover"] = 1
    out["carry_reason"] = "ENTRY_RETRY_READY"
    out["carry_origin_signal_date"] = out_code_norm.map(carry_meta["signal_date"].to_dict()).astype(str)
    out["carry_revalidated_at"] = now_ts()
    out["carry_prev_final_score"] = out_code_norm.map(prev_final_score_by_code)
    out["carry_prev_score"] = out_code_norm.map(prev_score_by_code)
    out["carry_score_gap"] = out_code_norm.map(score_gap_by_code)
    if "carryover_count" in carry_meta.columns:
        out["carryover_count"] = out_code_norm.map(carry_meta["carryover_count"].to_dict())
    if "captured_at" in carry_meta.columns:
        out["carry_captured_at"] = out_code_norm.map(carry_meta["captured_at"].to_dict())
    if "entry_day_override" in carry_meta.columns:
        out["entry_day_override"] = out_code_norm.map(carry_meta["entry_day_override"].to_dict()).astype(str)
    if "fallback_stage" in carry_meta.columns:
        out["fallback_stage"] = pd.to_numeric(
            out_code_norm.map(carry_meta["fallback_stage"].to_dict()),
            errors="coerce",
        ).fillna(0).astype(int)
    if "carry_origin_reason" in carry_meta.columns:
        out["carry_origin_reason"] = out_code_norm.map(carry_meta["carry_origin_reason"].to_dict()).astype(str)
    if "carry_origin_reason" in out.columns:
        recheck_mask = out["carry_origin_reason"].astype(str).str.strip().str.upper().eq("CLOSE_CUTOFF_RECHECK")
        if bool(recheck_mask.any()):
            out.loc[recheck_mask, "signal_date"] = now_ymd()
            out.loc[recheck_mask, "carry_reason"] = "ENTRY_RECHECK_READY"
    for _carry_field in [
        "entry_timing",
        "surge_type_entry_timing",
        "_surge_immediate",
        "surge_type",
        "surge_per_symbol_alloc_pct",
        "surge_total_alloc_pct",
        "surge_type_qty_multiplier",
        "surge_type_policy_note",
        "surge_type_first_ratio",
        "surge_type_stop_pct",
        "surge_type_tp_pct",
        "surge_type_max_hold_days",
    ]:
        if _carry_field in carry_meta.columns:
            out[_carry_field] = out_code_norm.map(carry_meta[_carry_field].to_dict())
    if "split_entry_2nd" in carry_meta.columns:
        out["split_entry_2nd"] = out_code_norm.map(carry_meta["split_entry_2nd"].to_dict())
    if "split_remaining_qty" in carry_meta.columns:
        out["split_remaining_qty"] = out_code_norm.map(carry_meta["split_remaining_qty"].to_dict())
    if "split_first_entry_price" in carry_meta.columns:
        out["split_first_entry_price"] = out_code_norm.map(carry_meta["split_first_entry_price"].to_dict())
    if "split_first_order_id" in carry_meta.columns:
        out["split_first_order_id"] = out_code_norm.map(carry_meta["split_first_order_id"].to_dict())
    if "signal_date" in carry_meta.columns and "split_entry_2nd" in out.columns:
        split_mask = out["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"})
        pending_signal_dates = out_code_norm.map(carry_meta["signal_date"].to_dict()).astype(str)
        out.loc[split_mask, "signal_date"] = pending_signal_dates.loc[split_mask]
        out.loc[split_mask, "execution_pool"] = True
        out.loc[split_mask, "sector_entry_allowed"] = True
    if "signal_date" not in out.columns or out["signal_date"].astype(str).str.len().eq(8).sum() == 0:
        out["signal_date"] = now_ymd()

    revalidated = int(len(out))
    loaded = int(len(pending))
    dropped = max(0, loaded - revalidated)
    return out, {
        "loaded": loaded,
        "revalidated": revalidated,
        "dropped": dropped,
        "score_gap_failed": int(score_gap_failed),
        "min_score_failed": int(min_score_failed),
        "failed_codes": failed_codes,
        "failed_reason_counts": failed_reason_counts,
    }

def _load_replay_orders(max_age_days: int) -> pd.DataFrame:
    q = _load_replay_orders_raw(max_age_days=max_age_days)
    if q.empty:
        return q

    today = now_ymd()
    q = q[q["next_session_ymd"] == today].copy()
    if q.empty:
        return q

    out = pd.DataFrame(
        {
            "signal_date": q["exec_date"].where(q["exec_date"].astype(str).str.len() == 8, q["next_session_ymd"]),
            "entry_day_override": q["next_session_ymd"],
            "code": q["code"],
            "name": q["name"] if "name" in q.columns else "",
            "carry_reason": "OPEN_ORDER_REPLAY",
            "captured_at": now_ts(),
            "qty_override": q["remaining_qty"],
            "remaining_qty": q["remaining_qty"],
            "remaining_action": q["remaining_action"] if "remaining_action" in q.columns else "",
            "replay_source_order_id": q["replay_source_order_id"] if "replay_source_order_id" in q.columns else (q["order_id"] if "order_id" in q.columns else ""),
            "replay_source_intent_id": q["replay_source_intent_id"] if "replay_source_intent_id" in q.columns else (q["intent_id"] if "intent_id" in q.columns else ""),
            "replay_source_trace_id": q["replay_source_trace_id"] if "replay_source_trace_id" in q.columns else (q["trace_id"] if "trace_id" in q.columns else ""),
            "replay_policy": q["replay_policy"] if "replay_policy" in q.columns else "",
            "time_in_force": q["time_in_force"] if "time_in_force" in q.columns else "",
            "replay_chain_id": q["replay_chain_id"] if "replay_chain_id" in q.columns else "",
            "replay_depth": q["replay_depth"] if "replay_depth" in q.columns else 0,
            "_replay_order": 1,
        }
    )
    out = out.sort_values(["entry_day_override", "code"], ascending=[True, True]).drop_duplicates(["code", "entry_day_override"], keep="first")
    return out

def _save_pending_signals(rows: List[Dict[str, Any]], max_age_days: int, px: Optional[pd.DataFrame] = None) -> None:
    all_df = pd.DataFrame(rows)
    if all_df.empty:
        _write_empty_pending_signals_file()
        return
    all_df["signal_date"] = all_df["signal_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    all_df["code"] = all_df["code"].astype(str).str.zfill(6)
    all_df = all_df[(all_df["signal_date"].str.len() == 8) & (all_df["code"].str.len() == 6)].copy()
    # apply per-row carryover_max_age_days if present, otherwise use global max_age_days
    if "carryover_max_age_days" in all_df.columns:
        row_age = pd.to_numeric(all_df["carryover_max_age_days"], errors="coerce").fillna(max_age_days).astype(int)
        filtered_parts: List[pd.DataFrame] = []
        for age_val in sorted(row_age.unique()):
            subset = all_df[row_age == age_val]
            filtered_parts.append(_filter_pending_by_trading_age(subset, px=px, max_age_days=int(age_val)))
        all_df = pd.concat(filtered_parts, ignore_index=True) if filtered_parts else all_df.iloc[0:0]
    else:
        all_df = _filter_pending_by_trading_age(all_df, px=px, max_age_days=max_age_days)

    if all_df.empty:
        _write_empty_pending_signals_file()
        return

    all_df = all_df.sort_values(["signal_date", "code"], ascending=[False, True]).drop_duplicates(["code", "signal_date"], keep="first")
    for col in PENDING_SIGNALS_SCHEMA:
        if col not in all_df.columns:
            all_df[col] = pd.NA
    all_df = all_df[PENDING_SIGNALS_SCHEMA].copy()
    all_df.to_csv(PENDING_SIGNALS_PATH, index=False, encoding="utf-8-sig")

def _write_pending_status(
    market_regime: str,
    max_new: int,
    max_new_surge: int,
    max_new_zero_reason: str,
    candidates_after_caps: int,
    entry_ready: int,
    filled: int,
    no_next_day: int,
    replay_due_today: int,
    replay_future_queue_len: int,
    replay_expired_rows: int,
    replay_used_today: int,
    replay_discard_rows: int,
    replay_manual_review_rows: int,
    recovery_auto_recovered_positions: int,
    recovery_manual_review_positions: int,
    replay_regeneration_reason: str,
    replay_queue_scan: Optional[Dict[str, Any]],
    replay_quarantine: Optional[Dict[str, Any]],
    replay_prune: Optional[Dict[str, Any]],
    replay_consistency: Optional[Dict[str, Any]],
    replay_consistency_remediation: Optional[Dict[str, Any]],
    replay_summary_sync: Optional[Dict[str, Any]],
    entry_exit_lifecycle: Optional[Dict[str, Any]],
    state_machine_summary: Optional[Dict[str, Any]],
    sell_order_lifecycle_summary: Optional[Dict[str, Any]],
    partial_exit_policy_summary: Optional[Dict[str, Any]],
    sell_recovery_chain_summary: Optional[Dict[str, Any]],
    sell_adapter_summary: Optional[Dict[str, Any]],
    sell_validation_report: Optional[Dict[str, Any]],
    order_pre_validation_summary: Optional[Dict[str, Any]],
    order_lifecycle_summary: Optional[Dict[str, Any]],
    order_amend_cancel_summary: Optional[Dict[str, Any]],
    order_chain_ssot_summary: Optional[Dict[str, Any]],
    order_recovery_replay_summary: Optional[Dict[str, Any]],
    order_session_rule_summary: Optional[Dict[str, Any]],
    order_validation_report: Optional[Dict[str, Any]],
    symbol_stop_summary: Optional[Dict[str, Any]],
    legacy_derisk_summary: Optional[Dict[str, Any]],
    carryover_revalidate_summary: Optional[Dict[str, Any]],
    surge_inject_status: Optional[Dict[str, Any]],
    max_positions_meta: Optional[Dict[str, Any]],
    entry_decision_rows: Optional[List[Dict[str, Any]]],
    max_age_days: int,
) -> None:
    try:
        pending_rows = _load_pending_signals(max_age_days=max_age_days)
        pending_signal_rows_raw = int(len(pending_rows))
        pending_split_entry_2nd_rows = 0
        if pending_signal_rows_raw > 0 and "carry_origin_reason" in pending_rows.columns:
            pending_split_entry_2nd_rows = int(
                (pending_rows["carry_origin_reason"].astype(str).str.strip().str.upper() == "SPLIT_ENTRY_2ND").sum()
            )
        pending_signal_rows = int(max(0, pending_signal_rows_raw - pending_split_entry_2nd_rows))
        pending_queue_len = int(pending_signal_rows + max(0, int(replay_due_today)))
        open_positions_after = int(((entry_exit_lifecycle or {}).get("open_positions_after")) or 0)
        entry_fill_rows = int(((entry_exit_lifecycle or {}).get("entry_fill_rows")) or 0)
        carry_reason_counts: Dict[str, int] = {}
        if pending_signal_rows_raw > 0 and "carry_reason" in pending_rows.columns:
            carry_reason_counts = {
                str(k): int(v)
                for k, v in pending_rows["carry_reason"].astype(str).value_counts(dropna=False).to_dict().items()
            }
        carry_origin_reason_counts: Dict[str, int] = {}
        if pending_signal_rows_raw > 0 and "carry_origin_reason" in pending_rows.columns:
            carry_origin_reason_counts = {
                str(k): int(v)
                for k, v in pending_rows["carry_origin_reason"].astype(str).value_counts(dropna=False).to_dict().items()
            }
        entry_decision_payload: List[Dict[str, Any]] = []
        entry_decision_reason_counts: Dict[str, int] = {}
        for row in (entry_decision_rows or [])[:50]:
            if not isinstance(row, dict):
                continue
            reason = str(row.get("reason") or "").strip()
            if reason:
                reason_key = reason.upper()
                entry_decision_reason_counts[reason_key] = int(entry_decision_reason_counts.get(reason_key, 0)) + 1
            entry_decision_payload.append(
                {
                    "code": str(row.get("code") or "").strip().zfill(6),
                    "signal_date": str(row.get("signal_date") or ""),
                    "signal": str(row.get("signal") or ""),
                    "reason": reason,
                    "rank_score": row.get("rank_score", ""),
                    "is_surge": bool(row.get("is_surge", False)),
                    "is_replay": bool(row.get("is_replay", False)),
                    "is_carryover": bool(row.get("is_carryover", False)),
                    "entry_day": str(row.get("entry_day") or ""),
                    "entry_price": row.get("entry_price", ""),
                    "qty": row.get("qty", ""),
                    "order_id": str(row.get("order_id") or ""),
                    "strategy_type": str(row.get("strategy_type") or ("SURGE" if row.get("is_surge") else ("CARRYOVER" if row.get("is_carryover") else "NORMAL"))),
                    "final_score": row.get("final_score", ""),
                    "alloc_weight": row.get("alloc_weight", ""),
                }
            )
        if pending_queue_len > 0 and filled > 0:
            status = "ACTIVE_OR_PARTIAL"
            status_reason = "ACTIVE_OR_PARTIAL"
        elif pending_queue_len > 0:
            status = "ACTIVE"
            status_reason = "PENDING_QUEUE_REMAINS"
        elif open_positions_after > 0:
            status = "ACTIVE"
            status_reason = "OPEN_POSITION_ACTIVE"
        elif filled > 0:
            if open_positions_after > 0 or entry_fill_rows > 0:
                status = "ACTIVE"
                status_reason = "ENTRY_FILLED_OPEN_POSITION"
            else:
                status = "PARTIAL"
                status_reason = "FILLED_WITHOUT_PENDING"
        else:
            status = "IDLE"
            status_reason = "NO_PENDING_QUEUE"

        raw_candidates_after_caps = int(candidates_after_caps)
        effective_candidates_after_caps = 0 if int(max_new) <= 0 else raw_candidates_after_caps
        surge_inject_payload = surge_inject_status or {}

        payload = {
            "generated_at": now_ts(),
            "run_label": str(RUN_LABEL or ""),
            "paper_session_id": str(PAPER_SESSION_ID or ""),
            "status": status,
            "status_reason": status_reason,
            "deferred_all_no_next_day": bool(no_next_day >= effective_candidates_after_caps and effective_candidates_after_caps > 0),
            "market_regime": str(market_regime or ""),
            "max_new": int(max_new),
            "max_new_surge": int(max_new_surge),
            "max_new_zero_reason": str(max_new_zero_reason or ""),
            "max_positions_meta": max_positions_meta or {},
            "candidates_after_caps": int(effective_candidates_after_caps),
            "entry_ready": int(entry_ready),
            "filled": int(filled),
            "no_next_day": int(no_next_day),
            "pending_queue_len": pending_queue_len,
            "pending_signal_rows": pending_signal_rows,
            "pending_queue_len_raw": int(pending_signal_rows_raw + max(0, int(replay_due_today))),
            "pending_signal_rows_raw": pending_signal_rows_raw,
            "pending_split_entry_2nd_rows": pending_split_entry_2nd_rows,
            "carry_reason_counts": carry_reason_counts,
            "carry_origin_reason_counts": carry_origin_reason_counts,
            "entry_decision_rows": entry_decision_payload,
            "entry_decision_reason_counts": entry_decision_reason_counts,
            "surge_inject_status": surge_inject_payload,
            "afternoon_session_block_count": int(surge_inject_payload.get("afternoon_session_block_count", 0) or 0),
            "price_vol_divergence_block_count": int(surge_inject_payload.get("price_vol_divergence_block_count", 0) or 0),
            "choppy_trend_block_count": int(surge_inject_payload.get("choppy_trend_block_count", 0) or 0),
            "surge_type_blocked_count": int(surge_inject_payload.get("surge_type_blocked_count", 0) or 0),
            "replay_queue_len": int(replay_due_today),
            "replay_future_queue_len": int(replay_future_queue_len),
            "replay_expired_rows": int(replay_expired_rows),
            "replay_used_today": int(replay_used_today),
            "replay_discard_rows": int(replay_discard_rows),
            "replay_manual_review_rows": int(replay_manual_review_rows),
            "recovery_auto_recovered_positions": int(recovery_auto_recovered_positions),
            "recovery_manual_review_positions": int(recovery_manual_review_positions),
            "replay_regeneration_reason": str(replay_regeneration_reason or ""),
            "replay_queue_scan": replay_queue_scan or {},
            "replay_quarantine": replay_quarantine or {},
            "replay_prune": replay_prune or {},
            "replay_consistency": replay_consistency or {},
            "replay_consistency_remediation": replay_consistency_remediation or {},
            "replay_summary_sync": replay_summary_sync or {},
            "entry_exit_lifecycle": entry_exit_lifecycle or {},
            "state_machine_summary": state_machine_summary or {},
            "sell_order_lifecycle_summary": sell_order_lifecycle_summary or {},
            "partial_exit_policy_summary": partial_exit_policy_summary or {},
            "sell_recovery_chain_summary": sell_recovery_chain_summary or {},
            "sell_adapter_summary": sell_adapter_summary or {},
            "sell_validation_report": sell_validation_report or {},
            "order_pre_validation_summary": order_pre_validation_summary or {},
            "order_lifecycle_summary": order_lifecycle_summary or {},
            "order_amend_cancel_summary": order_amend_cancel_summary or {},
            "order_chain_ssot_summary": order_chain_ssot_summary or {},
            "order_recovery_replay_summary": order_recovery_replay_summary or {},
            "order_session_rule_summary": order_session_rule_summary or {},
            "order_validation_report": order_validation_report or {},
            "symbol_stop_summary": symbol_stop_summary or {},
            "legacy_derisk_summary": legacy_derisk_summary or {},
            "carryover_revalidate_summary": carryover_revalidate_summary or {},
        }
        PENDING_STATUS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[WARN] pending status write failed: {type(e).__name__}: {e}")

def _build_entry_exit_lifecycle(
    open_positions_before: int,
    open_positions_after: int,
    opened_position_rows: int,
    closed_position_rows: int,
    entry_fill_rows: int,
    exit_fill_rows: int,
    closed_trade_rows: int,
    pending_signal_rows: int,
    replay_due_today: int,
    replay_manual_review_rows: int,
    recovery_manual_review_positions: int,
    replay_discard_rows: int,
) -> Dict[str, Any]:
    # 신규 BUY 체결은 기존 포지션 증액일 수 있으므로, 포지션 수 증분은
    # "신규 생성 포지션 수" 기준으로 계산해야 open count와 일치한다.
    expected_open_after = int(open_positions_before) + int(opened_position_rows) - int(closed_position_rows)
    issues: List[str] = []
    status = "PASS"

    exit_fill_rows_i = int(exit_fill_rows)
    closed_trade_rows_i = int(closed_trade_rows)
    has_exit_trade_mismatch = bool(exit_fill_rows_i != closed_trade_rows_i)
    exit_trade_delta = exit_fill_rows_i - closed_trade_rows_i
    open_delta = int(expected_open_after) - int(open_positions_after)
    tolerated_retry_open_delta = bool(
        int(pending_signal_rows) > 0
        and int(replay_due_today) == 0
        and int(replay_manual_review_rows) == 0
        and int(recovery_manual_review_positions) == 0
        and int(replay_discard_rows) == 0
        and abs(int(open_delta)) <= int(max(1, pending_signal_rows))
    )
    tolerated_exit_trade_delta = bool(
        int(pending_signal_rows) > 0
        and int(replay_due_today) == 0
        and int(replay_manual_review_rows) == 0
        and int(recovery_manual_review_positions) == 0
        and int(replay_discard_rows) == 0
        and exit_fill_rows_i >= closed_trade_rows_i
        and int(exit_trade_delta) <= int(max(1, pending_signal_rows))
    )
    tolerated_legacy_exit_trade_gap = bool(
        has_exit_trade_mismatch
        and int(exit_fill_rows_i) > 0
        and int(pending_signal_rows) == 0
        and int(replay_due_today) == 0
        and int(replay_manual_review_rows) == 0
        and int(recovery_manual_review_positions) == 0
        and int(replay_discard_rows) == 0
        and int(open_delta) == 0
        and int(closed_position_rows) == int(exit_fill_rows_i)
        and int(closed_trade_rows_i) <= int(exit_fill_rows_i)
    )
    if (
        has_exit_trade_mismatch
        and not tolerated_exit_trade_delta
        and not tolerated_legacy_exit_trade_gap
    ):
        issues.append(
            f"exit_trade_mismatch:exit_fills={exit_fill_rows_i} closed_trades={closed_trade_rows_i}"
        )
    if expected_open_after != int(open_positions_after) and not tolerated_retry_open_delta:
        issues.append(
            f"open_position_mismatch:expected={expected_open_after} actual={int(open_positions_after)}"
        )
    if int(replay_manual_review_rows) > 0:
        issues.append(f"manual_replay_remaining={int(replay_manual_review_rows)}")
    if int(recovery_manual_review_positions) > 0:
        issues.append(f"manual_recovery_positions={int(recovery_manual_review_positions)}")

    if issues:
        status = "FAIL"
    elif tolerated_legacy_exit_trade_gap:
        status = "CAUTION"
    elif int(pending_signal_rows) > 0 or int(replay_due_today) > 0 or int(replay_discard_rows) > 0:
        status = "CAUTION"

    return {
        "generated_at": now_ts(),
        "status": status,
        "open_positions_before": int(open_positions_before),
        "open_positions_after": int(open_positions_after),
        "opened_position_rows": int(opened_position_rows),
        "closed_position_rows": int(closed_position_rows),
        "entry_fill_rows": int(entry_fill_rows),
        "exit_fill_rows": int(exit_fill_rows),
        "closed_trade_rows": int(closed_trade_rows),
        "exit_trade_delta": int(exit_trade_delta),
        "exit_trade_delta_tolerated": bool(tolerated_exit_trade_delta),
        "exit_trade_delta_legacy_tolerated": bool(tolerated_legacy_exit_trade_gap),
        "expected_open_positions_after": int(expected_open_after),
        "open_position_delta": int(open_delta),
        "open_position_delta_tolerated": bool(tolerated_retry_open_delta),
        "pending_signal_rows": int(pending_signal_rows),
        "replay_due_today": int(replay_due_today),
        "replay_manual_review_rows": int(replay_manual_review_rows),
        "recovery_manual_review_positions": int(recovery_manual_review_positions),
        "replay_discard_rows": int(replay_discard_rows),
        "issues": issues,
    }

def _build_state_machine_summary(
    entry_exit_lifecycle: Dict[str, Any],
    replay_recovery_summary: Dict[str, Any],
    replay_queue_status: Dict[str, Any],
    replay_consistency: Dict[str, Any],
    recovery_summary: Dict[str, Any],
) -> Dict[str, Any]:
    lifecycle = entry_exit_lifecycle or {}
    replay = replay_recovery_summary or {}
    queue = replay_queue_status or {}
    consistency = replay_consistency or {}
    recovery = recovery_summary or {}

    lifecycle_status = str(lifecycle.get("status") or "").strip().upper()
    consistency_status = str(consistency.get("status") or "").strip().upper()
    open_after = int(lifecycle.get("open_positions_after") or 0)
    pending_rows = int(lifecycle.get("pending_signal_rows") or 0)
    replay_due_today = int(lifecycle.get("replay_due_today") or 0)
    replay_future_rows = int(queue.get("future_rows") or 0)
    replay_expired_rows = int(queue.get("expired_rows") or 0)
    replay_manual_review_rows = int(replay.get("manual_review_rows") or 0)
    replay_discard_rows = int(replay.get("discard_rows") or 0)
    recovery_manual_review_positions = int(recovery.get("manual_review_positions") or 0)
    closed_trade_rows = int(lifecycle.get("closed_trade_rows") or 0)
    entry_fill_rows = int(lifecycle.get("entry_fill_rows") or 0)
    exit_fill_rows = int(lifecycle.get("exit_fill_rows") or 0)

    states_present: List[str] = []
    if pending_rows > 0:
        states_present.append("PENDING_SIGNAL")
    if replay_due_today > 0:
        states_present.append("REPLAY_DUE")
    if replay_future_rows > 0:
        states_present.append("REPLAY_FUTURE")
    if replay_expired_rows > 0:
        states_present.append("REPLAY_EXPIRED")
    if replay_discard_rows > 0:
        states_present.append("REPLAY_DISCARDED")
    if replay_manual_review_rows > 0 or recovery_manual_review_positions > 0:
        states_present.append("MANUAL_REVIEW")
    if entry_fill_rows > 0:
        states_present.append("ENTRY_FILLED")
    if open_after > 0:
        states_present.append("OPEN_POSITION")
    if exit_fill_rows > 0:
        states_present.append("EXIT_FILLED")
    if closed_trade_rows > 0:
        states_present.append("CLOSED_TRADE")
    if not states_present:
        states_present.append("IDLE")

    issues: List[str] = []
    if lifecycle_status == "FAIL":
        issues.append("lifecycle_fail")
    if consistency_status not in {"", "OK"}:
        issues.append(f"replay_consistency_{consistency_status.lower()}")
    if replay_manual_review_rows > 0:
        issues.append(f"manual_replay_rows={replay_manual_review_rows}")
    if recovery_manual_review_positions > 0:
        issues.append(f"manual_recovery_positions={recovery_manual_review_positions}")

    status = "PASS"
    if issues:
        status = "FAIL"
    elif pending_rows > 0 or replay_due_today > 0 or replay_future_rows > 0 or replay_discard_rows > 0:
        status = "CAUTION"

    return {
        "generated_at": now_ts(),
        "status": status,
        "lifecycle_status": lifecycle_status or "",
        "replay_consistency_status": consistency_status or "",
        "states_present": states_present,
        "allowed_states": [
            "IDLE",
            "PENDING_SIGNAL",
            "ENTRY_FILLED",
            "OPEN_POSITION",
            "REPLAY_DUE",
            "REPLAY_FUTURE",
            "REPLAY_EXPIRED",
            "REPLAY_DISCARDED",
            "MANUAL_REVIEW",
            "EXIT_FILLED",
            "CLOSED_TRADE",
        ],
        "allowed_transitions": [
            "IDLE->PENDING_SIGNAL",
            "PENDING_SIGNAL->ENTRY_FILLED",
            "ENTRY_FILLED->OPEN_POSITION",
            "OPEN_POSITION->REPLAY_DUE",
            "REPLAY_DUE->ENTRY_FILLED",
            "OPEN_POSITION->EXIT_FILLED",
            "EXIT_FILLED->CLOSED_TRADE",
            "OPEN_POSITION->MANUAL_REVIEW",
            "REPLAY_DUE->MANUAL_REVIEW",
            "REPLAY_DUE->REPLAY_DISCARDED",
        ],
        "counts": {
            "pending_signal_rows": pending_rows,
            "replay_due_today": replay_due_today,
            "replay_future_rows": replay_future_rows,
            "replay_expired_rows": replay_expired_rows,
            "replay_discard_rows": replay_discard_rows,
            "replay_manual_review_rows": replay_manual_review_rows,
            "recovery_manual_review_positions": recovery_manual_review_positions,
            "entry_fill_rows": entry_fill_rows,
            "open_positions_after": open_after,
            "exit_fill_rows": exit_fill_rows,
            "closed_trade_rows": closed_trade_rows,
        },
        "issues": issues,
    }

def _build_symbol_stop_summary(
    stop_loss: float,
    take_profit: Optional[float],
    trail_pct: Optional[float],
    fills_new: List[List[Any]],
    trades_new: List[List[Any]],
    persisted_stop_sell_rows: Optional[int] = None,
    persisted_stop_trade_rows: Optional[int] = None,
    persisted_stop_reasons: Optional[List[str]] = None,
) -> Dict[str, Any]:
    stop_sell_rows = 0
    stop_trade_rows = 0
    stop_reasons: List[str] = []

    for row in fills_new or []:
        note = str(row[6] if len(row) >= 7 else row[10] if len(row) >= 11 else "")
        if "exit_reason=STOP;" in note or "exit_reason=STOP_GAP;" in note:
            stop_sell_rows += 1

    for row in trades_new or []:
        exit_reason = str(row[8]).strip().upper() if len(row) >= 9 else ""
        note = str(row[-1] or "") if row else ""
        if not exit_reason and "exit_reason=" in note:
            exit_reason = note.split("exit_reason=", 1)[1].split(";", 1)[0].strip().upper()
        if exit_reason in {"STOP", "STOP_GAP"}:
            stop_trade_rows += 1
            stop_reasons.append(exit_reason)

    if persisted_stop_sell_rows is not None:
        stop_sell_rows = int(persisted_stop_sell_rows)
    if persisted_stop_trade_rows is not None:
        stop_trade_rows = int(persisted_stop_trade_rows)
    if persisted_stop_reasons is not None:
        stop_reasons = [str(x).strip().upper() for x in (persisted_stop_reasons or []) if str(x).strip()]

    issues: List[str] = []
    status = "PASS"
    if stop_trade_rows > stop_sell_rows:
        status = "FAIL"
        issues.append("stop_trade_rows_exceed_sell_rows")

    return {
        "generated_at": now_ts(),
        "status": status,
        "stop_loss": float(stop_loss or 0.0),
        "take_profit": (None if take_profit is None else float(take_profit)),
        "trail_pct": (None if trail_pct is None else float(trail_pct)),
        "counts": {
            "stop_sell_rows": int(stop_sell_rows),
            "stop_trade_rows": int(stop_trade_rows),
        },
        "stop_reasons": sorted(set(stop_reasons)),
        "issues": issues,
    }

def _collect_persisted_today_runtime_metrics(schema: str, runtime_ymd: str) -> Dict[str, Any]:
    target_ymd = _norm_ymd_text(runtime_ymd)
    if len(target_ymd) != 8:
        return {
            "entry_fill_rows": 0,
            "exit_fill_rows": 0,
            "closed_trade_rows": 0,
            "stop_sell_rows": 0,
            "stop_trade_rows": 0,
            "stop_reasons": [],
        }

    fills_df = read_csv_safe(FILLS)
    trades_df = read_csv_safe(TRADES)

    entry_fill_rows = 0
    exit_fill_rows = 0
    stop_sell_rows = 0
    stop_trade_rows = 0
    stop_reasons: List[str] = []
    closed_trade_rows = 0

    if isinstance(fills_df, pd.DataFrame) and not fills_df.empty:
        fills_ymd = pd.Series("", index=fills_df.index, dtype="object")
        if "date" in fills_df.columns:
            fills_ymd = fills_df["date"].map(_norm_ymd_text)
        elif "ts" in fills_df.columns:
            fills_ymd = fills_df["ts"].map(_extract_ymd_from_ts_text)
        elif "datetime" in fills_df.columns:
            fills_ymd = fills_df["datetime"].map(_extract_ymd_from_ts_text)
        fills_today = fills_df.loc[fills_ymd == target_ymd].copy()
        if not fills_today.empty and "side" in fills_today.columns:
            fill_side = fills_today["side"].astype(str).str.strip().str.upper()
            entry_fill_rows = int((fill_side == "BUY").sum())
            exit_fill_rows = int((fill_side == "SELL").sum())
            if "note" in fills_today.columns:
                sell_notes = fills_today.loc[fill_side == "SELL", "note"].astype(str)
                stop_mask = sell_notes.str.contains("exit_reason=STOP;", regex=False) | sell_notes.str.contains("exit_reason=STOP_GAP;", regex=False)
                stop_sell_rows = int(stop_mask.sum())

    if isinstance(trades_df, pd.DataFrame) and not trades_df.empty:
        trades_ymd = pd.Series("", index=trades_df.index, dtype="object")
        if "exit_date" in trades_df.columns:
            trades_ymd = trades_df["exit_date"].map(_norm_ymd_text)
        elif "exit_ts" in trades_df.columns:
            trades_ymd = trades_df["exit_ts"].map(_extract_ymd_from_ts_text)
        trades_today = trades_df.loc[trades_ymd == target_ymd].copy()
        if not trades_today.empty:
            closed_trade_rows = int(len(trades_today))
            if "exit_reason" in trades_today.columns:
                exit_reason_series = trades_today["exit_reason"].astype(str).str.strip().str.upper()
            else:
                exit_reason_series = pd.Series("", index=trades_today.index, dtype="object")
            if "note" in trades_today.columns:
                note_series = trades_today["note"].astype(str)
                missing_mask = exit_reason_series.eq("") & note_series.str.contains("exit_reason=", regex=False)
                extracted = note_series.loc[missing_mask].str.extract(r"exit_reason=([^;]+)", expand=False).fillna("")
                exit_reason_series.loc[missing_mask] = extracted.astype(str).str.strip().str.upper()
            stop_reason_series = exit_reason_series[exit_reason_series.isin(["STOP", "STOP_GAP"])]
            stop_trade_rows = int(len(stop_reason_series))
            stop_reasons = sorted(set(stop_reason_series.astype(str).tolist()))

    return {
        "entry_fill_rows": int(entry_fill_rows),
        "exit_fill_rows": int(exit_fill_rows),
        "closed_trade_rows": int(closed_trade_rows),
        "stop_sell_rows": int(stop_sell_rows),
        "stop_trade_rows": int(stop_trade_rows),
        "stop_reasons": stop_reasons,
    }

def _collect_persisted_today_order_artifacts(schema: str, runtime_ymd: str) -> Dict[str, Any]:
    target_ymd = _norm_ymd_text(runtime_ymd)
    fills_df = read_csv_safe(FILLS)
    trades_df = read_csv_safe(TRADES)

    order_rows: List[Dict[str, Any]] = []
    trade_rows: List[Dict[str, Any]] = []

    sell_lookup: Dict[Tuple[str, str, int, str], Dict[str, Any]] = {}
    sell_artifacts = _collect_persisted_today_sell_artifacts(schema=schema, runtime_ymd=runtime_ymd)
    for srow in list(sell_artifacts.get("sell_trades") or []):
        key = (
            str(srow.get("code") or "").strip().zfill(6),
            str(srow.get("exit_reason") or "").strip().upper(),
            int(_to_int(srow.get("sell_qty"), 0)),
            str(srow.get("source_order_id") or "").strip(),
        )
        sell_lookup[key] = dict(srow)

    if isinstance(fills_df, pd.DataFrame) and not fills_df.empty:
        fills_ymd = pd.Series("", index=fills_df.index, dtype="object")
        if "date" in fills_df.columns:
            fills_ymd = fills_df["date"].map(_norm_ymd_text)
        elif "ts" in fills_df.columns:
            fills_ymd = fills_df["ts"].map(_extract_ymd_from_ts_text)
        elif "datetime" in fills_df.columns:
            fills_ymd = fills_df["datetime"].map(_extract_ymd_from_ts_text)
        fills_today = fills_df.loc[fills_ymd == target_ymd].copy()
        if not fills_today.empty:
            for _, row in fills_today.iterrows():
                note_fields = _parse_note_fields(row.get("note", ""))
                side = str(row.get("side") or "").strip().upper()
                if side not in {"BUY", "SELL"}:
                    continue
                code = str(row.get("code") or "").strip().zfill(6)
                order_id = str(row.get("order_id") or "").strip()
                qty = int(_to_int(row.get("qty"), 0))
                price = float(_to_float(row.get("price"), 0.0) or 0.0)
                base_order_id = order_id
                if "_Q" in base_order_id:
                    base_order_id = base_order_id.rsplit("_Q", 1)[0]
                intended_session = ""
                fill_session = ""
                time_in_force = ""
                state = "FILLED_FULL"
                partial_exit = False
                exit_reason = ""
                source_order_id = str(note_fields.get("source_order_id") or "").strip()
                lineage_origin = str(note_fields.get("lineage_origin") or "").strip().upper()
                replay_chain_id = str(note_fields.get("replay_chain_id") or "").strip()
                if side == "BUY":
                    raw_entry_timing = str(note_fields.get("entry_timing") or "").strip().upper()
                    if raw_entry_timing == "SAME_CLOSE":
                        intended_session = "SAME_CLOSE"
                        fill_session = "SAME_CLOSE"
                    elif raw_entry_timing == "INTRADAY_REALTIME":
                        intended_session = "INTRADAY_REALTIME"
                        fill_session = "INTRADAY_REALTIME"
                    else:
                        intended_session = "NEXT_OPEN"
                        fill_session = "NEXT_OPEN"
                    time_in_force = "DAY"
                    state = "FILLED_FULL"
                else:
                    exit_reason = str(note_fields.get("exit_reason") or "").strip().upper()
                    partial_exit = bool(_truthy(note_fields.get("partial_exit", 0)))
                    intended_session = "REGULAR_DAILY_EXIT"
                    fill_session = "REGULAR_DAILY_EXIT"
                    time_in_force = "DAY"
                    state = "FILLED_PARTIAL" if partial_exit else "FILLED_FULL"
                trade_chain_matched = False
                if side == "SELL":
                    lookup_key = (
                        code,
                        exit_reason,
                        int(_to_int(note_fields.get("sell_qty"), qty)),
                        source_order_id,
                    )
                    trade_chain_matched = lookup_key in sell_lookup
                order_rows.append(
                    {
                        "runtime_ymd": target_ymd,
                        "order_id": order_id,
                        "base_order_id": base_order_id,
                        "code": code,
                        "side": side,
                        "qty": qty,
                        "price": price,
                        "state": state,
                        "intended_session": intended_session,
                        "fill_session": fill_session,
                        "time_in_force": time_in_force,
                        "exit_reason": exit_reason,
                        "partial_exit": bool(partial_exit),
                        "source_order_id": source_order_id,
                        "lineage_origin": lineage_origin,
                        "replay_chain_id": replay_chain_id,
                        "trade_chain_matched": bool(trade_chain_matched),
                        "note_fields": note_fields,
                    }
                )

    if isinstance(trades_df, pd.DataFrame) and not trades_df.empty:
        trades_ymd = pd.Series("", index=trades_df.index, dtype="object")
        if "exit_date" in trades_df.columns:
            trades_ymd = trades_df["exit_date"].map(_norm_ymd_text)
        elif "exit_ts" in trades_df.columns:
            trades_ymd = trades_df["exit_ts"].map(_extract_ymd_from_ts_text)
        trades_today = trades_df.loc[trades_ymd == target_ymd].copy()
        if not trades_today.empty:
            for _, row in trades_today.iterrows():
                note = str(row.get("note") or row.iloc[-1] if len(row.index) > 0 else "")
                note_fields = _parse_note_fields(note)
                trade_rows.append(
                    {
                        "code": str(row.get("code") or "").strip().zfill(6),
                        "exit_reason": str(row.get("exit_reason") or note_fields.get("exit_reason") or "").strip().upper(),
                        "sell_qty": int(_to_int(note_fields.get("sell_qty"), row.get("qty"))),
                        "source_order_id": str(note_fields.get("source_order_id") or "").strip(),
                        "lineage_origin": str(note_fields.get("lineage_origin") or "").strip().upper(),
                        "replay_chain_id": str(note_fields.get("replay_chain_id") or "").strip(),
                        "note_fields": note_fields,
                    }
                )

    buy_fill_rows = int(sum(1 for row in order_rows if str(row.get("side") or "") == "BUY"))
    sell_fill_rows = int(sum(1 for row in order_rows if str(row.get("side") or "") == "SELL"))
    return {
        "runtime_ymd": target_ymd,
        "order_rows": order_rows,
        "trade_rows": trade_rows,
        "buy_fill_rows": buy_fill_rows,
        "sell_fill_rows": sell_fill_rows,
        "total_fill_rows": int(len(order_rows)),
        "closed_trade_rows": int(len(trade_rows)),
    }

def _next_available_legacy_trade_id(used_ids: set[str]) -> str:
    used_nums: set[int] = set()
    for tid in used_ids:
        m = re.fullmatch(r"T(\d+)", str(tid or "").strip())
        if not m:
            continue
        try:
            used_nums.add(int(m.group(1)))
        except Exception:
            continue
    seq = (max(used_nums) + 1) if used_nums else 1
    trade_id = f"T{seq:06d}"
    used_ids.add(trade_id)
    return trade_id

def _recover_missing_sell_trades_legacy(
    *,
    runtime_ymd: str,
    fee_pct: float,
    pos_slip_pct: float,
    sell_tax_pct: float,
) -> Dict[str, Any]:
    target_ymd = _norm_ymd_text(runtime_ymd)
    if len(target_ymd) != 8:
        return {"status": "SKIP", "reason": "runtime_ymd_invalid", "recovered_rows": 0}

    fills_df = read_csv_safe(FILLS)
    trades_df = read_csv_safe(TRADES)
    if not isinstance(fills_df, pd.DataFrame) or fills_df.empty:
        return {"status": "SKIP", "reason": "fills_missing", "recovered_rows": 0}
    if not isinstance(trades_df, pd.DataFrame):
        trades_df = pd.DataFrame(columns=LEGACY_TRADES_HEADER)

    required_fill_cols = {"datetime", "code", "side", "qty", "price", "order_id", "note"}
    if not required_fill_cols.issubset(set(fills_df.columns)):
        return {"status": "SKIP", "reason": "fills_schema_incomplete", "recovered_rows": 0}

    buy_by_order_id: Dict[str, pd.Series] = {}
    for _, row in fills_df.iterrows():
        if str(row.get("side", "")).strip().upper() != "BUY":
            continue
        order_id = str(row.get("order_id", "") or "").strip()
        if order_id:
            buy_by_order_id[order_id] = row

    trade_key_counts: Dict[Tuple[str, str, int, str], int] = {}
    trade_lifecycle_sig_counts: Dict[tuple[str, str, int, str, str, str, int, str], int] = {}
    used_trade_ids: set[str] = set()
    if not trades_df.empty:
        for _, row in trades_df.iterrows():
            tid = str(row.get("trade_id", "") or "").strip()
            if tid:
                used_trade_ids.add(tid)
            exit_day = _norm_ymd_text(row.get("exit_date", ""))
            if exit_day != target_ymd:
                continue
            note_fields = _parse_note_fields(row.get("note", ""))
            exit_reason = str(row.get("exit_reason") or note_fields.get("exit_reason") or "").strip().upper()
            sell_qty = int(_to_int(note_fields.get("sell_qty"), 0))
            source_order_id = str(note_fields.get("source_order_id") or "").strip()
            key = (
                str(row.get("code") or "").strip().zfill(6),
                exit_reason,
                sell_qty,
                source_order_id,
            )
            trade_key_counts[key] = int(trade_key_counts.get(key, 0)) + 1
            sig = _sell_lifecycle_signature(
                code=row.get("code", ""),
                exit_reason=exit_reason,
                sell_qty=sell_qty,
                note=row.get("note", ""),
            )
            if sig[0] and sig[1] and sig[2] > 0 and sig[3] and sig[7]:
                trade_lifecycle_sig_counts[sig] = int(trade_lifecycle_sig_counts.get(sig, 0)) + 1

    rows_to_append: List[List[Any]] = []
    recovered_orders: List[str] = []
    skip_reasons: Dict[str, int] = {}
    fills_today = fills_df.loc[fills_df["datetime"].map(_extract_ymd_from_ts_text) == target_ymd].copy()
    seen_fill_lifecycle_sigs: set[tuple[str, str, int, str, str, str, int, str]] = set()
    for _, fill in fills_today.iterrows():
        if str(fill.get("side", "")).strip().upper() != "SELL":
            continue
        note = str(fill.get("note", "") or "")
        note_fields = _parse_note_fields(note)
        code = str(fill.get("code", "") or "").strip().zfill(6)
        exit_reason = str(note_fields.get("exit_reason") or "").strip().upper()
        sell_qty = int(_to_int(note_fields.get("sell_qty"), fill.get("qty")))
        source_order_id = str(note_fields.get("source_order_id") or "").strip()
        key = (code, exit_reason, sell_qty, source_order_id)
        lifecycle_sig = _sell_lifecycle_signature(code=code, exit_reason=exit_reason, sell_qty=sell_qty, note=note)
        if lifecycle_sig in seen_fill_lifecycle_sigs:
            skip_reasons["duplicate_sell_lifecycle_fill"] = int(skip_reasons.get("duplicate_sell_lifecycle_fill", 0)) + 1
            continue
        seen_fill_lifecycle_sigs.add(lifecycle_sig)
        lifecycle_sig_valid = bool(lifecycle_sig[0] and lifecycle_sig[1] and lifecycle_sig[2] > 0 and lifecycle_sig[3] and lifecycle_sig[7])
        if lifecycle_sig_valid:
            if int(trade_lifecycle_sig_counts.get(lifecycle_sig, 0)) > 0:
                trade_lifecycle_sig_counts[lifecycle_sig] = int(trade_lifecycle_sig_counts.get(lifecycle_sig, 0)) - 1
                continue
        if (not lifecycle_sig_valid) and int(trade_key_counts.get(key, 0)) > 0:
            trade_key_counts[key] = int(trade_key_counts.get(key, 0)) - 1
            continue

        entry_order_id = str(note_fields.get("entry_order_id") or source_order_id or "").strip()
        buy = buy_by_order_id.get(entry_order_id)
        if buy is None:
            skip_reasons["entry_buy_fill_missing"] = int(skip_reasons.get("entry_buy_fill_missing", 0)) + 1
            continue
        entry_date = _extract_ymd_from_ts_text(buy.get("datetime", ""))
        entry_price = float(_to_float(buy.get("price"), 0.0) or 0.0)
        exit_price = float(_to_float(fill.get("price"), 0.0) or 0.0)
        if not entry_date or entry_price <= 0 or exit_price <= 0 or sell_qty <= 0 or not exit_reason:
            skip_reasons["required_value_missing"] = int(skip_reasons.get("required_value_missing", 0)) + 1
            continue

        pnl_pct = calc_net_ret(entry_price, exit_price, fee_pct, pos_slip_pct, sell_tax_pct)
        pnl_krw = round(float(pnl_pct) * float(entry_price) * float(sell_qty), 2)
        is_surge = 1 if str(note_fields.get("surge_type") or "").strip() not in {"", "None", "nan"} else 0
        trade_id = _next_available_legacy_trade_id(used_trade_ids)
        rows_to_append.append([
            trade_id,
            code,
            entry_date,
            entry_price,
            target_ymd,
            exit_price,
            round(pnl_pct, 8),
            pnl_krw,
            exit_reason,
            note,
            is_surge,
        ])
        trade_key_counts[key] = int(trade_key_counts.get(key, 0)) + 1
        if lifecycle_sig_valid:
            trade_lifecycle_sig_counts[lifecycle_sig] = int(trade_lifecycle_sig_counts.get(lifecycle_sig, 0)) + 1
        recovered_orders.append(str(fill.get("order_id", "") or "").strip())

    backup_path = ""
    if rows_to_append:
        backup_path = _append_rows_atomic(TRADES, rows_to_append, LEGACY_TRADES_HEADER)
    status = "APPLIED" if rows_to_append else ("SKIP" if skip_reasons else "PASS")
    return {
        "status": status,
        "runtime_ymd": target_ymd,
        "recovered_rows": int(len(rows_to_append)),
        "recovered_order_ids": recovered_orders,
        "skip_reasons": skip_reasons,
        "backup_path": backup_path,
    }

def _write_ops_alert(payload: Dict[str, Any]) -> None:
    try:
        OPS_ALERT_LATEST_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def append_rows(path: Path, rows: List[List[Any]]) -> None:
    # Best-effort durability: reduce chance of torn/partial rows on crash.
    # (Not a perfect transactional guarantee, but materially safer than buffered append.)
    import os
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow(r)
        f.flush()
        os.fsync(f.fileno())

def _append_rows_atomic(path: Path, rows: List[List[Any]], header: List[str]) -> str:
    if not rows:
        return ""
    existing = read_csv_safe(path)
    if not isinstance(existing, pd.DataFrame):
        existing = pd.DataFrame(columns=list(header))
    add_df = pd.DataFrame(rows, columns=list(header))
    merged = _concat_drop_all_na_columns([existing, add_df], ignore_index=True)
    return _atomic_write_csv_with_backup(path, merged, header)

def _restore_backup_file(path: Path, backup_path: str) -> None:
    if not backup_path:
        return
    bak = Path(backup_path)
    if not bak.exists():
        return
    if path.exists():
        path.unlink()
    shutil.copy2(bak, path)

def _sync_new_fills_to_live_bridge(
    fills_new: List[List[Any]],
    schema: str,
    prices_df: Optional[pd.DataFrame] = None,
    live_path: Path = ROOTB_REGEN_LIVE_FILLS,
) -> Dict[str, Any]:
    if _truthy(os.getenv("PAPER_DISABLE_LIVE_BRIDGE_SYNC", "")):
        return {"attempted": False, "added_rows": 0, "total_rows": 0, "path": str(live_path), "status": "disabled_by_env"}
    live_path = _path_from_env("PAPER_LIVE_BRIDGE_FILLS_PATH", live_path)
    if not fills_new:
        return {"attempted": False, "added_rows": 0, "total_rows": 0, "path": str(live_path), "status": "no_new_fills"}

    rows: List[Dict[str, Any]] = []
    for row in fills_new:
        try:
            if schema == "legacy":
                dt_text = str(row[0] or "")
                code = str(row[1] or "").zfill(6)
                side = str(row[2] or "").upper().strip()
                qty = int(float(row[3] or 0))
                price = float(row[4] or 0)
                order_id = str(row[5] or "").strip()
                note = str(row[6] or "")
            else:
                dt_text = str(row[0] or "")
                code = str(row[2] or "").zfill(6)
                side = str(row[4] or "").upper().strip()
                qty = int(float(row[5] or 0))
                price = float(row[6] or 0)
                order_id = str(row[9] or "").strip()
                note = str(row[10] or "")
            date8 = re.sub(r"[^0-9]", "", dt_text)[:8]
            if len(date8) != 8:
                date8 = now_ymd()
            if len(code) != 6 or side not in {"BUY", "SELL"} or qty <= 0 or price <= 0:
                continue
            ref_close = 0.0
            ref_source = ""
            if isinstance(prices_df, pd.DataFrame) and not prices_df.empty:
                sig_day = re.sub(r"[^0-9]", "", _extract_note_field(note, "signal_date"))[:8]
                if len(sig_day) == 8:
                    sig_ohlc = get_ohlc(prices_df, code, sig_day)
                    if sig_ohlc and float(sig_ohlc.get("close", 0) or 0) > 0:
                        ref_close = float(sig_ohlc["close"])
                        ref_source = "signal_date_close"
                if ref_close <= 0:
                    prev_day = prev_trading_date(prices_df, code, date8)
                    if prev_day:
                        prev_ohlc = get_ohlc(prices_df, code, prev_day)
                        if prev_ohlc and float(prev_ohlc.get("close", 0) or 0) > 0:
                            ref_close = float(prev_ohlc["close"])
                            ref_source = "prev_close"
                if ref_close <= 0:
                    same_day_ohlc = get_ohlc(prices_df, code, date8)
                    if same_day_ohlc and float(same_day_ohlc.get("close", 0) or 0) > 0:
                        ref_close = float(same_day_ohlc["close"])
                        ref_source = "same_day_close"

            slippage_actual_bps: Optional[float] = None
            slippage_actual_cost_krw: Optional[float] = None
            if ref_close > 0:
                raw_bps = ((float(price) - float(ref_close)) / float(ref_close)) * 10000.0
                signed_bps = raw_bps if side == "BUY" else (-raw_bps if side == "SELL" else raw_bps)
                slippage_actual_bps = float(signed_bps)
                slippage_actual_cost_krw = float(price) * float(qty) * (float(signed_bps) / 10000.0)
            rows.append(
                {
                    "date": date8,
                    "datetime": dt_text,
                    "code": code,
                    "side": side,
                    "fill_price": int(round(price)),
                    "fill_qty": int(qty),
                    "ref_close": (int(round(ref_close)) if ref_close > 0 else ""),
                    "slippage_actual_bps": (round(float(slippage_actual_bps), 4) if slippage_actual_bps is not None else ""),
                    "slippage_actual_cost_krw": (round(float(slippage_actual_cost_krw), 2) if slippage_actual_cost_krw is not None else ""),
                    "slippage_ref_source": ref_source,
                    "qty": int(qty),
                    "price": int(round(price)),
                    "event_seq": "0",
                    "intent_id": "",
                    "trace_id": "",
                    "fill_id": "",
                    "order_id": order_id,
                    "intended_session": "NEXT_OPEN",
                    "fill_session": "NEXT_OPEN",
                    "time_in_force": "DAY",
                    "replay_policy": "NEXT_SESSION_REPLAY_ONCE",
                    "note": note,
                    "source": "PAPER_ENGINE_BRIDGE",
                    "replay_chain_id": "",
                }
            )
        except Exception:
            continue

    if not rows:
        return {"attempted": True, "added_rows": 0, "total_rows": 0, "path": str(live_path), "status": "normalized_empty"}

    out_cols = [
        "date", "datetime", "code", "side", "fill_price", "fill_qty", "ref_close",
        "slippage_actual_bps", "slippage_actual_cost_krw", "slippage_ref_source",
        "qty", "price", "event_seq", "intent_id", "trace_id", "fill_id", "order_id",
        "intended_session", "fill_session", "time_in_force", "replay_policy",
        "note", "source", "replay_chain_id",
    ]
    new_df = pd.DataFrame(rows, columns=out_cols)
    live_path.parent.mkdir(parents=True, exist_ok=True)
    if live_path.exists():
        try:
            old_df = pd.read_csv(live_path, dtype=str, encoding="utf-8-sig")
        except Exception:
            old_df = pd.DataFrame(columns=out_cols)
    else:
        old_df = pd.DataFrame(columns=out_cols)

    for col in out_cols:
        if col not in old_df.columns:
            old_df[col] = ""
        if col not in new_df.columns:
            new_df[col] = ""
    old_df = old_df[out_cols].copy()
    new_df = new_df[out_cols].copy()

    old_key = (
        old_df["date"].astype(str).str[:8]
        + "|"
        + old_df["code"].astype(str).str.zfill(6)
        + "|"
        + old_df["side"].astype(str).str.upper()
        + "|"
        + old_df["fill_qty"].astype(str)
        + "|"
        + old_df["fill_price"].astype(str)
        + "|"
        + old_df["order_id"].astype(str).str.strip()
    )
    old_keys = set(old_key.tolist())
    new_key = (
        new_df["date"].astype(str).str[:8]
        + "|"
        + new_df["code"].astype(str).str.zfill(6)
        + "|"
        + new_df["side"].astype(str).str.upper()
        + "|"
        + new_df["fill_qty"].astype(str)
        + "|"
        + new_df["fill_price"].astype(str)
        + "|"
        + new_df["order_id"].astype(str).str.strip()
    )
    add_mask = ~new_key.isin(old_keys)
    add_df = new_df.loc[add_mask].copy()
    out_df = pd.concat([old_df, add_df], ignore_index=True)

    tmp = live_path.with_suffix(live_path.suffix + ".tmp")
    out_df.to_csv(tmp, index=False, encoding="utf-8-sig")
    tmp.replace(live_path)
    return {
        "attempted": True,
        "added_rows": int(len(add_df)),
        "total_rows": int(len(out_df)),
        "path": str(live_path),
        "status": "ok",
    }

def _compute_current_open_notional(open_pos: List[Dict[str, Any]], px: pd.DataFrame) -> float:
    if not open_pos or px is None or px.empty:
        return 0.0

    mtm_notional = 0.0
    for position in open_pos:
        code = norm_code(position.get("code", ""))
        qty = int(position.get("qty", 0) or 0)
        entry_price = float(position.get("entry_price", 0) or 0)
        if qty <= 0:
            continue
        price = entry_price
        if code:
            code_px = px[px["code"] == code]
            if not code_px.empty:
                last_close = float(code_px.sort_values("date").iloc[-1]["close"])
                if last_close > 0:
                    price = last_close
        if price > 0:
            mtm_notional += float(qty) * float(price)
    return float(mtm_notional)

def _count_open_position_slots(open_pos: List[Dict[str, Any]]) -> int:
    if not open_pos:
        return 0
    seen: set[str] = set()
    slots = 0
    for pos in open_pos:
        if not isinstance(pos, dict):
            continue
        code = norm_code(pos.get("code", ""))
        source_oid = str(pos.get("source_order_id") or "").strip()
        entry_oid = str(pos.get("entry_order_id") or "").strip()
        entry_date = str(pos.get("entry_date") or "").strip()
        # Deduplicate recovered duplicates first by source/entry order lineage.
        key = source_oid or entry_oid or f"{code}:{entry_date}"
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        slots += 1
    return int(slots)

def _recalculate_open_notional_and_alert(
    *,
    still_open: List[Dict[str, Any]],
    prices_df: pd.DataFrame,
    current_open_notional: float,
    gross_cap_krw: Optional[float],
    ops_alert: Dict[str, Any],
    ops_enabled: bool,
) -> float:
    open_notional_after_exit = 0.0
    if still_open and prices_df is not None and not prices_df.empty:
        for pos in still_open:
            try:
                code = norm_code(pos.get("code", ""))
                qty = int(pos.get("qty", 0) or 0)
                entry_price = float(pos.get("entry_price", 0) or 0)
                if qty <= 0:
                    continue
                mark_price = entry_price
                if code:
                    code_px = prices_df[prices_df["code"] == code]
                    if not code_px.empty:
                        last_close = float(code_px.sort_values("date").iloc[-1]["close"])
                        if last_close > 0:
                            mark_price = last_close
                if mark_price > 0:
                    open_notional_after_exit += float(qty) * float(mark_price)
            except Exception:
                continue

    open_notional_before_entry = float(current_open_notional)
    open_notional_after_exit = float(open_notional_after_exit)
    gross_recalc_delta = open_notional_after_exit - open_notional_before_entry
    ops_alert["open_notional_before_entry"] = round(open_notional_before_entry, 2)
    ops_alert["open_notional_after_exit"] = round(open_notional_after_exit, 2)
    ops_alert["gross_recalc_delta"] = round(gross_recalc_delta, 2)
    ops_alert["gross_recalc_applied"] = True
    if gross_cap_krw is not None and gross_cap_krw > 0:
        ops_alert["gross_cap_krw"] = round(float(gross_cap_krw), 2)
        ops_alert["gross_headroom_after_exit"] = round(float(gross_cap_krw - open_notional_after_exit), 2)
        ops_alert["gross_cap_util_after_exit"] = round(float(open_notional_after_exit / gross_cap_krw), 6)
    if ops_enabled:
        _write_ops_alert(ops_alert)
    if gross_cap_krw is not None:
        print(
            f"[RISK_CAP_RECALC] open_notional_before={open_notional_before_entry:.0f} "
            f"open_notional_after_exit={open_notional_after_exit:.0f} gross_cap={gross_cap_krw:.0f}"
        )
    return open_notional_after_exit

def _apply_sector_rebalance(
    *,
    config: Dict[str, Any],
    schema: str,
    still_open: List[Dict[str, Any]],
    prices_df: pd.DataFrame,
    sector_db: Dict[str, Any],
    fee_pct: float,
    slip_pct: float,
    sell_tax_pct: float,
    fills_new: List[Any],
    trades_new: List[Any],
    existing_fill_order_ids: Set[str],
    existing_trade_sigs: Set[str],
    committed_source_order_ids: Set[str],
    next_seq_start: int,
) -> int:
    next_seq = int(next_seq_start)
    _srb = config.get("sector_rebalance", {}) if isinstance(config, dict) else {}
    if _srb.get("enabled") and still_open:
        _srb_limit = float(_srb.get("limit_pct", 0.40))
        _srb_sell_ratio = float(_srb.get("sell_ratio_pct", 30.0))
        # calc current-price sector notional
        _srb_sec_notional: Dict[str, float] = {}
        _srb_total_notional = 0.0
        _srb_pos_info: List[Dict[str, Any]] = []
        for _sp in still_open:
            _sc = norm_code(_sp.get("code", ""))
            _sq = int(_sp.get("qty", 0) or 0)
            _sep = float(_sp.get("entry_price", 0) or 0)
            _sec = str(_sp.get("sector", "") or sector_db.get(_sc, "") or "").strip()
            _spx = prices_df[prices_df["code"] == _sc]
            _slc = float(_spx.sort_values("date").iloc[-1]["close"]) if not _spx.empty else _sep
            _sn = float(_sq) * _slc if _sq > 0 and _slc > 0 else 0.0
            _srb_total_notional += _sn
            if _sec and _sn > 0:
                _srb_sec_notional[_sec] = _srb_sec_notional.get(_sec, 0.0) + _sn
            _pnl = (_slc - _sep) / _sep if _sep > 0 and _slc > 0 else 0.0
            _srb_pos_info.append({"pos": _sp, "sector": _sec, "notional": _sn, "last_close": _slc, "pnl_pct": _pnl})
        if _srb_total_notional > 0:
            for _sec, _sn in _srb_sec_notional.items():
                _conc = _sn / _srb_total_notional
                if _conc <= _srb_limit:
                    continue
                # find weakest position in this sector
                _sec_pos = [p for p in _srb_pos_info if p["sector"] == _sec and p["notional"] > 0]
                if not _sec_pos:
                    continue
                _weakest = min(_sec_pos, key=lambda x: x["pnl_pct"])
                _wp = _weakest["pos"]
                _wc = norm_code(_wp.get("code", ""))
                _wq = int(_wp.get("qty", 0) or 0)
                _wep = float(_wp.get("entry_price", 0) or 0)
                _wlc = _weakest["last_close"]
                _wpnl = _weakest["pnl_pct"]
                _sell_qty = max(1, int(math.floor(_wq * _srb_sell_ratio / 100.0)))
                _sell_qty = min(_sell_qty, _wq)
                _today = now_ymd()
                _wp_slip_pct = max(
                    resolve_slip_pct(float(_wp.get("market_cap") or 0), config, slip_pct),
                    float(_to_float(_wp.get("entry_slippage_pct"), 0.0) or 0.0),
                )
                _srb_order_id = f"PAPER_SELL_{_wc}_{_today}_SECTOR_REBALANCE"
                if _srb_order_id not in existing_fill_order_ids and _wlc > 0:
                    _srb_note = (
                        f"exit_reason=SECTOR_REBALANCE;sector={_sec};concentration={_conc:.3f};"
                        f"sell_ratio_pct={_srb_sell_ratio};sell_qty={_sell_qty};"
                        f"partial_exit={1 if _sell_qty < _wq else 0};"
                        f"signal_date={_wp.get('signal_date', '')}"
                    )
                    _srb_exit_ts = _paper_exit_ts(_today)
                    if schema == "legacy":
                        fills_new.append([_srb_exit_ts, _wc, "SELL", _sell_qty, _wlc, _srb_order_id, _srb_note])
                        _srb_pnl_pct = calc_net_ret(_wep, _wlc, fee_pct, _wp_slip_pct, sell_tax_pct)
                        _srb_sig = "|".join([
                            _wc, str(_wp.get("entry_date", "")), _sig_float(_wep),
                            _today, _sig_float(_wlc),
                            _sig_float(round(_srb_pnl_pct, 8)), "SECTOR_REBALANCE", _sig_float(_sell_qty),
                            f"signal_date={_wp.get('signal_date', '')}",
                        ])
                        if _srb_sig not in existing_trade_sigs:
                            _srb_trade_id = f"T{next_seq:06d}"
                            _srb_pnl_krw = round(float(_srb_pnl_pct) * float(_wep) * float(_sell_qty), 2)
                            trades_new.append([_srb_trade_id, _wc, str(_wp.get("entry_date", "")), _wep,
                                               _today, _wlc, round(_srb_pnl_pct, 8), _srb_pnl_krw,
                                               "SECTOR_REBALANCE", _srb_note,
                                               int(_truthy(_wp.get("_surge_immediate", 0)))])
                            existing_trade_sigs.add(_srb_sig)
                            next_seq += 1
                    else:
                        fills_new.append([_srb_exit_ts, _today, _wc, str(_wp.get("name", "")),
                                          "SELL", _sell_qty, _wlc,
                                          round(calc_exit_fee(_wlc, _sell_qty, fee_pct), 6),
                                          round(_wlc * _sell_qty * _wp_slip_pct, 6),
                                          _srb_order_id, _srb_note])
                        _srb_fee = calc_roundtrip_fee(_wep, _wlc, _sell_qty, fee_pct)
                        _srb_slp = calc_roundtrip_slippage(_wep, _wlc, _sell_qty, _wp_slip_pct)
                        _srb_gross = (_wlc - _wep) / _wep if _wep > 0 else 0.0
                        _srb_net = calc_net_ret(_wep, _wlc, fee_pct, _wp_slip_pct, sell_tax_pct)
                        _srb_entry_ts = str(_wp.get("entry_ts", "") or f"{_wp.get('entry_date', _today)}T09:00:00")
                        _srb_trade_probe = pd.Series({
                            "code": _wc, "entry_ts": _srb_entry_ts, "exit_ts": _srb_exit_ts,
                            "qty": _sell_qty, "entry_price": round(_wep, 6), "exit_price": round(_wlc, 6),
                            "net_ret": round(_srb_net, 8), "note": _srb_note,
                        })
                        _srb_sig = _v411_trade_sig(_srb_trade_probe)
                        if _srb_sig not in existing_trade_sigs:
                            _srb_trade_id = f"T{next_seq:06d}"
                            trades_new.append([_srb_trade_id, _srb_entry_ts, _srb_exit_ts,
                                               _wc, str(_wp.get("name", "")), "LONG", _sell_qty,
                                               round(_wep, 6), round(_wlc, 6),
                                               round(_srb_gross, 8), round(_srb_net, 8),
                                               round(_srb_fee, 6), round(_srb_slp, 6),
                                               "0", "0", "0", _srb_note])
                            existing_trade_sigs.add(_srb_sig)
                            next_seq += 1
                    existing_fill_order_ids.add(_srb_order_id)
                    _wp["qty"] = max(0, _wq - _sell_qty)
                    print(f"[SECTOR_REBALANCE] sector={_sec} concentration={_conc:.3f} > limit={_srb_limit:.3f} "
                          f"code={_wc} sell_qty={_sell_qty} pnl={_wpnl:.3f}")
    return int(next_seq)

def _prioritize_open_positions_for_sell(
    open_pos: List[Dict[str, Any]],
    ddm_liquidation_targets: Set[str],
) -> List[Dict[str, Any]]:
    rows = list(open_pos or [])
    targets = set(ddm_liquidation_targets or set())

    def _key(item: Tuple[int, Dict[str, Any]]) -> Tuple[int, int, float, float, str]:
        idx, pos = item
        pos_key = _ddm_pos_key(pos, idx)
        code = str((pos or {}).get("code") or "").strip().zfill(6)
        qty = max(0, int(_to_int((pos or {}).get("qty"), 0)))
        entry_price = max(0.0, float(_to_float((pos or {}).get("entry_price"), 0.0) or 0.0))
        market_cap = float(_to_float((pos or {}).get("market_cap"), 0.0) or 0.0)
        prior_stop_count = max(0, int(_to_int((pos or {}).get("stop_exit_count"), 0)))
        notional = float(qty) * float(entry_price)
        return (
            0 if pos_key in targets else 1,
            -prior_stop_count,
            -notional,
            -market_cap,
            code,
        )

    return [pos for _, pos in sorted(enumerate(rows), key=_key)]

def _process_position_rows(
    *,
    config: Dict[str, Any],
    schema: str,
    prices_df: pd.DataFrame,
    open_pos: List[Dict[str, Any]],
    fundamentals_db: Dict[str, Any],
    sector_db: Dict[str, Any],
    max_hold_days: int,
    sell_rules: Dict[str, Any],
    sell_rules_enabled: bool,
    ddm_liquidation_targets: Set[str],
    ddm_liquidation_price_mode: str,
    ddm_action: Any,
    fee_pct: float,
    slip_pct: float,
    sell_tax_pct: float,
    fills_new: List[Any],
    trades_new: List[Any],
    existing_fill_order_ids: Set[str],
    existing_trade_sigs: Set[str],
    committed_source_order_ids: Set[str],
    next_seq_start: int,
    stop_loss: float,
    take_profit: Any,
    trail_pct: Any,
    vix_proxy: Any,
    fx_ctx: Dict[str, Any],
    macro_snapshot: Dict[str, Any],
    p0_snapshot: Dict[str, Any],
    market_regime: str,
    active_candidate_codes: Set[str],
    candidate_dropout_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    cfg = config
    px = prices_df
    next_seq = int(next_seq_start)
    still_open: List[Dict[str, Any]] = []
    candidate_dropout_sold_codes: Set[str] = set()
    prioritized_open_pos = _prioritize_open_positions_for_sell(open_pos, ddm_liquidation_targets)

    for idx_pos, pos in enumerate(prioritized_open_pos):
        single_result = _process_single_position_exit(
            config=cfg,
            schema=schema,
            prices_df=px,
            pos=pos,
            idx_pos=int(idx_pos),
            fundamentals_db=fundamentals_db,
            sector_db=sector_db,
            max_hold_days=int(max_hold_days),
            sell_rules=sell_rules,
            sell_rules_enabled=bool(sell_rules_enabled),
            ddm_liquidation_targets=ddm_liquidation_targets,
            ddm_liquidation_price_mode=ddm_liquidation_price_mode,
            ddm_action=ddm_action,
            fee_pct=float(fee_pct),
            slip_pct=float(slip_pct),
            sell_tax_pct=float(sell_tax_pct),
            fills_new=fills_new,
            trades_new=trades_new,
            existing_fill_order_ids=existing_fill_order_ids,
            existing_trade_sigs=existing_trade_sigs,
            committed_source_order_ids=committed_source_order_ids,
            next_seq_start=int(next_seq),
            stop_loss=float(stop_loss),
            take_profit=take_profit,
            trail_pct=trail_pct,
            vix_proxy=vix_proxy,
            fx_ctx=fx_ctx,
            macro_snapshot=macro_snapshot,
            p0_snapshot=p0_snapshot,
            market_regime=market_regime,
            active_candidate_codes=active_candidate_codes,
            candidate_dropout_cfg=candidate_dropout_cfg,
            candidate_dropout_sold_codes=candidate_dropout_sold_codes,
        )
        positions_out = single_result["positions_out"]
        if positions_out:
            still_open.extend(positions_out)
        next_seq = int(single_result["next_seq"])

    return {
        "still_open": still_open,
        "next_seq": int(next_seq),
    }

def _process_open_positions_and_rebalance(
    *,
    config: Dict[str, Any],
    schema: str,
    prices_df: pd.DataFrame,
    open_pos: List[Dict[str, Any]],
    fundamentals_db: Dict[str, Any],
    sector_db: Dict[str, Any],
    max_hold_days: int,
    sell_rules: Dict[str, Any],
    sell_rules_enabled: bool,
    ddm_liquidation_targets: Set[str],
    ddm_liquidation_price_mode: str,
    ddm_action: Any,
    fee_pct: float,
    slip_pct: float,
    sell_tax_pct: float,
    fills_new: List[Any],
    trades_new: List[Any],
    existing_fill_order_ids: Set[str],
    existing_trade_sigs: Set[str],
    committed_source_order_ids: Set[str],
    next_seq_start: int,
    stop_loss: float,
    take_profit: Any,
    trail_pct: Any,
    vix_proxy: Any,
    fx_ctx: Dict[str, Any],
    macro_snapshot: Dict[str, Any],
    p0_snapshot: Dict[str, Any],
    market_regime: str,
    candidate_df: pd.DataFrame,
) -> Dict[str, Any]:
    cfg = config
    px = prices_df
    next_seq = int(next_seq_start)
    still_open: List[Dict[str, Any]] = []
    next_seq = int(next_seq_start)
    active_candidate_codes: Set[str] = set()
    if isinstance(candidate_df, pd.DataFrame) and not candidate_df.empty and "code" in candidate_df.columns:
        active_candidate_codes = {str(x).strip().zfill(6) for x in candidate_df["code"].astype(str).tolist() if str(x).strip()}
    candidate_dropout_cfg = cast(Dict[str, Any], _get_dict(sell_rules, "candidate_dropout"))
    if candidate_dropout_cfg:
        candidate_dropout_cfg = dict(candidate_dropout_cfg)
        require_natural_pass = bool(candidate_dropout_cfg.get("require_natural_pass", True))
        if require_natural_pass:
            natural_pass_count = 0
            _meta_raw: Dict[str, Any] = {}
            try:
                _meta_raw = json.loads(CANDIDATES_META_PATH.read_text(encoding="utf-8"))
            except Exception:
                try:
                    _meta_raw = json.loads(CANDIDATES_META_PATH.read_text(encoding="utf-8-sig"))
                except Exception:
                    _meta_raw = {}
            if isinstance(_meta_raw, dict):
                _ep = _get_dict(_meta_raw, "execution_pool")
                natural_pass_count = _to_int(_ep.get("natural_pass_count"), 0)
            if natural_pass_count <= 0:
                candidate_dropout_cfg["_runtime_blocked"] = True
                candidate_dropout_cfg["_runtime_block_reason"] = "natural_pass_count=0"
                print("[CANDIDATE_DROPOUT] runtime disabled: natural_pass_count=0")

    position_rows_result = _process_position_rows(
        config=cfg,
        schema=schema,
        prices_df=px,
        open_pos=open_pos,
        fundamentals_db=fundamentals_db,
        sector_db=sector_db,
        max_hold_days=int(max_hold_days),
        sell_rules=sell_rules,
        sell_rules_enabled=bool(sell_rules_enabled),
        ddm_liquidation_targets=ddm_liquidation_targets,
        ddm_liquidation_price_mode=ddm_liquidation_price_mode,
        ddm_action=ddm_action,
        fee_pct=float(fee_pct),
        slip_pct=float(slip_pct),
        sell_tax_pct=float(sell_tax_pct),
        fills_new=fills_new,
        trades_new=trades_new,
        existing_fill_order_ids=existing_fill_order_ids,
        existing_trade_sigs=existing_trade_sigs,
        committed_source_order_ids=committed_source_order_ids,
        next_seq_start=int(next_seq),
        stop_loss=float(stop_loss),
        take_profit=take_profit,
        trail_pct=trail_pct,
        vix_proxy=vix_proxy,
        fx_ctx=fx_ctx,
        macro_snapshot=macro_snapshot,
        p0_snapshot=p0_snapshot,
        market_regime=market_regime,
        active_candidate_codes=active_candidate_codes,
        candidate_dropout_cfg=candidate_dropout_cfg,
    )
    still_open = position_rows_result["still_open"]
    next_seq = int(position_rows_result["next_seq"])

    next_seq = _apply_sector_rebalance(
        config=cfg,
        schema=schema,
        still_open=still_open,
        prices_df=px,
        sector_db=sector_db,
        fee_pct=float(fee_pct),
        slip_pct=float(slip_pct),
        sell_tax_pct=float(sell_tax_pct),
        fills_new=fills_new,
        trades_new=trades_new,
        existing_fill_order_ids=existing_fill_order_ids,
        existing_trade_sigs=existing_trade_sigs,
        committed_source_order_ids=committed_source_order_ids,
        next_seq_start=int(next_seq),
    )

    return {
        "still_open": still_open,
        "next_seq": int(next_seq),
    }
