# -*- coding: utf-8 -*-
"""
paper_pnl_report.py

- Reads paper/trades.csv
- Writes 2_Logs/paper_pnl_summary_YYYYMMDD_HHMMSS.json
- Adds equity / drawdown / latest-day return metrics for Kill Switch usage.
"""
from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import pandas as pd

def try_read_json(p: "Path"):
    try:
        if p is None:
            return None
        pp = Path(p)
        if not pp.exists():
            return None
        import json
        return json.loads(pp.read_text(encoding="utf-8"))
    except Exception:
        return None

def file_meta(p: "Path") -> dict:
    try:
        if p is None:
            return {"path": None, "exists": False}
        pp = Path(p)
        if not pp.exists():
            return {"path": str(pp), "exists": False}
        st = pp.stat()
        return {
            "path": str(pp),
            "exists": True,
            "size": int(st.st_size),
            "mtime": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
        }
    except Exception as e:
        return {"path": str(p) if p is not None else None, "exists": False, "error": str(e)}



def _to_float(x) -> float:
    try:
        if x is None:
            return float("nan")
        s = str(x).strip()
        if s == "":
            return float("nan")
        return float(s)
    except Exception:
        return float("nan")


def _normalize_ymd(s: str) -> str:
    # Accept "YYYYMMDD" or "YYYY-MM-DD" etc -> "YYYYMMDD"
    if s is None:
        return ""
    t = "".join(ch for ch in str(s) if ch.isdigit())
    if len(t) >= 8:
        return t[:8]
    return t


def _norm_code(v) -> str:
    s = str(v or "").strip().replace(".0", "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def _truthy(v) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _extract_note_value(note, key: str) -> str:
    pattern = rf"(?:^|[;|\s]){re.escape(key)}=([^;|\s]+)"
    m = re.search(pattern, str(note or ""))
    return m.group(1).strip() if m else ""


def _ymd_to_ts(ymd: str) -> str:
    ymd = _normalize_ymd(ymd)
    if len(ymd) != 8:
        return ""
    return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]} 15:20:00"


def _trade_calc_key(row: pd.Series | dict) -> tuple[str, str, str, str, str]:
    return (
        _norm_code(row.get("code")),
        _normalize_ymd(row.get("entry_ts") or row.get("entry_date")),
        _normalize_ymd(row.get("exit_ts") or row.get("exit_date")),
        str(row.get("entry_price") or "").strip(),
        str(row.get("exit_price") or "").strip(),
    )


def _sync_trades_calc_from_trades_for_d(base_dir: Path, d_rule_ymd: str, cfg: dict | None = None) -> dict:
    trades_path = base_dir / "paper" / "trades.csv"
    calc_path = base_dir / "paper" / "trades_calc.csv"
    meta = {
        "enabled": bool(d_rule_ymd),
        "d_rule_ymd": d_rule_ymd or None,
        "source": str(trades_path),
        "target": str(calc_path),
        "source_exit_rows_d": 0,
        "target_exit_rows_d_before": 0,
        "appended_rows": 0,
        "target_exit_rows_d_after": 0,
        "status": "SKIP",
        "reason": "",
    }
    if not d_rule_ymd:
        meta["reason"] = "missing_d_rule"
        return meta
    if not trades_path.exists():
        meta["reason"] = "trades_missing"
        return meta

    trades = _ensure_exit_date(_load_trades(trades_path)).fillna("")
    if calc_path.exists():
        calc = _ensure_exit_date(_load_trades(calc_path)).fillna("")
    else:
        calc = pd.DataFrame(
            columns=[
                "trade_id",
                "entry_ts",
                "exit_ts",
                "code",
                "name",
                "side",
                "qty",
                "entry_price",
                "exit_price",
                "gross_ret",
                "net_ret",
                "fee_rate",
                "slippage_rate",
                "sell_tax_rate",
                "note",
            ]
        )

    source_d = trades.loc[trades.get("exit_date", pd.Series("", index=trades.index)).astype(str).map(_normalize_ymd).eq(d_rule_ymd)].copy()
    target_d = calc.loc[calc.get("exit_date", pd.Series("", index=calc.index)).astype(str).map(_normalize_ymd).eq(d_rule_ymd)].copy() if not calc.empty else pd.DataFrame()
    meta["source_exit_rows_d"] = int(len(source_d))
    meta["target_exit_rows_d_before"] = int(len(target_d))
    if source_d.empty or len(target_d) >= len(source_d):
        meta["target_exit_rows_d_after"] = int(len(target_d))
        meta["status"] = "PASS"
        meta["reason"] = "already_aligned_or_no_source_rows"
        return meta

    existing_trade_ids = {str(row.get("trade_id") or "").strip() for _, row in calc.iterrows()} if not calc.empty else set()
    existing_key_counts: dict[tuple[str, str, str, str, str], int] = {}
    if not calc.empty:
        for _, row in calc.iterrows():
            key = _trade_calc_key(row)
            existing_key_counts[key] = existing_key_counts.get(key, 0) + 1
    source_key_counts: dict[tuple[str, str, str, str, str], int] = {}
    for _, row in source_d.iterrows():
        key = _trade_calc_key(row)
        source_key_counts[key] = source_key_counts.get(key, 0) + 1
    fee_rate = _to_float((cfg or {}).get("fee_pct")) if isinstance(cfg, dict) else 0.0
    slippage_rate = _to_float((cfg or {}).get("slippage_pct")) if isinstance(cfg, dict) else 0.0
    sell_tax_rate = _to_float((cfg or {}).get("sell_tax_pct")) if isinstance(cfg, dict) else 0.0
    fee_rate = 0.0 if math.isnan(fee_rate) else fee_rate
    slippage_rate = 0.0 if math.isnan(slippage_rate) else slippage_rate
    sell_tax_rate = 0.0 if math.isnan(sell_tax_rate) else sell_tax_rate
    appended: list[dict] = []
    for _, row in source_d.iterrows():
        key = _trade_calc_key(row)
        source_trade_id = str(row.get("trade_id") or "").strip()
        if source_trade_id and source_trade_id in existing_trade_ids:
            continue
        if existing_key_counts.get(key, 0) >= source_key_counts.get(key, 0):
            continue
        # trades.csv's pnl_pct is NOT gross -- it already carries a cost deduction, and its
        # implied rate is not even constant across rows (2026-08-18 audit: implied fee+slip
        # median 0.0152, min 0.00104, max 0.0689). Treating it as gross double-charged cost.
        # trades_calc.csv is the authoritative P&L ledger, so derive gross from prices and
        # apply the same formula pricing_engine.compute_costs() uses, which is what every
        # existing trades_calc row satisfies (629/629, zero variance).
        entry_px = _to_float(row.get("entry_price"))
        exit_px = _to_float(row.get("exit_price"))
        if math.isnan(entry_px) or math.isnan(exit_px) or entry_px <= 0:
            gross_ret = float("nan")
            net_ret = float("nan")
        else:
            gross_ret = (exit_px - entry_px) / entry_px
            net_ret = (
                gross_ret
                - ((entry_px + exit_px) / entry_px) * (fee_rate + slippage_rate)
                - sell_tax_rate
            )
        note = str(row.get("note") or "")
        if note:
            note = f"{note} | trades_calc_sync_source=trades.csv"
        else:
            note = "trades_calc_sync_source=trades.csv"
        appended.append(
            {
                "trade_id": source_trade_id,
                "entry_ts": _ymd_to_ts(row.get("entry_date")),
                "exit_ts": _ymd_to_ts(row.get("exit_date")),
                "code": _norm_code(row.get("code")),
                "name": str(row.get("name") or ""),
                "side": "LONG",
                "qty": _extract_note_value(note, "sell_qty"),
                "entry_price": str(row.get("entry_price") or ""),
                "exit_price": str(row.get("exit_price") or ""),
                "gross_ret": "" if math.isnan(gross_ret) else str(float(gross_ret)),
                "net_ret": "" if math.isnan(net_ret) else str(float(net_ret)),
                "fee_rate": str(float(fee_rate)),
                "slippage_rate": str(float(slippage_rate)),
                "sell_tax_rate": str(float(sell_tax_rate)),
                "note": note,
            }
        )
        if source_trade_id:
            existing_trade_ids.add(source_trade_id)
        existing_key_counts[key] = existing_key_counts.get(key, 0) + 1

    if appended:
        out = pd.concat([calc.drop(columns=["exit_date"], errors="ignore"), pd.DataFrame(appended)], ignore_index=True)
        out.to_csv(calc_path, index=False, encoding="utf-8-sig")
    refreshed = _ensure_exit_date(_load_trades(calc_path)).fillna("")
    refreshed_d = refreshed.loc[refreshed.get("exit_date", pd.Series("", index=refreshed.index)).astype(str).map(_normalize_ymd).eq(d_rule_ymd)].copy()
    meta["appended_rows"] = int(len(appended))
    meta["target_exit_rows_d_after"] = int(len(refreshed_d))
    meta["status"] = "PASS" if len(refreshed_d) >= len(source_d) else "PARTIAL"
    meta["reason"] = "synced_missing_d_rows_from_trades"
    return meta


def _derive_d_from_fills(fills_path: Path) -> dict:
    out = {
        "fills_path": str(fills_path),
        "fills_exists": bool(fills_path.exists()),
        "fills_rows": 0,
        "buy_rows": 0,
        "latest_buy_ymd": None,
        "latest_any_ymd": None,
        "d_rule_ymd": None,
        "d_rule_source": None,
    }
    if not fills_path.exists():
        out["d_rule_source"] = "fills_missing"
        return out
    try:
        fills = pd.read_csv(fills_path, dtype=str, encoding="utf-8-sig")
    except UnicodeError:
        fills = pd.read_csv(fills_path, dtype=str)
    except Exception as e:
        out["d_rule_source"] = f"fills_read_error:{type(e).__name__}"
        return out
    out["fills_rows"] = int(len(fills))
    if fills.empty:
        out["d_rule_source"] = "fills_empty"
        return out
    if "datetime" in fills.columns:
        date_s = fills["datetime"].map(_normalize_ymd)
    elif "date" in fills.columns:
        date_s = fills["date"].map(_normalize_ymd)
    else:
        date_s = pd.Series([""] * len(fills), index=fills.index, dtype=str)
    any_dates = sorted([x for x in date_s.tolist() if len(str(x)) == 8])
    out["latest_any_ymd"] = any_dates[-1] if any_dates else None
    side_s = fills["side"].fillna("").astype(str).str.upper() if "side" in fills.columns else pd.Series([""] * len(fills), index=fills.index)
    buy_dates = sorted([x for x in date_s[side_s.eq("BUY")].tolist() if len(str(x)) == 8])
    out["buy_rows"] = int(side_s.eq("BUY").sum())
    out["latest_buy_ymd"] = buy_dates[-1] if buy_dates else None
    out["d_rule_ymd"] = out["latest_buy_ymd"] or out["latest_any_ymd"]
    out["d_rule_source"] = "latest_buy_ymd" if out["latest_buy_ymd"] else "latest_any_ymd"
    return out


def _resolve_oper_start_ymd() -> str:
    raw = os.getenv("PAPER_OPER_START_YMD", "20260301")
    ymd = _normalize_ymd(raw)
    return ymd if len(ymd) == 8 else ""


def _pick_trade_date_col(df: pd.DataFrame) -> str | None:
    for c in ("exit_date", "date", "datetime", "exit_ts", "ts"):
        if c in df.columns:
            return c
    return None


def _ensure_exit_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize exit date source for mixed schemas:
    - prefer existing exit_date
    - fallback: derive YYYYMMDD from exit_ts
    """
    if df.empty:
        return df
    if "exit_date" in df.columns:
        return df
    if "exit_ts" in df.columns:
        out = df.copy()
        out["exit_date"] = out["exit_ts"].astype(str).map(_normalize_ymd)
        return out
    return df


def _apply_operational_scope(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    start_ymd = _resolve_oper_start_ymd()
    scope = {
        "enabled": bool(start_ymd),
        "start_ymd": start_ymd or None,
        "applied": False,
        "filter_col": None,
        "rows_before": int(len(df)),
        "rows_after": int(len(df)),
    }
    if df.empty or not start_ymd:
        return df, scope

    date_col = _pick_trade_date_col(df)
    if not date_col:
        return df, scope

    dser = df[date_col].astype(str).apply(_normalize_ymd)
    mask = (dser.str.len() == 8) & (dser >= start_ymd)
    out = df.loc[mask].copy()

    scope["applied"] = True
    scope["filter_col"] = date_col
    scope["rows_after"] = int(len(out))
    return out, scope


def _entry_dates_for_block_filter(df: pd.DataFrame) -> tuple[str, ...]:
    dates: set[str] = set()
    for c in ("entry_date", "entry_ts", "date", "datetime"):
        if c in df.columns:
            for v in df[c].dropna().astype(str).tolist():
                ymd = _normalize_ymd(v)
                if len(ymd) == 8:
                    dates.add(ymd)
    return tuple(sorted(dates))


def _blocked_order_ids_from_row(row: pd.Series) -> set[str]:
    ids: set[str] = set()
    note = row.get("note", "")
    for c in ("order_id", "entry_order_id", "source_order_id"):
        v = str(row.get(c, "") or "").strip()
        if v:
            ids.add(v)
    for k in ("order_id", "entry_order_id", "source_order_id"):
        v = _extract_note_value(note, k)
        if v:
            ids.add(v)
    code = _norm_code(row.get("code"))
    signal_date = _normalize_ymd(row.get("signal_date") or _extract_note_value(note, "signal_date"))
    if code and signal_date:
        ids.add(f"PAPER_BUY_{code}_{signal_date}")
    return ids


@lru_cache(maxsize=16)
def _load_blocked_entry_keys_for_dates(date_tuple: tuple[str, ...], orders_dir_s: str) -> tuple[frozenset[str], frozenset[str], dict]:
    orders_dir = Path(orders_dir_s)
    blocked_ids: set[str] = set()
    blocked_code_signal: set[str] = set()
    scanned = 0
    missing = 0
    blocked_rows = 0
    read_errors: list[str] = []

    for ymd in date_tuple:
        if not re.fullmatch(r"\d{8}", str(ymd or "")):
            continue
        path = orders_dir / f"orders_{ymd}_exec.xlsx"
        if not path.exists():
            missing += 1
            continue
        try:
            orders = pd.read_excel(path, dtype=str).fillna("")
        except Exception as e:
            missing += 1
            read_errors.append(f"{path.name}:{type(e).__name__}")
            continue
        scanned += 1
        required = {"side", "code", "is_stop", "entry_blocked"}
        if not required.issubset(set(orders.columns)):
            continue

        side = orders["side"].astype(str).str.upper().str.strip()
        stop = orders["is_stop"].apply(_truthy)
        entry_blocked = orders["entry_blocked"].apply(_truthy)
        posthoc = orders.get("posthoc_policy_violation", pd.Series(False, index=orders.index)).apply(_truthy)
        execution_blocked = orders.get("execution_blocked", pd.Series(False, index=orders.index)).apply(_truthy)
        qty = pd.to_numeric(orders.get("fill_qty", pd.Series(0, index=orders.index)), errors="coerce").fillna(0.0)
        mask = side.eq("BUY") & (~stop) & (entry_blocked | posthoc | execution_blocked) & (qty > 0)

        for _, row in orders.loc[mask].iterrows():
            row_ids = _blocked_order_ids_from_row(row)
            blocked_ids.update(row_ids)
            code = _norm_code(row.get("code"))
            signal_date = _normalize_ymd(row.get("signal_date") or _extract_note_value(row.get("note", ""), "signal_date"))
            if code and signal_date:
                blocked_code_signal.add(f"{code}|{signal_date}")
            blocked_rows += 1

    meta = {
        "orders_exec_scanned": int(scanned),
        "orders_exec_missing_or_unreadable": int(missing),
        "orders_exec_read_errors": read_errors[:20],
        "blocked_orders_rows": int(blocked_rows),
        "blocked_entry_order_ids": int(len(blocked_ids)),
        "blocked_code_signal_keys": int(len(blocked_code_signal)),
    }
    return frozenset(blocked_ids), frozenset(blocked_code_signal), meta


def _filter_blocked_entry_trades(df: pd.DataFrame, orders_dir: Path) -> tuple[pd.DataFrame, dict]:
    include_blocked = os.getenv("PAPER_PNL_INCLUDE_BLOCKED_ENTRY_TRADES", "").strip().lower() in {"1", "true", "yes", "y"}
    if df.empty or include_blocked:
        return df, {"enabled": False, "reason": "empty_or_env_include", "removed_rows": 0}

    dates = _entry_dates_for_block_filter(df)
    blocked_ids, blocked_code_signal, meta = _load_blocked_entry_keys_for_dates(dates, str(orders_dir))
    note_s = df.get("note", pd.Series("", index=df.index)).astype(str)
    code_s = df.get("code", pd.Series("", index=df.index)).map(_norm_code)
    signal_s = note_s.map(lambda s: _normalize_ymd(_extract_note_value(s, "signal_date")))

    linked_ids = pd.Series(False, index=df.index)
    trade_has_id = pd.Series(False, index=df.index)
    for key in ("order_id", "entry_order_id", "source_order_id"):
        extracted = note_s.map(lambda s, k=key: _extract_note_value(s, k))
        trade_has_id = trade_has_id | extracted.astype(str).str.strip().ne("")
        linked_ids = linked_ids | extracted.isin(blocked_ids)
    for col in ("order_id", "entry_order_id", "source_order_id"):
        if col in df.columns:
            col_s = df[col].astype(str).str.strip()
            trade_has_id = trade_has_id | col_s.ne("")
            linked_ids = linked_ids | col_s.isin(blocked_ids)

    code_signal = (code_s.astype(str) + "|" + signal_s.astype(str)).isin(blocked_code_signal) & (~trade_has_id)
    remove = linked_ids | code_signal
    out = df.loc[~remove].copy()

    removed_exit_dates: dict[str, int] = {}
    if remove.any():
        exit_date_s = _ensure_exit_date(df).get("exit_date", pd.Series("", index=df.index)).astype(str).map(_normalize_ymd)
        removed_exit_dates = {
            str(k): int(v)
            for k, v in exit_date_s[remove].value_counts().sort_index().items()
            if str(k)
        }

    removed_trade_ids = []
    if "trade_id" in df.columns and remove.any():
        removed_trade_ids = [str(x) for x in df.loc[remove, "trade_id"].astype(str).head(30).tolist()]

    meta.update(
        {
            "enabled": True,
            "entry_dates_scanned": int(len(dates)),
            "removed_rows": int(remove.sum()),
            "removed_by_exit_date": removed_exit_dates,
            "removed_trade_ids_sample": removed_trade_ids,
        }
    )
    return out, meta


def _filter_unauditable_surge_trades(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    include_unauditable = os.getenv("PAPER_PNL_INCLUDE_UNAUDITABLE_SURGE_TRADES", "").strip().lower() in {"1", "true", "yes", "y"}
    if df.empty or include_unauditable:
        return df, {"enabled": False, "reason": "empty_or_env_include", "removed_rows": 0}

    note_s = df.get("note", pd.Series("", index=df.index)).astype(str)
    is_surge = note_s.str.contains(r"(?:^|[;|\s])surge_immediate=1(?:$|[;|\s])", regex=True, na=False)
    required_fields = (
        "surge_spread_bps",
        "surge_orderflow_tag",
        "surge_orderflow_risk_score",
        "surge_lob_slippage_source",
    )
    missing_any = pd.Series(False, index=df.index)
    missing_counts: dict[str, int] = {}
    for field in required_fields:
        vals = note_s.map(lambda s, k=field: str(_extract_note_value(s, k) or "").strip())
        missing = vals.eq("") | vals.str.upper().isin({"UNKNOWN", "NAN", "NONE", "NULL"})
        missing_counts[field] = int((is_surge & missing).sum())
        missing_any = missing_any | missing

    cfg_path = Path(__file__).resolve().parent / "paper" / "paper_engine_config.json"
    cfg = try_read_json(cfg_path)
    dyn = (((cfg.get("surge_entry_policy") or {}).get("dynamic_max_new") or {}) if isinstance(cfg, dict) else {})
    spread_max = _to_float(dyn.get("medium_spread_bps_max"))
    risk_max = _to_float(dyn.get("orderflow_risk_score_max"))
    spread_max = 40.0 if spread_max is None else float(spread_max)
    risk_max = 0.6 if risk_max is None else float(risk_max)

    tag_s = note_s.map(lambda s: str(_extract_note_value(s, "surge_orderflow_tag") or "").strip().upper())
    spread_s = pd.to_numeric(note_s.map(lambda s: _extract_note_value(s, "surge_spread_bps")), errors="coerce")
    risk_s = pd.to_numeric(note_s.map(lambda s: _extract_note_value(s, "surge_orderflow_risk_score")), errors="coerce")
    no_lob_or_history = tag_s.isin({"NO_LOB", "NO_HISTORY"})
    spread_over = spread_s.gt(spread_max)
    risk_over = risk_s.gt(risk_max)

    remove = is_surge & (missing_any | no_lob_or_history | spread_over | risk_over)
    out = df.loc[~remove].copy()

    removed_exit_dates: dict[str, int] = {}
    if remove.any():
        exit_date_s = _ensure_exit_date(df).get("exit_date", pd.Series("", index=df.index)).astype(str).map(_normalize_ymd)
        removed_exit_dates = {
            str(k): int(v)
            for k, v in exit_date_s[remove].value_counts().sort_index().items()
            if str(k)
        }

    removed_trade_ids = []
    if "trade_id" in df.columns and remove.any():
        removed_trade_ids = [str(x) for x in df.loc[remove, "trade_id"].astype(str).head(30).tolist()]

    return out, {
        "enabled": True,
        "policy": "exclude_surge_immediate_trades_failing_current_orderbook_quality_policy",
        "required_note_fields": list(required_fields),
        "thresholds": {
            "medium_spread_bps_max": float(spread_max),
            "orderflow_risk_score_max": float(risk_max),
        },
        "surge_rows_scanned": int(is_surge.sum()),
        "removed_rows": int(remove.sum()),
        "missing_counts": missing_counts,
        "no_lob_or_no_history_rows": int((is_surge & no_lob_or_history).sum()),
        "spread_over_rows": int((is_surge & spread_over).sum()),
        "orderflow_risk_over_rows": int((is_surge & risk_over).sum()),
        "removed_by_exit_date": removed_exit_dates,
        "removed_trade_ids_sample": removed_trade_ids,
    }


def _load_trades(trades_path: Path) -> pd.DataFrame:
    if not trades_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(trades_path, encoding="utf-8-sig")
    except Exception:
        # fallback for legacy encodings
        return pd.read_csv(trades_path, encoding="cp949", errors="replace")


def _pick_ret_col(df: pd.DataFrame) -> str | None:
    # Prefer net_ret when available to keep paper/trades_calc and summary math consistent.
    for c in ["net_ret", "pnl_pct", "ret_pct", "ret", "pnl", "profit_pct"]:
        if c in df.columns:
            return c
    return None


def _prepare_pnl_source(trades_path: Path, base_dir: Path) -> dict:
    df_raw = _ensure_exit_date(_load_trades(trades_path))
    df, op_scope = _apply_operational_scope(df_raw)
    df, blocked_filter = _filter_blocked_entry_trades(df, base_dir / "paper")
    op_scope["rows_after_blocked_entry_filter"] = int(len(df))
    df, unauditable_surge_filter = _filter_unauditable_surge_trades(df)
    op_scope["rows_after_unauditable_surge_filter"] = int(len(df))
    ret_col = _pick_ret_col(df)
    if ret_col is None and ("net_ret" in df.columns):
        ret_col = "net_ret"
    as_of_ymd = ""
    if ret_col is not None and not df.empty:
        as_of_ymd = str((_equity_metrics(df, ret_col) or {}).get("last_exit_date") or "")
    return {
        "path": trades_path,
        "df": df,
        "op_scope": op_scope,
        "blocked_filter": blocked_filter,
        "unauditable_surge_filter": unauditable_surge_filter,
        "ret_col": ret_col,
        "as_of_ymd": as_of_ymd,
        "rows_total": int(op_scope.get("rows_before", 0) or 0),
        "rows_after_filters": int(len(df)),
    }


def _select_pnl_source(base_dir: Path, ssot_chain: dict) -> dict:
    calc_path = base_dir / "paper" / "trades_calc.csv"
    trades_path = base_dir / "paper" / "trades.csv"
    candidates: list[dict] = []
    for path in (calc_path, trades_path):
        if path.exists():
            candidates.append(_prepare_pnl_source(path, base_dir))
    if not candidates:
        empty = _prepare_pnl_source(trades_path, base_dir)
        empty["selection_meta"] = {"reason": "no_source_exists", "candidates": []}
        return empty

    selected = candidates[0]
    reason = "default_first_available"
    d_rule = str(ssot_chain.get("d_rule_ymd") or "")
    if d_rule and selected.get("as_of_ymd") != d_rule:
        exact = [c for c in candidates if str(c.get("as_of_ymd") or "") == d_rule]
        if exact:
            selected = exact[0]
            reason = "as_of_matches_d_rule"
        else:
            fresher = sorted(candidates, key=lambda c: str(c.get("as_of_ymd") or ""), reverse=True)[0]
            if str(fresher.get("as_of_ymd") or "") > str(selected.get("as_of_ymd") or ""):
                selected = fresher
                reason = "fresher_as_of_fallback"

    selected = dict(selected)
    selected["selection_meta"] = {
        "reason": reason,
        "d_rule_ymd": d_rule or None,
        "selected_source": str(selected.get("path")),
        "candidates": [
            {
                "path": str(c.get("path")),
                "exists": bool(c.get("path") and c.get("path").exists()),
                "as_of_ymd": c.get("as_of_ymd") or None,
                "rows_total": int(c.get("rows_total") or 0),
                "rows_after_filters": int(c.get("rows_after_filters") or 0),
                "ret_col": c.get("ret_col"),
            }
            for c in candidates
        ],
    }
    return selected


def _resolve_capital_total(cfg_path: Path | None = None) -> float:
    path = cfg_path or (Path(__file__).resolve().parent / "paper" / "paper_engine_config.json")
    cfg = try_read_json(path)
    if not isinstance(cfg, dict):
        return 0.0
    try:
        return float(cfg.get("capital_total") or 0.0)
    except Exception:
        return 0.0


def _equity_metrics(df: pd.DataFrame, ret_col: str, capital_total: float | None = None) -> dict:
    """
    Build day-level equity curve from CLOSED trades (exit_date present).
    daily_ret = prod(1+ret) - 1 for each exit_date
    """
    if df.empty:
        return {
            "last_exit_date": "",
            "last_day_ret": 0.0,
            "end_equity": 1.0,
            "peak_equity": 1.0,
            "dd_end_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "last_5_days": [],
            "daily_return_basis": "empty",
            "capital_total": None,
        }

    dfe = _ensure_exit_date(df)
    exit_col = "exit_date" if "exit_date" in dfe.columns else None
    if exit_col is None:
        return {
            "last_exit_date": "",
            "last_day_ret": 0.0,
            "end_equity": 1.0,
            "peak_equity": 1.0,
            "dd_end_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "last_5_days": [],
            "daily_return_basis": "missing_exit_date",
            "capital_total": None,
        }

    dfx = dfe.copy()
    dfx[ret_col] = dfx[ret_col].apply(_to_float)
    dfx[exit_col] = dfx[exit_col].apply(_normalize_ymd)

    dfx = dfx[dfx[exit_col].astype(str).str.len() == 8]
    dfx = dfx[~dfx[ret_col].isna()]

    if dfx.empty:
        return {
            "last_exit_date": "",
            "last_day_ret": 0.0,
            "end_equity": 1.0,
            "peak_equity": 1.0,
            "dd_end_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "last_5_days": [],
            "daily_return_basis": "empty_after_filter",
            "capital_total": None,
        }

    capital = float(capital_total or 0.0)
    if capital <= 0.0:
        capital = _resolve_capital_total()

    # Day-level closed PnL return:
    # Prefer account-capital basis so partial/slot-sized exits do not move the
    # whole equity curve as if the entire account was invested in closed rows.
    def day_ret(group: pd.DataFrame) -> float:
        vals = pd.to_numeric(group[ret_col], errors="coerce").fillna(0.0)
        if vals.empty:
            return 0.0

        if all(c in group.columns for c in ("qty", "entry_price")):
            qty = pd.to_numeric(group["qty"], errors="coerce").fillna(0.0)
            ep = pd.to_numeric(group["entry_price"], errors="coerce").fillna(0.0)
            w = (qty * ep).astype(float)
            wsum = float(w.sum())
            if capital > 0.0 and wsum > 0.0:
                return float((vals * w).sum() / capital)
            if wsum > 0.0:
                return float((vals * w).sum() / wsum)
        return float(vals.mean())

    # Avoid GroupBy.apply deprecation semantics changes in newer pandas.
    g = pd.Series(
        {str(k): float(day_ret(grp)) for k, grp in dfx.groupby(exit_col, sort=True)},
        dtype="float64",
    ).sort_index()

    eq = (1.0 + g).cumprod()
    peak = eq.cummax()
    dd = (eq / peak) - 1.0

    last_exit = str(g.index[-1])
    last_day_ret = float(g.iloc[-1])
    end_equity = float(eq.iloc[-1])
    peak_equity = float(peak.iloc[-1])
    dd_end = float(dd.iloc[-1])
    max_dd = float(dd.min())

    last_5 = []
    tail = g.tail(5)
    for k, v in tail.items():
        last_5.append({"date": str(k), "ret": float(v)})

    return {
        "last_exit_date": last_exit,
        "last_day_ret": last_day_ret,
        "end_equity": end_equity,
        "peak_equity": peak_equity,
        "dd_end_pct": dd_end,
        "max_drawdown_pct": max_dd,
        "last_5_days": last_5,
        "daily_return_basis": "account_capital_realized_pnl" if capital > 0.0 else "closed_trade_notional_weighted",
        "capital_total": float(capital) if capital > 0.0 else None,
    }


def _return_stats(rets: list[float]) -> dict:
    clean = [float(r) for r in rets if isinstance(r, (int, float)) and not math.isnan(float(r))]
    trades_used = int(len(clean))
    wins = int(sum(1 for r in clean if r > 0))
    losses = int(sum(1 for r in clean if r < 0))
    flat = int(sum(1 for r in clean if r == 0))
    gpos = float(sum(r for r in clean if r > 0))
    gneg_abs = float(sum(-r for r in clean if r < 0))
    gross_pf = (gpos / gneg_abs) if gneg_abs > 0 else None
    return {
        "trades_used": trades_used,
        "wins": wins,
        "losses": losses,
        "flat": flat,
        "win_rate": (float(wins) / float(trades_used)) if trades_used > 0 else None,
        "avg_ret": (float(sum(clean)) / float(trades_used)) if trades_used > 0 else 0.0,
        "gross_pf": gross_pf,
        "gross_profit_sum": gpos,
        "gross_loss_abs_sum": gneg_abs,
    }


def _strategy_key_from_note(note: str) -> str:
    source_kind = _extract_note_value(note, "entry_source_kind").strip().lower()
    if _truthy(_extract_note_value(note, "surge_immediate")) or source_kind.startswith("surge"):
        return "surge"
    if source_kind:
        return source_kind
    if _extract_note_value(note, "split_entry"):
        return "split_entry"
    return "normal"


def _build_strategy_stats(df: pd.DataFrame, ret_col: str | None) -> list[dict]:
    if df.empty or ret_col is None or ret_col not in df.columns:
        return []

    work = df.copy()
    work["_ret_for_strategy"] = work[ret_col].apply(_to_float)
    note_s = work.get("note", pd.Series("", index=work.index)).fillna("").astype(str)
    work["_strategy"] = note_s.map(_strategy_key_from_note)
    work = work[~work["_ret_for_strategy"].isna()]
    if work.empty:
        return []

    strategies: list[dict] = []
    total_trades = int(len(work))
    for strategy, part in work.groupby("_strategy", sort=True):
        stats = _return_stats(part["_ret_for_strategy"].tolist())
        strategies.append({
            "strategy": str(strategy),
            "trades_used": int(stats["trades_used"]),
            "trade_share": (float(stats["trades_used"]) / float(total_trades)) if total_trades > 0 else None,
            "wins": int(stats["wins"]),
            "losses": int(stats["losses"]),
            "flat": int(stats["flat"]),
            "win_rate": stats["win_rate"],
            "avg_ret": float(stats["avg_ret"]),
            "gross_pf": stats["gross_pf"],
            "gross_profit_sum": float(stats["gross_profit_sum"]),
            "gross_loss_abs_sum": float(stats["gross_loss_abs_sum"]),
        })

    strategies.sort(key=lambda x: (-int(x.get("trades_used") or 0), str(x.get("strategy") or "")))
    return strategies


def _promote_after_close(payload: dict) -> None:
    try:
        rc = (payload.get("meta", {}) or {}).get("risk_context", {})
        ac = (rc or {}).get("after_close_summary_last", {})
        if isinstance(ac, dict):
            ro = ac.get("risk_off")
            rs = ac.get("risk_off_reasons") or ac.get("reasons")
            if isinstance(ro, bool) and isinstance(rs, list):
                payload["after_close"] = {"risk_off": bool(ro), "reasons": [str(x) for x in rs]}
    except Exception as e:
        payload.setdefault("meta", {})
        payload["meta"].setdefault("warnings", [])
        payload["meta"]["warnings"].append(f"after_close_promote_fail:{type(e).__name__}:{e}")


def _build_rows_validation(rows_total: int, rows_as_of: int) -> dict:
    valid = (rows_total >= 0) and (rows_as_of >= 0) and (rows_as_of <= rows_total)
    return {
        "rows_total": int(rows_total),
        "rows_as_of": int(rows_as_of),
        "delta_total_minus_as_of": int(rows_total - rows_as_of),
        "valid": bool(valid),
    }


def _attach_ssot_chain_meta(payload: dict, ssot_chain: dict, df: pd.DataFrame, as_of_ymd: str) -> None:
    d_rule_ymd = str(ssot_chain.get("d_rule_ymd") or "")
    exit_rows_d = 0
    if d_rule_ymd and "exit_date" in df.columns:
        exit_rows_d = int(df["exit_date"].map(_normalize_ymd).eq(d_rule_ymd).sum())
    ssot = dict(ssot_chain)
    ssot["stats"] = {
        "as_of": as_of_ymd or None,
        "as_of_semantics": "last_exit_date",
        "d_rule_ymd": d_rule_ymd or None,
        "as_of_matches_d_rule": bool(as_of_ymd and d_rule_ymd and as_of_ymd == d_rule_ymd),
        "trades_rows_exit_d_rule": int(exit_rows_d),
    }
    payload["d_rule_ymd"] = d_rule_ymd or None
    payload.setdefault("meta", {})["ssot_chain"] = ssot



def _as_of_freshness(as_of_ymd: str) -> dict:
    """`as_of` 가 **달력 기준으로** 얼마나 뒤처졌는지 잰다.

    [2026-09-10] 신설. 이 요약은 매일 새로 생성돼 `generated_at` 이 늘 오늘이다.
    그런데 내용은 `trades_calc.csv` 의 마지막 청산일에 묶여 있다.
    2026-09-10 07:36 에 만들어진 요약의 `as_of` 가 **20260825** 였다 -
    v41.1 이 `PAPER_EXIT_ONLY=1` 로 청산 전용이라 새 거래가 없어서다.

    파일 mtime 도 generated_at 도 신선하니 소비자는 살아 있는 것으로 읽는다.
    실제로 `tools/build_capital_sizing_scenario_report.py` 가 신선도로 `generated_at` 을 쓴다.

    **기준선을 자기 자료가 아니라 달력에서 잡는다.** 자기 자료에서 뽑으면 언제나 통과한다.
    """
    out = {"as_of": as_of_ymd or None, "reference": None,
           "stale_trading_days": None, "stale": None, "note": ""}
    try:
        import datetime as _dt
        from holiday_manager import HolidayManager
        hm = HolidayManager()
        ref = hm.previous_trading_day(_dt.date.today().strftime("%Y%m%d"))
        out["reference"] = ref
        out["reference_semantics"] = "직전 거래일 (holidays.json 정본)"
        if not as_of_ymd:
            out["stale"] = True
            out["note"] = "as_of 가 비어 있어요"
            return out
        n, cur = 0, ref
        while cur > str(as_of_ymd) and n < 400:
            cur = hm.previous_trading_day(cur)
            n += 1
        out["stale_trading_days"] = n
        out["stale"] = bool(n > 1)
        if out["stale"]:
            out["note"] = ("내용이 %s 에 묶여 있어요 - 직전 거래일 %s 기준 %d거래일 뒤처졌어요. "
                           "generated_at 이 오늘이어도 **새 자료가 아니에요**" % (as_of_ymd, ref, n))
    except Exception as exc:
        out["stale"] = None
        out["note"] = "판정 실패 (%s: %s)" % (type(exc).__name__, exc)
    return out


def _cost_profile_meta(cfg_path: Path, trades_path: Path) -> dict:
    cfg = try_read_json(cfg_path)
    if not isinstance(cfg, dict):
        cfg = {}
    meta = {
        "source_config_path": str(cfg_path),
        "source_trades_path": str(trades_path),
        "fee_pct": cfg.get("fee_pct"),
        "slippage_pct": cfg.get("slippage_pct"),
        "sell_tax_pct": cfg.get("sell_tax_pct"),
        "ret_source": "trades_calc.net_ret" if trades_path.name == "trades_calc.csv" else "trades.pnl_pct",
    }
    return meta


def _attach_stats_trace(
    payload: dict,
    *,
    run_id: str,
    as_of_ymd: str,
    rows_total: int,
    rows_as_of: int,
    ret_col: str | None,
    trades_used: int,
    avg_ret: float,
    win_rate: float | None,
    strategies: list[dict],
    gross_pf: float | None,
    gpos: float,
    gneg_abs: float,
    eqm: dict,
) -> None:
    link_key = f"{run_id}:{as_of_ymd or 'NA'}"
    payload["as_of_ymd"] = as_of_ymd or None
    payload["summary_detail_link"] = {"key": link_key, "summary_path": "summary", "detail_path": "detail"}
    payload["summary"] = {
        "key": link_key,
        "as_of_ymd": as_of_ymd or None,
        "run_id": run_id,
        "trades_used": int(trades_used),
        "avg_ret": float(avg_ret),
        "win_rate": None if win_rate is None else float(win_rate),
        "gross_pf": None if gross_pf is None else float(gross_pf),
        "max_drawdown_pct": float((eqm or {}).get("max_drawdown_pct") or 0.0),
        "rows_total": int(rows_total),
        "rows_as_of": int(rows_as_of),
        "strategies": strategies,
    }
    payload["detail"] = {
        "key": link_key,
        "ret_col": ret_col,
        "returns_components": {
            "gross_profit_sum": float(gpos),
            "gross_loss_abs_sum": float(gneg_abs),
            "trades_used": int(trades_used),
            "wins": int(round(float(win_rate) * int(trades_used))) if win_rate is not None else 0,
            "win_rate": None if win_rate is None else float(win_rate),
        },
        "strategies": strategies,
        "equity": eqm,
    }
    payload.setdefault("meta", {})
    payload["meta"]["formula_trace"] = {
        "avg_ret_formula": "avg_ret = sum(returns) / trades_used",
        "win_rate_formula": "win_rate = count(return > 0) / trades_used",
        "gross_pf_formula": "gross_pf = gross_profit_sum / gross_loss_abs_sum (when gross_loss_abs_sum > 0)",
        "strategies_formula": "strategies grouped by note.entry_source_kind, surge_immediate, split_entry, else normal",
        "returns_col": ret_col,
        "returns_count_used": int(trades_used),
    }
    payload["meta"]["identity"] = {"run_id": run_id, "as_of_ymd": as_of_ymd or None}
    payload["meta"]["rows_validation"] = _build_rows_validation(rows_total, rows_as_of)


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    logs_dir = base_dir / "2_Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # --- META: config/lock + (optional) risk context ---
    cfg_path = CFG_PATH if 'CFG_PATH' in globals() else (Path(__file__).resolve().parent / "paper" / "paper_engine_config.json")
    lock_path = LOCK_PATH if 'LOCK_PATH' in globals() else (Path(__file__).resolve().parent / "paper" / "paper_engine_config.lock.json")
    gate_last_path = GATE_LAST if 'GATE_LAST' in globals() else (Path(__file__).resolve().parent / "2_Logs" / "gate_daily_last.json")
    after_close_last_path = AFTER_LAST if 'AFTER_LAST' in globals() else (Path(__file__).resolve().parent / "2_Logs" / "after_close_summary_last.json")
    cfg = try_read_json(cfg_path)
    if not isinstance(cfg, dict):
        cfg = {}

    ssot_chain = _derive_d_from_fills(base_dir / "paper" / "fills.csv")
    trades_calc_sync = _sync_trades_calc_from_trades_for_d(base_dir, str(ssot_chain.get("d_rule_ymd") or ""), cfg)
    selected_source = _select_pnl_source(base_dir, ssot_chain)
    trades_path = selected_source["path"]
    df = selected_source["df"]
    op_scope = selected_source["op_scope"]
    blocked_filter = selected_source["blocked_filter"]
    unauditable_surge_filter = selected_source["unauditable_surge_filter"]

    cfg_file = file_meta(cfg_path)
    lock_file = file_meta(lock_path)
    cost_meta = _cost_profile_meta(cfg_path, trades_path)

    risk_ctx = {}
    g = try_read_json(gate_last_path)
    if isinstance(g, dict):
        # gate 援ъ“???ㅼ뼇?쒕뜲, risk_off/reasons留??덉쑝硫??ｋ뒗???놁쑝硫?None)
        risk_ctx["gate_daily_last"] = {
            "path": str(gate_last_path),
            "generated_at": g.get("generated_at") or g.get("ts") or g.get("as_of") or None,
            "risk_off": g.get("risk_off") if "risk_off" in g else (g.get("p0", {}) or {}).get("risk_off"),
            "reasons": g.get("reasons") if "reasons" in g else (g.get("p0", {}) or {}).get("reasons"),
        }
    a = try_read_json(after_close_last_path)
    if isinstance(a, dict):
        risk_ctx["after_close_summary_last"] = {
            "path": str(after_close_last_path),
            "generated_at": a.get("generated_at") or None,
            # SSOT: after_close_summary_last.json -> p0.risk_off_reasons, gate.snapshot.reasons
            "risk_off": (a.get("p0") or {}).get("risk_off"),
            "risk_off_reasons": (
                ((a.get("p0") or {}).get("risk_off_reasons"))
                or (((((a.get("gate") or {}).get("snapshot") or {}).get("risk_off") or {})).get("reasons"))
                or (((a.get("gate") or {}).get("snapshot") or {}).get("reasons"))
                or None
            ),
            "reasons": (
                ((a.get("p0") or {}).get("risk_off_reasons"))
                or (((a.get("gate") or {}).get("snapshot") or {}).get("reasons"))
                or (((((a.get("gate") or {}).get("snapshot") or {}).get("risk_off") or {})).get("reasons"))
                or None
            ),
            "note": ((a.get("gate") or {}).get("note") if isinstance(a.get("gate"), dict) else None),
        }
    ret_col = _pick_ret_col(df)
    if ret_col is None and ("net_ret" in df.columns):
        ret_col = "net_ret"
    if ret_col is None:
        ret_col = selected_source.get("ret_col")

    now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = str(os.getenv("RUN_ID", "")).strip() or f"paper_pnl_{now_tag}"
    out_json = logs_dir / f"paper_pnl_summary_{now_tag}.json"
    last_json = logs_dir / "paper_pnl_summary_last.json"
    rows_total = int(op_scope.get("rows_before", 0) or 0)
    rows_as_of = int(len(df))
    rows_check = _build_rows_validation(rows_total, rows_as_of)
    if not rows_check["valid"]:
        print(
            f"[HARD_FAIL] invalid rows scope rows_total={rows_total} rows_as_of={rows_as_of} "
            f"source={trades_path}"
        )
        return 2

    if df.empty or ret_col is None:
        as_of_ymd = ""
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "run_id": run_id,
            "as_of": as_of_ymd,
            "source": str(trades_path),
            "rows_total": rows_total,
            "rows_as_of": rows_as_of,
            "trades_used": 0,
            "ret_col": ret_col,
            "avg_ret": 0.0,
            "win_rate": None,
            "strategies": [],
            "gross_pf": None,
            "note": "no trades or missing return column",
            "equity": _equity_metrics(pd.DataFrame(), ret_col or "pnl_pct", _to_float((cfg or {}).get("capital_total") if isinstance(cfg, dict) else None)),
            "meta": {
                "config_file": cfg_file,
                "lock_file": lock_file,
                "risk_context": risk_ctx,
                "operational_scope": op_scope,
                "blocked_entry_filter": blocked_filter,
                "unauditable_surge_filter": unauditable_surge_filter,
                "cost_profile": cost_meta,
                "trades_calc_sync": trades_calc_sync,
                "source_selection": selected_source.get("selection_meta", {}),
            },
        }
        _attach_ssot_chain_meta(payload, ssot_chain, df, as_of_ymd)
        _attach_stats_trace(
            payload,
            run_id=run_id,
            as_of_ymd=as_of_ymd,
            rows_total=rows_total,
            rows_as_of=rows_as_of,
            ret_col=ret_col,
            trades_used=0,
            avg_ret=0.0,
            win_rate=None,
            strategies=[],
            gross_pf=None,
            gpos=0.0,
            gneg_abs=0.0,
            eqm=payload.get("equity") or {},
        )
        _promote_after_close(payload)
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        last_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[PNL_META] run_id={run_id} as_of={as_of_ymd or '-'} rows_total={rows_total} rows_as_of={rows_as_of}")
        print(str(out_json))
        return 0

    # numeric returns
    rets = [v for v in df[ret_col].apply(_to_float).tolist() if isinstance(v, (int, float)) and not math.isnan(v)]
    trades_used = int(len(rets))

    overall_stats = _return_stats(rets)
    avg_ret = float(overall_stats["avg_ret"])
    win_rate = overall_stats["win_rate"]
    gpos = float(overall_stats["gross_profit_sum"])
    gneg_abs = float(overall_stats["gross_loss_abs_sum"])
    gross_pf = overall_stats["gross_pf"]
    strategies = _build_strategy_stats(df, ret_col)

    eqm = _equity_metrics(df, ret_col, _to_float((cfg or {}).get("capital_total") if isinstance(cfg, dict) else None))
    as_of_ymd = str(eqm.get("last_exit_date") or "")

    freshness = _as_of_freshness(as_of_ymd)
    if freshness.get("stale"):
        print("[STALE] paper_pnl_summary: %s" % freshness.get("note"))
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "as_of": as_of_ymd,
        # [2026-09-10] generated_at 은 늘 오늘이다. 신선도는 **이 칸**을 봐야 한다.
        "as_of_freshness": freshness,
        "source": str(trades_path),
        "rows_total": rows_total,
        "rows_as_of": rows_as_of,
        "trades_used": trades_used,
        "ret_col": ret_col,
        "avg_ret": avg_ret,
        "win_rate": win_rate,
        "strategies": strategies,
        "gross_pf": gross_pf,
        "equity": eqm,
        "meta": {
            "config_file": cfg_file,
            "lock_file": lock_file,
            "risk_context": risk_ctx,
            "operational_scope": op_scope,
            "blocked_entry_filter": blocked_filter,
            "unauditable_surge_filter": unauditable_surge_filter,
            "trades_calc_sync": trades_calc_sync,
            "source_selection": selected_source.get("selection_meta", {}),
        },
    }
    payload["meta"]["cost_profile"] = cost_meta
    _attach_ssot_chain_meta(payload, ssot_chain, df, as_of_ymd)
    _attach_stats_trace(
        payload,
        run_id=run_id,
        as_of_ymd=as_of_ymd,
        rows_total=rows_total,
        rows_as_of=rows_as_of,
        ret_col=ret_col,
        trades_used=trades_used,
        avg_ret=avg_ret,
        win_rate=win_rate,
        strategies=strategies,
        gross_pf=gross_pf,
        gpos=gpos,
        gneg_abs=gneg_abs,
        eqm=eqm,
    )
    _promote_after_close(payload)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    # keep a stable pointer for other scripts
    last_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[PNL_META] run_id={run_id} as_of={as_of_ymd or '-'} rows_total={rows_total} rows_as_of={rows_as_of}")
    print(str(out_json))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
