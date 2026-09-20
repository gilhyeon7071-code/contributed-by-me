from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
ROOTB = Path(r"E:\vibe\buffett")
LIVE_PATH = ROOTB / "data" / "live" / "live_fills.csv"
LEDGER_PATH = ROOTB / "data" / "ledger" / "paper_fills_ledger.csv"
PAPER_FILLS_PATH = ROOT / "paper" / "fills.csv"
STATUS_PATH = ROOT / "2_Logs" / "ledger_live_fills_repair_latest.json"


def _norm_date_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r"\D", "", regex=True).str.slice(0, 8)


def _norm_code_value(v: object) -> str:
    raw = str(v or "").strip().replace(".0", "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def _norm_side_value(v: object) -> str:
    text = str(v or "").strip().upper()
    return {"B": "BUY", "S": "SELL"}.get(text, text)


def _nonblank_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    return df[col].fillna("").astype(str).str.strip().replace({"nan": "", "None": "", "<NA>": ""})


def _datetime_series(df: pd.DataFrame) -> pd.Series:
    return _nonblank_series(df, "datetime")


def _loose_trade_keys(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    date_s = _norm_date_series(df["date"]) if "date" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    datetime_s = _datetime_series(df)
    code_s = df["code"].map(_norm_code_value) if "code" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    side_s = df["side"].map(_norm_side_value) if "side" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    qty_col = "fill_qty" if "fill_qty" in df.columns else "qty"
    qty_s = pd.to_numeric(df[qty_col] if qty_col in df.columns else pd.Series([0] * len(df), index=df.index), errors="coerce").round(6).astype(str)
    return date_s + "|" + datetime_s + "|" + code_s + "|" + side_s + "|" + qty_s


def _exact_trade_keys(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    date_s = _norm_date_series(df["date"]) if "date" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    datetime_s = _datetime_series(df)
    code_s = df["code"].map(_norm_code_value) if "code" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    side_s = df["side"].map(_norm_side_value) if "side" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    qty_col = "fill_qty" if "fill_qty" in df.columns else "qty"
    px_col = "fill_price" if "fill_price" in df.columns else "price"
    qty_s = pd.to_numeric(df[qty_col] if qty_col in df.columns else pd.Series([0] * len(df), index=df.index), errors="coerce").round(6).astype(str)
    px_s = pd.to_numeric(df[px_col] if px_col in df.columns else pd.Series([0] * len(df), index=df.index), errors="coerce").round(6).astype(str)
    return date_s + "|" + datetime_s + "|" + code_s + "|" + side_s + "|" + qty_s + "|" + px_s


def _trade_identity_keys(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    date_s = _norm_date_series(df["date"]) if "date" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    datetime_s = _datetime_series(df)
    code_s = df["code"].map(_norm_code_value) if "code" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    side_s = df["side"].map(_norm_side_value) if "side" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    return date_s + "|" + datetime_s + "|" + code_s + "|" + side_s


def _repair_identity_keys(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    order_s = _nonblank_series(df, "order_id")
    fill_s = _nonblank_series(df, "fill_id")
    exact_s = _exact_trade_keys(df)
    keys = pd.Series([""] * len(df), index=df.index, dtype=str)
    has_order = order_s.ne("")
    has_fill = fill_s.ne("")
    keys.loc[has_order] = "ORDER|" + order_s.loc[has_order] + "|" + exact_s.loc[has_order]
    fill_only = ~has_order & has_fill
    keys.loc[fill_only] = "FILL|" + fill_s.loc[fill_only]
    trade_only = ~has_order & ~has_fill & exact_s.ne("")
    keys.loc[trade_only] = "TRADE|" + exact_s.loc[trade_only]
    return keys


def _exec_sync_identity_dedupe_mask(df: pd.DataFrame, start_ymd: str) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=bool)
    keys = _trade_identity_keys(df)
    date_s = _norm_date_series(df["date"]) if "date" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    source_s = _nonblank_series(df, "source").str.upper()
    eligible = date_s.ge(start_ymd) & date_s.str.len().eq(8)
    exec_keys = set(keys[eligible & source_s.eq("EXEC_SYNC")].tolist())
    stale_source = source_s.isin(["", "PAPER"])
    return eligible & keys.isin(exec_keys) & stale_source


def _oper_start_ymd(raw: str = "") -> str:
    text = "".join(ch for ch in str(raw or os.getenv("PAPER_OPER_START_YMD", "20260301")) if ch.isdigit())
    return text[:8] if len(text) >= 8 else "20260301"


def _source_order_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    source_order = _nonblank_series(df, "source_order_id")
    if "note" in df.columns:
        note_source_order = _nonblank_series(df, "note").str.extract(r"(?:^|;)source_order_id=([^;]*)", expand=False).fillna("").astype(str).str.strip()
        source_order = source_order.where(source_order.ne(""), note_source_order)
    order = _nonblank_series(df, "order_id")
    return source_order.where(source_order.ne(""), order)


def _entry_order_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    entry_order = _nonblank_series(df, "entry_order_id")
    if "note" in df.columns:
        note_entry_order = _nonblank_series(df, "note").str.extract(r"(?:^|;)entry_order_id=([^;]*)", expand=False).fillna("").astype(str).str.strip()
        entry_order = entry_order.where(entry_order.ne(""), note_entry_order)
    return entry_order


def _entry_lineage_keys(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    date_s = _norm_date_series(df["date"]) if "date" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    code_s = df["code"].map(_norm_code_value) if "code" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    side_s = df["side"].map(_norm_side_value) if "side" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    return date_s + "|" + code_s + "|" + side_s + "|" + _entry_order_series(df)


def _exec_sync_entry_lineage_dedupe_mask(df: pd.DataFrame, start_ymd: str) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=bool)
    bridge_sources = {"EXEC_SYNC", "PAPER_ENGINE_BRIDGE"}
    date_s = _norm_date_series(df["date"]) if "date" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    source_s = _nonblank_series(df, "source").str.upper()
    entry_s = _entry_order_series(df)
    eligible = date_s.ge(start_ymd) & date_s.str.len().eq(8) & entry_s.ne("")
    keys = _entry_lineage_keys(df)
    native_keys = set(keys[eligible & (~source_s.isin(bridge_sources))].tolist())
    return eligible & source_s.isin(bridge_sources) & keys.isin(native_keys)


def _exec_sync_redundant_keys(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    date_s = _norm_date_series(df["date"]) if "date" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    code_s = df["code"].map(_norm_code_value) if "code" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    side_s = df["side"].map(_norm_side_value) if "side" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    qty_col = "fill_qty" if "fill_qty" in df.columns else "qty"
    qty_s = pd.to_numeric(df[qty_col] if qty_col in df.columns else pd.Series([0] * len(df), index=df.index), errors="coerce").fillna(0.0).astype(str)
    return date_s + "|" + code_s + "|" + side_s + "|" + qty_s + "|" + _source_order_series(df)


def _redundant_exec_sync_missing_mask(missing: pd.DataFrame, ledger: pd.DataFrame) -> pd.Series:
    if missing.empty:
        return pd.Series([], dtype=bool)
    if ledger.empty:
        return pd.Series([False] * len(missing), index=missing.index, dtype=bool)
    bridge_sources = {"EXEC_SYNC", "PAPER_ENGINE_BRIDGE"}
    ledger_source = _nonblank_series(ledger, "source").str.upper()
    ledger_source_order = _source_order_series(ledger)
    ledger_keys = set(
        _exec_sync_redundant_keys(ledger)
        .loc[(~ledger_source.isin(bridge_sources)) & ledger_source_order.ne("")]
        .tolist()
    )
    missing_source = _nonblank_series(missing, "source").str.upper()
    missing_side = missing["side"].map(_norm_side_value) if "side" in missing.columns else pd.Series([""] * len(missing), index=missing.index, dtype=str)
    missing_source_order = _source_order_series(missing)
    legacy_redundant = (
        missing_source.isin(bridge_sources)
        & missing_side.eq("SELL")
        & missing_source_order.ne("")
        & _exec_sync_redundant_keys(missing).isin(ledger_keys)
    )
    missing_entry_order = _entry_order_series(missing)
    ledger_entry_order = _entry_order_series(ledger)
    ledger_lineage_keys = set(
        _entry_lineage_keys(ledger)
        .loc[(~ledger_source.isin(bridge_sources)) & ledger_entry_order.ne("")]
        .tolist()
    )
    entry_lineage_redundant = (
        missing_source.isin(bridge_sources)
        & missing_entry_order.ne("")
        & _entry_lineage_keys(missing).isin(ledger_lineage_keys)
    )
    return legacy_redundant | entry_lineage_redundant


def _paper_closed_codes(start_ymd: str) -> set[str]:
    if not PAPER_FILLS_PATH.exists():
        return set()
    try:
        fills = pd.read_csv(PAPER_FILLS_PATH, dtype=str, encoding="utf-8-sig")
    except Exception:
        return set()
    if fills.empty or "code" not in fills.columns or "side" not in fills.columns:
        return set()
    date_source = fills["datetime"] if "datetime" in fills.columns else fills.get("date", pd.Series([""] * len(fills), index=fills.index))
    date_s = _norm_date_series(date_source)
    work = fills.loc[date_s.ge(start_ymd) & date_s.str.len().eq(8)].copy()
    if work.empty:
        return set()
    qty_col = "fill_qty" if "fill_qty" in work.columns else "qty"
    qty = pd.to_numeric(work[qty_col] if qty_col in work.columns else pd.Series([0] * len(work), index=work.index), errors="coerce").fillna(0.0)
    side = work["side"].map(_norm_side_value)
    signed = qty.where(side.eq("BUY"), -qty.where(side.eq("SELL"), 0.0))
    net = signed.groupby(work["code"].map(_norm_code_value)).sum()
    return {str(code) for code, value in net.items() if str(code) and float(value) <= 0.0}


def _paper_closed_exec_sync_buy_mask(df: pd.DataFrame, start_ymd: str, closed_codes: set[str] | None = None) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=bool)
    closed = closed_codes if closed_codes is not None else _paper_closed_codes(start_ymd)
    if not closed:
        return pd.Series([False] * len(df), index=df.index, dtype=bool)
    date_s = _norm_date_series(df["date"]) if "date" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    source_s = _nonblank_series(df, "source").str.upper()
    side_s = df["side"].map(_norm_side_value) if "side" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    code_s = df["code"].map(_norm_code_value) if "code" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    return (
        date_s.ge(start_ymd)
        & date_s.str.len().eq(8)
        & source_s.isin({"EXEC_SYNC", "PAPER_ENGINE_BRIDGE"})
        & side_s.eq("BUY")
        & code_s.isin(closed)
    )


def _paper_exact_trade_keys(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    date_s = _norm_date_series(df["datetime"]) if "datetime" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    datetime_s = _datetime_series(df)
    code_s = df["code"].map(_norm_code_value) if "code" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    side_s = df["side"].map(_norm_side_value) if "side" in df.columns else pd.Series([""] * len(df), index=df.index, dtype=str)
    qty_col = "qty" if "qty" in df.columns else "fill_qty"
    px_col = "price" if "price" in df.columns else "fill_price"
    qty_s = pd.to_numeric(df[qty_col] if qty_col in df.columns else pd.Series([0] * len(df), index=df.index), errors="coerce").round(6).astype(str)
    px_s = pd.to_numeric(df[px_col] if px_col in df.columns else pd.Series([0] * len(df), index=df.index), errors="coerce").round(6).astype(str)
    return date_s + "|" + datetime_s + "|" + code_s + "|" + side_s + "|" + qty_s + "|" + px_s


def _roota_ledger_rows(start_ymd: str, ledger_columns: list[str]) -> pd.DataFrame:
    if not PAPER_FILLS_PATH.exists():
        return pd.DataFrame(columns=ledger_columns)
    try:
        fills = pd.read_csv(PAPER_FILLS_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception:
        return pd.DataFrame(columns=ledger_columns)
    need = {"datetime", "code", "side", "qty", "price"}
    if fills.empty or not need.issubset(set(fills.columns)):
        return pd.DataFrame(columns=ledger_columns)
    date_s = _norm_date_series(fills["datetime"])
    work = fills.loc[date_s.ge(start_ymd) & date_s.str.len().eq(8)].copy()
    if work.empty:
        return pd.DataFrame(columns=ledger_columns)
    note = work["note"] if "note" in work.columns else pd.Series([""] * len(work), index=work.index)
    order = work["order_id"] if "order_id" in work.columns else pd.Series([""] * len(work), index=work.index)
    out = pd.DataFrame(
        {
            "date": _norm_date_series(work["datetime"]),
            "datetime": work["datetime"].astype(str),
            "code": work["code"].map(_norm_code_value),
            "side": work["side"].map(_norm_side_value),
            "fill_price": pd.to_numeric(work["price"], errors="coerce"),
            "fill_qty": pd.to_numeric(work["qty"], errors="coerce"),
            "qty": pd.to_numeric(work["qty"], errors="coerce"),
            "price": pd.to_numeric(work["price"], errors="coerce"),
            "order_id": order.astype(str).str.strip(),
            "source_order_id": order.astype(str).str.strip(),
            "note": note.astype(str),
            "source": "PAPER",
            "as_of": _norm_date_series(work["datetime"]),
        }
    )
    out = out[(out["code"].str.len().eq(6)) & (out["side"].isin(["BUY", "SELL"]))].copy()
    out = out[(pd.to_numeric(out["fill_qty"], errors="coerce").fillna(0.0) > 0) & (pd.to_numeric(out["fill_price"], errors="coerce").fillna(0.0) > 0)].copy()
    if ledger_columns:
        for col in ledger_columns:
            if col not in out.columns:
                out[col] = ""
        out = out[ledger_columns].copy()
    return out


def _roota_bridge_extra_mask(ledger: pd.DataFrame, start_ymd: str, roota_keys: set[str]) -> pd.Series:
    if ledger.empty or not roota_keys:
        return pd.Series([False] * len(ledger), index=ledger.index, dtype=bool)
    date_s = _norm_date_series(ledger["date"]) if "date" in ledger.columns else pd.Series([""] * len(ledger), index=ledger.index, dtype=str)
    source_s = _nonblank_series(ledger, "source").str.upper()
    ledger_keys = _exact_trade_keys(ledger)
    return (
        date_s.ge(start_ymd)
        & date_s.str.len().eq(8)
        & source_s.isin({"EXEC_SYNC", "PAPER_ENGINE_BRIDGE"})
        & ~ledger_keys.isin(roota_keys)
    )


def _roota_absent_bridge_missing_mask(missing: pd.DataFrame, start_ymd: str, roota_keys: set[str]) -> pd.Series:
    if missing.empty or not roota_keys:
        return pd.Series([False] * len(missing), index=missing.index, dtype=bool)
    date_s = _norm_date_series(missing["date"]) if "date" in missing.columns else pd.Series([""] * len(missing), index=missing.index, dtype=str)
    source_s = _nonblank_series(missing, "source").str.upper()
    missing_keys = _exact_trade_keys(missing)
    return (
        date_s.ge(start_ymd)
        & date_s.str.len().eq(8)
        & source_s.isin({"", "PAPER", "LEGACY_PAPER", "EXEC_SYNC", "PAPER_ENGINE_BRIDGE"})
        & ~missing_keys.isin(roota_keys)
    )


def _atomic_write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp_{dt.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    tmp.replace(path)


def _load_ledger_helpers():
    import sys

    sys.path.insert(0, str(ROOTB / "tools"))
    from ledger_cost_model import build_enriched_ledger, deduplicate_ledger  # type: ignore

    return build_enriched_ledger, deduplicate_ledger


def _backup_inputs(run_id: str) -> str:
    today = dt.datetime.now().strftime("%Y%m%d")
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = ROOT / "backup" / f"{today}_ledger_live_fills_repair" / stamp
    backup_dir.mkdir(parents=True, exist_ok=True)
    if LEDGER_PATH.exists():
        shutil.copy2(LEDGER_PATH, backup_dir / "paper_fills_ledger.csv.bak")
    if LIVE_PATH.exists():
        shutil.copy2(LIVE_PATH, backup_dir / "live_fills.csv.bak")
    return str(backup_dir)


def build_repair(apply: bool, start_ymd: str) -> Dict[str, Any]:
    run_id = "LEDGER_LIVE_FILL_REPAIR_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    live = pd.read_csv(LIVE_PATH, dtype=str, encoding="utf-8-sig") if LIVE_PATH.exists() else pd.DataFrame()
    ledger = pd.read_csv(LEDGER_PATH, dtype=str, encoding="utf-8-sig") if LEDGER_PATH.exists() else pd.DataFrame()
    start = _oper_start_ymd(start_ymd)

    live_ids = set(_nonblank_series(live, "fill_id").loc[lambda x: x.ne("")].tolist()) if not live.empty else set()
    ledger_ids = set(_nonblank_series(ledger, "fill_id").loc[lambda x: x.ne("")].tolist()) if not ledger.empty else set()
    live_repair_ids = set(_repair_identity_keys(live).loc[lambda x: x.ne("")].tolist()) if not live.empty else set()
    ledger_repair_ids = set(_repair_identity_keys(ledger).loc[lambda x: x.ne("")].tolist()) if not ledger.empty else set()
    live_fill_by_repair_key: Dict[str, str] = {}
    if not live.empty:
        live_repair_key_s = _repair_identity_keys(live)
        live_fill_s = _nonblank_series(live, "fill_id")
        for key, fill_id in zip(live_repair_key_s.tolist(), live_fill_s.tolist()):
            if str(key).strip() and str(fill_id).strip():
                live_fill_by_repair_key[str(key).strip()] = str(fill_id).strip()
    if live.empty:
        missing = pd.DataFrame()
    else:
        work = live.copy()
        work["_date8"] = _norm_date_series(work["date"]) if "date" in work.columns else ""
        work["_fill_id"] = _nonblank_series(work, "fill_id")
        work["_repair_key"] = _repair_identity_keys(work)
        missing = work[
            (work["_date8"] >= start)
            & (work["_date8"].str.len() == 8)
            & (work["_repair_key"].ne(""))
            & (
                (
                    work["_fill_id"].ne("")
                    & (~work["_fill_id"].isin(ledger_ids))
                    & (~work["_repair_key"].isin(ledger_repair_ids))
                )
                | (
                    work["_fill_id"].eq("")
                    & (~work["_repair_key"].isin(ledger_repair_ids))
                )
            )
        ].copy()
        missing = missing.drop_duplicates(subset=["_repair_key"], keep="last")

    roota_rows = _roota_ledger_rows(start, list(ledger.columns) if not ledger.empty else [])
    roota_keys: set[str] = set()
    if not roota_rows.empty:
        roota_keys = set(_exact_trade_keys(roota_rows).loc[lambda x: x.ne("")].tolist())
    roota_absent_bridge_missing = _roota_absent_bridge_missing_mask(missing, start, roota_keys) if len(missing) else pd.Series([], dtype=bool)
    roota_absent_bridge_missing_rows = int(roota_absent_bridge_missing.sum()) if len(roota_absent_bridge_missing) else 0
    if roota_absent_bridge_missing_rows > 0:
        missing = missing.loc[~roota_absent_bridge_missing].copy()
    redundant_exec_sync_missing = _redundant_exec_sync_missing_mask(missing, ledger) if len(missing) else pd.Series([], dtype=bool)
    redundant_exec_sync_missing_rows = int(redundant_exec_sync_missing.sum()) if len(redundant_exec_sync_missing) else 0
    if redundant_exec_sync_missing_rows > 0:
        missing = missing.loc[~redundant_exec_sync_missing].copy()
    paper_closed_codes = _paper_closed_codes(start)
    redundant_exec_sync_buy_missing = _paper_closed_exec_sync_buy_mask(missing, start, paper_closed_codes) if len(missing) else pd.Series([], dtype=bool)
    if len(redundant_exec_sync_buy_missing) and redundant_exec_sync_buy_missing.any():
        missing_fill_ids_for_closed = _nonblank_series(missing, "fill_id")
        missing_exact_for_closed = _exact_trade_keys(missing)
        confirmed_missing = missing_fill_ids_for_closed.isin(live_ids)
        if roota_keys:
            confirmed_missing = confirmed_missing | missing_exact_for_closed.isin(roota_keys)
        redundant_exec_sync_buy_missing = redundant_exec_sync_buy_missing & ~confirmed_missing
    redundant_exec_sync_buy_missing_rows = int(redundant_exec_sync_buy_missing.sum()) if len(redundant_exec_sync_buy_missing) else 0
    if redundant_exec_sync_buy_missing_rows > 0:
        missing = missing.loc[~redundant_exec_sync_buy_missing].copy()
    roota_missing_rows = pd.DataFrame()
    if not roota_rows.empty:
        ledger_exact_keys = set(_exact_trade_keys(ledger).loc[lambda x: x.ne("")].tolist()) if not ledger.empty else set()
        pending_exact_keys = set(_exact_trade_keys(missing).loc[lambda x: x.ne("")].tolist()) if len(missing) else set()
        roota_row_keys = _exact_trade_keys(roota_rows)
        roota_missing_rows = roota_rows.loc[(~roota_row_keys.isin(ledger_exact_keys)) & (~roota_row_keys.isin(pending_exact_keys))].copy()
        if not roota_missing_rows.empty:
            missing = pd.concat([missing.drop(columns=["_date8", "_fill_id", "_repair_key"], errors="ignore"), roota_missing_rows], ignore_index=True)

    result: Dict[str, Any] = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "apply": bool(apply),
        "oper_start_ymd": start,
        "live_path": str(LIVE_PATH),
        "ledger_path": str(LEDGER_PATH),
        "live_rows": int(len(live)),
        "ledger_rows_before": int(len(ledger)),
        "missing_rows": int(len(missing)),
        "missing_fill_ids": _nonblank_series(missing, "fill_id").loc[lambda x: x.ne("")].tolist() if len(missing) else [],
        "missing_repair_keys": _nonblank_series(missing, "_repair_key").tolist() if len(missing) else [],
        "missing_blank_fill_id_rows": int(_nonblank_series(missing, "fill_id").eq("").sum()) if len(missing) else 0,
        "replaceable_rows": 0,
        "replaceable_rows_by_exact_qty": 0,
        "replaceable_rows_by_trade_identity": 0,
        "pure_add_rows": 0,
        "replaced_rows": 0,
        "metadata_blank_rows": 0,
        "metadata_filled_rows": 0,
        "exec_sync_identity_dedupe_rows": 0,
        "redundant_exec_sync_missing_rows": int(redundant_exec_sync_missing_rows),
        "redundant_exec_sync_buy_missing_rows": int(redundant_exec_sync_buy_missing_rows),
        "roota_absent_bridge_missing_rows": int(roota_absent_bridge_missing_rows),
        "roota_missing_rows": int(len(roota_missing_rows)),
        "roota_bridge_extra_rows": 0,
        "paper_closed_exec_sync_buy_rows": 0,
        "live_absent_duplicate_rows": 0,
        "fill_id_drift_rows": 0,
        "ledger_rows_after": int(len(ledger)),
        "backup_dir": "",
        "status": "NOOP",
    }
    if len(missing):
        missing = missing.drop(columns=["_date8", "_fill_id", "_repair_key"], errors="ignore")
    metadata_mask = pd.Series([], dtype=bool)
    if not ledger.empty and "date" in ledger.columns:
        if "run_id" not in ledger.columns:
            ledger["run_id"] = ""
        if "as_of" not in ledger.columns:
            ledger["as_of"] = ""
        ledger_dates = _norm_date_series(ledger["date"])
        run_blank = ledger["run_id"].fillna("").astype(str).str.strip().eq("")
        asof_blank = ledger["as_of"].fillna("").astype(str).str.strip().eq("")
        metadata_mask = (ledger_dates >= start) & ledger_dates.str.len().eq(8) & (run_blank | asof_blank)
        result["metadata_blank_rows"] = int(metadata_mask.sum())
    dedupe_mask = _exec_sync_identity_dedupe_mask(ledger, start) if not ledger.empty else pd.Series([], dtype=bool)
    if not ledger.empty and len(dedupe_mask):
        ledger_fill_ids = _nonblank_series(ledger, "fill_id")
        dedupe_mask = dedupe_mask & ~ledger_fill_ids.isin(live_ids)
        if roota_keys:
            dedupe_mask = dedupe_mask & ~_exact_trade_keys(ledger).isin(roota_keys)
    result["exec_sync_identity_dedupe_rows"] = int(dedupe_mask.sum()) if not ledger.empty else 0
    entry_lineage_dedupe_mask = _exec_sync_entry_lineage_dedupe_mask(ledger, start) if not ledger.empty else pd.Series([], dtype=bool)
    result["exec_sync_entry_lineage_dedupe_rows"] = int(entry_lineage_dedupe_mask.sum()) if not ledger.empty else 0
    paper_closed_buy_mask = _paper_closed_exec_sync_buy_mask(ledger, start, paper_closed_codes) if not ledger.empty else pd.Series([], dtype=bool)
    if not ledger.empty and len(paper_closed_buy_mask) and paper_closed_buy_mask.any():
        ledger_fill_ids_for_closed = _nonblank_series(ledger, "fill_id")
        ledger_exact_for_closed = _exact_trade_keys(ledger)
        confirmed_ledger = ledger_fill_ids_for_closed.isin(live_ids)
        if roota_keys:
            confirmed_ledger = confirmed_ledger | ledger_exact_for_closed.isin(roota_keys)
        paper_closed_buy_mask = paper_closed_buy_mask & ~confirmed_ledger
    result["paper_closed_exec_sync_buy_rows"] = int(paper_closed_buy_mask.sum()) if not ledger.empty else 0
    roota_bridge_extra_mask = _roota_bridge_extra_mask(ledger, start, roota_keys) if not ledger.empty else pd.Series([], dtype=bool)
    result["roota_bridge_extra_rows"] = int(roota_bridge_extra_mask.sum()) if not ledger.empty and len(roota_bridge_extra_mask) else 0
    live_absent_mask = pd.Series([], dtype=bool)
    if not ledger.empty and not live.empty:
        ledger_dates = _norm_date_series(ledger["date"]) if "date" in ledger.columns else pd.Series([""] * len(ledger), index=ledger.index, dtype=str)
        ledger_fill_ids = _nonblank_series(ledger, "fill_id")
        ledger_repair_id_s = _repair_identity_keys(ledger)
        ledger_source_s = _nonblank_series(ledger, "source").str.upper()
        live_exact_keys = set(_exact_trade_keys(live).tolist())
        ledger_exact_keys = _exact_trade_keys(ledger)
        eligible = ledger_dates.ge(start) & ledger_dates.str.len().eq(8)
        live_absent_mask = (
            eligible
            & ledger_exact_keys.isin(live_exact_keys)
            & ~ledger_fill_ids.isin(live_ids)
            & ~ledger_repair_id_s.isin(live_repair_ids)
            & ledger_source_s.isin(["", "PAPER", "EXEC_SYNC", "PAPER_ENGINE_BRIDGE", "LEGACY_PAPER"])
        )
        if roota_keys:
            live_absent_mask = live_absent_mask & ~ledger_exact_keys.isin(roota_keys)
    result["live_absent_duplicate_rows"] = int(live_absent_mask.sum()) if not ledger.empty and len(live_absent_mask) else 0
    fill_id_drift_mask = pd.Series([], dtype=bool)
    fill_id_drift_target = pd.Series([], dtype=str)
    if not ledger.empty and live_fill_by_repair_key:
        ledger_dates = _norm_date_series(ledger["date"]) if "date" in ledger.columns else pd.Series([""] * len(ledger), index=ledger.index, dtype=str)
        ledger_repair_key_s = _repair_identity_keys(ledger)
        ledger_fill_s = _nonblank_series(ledger, "fill_id")
        fill_id_drift_target = ledger_repair_key_s.map(live_fill_by_repair_key).fillna("").astype(str)
        fill_id_drift_mask = (
            ledger_dates.ge(start)
            & ledger_dates.str.len().eq(8)
            & ledger_repair_key_s.ne("")
            & fill_id_drift_target.ne("")
            & ledger_fill_s.ne(fill_id_drift_target)
        )
    result["fill_id_drift_rows"] = int(fill_id_drift_mask.sum()) if not ledger.empty and len(fill_id_drift_mask) else 0

    if (
        missing.empty
        and int(result["metadata_blank_rows"]) == 0
        and int(result["exec_sync_identity_dedupe_rows"]) == 0
        and int(result["exec_sync_entry_lineage_dedupe_rows"]) == 0
        and int(result["paper_closed_exec_sync_buy_rows"]) == 0
        and int(result["roota_missing_rows"]) == 0
        and int(result["roota_bridge_extra_rows"]) == 0
        and int(result["live_absent_duplicate_rows"]) == 0
        and int(result["fill_id_drift_rows"]) == 0
    ):
        return result

    # Use exact keys (including price) so sibling partial fills at the same
    # timestamp with the same quantity but different prices are not dropped.
    missing_keys = set(_exact_trade_keys(missing).tolist())
    ledger_keys = _exact_trade_keys(ledger) if not ledger.empty else pd.Series([], dtype=str)
    ledger_source_s = _nonblank_series(ledger, "source").str.upper() if not ledger.empty else pd.Series([], dtype=str)
    stale_source = ledger_source_s.isin(["", "PAPER"]) if not ledger.empty else pd.Series([], dtype=bool)
    exact_replace_mask = (ledger_keys.isin(missing_keys) & stale_source) if not ledger.empty else pd.Series([], dtype=bool)
    # Same timestamp/code/side can contain separate partial exits with different
    # quantities, so identity-only replacement would drop valid sibling fills.
    identity_replace_mask = pd.Series([False] * len(ledger), index=ledger.index, dtype=bool) if not ledger.empty else pd.Series([], dtype=bool)
    replace_mask = (exact_replace_mask | identity_replace_mask) if not ledger.empty else pd.Series([], dtype=bool)
    drop_mask = (replace_mask | dedupe_mask | entry_lineage_dedupe_mask | paper_closed_buy_mask | roota_bridge_extra_mask | live_absent_mask) if not ledger.empty else pd.Series([], dtype=bool)
    result["replaceable_rows_by_exact_qty"] = int(exact_replace_mask.sum()) if not ledger.empty else 0
    result["replaceable_rows_by_trade_identity"] = int(identity_replace_mask.sum()) if not ledger.empty else 0
    result["replaceable_rows"] = int(replace_mask.sum()) if not ledger.empty else 0
    result["pure_add_rows"] = int(max(0, len(missing) - int(result["replaceable_rows"])))
    result["status"] = "DRY_RUN"
    if not apply:
        return result

    backup_dir = _backup_inputs(run_id)
    build_enriched_ledger, deduplicate_ledger = _load_ledger_helpers()
    if not ledger.empty and len(fill_id_drift_mask) and int(result["fill_id_drift_rows"]) > 0:
        ledger.loc[fill_id_drift_mask, "fill_id"] = fill_id_drift_target.loc[fill_id_drift_mask].values
    if ledger.empty:
        merged_base = missing.copy()
    else:
        merged_base = pd.concat([ledger.loc[~drop_mask].copy(), missing.copy()], ignore_index=True)
    if not merged_base.empty and "date" in merged_base.columns:
        if "run_id" not in merged_base.columns:
            merged_base["run_id"] = ""
        if "as_of" not in merged_base.columns:
            merged_base["as_of"] = ""
        merged_dates = _norm_date_series(merged_base["date"])
        run_blank = merged_base["run_id"].fillna("").astype(str).str.strip().eq("")
        asof_blank = merged_base["as_of"].fillna("").astype(str).str.strip().eq("")
        fill_mask = (merged_dates >= start) & merged_dates.str.len().eq(8) & (run_blank | asof_blank)
        merged_base.loc[fill_mask & run_blank, "run_id"] = run_id
        merged_base.loc[fill_mask & asof_blank, "as_of"] = merged_dates[fill_mask & asof_blank]
        result["metadata_filled_rows"] = int(fill_mask.sum())
    merged_base = merged_base.astype(object)
    enriched = build_enriched_ledger(merged_base, root=str(ROOTB), default_source="PAPER")
    out = deduplicate_ledger(enriched)
    _atomic_write_csv(LEDGER_PATH, out)
    result["backup_dir"] = backup_dir
    result["replaced_rows"] = int(drop_mask.sum()) if not ledger.empty else 0
    result["ledger_rows_after"] = int(len(out))
    if not missing.empty:
        result["status"] = "APPLIED"
    elif int(result["fill_id_drift_rows"]) > 0:
        result["status"] = "APPLIED_FILL_ID_DRIFT"
    elif (
        int(result["exec_sync_identity_dedupe_rows"]) > 0
        or int(result["exec_sync_entry_lineage_dedupe_rows"]) > 0
        or int(result["paper_closed_exec_sync_buy_rows"]) > 0
        or int(result["roota_missing_rows"]) > 0
        or int(result["roota_bridge_extra_rows"]) > 0
        or int(result["live_absent_duplicate_rows"]) > 0
    ):
        result["status"] = "APPLIED_DEDUPE"
    else:
        result["status"] = "APPLIED_METADATA"
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description="Repair RootB ledger fill_id gaps from live_fills for the operational period.")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--start-ymd", default="")
    args = ap.parse_args()

    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    result = build_repair(apply=bool(args.apply), start_ymd=str(args.start_ymd or ""))
    STATUS_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] in {"NOOP", "DRY_RUN", "APPLIED", "APPLIED_FILL_ID_DRIFT", "APPLIED_DEDUPE", "APPLIED_METADATA"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
