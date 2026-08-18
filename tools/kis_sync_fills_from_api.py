from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kis_order_client import KISApiError, KISOrderClient
from utils.pipeline_audit import log_pipeline_event, count_rows


PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
logger = logging.getLogger("kis_sync_fills_from_api")


def _norm_ymd(v: object) -> str:
    s = str(v or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8]


def _to_int(v: object, default: int = 0) -> int:
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return int(default)


def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    colset = {c.lower(): c for c in cols}
    for cand in candidates:
        hit = colset.get(cand.lower())
        if hit is not None:
            return hit
    return None


def _series_or_default(df: pd.DataFrame, col: Optional[str], default: str) -> pd.Series:
    if col and col in df.columns:
        return df[col].astype(str)
    return pd.Series([default] * len(df), index=df.index, dtype="object")


def _read_csv_safe(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame()


def _read_excel_safe(path: Path) -> pd.DataFrame:
    try:
        return pd.read_excel(path, dtype=str)
    except Exception:
        return pd.DataFrame()


def _norm_side(v: object) -> str:
    s = str(v or "").strip().upper()
    if s in {"02", "2", "BUY", "B", "매수"}:
        return "BUY"
    if s in {"01", "1", "SELL", "S", "매도"}:
        return "SELL"
    return ""


def _stable_digest(*parts: object) -> str:
    def _norm(v: object) -> str:
        s = str(v or "").strip()
        return "" if s.lower() in {"", "nan", "none", "<na>"} else s
    text = "|".join(_norm(part) for part in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _synth_lineage_for_broker_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Generate deterministic lineage for rows that came from KIS_API_BRIDGE
    and have no paper/mock lineage. Mirrors ledger_cost_model fallback logic
    so live_fills.csv and the enriched ledger stay consistent."""
    if df.empty:
        return df
    out = df.copy()
    source_s = out.get("source", pd.Series([""] * len(out), index=out.index)).astype(str).str.strip().str.upper()
    order_s = out.get("order_id", pd.Series([""] * len(out), index=out.index)).astype(str).str.strip()
    fill_s = out.get("fill_id", pd.Series([""] * len(out), index=out.index)).astype(str).str.strip()

    # Only fill rows that are broker rows with blank identifiers.
    needs = source_s.eq("KIS_API_BRIDGE") & order_s.eq("") & fill_s.eq("")
    if not needs.any():
        return out

    # event_seq: per (source, date, datetime, code, side) group, 1-based.
    seq_group = (
        out.loc[needs, "source"].astype(str).str.strip()
        + "|"
        + out.loc[needs, "date"].astype(str).str.strip()
        + "|"
        + out.loc[needs, "datetime"].astype(str).str.strip()
        + "|"
        + out.loc[needs, "code"].astype(str).str.strip()
        + "|"
        + out.loc[needs, "side"].astype(str).str.strip()
    )
    event_seq = seq_group.groupby(seq_group).cumcount().add(1).astype(int)

    def _row_intent(row: pd.Series) -> str:
        return f"{row['source']}_{row['date']}_{row['side']}_{row['code']}_{int(row['_event_seq']):02d}"

    tmp = out.loc[needs].copy()
    tmp["_event_seq"] = event_seq.values
    intent_ids = tmp.apply(_row_intent, axis=1)

    out.loc[needs, "intent_id"] = intent_ids
    out.loc[needs, "order_id"] = intent_ids
    trace_ids = tmp.apply(
        lambda row: _stable_digest(
            row["source"], row["date"], row["datetime"], row["code"],
            row["side"], row["intent_id"], row["order_id"], row.get("note", ""),
        ),
        axis=1,
    )
    fill_ids = tmp.apply(
        lambda row: _stable_digest(
            row["source"], row["date"], row["datetime"], row["code"],
            row["side"], row["order_id"], row["fill_price"], row["fill_qty"], row["_event_seq"],
        ),
        axis=1,
    )
    out.loc[needs, "trace_id"] = trace_ids
    out.loc[needs, "fill_id"] = fill_ids
    return out


def _is_soft_daily_ccld_unavailable(err: KISApiError) -> bool:
    code = str(getattr(err, "code", "") or "").strip().upper()
    path = str(getattr(err, "path", "") or "").strip().lower()
    status_code = getattr(err, "status_code", None)
    msg = str(err)
    if "inquire-daily-ccld" not in path and "inquire-daily-ccld" not in msg.lower():
        return False
    if code == "EGW02004":
        return True
    if status_code == 500 and "inquire-daily-ccld" in msg.lower():
        return True
    return False


def _resolve_mock_arg(v: str) -> Tuple[Optional[bool], str]:
    mode = str(v or "auto").strip().lower()
    if mode == "true":
        return True, "true"
    if mode == "false":
        return False, "false"
    env = str(os.getenv("KIS_MOCK", "")).strip().lower()
    if env in {"1", "true", "y", "yes"}:
        return True, "auto_env_true"
    if env in {"0", "false", "n", "no"}:
        return False, "auto_env_false"
    return True, "auto_default_mock"


def _normalize_rows(rows: List[Dict[str, object]], d: str) -> Tuple[pd.DataFrame, Dict[str, object]]:
    info: Dict[str, object] = {"warnings": [], "detected_cols": {}}
    if not rows:
        df = pd.DataFrame(
            columns=[
                "date",
                "code",
                "side",
                "fill_qty",
                "fill_price",
                "order_no",
                "order_branch_no",
                "ord_tmd",
                "ccld_tmd",
                "raw_side",
                "raw_qty",
                "raw_price",
            ]
        )
        return df, info

    raw = pd.DataFrame(rows)

    side_col = _pick_col(raw.columns.tolist(), ["sll_buy_dvsn_cd", "sll_buy_dvsn", "side", "trad_dvsn_name"])
    code_col = _pick_col(raw.columns.tolist(), ["pdno", "item_no", "code", "symbol"])
    qty_col = _pick_col(raw.columns.tolist(), ["tot_ccld_qty", "ccld_qty", "exec_qty", "qty", "ord_qty"])
    price_col = _pick_col(raw.columns.tolist(), ["tot_ccld_unpr", "ccld_unpr", "avg_prvs", "exec_prc", "ord_unpr", "price"])
    odno_col = _pick_col(raw.columns.tolist(), ["odno", "ord_no", "order_no"])
    brno_col = _pick_col(raw.columns.tolist(), ["ord_gno_brno", "order_branch_no", "orgn_odno"])
    ord_tmd_col = _pick_col(raw.columns.tolist(), ["ord_tmd", "ord_tm", "order_time"])
    ccld_tmd_col = _pick_col(raw.columns.tolist(), ["ccld_tmd", "exec_time", "chegyul_time"])

    info["detected_cols"] = {
        "side_col": side_col,
        "code_col": code_col,
        "qty_col": qty_col,
        "price_col": price_col,
        "odno_col": odno_col,
        "brno_col": brno_col,
        "ord_tmd_col": ord_tmd_col,
        "ccld_tmd_col": ccld_tmd_col,
    }

    if code_col is None:
        info["warnings"].append("missing code col candidate")
    if side_col is None:
        info["warnings"].append("missing side col candidate")
    if qty_col is None:
        info["warnings"].append("missing qty col candidate")
    if price_col is None:
        info["warnings"].append("missing price col candidate")

    out = pd.DataFrame(index=raw.index)
    out["date"] = str(d)
    out["code"] = _series_or_default(raw, code_col, "").str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    out["raw_side"] = _series_or_default(raw, side_col, "")
    out["side"] = out["raw_side"].apply(_norm_side)
    out["raw_qty"] = _series_or_default(raw, qty_col, "0")
    out["fill_qty"] = out["raw_qty"].apply(_to_int)
    out["raw_price"] = _series_or_default(raw, price_col, "0")
    out["fill_price"] = out["raw_price"].apply(_to_int)
    out["order_no"] = _series_or_default(raw, odno_col, "").str.strip()
    out["order_branch_no"] = _series_or_default(raw, brno_col, "").str.strip()
    out["ord_tmd"] = _series_or_default(raw, ord_tmd_col, "").str.strip()
    out["ccld_tmd"] = _series_or_default(raw, ccld_tmd_col, "").str.strip()

    before = len(out)
    out = out[(out["code"].str.len() == 6) & (out["fill_qty"] > 0)].copy()
    out = out[out["side"].isin(["BUY", "SELL"])].copy()
    out = out.reset_index(drop=True)
    after = len(out)
    dropped = before - after
    if dropped > 0:
        info["warnings"].append(f"dropped_rows_after_normalization={dropped}")
    info["rows_before"] = int(before)
    info["rows_after"] = int(after)

    return out, info


def _fmt_intraday_datetime(d: str, raw_tmd: object) -> str:
    digits = "".join(ch for ch in str(raw_tmd or "") if ch.isdigit())
    if len(digits) >= 6:
        return f"{str(d)}T{digits[:2]}:{digits[2:4]}:{digits[4:6]}"
    return f"{str(d)}T09:00:00"


def _load_ref_close_maps(d: str) -> Tuple[Dict[str, float], Dict[str, float]]:
    path = PAPER_DIR / "prices" / "ohlcv_paper.parquet"
    if not path.exists():
        return {}, {}
    try:
        df = pd.read_parquet(path)
    except Exception:
        return {}, {}
    if df is None or df.empty:
        return {}, {}

    date_col = _pick_col(df.columns.tolist(), ["date", "ymd", "datetime", "dt"])
    code_col = _pick_col(df.columns.tolist(), ["code", "ticker", "symbol"])
    close_col = _pick_col(df.columns.tolist(), ["close", "Close", "adj_close"])
    if date_col is None or code_col is None or close_col is None:
        return {}, {}

    work = df[[date_col, code_col, close_col]].copy()
    work.columns = ["date", "code", "close"]
    work["date"] = work["date"].apply(_norm_ymd)
    work["code"] = work["code"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    work["close"] = pd.to_numeric(work["close"], errors="coerce")
    work = work[(work["date"].str.len() == 8) & (work["code"].str.len() == 6) & (work["close"] > 0)].copy()
    if work.empty:
        return {}, {}

    d = _norm_ymd(d)
    same = work[work["date"] == d].copy()
    prev = work[work["date"] < d].copy()
    prev = prev.sort_values(["code", "date"]).groupby("code", as_index=False).tail(1)

    prev_map = {str(r["code"]): float(r["close"]) for _, r in prev.iterrows()}
    same_map = {str(r["code"]): float(r["close"]) for _, r in same.iterrows()}
    return prev_map, same_map


def _detect_orders_exec_path(exec_date: str, live_path: Path) -> Optional[Path]:
    candidates: List[Path] = []
    if live_path.parent.name.lower() == "live":
        candidates.append(live_path.parent.parent / "orders" / f"orders_{exec_date}_exec.xlsx")
    candidates.append(PAPER_DIR / f"orders_{exec_date}_exec.xlsx")

    for cand in candidates:
        if cand.exists():
            return cand
    return None


def _build_submit_lineage_map(live_path: Path) -> Dict[str, Dict[str, object]]:
    # Only mock/paper submit logs carry paper lineage. Prod submit logs must NOT
    # be used to look up lineage from orders_*_exec.xlsx (paper fills), because
    # prod ACCEPTED rows share exec_date files with paper rows but source_row
    # points into the paper exec sheet, causing code/side/date mismatches.
    submit_files = sorted(
        PAPER_DIR.glob("orders_*_broker_submit_mock.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    orders_cache: Dict[str, pd.DataFrame] = {}
    out: Dict[str, Dict[str, object]] = {}

    for submit_path in submit_files:
        submit_df = _read_csv_safe(submit_path)
        if len(submit_df) == 0 or "ord_no" not in submit_df.columns:
            continue

        submit_df["dispatch_status"] = submit_df.get("dispatch_status", "").astype(str)
        submit_df["ord_no"] = submit_df.get("ord_no", "").astype(str).str.strip()
        submit_df["exec_date"] = submit_df.get("exec_date", "").apply(_norm_ymd)
        submit_df["source_row"] = submit_df.get("source_row", "0").apply(_to_int)
        submit_df["code"] = submit_df.get("code", "").astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
        submit_df["side"] = submit_df.get("side", "").astype(str).str.strip().str.upper()
        submit_df["qty"] = submit_df.get("qty", "0").apply(_to_int)
        submit_df["price"] = submit_df.get("price", "0").apply(_to_int)
        submit_df["signal_date"] = submit_df.get("signal_date", "").apply(_norm_ymd)
        submit_df["note"] = submit_df.get("note", "").astype(str)

        accepted = submit_df[
            (submit_df["dispatch_status"] == "ACCEPTED")
            & (submit_df["ord_no"].str.len() > 0)
            & (submit_df["exec_date"].str.len() == 8)
        ].copy()

        for _, rec in accepted.iterrows():
            ord_no = str(rec.get("ord_no", "")).strip()
            if not ord_no or ord_no in out:
                continue

            exec_date = str(rec.get("exec_date", "")).strip()
            source_row = _to_int(rec.get("source_row", 0))
            orders_path = _detect_orders_exec_path(exec_date, live_path)
            if not orders_path:
                continue

            cache_key = str(orders_path)
            orders_df = orders_cache.get(cache_key)
            if orders_df is None:
                orders_df = _read_excel_safe(orders_path)
                if len(orders_df):
                    orders_df = orders_df.copy()
                    if "code" in orders_df.columns:
                        orders_df["code"] = orders_df["code"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
                    if "side" in orders_df.columns:
                        orders_df["side"] = orders_df["side"].astype(str).str.strip().str.upper()
                orders_cache[cache_key] = orders_df

            if len(orders_df) == 0 or source_row < 0 or source_row >= len(orders_df):
                continue

            src = orders_df.iloc[source_row]
            out[ord_no] = {
                "datetime": _fmt_intraday_datetime(exec_date, rec.get("ord_tmd", "")),
                "qty": _to_int(src.get("fill_qty", rec.get("qty", 0))),
                "price": _to_int(src.get("fill_price", rec.get("price", 0))),
                "event_seq": "0",
                "intent_id": str(src.get("intent_id", "") or "").strip(),
                "trace_id": str(src.get("trace_id", "") or "").strip(),
                "fill_id": str(src.get("fill_id", "") or "").strip(),
                "order_id": str(src.get("order_id", "") or "").strip(),
                "intended_session": str(src.get("intended_session", "NEXT_OPEN") or "NEXT_OPEN").strip(),
                "fill_session": str(src.get("fill_session", "NEXT_OPEN") or "NEXT_OPEN").strip(),
                "time_in_force": str(src.get("time_in_force", "DAY") or "DAY").strip(),
                "replay_policy": str(src.get("replay_policy", "NEXT_SESSION_REPLAY_ONCE") or "NEXT_SESSION_REPLAY_ONCE").strip(),
                "note": str(src.get("note", rec.get("note", "")) or rec.get("note", "") or "").strip(),
                "source": str(src.get("source", "KIS_API_BRIDGE") or "KIS_API_BRIDGE").strip(),
                "replay_chain_id": str(src.get("replay_chain_id", "") or "").strip(),
            }

    return out


def _build_auto_dispatch_ord_nos(target_date: str) -> set[str]:
    """Collect ord_no from both mock and prod broker submit logs that were ACCEPTED.

    This identifies fills that originated from system dispatch rather than manual
    trades. Prod submit logs must NOT be used to look up paper lineage (intent_id,
    trace_id, fill_id, order_id), because source_row points into the paper exec
    sheet and can mismatch code/side/date. We only use prod ACCEPTED ord_no as a
    membership key to tag the fill as AUTO_DISPATCH.
    """
    submit_files = sorted(
        list(PAPER_DIR.glob("orders_*_broker_submit_mock.csv")) +
        list(PAPER_DIR.glob("orders_*_broker_submit_prod.csv")),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    out: set[str] = set()
    target_date = _norm_ymd(target_date)
    for submit_path in submit_files:
        submit_df = _read_csv_safe(submit_path)
        if len(submit_df) == 0 or "ord_no" not in submit_df.columns:
            continue
        submit_df["dispatch_status"] = submit_df.get("dispatch_status", "").astype(str).str.strip().str.upper()
        submit_df["ord_no"] = submit_df.get("ord_no", "").astype(str).str.strip()
        submit_df["exec_date"] = submit_df.get("exec_date", "").apply(_norm_ymd)
        accepted = submit_df[
            (submit_df["dispatch_status"] == "ACCEPTED")
            & (submit_df["ord_no"].str.len() > 0)
            & (submit_df["exec_date"] == target_date)
        ].copy()
        for ord_no in accepted["ord_no"].unique():
            out.add(str(ord_no).strip())
    return out


def _build_expected_dispatch_map(target_date: str) -> Dict[str, Dict[str, object]]:
    submit_files = sorted(
        PAPER_DIR.glob("orders_*_broker_submit_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    out: Dict[str, Dict[str, object]] = {}
    target_date = _norm_ymd(target_date)

    for submit_path in submit_files:
        submit_df = _read_csv_safe(submit_path)
        if len(submit_df) == 0 or "ord_no" not in submit_df.columns:
            continue

        submit_df["dispatch_status"] = submit_df.get("dispatch_status", "").astype(str).str.strip().str.upper()
        submit_df["ord_no"] = submit_df.get("ord_no", "").astype(str).str.strip()
        submit_df["exec_date"] = submit_df.get("exec_date", "").apply(_norm_ymd)
        submit_df["side"] = submit_df.get("side", "").astype(str).str.strip().str.upper()
        submit_df["qty"] = submit_df.get("qty", "0").apply(_to_int)

        accepted = submit_df[
            (submit_df["dispatch_status"] == "ACCEPTED")
            & (submit_df["ord_no"].str.len() > 0)
            & (submit_df["exec_date"] == target_date)
        ].copy()
        if len(accepted) == 0:
            continue

        for ord_no, g in accepted.groupby("ord_no", sort=False):
            if ord_no in out:
                continue
            side_vals = sorted({str(v).strip().upper() for v in g["side"].tolist() if str(v).strip()})
            side = side_vals[0] if len(side_vals) == 1 else ""
            out[str(ord_no)] = {
                "exec_date": target_date,
                "side": side,
                "order_qty": int(g["qty"].sum()),
                "dispatch_rows": int(len(g)),
                "dispatch_file": str(submit_path),
            }
    return out


def _build_fill_match_report(
    fills_df: pd.DataFrame,
    target_date: str,
    expected_map: Dict[str, Dict[str, object]],
) -> Dict[str, object]:
    target_date = _norm_ymd(target_date)
    work = fills_df.copy()
    if len(work) == 0:
        return {
            "date": target_date,
            "rules": {
                "match_key": "order_no (dispatch ACCEPTED ord_no ↔ fills order_no)",
                "partial_fill_rule": "sum(fill_qty by order_no) < order_qty and > 0",
                "overfill_rule": "sum(fill_qty by order_no) > order_qty => FAIL",
                "side_rule": "fills.side must equal dispatch side",
                "date_rule": "fills.date must equal dispatch exec_date and target D",
            },
            "counts": {
                "fills_rows": 0,
                "fills_with_order_no_rows": 0,
                "expected_orders": int(len(expected_map)),
                "matched_orders": 0,
                "partial_fill_orders": 0,
                "full_fill_orders": 0,
                "unfilled_expected_orders": int(len(expected_map)),
                "unmatched_fill_orders": 0,
                "side_mismatch_orders": 0,
                "date_mismatch_orders": 0,
                "overfill_orders": 0,
            },
            "rates": {
                "order_match_rate": 0.0 if len(expected_map) else 1.0,
                "fill_row_match_rate": 1.0,
            },
            "issues": {
                "unfilled_expected_order_nos": sorted(expected_map.keys())[:100],
                "unmatched_fill_order_nos": [],
                "side_mismatch_order_nos": [],
                "date_mismatch_order_nos": [],
                "overfill_order_nos": [],
            },
            "checks": {
                "exec_date_eq_D": True,
                "side_mismatch_zero": True,
                "overfill_zero": True,
            },
        }

    work["date"] = work.get("date", "").apply(_norm_ymd)
    work["order_no"] = work.get("order_no", "").astype(str).str.strip()
    work["side"] = work.get("side", "").astype(str).str.upper().str.strip()
    work["fill_qty"] = work.get("fill_qty", "0").apply(_to_int)
    work = work[work["fill_qty"] > 0].copy()

    with_ord = work[work["order_no"].str.len() > 0].copy()
    fills_by_ord: Dict[str, Dict[str, object]] = {}
    if len(with_ord):
        for ord_no, g in with_ord.groupby("order_no", sort=False):
            fills_by_ord[str(ord_no)] = {
                "fill_qty_sum": int(g["fill_qty"].sum()),
                "side_vals": sorted({str(v).strip().upper() for v in g["side"].tolist() if str(v).strip()}),
                "date_vals": sorted({str(v).strip() for v in g["date"].tolist() if str(v).strip()}),
                "fill_rows": int(len(g)),
            }

    expected_keys = set(expected_map.keys())
    fill_keys = set(fills_by_ord.keys())
    matched_keys = expected_keys & fill_keys
    unmatched_fill_keys = sorted(fill_keys - expected_keys)
    unfilled_expected_keys = sorted(expected_keys - fill_keys)

    partial_keys: List[str] = []
    full_keys: List[str] = []
    overfill_keys: List[str] = []
    side_mismatch_keys: List[str] = []
    date_mismatch_keys: List[str] = []
    for ord_no in sorted(matched_keys):
        exp = expected_map.get(ord_no, {})
        got = fills_by_ord.get(ord_no, {})
        exp_qty = _to_int(exp.get("order_qty", 0))
        got_qty = _to_int(got.get("fill_qty_sum", 0))
        if got_qty < exp_qty:
            partial_keys.append(ord_no)
        elif got_qty == exp_qty:
            full_keys.append(ord_no)
        else:
            overfill_keys.append(ord_no)

        exp_side = str(exp.get("side", "")).strip().upper()
        side_vals = got.get("side_vals", []) if isinstance(got.get("side_vals", []), list) else []
        if exp_side and (len(side_vals) != 1 or side_vals[0] != exp_side):
            side_mismatch_keys.append(ord_no)

        exp_date = str(exp.get("exec_date", "")).strip()
        date_vals = got.get("date_vals", []) if isinstance(got.get("date_vals", []), list) else []
        if exp_date and (len(date_vals) != 1 or date_vals[0] != exp_date):
            date_mismatch_keys.append(ord_no)

    fills_with_order_rows = int(len(with_ord))
    matched_fill_rows = int(
        with_ord[with_ord["order_no"].isin(matched_keys)].shape[0]
    ) if fills_with_order_rows else 0
    expected_n = int(len(expected_keys))
    order_match_rate = float(len(matched_keys) / expected_n) if expected_n > 0 else 1.0
    fill_row_match_rate = float(matched_fill_rows / fills_with_order_rows) if fills_with_order_rows > 0 else 1.0
    any_date_mismatch_rows = int((work["date"] != target_date).sum())

    return {
        "date": target_date,
        "rules": {
            "match_key": "order_no (dispatch ACCEPTED ord_no ↔ fills order_no)",
            "partial_fill_rule": "sum(fill_qty by order_no) < order_qty and > 0",
            "overfill_rule": "sum(fill_qty by order_no) > order_qty => FAIL",
            "side_rule": "fills.side must equal dispatch side",
            "date_rule": "fills.date must equal dispatch exec_date and target D",
        },
        "counts": {
            "fills_rows": int(len(work)),
            "fills_with_order_no_rows": fills_with_order_rows,
            "expected_orders": expected_n,
            "matched_orders": int(len(matched_keys)),
            "partial_fill_orders": int(len(partial_keys)),
            "full_fill_orders": int(len(full_keys)),
            "unfilled_expected_orders": int(len(unfilled_expected_keys)),
            "unmatched_fill_orders": int(len(unmatched_fill_keys)),
            "side_mismatch_orders": int(len(side_mismatch_keys)),
            "date_mismatch_orders": int(len(date_mismatch_keys)),
            "overfill_orders": int(len(overfill_keys)),
            "target_date_mismatch_rows": any_date_mismatch_rows,
        },
        "rates": {
            "order_match_rate": round(order_match_rate, 6),
            "fill_row_match_rate": round(fill_row_match_rate, 6),
        },
        "issues": {
            "unfilled_expected_order_nos": unfilled_expected_keys[:200],
            "unmatched_fill_order_nos": unmatched_fill_keys[:200],
            "side_mismatch_order_nos": side_mismatch_keys[:200],
            "date_mismatch_order_nos": date_mismatch_keys[:200],
            "overfill_order_nos": overfill_keys[:200],
        },
        "checks": {
            "exec_date_eq_D": any_date_mismatch_rows == 0,
            "side_mismatch_zero": len(side_mismatch_keys) == 0,
            "overfill_zero": len(overfill_keys) == 0,
        },
    }


def _load_ledger_helpers(rootb: Path):
    module_path = rootb / "tools" / "ledger_cost_model.py"
    spec = importlib.util.spec_from_file_location("kis_bridge_ledger_cost_model", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"ledger_cost_model load failed: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.build_enriched_ledger, module.deduplicate_ledger


def _concat_nonempty_frames(frames):
    usable = []
    for frame in frames:
        if frame is None:
            continue
        if not isinstance(frame, pd.DataFrame):
            continue
        if frame.empty and len(frame.columns) == 0:
            continue
        normalized = frame.dropna(axis=1, how="all").copy()
        if normalized.empty and len(normalized.columns) == 0:
            continue
        usable.append(normalized)
    if not usable:
        return pd.DataFrame()
    if len(usable) == 1:
        return usable[0].copy()
    return pd.concat(usable, ignore_index=True)


def _dedupe_live_fills_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df, 0
    out = df.copy()
    for col in ["date", "code", "side", "fill_qty", "fill_price", "order_id", "datetime", "fill_id", "source"]:
        if col not in out.columns:
            out[col] = ""
    out["_k_date"] = out["date"].astype(str).str[:8]
    out["_k_code"] = out["code"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    out["_k_side"] = out["side"].astype(str).str.upper().str.strip()
    out["_k_qty"] = pd.to_numeric(out["fill_qty"], errors="coerce").fillna(0).round(6).astype(str)
    out["_k_px"] = pd.to_numeric(out["fill_price"], errors="coerce").fillna(0).round(6).astype(str)
    out["_k_oid"] = out["order_id"].astype(str).str.strip()
    out["_k_dt"] = out["datetime"].astype(str).str.strip()
    before = len(out)
    out = out.drop_duplicates(subset=["_k_date", "_k_code", "_k_side", "_k_qty", "_k_px", "_k_oid", "_k_dt"], keep="last").copy()
    fill_id_s = out["fill_id"].astype(str).str.strip().replace({"nan": "", "None": ""})
    source_s = out["source"].astype(str).str.upper().str.strip()
    key_cols = ["_k_date", "_k_code", "_k_side", "_k_qty", "_k_px"]
    loose_key_cols = ["_k_date", "_k_code", "_k_side", "_k_qty"]
    confirmed_keys = set(
        out.loc[fill_id_s.ne("") | source_s.isin({"KIS_API_BRIDGE"}), key_cols]
        .astype(str)
        .agg("|".join, axis=1)
        .tolist()
    )
    confirmed_loose_keys = set(
        out.loc[fill_id_s.ne(""), loose_key_cols]
        .astype(str)
        .agg("|".join, axis=1)
        .tolist()
    )
    placeholder_key = out[key_cols].astype(str).agg("|".join, axis=1)
    placeholder_loose_key = out[loose_key_cols].astype(str).agg("|".join, axis=1)
    placeholder_mask = fill_id_s.eq("") & (
        (
            source_s.isin({"PAPER_ENGINE_BRIDGE", "LEGACY_PAPER"})
            & placeholder_key.isin(confirmed_keys)
        )
        | (
            source_s.isin({"EXEC_SYNC"})
            & placeholder_loose_key.isin(confirmed_loose_keys)
        )
    )
    out = out.loc[~placeholder_mask].copy()
    out = out.drop(columns=["_k_date", "_k_code", "_k_side", "_k_qty", "_k_px", "_k_oid", "_k_dt"], errors="ignore")
    removed = max(0, int(before - len(out)))
    return out, removed


def _refresh_ledger_for_today(today_bridge: pd.DataFrame, d: str, live_path: Path) -> Dict[str, object]:
    rootb = live_path.parent.parent.parent
    ledger_path = live_path.parent.parent / "ledger" / "paper_fills_ledger.csv"
    build_enriched_ledger, deduplicate_ledger = _load_ledger_helpers(rootb)

    ledger_input = today_bridge.copy()
    ledger_input["run_id"] = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    ledger_input["as_of"] = str(d)
    ledger_input["source"] = ledger_input.get("source", pd.Series(index=ledger_input.index, dtype=str)).astype(str).replace("", "BROKER")
    new_led = build_enriched_ledger(ledger_input, root=str(rootb), default_source="BROKER")

    if ledger_path.exists():
        old = pd.read_csv(ledger_path, dtype=str, encoding="utf-8-sig")
        old = build_enriched_ledger(old, root=str(rootb), default_source="BROKER")
        old = old[old["as_of"].astype(str).str.slice(0, 8) != str(d)].copy()
    else:
        old = build_enriched_ledger(pd.DataFrame(), root=str(rootb), default_source="BROKER")

    out_led = deduplicate_ledger(_concat_nonempty_frames([old, new_led]))
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    out_led.to_csv(ledger_path, index=False, encoding="utf-8-sig")

    return {
        "ledger_path": str(ledger_path),
        "rows_written_today": int(len(new_led)),
        "rows_total_ledger": int(len(out_led)),
    }


def _bridge_to_live_fills(fills_df: pd.DataFrame, d: str, live_path: Path) -> Dict[str, object]:
    live_path.parent.mkdir(parents=True, exist_ok=True)
    lineage_map = _build_submit_lineage_map(live_path)
    auto_ord_nos = _build_auto_dispatch_ord_nos(d)
    expected_map = _build_expected_dispatch_map(d)
    ref_prev_map, ref_same_map = _load_ref_close_maps(d)

    ord_tmd = fills_df.get("ord_tmd", pd.Series(dtype=str)).astype(str)
    ccld_tmd = fills_df.get("ccld_tmd", pd.Series(dtype=str)).astype(str)
    order_no = fills_df.get("order_no", pd.Series(dtype=str)).astype(str).str.strip()
    base_time = ccld_tmd.where(ccld_tmd.str.len() > 0, ord_tmd)

    default_datetime = base_time.apply(lambda v: _fmt_intraday_datetime(d, v))
    bridge = pd.DataFrame(index=fills_df.index)
    bridge["date"] = str(d)
    bridge["datetime"] = default_datetime
    bridge["code"] = fills_df.get("code", pd.Series(dtype=str)).astype(str).str.zfill(6)
    bridge["side"] = fills_df.get("side", pd.Series(dtype=str)).astype(str).str.upper()
    bridge["fill_price"] = fills_df.get("fill_price", pd.Series(dtype=int)).apply(_to_int)
    bridge["fill_qty"] = fills_df.get("fill_qty", pd.Series(dtype=int)).apply(_to_int)
    bridge["ref_close"] = ""
    bridge["qty"] = bridge["fill_qty"]
    bridge["price"] = bridge["fill_price"]
    bridge["event_seq"] = "0"
    bridge["intent_id"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("intent_id", "")))
    bridge["trace_id"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("trace_id", "")))
    bridge["fill_id"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("fill_id", "")))
    bridge["order_id"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("order_id", "")))
    bridge["intended_session"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("intended_session", "NEXT_OPEN")))
    bridge["fill_session"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("fill_session", "NEXT_OPEN")))
    bridge["time_in_force"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("time_in_force", "DAY")))
    bridge["replay_policy"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("replay_policy", "NEXT_SESSION_REPLAY_ONCE")))
    bridge["note"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("note", "")))
    bridge["source"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("source", "KIS_API_BRIDGE")))
    bridge["replay_chain_id"] = order_no.map(lambda x: str(lineage_map.get(str(x), {}).get("replay_chain_id", "")))
    bridge["lineage_origin"] = order_no.map(lambda x: "AUTO_DISPATCH" if str(x).strip() in auto_ord_nos else "MANUAL_BROKER")
    bridge["slippage_actual_bps"] = ""
    bridge["slippage_actual_cost_krw"] = ""
    bridge["slippage_ref_source"] = ""

    for col in ["datetime", "qty", "price", "event_seq", "intent_id", "trace_id", "fill_id", "order_id", "intended_session", "fill_session", "time_in_force", "replay_policy", "note", "source", "lineage_origin"]:
        bridge[col] = bridge[col].astype(str)

    # Prod fills have no paper lineage. Generate deterministic identifiers that
    # match the ledger enrichment fallback so live_fills and ledger stay in sync.
    bridge = _synth_lineage_for_broker_rows(bridge)

    bridge = bridge[(bridge["code"].str.len() == 6) & (bridge["fill_qty"] > 0)].copy()
    bridge = bridge[bridge["side"].isin(["BUY", "SELL"])].copy()

    match_report, out_match = _write_fill_match_report(fills_df=fills_df, d=d, expected_map=expected_map)
    _assert_fill_match_report(match_report, out_match)

    if not bridge.empty:
        ref_close_vals: List[object] = []
        ref_src_vals: List[str] = []
        for _, rr in bridge.iterrows():
            code = str(rr.get("code", "")).zfill(6)
            ref_close = ref_prev_map.get(code)
            ref_src = "prev_close"
            if ref_close is None:
                ref_close = ref_same_map.get(code)
                ref_src = "same_day_close"
            if ref_close is None or float(ref_close) <= 0:
                ref_close_vals.append("")
                ref_src_vals.append("")
            else:
                ref_close_vals.append(int(round(float(ref_close))))
                ref_src_vals.append(ref_src)
        bridge["ref_close"] = ref_close_vals
        bridge["slippage_ref_source"] = ref_src_vals

        fp = pd.to_numeric(bridge["fill_price"], errors="coerce")
        fq = pd.to_numeric(bridge["fill_qty"], errors="coerce")
        rc = pd.to_numeric(bridge["ref_close"], errors="coerce")
        side_sign = bridge["side"].astype(str).str.upper().map(lambda v: 1.0 if v == "BUY" else (-1.0 if v == "SELL" else 1.0))
        bps = ((fp - rc) / rc) * 10000.0 * side_sign
        cost = fp * fq * (bps / 10000.0)
        bridge["slippage_actual_bps"] = bps.where(rc > 0, "").round(4)
        bridge["slippage_actual_cost_krw"] = cost.where(rc > 0, "").round(2)

    if live_path.exists():
        try:
            old = pd.read_csv(live_path, dtype=str)
        except Exception:
            old = pd.DataFrame(columns=["date", "code", "side", "fill_price", "fill_qty", "ref_close", "slippage_actual_bps", "slippage_actual_cost_krw", "slippage_ref_source", "lineage_origin"])
    else:
        old = pd.DataFrame(columns=["date", "code", "side", "fill_price", "fill_qty", "ref_close", "slippage_actual_bps", "slippage_actual_cost_krw", "slippage_ref_source", "lineage_origin"])

    if "lineage_origin" not in old.columns:
        old["lineage_origin"] = ""

    old_today_keep = pd.DataFrame()
    if len(old):
        old["date"] = old["date"].astype(str).str[:8]
        source_s = old.get("source", pd.Series([""] * len(old), index=old.index)).astype(str).str.upper().str.strip()
        old_today_keep = old[(old["date"] == str(d)) & (~source_s.isin({"KIS_API_BRIDGE", "BROKER"}))].copy()
        old = old[old["date"] != str(d)].copy()

    out = _concat_nonempty_frames([old, old_today_keep, bridge])
    out, dedup_removed = _dedupe_live_fills_rows(out)
    out.to_csv(live_path, index=False, encoding="utf-8-sig")

    today_path = live_path.with_name("live_fills_today.csv")
    out[out["date"].astype(str) == str(d)].copy().to_csv(today_path, index=False, encoding="utf-8-sig")
    ledger_today = out[out["date"].astype(str) == str(d)].copy()
    ledger_info = _refresh_ledger_for_today(ledger_today, d, live_path)

    return {
        "live_path": str(live_path),
        "today_path": str(today_path),
        "rows_written_today": int(len(bridge)),
        "rows_total_live": int(len(out)),
        "dedup_removed": int(dedup_removed),
        "lineage_map_size": int(len(lineage_map)),
        "expected_map_size": int(len(expected_map)),
        "intent_rows_today": int((bridge["intent_id"].astype(str).str.len() > 0).sum()),
        "trace_rows_today": int((bridge["trace_id"].astype(str).str.len() > 0).sum()),
        "match_report_path": str(out_match),
        "match_summary": {
            "order_match_rate": match_report["rates"]["order_match_rate"],
            "partial_fill_orders": match_report["counts"]["partial_fill_orders"],
            "side_mismatch_orders": match_report["counts"]["side_mismatch_orders"],
            "overfill_orders": match_report["counts"]["overfill_orders"],
        },
        "ledger": ledger_info,
    }


def _write_fill_match_report(
    fills_df: pd.DataFrame,
    d: str,
    expected_map: Dict[str, Dict[str, object]] | None = None,
) -> Tuple[Dict[str, object], Path]:
    expected = expected_map if expected_map is not None else _build_expected_dispatch_map(d)
    match_report = _build_fill_match_report(fills_df=fills_df, target_date=d, expected_map=expected)
    match_report["generated_at"] = dt.datetime.now().isoformat(timespec="seconds")
    out_match = LOG_DIR / f"fills_match_report_{d}.json"
    out_match.write_text(json.dumps(match_report, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(
        "[FILL_MATCH] date=%s expected=%s matched=%s partial=%s side_mismatch=%s overfill=%s order_match_rate=%.4f",
        d,
        match_report["counts"]["expected_orders"],
        match_report["counts"]["matched_orders"],
        match_report["counts"]["partial_fill_orders"],
        match_report["counts"]["side_mismatch_orders"],
        match_report["counts"]["overfill_orders"],
        float(match_report["rates"]["order_match_rate"]),
    )
    return match_report, out_match


def _assert_fill_match_report(match_report: Dict[str, object], out_match: Path) -> None:
    checks = match_report.get("checks") if isinstance(match_report.get("checks"), dict) else {}
    if (not bool(checks.get("exec_date_eq_D"))) or (not bool(checks.get("side_mismatch_zero"))) or (
        not bool(checks.get("overfill_zero"))
    ):
        raise RuntimeError(
            f"fill_match_assert_failed exec_date_eq_D={checks.get('exec_date_eq_D')} "
            f"side_mismatch_zero={checks.get('side_mismatch_zero')} "
            f"overfill_zero={checks.get('overfill_zero')} report={out_match}"
        )


def _preserve_roota_order_ids_after_bridge(d: str) -> Dict[str, object]:
    spec = importlib.util.spec_from_file_location(
        "repair_rootb_order_id_from_roota_fills",
        THIS_DIR / "repair_rootb_order_id_from_roota_fills.py",
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("order_id_preserve_loader_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    repair_fn = getattr(module, "repair", None)
    if repair_fn is None:
        raise RuntimeError("order_id_preserve_repair_unavailable")
    payload = repair_fn(str(d), True)
    return payload if isinstance(payload, dict) else {"status": "UNKNOWN", "payload": payload}


def _write_canonical_shadow(fills_csv: Path, d: str) -> Dict[str, object]:
    spec = importlib.util.spec_from_file_location(
        "canonical_fills_shadow",
        THIS_DIR / "canonical_fills_shadow.py",
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("canonical_shadow_loader_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    run_shadow = getattr(module, "run_shadow", None)
    if run_shadow is None:
        raise RuntimeError("canonical_shadow_run_unavailable")
    rc, payload = run_shadow(fills_path=fills_csv, date=str(d))
    if rc != 0:
        raise RuntimeError(f"canonical_shadow_failed status={payload.get('status')} reason={payload.get('reason')}")
    return {
        "status": payload.get("status"),
        "mode": payload.get("mode"),
        "fills_log": payload.get("fills_log"),
        "state_hash_log": ((payload.get("replay") or {}).get("state_hash_log") if isinstance(payload.get("replay"), dict) else ""),
        "events_input": payload.get("events_input"),
        "events_appended": payload.get("events_appended"),
        "duplicates_ignored": payload.get("duplicates_ignored"),
        "conflicts": payload.get("conflicts"),
        "events_replayed": ((payload.get("replay") or {}).get("events_replayed") if isinstance(payload.get("replay"), dict) else None),
        "final_state_hash": ((payload.get("replay") or {}).get("final_state_hash") if isinstance(payload.get("replay"), dict) else ""),
        "chain_hash": ((payload.get("replay") or {}).get("chain_hash") if isinstance(payload.get("replay"), dict) else ""),
    }


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

    _audit_ymd = dt.datetime.now().strftime("%Y%m%d")
    log_pipeline_event(
        stage="broker_sync",
        batch_label="[13.5/16]",
        event="START",
        date=_audit_ymd,
    )

    ap = argparse.ArgumentParser(description="Fetch KIS daily filled history and save normalized fills")
    ap.add_argument("--date", default="", help="YYYYMMDD (default=today)")
    ap.add_argument("--start-date", default="", help="YYYYMMDD, default=date")
    ap.add_argument("--end-date", default="", help="YYYYMMDD, default=date")
    ap.add_argument("--pd-dv", default="inner", choices=["before", "inner"])
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--bridge-write", action="store_true", help="Also write normalized fills to live_fills bridge")
    ap.add_argument("--bridge-live-path", default=str(Path(__file__).resolve().parents[1].parent / "vibe" / "buffett" / "data" / "live" / "live_fills.csv"))
    args = ap.parse_args()

    today = dt.datetime.now().strftime("%Y%m%d")
    d = _norm_ymd(args.date) or today
    sdate = _norm_ymd(args.start_date) or d
    edate = _norm_ymd(args.end_date) or d

    mock_opt, mock_resolved = _resolve_mock_arg(args.mock)

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    out_raw_json = PAPER_DIR / f"kis_daily_ccld_raw_{d}.json"
    out_raw_csv = PAPER_DIR / f"kis_daily_ccld_raw_{d}.csv"
    out_fill_csv = PAPER_DIR / f"kis_fills_api_{d}.csv"
    out_summary_json = LOG_DIR / f"kis_fills_sync_{d}.json"

    api_soft_fail = False
    api_soft_fail_reason = ""
    rsp: Dict[str, object] = {}
    rows: List[Dict[str, object]] = []

    try:
        client = KISOrderClient.from_env(mock=mock_opt)
        rsp = client.inquire_daily_ccld(
            start_ymd=sdate,
            end_ymd=edate,
            pd_dv=str(args.pd_dv),
            ccld_dvsn="01",
            inqr_dvsn="00",
            inqr_dvsn_3="00",
            sll_buy_dvsn_cd="00",
            excg_id_dvsn_cd="KRX",
        )
        rows = rsp.get("rows", []) or []
    except KISApiError as e:
        if _is_soft_daily_ccld_unavailable(e):
            api_soft_fail = True
            api_soft_fail_reason = str(e)
            logger.warning("[SOFT] daily ccld unavailable, continue with empty rows: %s", e)
            rsp = {
                "tr_id": "",
                "pages": 0,
                "rows": [],
            }
            rows = []
        else:
            logger.error("[STOP] KIS API error: %s", e)
            return 2
    except Exception as e:
        logger.error("[STOP] sync failed: %s", e)
        return 2

    if api_soft_fail:
        summary = {
            "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
            "date": d,
            "start_date": sdate,
            "end_date": edate,
            "pd_dv": args.pd_dv,
            "mock": args.mock,
            "mock_resolved": mock_resolved,
            "tr_id": rsp.get("tr_id"),
            "pages": int(rsp.get("pages", 0)),
            "rows_raw": 0,
            "rows_normalized": 0,
            "api_soft_fail": True,
            "api_soft_fail_reason": str(api_soft_fail_reason),
            "status": "SOFT_FAIL_NO_OVERWRITE",
            "preserved_existing_outputs": True,
            "bridge_write": bool(args.bridge_write),
            "bridge": None,
            "paths": {
                "raw_json": str(out_raw_json),
                "raw_csv": str(out_raw_csv),
                "fills_csv": str(out_fill_csv),
            },
        }
        out_summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.error("[STOP] daily ccld soft fail; preserved existing outputs summary=%s", out_summary_json)
        return 2

    raw_df = pd.DataFrame(rows)
    raw_df.to_csv(out_raw_csv, index=False, encoding="utf-8-sig")
    out_raw_json.write_text(json.dumps({"rows": rows}, ensure_ascii=False, indent=2), encoding="utf-8")

    fills_df, norm_info = _normalize_rows(rows, d)
    fills_df.to_csv(out_fill_csv, index=False, encoding="utf-8-sig")
    match_report, out_match = _write_fill_match_report(fills_df=fills_df, d=d)
    try:
        canonical_shadow = _write_canonical_shadow(out_fill_csv, d)
    except Exception as e:
        logger.error("[STOP] canonical shadow failed: %s", e)
        return 2

    bridge_info = None
    if args.bridge_write:
        try:
            bridge_info = _bridge_to_live_fills(fills_df, d, Path(args.bridge_live_path))
            bridge_info["order_id_preserve"] = _preserve_roota_order_ids_after_bridge(d)
        except Exception as e:
            logger.error("[STOP] bridge write failed: %s", e)
            return 2

    summary = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "date": d,
        "start_date": sdate,
        "end_date": edate,
        "pd_dv": args.pd_dv,
        "mock": args.mock,
        "mock_resolved": mock_resolved,
        "tr_id": rsp.get("tr_id"),
        "pages": int(rsp.get("pages", 0)),
        "rows_raw": int(len(raw_df)),
        "rows_normalized": int(len(fills_df)),
        "api_soft_fail": bool(api_soft_fail),
        "api_soft_fail_reason": str(api_soft_fail_reason),
        "normalize_info": norm_info,
        "canonical_shadow": canonical_shadow,
        "bridge_write": bool(args.bridge_write),
        "bridge": bridge_info,
        "paths": {
            "raw_json": str(out_raw_json),
            "raw_csv": str(out_raw_csv),
            "fills_csv": str(out_fill_csv),
            "match_report": str(out_match),
        },
        "match_summary": {
            "order_match_rate": match_report["rates"]["order_match_rate"],
            "partial_fill_orders": match_report["counts"]["partial_fill_orders"],
            "side_mismatch_orders": match_report["counts"]["side_mismatch_orders"],
            "overfill_orders": match_report["counts"]["overfill_orders"],
        },
    }
    out_summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("[OK] raw_csv=%s rows=%s", out_raw_csv, len(raw_df))
    logger.info("[OK] fills_csv=%s rows=%s", out_fill_csv, len(fills_df))
    if norm_info.get("warnings"):
        logger.warning("normalize warnings: %s", norm_info.get("warnings"))
    if bridge_info:
        logger.info("[OK] bridge_live=%s rows_written_today=%s", bridge_info["live_path"], bridge_info["rows_written_today"])
    logger.info("[OK] summary=%s", out_summary_json)
    log_pipeline_event(
        stage="broker_sync",
        batch_label="[13.5/16]",
        event="END",
        date=_audit_ymd,
        output_files={
            "kis_fills_sync_json": {"path": str(out_summary_json), "rows_normalized": int(summary.get("rows_normalized", 0))},
            "live_fills_csv": {
                "path": bridge_info["live_path"] if bridge_info else str(Path(args.bridge_live_path)),
                "rows_added": int(bridge_info["rows_written_today"]) if bridge_info else 0,
                "rows_total": count_rows(bridge_info["live_path"]) if bridge_info else None,
            },
        },
        metrics={
            "rows_normalized": int(summary.get("rows_normalized", 0)),
            "bridge_write": bool(args.bridge_write),
            "rows_written_today": int(bridge_info["rows_written_today"]) if bridge_info else 0,
        },
        status="PASS",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

