from __future__ import annotations

import bisect
import json
import math
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = Path(os.getenv("BT_REAL_LEDGER_CSV", r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv"))
DEFAULT_PRICES = Path(os.getenv("BT_REAL_PRICE_PARQUET", str(ROOT / "paper" / "prices" / "ohlcv_paper.parquet")))
DEFAULT_CFG = Path(os.getenv("BT_REAL_ENGINE_CONFIG", str(ROOT / "paper" / "paper_engine_config.json")))
DEFAULT_OPER_START_YMD = "20260301"
ORDERS_EXEC_DIR = ROOT / "paper"


def _norm_code(v: Any) -> str:
    s = str(v or "").strip().replace(".0", "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def _norm_ymd(v: Any) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s[:8] if len(s) >= 8 else ""


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _extract_note_value(note: Any, key: str) -> str:
    m = re.search(rf"(?:^|[;|]){re.escape(key)}=([^;|]+)", str(note or ""))
    return m.group(1).strip() if m else ""


def _oper_start_ymd() -> str:
    ymd = _norm_ymd(os.getenv("PAPER_OPER_START_YMD", DEFAULT_OPER_START_YMD))
    return ymd if len(ymd) == 8 else DEFAULT_OPER_START_YMD


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _annualized_sharpe(returns: pd.Series) -> float:
    r = pd.to_numeric(returns, errors="coerce").dropna()
    if len(r) < 2:
        return float("nan")
    std = float(r.std(ddof=1))
    if std <= 0:
        return float("nan")
    return float((r.mean() / std) * math.sqrt(252.0))


def _max_drawdown(equity: pd.Series) -> float:
    eq = pd.to_numeric(equity, errors="coerce").dropna()
    if len(eq) == 0:
        return float("nan")
    peak = eq.cummax()
    dd = eq / peak - 1.0
    return float(dd.min())


@lru_cache(maxsize=1)
def _load_capital_total() -> float:
    cfg = _read_json(DEFAULT_CFG)
    try:
        cap = float(cfg.get("capital_total", 100000000) or 100000000)
    except Exception:
        cap = 100000000.0
    return max(cap, 1.0)


@lru_cache(maxsize=1)
def _normal_intraday_validation_policy() -> Dict[str, Any]:
    cfg = _read_json(DEFAULT_CFG)
    policy = cfg.get("normal_intraday_realtime_policy", {}) if isinstance(cfg, dict) else {}
    return policy if isinstance(policy, dict) else {}


def _exclude_reduced_entry_fills_in_validation() -> bool:
    policy = _normal_intraday_validation_policy()
    return bool(policy.get("exclude_reduced_entry_fills_in_validation", False))


def _is_reduced_validation_note(note: Any) -> bool:
    text = str(note or "").lower()
    return any(
        token in text
        for token in (
            "validation_reduce",
            "overheat_reduce",
            "sector_hrp_reduce",
            "sector_corr_reduce",
        )
    )


@lru_cache(maxsize=16)
def _load_blocked_entry_order_ids_for_dates(date_tuple: tuple[str, ...]) -> tuple[frozenset[str], Dict[str, Any]]:
    blocked: set[str] = set()
    scanned = 0
    missing = 0
    blocked_rows = 0
    for ymd in date_tuple:
        if not re.fullmatch(r"\d{8}", str(ymd or "")):
            continue
        path = ORDERS_EXEC_DIR / f"orders_{ymd}_exec.xlsx"
        if not path.exists():
            missing += 1
            continue
        try:
            df = pd.read_excel(path, dtype=str).fillna("")
        except Exception:
            missing += 1
            continue
        scanned += 1
        required = {"side", "code", "is_stop", "entry_blocked"}
        if not required.issubset(set(df.columns)):
            continue
        side = df["side"].astype(str).str.upper().str.strip()
        stop = df["is_stop"].apply(_truthy)
        entry_blocked = df["entry_blocked"].apply(_truthy)
        posthoc = df.get("posthoc_policy_violation", pd.Series(False, index=df.index)).apply(_truthy)
        execution_blocked = df.get("execution_blocked", pd.Series(False, index=df.index)).apply(_truthy)
        qty = pd.to_numeric(df.get("fill_qty", pd.Series(0, index=df.index)), errors="coerce").fillna(0.0)
        mask = side.eq("BUY") & (~stop) & (entry_blocked | posthoc | execution_blocked) & (qty > 0)
        for _, row in df.loc[mask].iterrows():
            order_id = str(row.get("order_id", "") or "").strip()
            code = _norm_code(row.get("code"))
            signal_date = _norm_ymd(row.get("signal_date"))
            if not order_id and code and signal_date:
                order_id = f"PAPER_BUY_{code}_{signal_date}"
            if order_id:
                blocked.add(order_id)
                blocked_rows += 1
    meta = {
        "orders_exec_scanned": scanned,
        "orders_exec_missing_or_unreadable": missing,
        "blocked_entry_order_ids": len(blocked),
        "blocked_orders_rows": blocked_rows,
    }
    return frozenset(blocked), meta


def _filter_blocked_entry_fills(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    if df.empty or os.getenv("BT_INCLUDE_BLOCKED_ENTRY_FILLS", "").strip() in {"1", "true", "TRUE", "yes"}:
        return df, {"enabled": False, "reason": "empty_or_env_include"}
    dates = tuple(sorted({str(x) for x in df.get("date", pd.Series(dtype=str)).dropna().astype(str).tolist() if re.fullmatch(r"\d{8}", str(x))}))
    blocked_ids, meta = _load_blocked_entry_order_ids_for_dates(dates)
    order_id_s = df.get("order_id", pd.Series("", index=df.index)).astype(str)
    entry_order_id_s = df.get("entry_order_id", pd.Series("", index=df.index)).astype(str)
    source_order_id_s = df.get("source_order_id", pd.Series("", index=df.index)).astype(str)
    note_s = df.get("note", pd.Series("", index=df.index)).astype(str)
    side_s = df.get("side", pd.Series("", index=df.index)).astype(str).str.upper().str.strip()

    reduced_ids: set[str] = set()
    if _exclude_reduced_entry_fills_in_validation():
        reduced_buy = side_s.eq("BUY") & note_s.apply(_is_reduced_validation_note)
        for value in pd.concat(
            [
                order_id_s.loc[reduced_buy],
                entry_order_id_s.loc[reduced_buy],
                source_order_id_s.loc[reduced_buy],
            ]
        ).astype(str):
            value = value.strip()
            if value:
                reduced_ids.add(value)

    if not blocked_ids and not reduced_ids:
        meta.update(
            {
                "enabled": True,
                "removed_rows": 0,
                "removed_buy_rows": 0,
                "removed_sell_rows": 0,
                "reduced_entry_policy_enabled": _exclude_reduced_entry_fills_in_validation(),
                "reduced_entry_order_ids": 0,
                "reduced_entry_removed_rows": 0,
                "reduced_entry_removed_buy_rows": 0,
                "reduced_entry_removed_sell_rows": 0,
            }
        )
        return df, meta

    direct = side_s.eq("BUY") & order_id_s.isin(blocked_ids)
    linked_entry = entry_order_id_s.isin(blocked_ids) | note_s.apply(lambda s: _extract_note_value(s, "entry_order_id")).isin(blocked_ids)
    linked_source = source_order_id_s.isin(blocked_ids) | note_s.apply(lambda s: _extract_note_value(s, "source_order_id")).isin(blocked_ids)
    blocked_remove = direct | linked_entry | linked_source

    reduced_direct = side_s.eq("BUY") & (
        order_id_s.isin(reduced_ids) | entry_order_id_s.isin(reduced_ids) | source_order_id_s.isin(reduced_ids)
    )
    reduced_linked_entry = entry_order_id_s.isin(reduced_ids) | note_s.apply(lambda s: _extract_note_value(s, "entry_order_id")).isin(reduced_ids)
    reduced_linked_source = source_order_id_s.isin(reduced_ids) | note_s.apply(lambda s: _extract_note_value(s, "source_order_id")).isin(reduced_ids)
    reduced_remove = reduced_direct | reduced_linked_entry | reduced_linked_source

    remove = blocked_remove | reduced_remove
    out = df.loc[~remove].copy()
    meta.update(
        {
            "enabled": True,
            "removed_rows": int(remove.sum()),
            "removed_buy_rows": int((remove & side_s.eq("BUY")).sum()),
            "removed_sell_rows": int((remove & side_s.eq("SELL")).sum()),
            "reduced_entry_policy_enabled": _exclude_reduced_entry_fills_in_validation(),
            "reduced_entry_order_ids": len(reduced_ids),
            "reduced_entry_removed_rows": int((reduced_remove & ~blocked_remove).sum()),
            "reduced_entry_removed_buy_rows": int((reduced_remove & ~blocked_remove & side_s.eq("BUY")).sum()),
            "reduced_entry_removed_sell_rows": int((reduced_remove & ~blocked_remove & side_s.eq("SELL")).sum()),
        }
    )
    out.attrs["blocked_entry_filter"] = meta
    return out, meta


def _surge_stop_gap_validation_scenario() -> str:
    return str(os.getenv("BT_SURGE_STOP_GAP_SCENARIO", "") or "").strip().lower()


def _reentry_cooldown_validation_scenario() -> str:
    return str(os.getenv("BT_REENTRY_COOLDOWN_SCENARIO", "") or "").strip().lower()


def _legacy_model_day_validation_scenario() -> str:
    return str(os.getenv("BT_LEGACY_MODEL_DAY_SCENARIO", "") or "").strip().lower()


def _entry_quality_audit_validation_scenario() -> str:
    return str(os.getenv("BT_ENTRY_QUALITY_AUDIT_SCENARIO", "") or "").strip().lower()


def _remove_entry_order_ids(df: pd.DataFrame, target_orders: set[str]) -> tuple[pd.DataFrame, Dict[str, Any]]:
    cols = [c for c in ("entry_order_id", "source_order_id", "order_id") if c in df.columns]
    if not cols:
        return df, {"removed_rows": 0, "removed_buy_rows": 0, "removed_sell_rows": 0, "reason": "order_columns_missing"}
    mask = pd.Series(False, index=df.index)
    for col in cols:
        mask = mask | df[col].astype(str).isin(target_orders)
    if "note" in df.columns:
        note_s = df["note"].astype(str)
        for order_id in target_orders:
            mask = mask | note_s.str.contains(order_id, regex=False, na=False)
    side = df.get("side", pd.Series("", index=df.index)).astype(str).str.upper().str.strip()
    out = df.loc[~mask].copy()
    return out, {
        "removed_rows": int(mask.sum()),
        "removed_buy_rows": int((mask & side.eq("BUY")).sum()),
        "removed_sell_rows": int((mask & side.eq("SELL")).sum()),
    }


def _apply_entry_quality_audit_validation_scenario(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    scenario = _entry_quality_audit_validation_scenario()
    if not scenario:
        return df, {"enabled": False, "reason": "env_not_set"}
    if scenario != "block_flagged_orders":
        return df, {"enabled": False, "reason": f"unsupported_scenario:{scenario}"}

    audit_path = Path(
        os.getenv("BT_ENTRY_QUALITY_AUDIT_PATH", str(ROOT / "2_Logs" / "entry_quality_cross_section_audit_latest.json"))
    )
    audit = _read_json(audit_path)
    rows = audit.get("rows") if isinstance(audit.get("rows"), list) else []
    target_orders = {str(r.get("entry_order_id", "")).strip() for r in rows if isinstance(r, dict)}
    target_orders = {x for x in target_orders if x}
    out, meta = _remove_entry_order_ids(df, target_orders)
    meta.update(
        {
            "enabled": True,
            "scenario": scenario,
            "audit_path": str(audit_path),
            "audit_status": audit.get("status", ""),
            "target_entry_order_ids": len(target_orders),
        }
    )
    return out, meta


def _apply_legacy_model_day_validation_scenario(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    scenario = _legacy_model_day_validation_scenario()
    if not scenario:
        return df, {"enabled": False, "reason": "env_not_set"}
    if scenario != "block_20260304_next_open":
        return df, {"enabled": False, "reason": f"unsupported_scenario:{scenario}"}
    target_orders = {
        "PAPER_BUY_010820_20260303",
        "PAPER_BUY_010950_20260303",
        "PAPER_BUY_011200_20260303",
        "PAPER_BUY_028670_20260303",
        "PAPER_BUY_096770_20260303",
        "PAPER_BUY_128820_20260303",
        "PAPER_BUY_218410_20260303",
    }
    out, meta = _remove_entry_order_ids(df, target_orders)
    meta.update({"enabled": True, "scenario": scenario, "target_entry_order_ids": sorted(target_orders)})
    return out, meta


def _apply_reentry_cooldown_validation_scenario(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    scenario = _reentry_cooldown_validation_scenario()
    if not scenario:
        return df, {"enabled": False, "reason": "env_not_set"}
    if scenario != "block_094170_20260604":
        return df, {"enabled": False, "reason": f"unsupported_scenario:{scenario}"}

    target_order = "PAPER_BUY_094170_20260604"
    out, meta = _remove_entry_order_ids(df, {target_order})
    meta.update({"enabled": True, "scenario": scenario, "target_entry_order_id": target_order})
    return out, meta


def _apply_surge_stop_gap_validation_scenario(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    scenario = _surge_stop_gap_validation_scenario()
    if not scenario:
        return df, {"enabled": False, "reason": "env_not_set"}

    if scenario not in {"block_20260428", "cap_20260428_0_15"}:
        return df, {"enabled": False, "reason": f"unsupported_scenario:{scenario}"}

    target_traces = {"538d43c8d215c772", "c341cb0e33e0837f"}
    trace_cols = [c for c in ("entry_trace_id", "source_trace_id", "trace_id") if c in df.columns]
    if not trace_cols:
        return df, {"enabled": True, "scenario": scenario, "removed_rows": 0, "scaled_rows": 0, "reason": "trace_columns_missing"}

    mask = pd.Series(False, index=df.index)
    for col in trace_cols:
        mask = mask | df[col].astype(str).isin(target_traces)
    if not bool(mask.any()):
        return df, {"enabled": True, "scenario": scenario, "removed_rows": 0, "scaled_rows": 0, "reason": "target_traces_not_found"}

    if scenario == "block_20260428":
        out = df.loc[~mask].copy()
        meta = {
            "enabled": True,
            "scenario": scenario,
            "target_trace_ids": sorted(target_traces),
            "removed_rows": int(mask.sum()),
            "removed_buy_rows": int((mask & df["side"].astype(str).str.upper().eq("BUY")).sum()),
            "removed_sell_rows": int((mask & df["side"].astype(str).str.upper().eq("SELL")).sum()),
            "scaled_rows": 0,
            "scale_factor": 0.0,
        }
        return out, meta

    scale_factor = 0.15 / 0.35
    out = df.copy()
    scale_cols = [
        "fill_qty",
        "gross_notional_krw",
        "net_cash_flow_krw",
        "fee_krw",
        "tax_krw",
        "explicit_cost_krw",
        "slippage_ref_krw",
        "slippage_actual_cost_krw",
        "position_qty_before",
        "position_qty_after",
        "avg_cost_before_krw",
        "avg_cost_after_krw",
        "position_cost_before_krw",
        "position_cost_after_krw",
        "matched_qty",
        "unmatched_qty",
        "realized_basis_krw",
        "realized_pnl_krw",
    ]
    for col in scale_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).astype(float)
            out.loc[mask, col] = out.loc[mask, col] * float(scale_factor)
    meta = {
        "enabled": True,
        "scenario": scenario,
        "target_trace_ids": sorted(target_traces),
        "removed_rows": 0,
        "removed_buy_rows": 0,
        "removed_sell_rows": 0,
        "scaled_rows": int(mask.sum()),
        "scaled_buy_rows": int((mask & df["side"].astype(str).str.upper().eq("BUY")).sum()),
        "scaled_sell_rows": int((mask & df["side"].astype(str).str.upper().eq("SELL")).sum()),
        "scale_factor": float(scale_factor),
    }
    return out, meta


@lru_cache(maxsize=1)
def _load_ledger() -> pd.DataFrame:
    if not DEFAULT_LEDGER.exists():
        raise FileNotFoundError(f"missing ledger csv: {DEFAULT_LEDGER}")
    df = pd.read_csv(DEFAULT_LEDGER, dtype=str, encoding="utf-8-sig")
    if df.empty:
        return df
    if "date" not in df.columns:
        raise ValueError("ledger csv missing date column")
    df["date"] = df["date"].map(_norm_ymd)
    df["datetime"] = df.get("datetime", "").astype(str)
    df["code"] = df.get("code", "").map(_norm_code)
    df["side"] = df.get("side", "").astype(str).str.upper().str.strip()
    for col in ("fill_qty", "gross_notional_krw", "net_cash_flow_krw"):
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0.0)
    if "input_seq" in df.columns:
        df["input_seq"] = pd.to_numeric(df["input_seq"], errors="coerce").fillna(0).astype(int)
        df = df.sort_values(["date", "datetime", "input_seq"], na_position="last")
    else:
        df = df.sort_values(["date", "datetime"], na_position="last")
    df, surge_stop_gap_meta = _apply_surge_stop_gap_validation_scenario(df.reset_index(drop=True))
    df, reentry_cooldown_meta = _apply_reentry_cooldown_validation_scenario(df.reset_index(drop=True))
    df, legacy_model_day_meta = _apply_legacy_model_day_validation_scenario(df.reset_index(drop=True))
    df, entry_quality_audit_meta = _apply_entry_quality_audit_validation_scenario(df.reset_index(drop=True))
    df, blocked_meta = _filter_blocked_entry_fills(df.reset_index(drop=True))
    out = df.reset_index(drop=True)
    out.attrs["blocked_entry_filter"] = blocked_meta
    out.attrs["surge_stop_gap_validation_scenario"] = surge_stop_gap_meta
    out.attrs["reentry_cooldown_validation_scenario"] = reentry_cooldown_meta
    out.attrs["legacy_model_day_validation_scenario"] = legacy_model_day_meta
    out.attrs["entry_quality_audit_validation_scenario"] = entry_quality_audit_meta
    return out


@lru_cache(maxsize=1)
def _load_close_maps() -> tuple[Dict[tuple[str, str], float], Dict[str, list[tuple[str, float]]]]:
    if not DEFAULT_PRICES.exists():
        raise FileNotFoundError(f"missing price parquet: {DEFAULT_PRICES}")
    px = pd.read_parquet(DEFAULT_PRICES)
    if px.empty:
        return {}, {}
    cols = {c.lower(): c for c in px.columns}
    date_col = next((cols[k] for k in ("date", "ymd", "datetime", "dt") if k in cols), None)
    code_col = next((cols[k] for k in ("code", "ticker", "symbol") if k in cols), None)
    close_col = next((cols[k] for k in ("close", "adj_close") if k in cols), None)
    if not date_col or not code_col or not close_col:
        raise ValueError("ohlcv_paper parquet missing date/code/close columns")
    work = px[[date_col, code_col, close_col]].copy()
    work.columns = ["date", "code", "close"]
    work["date"] = work["date"].map(_norm_ymd)
    work["code"] = work["code"].map(_norm_code)
    work["close"] = pd.to_numeric(work["close"], errors="coerce")
    work = work[(work["date"].str.len() == 8) & (work["code"].str.len() == 6) & (work["close"] > 0)].copy()
    same_day = {(str(r.code), str(r.date)): float(r.close) for r in work.itertuples(index=False)}
    prev_map: Dict[str, list[tuple[str, float]]] = {}
    for code, g in work.sort_values(["code", "date"]).groupby("code"):
        prev_map[str(code)] = [(str(d), float(c)) for d, c in zip(g["date"].tolist(), g["close"].tolist())]
    return same_day, prev_map


def _lookup_close(
    code: str,
    day8: str,
    same_day: Dict[tuple[str, str], float],
    prev_map: Dict[str, list[tuple[str, float]]],
) -> float:
    same = same_day.get((code, day8))
    if same is not None:
        return float(same)
    arr = prev_map.get(code) or []
    if not arr:
        return 0.0
    dates = [d for d, _ in arr]
    idx = bisect.bisect_right(dates, day8) - 1
    if idx < 0:
        return 0.0
    return float(arr[idx][1])


@lru_cache(maxsize=4)
def _build_daily_frame_cached(day_tuple: tuple[str, ...], oper_start: str) -> pd.DataFrame:
    ledger = _load_ledger()
    same_day, prev_map = _load_close_maps()
    capital_total = _load_capital_total()
    day_list = list(day_tuple)
    if oper_start:
        ledger = ledger[ledger["date"].astype(str) >= oper_start].copy()
    rows_by_day: Dict[str, list[dict[str, Any]]] = {
        str(day): grp.to_dict(orient="records") for day, grp in ledger.groupby("date", sort=False)
    }

    holdings: Dict[str, float] = {}
    cash = float(capital_total)
    rows: list[dict[str, Any]] = []

    for idx, day8 in enumerate(day_list):
        trade_rows = rows_by_day.get(day8, [])
        traded_notional = 0.0
        for row in trade_rows:
            code = _norm_code(row.get("code"))
            qty = float(row.get("fill_qty") or 0.0)
            if not code or qty <= 0:
                continue
            side = str(row.get("side") or "").upper().strip()
            net_cf = float(row.get("net_cash_flow_krw") or 0.0)
            gross_notional = abs(float(row.get("gross_notional_krw") or 0.0))
            traded_notional += gross_notional
            cash += net_cf
            if side == "BUY":
                holdings[code] = float(holdings.get(code, 0.0) + qty)
            elif side == "SELL":
                holdings[code] = float(holdings.get(code, 0.0) - qty)
                if holdings[code] <= 0:
                    holdings.pop(code, None)

        market_value = 0.0
        for code, qty in holdings.items():
            if qty <= 0:
                continue
            close_px = _lookup_close(code, day8, same_day, prev_map)
            if close_px > 0:
                market_value += float(qty * close_px)

        equity_krw = float(cash + market_value)
        position_ratio = float(market_value / capital_total) if capital_total > 0 else 0.0
        turnover_ratio = float(traded_notional / capital_total) if capital_total > 0 else 0.0
        rows.append(
            {
                "date": day8,
                "equity_krw": equity_krw,
                "position": position_ratio,
                "turnover": turnover_ratio,
                "trade_rows": int(len(trade_rows)),
                "market_value_krw": market_value,
            }
        )

    out = pd.DataFrame(rows, index=pd.to_datetime(pd.Index(day_list), format="%Y%m%d", errors="coerce"))
    out["equity_norm"] = pd.to_numeric(out["equity_krw"], errors="coerce") / float(capital_total)
    out["net_return"] = out["equity_norm"].pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out["signal"] = pd.to_numeric(out["position"], errors="coerce").fillna(0.0)
    return out


def _build_daily_frame(market_df: pd.DataFrame) -> pd.DataFrame:
    dates = pd.to_datetime(pd.Index(market_df.index), errors="coerce")
    if pd.isna(dates).all():
        raise ValueError("market_df index has no valid datetime values")
    day_tuple = tuple(_norm_ymd(x) for x in dates)
    return _build_daily_frame_cached(day_tuple, _oper_start_ymd()).copy()


def _apply_validation_turnover_cap(daily: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    out = daily.copy()
    cap_raw = (params or {}).get("daily_gross_turnover_cap_pct")
    if cap_raw in (None, ""):
        return out
    try:
        cap = float(cap_raw)
    except Exception:
        return out
    if cap <= 0:
        return out
    if cap > 1.0:
        cap = cap / 100.0
    out["turnover_raw"] = pd.to_numeric(out.get("turnover"), errors="coerce").fillna(0.0).clip(lower=0.0)
    out["turnover"] = out["turnover_raw"].clip(upper=float(cap))
    raw = out["turnover_raw"].replace(0.0, np.nan)
    out["turnover_scale"] = (out["turnover"] / raw).replace([np.inf, -np.inf], np.nan).fillna(1.0).clip(lower=0.0, upper=1.0)
    if "net_return" in out.columns:
        out["net_return_raw"] = pd.to_numeric(out["net_return"], errors="coerce").fillna(0.0)
        out["net_return"] = out["net_return_raw"] * out["turnover_scale"]
    out["turnover_cap_pct"] = float(cap)
    return out


def real_strategy_signal(market_df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    daily = _apply_validation_turnover_cap(_build_daily_frame(market_df), params)
    out = daily[["signal", "turnover"]].copy()
    out["signal"] = pd.to_numeric(out["turnover"], errors="coerce").shift(1).fillna(0.0)
    out["position"] = pd.to_numeric(out["signal"], errors="coerce").shift(1).fillna(0.0)
    return out[["signal", "position", "turnover"]].copy()


def real_strategy_backtest(
    market_df: pd.DataFrame,
    signal_df: pd.DataFrame,
    params: Dict[str, Any],
    cost_model: Any,
) -> Dict[str, Any]:
    daily = _apply_validation_turnover_cap(_build_daily_frame(market_df), params)
    blocked_filter_meta = dict(getattr(_load_ledger(), "attrs", {}).get("blocked_entry_filter", {}) or {})
    surge_stop_gap_meta = dict(getattr(_load_ledger(), "attrs", {}).get("surge_stop_gap_validation_scenario", {}) or {})
    reentry_cooldown_meta = dict(getattr(_load_ledger(), "attrs", {}).get("reentry_cooldown_validation_scenario", {}) or {})
    legacy_model_day_meta = dict(getattr(_load_ledger(), "attrs", {}).get("legacy_model_day_validation_scenario", {}) or {})
    entry_quality_audit_meta = dict(getattr(_load_ledger(), "attrs", {}).get("entry_quality_audit_validation_scenario", {}) or {})
    turnover = pd.to_numeric(daily["turnover"], errors="coerce").fillna(0.0)
    ledger_returns = pd.to_numeric(daily["net_return"], errors="coerce").fillna(0.0)
    commission_bps = float(getattr(cost_model, "commission_bps", 0.0) or 0.0)
    slippage_bps = float(getattr(cost_model, "slippage_bps", 0.0) or 0.0)
    spread_bps = float(getattr(cost_model, "spread_bps", 0.0) or 0.0)
    impact_bps = float(getattr(cost_model, "impact_bps", 0.0) or 0.0)
    adverse_bps = float(getattr(cost_model, "adverse_bps", 0.0) or 0.0)
    validation_cost_bps = max(0.0, commission_bps + slippage_bps + spread_bps + impact_bps + adverse_bps)
    cost_stress = turnover * (validation_cost_bps / 10000.0)
    returns = (ledger_returns - cost_stress).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    equity = (1.0 + returns).cumprod()
    annual_turnover = float(turnover.mean() * 252.0) if len(turnover) else float("nan")
    sharpe = _annualized_sharpe(returns)
    max_dd = _max_drawdown(equity)
    total_return = float(equity.iloc[-1] - 1.0) if len(equity) else 0.0
    gains = returns[returns > 0].sum()
    losses = -returns[returns < 0].sum()
    profit_factor = float(gains / losses) if losses > 0 else (float("inf") if gains > 0 else float("nan"))
    vals = returns.dropna().to_numpy(dtype=float)
    if len(vals) >= 2:
        rng = np.random.default_rng(42)
        totals = []
        for _ in range(1000):
            sample = rng.choice(vals, size=len(vals), replace=True)
            totals.append(float(np.prod(1.0 + sample) - 1.0))
        ci_low = float(np.percentile(totals, 2.5))
        ci_med = float(np.percentile(totals, 50.0))
        ci_high = float(np.percentile(totals, 97.5))
    else:
        ci_low = ci_med = ci_high = float("nan")

    trades = daily.loc[turnover > 0, ["date", "net_return", "turnover"]].copy()
    trades["return"] = returns.loc[trades.index]
    trades = trades.rename(columns={"date": "timestamp", "net_return": "ledger_return"})
    buy_trades = _load_ledger()
    if not buy_trades.empty:
        buy_side = buy_trades.get("side", pd.Series("", index=buy_trades.index)).astype(str).str.upper().str.strip()
        buy_date = buy_trades.get("date", pd.Series("", index=buy_trades.index)).astype(str)
        buy_trades = buy_trades.loc[buy_side.eq("BUY") & (buy_date >= _oper_start_ymd())].copy()

    return {
        "returns": returns,
        "equity": equity,
        "trades": trades,
        "metrics": {
            "sharpe": sharpe,
            "max_drawdown": max_dd,
            "annual_turnover": annual_turnover,
            "pnl_total_return": total_return,
            "profit_factor": profit_factor,
            "pnl_total_return_ci95_low": ci_low,
            "pnl_total_return_ci95_med": ci_med,
            "pnl_total_return_ci95_high": ci_high,
            "n_trade_proxy": float(len(buy_trades)),
            "n_buy_trades": float(len(buy_trades)),
            "n_active_days": float(len(returns)),
        },
        "meta": {
            "engine": "real_strategy_backtest",
            "source": {
                "ledger_csv": str(DEFAULT_LEDGER),
                "price_parquet": str(DEFAULT_PRICES),
                "engine_config": str(DEFAULT_CFG),
            },
            "blocked_entry_filter": blocked_filter_meta,
            "surge_stop_gap_validation_scenario": surge_stop_gap_meta,
            "reentry_cooldown_validation_scenario": reentry_cooldown_meta,
            "legacy_model_day_validation_scenario": legacy_model_day_meta,
            "entry_quality_audit_validation_scenario": entry_quality_audit_meta,
            "execution": {
                "turnover_annualized": annual_turnover,
                "capital_total": _load_capital_total(),
                "validation_cost_stress_applied": True,
                "validation_cost_bps": validation_cost_bps,
                "cost_breakdown_bps": {
                    "commission": commission_bps,
                    "slippage": slippage_bps,
                    "spread": spread_bps,
                    "impact": impact_bps,
                    "adverse_selection": adverse_bps,
                },
                "daily_gross_turnover_cap_pct": (
                    None
                    if "turnover_cap_pct" not in daily.columns
                    else float(pd.to_numeric(daily["turnover_cap_pct"], errors="coerce").dropna().iloc[0])
                ),
            },
        },
    }
