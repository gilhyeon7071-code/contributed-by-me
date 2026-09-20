from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"
PRICE_PATH = PAPER_DIR / "prices" / "ohlcv_paper.parquet"
P1_GATE_PATH = LOG_DIR / "p1_entry_gate_status_latest.json"
ENTRY_SIGNAL_PATH = LOG_DIR / "entry_signal_snapshot_latest.csv"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except UnicodeDecodeError:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _norm_code(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text.upper()


def _ymd_from_any(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return ""


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "y", "yes"}


def _ret(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _latest_buy_ymd(fills: pd.DataFrame) -> tuple[str, str]:
    if fills.empty or "datetime" not in fills.columns:
        return "", "fills_missing_or_no_datetime"
    df = fills.copy()
    df["_ymd"] = df["datetime"].map(_ymd_from_any)
    side_col = "side" if "side" in df.columns else ""
    if side_col:
        buy = df[df[side_col].astype(str).str.upper().eq("BUY")].copy()
        if not buy.empty:
            ymd = str(buy["_ymd"].max() or "")
            if ymd:
                return ymd, "latest_buy_ymd"
    ymd = str(df["_ymd"].max() or "")
    return ymd, "latest_any_ymd" if ymd else "fills_no_valid_ymd"


def _candidate_path() -> Path:
    paths = [
        LOG_DIR / "candidates_latest_data.with_final_score.csv",
        LOG_DIR / "candidates_latest_data.csv",
        LOG_DIR / "candidates_latest_data.filtered.csv",
        LOG_DIR / "candidates_latest.csv",
    ]
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def _numeric_series(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def _bool_condition_columns(candidates: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    out = candidates.copy()
    if out.empty:
        return out
    rs = _numeric_series(out, "rs")
    rs_excess = _numeric_series(out, "rs_excess")
    out["alpha_rs_pass"] = (rs > _to_float(params.get("rs_lim"), 1.7)) | (
        rs_excess > _to_float(params.get("rs_excess_min"), 0.02)
    )
    out["alpha_v_accel_pass"] = _numeric_series(out, "v_accel") > _to_float(params.get("v_accel_lim"), 2.5)
    out["alpha_value_pass"] = _numeric_series(out, "value") > _to_float(params.get("value_min"), 0.0)
    out["alpha_atr_pass"] = _numeric_series(out, "atr14_pct") < _to_float(params.get("atr_max"), 0.12)
    out["alpha_rsi_pass"] = _numeric_series(out, "rsi14") < _to_float(params.get("rsi_max"), 70.0)
    out["alpha_adx_pass"] = _numeric_series(out, "adx14") >= _to_float(params.get("adx_trend_min"), 20.0)
    macd_src = out["macd_bullish"] if "macd_bullish" in out.columns else out.get("macd_golden", pd.Series([""] * len(out)))
    out["alpha_macd_pass"] = macd_src.map(_to_bool)
    stoch = _numeric_series(out, "stoch_k")
    out["alpha_stoch_pass"] = (stoch >= _to_float(params.get("stoch_k_min"), 20.0)) & (
        stoch <= _to_float(params.get("stoch_k_max"), 85.0)
    )
    out["alpha_volcorr_pass"] = _numeric_series(out, "vol_close_corr20") >= _to_float(
        params.get("vol_close_corr_min"), 0.0
    )
    out["alpha_disparity20_pass"] = _numeric_series(out, "disparity20", 999.0) <= _to_float(
        params.get("disparity20_max"), 1.06
    )
    out["alpha_disparity60_pass"] = _numeric_series(out, "disparity60", 999.0) <= _to_float(
        params.get("disparity60_max"), 1.12
    )
    out["alpha_high52_pass"] = _numeric_series(out, "high_52w_gap", 999.0) <= _to_float(
        params.get("near_52w_high_gap_max"), 0.05
    )
    out["alpha_listing_pass"] = _numeric_series(out, "listing_days") >= _to_float(params.get("min_listing_days"), 126.0)
    return out


def _build_reason_summary(row: pd.Series) -> str:
    keys = [
        "candidate_origin",
        "execution_pool",
        "natural_pass",
        "final_score",
        "score",
        "fundamental_score",
        "sector_action",
        "sector_entry_allowed",
        "news_implication_top_actions",
        "junk_risk_score",
        "junk_risk_grade",
    ]
    parts = []
    for key in keys:
        value = row.get(key, "")
        if str(value).strip():
            parts.append(f"{key}={value}")
    pass_cols = [
        "alpha_rs_pass",
        "alpha_v_accel_pass",
        "alpha_value_pass",
        "alpha_atr_pass",
        "alpha_rsi_pass",
        "alpha_adx_pass",
        "alpha_macd_pass",
        "alpha_stoch_pass",
        "alpha_volcorr_pass",
        "alpha_disparity20_pass",
        "alpha_disparity60_pass",
        "alpha_high52_pass",
        "alpha_listing_pass",
    ]
    passed = [c.replace("alpha_", "").replace("_pass", "") for c in pass_cols if bool(row.get(c, False))]
    failed = [c.replace("alpha_", "").replace("_pass", "") for c in pass_cols if not bool(row.get(c, False))]
    parts.append("passed=" + "|".join(passed))
    parts.append("failed=" + "|".join(failed))
    return "; ".join(parts)


def _prepare_candidates(path: Path, params: dict[str, Any]) -> pd.DataFrame:
    df = _read_csv(path)
    if df.empty:
        return df
    df = df.copy()
    df["code_norm"] = df["code"].map(_norm_code) if "code" in df.columns else ""
    if "date_yyyymmdd" in df.columns:
        df["signal_ymd"] = df["date_yyyymmdd"].map(_ymd_from_any)
    elif "date" in df.columns:
        df["signal_ymd"] = df["date"].map(_ymd_from_any)
    else:
        df["signal_ymd"] = ""
    df = _bool_condition_columns(df, params)
    df["alpha_reason_summary"] = df.apply(_build_reason_summary, axis=1)
    return df


def _prepare_fills(path: Path, d_rule_ymd: str) -> pd.DataFrame:
    df = _read_csv(path)
    if df.empty:
        return df
    df = df.copy()
    df["code_norm"] = df["code"].map(_norm_code) if "code" in df.columns else ""
    df["fill_ymd"] = df["datetime"].map(_ymd_from_any) if "datetime" in df.columns else ""
    side = df["side"].astype(str).str.upper() if "side" in df.columns else pd.Series([""] * len(df))
    df = df[side.eq("BUY")].copy()
    if d_rule_ymd:
        df = df[df["fill_ymd"].le(d_rule_ymd)].copy()
    qty_col = "fill_qty" if "fill_qty" in df.columns else ("qty" if "qty" in df.columns else "")
    price_col = "fill_price" if "fill_price" in df.columns else ("price" if "price" in df.columns else "")
    df["_fill_qty_num"] = pd.to_numeric(df[qty_col], errors="coerce") if qty_col else 0.0
    df["_fill_price_num"] = pd.to_numeric(df[price_col], errors="coerce") if price_col else 0.0
    return df


def _prepare_trades(path: Path, d_rule_ymd: str) -> pd.DataFrame:
    df = _read_csv(path)
    if df.empty:
        return df
    df = df.copy()
    df["code_norm"] = df["code"].map(_norm_code) if "code" in df.columns else ""
    df["entry_ymd"] = df["entry_ts"].map(_ymd_from_any) if "entry_ts" in df.columns else ""
    if d_rule_ymd:
        df = df[df["entry_ymd"].le(d_rule_ymd)].copy()
    return df


def _first_match(rows: pd.DataFrame, ymd_col: str, signal_ymd: str, d_rule_ymd: str) -> pd.DataFrame:
    if rows.empty:
        return rows
    out = rows.copy()
    if signal_ymd:
        out = out[out[ymd_col].ge(signal_ymd)].copy()
    if d_rule_ymd:
        out = out[out[ymd_col].le(d_rule_ymd)].copy()
    return out.sort_values(ymd_col, kind="mergesort")


def _attach_execution(candidates: pd.DataFrame, fills: pd.DataFrame, trades: pd.DataFrame, d_rule_ymd: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    fill_groups = {code: grp for code, grp in fills.groupby("code_norm")} if not fills.empty and "code_norm" in fills else {}
    trade_groups = {code: grp for code, grp in trades.groupby("code_norm")} if not trades.empty and "code_norm" in trades else {}
    for _, row in candidates.iterrows():
        rec = row.to_dict()
        code = str(rec.get("code_norm") or "")
        signal_ymd = str(rec.get("signal_ymd") or "")
        fmatch = _first_match(fill_groups.get(code, pd.DataFrame()), "fill_ymd", signal_ymd, d_rule_ymd)
        tmatch = _first_match(trade_groups.get(code, pd.DataFrame()), "entry_ymd", signal_ymd, d_rule_ymd)
        rec["matched_buy_count"] = int(len(fmatch))
        rec["matched_buy"] = bool(len(fmatch) > 0)
        rec["first_buy_ymd"] = str(fmatch.iloc[0].get("fill_ymd", "")) if len(fmatch) else ""
        rec["first_buy_datetime"] = str(fmatch.iloc[0].get("datetime", "")) if len(fmatch) else ""
        rec["first_buy_order_id"] = str(fmatch.iloc[0].get("order_id", "")) if len(fmatch) else ""
        rec["matched_buy_qty_sum"] = float(pd.to_numeric(fmatch.get("_fill_qty_num", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if len(fmatch) else 0.0
        rec["first_buy_price"] = str(fmatch.iloc[0].get("fill_price", fmatch.iloc[0].get("price", ""))) if len(fmatch) else ""
        rec["matched_trade_count"] = int(len(tmatch))
        rec["matched_trade"] = bool(len(tmatch) > 0)
        rec["first_trade_id"] = str(tmatch.iloc[0].get("trade_id", "")) if len(tmatch) else ""
        rec["first_trade_entry_ymd"] = str(tmatch.iloc[0].get("entry_ymd", "")) if len(tmatch) else ""
        rec["first_trade_exit_ymd"] = _ymd_from_any(tmatch.iloc[0].get("exit_ts", "")) if len(tmatch) else ""
        rec["first_trade_net_ret"] = str(tmatch.iloc[0].get("net_ret", "")) if len(tmatch) else ""
        rec["first_trade_exit_reason"] = str(tmatch.iloc[0].get("exit_reason", "")) if len(tmatch) else ""
        rows.append(rec)
    return pd.DataFrame(rows)


def _load_prices(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if df.empty or not {"date", "code", "close"}.issubset(df.columns):
        return pd.DataFrame()
    out = df[["date", "code", "close"]].copy()
    out["date_ymd"] = out["date"].map(_ymd_from_any)
    out["code_norm"] = out["code"].map(_norm_code)
    out["close_num"] = pd.to_numeric(out["close"], errors="coerce")
    out = out[(out["date_ymd"] != "") & out["code_norm"].ne("") & out["close_num"].gt(0)].copy()
    return out.sort_values(["code_norm", "date_ymd"], kind="mergesort")


def _price_label_row(price_rows: pd.DataFrame, signal_ymd: str, horizons: tuple[int, ...]) -> dict[str, Any]:
    labels: dict[str, Any] = {
        "label_entry_price_source": "",
        "label_entry_close": "",
        "label_entry_close_ymd": "",
        "label_status": "NO_PRICE_HISTORY",
    }
    for h in horizons:
        labels[f"fwd_{h}d_close"] = ""
        labels[f"fwd_{h}d_ymd"] = ""
        labels[f"fwd_{h}d_ret"] = ""
        labels[f"fwd_{h}d_status"] = "NO_PRICE_HISTORY"
    if price_rows.empty or not signal_ymd:
        return labels
    rows = price_rows[price_rows["date_ymd"].ge(signal_ymd)].copy()
    if rows.empty:
        labels["label_status"] = "NO_ENTRY_CLOSE"
        for h in horizons:
            labels[f"fwd_{h}d_status"] = "NO_ENTRY_CLOSE"
        return labels
    entry = rows.iloc[0]
    entry_close = _to_float(entry.get("close_num"), 0.0)
    labels["label_entry_price_source"] = "ohlcv_paper.close"
    labels["label_entry_close"] = entry_close
    labels["label_entry_close_ymd"] = str(entry.get("date_ymd") or "")
    labels["label_status"] = "PENDING_FUTURE"
    for h in horizons:
        if len(rows) <= h:
            labels[f"fwd_{h}d_status"] = "INSUFFICIENT_FUTURE_PRICE"
            continue
        future = rows.iloc[h]
        future_close = _to_float(future.get("close_num"), 0.0)
        labels[f"fwd_{h}d_close"] = future_close
        labels[f"fwd_{h}d_ymd"] = str(future.get("date_ymd") or "")
        if entry_close > 0 and future_close > 0:
            labels[f"fwd_{h}d_ret"] = (future_close / entry_close) - 1.0
            labels[f"fwd_{h}d_status"] = "LABELED"
            labels["label_status"] = "LABELED"
    return labels


def _attach_performance_labels(dataset: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    if dataset.empty:
        return dataset
    out_rows: list[dict[str, Any]] = []
    price_groups = {code: grp for code, grp in prices.groupby("code_norm")} if not prices.empty else {}
    horizons = (1, 3, 5)
    for _, row in dataset.iterrows():
        rec = row.to_dict()
        code = str(rec.get("code_norm") or "")
        signal_ymd = str(rec.get("signal_ymd") or "")
        rec.update(_price_label_row(price_groups.get(code, pd.DataFrame()), signal_ymd, horizons))
        trade_ret = _ret(rec.get("first_trade_net_ret"))
        fwd1 = _ret(rec.get("fwd_1d_ret"))
        if trade_ret is not None:
            rec["eval_return_source"] = "first_trade_net_ret"
            rec["eval_return"] = trade_ret
            rec["eval_label_status"] = "LABELED"
        elif fwd1 is not None:
            rec["eval_return_source"] = "fwd_1d_ret"
            rec["eval_return"] = fwd1
            rec["eval_label_status"] = "LABELED"
        else:
            rec["eval_return_source"] = ""
            rec["eval_return"] = ""
            rec["eval_label_status"] = "INSUFFICIENT"
        out_rows.append(rec)
    return pd.DataFrame(out_rows)


def _performance_stats(df: pd.DataFrame, return_col: str = "eval_return") -> dict[str, Any]:
    if df.empty or return_col not in df.columns:
        return {"rows": int(len(df)), "labeled_rows": 0, "win_rate": None, "avg_ret": None, "median_ret": None}
    vals = pd.to_numeric(df[return_col], errors="coerce").dropna()
    if vals.empty:
        return {"rows": int(len(df)), "labeled_rows": 0, "win_rate": None, "avg_ret": None, "median_ret": None}
    wins = vals.gt(0)
    gross_profit = vals[vals.gt(0)].sum()
    gross_loss = abs(vals[vals.lt(0)].sum())
    return {
        "rows": int(len(df)),
        "labeled_rows": int(len(vals)),
        "win_rate": float(wins.mean()),
        "avg_ret": float(vals.mean()),
        "median_ret": float(vals.median()),
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else None,
        "min_ret": float(vals.min()),
        "max_ret": float(vals.max()),
    }


def _score_bucket_report(dataset: pd.DataFrame, d_rule_ymd: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if dataset.empty or "final_score" not in dataset.columns:
        return pd.DataFrame(), {"status": "FAIL", "reason": "missing_final_score"}
    df = dataset.copy()
    df["_score_num"] = pd.to_numeric(df["final_score"], errors="coerce")
    df = df[df["_score_num"].notna()].copy()
    if df.empty:
        return pd.DataFrame(), {"status": "FAIL", "reason": "no_numeric_final_score"}
    df = df.sort_values(["_score_num", "code_norm"], ascending=[False, True], kind="mergesort").reset_index(drop=True)
    n = len(df)
    labels = []
    for i in range(n):
        pct = (i + 1) / n
        if pct <= 1 / 3:
            labels.append("TOP")
        elif pct <= 2 / 3:
            labels.append("MID")
        else:
            labels.append("BOTTOM")
    df["score_bucket"] = labels
    rows = []
    for bucket, grp in df.groupby("score_bucket", sort=False):
        stats = _performance_stats(grp)
        rows.append(
            {
                "d_rule_ymd": d_rule_ymd,
                "score_bucket": bucket,
                "score_min": float(grp["_score_num"].min()),
                "score_max": float(grp["_score_num"].max()),
                **stats,
            }
        )
    report = pd.DataFrame(rows)
    return report, {
        "status": "PASS",
        "bucket_count": int(len(report)),
        "labeled_rows": int(pd.to_numeric(df.get("eval_return"), errors="coerce").notna().sum()),
    }


def _condition_report(dataset: pd.DataFrame, d_rule_ymd: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    pass_cols = [c for c in dataset.columns if c.startswith("alpha_") and c.endswith("_pass")]
    rows = []
    for col in pass_cols:
        ser = dataset[col].map(_to_bool) if dataset[col].dtype == object else dataset[col].astype(bool)
        for state, grp in (("PASS", dataset[ser].copy()), ("FAIL", dataset[~ser].copy())):
            rows.append(
                {
                    "d_rule_ymd": d_rule_ymd,
                    "condition": col,
                    "condition_state": state,
                    **_performance_stats(grp),
                }
            )
        pass_stats = _performance_stats(dataset[ser].copy())
        fail_stats = _performance_stats(dataset[~ser].copy())
        pass_avg = pass_stats.get("avg_ret")
        fail_avg = fail_stats.get("avg_ret")
        rows.append(
            {
                "d_rule_ymd": d_rule_ymd,
                "condition": col,
                "condition_state": "PASS_MINUS_FAIL",
                "rows": int(len(dataset)),
                "labeled_rows": min(int(pass_stats.get("labeled_rows") or 0), int(fail_stats.get("labeled_rows") or 0)),
                "win_rate": None,
                "avg_ret": (float(pass_avg) - float(fail_avg)) if pass_avg is not None and fail_avg is not None else None,
                "median_ret": None,
                "profit_factor": None,
                "min_ret": None,
                "max_ret": None,
            }
        )
    report = pd.DataFrame(rows)
    labeled = int(pd.to_numeric(dataset.get("eval_return"), errors="coerce").notna().sum()) if "eval_return" in dataset.columns else 0
    return report, {"status": "PASS" if pass_cols else "FAIL", "condition_count": int(len(pass_cols)), "labeled_rows": labeled}


def _alpha_pass_count(row: pd.Series) -> int:
    pass_cols = [c for c in row.index if c.startswith("alpha_") and c.endswith("_pass")]
    return int(sum(1 for c in pass_cols if bool(row.get(c, False))))


def _alpha_decision(row: pd.Series) -> tuple[str, str]:
    score = _to_float(row.get("final_score"), 0.0)
    origin = str(row.get("candidate_origin") or "").strip().upper()
    execution_pool = _to_bool(row.get("execution_pool"))
    pass_count = _alpha_pass_count(row)
    if execution_pool:
        return "ALPHA_ELIGIBLE", f"execution_pool=true; final_score={score:.6f}; pass_count={pass_count}; origin={origin}"
    if score > 0 and pass_count >= 8:
        return "ALPHA_WATCH", f"final_score>0; pass_count={pass_count}; origin={origin}"
    return "ALPHA_WEAK", f"final_score={score:.6f}; pass_count={pass_count}; origin={origin}"


def _load_entry_signal_map(path: Path) -> dict[str, dict[str, Any]]:
    df = _read_csv(path)
    if df.empty or "code" not in df.columns:
        return {}
    df = df.copy()
    df["code_norm"] = df["code"].map(_norm_code)
    out: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        out[str(row.get("code_norm") or "")] = row.to_dict()
    return out


def _gate_context() -> dict[str, Any]:
    p1 = _read_json(P1_GATE_PATH)
    runtime = p1.get("risk_gate_runtime") if isinstance(p1.get("risk_gate_runtime"), dict) else {}
    entry_gate = runtime.get("entry_gate") if isinstance(runtime.get("entry_gate"), dict) else {}
    risk_orch = runtime.get("risk_orchestration") if isinstance(runtime.get("risk_orchestration"), dict) else {}
    prod = runtime.get("production_risk_playbook") if isinstance(runtime.get("production_risk_playbook"), dict) else {}
    return {
        "p1": p1,
        "runtime": runtime,
        "entry_gate_decision": str(entry_gate.get("decision") or p1.get("entry_gate_decision_before_p1") or ""),
        "entry_gate_reason": str(entry_gate.get("reason") or p1.get("entry_gate_reason_before_p1") or ""),
        "entry_stop_new_orders": bool(entry_gate.get("stop_new_orders", False)),
        "risk_off_enabled": bool(runtime.get("risk_off_enabled", False)),
        "risk_off_reasons": "|".join(str(x) for x in (runtime.get("risk_off_reasons") or [])),
        "risk_orch_scale": _to_float(risk_orch.get("scale"), 1.0),
        "risk_orch_zero_causes": "|".join(str(x) for x in (risk_orch.get("scale_zero_causes") or [])),
        "production_risk_decision": str(prod.get("decision") or ""),
        "production_risk_action": str(prod.get("action") or ""),
        "production_risk_reason": str(prod.get("reason") or ""),
        "position_size_multiplier": _to_float(runtime.get("position_size_multiplier"), 1.0),
        "entry_candidates_before_p1": int(p1.get("entry_candidates_before", 0) or 0),
        "entry_candidates_after_p1": int(p1.get("entry_candidates_after", 0) or 0),
        "max_new_before": int(p1.get("max_new_before", 0) or 0),
        "max_new_after": int(p1.get("max_new_after", 0) or 0),
    }


def _attach_decision_layers(dataset: pd.DataFrame, d_rule_ymd: str) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if dataset.empty:
        return dataset, pd.DataFrame(), {"status": "FAIL", "reason": "empty_dataset"}
    ctx = _gate_context()
    signal_map = _load_entry_signal_map(ENTRY_SIGNAL_PATH)
    rows = []
    for _, row in dataset.iterrows():
        rec = row.to_dict()
        alpha_decision, alpha_reason = _alpha_decision(row)
        code = str(rec.get("code_norm") or "")
        sig = signal_map.get(code, {})
        signal_decision = str(sig.get("signal") or "")
        signal_reason = str(sig.get("reason") or "")
        risk_blocked = bool(ctx["risk_off_enabled"]) or str(ctx["entry_gate_decision"]).upper() == "BLOCK"
        risk_reduced = str(ctx["entry_gate_decision"]).upper() in {"REDUCE", "CAUTION"} or ctx["risk_orch_scale"] <= 0.0
        execution_blocked = "FAIL_CLOSED" in signal_reason or "CLOSE_CUTOFF" in signal_reason or signal_decision.upper() == "HOLD"
        rec.update(
            {
                "alpha_decision": alpha_decision,
                "alpha_decision_reason": alpha_reason,
                "risk_gate_decision": ctx["entry_gate_decision"],
                "risk_gate_reason": ctx["entry_gate_reason"],
                "risk_off_enabled": ctx["risk_off_enabled"],
                "risk_off_reasons": ctx["risk_off_reasons"],
                "risk_orch_scale": ctx["risk_orch_scale"],
                "risk_orch_zero_causes": ctx["risk_orch_zero_causes"],
                "production_risk_decision": ctx["production_risk_decision"],
                "production_risk_action": ctx["production_risk_action"],
                "production_risk_reason": ctx["production_risk_reason"],
                "position_size_multiplier": ctx["position_size_multiplier"],
                "risk_layer_state": "BLOCKED" if risk_blocked else ("REDUCED" if risk_reduced else "ALLOW"),
                "execution_signal": signal_decision,
                "execution_signal_reason": signal_reason,
                "execution_layer_state": "BLOCKED" if execution_blocked else ("FILLED" if bool(rec.get("matched_buy")) else "NOT_FILLED"),
            }
        )
        if alpha_decision in {"ALPHA_ELIGIBLE", "ALPHA_WATCH"} and (risk_blocked or risk_reduced or execution_blocked):
            rec["separation_summary"] = "GOOD_OR_WATCH_ALPHA_LIMITED_BY_RISK_OR_EXECUTION"
        elif alpha_decision == "ALPHA_WEAK":
            rec["separation_summary"] = "WEAK_ALPHA"
        elif bool(rec.get("matched_buy")):
            rec["separation_summary"] = "ALPHA_TO_BUY_LINKED"
        else:
            rec["separation_summary"] = "ALPHA_NOT_EXECUTED"
        rows.append(rec)
    out = pd.DataFrame(rows)
    layer_rows = []
    for layer, col in [
        ("alpha", "alpha_decision"),
        ("risk", "risk_layer_state"),
        ("execution", "execution_layer_state"),
        ("summary", "separation_summary"),
    ]:
        counts = out[col].astype(str).value_counts(dropna=False).to_dict()
        for state, count in counts.items():
            layer_rows.append({"d_rule_ymd": d_rule_ymd, "layer": layer, "state": state, "count": int(count)})
    layer_report = pd.DataFrame(layer_rows)
    status = {
        "status": "PASS",
        "alpha_eligible_or_watch": int(out["alpha_decision"].isin(["ALPHA_ELIGIBLE", "ALPHA_WATCH"]).sum()),
        "risk_reduced_or_blocked": int(out["risk_layer_state"].isin(["REDUCED", "BLOCKED"]).sum()),
        "execution_blocked": int(out["execution_layer_state"].eq("BLOCKED").sum()),
    }
    return out, layer_report, status


def _write_daily_report(
    *,
    d_rule_ymd: str,
    status: dict[str, Any],
    dataset: pd.DataFrame,
    layer_report: pd.DataFrame,
    bucket_report: pd.DataFrame,
    condition_report: pd.DataFrame,
    latest_txt: Path,
    dated_txt: Path,
    latest_json: Path,
    dated_json: Path,
) -> dict[str, Any]:
    alpha_counts = (
        dataset["alpha_decision"].astype(str).value_counts().to_dict()
        if "alpha_decision" in dataset.columns else {}
    )
    summary_counts = (
        dataset["separation_summary"].astype(str).value_counts().to_dict()
        if "separation_summary" in dataset.columns else {}
    )
    labeled_rows = int(pd.to_numeric(dataset.get("eval_return"), errors="coerce").notna().sum()) if "eval_return" in dataset.columns else 0
    report = {
        "schema_version": "entry_alpha_daily_report_v1",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "d_rule_ymd": d_rule_ymd,
        "status": "PASS",
        "current_evaluation_state": "WAITING_FOR_PERFORMANCE_LABELS" if labeled_rows == 0 else "EVALUATED",
        "counts": dict(status.get("counts") or {}),
        "alpha_counts": {str(k): int(v) for k, v in alpha_counts.items()},
        "separation_summary_counts": {str(k): int(v) for k, v in summary_counts.items()},
        "score_bucket_rows": int(len(bucket_report)),
        "condition_report_rows": int(len(condition_report)),
        "layer_report_rows": int(len(layer_report)),
        "observations": [
            f"D={d_rule_ymd}",
            f"candidate_rows={status.get('counts', {}).get('candidate_rows', 0)}",
            f"matched_buy_candidates={status.get('counts', {}).get('matched_buy_candidates', 0)}",
            f"eval_labeled_rows={labeled_rows}",
        ],
        "not_tested": [
            "No buy policy effectiveness conclusion when eval_labeled_rows=0.",
            "No order, fill, ledger, gate, lock, threshold, or score formula mutation.",
        ],
        "outputs": dict(status.get("outputs") or {}),
    }
    lines = [
        f"Entry Alpha Daily Report D={d_rule_ymd}",
        f"state={report['current_evaluation_state']}",
        f"candidate_rows={report['counts'].get('candidate_rows', 0)}",
        f"matched_buy_candidates={report['counts'].get('matched_buy_candidates', 0)}",
        f"eval_labeled_rows={labeled_rows}",
        "",
        "alpha_counts:",
    ]
    lines.extend([f"- {k}: {v}" for k, v in report["alpha_counts"].items()])
    lines.append("")
    lines.append("separation_summary_counts:")
    lines.extend([f"- {k}: {v}" for k, v in report["separation_summary_counts"].items()])
    lines.append("")
    lines.append("not_tested:")
    lines.extend([f"- {x}" for x in report["not_tested"]])
    text = "\n".join(lines) + "\n"
    dated_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    dated_txt.write_text(text, encoding="utf-8")
    latest_txt.write_text(text, encoding="utf-8")
    return report


def build(date_override: str = "") -> dict[str, Any]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fills_path = PAPER_DIR / "fills.csv"
    trades_path = PAPER_DIR / "trades_calc.csv"
    meta_path = LOG_DIR / "candidates_latest_meta.json"
    candidate_path = _candidate_path()

    fills_all = _read_csv(fills_path)
    d_rule_ymd, d_source = _latest_buy_ymd(fills_all)
    if date_override:
        d_rule_ymd = _ymd_from_any(date_override)
        d_source = "arg_date"
    meta = _read_json(meta_path)
    params = meta.get("chosen_params") if isinstance(meta.get("chosen_params"), dict) else {}
    candidates = _prepare_candidates(candidate_path, params)
    fills = _prepare_fills(fills_path, d_rule_ymd)
    trades = _prepare_trades(trades_path, d_rule_ymd)
    dataset = _attach_execution(candidates, fills, trades, d_rule_ymd) if not candidates.empty else pd.DataFrame()
    prices = _load_prices(PRICE_PATH)
    dataset = _attach_performance_labels(dataset, prices) if not dataset.empty else dataset
    dataset, layer_report, layer_status = _attach_decision_layers(dataset, d_rule_ymd) if not dataset.empty else (dataset, pd.DataFrame(), {"status": "FAIL", "reason": "empty_dataset"})
    bucket_report, bucket_status = _score_bucket_report(dataset, d_rule_ymd)
    condition_report, condition_status = _condition_report(dataset, d_rule_ymd)

    dated_csv = LOG_DIR / f"entry_alpha_basis_{d_rule_ymd or 'unknown'}.csv"
    latest_csv = LOG_DIR / "entry_alpha_basis_latest.csv"
    dated_labeled_csv = LOG_DIR / f"entry_alpha_labeled_{d_rule_ymd or 'unknown'}.csv"
    latest_labeled_csv = LOG_DIR / "entry_alpha_labeled_latest.csv"
    dated_bucket_csv = LOG_DIR / f"entry_alpha_score_bucket_{d_rule_ymd or 'unknown'}.csv"
    latest_bucket_csv = LOG_DIR / "entry_alpha_score_bucket_latest.csv"
    dated_condition_csv = LOG_DIR / f"entry_alpha_condition_contribution_{d_rule_ymd or 'unknown'}.csv"
    latest_condition_csv = LOG_DIR / "entry_alpha_condition_contribution_latest.csv"
    dated_layer_csv = LOG_DIR / f"entry_alpha_decision_layers_{d_rule_ymd or 'unknown'}.csv"
    latest_layer_csv = LOG_DIR / "entry_alpha_decision_layers_latest.csv"
    dated_report_json = LOG_DIR / f"entry_alpha_daily_report_{d_rule_ymd or 'unknown'}.json"
    latest_report_json = LOG_DIR / "entry_alpha_daily_report_latest.json"
    dated_report_txt = LOG_DIR / f"entry_alpha_daily_report_{d_rule_ymd or 'unknown'}.txt"
    latest_report_txt = LOG_DIR / "entry_alpha_daily_report_latest.txt"
    dated_json = LOG_DIR / f"entry_alpha_basis_{d_rule_ymd or 'unknown'}.json"
    latest_json = LOG_DIR / "entry_alpha_basis_latest.json"
    if not dataset.empty:
        dataset.to_csv(dated_csv, index=False, encoding="utf-8-sig")
        dataset.to_csv(latest_csv, index=False, encoding="utf-8-sig")
    else:
        pd.DataFrame().to_csv(dated_csv, index=False, encoding="utf-8-sig")
        pd.DataFrame().to_csv(latest_csv, index=False, encoding="utf-8-sig")
    dataset.to_csv(dated_labeled_csv, index=False, encoding="utf-8-sig")
    dataset.to_csv(latest_labeled_csv, index=False, encoding="utf-8-sig")
    bucket_report.to_csv(dated_bucket_csv, index=False, encoding="utf-8-sig")
    bucket_report.to_csv(latest_bucket_csv, index=False, encoding="utf-8-sig")
    condition_report.to_csv(dated_condition_csv, index=False, encoding="utf-8-sig")
    condition_report.to_csv(latest_condition_csv, index=False, encoding="utf-8-sig")
    layer_report.to_csv(dated_layer_csv, index=False, encoding="utf-8-sig")
    layer_report.to_csv(latest_layer_csv, index=False, encoding="utf-8-sig")

    status = {
        "schema_version": "entry_alpha_basis_v1",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if d_rule_ymd and not candidates.empty else "FAIL",
        "d_rule_ymd": d_rule_ymd,
        "d_rule_source": d_source,
        "inputs": {
            "candidates": str(candidate_path),
            "candidates_meta": str(meta_path),
            "fills": str(fills_path),
            "trades": str(trades_path),
            "prices": str(PRICE_PATH),
        },
        "outputs": {
            "latest_csv": str(latest_csv),
            "dated_csv": str(dated_csv),
            "latest_labeled_csv": str(latest_labeled_csv),
            "dated_labeled_csv": str(dated_labeled_csv),
            "latest_score_bucket_csv": str(latest_bucket_csv),
            "dated_score_bucket_csv": str(dated_bucket_csv),
            "latest_condition_contribution_csv": str(latest_condition_csv),
            "dated_condition_contribution_csv": str(dated_condition_csv),
            "latest_decision_layers_csv": str(latest_layer_csv),
            "dated_decision_layers_csv": str(dated_layer_csv),
            "latest_daily_report_json": str(latest_report_json),
            "dated_daily_report_json": str(dated_report_json),
            "latest_daily_report_txt": str(latest_report_txt),
            "dated_daily_report_txt": str(dated_report_txt),
            "latest_json": str(latest_json),
            "dated_json": str(dated_json),
        },
        "counts": {
            "candidate_rows": int(len(candidates)),
            "dataset_rows": int(len(dataset)),
            "matched_buy_candidates": int(dataset["matched_buy"].sum()) if "matched_buy" in dataset.columns else 0,
            "matched_trade_candidates": int(dataset["matched_trade"].sum()) if "matched_trade" in dataset.columns else 0,
            "unmatched_candidates": int((~dataset["matched_buy"]).sum()) if "matched_buy" in dataset.columns else 0,
            "eval_labeled_rows": int(pd.to_numeric(dataset.get("eval_return"), errors="coerce").notna().sum()) if "eval_return" in dataset.columns else 0,
            "fwd_1d_labeled_rows": int(pd.to_numeric(dataset.get("fwd_1d_ret"), errors="coerce").notna().sum()) if "fwd_1d_ret" in dataset.columns else 0,
            "score_bucket_rows": int(len(bucket_report)),
            "condition_report_rows": int(len(condition_report)),
            "decision_layer_rows": int(len(layer_report)),
        },
        "performance_labels": {
            "status": "PASS",
            "entry_price_source": "ohlcv_paper.close",
            "horizons": [1, 3, 5],
            "max_price_date": str(prices["date_ymd"].max()) if not prices.empty else "",
            "note": "Forward labels remain INSUFFICIENT until future close rows exist after signal_ymd.",
        },
        "score_bucket_report": bucket_status,
        "condition_contribution_report": condition_status,
        "decision_layer_report": layer_status,
        "criteria_columns": [
            c for c in dataset.columns
            if c.startswith("alpha_") or c in {"candidate_origin", "execution_pool", "natural_pass", "final_score", "score"}
        ],
        "policy": {
            "read_only_for_trading": True,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "gate_modified": False,
            "score_policy_modified": False,
        },
    }
    daily_report = _write_daily_report(
        d_rule_ymd=d_rule_ymd,
        status=status,
        dataset=dataset,
        layer_report=layer_report,
        bucket_report=bucket_report,
        condition_report=condition_report,
        latest_txt=latest_report_txt,
        dated_txt=dated_report_txt,
        latest_json=latest_report_json,
        dated_json=dated_report_json,
    )
    status["daily_report"] = {
        "status": daily_report.get("status"),
        "current_evaluation_state": daily_report.get("current_evaluation_state"),
    }
    dated_json.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description="Build candidate-to-buy alpha basis dataset.")
    parser.add_argument("--date", default="", help="Optional D override in YYYYMMDD format.")
    args = parser.parse_args()
    status = build(args.date)
    print(
        "[ENTRY_ALPHA_BASIS] "
        f"status={status.get('status')} D={status.get('d_rule_ymd')} "
        f"candidates={status.get('counts', {}).get('candidate_rows')} "
        f"matched_buy={status.get('counts', {}).get('matched_buy_candidates')} "
        f"out={status.get('outputs', {}).get('latest_csv')}"
    )
    return 0 if status.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
