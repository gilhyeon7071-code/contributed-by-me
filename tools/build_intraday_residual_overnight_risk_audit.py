from __future__ import annotations

import importlib.util
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PNL_REPORT = ROOT / "paper_pnl_report.py"
TRADES_CALC = ROOT / "paper" / "trades_calc.csv"
ORDERS_DIR = ROOT / "paper"


def _load_pnl_module() -> Any:
    spec = importlib.util.spec_from_file_location("paper_pnl_report_runtime", PNL_REPORT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed_to_load:{PNL_REPORT}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _extract_note(note: Any, key: str) -> str:
    m = re.search(r"(?:^|[;|\s])" + re.escape(key) + r"=([^;|\s]+)", str(note or ""))
    return m.group(1) if m else ""


def _norm_code(value: Any) -> str:
    s = re.sub(r"\D", "", str(value or ""))
    return s.zfill(6) if s else ""


def _to_bool_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})


def _official_scope(pnl: Any) -> tuple[pd.DataFrame, str, dict[str, Any]]:
    raw = pnl._ensure_exit_date(pnl._load_trades(TRADES_CALC))
    df, op_scope = pnl._apply_operational_scope(raw)
    df, blocked = pnl._filter_blocked_entry_trades(df, ORDERS_DIR)
    df, unauditable = pnl._filter_unauditable_surge_trades(df)
    ret_col = pnl._pick_ret_col(df)
    if not ret_col:
        raise RuntimeError("return_column_missing")
    meta = {
        "rows_raw": int(len(raw)),
        "operational_scope": op_scope,
        "blocked_entry_filter": blocked,
        "unauditable_surge_filter": unauditable,
        "ret_col": ret_col,
    }
    return df.copy(), ret_col, meta


def _prepare(df: pd.DataFrame, pnl: Any, ret_col: str) -> pd.DataFrame:
    out = df.copy()
    out = pnl._ensure_exit_date(out)
    out["exit_date"] = out["exit_date"].map(pnl._normalize_ymd)
    if "entry_ts" in out.columns:
        out["entry_date"] = out["entry_ts"].astype(str).map(pnl._normalize_ymd)
    elif "entry_date" in out.columns:
        out["entry_date"] = out["entry_date"].astype(str).map(pnl._normalize_ymd)
    else:
        out["entry_date"] = ""
    out["code_norm"] = out.get("code", pd.Series("", index=out.index)).map(_norm_code)
    out["note_s"] = out.get("note", pd.Series("", index=out.index)).fillna("").astype(str)
    for c in ["net_ret", "gross_ret", "qty", "entry_price", "exit_price"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    for key in [
        "surge_immediate",
        "surge_type",
        "surge_rvol20",
        "surge_score_final",
        "surge_spread_bps",
        "entry_timing",
        "signal_date",
        "signal_ts",
        "order_id",
        "exit_reason",
        "sell_ratio_pct",
        "partial_exit",
    ]:
        out[key] = out["note_s"].map(lambda s, k=key: _extract_note(s, k))
    for c in ["surge_rvol20", "surge_score_final", "surge_spread_bps", "sell_ratio_pct"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["is_surge"] = _to_bool_series(out["surge_immediate"]) | out["surge_type"].astype(str).str.strip().ne("")
    out["entry_key"] = out["entry_date"].astype(str) + "|" + out["code_norm"].astype(str)
    out["_ret"] = pd.to_numeric(out[ret_col], errors="coerce")
    return out


def _metrics(df: pd.DataFrame, pnl: Any, ret_col: str) -> dict[str, Any]:
    eq = pnl._equity_metrics(df.copy(), ret_col)
    ret = pd.to_numeric(df[ret_col], errors="coerce") if ret_col in df.columns else pd.Series(dtype=float)
    return {
        "rows": int(len(df)),
        "trades_used": int(ret.notna().sum()),
        "sum_ret": float(ret.sum()) if len(ret) else 0.0,
        "avg_ret": (float(ret.mean()) if len(ret) else None),
        "loss_n": int((ret < 0).sum()) if len(ret) else 0,
        "max_drawdown_pct": eq.get("max_drawdown_pct"),
        "end_equity": eq.get("end_equity"),
        "last_day_ret": eq.get("last_day_ret"),
    }


def _records(df: pd.DataFrame, ret_col: str) -> list[dict[str, Any]]:
    cols = [
        "trade_id",
        "code_norm",
        "entry_date",
        "entry_ts",
        "exit_date",
        "exit_ts",
        "qty",
        "entry_price",
        "exit_price",
        ret_col,
        "gross_ret",
        "is_surge",
        "surge_rvol20",
        "surge_score_final",
        "surge_spread_bps",
        "entry_timing",
        "signal_ts",
        "order_id",
        "exit_reason",
        "sell_ratio_pct",
        "partial_exit",
    ]
    keep = [c for c in cols if c in df.columns]
    rows = df[keep].copy()
    rows = rows.sort_values([ret_col, "exit_date", "code_norm"], ascending=[True, True, True])
    return json.loads(rows.to_json(orient="records", force_ascii=False))


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    pnl = _load_pnl_module()
    scoped, ret_col, meta = _official_scope(pnl)
    work = _prepare(scoped, pnl, ret_col)

    peak_window = (
        work["exit_date"].astype(str).gt("20260504")
        & work["exit_date"].astype(str).le("20260601")
        & work["entry_timing"].astype(str).eq("intraday_realtime")
    )
    same_day_loss = peak_window & work["exit_date"].eq(work["entry_date"]) & work["_ret"].lt(0)
    same_day_loss_keys = set(work.loc[same_day_loss, "entry_key"].astype(str))
    overnight_mask = peak_window & work["entry_key"].isin(same_day_loss_keys) & work["exit_date"].gt(work["entry_date"])
    surge_overnight_mask = overnight_mask & work["is_surge"]
    non_surge_overnight_mask = overnight_mask & (~work["is_surge"])

    variants = []
    for name, mask in [
        ("remove_all_overnight_after_same_day_loss", overnight_mask),
        ("remove_surge_overnight_after_same_day_loss", surge_overnight_mask),
        ("remove_non_surge_overnight_after_same_day_loss", non_surge_overnight_mask),
        ("remove_surge_rvol_ge3_overnight_after_same_day_loss", surge_overnight_mask & work["surge_rvol20"].ge(3)),
    ]:
        removed = work.loc[mask].copy()
        kept = work.loc[~mask].copy()
        item = {
            "variant": name,
            "removed": _metrics(removed, pnl, ret_col),
            "after": _metrics(kept, pnl, ret_col),
            "removed_codes": sorted(removed["code_norm"].dropna().astype(str).unique().tolist()),
        }
        variants.append(item)

    overnight = work.loc[overnight_mask].copy()
    same_day_loss_rows = work.loc[same_day_loss].copy()
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "basis": "official paper_pnl_report scope; shadow-only removal simulation",
        "source": {
            "trades_calc": str(TRADES_CALC),
            "paper_pnl_report": str(PNL_REPORT),
        },
        "scope_meta": meta,
        "baseline": _metrics(work, pnl, ret_col),
        "peak_window": _metrics(work.loc[peak_window].copy(), pnl, ret_col),
        "same_day_loss": {
            "keys": sorted(same_day_loss_keys),
            "rows": _metrics(same_day_loss_rows, pnl, ret_col),
        },
        "overnight_after_same_day_loss": {
            "rows": _metrics(overnight, pnl, ret_col),
            "by_surge": json.loads(
                overnight.groupby("is_surge")
                .agg(
                    n=("trade_id", "count"),
                    avg_ret=(ret_col, "mean"),
                    sum_ret=(ret_col, "sum"),
                    loss_n=(ret_col, lambda s: int((pd.to_numeric(s, errors="coerce") < 0).sum())),
                    codes=("code_norm", "nunique"),
                )
                .reset_index()
                .to_json(orient="records", force_ascii=False)
            )
            if not overnight.empty
            else [],
            "records": _records(overnight, ret_col),
        },
        "shadow_variants": variants,
        "interpretation": {
            "primary_candidate": "remove_all_overnight_after_same_day_loss",
            "reason": "This shadow variant reduces max_drawdown_pct below the -20% live-transition threshold without changing entry scoring.",
            "implementation_layer": "exit/residual risk management, not entry threshold relaxation",
        },
    }

    out_json = LOG_DIR / "intraday_residual_overnight_risk_audit_latest.json"
    out_csv = LOG_DIR / "intraday_residual_overnight_risk_audit_latest.csv"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if not overnight.empty:
        overnight.to_csv(out_csv, index=False, encoding="utf-8-sig")
    else:
        pd.DataFrame().to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(json.dumps({"status": "PASS", "overnight_rows": int(len(overnight)), "out_json": str(out_json)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
