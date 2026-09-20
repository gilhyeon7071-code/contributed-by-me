from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"
LIVE_FILLS = ROOT.parent / "vibe" / "buffett" / "data" / "live" / "live_fills.csv"
OUT_LATEST_JSON = LOG_DIR / "fill_survival_diagnostic_latest.json"
OUT_LATEST_CSV = LOG_DIR / "fill_survival_diagnostic_latest.csv"


def _now() -> datetime:
    return datetime.now()


def _norm_code(v: Any) -> str:
    digits = "".join(ch for ch in str(v or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _read_csv(path: Path, **kwargs: Any) -> pd.DataFrame:
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:
        return pd.DataFrame()


def _latest_order_files(max_files: int) -> List[Path]:
    files = sorted(PAPER_DIR.glob("orders_*_broker_submit_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[: max(1, int(max_files))]


def _load_orders(max_files: int) -> Tuple[pd.DataFrame, List[str]]:
    frames: List[pd.DataFrame] = []
    used: List[str] = []
    for path in _latest_order_files(max_files):
        df = _read_csv(path, dtype=str)
        if df.empty or ("order_id" not in df.columns and "entry_order_id" not in df.columns):
            continue
        df = df.copy()
        if "dispatch_ts" not in df.columns:
            df["dispatch_ts"] = ""
        df["dispatch_dt"] = pd.to_datetime(df["dispatch_ts"], errors="coerce")
        if "order_id" not in df.columns:
            df["order_id"] = ""
        df["order_id"] = df["order_id"].astype(str).str.strip()
        if "entry_order_id" in df.columns:
            df["entry_order_id"] = df["entry_order_id"].astype(str).str.strip()
        else:
            df["entry_order_id"] = ""
        df["join_order_id"] = df["entry_order_id"].where(df["entry_order_id"] != "", df["order_id"])
        df["code"] = df.get("code", "").map(_norm_code)
        df["side"] = df.get("side", "").astype(str).str.upper().str.strip()
        for col in ("qty", "price", "p_fill", "spread_bps", "imbalance", "order_score", "estimated_tc", "signed_markout"):
            if col not in df.columns:
                df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        if "order_type" not in df.columns:
            df["order_type"] = ""
        if "mode" not in df.columns:
            df["mode"] = ""
        if "precheck" not in df.columns:
            df["precheck"] = ""
        if "dispatch_status" not in df.columns:
            df["dispatch_status"] = ""
        frames.append(df)
        used.append(str(path))
    if not frames:
        return pd.DataFrame(), used
    out = pd.concat(frames, ignore_index=True)
    out = out[(out["join_order_id"] != "") & (out["dispatch_dt"].notna())]
    out = out.drop_duplicates(["join_order_id", "dispatch_dt"], keep="last")
    return out, used


def _load_fills() -> Tuple[pd.DataFrame, List[str]]:
    paths = [PAPER_DIR / "fills.csv", LIVE_FILLS]
    frames: List[pd.DataFrame] = []
    used: List[str] = []
    for path in paths:
        if not path.exists():
            continue
        df = _read_csv(path, dtype=str)
        if df.empty or "order_id" not in df.columns:
            continue
        df = df.copy()
        dt_col = "datetime" if "datetime" in df.columns else ("ts" if "ts" in df.columns else "")
        if not dt_col:
            continue
        df["fill_dt"] = pd.to_datetime(df[dt_col], errors="coerce")
        df["order_id"] = df["order_id"].astype(str).str.strip()
        df["code"] = df.get("code", "").map(_norm_code)
        df["side"] = df.get("side", "").astype(str).str.upper().str.strip()
        qty_col = "fill_qty" if "fill_qty" in df.columns else ("qty" if "qty" in df.columns else "")
        price_col = "fill_price" if "fill_price" in df.columns else ("price" if "price" in df.columns else "")
        df["fill_qty"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0.0) if qty_col else 0.0
        df["fill_price"] = pd.to_numeric(df[price_col], errors="coerce").fillna(0.0) if price_col else 0.0
        df = df[(df["order_id"] != "") & (df["fill_dt"].notna())]
        if df.empty:
            continue
        frames.append(df[["order_id", "fill_dt", "code", "side", "fill_qty", "fill_price"]])
        used.append(str(path))
    if not frames:
        return pd.DataFrame(), used
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values("fill_dt")
    return out, used


def _build_survival_rows(orders: pd.DataFrame, fills: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame()
    fill_groups: Dict[str, pd.DataFrame] = {}
    if not fills.empty:
        for oid, g in fills.rename(columns={"order_id": "join_order_id"}).groupby("join_order_id"):
            fill_groups[str(oid)] = g.sort_values("fill_dt").copy()
    first_fill_dt: List[Any] = []
    filled_qty: List[float] = []
    event_seconds: List[Any] = []
    for _, row in orders.iterrows():
        oid = str(row.get("join_order_id") or "").strip()
        dispatch_dt = row.get("dispatch_dt")
        g = fill_groups.get(oid, pd.DataFrame())
        if g.empty or pd.isna(dispatch_dt):
            first_fill_dt.append(pd.NaT)
            filled_qty.append(0.0)
            event_seconds.append(pd.NA)
            continue
        eligible = g[pd.to_datetime(g["fill_dt"], errors="coerce") >= dispatch_dt]
        if eligible.empty:
            first_fill_dt.append(pd.NaT)
            filled_qty.append(0.0)
            event_seconds.append(pd.NA)
            continue
        first_dt = pd.to_datetime(eligible["fill_dt"], errors="coerce").min()
        qty_sum = float(pd.to_numeric(eligible["fill_qty"], errors="coerce").fillna(0.0).sum())
        first_fill_dt.append(first_dt)
        filled_qty.append(qty_sum)
        event_seconds.append(float((first_dt - dispatch_dt).total_seconds()) if pd.notna(first_dt) else pd.NA)
    df = orders.copy()
    df["first_fill_dt"] = first_fill_dt
    df["filled_qty"] = filled_qty
    df["event_time_seconds"] = event_seconds
    qty = pd.to_numeric(df["qty"], errors="coerce").fillna(0.0)
    df["event_type"] = "censored"
    df.loc[(df["filled_qty"] > 0) & (df["filled_qty"] < qty), "event_type"] = "partial_fill"
    df.loc[(df["filled_qty"] > 0) & (df["filled_qty"] >= qty), "event_type"] = "full_fill"
    df["filled_any"] = df["event_type"].isin(["partial_fill", "full_fill"])
    for horizon in (30, 60, 300):
        df[f"fill_within_{horizon}s"] = df["filled_any"] & (pd.to_numeric(df["event_time_seconds"], errors="coerce") <= horizon)
    keep = [
        "dispatch_ts",
        "exec_date",
        "join_order_id",
        "order_id",
        "code",
        "side",
        "qty",
        "price",
        "order_type",
        "mode",
        "precheck",
        "dispatch_status",
        "p_fill",
        "spread_bps",
        "imbalance",
        "order_score",
        "estimated_tc",
        "signed_markout",
        "event_type",
        "event_time_seconds",
        "filled_qty",
        "filled_any",
        "fill_within_30s",
        "fill_within_60s",
        "fill_within_300s",
    ]
    for col in keep:
        if col not in df.columns:
            df[col] = ""
    return df[keep].copy()


def _bucket_pfill(v: Any) -> str:
    try:
        x = float(v)
    except Exception:
        x = 0.0
    if x <= 0:
        return "p00_missing"
    decile = min(9, max(0, int(x * 10)))
    return f"p{decile:02d}_{decile/10:.1f}-{(decile+1)/10:.1f}"


def _summary(rows: pd.DataFrame) -> Dict[str, Any]:
    if rows.empty:
        return {"rows": 0, "event_counts": {}, "horizon_fill_rate": {}}
    out: Dict[str, Any] = {
        "rows": int(len(rows)),
        "event_counts": {str(k): int(v) for k, v in rows["event_type"].value_counts(dropna=False).to_dict().items()},
        "horizon_fill_rate": {},
    }
    for horizon in (30, 60, 300):
        col = f"fill_within_{horizon}s"
        out["horizon_fill_rate"][col] = float(pd.Series(rows[col]).astype(bool).mean()) if col in rows.columns else 0.0
    if "p_fill" in rows.columns:
        work = rows.copy()
        work["p_fill_decile"] = work["p_fill"].map(_bucket_pfill)
        decile_rows: List[Dict[str, Any]] = []
        for bucket, g in work.groupby("p_fill_decile"):
            decile_rows.append(
                {
                    "bucket": str(bucket),
                    "n": int(len(g)),
                    "empirical_60s": float(g["fill_within_60s"].astype(bool).mean()),
                    "avg_p_fill": float(pd.to_numeric(g["p_fill"], errors="coerce").fillna(0.0).mean()),
                    "brier_60s": float(((g["fill_within_60s"].astype(float) - pd.to_numeric(g["p_fill"], errors="coerce").fillna(0.0)) ** 2).mean()),
                }
            )
        out["p_fill_decile_calibration"] = decile_rows
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Build fill survival/competing-risk diagnostic dataset.")
    ap.add_argument("--max-order-files", type=int, default=30)
    ap.add_argument("--min-prod-orders", type=int, default=100000)
    args = ap.parse_args()

    ts = _now()
    ymd = ts.strftime("%Y%m%d")
    out_json = LOG_DIR / f"fill_survival_diagnostic_{ymd}.json"
    out_csv = LOG_DIR / f"fill_survival_diagnostic_{ymd}.csv"

    orders, order_files = _load_orders(args.max_order_files)
    fills, fill_files = _load_fills()
    rows = _build_survival_rows(orders, fills)
    rows.to_csv(out_csv, index=False, encoding="utf-8-sig")
    rows.to_csv(OUT_LATEST_CSV, index=False, encoding="utf-8-sig")

    n = int(len(rows))
    filled = int(rows["filled_any"].sum()) if "filled_any" in rows.columns else 0
    status = "OK_DIAGNOSTIC" if n > 0 else "NO_ORDER_DATA"
    production_ready = bool(n >= int(args.min_prod_orders) and filled >= 100)
    reason = "diagnostic_only_sample_below_production_threshold"
    if n == 0:
        reason = "no_orders_with_dispatch_ts_and_order_id"
    elif filled == 0:
        reason = "no_matching_fills_by_order_id"

    payload = {
        "generated_at": ts.isoformat(timespec="seconds"),
        "status": status,
        "reason": reason,
        "method": "FILL_SURVIVAL_COMPETING_RISKS_DIAGNOSTIC",
        "operational_use": "diagnostic_only_no_order_replace_or_cancel",
        "production_ready": production_ready,
        "production_threshold": {
            "min_orders": int(args.min_prod_orders),
            "min_filled_events": 100,
            "calibration_ci_width_required": "<=5pct_not_evaluated_without_bootstrap",
        },
        "input": {
            "order_files": order_files,
            "fill_files": fill_files,
            "orders_loaded": int(len(orders)),
            "fills_loaded": int(len(fills)),
        },
        "dataset": _summary(rows),
        "required_missing_features": [
            name
            for name in ["queue_pct", "depth_ratio", "oddlot_frac", "cancel_rate_60s", "auction_flag"]
            if name not in rows.columns
        ],
        "available_execution_features": [
            name
            for name in ["p_fill", "spread_bps", "imbalance", "order_score", "estimated_tc", "signed_markout"]
            if name in rows.columns
        ],
        "output_csv": str(out_csv),
        "output_latest_csv": str(OUT_LATEST_CSV),
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
