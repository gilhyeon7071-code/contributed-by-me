from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import surge_lob_ingest as lob_ingest  # noqa: E402


LOG_DIR = ROOT / "2_Logs"
INPUT_CSV = LOG_DIR / "surge_probe_review_cohort_diagnostic_latest.csv"
INTRADAY_PRICES = LOG_DIR / "intraday_prices_latest.csv"
OUT_JSON = LOG_DIR / "surge_probe_review_lob_quality_latest.json"
OUT_CSV = LOG_DIR / "surge_probe_review_lob_quality_latest.csv"
OUT_COLUMNS = [
    "code",
    "name",
    "state",
    "precursor_stage",
    "intraday_stage",
    "ret_10m_pct",
    "outcome_status_10m",
    "risk_tags",
    "risk_bucket",
    "suggested_review_action",
    "current_price",
    "lob_quality_class",
    "lob_quality_reason",
    "paper_order_route",
    "broker_order_route",
    "dispatch_enabled",
    "trading_allowed",
    "policy_change",
]
REQUIRED_INPUT_COLUMNS = [
    "code",
    "suggested_review_action",
]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _missing_columns(df: pd.DataFrame, columns: list[str]) -> list[str]:
    existing = {str(col) for col in df.columns}
    return [col for col in columns if col not in existing]


def _code_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)


def _build_lob_metrics(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["current_price"] + lob_ingest._hoga_numeric_columns():
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    spread = (df["ask1"] - df["bid1"]).clip(lower=0.0)
    mid = ((df["ask1"] + df["bid1"]) / 2.0).where(
        (df["ask1"] > 0) & (df["bid1"] > 0),
        df["current_price"].clip(lower=0.0),
    )
    df["mid_price"] = mid.fillna(0.0)
    df["spread"] = spread
    df["spread_bps"] = (spread / mid.replace(0, float("nan")) * 10000.0).fillna(0.0)
    denom = (df["bidq1"] + df["askq1"]).replace(0, float("nan"))
    df["order_imbalance_l1"] = ((df["bidq1"] - df["askq1"]) / denom).fillna(0.0)
    ask_depth = pd.Series(0, index=df.index, dtype="int64")
    for level in range(1, lob_ingest.HOGA_LEVELS + 1):
        ask_depth += ((df[f"ask{level}"] > 0) & (df[f"askq{level}"] > 0)).astype(int)
    df["ask_depth_levels_live"] = ask_depth
    lob_mask = (df["ask1"] > 0) & (df["bid1"] > 0)
    bid_only_limit_mask = (
        (df["ask1"] <= 0)
        & (df["bid1"] > 0)
        & (df["current_price"] > 0)
        & (df["bid1"] >= df["current_price"])
    )
    df["lob_available_live"] = lob_mask | bid_only_limit_mask
    df["lob_status_live"] = "NO_LOB"
    df.loc[lob_mask, "lob_status_live"] = "OK"
    df.loc[bid_only_limit_mask, "lob_status_live"] = "BID_ONLY_LIMIT"
    return df


def _quality_class(row: pd.Series) -> tuple[str, str]:
    status = str(row.get("hoga_fetch_status") or "").upper()
    lob_status = str(row.get("lob_status_live") or "").upper()
    spread_bps = float(row.get("spread_bps") or 0.0)
    ask_depth = float(row.get("ask_depth_levels_live") or 0.0)
    if status in {"ERROR", "CLIENT_ERROR"}:
        return "LOB_FETCH_ERROR", str(row.get("hoga_fetch_error") or status)
    if lob_status == "BID_ONLY_LIMIT":
        return "BID_ONLY_LIMIT_REVIEW", "bid-only limit state; fill quality uncertain"
    if lob_status != "OK":
        return "LOB_NOT_AVAILABLE", f"lob_status={lob_status or 'UNKNOWN'}"
    if spread_bps > 20:
        return "LOB_WIDE_SPREAD", f"spread_bps={spread_bps:.2f} > 20"
    if ask_depth < 3:
        return "LOB_SHALLOW_DEPTH", f"ask_depth_levels={ask_depth:.0f} < 3"
    if spread_bps <= 15 and ask_depth >= 3:
        return "LOB_OK_REVIEWABLE", "spread <= 15bps and ask depth >= 3"
    return "LOB_CAUTION_REVIEW", "LOB available but not clean"


def _to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(df.where(pd.notna(df), None).to_json(orient="records", force_ascii=False))


def _group(df: pd.DataFrame, keys: list[str]) -> list[dict[str, Any]]:
    required = set(keys) | {"code", "ret_10m_pct", "spread_bps", "ask_depth_levels_live", "outcome_status_10m"}
    if df.empty or any(col not in df.columns for col in required):
        return []
    out = (
        df.groupby(keys, dropna=False)
        .agg(
            rows=("code", "count"),
            codes=("code", "nunique"),
            avg_ret_10m_pct=("ret_10m_pct", "mean"),
            avg_spread_bps=("spread_bps", "mean"),
            avg_ask_depth=("ask_depth_levels_live", "mean"),
            positive=("outcome_status_10m", lambda x: int((x == "POSITIVE_MARKOUT").sum())),
            negative=("outcome_status_10m", lambda x: int((x == "NEGATIVE_MARKOUT").sum())),
            flat=("outcome_status_10m", lambda x: int((x == "FLAT_MARKOUT").sum())),
        )
        .reset_index()
        .sort_values(["rows", "avg_ret_10m_pct"], ascending=[False, False])
    )
    return _to_records(out)


def build_report(input_csv: Path, max_fetch: int) -> tuple[dict[str, Any], pd.DataFrame]:
    src = _read_csv(input_csv)
    if src.empty:
        return {"status": "NO_INPUT_ROWS", "input_csv": str(input_csv)}, pd.DataFrame()
    missing = _missing_columns(src, REQUIRED_INPUT_COLUMNS)
    if missing:
        return {
            "status": "NO_REQUIRED_COLUMNS",
            "input_csv": str(input_csv),
            "missing_columns": missing,
            "available_columns": [str(col) for col in src.columns],
            "trading_allowed": False,
            "dispatch_enabled": False,
            "policy_change": False,
        }, pd.DataFrame()
    src["code"] = _code_series(src["code"])
    work = src[src["suggested_review_action"].astype(str).eq("REVIEW_LOB_QUALITY_FIRST")].copy()
    if work.empty:
        return {
            "status": "NO_REVIEW_LOB_QUALITY_FIRST_ROWS",
            "input_csv": str(input_csv),
            "trading_allowed": False,
            "dispatch_enabled": False,
            "policy_change": False,
        }, pd.DataFrame()

    prices = _read_csv(INTRADAY_PRICES)
    if not prices.empty and "code" in prices.columns:
        prices["code"] = _code_series(prices["code"])
        keep = [col for col in ["code", "current_price", "open", "high", "low", "volume"] if col in prices.columns]
        work = work.merge(prices[keep].drop_duplicates("code", keep="last"), on="code", how="left", suffixes=("", "_latest"))
    if "current_price" not in work.columns:
        work["current_price"] = 0.0

    for col in lob_ingest._hoga_numeric_columns():
        if col not in work.columns:
            work[col] = 0.0
    work["hoga_fetch_status"] = ""
    work["hoga_fetch_error"] = ""

    prev_max_fetch = os.environ.get("SURGE_LOB_HOGA_MAX_FETCH")
    if max_fetch > 0:
        os.environ["SURGE_LOB_HOGA_MAX_FETCH"] = str(max_fetch)
    try:
        work = lob_ingest._fill_hoga_from_ws(work)
        work = lob_ingest._fill_hoga_from_kis(work)
    finally:
        if prev_max_fetch is None:
            os.environ.pop("SURGE_LOB_HOGA_MAX_FETCH", None)
        else:
            os.environ["SURGE_LOB_HOGA_MAX_FETCH"] = prev_max_fetch

    work = _build_lob_metrics(work)
    classified = work.apply(_quality_class, axis=1, result_type="expand")
    work["lob_quality_class"] = classified[0]
    work["lob_quality_reason"] = classified[1]
    work["paper_order_route"] = False
    work["broker_order_route"] = False
    work["dispatch_enabled"] = False
    work["trading_allowed"] = False
    work["policy_change"] = False

    keep_cols = [
        "code",
        "name",
        "state",
        "precursor_stage",
        "intraday_stage",
        "ret_10m_pct",
        "outcome_status_10m",
        "risk_tags",
        "risk_bucket",
        "suggested_review_action",
        "current_price",
        "hoga_fetch_status",
        "hoga_fetch_error",
        "lob_status_live",
        "lob_available_live",
        "lob_quality_class",
        "lob_quality_reason",
        "spread_bps",
        "order_imbalance_l1",
        "ask_depth_levels_live",
        "ask1",
        "bid1",
        "askq1",
        "bidq1",
        "paper_order_route",
        "broker_order_route",
        "dispatch_enabled",
        "trading_allowed",
        "policy_change",
    ]
    rows = work[[col for col in keep_cols if col in work.columns]].copy()
    rows = rows.sort_values(["lob_quality_class", "ret_10m_pct"], ascending=[True, False])
    report = {
        "status": "PASS",
        "scope": "surge_probe_review_lob_quality",
        "input_csv": str(input_csv),
        "rows": int(len(rows)),
        "codes": int(rows["code"].nunique()),
        "max_fetch": int(max_fetch),
        "trading_allowed": False,
        "dispatch_enabled": False,
        "policy_change": False,
        "summary_by_lob_quality": _group(rows, ["lob_quality_class"]),
        "summary_by_fetch_status": _group(rows, ["hoga_fetch_status"]),
        "rows_preview": _to_records(rows),
    }
    return report, rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", default=str(INPUT_CSV))
    parser.add_argument("--output-json", default=str(OUT_JSON))
    parser.add_argument("--output-csv", default=str(OUT_CSV))
    parser.add_argument("--max-fetch", type=int, default=7)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    report, rows = build_report(Path(args.input_csv), max_fetch=int(args.max_fetch))
    if args.dry_run:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if not rows.empty:
            print(rows.to_string(index=False))
        return 0 if report.get("status") == "PASS" else 2

    out_json = Path(args.output_json)
    out_csv = Path(args.output_csv)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if rows.empty:
        rows = pd.DataFrame(columns=OUT_COLUMNS)
    rows.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(
        "[SURGE_PROBE_REVIEW_LOB_QUALITY] "
        f"status={report.get('status')} rows={report.get('rows', 0)} json={out_json} csv={out_csv}"
    )
    return 0 if str(report.get("status") or "").startswith("NO_") or report.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
