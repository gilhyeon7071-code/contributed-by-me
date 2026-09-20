from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
ARCHIVE_DIR = ROOT / "krx_daily_archive"
SCAN_DIR = LOG_DIR / "research_report_entry_trigger_v2_transition_value2b_ret1_1_relaxation_scope_scan_20260713_132200"

SOURCE_JSON = LOG_DIR / "report_entry_trigger_v2_transition_value2b_ret1_1_c3_cross_variant_validation_latest.json"
PROXY_SUMMARY_CSV = LOG_DIR / "c3_cross_variant_exit_repair_proxy_summary_latest.csv"

DETAIL_CSV = LOG_DIR / "c3_cross_variant_exit_repair_exact_entry_latest.csv"
SUMMARY_CSV = LOG_DIR / "c3_cross_variant_exit_repair_exact_entry_summary_latest.csv"
OUT_JSON = LOG_DIR / "c3_cross_variant_exit_repair_exact_entry_latest.json"
OUT_MD = LOG_DIR / "c3_cross_variant_exit_repair_exact_entry_latest.md"

STATUS = "READ_ONLY_C3_CROSS_VARIANT_EXIT_REPAIR_EXACT_ENTRY_NOT_OPERATIONAL"
PATH_METHOD = "report_trade_csv_entry_px_plus_archive_path"
TP_THRESHOLD_DECIMAL = 0.05
TP_THRESHOLD_PCT = TP_THRESHOLD_DECIMAL * 100.0
TP_FRACTION = 0.5


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _safe_float(value: Any, default: float = math.nan) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_ymd(value: Any) -> str:
    s = str(value).strip()
    if not s:
        return ""
    s = s[:10].replace("-", "")
    if len(s) != 8 or not s.isdigit():
        return ""
    return s


def _norm_iso_date(value: Any) -> str:
    ymd = _parse_ymd(value)
    if not ymd:
        return ""
    return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"


def _norm_code(value: Any) -> str:
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6)[-6:]


def _archive_file_range(path: Path) -> tuple[str, str] | None:
    m = re.search(r"krx_daily_(\d{8})_(\d{8})_clean\.parquet$", path.name)
    if not m:
        return None
    return m.group(1), m.group(2)


def _overlaps(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    return a_start <= b_end and b_start <= a_end


def _load_cross_rows() -> pd.DataFrame:
    payload = _read_json(SOURCE_JSON)
    rows = payload.get("rows") or []
    if not rows:
        raise SystemExit(f"no rows in {SOURCE_JSON}")
    df = pd.DataFrame(rows)
    df = df.rename(columns={"variant": "source_variant"})
    df["code"] = df["code"].map(_norm_code)
    df["signal_date"] = df["signal_date"].map(_norm_iso_date)
    df["entry_date"] = df["entry_date"].map(_norm_iso_date)
    df["exit_date"] = df["exit_date"].map(_norm_iso_date)
    df["entry_ymd"] = df["entry_date"].map(_parse_ymd)
    df["exit_ymd"] = df["exit_date"].map(_parse_ymd)
    df["baseline_ret"] = pd.to_numeric(df["ret"], errors="coerce").fillna(0.0)
    df["year"] = df["entry_ymd"].str.slice(0, 4)
    return df


def _load_trade_csvs() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(SCAN_DIR.glob("*_trades.csv")):
        variant = path.name.removesuffix("_trades.csv")
        df = pd.read_csv(path, dtype={"code": str})
        if df.empty:
            continue
        df["source_variant"] = variant
        df["code"] = df["code"].map(_norm_code)
        for col in ["signal_date", "entry_date", "exit_date"]:
            df[col] = df[col].map(_norm_iso_date)
        frames.append(df)
    if not frames:
        raise SystemExit(f"no trade csv files under {SCAN_DIR}")
    return pd.concat(frames, ignore_index=True)


def _load_archive_price_frame(min_ymd: str, max_ymd: str, codes: set[str]) -> tuple[pd.DataFrame, list[str]]:
    frames: list[pd.DataFrame] = []
    loaded_files: list[str] = []
    for path in sorted(ARCHIVE_DIR.glob("krx_daily_*_clean.parquet")):
        file_range = _archive_file_range(path)
        if not file_range:
            continue
        if not _overlaps(file_range[0], file_range[1], min_ymd, max_ymd):
            continue
        df = pd.read_parquet(path, columns=["date", "code", "open", "high", "low", "close"])
        df["date"] = df["date"].map(_parse_ymd)
        df["code"] = df["code"].map(_norm_code)
        df = df[(df["date"] >= min_ymd) & (df["date"] <= max_ymd) & (df["code"].isin(codes))].copy()
        if df.empty:
            continue
        df["_archive_file"] = path.name
        frames.append(df)
        loaded_files.append(path.name)
    if not frames:
        return pd.DataFrame(columns=["date", "code", "open", "high", "low", "close", "_archive_file"]), loaded_files
    out = pd.concat(frames, ignore_index=True)
    before = len(out)
    out = out.sort_values(["date", "code", "_archive_file"]).drop_duplicates(["date", "code"], keep="last")
    out.attrs["duplicate_price_rows_removed"] = before - len(out)
    for col in ["open", "high", "low", "close"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out, loaded_files


def _build_path_lookup(price_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if price_df.empty:
        return {}
    return {code: part.sort_values("date").reset_index(drop=True) for code, part in price_df.groupby("code", sort=False)}


def _calc_path(row: pd.Series, price_by_code: dict[str, pd.DataFrame]) -> dict[str, Any]:
    code = _norm_code(row.get("code"))
    entry_ymd = _parse_ymd(row.get("entry_date"))
    exit_ymd = _parse_ymd(row.get("exit_date"))
    entry_px = _safe_float(row.get("entry_px"))
    part = price_by_code.get(code)
    if not math.isfinite(entry_px) or entry_px <= 0:
        return {"path_available": 0, "path_missing_reason": "entry_px_missing_or_invalid", "path_rows": 0, "max_high_pct_to_exit": math.nan, "min_low_pct_to_exit": math.nan}
    if part is None or part.empty:
        return {"path_available": 0, "path_missing_reason": "code_not_found_in_archive_slice", "path_rows": 0, "max_high_pct_to_exit": math.nan, "min_low_pct_to_exit": math.nan}
    span = part[(part["date"] >= entry_ymd) & (part["date"] <= exit_ymd)]
    if span.empty:
        return {"path_available": 0, "path_missing_reason": "no_price_rows_between_entry_and_exit", "path_rows": 0, "max_high_pct_to_exit": math.nan, "min_low_pct_to_exit": math.nan}
    high = pd.to_numeric(span["high"], errors="coerce").max()
    low = pd.to_numeric(span["low"], errors="coerce").min()
    return {
        "path_available": 1,
        "path_missing_reason": "",
        "path_rows": int(len(span)),
        "max_high_pct_to_exit": float((high / entry_px - 1.0) * 100.0) if pd.notna(high) else math.nan,
        "min_low_pct_to_exit": float((low / entry_px - 1.0) * 100.0) if pd.notna(low) else math.nan,
    }


def _profit_then_stop_repair(row: pd.Series) -> tuple[float, str]:
    base_ret = _safe_float(row.get("baseline_ret"))
    max_high_pct = _safe_float(row.get("max_high_pct_to_exit"))
    exit_reason = str(row.get("exit_reason") or "").upper()
    if "STOP" in exit_reason and math.isfinite(max_high_pct) and max_high_pct >= TP_THRESHOLD_PCT:
        return TP_FRACTION * TP_THRESHOLD_DECIMAL + (1.0 - TP_FRACTION) * base_ret, "exact_entry_repair_hit_stop_after_5pct_high"
    return base_ret, "repair_not_applicable"


def _partial_tp_all(row: pd.Series) -> tuple[float, str]:
    base_ret = _safe_float(row.get("baseline_ret"))
    max_high_pct = _safe_float(row.get("max_high_pct_to_exit"))
    if math.isfinite(max_high_pct) and max_high_pct >= TP_THRESHOLD_PCT:
        return TP_FRACTION * TP_THRESHOLD_DECIMAL + (1.0 - TP_FRACTION) * base_ret, "exact_entry_partial_tp_hit"
    return base_ret, "exact_entry_partial_tp_not_hit"


def _add_tier_rows(df: pd.DataFrame) -> pd.DataFrame:
    tiers: list[pd.DataFrame] = []
    rules = [
        ("all_cross_rows", pd.Series(True, index=df.index)),
        ("exact_c3_after_recheck", pd.to_numeric(df["c3_after_recheck"], errors="coerce").fillna(0).eq(1)),
        ("c3_raw_market", pd.to_numeric(df["c3_raw_market"], errors="coerce").fillna(0).eq(1)),
        (
            "c3_like_score080_follow1_v080",
            (pd.to_numeric(df["score"], errors="coerce").fillna(0) >= 0.8)
            & (pd.to_numeric(df["followthrough_1d"], errors="coerce").fillna(0) == 1)
            & (pd.to_numeric(df["signal_v_accel"], errors="coerce").fillna(0) >= 0.8),
        ),
        (
            "c3_like_score050_follow1_v080",
            (pd.to_numeric(df["score"], errors="coerce").fillna(0) >= 0.5)
            & (pd.to_numeric(df["followthrough_1d"], errors="coerce").fillna(0) == 1)
            & (pd.to_numeric(df["signal_v_accel"], errors="coerce").fillna(0) >= 0.8),
        ),
        ("entry_2025_2026", df["entry_ymd"].between("20250101", "20261231")),
    ]
    for name, mask in rules:
        part = df[mask].copy()
        if part.empty:
            continue
        part["tier"] = name
        tiers.append(part)
    if not tiers:
        return df.iloc[0:0].copy()
    return pd.concat(tiers, ignore_index=True)


def _summarize(group: pd.DataFrame) -> dict[str, Any]:
    ret = pd.to_numeric(group["variant_ret"], errors="coerce").fillna(0.0)
    wins = ret[ret > 0]
    losses = ret[ret < 0]
    gross_profit = float(wins.sum())
    gross_loss_abs = float(-losses.sum())
    pf = gross_profit / gross_loss_abs if gross_loss_abs > 0 else (math.inf if gross_profit > 0 else math.nan)
    return {
        "n": int(len(group)),
        "win_n": int((ret > 0).sum()),
        "loss_n": int((ret < 0).sum()),
        "win_rate": round(float((ret > 0).mean()), 6) if len(group) else math.nan,
        "ret_sum": round(float(ret.sum()), 6),
        "ret_mean": round(float(ret.mean()), 6) if len(group) else math.nan,
        "profit_factor": round(pf, 6) if math.isfinite(pf) else "inf",
        "repair_hit_n": int((group["variant_reason"].astype(str).str.contains("repair_hit", na=False)).sum()),
        "tp_hit_n": int((group["variant_reason"].astype(str).str.contains("partial_tp_hit", na=False)).sum()),
        "path_available_n": int(pd.to_numeric(group["path_available"], errors="coerce").fillna(0).sum()),
    }


def _build_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    dedup = (
        detail.sort_values(["source_variant", "code", "signal_date", "entry_date", "exit_date"])
        .drop_duplicates(["repair_variant", "code", "signal_date", "entry_date", "exit_date"], keep="first")
        .copy()
    )
    for sample_mode, sample in [("with_cross_variant_duplicates", detail), ("dedup_trade_key", dedup)]:
        tiered = _add_tier_rows(sample)
        for keys, group in tiered.groupby(["tier", "source_variant", "repair_variant"], dropna=False):
            rec = {"sample_mode": sample_mode, "tier": keys[0], "source_variant": keys[1], "repair_variant": keys[2]}
            rec.update(_summarize(group))
            rows.append(rec)
        for keys, group in tiered.groupby(["tier", "repair_variant"], dropna=False):
            rec = {"sample_mode": sample_mode, "tier": keys[0], "source_variant": "__ALL_SOURCE_VARIANTS__", "repair_variant": keys[1]}
            rec.update(_summarize(group))
            rows.append(rec)
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["sample_mode", "tier", "source_variant", "repair_variant"]).reset_index(drop=True)
    return out


def _compare_proxy(summary: pd.DataFrame) -> dict[str, Any]:
    if not PROXY_SUMMARY_CSV.exists():
        return {"available": False, "reason": "proxy_summary_missing"}
    proxy = pd.read_csv(PROXY_SUMMARY_CSV)
    exact = summary.copy()
    exact["repair_compare_key"] = exact["repair_variant"].replace(
        {
            "profit_then_stop_repair_exact_entry_5pct_50pct": "profit_then_stop_repair_5pct_50pct",
            "partial_tp_5pct_50pct_all_exact_entry": "partial_tp_5pct_50pct_all",
        }
    )
    proxy = proxy.copy()
    proxy["repair_compare_key"] = proxy["repair_variant"].replace(
        {
            "profit_then_stop_repair_proxy_5pct_50pct": "profit_then_stop_repair_5pct_50pct",
            "partial_tp_5pct_50pct_all_proxy": "partial_tp_5pct_50pct_all",
        }
    )
    keys = ["sample_mode", "tier", "source_variant", "repair_compare_key"]
    joined = exact.merge(proxy, on=keys, how="inner", suffixes=("_exact_entry", "_proxy"))
    if joined.empty:
        return {"available": False, "reason": "no_joined_summary_rows"}
    joined["ret_sum_diff"] = pd.to_numeric(joined["ret_sum_exact_entry"], errors="coerce") - pd.to_numeric(joined["ret_sum_proxy"], errors="coerce")
    focus = joined[
        joined["sample_mode"].eq("dedup_trade_key")
        & joined["source_variant"].eq("__ALL_SOURCE_VARIANTS__")
        & joined["tier"].isin(["all_cross_rows", "exact_c3_after_recheck", "c3_like_score080_follow1_v080"])
    ].copy()
    return {
        "available": True,
        "joined_summary_rows": int(len(joined)),
        "focus_rows": focus[[
            "tier",
            "repair_compare_key",
            "repair_variant_exact_entry",
            "repair_variant_proxy",
            "ret_sum_exact_entry",
            "ret_sum_proxy",
            "ret_sum_diff",
            "repair_hit_n_exact_entry",
            "repair_hit_n_proxy",
        ]].to_dict(orient="records"),
    }


def main() -> int:
    cross = _load_cross_rows()
    trades = _load_trade_csvs()
    join_keys = ["source_variant", "code", "signal_date", "entry_date", "exit_date"]
    trade_cols = join_keys + [
        "entry_px",
        "exit_px",
        "entry_trigger_type",
        "entry_trigger_px",
        "entry_signal_basis_px",
        "entry_trigger_reason",
        "entry_trigger_window_days",
        "entry_day_open_to_close_ret",
        "entry_day_high_from_open_pct",
        "entry_day_low_from_open_pct",
        "mfe_1d",
        "mae_1d",
    ]
    trades_small = trades[[col for col in trade_cols if col in trades.columns]].copy()
    before_join = len(cross)
    joined = cross.merge(trades_small, on=join_keys, how="left", validate="many_to_one", indicator=True)
    matched_rows = int(joined["_merge"].eq("both").sum())
    missing_trade_rows = int(joined["_merge"].ne("both").sum())
    joined = joined.drop(columns=["_merge"])
    joined["entry_px"] = pd.to_numeric(joined["entry_px"], errors="coerce")

    min_ymd = str(joined["entry_ymd"].min())
    max_ymd = str(joined["exit_ymd"].max())
    price_df, loaded_files = _load_archive_price_frame(min_ymd, max_ymd, set(joined["code"].dropna().astype(str)))
    duplicate_removed = int(price_df.attrs.get("duplicate_price_rows_removed", 0))
    price_by_code = _build_path_lookup(price_df)

    path_metrics = joined.apply(lambda row: _calc_path(row, price_by_code), axis=1, result_type="expand")
    enriched = pd.concat([joined.reset_index(drop=True), path_metrics.reset_index(drop=True)], axis=1)
    enriched["path_recalc_method"] = PATH_METHOD
    enriched["path_exactness"] = "exact_report_trade_csv_entry_px_with_archive_high_low_path"
    enriched["operation_effect"] = "none"
    enriched["research_only"] = 1
    enriched["operational_candidate"] = 0

    detail_parts: list[pd.DataFrame] = []
    for repair_variant, fn in [
        ("baseline", lambda row: (_safe_float(row.get("baseline_ret")), "baseline")),
        ("profit_then_stop_repair_exact_entry_5pct_50pct", _profit_then_stop_repair),
        ("partial_tp_5pct_50pct_all_exact_entry", _partial_tp_all),
    ]:
        part = enriched.copy()
        calc = part.apply(lambda row: fn(row), axis=1, result_type="expand")
        part["repair_variant"] = repair_variant
        part["variant_ret"] = pd.to_numeric(calc[0], errors="coerce")
        part["variant_ret_pct"] = part["variant_ret"] * 100.0
        part["variant_reason"] = calc[1]
        detail_parts.append(part)
    detail = pd.concat(detail_parts, ignore_index=True)

    detail_cols = [
        "source_variant",
        "repair_variant",
        "variant_reason",
        "code",
        "signal_date",
        "entry_date",
        "entry_ymd",
        "exit_date",
        "exit_ymd",
        "year",
        "market_regime",
        "market_raw",
        "market_after_recheck",
        "setup_family",
        "score",
        "followthrough_1d",
        "signal_v_accel",
        "signal_rs",
        "signal_ret1_pct",
        "entry_gap_pct",
        "entry_day_open_to_close_pct",
        "c3_raw_market",
        "c3_after_recheck",
        "exit_reason",
        "baseline_ret",
        "variant_ret",
        "variant_ret_pct",
        "entry_px",
        "exit_px",
        "entry_trigger_type",
        "entry_trigger_px",
        "entry_signal_basis_px",
        "entry_trigger_reason",
        "entry_trigger_window_days",
        "entry_day_open_to_close_ret",
        "entry_day_high_from_open_pct",
        "entry_day_low_from_open_pct",
        "mfe_1d",
        "mae_1d",
        "path_recalc_method",
        "path_exactness",
        "path_available",
        "path_missing_reason",
        "path_rows",
        "max_high_pct_to_exit",
        "min_low_pct_to_exit",
        "operation_effect",
        "research_only",
        "operational_candidate",
    ]
    detail = detail[[col for col in detail_cols if col in detail.columns]]
    detail.to_csv(DETAIL_CSV, index=False, encoding="utf-8-sig")

    summary = _build_summary(detail)
    summary.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    proxy_compare = _compare_proxy(summary)

    headline = summary[
        summary["sample_mode"].eq("dedup_trade_key")
        & summary["source_variant"].eq("__ALL_SOURCE_VARIANTS__")
        & summary["tier"].isin(["all_cross_rows", "exact_c3_after_recheck", "c3_like_score080_follow1_v080"])
    ].copy()

    metadata = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": STATUS,
        "source_file": str(SOURCE_JSON),
        "source_scan_dir": str(SCAN_DIR),
        "source_rows": int(before_join),
        "trade_csv_rows": int(len(trades)),
        "matched_trade_rows": matched_rows,
        "missing_trade_rows": missing_trade_rows,
        "detail_rows": int(len(detail)),
        "summary_rows": int(len(summary)),
        "price_source": "krx_daily_archive_only_for_high_low_path",
        "price_path_method": PATH_METHOD,
        "path_exactness": "entry_px is exact from report trade csv; high/low path is recalculated from current archive files",
        "loaded_archive_files": loaded_files,
        "archive_price_rows_loaded": int(len(price_df)),
        "archive_duplicate_price_rows_removed": duplicate_removed,
        "path_available_rows": int(enriched["path_available"].sum()),
        "path_missing_rows": int((enriched["path_available"] == 0).sum()),
        "proxy_summary_compare": proxy_compare,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "interpretation_boundary": [
            "This is a read-only exact-entry replay using entry_px already emitted by the report trade CSV files.",
            "It is stronger than archive entry-open proxy, but high/low path is still recalculated from current archive parquet.",
            "It does not approve trading, change gates, or modify candidate/order flow.",
        ],
        "output_files": {
            "detail_csv": str(DETAIL_CSV),
            "summary_csv": str(SUMMARY_CSV),
            "json": str(OUT_JSON),
            "markdown": str(OUT_MD),
        },
        "headline_summary": headline.to_dict(orient="records"),
    }
    OUT_JSON.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# C3 cross-variant exit repair exact-entry validation",
        "",
        f"- status: `{STATUS}`",
        f"- source rows: {before_join}",
        f"- matched trade rows: {matched_rows}",
        f"- missing trade rows: {missing_trade_rows}",
        f"- detail rows: {len(detail)}",
        f"- summary rows: {len(summary)}",
        f"- price path method: `{PATH_METHOD}`",
        "",
        "## Boundary",
        "",
        "- Read-only research evidence only.",
        "- Entry price comes from the report trade CSV output.",
        "- High/low path is recalculated from current archive parquet.",
        "- Candidate generation, backtest, HPO, paper, broker, gates, and policy are unchanged.",
        "",
        "## Dedup headline",
        "",
    ]
    if headline.empty:
        lines.append("- No headline rows.")
    else:
        for _, row in headline.iterrows():
            lines.append(
                "- "
                f"{row['tier']} / {row['repair_variant']}: "
                f"n={row['n']}, ret_sum={row['ret_sum']}, pf={row['profit_factor']}, "
                f"repair_hit_n={row['repair_hit_n']}, tp_hit_n={row['tp_hit_n']}"
            )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({
        "status": STATUS,
        "source_rows": int(before_join),
        "matched_trade_rows": matched_rows,
        "missing_trade_rows": missing_trade_rows,
        "detail_rows": int(len(detail)),
        "summary_rows": int(len(summary)),
        "path_available_rows": int(enriched["path_available"].sum()),
        "path_missing_rows": int((enriched["path_available"] == 0).sum()),
        "summary_csv": str(SUMMARY_CSV),
        "json": str(OUT_JSON),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
