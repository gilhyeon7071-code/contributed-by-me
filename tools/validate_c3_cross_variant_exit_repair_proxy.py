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

SOURCE_JSON = LOG_DIR / "report_entry_trigger_v2_transition_value2b_ret1_1_c3_cross_variant_validation_latest.json"
REFERENCE_PATH_CSV = LOG_DIR / "c3_like_exit_repair_broader_latest.csv"

DETAIL_CSV = LOG_DIR / "c3_cross_variant_exit_repair_proxy_latest.csv"
SUMMARY_CSV = LOG_DIR / "c3_cross_variant_exit_repair_proxy_summary_latest.csv"
OUT_JSON = LOG_DIR / "c3_cross_variant_exit_repair_proxy_latest.json"
OUT_MD = LOG_DIR / "c3_cross_variant_exit_repair_proxy_latest.md"

RUN_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
STATUS = "READ_ONLY_C3_CROSS_VARIANT_EXIT_REPAIR_PROXY_NOT_OPERATIONAL"
PATH_METHOD = "archive_entry_open_proxy"
TP_THRESHOLD_DECIMAL = 0.05
TP_THRESHOLD_PCT = TP_THRESHOLD_DECIMAL * 100.0
TP_FRACTION = 0.5


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _safe_float(value: Any, default: float = math.nan) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
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


def _calc_path_proxy(row: pd.Series, price_by_code: dict[str, pd.DataFrame]) -> dict[str, Any]:
    code = _norm_code(row.get("code"))
    entry_ymd = _parse_ymd(row.get("entry_date"))
    exit_ymd = _parse_ymd(row.get("exit_date"))
    part = price_by_code.get(code)
    if part is None or part.empty:
        return {
            "path_available": 0,
            "path_missing_reason": "code_not_found_in_archive_slice",
            "entry_px_proxy": math.nan,
            "path_rows": 0,
            "max_high_pct_to_exit_proxy": math.nan,
            "min_low_pct_to_exit_proxy": math.nan,
        }
    span = part[(part["date"] >= entry_ymd) & (part["date"] <= exit_ymd)]
    entry_row = part[part["date"] == entry_ymd]
    if span.empty:
        return {
            "path_available": 0,
            "path_missing_reason": "no_price_rows_between_entry_and_exit",
            "entry_px_proxy": math.nan,
            "path_rows": 0,
            "max_high_pct_to_exit_proxy": math.nan,
            "min_low_pct_to_exit_proxy": math.nan,
        }
    if entry_row.empty:
        return {
            "path_available": 0,
            "path_missing_reason": "entry_date_price_missing",
            "entry_px_proxy": math.nan,
            "path_rows": int(len(span)),
            "max_high_pct_to_exit_proxy": math.nan,
            "min_low_pct_to_exit_proxy": math.nan,
        }
    entry_open = _safe_float(entry_row.iloc[0].get("open"))
    if not math.isfinite(entry_open) or entry_open <= 0:
        return {
            "path_available": 0,
            "path_missing_reason": "entry_open_invalid",
            "entry_px_proxy": entry_open,
            "path_rows": int(len(span)),
            "max_high_pct_to_exit_proxy": math.nan,
            "min_low_pct_to_exit_proxy": math.nan,
        }
    high = pd.to_numeric(span["high"], errors="coerce").max()
    low = pd.to_numeric(span["low"], errors="coerce").min()
    return {
        "path_available": 1,
        "path_missing_reason": "",
        "entry_px_proxy": float(entry_open),
        "path_rows": int(len(span)),
        "max_high_pct_to_exit_proxy": float((high / entry_open - 1.0) * 100.0) if pd.notna(high) else math.nan,
        "min_low_pct_to_exit_proxy": float((low / entry_open - 1.0) * 100.0) if pd.notna(low) else math.nan,
    }


def _profit_then_stop_repair(row: pd.Series) -> tuple[float, str]:
    base_ret = _safe_float(row.get("baseline_ret"))
    max_high_pct = _safe_float(row.get("max_high_pct_to_exit_proxy"))
    exit_reason = str(row.get("exit_reason") or "").upper()
    if "STOP" in exit_reason and math.isfinite(max_high_pct) and max_high_pct >= TP_THRESHOLD_PCT:
        return TP_FRACTION * TP_THRESHOLD_DECIMAL + (1.0 - TP_FRACTION) * base_ret, "proxy_repair_hit_stop_after_5pct_high"
    return base_ret, "repair_not_applicable"


def _partial_tp_all(row: pd.Series) -> tuple[float, str]:
    base_ret = _safe_float(row.get("baseline_ret"))
    max_high_pct = _safe_float(row.get("max_high_pct_to_exit_proxy"))
    if math.isfinite(max_high_pct) and max_high_pct >= TP_THRESHOLD_PCT:
        return TP_FRACTION * TP_THRESHOLD_DECIMAL + (1.0 - TP_FRACTION) * base_ret, "proxy_partial_tp_hit"
    return base_ret, "proxy_partial_tp_not_hit"


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


def _add_tier_rows(df: pd.DataFrame) -> pd.DataFrame:
    tiers: list[pd.DataFrame] = []
    tier_rules = [
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
    for name, mask in tier_rules:
        part = df[mask].copy()
        if part.empty:
            continue
        part["tier"] = name
        tiers.append(part)
    if not tiers:
        return df.iloc[0:0].copy()
    return pd.concat(tiers, ignore_index=True)


def _compare_reference(detail_base: pd.DataFrame) -> dict[str, Any]:
    if not REFERENCE_PATH_CSV.exists():
        return {"available": False, "reason": "reference_csv_missing"}
    ref = pd.read_csv(REFERENCE_PATH_CSV, dtype={"code": str})
    if ref.empty or "max_high_pct_to_exit" not in ref.columns:
        return {"available": False, "reason": "reference_csv_empty_or_missing_column"}
    ref = ref[ref["variant"].astype(str).eq("baseline")].copy()
    if ref.empty:
        return {"available": False, "reason": "reference_baseline_rows_missing"}
    for col in ["code", "signal_date", "entry_date"]:
        ref[col] = ref[col].astype(str)
    ref["code"] = ref["code"].map(_norm_code)
    base = detail_base.copy()
    for col in ["code", "signal_date", "entry_date"]:
        base[col] = base[col].astype(str)
    base["code"] = base["code"].map(_norm_code)
    joined = ref.merge(
        base[["code", "signal_date", "entry_date", "max_high_pct_to_exit_proxy"]],
        on=["code", "signal_date", "entry_date"],
        how="inner",
    )
    if joined.empty:
        return {"available": False, "reason": "no_joined_reference_rows"}
    joined["abs_diff"] = (
        pd.to_numeric(joined["max_high_pct_to_exit"], errors="coerce")
        - pd.to_numeric(joined["max_high_pct_to_exit_proxy"], errors="coerce")
    ).abs()
    return {
        "available": True,
        "joined_rows": int(len(joined)),
        "mean_abs_diff_pct_point": round(float(joined["abs_diff"].mean()), 6),
        "median_abs_diff_pct_point": round(float(joined["abs_diff"].median()), 6),
        "max_abs_diff_pct_point": round(float(joined["abs_diff"].max()), 6),
        "note": "reference path used report-specific entry price; current output is archive entry-open proxy, not exact reproduction",
    }


def main() -> int:
    payload = _read_json(SOURCE_JSON)
    rows = payload.get("rows") or []
    if not rows:
        raise SystemExit(f"no rows in {SOURCE_JSON}")

    base = pd.DataFrame(rows)
    base["code"] = base["code"].map(_norm_code)
    base["entry_ymd"] = base["entry_date"].map(_parse_ymd)
    base["exit_ymd"] = base["exit_date"].map(_parse_ymd)
    base["baseline_ret"] = pd.to_numeric(base["ret"], errors="coerce").fillna(0.0)
    base["year"] = base["entry_ymd"].str.slice(0, 4)

    min_ymd = str(base["entry_ymd"].min())
    max_ymd = str(base["exit_ymd"].max())
    codes = set(base["code"].dropna().astype(str))
    price_df, loaded_files = _load_archive_price_frame(min_ymd, max_ymd, codes)
    duplicate_removed = int(price_df.attrs.get("duplicate_price_rows_removed", 0))
    price_by_code = _build_path_lookup(price_df)

    path_metrics = base.apply(lambda row: _calc_path_proxy(row, price_by_code), axis=1, result_type="expand")
    enriched = pd.concat([base.reset_index(drop=True), path_metrics.reset_index(drop=True)], axis=1)
    enriched["path_recalc_method"] = PATH_METHOD
    enriched["path_proxy_not_exact"] = 1
    enriched["operation_effect"] = "none"
    enriched["research_only"] = 1
    enriched["operational_candidate"] = 0

    detail_parts: list[pd.DataFrame] = []
    for variant_name, fn in [
        ("baseline", lambda row: (_safe_float(row.get("baseline_ret")), "baseline")),
        ("profit_then_stop_repair_proxy_5pct_50pct", _profit_then_stop_repair),
        ("partial_tp_5pct_50pct_all_proxy", _partial_tp_all),
    ]:
        part = enriched.copy()
        calc = part.apply(lambda row: fn(row), axis=1, result_type="expand")
        part["repair_variant"] = variant_name
        part["variant_ret"] = pd.to_numeric(calc[0], errors="coerce")
        part["variant_ret_pct"] = part["variant_ret"] * 100.0
        part["variant_reason"] = calc[1]
        detail_parts.append(part)

    detail = pd.concat(detail_parts, ignore_index=True)
    detail = detail.rename(columns={"variant": "source_variant"})

    detail_columns = [
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
        "path_recalc_method",
        "path_proxy_not_exact",
        "path_available",
        "path_missing_reason",
        "entry_px_proxy",
        "path_rows",
        "max_high_pct_to_exit_proxy",
        "min_low_pct_to_exit_proxy",
        "operation_effect",
        "research_only",
        "operational_candidate",
    ]
    detail = detail[[col for col in detail_columns if col in detail.columns]]
    detail.to_csv(DETAIL_CSV, index=False, encoding="utf-8-sig")

    summary_rows: list[dict[str, Any]] = []
    for sample_mode, sample in [
        ("with_cross_variant_duplicates", detail),
        (
            "dedup_trade_key",
            detail.sort_values(["source_variant", "code", "signal_date", "entry_date", "exit_date"])
            .drop_duplicates(["repair_variant", "code", "signal_date", "entry_date", "exit_date"], keep="first")
            .copy(),
        ),
    ]:
        tiered = _add_tier_rows(sample)
        if tiered.empty:
            continue
        group_cols = ["tier", "source_variant", "repair_variant"]
        for keys, group in tiered.groupby(group_cols, dropna=False):
            rec = {"sample_mode": sample_mode, "tier": keys[0], "source_variant": keys[1], "repair_variant": keys[2]}
            rec.update(_summarize(group))
            summary_rows.append(rec)
        for keys, group in tiered.groupby(["tier", "repair_variant"], dropna=False):
            rec = {"sample_mode": sample_mode, "tier": keys[0], "source_variant": "__ALL_SOURCE_VARIANTS__", "repair_variant": keys[1]}
            rec.update(_summarize(group))
            summary_rows.append(rec)

    summary = pd.DataFrame(summary_rows)
    if not summary.empty:
        key_cols = ["sample_mode", "tier", "source_variant", "repair_variant"]
        summary = summary.sort_values(key_cols).reset_index(drop=True)
    summary.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")

    baseline_detail = detail[detail["repair_variant"].eq("baseline")].copy()
    reference_compare = _compare_reference(baseline_detail)

    top_summary = summary[
        (summary["source_variant"].eq("__ALL_SOURCE_VARIANTS__"))
        & (summary["tier"].isin(["all_cross_rows", "exact_c3_after_recheck", "c3_like_score080_follow1_v080"]))
        & (summary["sample_mode"].eq("dedup_trade_key"))
    ].copy()

    metadata = {
        "created_at": RUN_TS,
        "status": STATUS,
        "source_file": str(SOURCE_JSON),
        "source_rows": int(len(base)),
        "detail_rows": int(len(detail)),
        "summary_rows": int(len(summary)),
        "price_source": "krx_daily_archive_only",
        "price_path_method": PATH_METHOD,
        "path_proxy_not_exact": True,
        "entry_price_caveat": "report_backtest uses trigger/open/slippage-specific entry price; this tool uses archive entry-day open as proxy",
        "loaded_archive_files": loaded_files,
        "archive_price_rows_loaded": int(len(price_df)),
        "archive_duplicate_price_rows_removed": duplicate_removed,
        "path_available_rows": int(enriched["path_available"].sum()),
        "path_missing_rows": int((enriched["path_available"] == 0).sum()),
        "reference_path_compare": reference_compare,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "interpretation_boundary": [
            "This validates a read-only exit-repair hypothesis on cross-variant rows.",
            "It does not approve trading, change gates, or replace the official backtest entry price.",
            "Because price path is proxy-based, use direction and sensitivity only; do not treat as exact PnL replication.",
        ],
        "output_files": {
            "detail_csv": str(DETAIL_CSV),
            "summary_csv": str(SUMMARY_CSV),
            "json": str(OUT_JSON),
            "markdown": str(OUT_MD),
        },
        "headline_summary": top_summary.to_dict(orient="records"),
    }
    OUT_JSON.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# C3 cross-variant exit repair proxy validation",
        "",
        f"- status: `{STATUS}`",
        f"- source rows: {len(base)}",
        f"- detail rows: {len(detail)}",
        f"- summary rows: {len(summary)}",
        f"- price path method: `{PATH_METHOD}`",
        f"- price source: `krx_daily_archive_only`",
        f"- path available rows: {int(enriched['path_available'].sum())}",
        f"- path missing rows: {int((enriched['path_available'] == 0).sum())}",
        "",
        "## Boundary",
        "",
        "- This is read-only research evidence.",
        "- It does not change candidate generation, backtest, HPO, paper, broker, gates, or policy.",
        "- Price path is an archive entry-open proxy, not exact report backtest reconstruction.",
        "",
        "## Dedup headline",
        "",
    ]
    if top_summary.empty:
        lines.append("- No headline rows.")
    else:
        for _, row in top_summary.iterrows():
            lines.append(
                "- "
                f"{row['tier']} / {row['repair_variant']}: "
                f"n={row['n']}, ret_sum={row['ret_sum']}, pf={row['profit_factor']}, "
                f"repair_hit_n={row['repair_hit_n']}, tp_hit_n={row['tp_hit_n']}"
            )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({
        "status": STATUS,
        "source_rows": int(len(base)),
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
