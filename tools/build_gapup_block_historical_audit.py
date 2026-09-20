"""Read-only historical audit of the live GAPUP_BLOCK filter (paper_engine.py).

Scope (descriptive audit, not a new candidate-generation methodology round):
this reuses the exact NORMAL candidate-selection condition already shipped in
generate_candidates_v41_1.py:_select_candidates() (rs/v_accel/stretch/value/atr/
rsi/vol_close_corr/high_52w_gap/listing_days, DEFAULT_PARAMS thresholds) and the
exact GAPUP_BLOCK formula from paper_engine.py (gap = entry_open/prev_close - 1,
blocked if gap > gap_up_max_pct=0.03) against several years of already-existing
price history. No new rule, threshold, or candidate-generation logic is
introduced; this measures the historical footprint of logic that is already
live. Does not touch orders/fills/ledger/Gate/LOCK/candidate-generation/paper
runtime in any way.

NORMAL_INTRADAY_MOMENTUM_BLOCK is out of scope here: it requires intraday
trading-value data only backfilled for ~40 recent days (see PLANS.md
2026-07-21/2026-07-23), so it cannot be audited over a multi-year window and
is deferred to a separate, shorter-window script.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
REPORT_PATH = ROOT / "report_backtest_v41_1.py"
GENERATOR_PATH = ROOT / "generate_candidates_v41_1.py"
MEMBERSHIP_PATH = ROOT / "_cache" / "krx_population_static_membership_latest.parquet"
OUT_PREFIX = "gapup_block_historical_audit_20260723"

# Exact thresholds copied from generate_candidates_v41_1.py DEFAULT_PARAMS /
# _select_candidates() — not re-derived or tuned here.
SELECT_PARAMS = {
    "rs_lim": 1.70,
    "v_accel_lim": 2.50,
    "stretch_max": 1.19,
    "value_min": 1_000_000_000.0,
    "atr_max": 0.12,
    "rsi_max": 70.0,
    "vol_close_corr_min": 0.0,
    "near_52w_high_gap_max": 0.05,
    "min_listing_days": 126.0,
}
# Exact threshold copied from paper_engine.py config default (no NORMAL/BEAR
# override found for this key; RALLY/CRASH-specific overrides exist but are
# out of scope since this audits the NORMAL/default path).
GAP_UP_MAX_PCT = 0.03
# [2026-09-10] 라이브 설정에 맞춰 교정. 이 상수들은 **편도**이고
#   왕복 = 2*FEE + 2*SLIPPAGE + SELL_TAX 로 쓰인다.
#   종전 0.005 는 왕복 1.0~1.4% 로 실제의 2.5~3.5배였다(BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2 + 0.001*2 + 0.002 = 0.00400 (optimize_params_v41_1.DEFAULT_FEE 와 동일).
#   **이 파일의 과거 산출물은 더 비싼 세계의 값이라 새 실행과 직접 비교할 수 없다.**

FEE = 0.0
SLIPPAGE = 0.001
SELL_TAX = 0.002
HORIZONS = (1, 2, 5)
PARTITIONS = (
    ("TRAIN_2020_2023", "2020-01-02", "2023-12-29"),
    ("VALIDATION_2024", "2024-01-01", "2024-12-31"),
    ("ASSESSMENT_2025", "2025-01-01", "2025-12-31"),
    ("RECENT_2026_H1", "2026-01-01", "2026-06-30"),
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def build_execution_panel(factors: pd.DataFrame) -> pd.DataFrame:
    work = factors.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["code"] = work["code"].astype(str).str.zfill(6)
    entity_key = "price_history_key" if "price_history_key" in work.columns else "code"
    work["_entity_key"] = work[entity_key].astype(str)
    work = work.sort_values(["_entity_key", "date"], kind="mergesort").reset_index(drop=True)
    calendar = pd.DatetimeIndex(sorted(work["date"].dropna().unique()))
    calendar_pos = {day: index for index, day in enumerate(calendar)}
    work["_global_index"] = work["date"].map(calendar_pos)
    for horizon in HORIZONS:
        exit_date_column = f"exit_date_h{horizon}"
        exit_close_column = f"exit_close_h{horizon}"
        net_column = f"net_return_h{horizon}"
        work[exit_date_column] = work["_global_index"].map(
            lambda index: calendar[int(index) + horizon]
            if pd.notna(index) and int(index) + horizon < len(calendar)
            else pd.NaT
        )
        lookup = work[["_entity_key", "date", "close"]].rename(
            columns={"date": exit_date_column, "close": exit_close_column}
        )
        work = work.merge(lookup, on=["_entity_key", exit_date_column], how="left", validate="many_to_one")
    work["entry_date"] = work["_global_index"].map(
        lambda index: calendar[int(index) + 1]
        if pd.notna(index) and int(index) + 1 < len(calendar)
        else pd.NaT
    )
    entry_lookup = work[["_entity_key", "date", "open"]].rename(columns={"date": "entry_date", "open": "entry_open"})
    work = work.merge(entry_lookup, on=["_entity_key", "entry_date"], how="left", validate="many_to_one")
    # GAPUP_BLOCK gap: paper_engine.py compares next-session entry price to the
    # signal day's close (prev_close), identical to this join.
    work["gap_up_pct"] = (work["entry_open"] / (work["close"] + 1e-9)) - 1.0
    for horizon in HORIZONS:
        exit_close_column = f"exit_close_h{horizon}"
        net_column = f"net_return_h{horizon}"
        valid = work["entry_open"].gt(0) & work[exit_close_column].gt(0)
        work[net_column] = (
            (work[exit_close_column] * (1.0 - FEE - SLIPPAGE - SELL_TAX))
            / (work["entry_open"] * (1.0 + FEE + SLIPPAGE))
            - 1.0
        ).where(valid)
    return work


def build_research_universe(panel: pd.DataFrame) -> pd.DataFrame:
    membership = pd.read_parquet(MEMBERSHIP_PATH, columns=["as_of_date", "code", "market", "security_type"])
    membership["date"] = pd.to_datetime(membership["as_of_date"].astype(str), format="%Y%m%d", errors="coerce")
    membership["code"] = membership["code"].astype(str).str.zfill(6)
    membership["market"] = membership["market"].fillna("").astype(str).str.upper().str.strip()
    membership = membership.loc[
        membership["market"].isin(["KOSPI", "KOSDAQ"])
        & membership["security_type"].fillna("").astype(str).str.upper().eq("COMMON"),
        ["date", "code", "market"],
    ].drop_duplicates(["date", "code"])
    if membership.duplicated(["date", "code"]).any():
        raise RuntimeError("duplicate point-in-time membership key")
    merged = panel.merge(membership, on=["date", "code"], how="inner", validate="many_to_one", suffixes=("", "_membership"))
    return merged.loc[merged["entry_open"].gt(0)].copy()


def select_candidates(universe: pd.DataFrame) -> pd.DataFrame:
    p = SELECT_PARAMS
    cond = (
        (universe["rs"] > p["rs_lim"])
        & (universe["v_accel"] > p["v_accel_lim"])
        & (universe["stretch"] < p["stretch_max"])
        & (universe["value"] > p["value_min"])
        & (universe["atr_pct"] < p["atr_max"])
        & (universe["rsi14"] < p["rsi_max"])
        & (universe["vol_close_corr20"] >= p["vol_close_corr_min"])
        & (universe["high_52w_gap"] <= p["near_52w_high_gap_max"])
        & (universe["listing_days"] >= p["min_listing_days"])
    )
    pool = universe.loc[cond].copy()
    pool["gapup_blocked"] = pool["gap_up_pct"].gt(GAP_UP_MAX_PCT)
    return pool


def summarize_partition(candidates: pd.DataFrame, label: str, start: str, end: str) -> list[dict[str, object]]:
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    part = candidates.loc[candidates["date"].between(start_ts, end_ts)].copy()
    records: list[dict[str, object]] = []
    for horizon in HORIZONS:
        net_column = f"net_return_h{horizon}"
        for group_label, group_mask in (("BLOCKED", part["gapup_blocked"]), ("PASSED", ~part["gapup_blocked"])):
            group = part.loc[group_mask & part[net_column].notna(), ["date", net_column]].copy()
            if group.empty:
                records.append({
                    "partition": label, "horizon": f"h{horizon}", "group": group_label,
                    "candidate_rows": 0, "signal_dates": 0,
                    "mean_daily_net_bps": None, "positive_daily_net_rate": None,
                    "status": "DEFERRED_INSUFFICIENT_SAMPLE",
                })
                continue
            daily = group.groupby("date", as_index=False)[net_column].mean()
            records.append({
                "partition": label, "horizon": f"h{horizon}", "group": group_label,
                "candidate_rows": int(len(group)),
                "signal_dates": int(len(daily)),
                "mean_daily_net_bps": round(float(daily[net_column].mean() * 10_000), 4),
                "positive_daily_net_rate": round(float(daily[net_column].gt(0).mean()), 6),
                "status": "DESCRIPTIVE" if len(daily) >= 30 else "DEFERRED_INSUFFICIENT_SAMPLE",
            })
    return records


def main() -> int:
    if not MEMBERSHIP_PATH.exists():
        raise RuntimeError(f"missing membership file: {MEMBERSHIP_PATH}")
    report = load_module("gapup_audit_report_backtest", REPORT_PATH)
    print("[1/5] loading integrity-filtered price history")
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    print("[2/5] computing factors identical to generate_candidates_v41_1.py")
    factors = report.compute_factors(raw)
    panel = build_execution_panel(factors)
    panel = build_research_universe(panel)
    print("[3/5] applying live _select_candidates() condition + GAPUP_BLOCK formula")
    candidates = select_candidates(panel)
    print("[4/5] summarizing fixed chronological partitions (blocked vs passed)")
    summary_rows: list[dict[str, object]] = []
    for label, start, end in PARTITIONS:
        summary_rows.extend(summarize_partition(candidates, label, start, end))
    summary = pd.DataFrame(summary_rows)
    print("[5/5] writing research-only evidence")
    summary_path = LOG_DIR / f"{OUT_PREFIX}_summary.csv"
    candidates_path = LOG_DIR / f"{OUT_PREFIX}_candidates.csv"
    json_path = LOG_DIR / f"{OUT_PREFIX}.json"
    md_path = LOG_DIR / f"{OUT_PREFIX}.md"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    candidate_columns = [
        "date", "code", "market", "rs", "v_accel", "stretch", "gap_up_pct", "gapup_blocked",
        "entry_open", "close",
        *[f"net_return_h{horizon}" for horizon in HORIZONS],
    ]
    candidates.loc[:, candidate_columns].sort_values(["date", "code"], kind="mergesort").to_csv(
        candidates_path, index=False, encoding="utf-8-sig"
    )
    overall_block_rate = float(candidates["gapup_blocked"].mean()) if len(candidates) else None
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_gapup_block_historical_audit",
        "descriptive_only": True,
        "note": "Audits already-shipped GAPUP_BLOCK logic against full historical data; introduces no new rule/threshold. NORMAL_INTRADAY_MOMENTUM_BLOCK is out of scope (needs intraday data, only ~40 recent days available).",
        "price_history_contract": integrity,
        "rule": {
            "candidate_selection": "generate_candidates_v41_1.py:_select_candidates() DEFAULT_PARAMS, reproduced verbatim",
            "select_params": SELECT_PARAMS,
            "gap_up_max_pct": GAP_UP_MAX_PCT,
            "gap_formula": "entry_open / signal_day_close - 1 (paper_engine.py GAPUP_BLOCK formula)",
            "entry": "next actual global trading-session open",
            "exits": [f"h{horizon} actual global trading-session close" for horizon in HORIZONS],
            "costs": {"fee_pct_each_side": FEE, "slippage_pct_each_side": SLIPPAGE, "sell_tax_pct": SELL_TAX},
        },
        "partitions": [{"label": label, "start": start, "end": end} for label, start, end in PARTITIONS],
        "input": {
            "universe_rows": int(len(panel)),
            "candidate_rows": int(len(candidates)),
            "candidate_signal_dates": int(candidates["date"].nunique()) if len(candidates) else 0,
            "overall_gapup_block_rate": round(overall_block_rate, 6) if overall_block_rate is not None else None,
            "membership_sha256": sha256(MEMBERSHIP_PATH),
            "runner_sha256": sha256(Path(__file__)),
            "report_backtest_sha256": sha256(REPORT_PATH),
            "generator_sha256": sha256(GENERATOR_PATH),
        },
        "summary": safe_records(summary),
        "outputs": {"summary_csv": str(summary_path), "candidates_csv": str(candidates_path), "markdown": str(md_path)},
        "operational_change": False,
        "promotion": "FORBIDDEN_DESCRIPTIVE_ONLY",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# GAPUP_BLOCK Historical Audit — Descriptive Result",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- universe_rows: {payload['input']['universe_rows']}",
        f"- candidate_rows: {payload['input']['candidate_rows']}",
        f"- overall_gapup_block_rate: {payload['input']['overall_gapup_block_rate']}",
        "- promotion: forbidden; operational_change: false",
        "",
        "## Audited Rule (already live, unmodified)",
        "",
        f"- Candidate selection: generate_candidates_v41_1.py:_select_candidates() DEFAULT_PARAMS: {SELECT_PARAMS}",
        f"- GAPUP_BLOCK: gap = entry_open/signal_close - 1, blocked if gap > {GAP_UP_MAX_PCT}",
        "- Next actual-session open entry, h1/h2/h5 exits, fixed round-trip cost assumption.",
        "",
        "## Summary (BLOCKED vs PASSED, by partition/horizon)",
        "",
        "```csv",
        summary.to_csv(index=False),
        "```",
        "",
        "## Interpretation Limit",
        "",
        "- Descriptive audit of already-shipped logic only; not a new candidate-generation methodology round, not a threshold search.",
        "- NORMAL_INTRADAY_MOMENTUM_BLOCK is out of scope here (intraday-data dependent, only ~40 recent days available).",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": "OK", "summary": str(summary_path), "json": str(json_path), "markdown": str(md_path),
        "candidate_rows": int(len(candidates)), "overall_gapup_block_rate": payload["input"]["overall_gapup_block_rate"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
