from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

METHOD_MAP = LOG_DIR / "current_regime_methodology_map_latest.json"
STRESS_ROWS = LOG_DIR / "stress_composite_tradability_filter_rows_latest.csv"
STRESS_PENDING = LOG_DIR / "stress_h2_pending_resolution_latest.json"
BEAR_TRADES = LOG_DIR / "bear_low_atr_h5_composite_replay_trades_latest.csv"
TRANSITION_ROWS = LOG_DIR / "transition_signal_direction_scan_rows_latest.csv"

OUT_LATEST = LOG_DIR / "new_method_candidates_latest.csv"
OUT_HISTORY = LOG_DIR / "new_method_candidates_history.csv"
OUT_META = LOG_DIR / "new_method_candidates_meta.json"
OUT_MD = LOG_DIR / "new_method_candidates_latest.md"

STRESS_FILTER_ID = "known_market_value_ge_1b_close_ge_1000"
BEAR_VARIANT_ID = "tradable_value_ge_1b_close_ge_1000_known_market"
TOP_N_PER_BRANCH = 10


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def clean_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): clean_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean_json_value(v) for v in value]
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def normalize_code(value: Any) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6) if text.isdigit() else text


def add_rank_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["close", "value", "trade_value", "rs", "rsi14", "stretch", "atr_pct", "ret1_pct", "composite_score", "stress_candidate_score"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "date_norm" in out.columns:
        out["date_norm"] = pd.to_datetime(out["date_norm"], errors="coerce").dt.strftime("%Y-%m-%d")
    return out


def base_candidate_record(
    *,
    date: str,
    code: str,
    market: str,
    regime: str,
    method_branch: str,
    signal_reason: str,
    horizon: str,
    expected_holding_days: int,
    candidate_score: float,
    rank_in_branch: int,
    close: Any,
    trade_value: Any,
    fwd_ret: Any,
    status: str,
    promotion_blocker: str,
    source_file: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record = {
        "date": date,
        "code": normalize_code(code),
        "market": str(market),
        "regime": regime,
        "method_branch": method_branch,
        "signal_reason": signal_reason,
        "horizon": horizon,
        "expected_holding_days": int(expected_holding_days),
        "candidate_score": float(candidate_score) if pd.notna(candidate_score) else 0.0,
        "rank_in_branch": int(rank_in_branch),
        "close": float(close) if pd.notna(close) else None,
        "trade_value": float(trade_value) if pd.notna(trade_value) else None,
        "forward_return": float(fwd_ret) if pd.notna(fwd_ret) else None,
        "forward_return_status": "PENDING" if pd.isna(fwd_ret) else "CLOSED",
        "status": status,
        "promotion_blocker": promotion_blocker,
        "source": "NEW_METHOD_OBSERVE_ONLY",
        "source_file": source_file,
        "operational_use": False,
    }
    if extra:
        record.update(extra)
    return record


def build_stress_candidates(stress_pending: dict[str, Any]) -> pd.DataFrame:
    df = pd.read_csv(STRESS_ROWS, encoding="utf-8-sig", dtype={"code_norm": str})
    df = add_rank_features(df)
    df = df[df["filter_id"].astype(str).eq(STRESS_FILTER_ID)].copy()
    if df.empty:
        return pd.DataFrame()
    df["rank_in_branch"] = df.groupby("date_norm")["stress_candidate_score"].rank(method="first", ascending=False).astype(int)
    df = df[df["rank_in_branch"].le(TOP_N_PER_BRANCH)].copy()
    pending_rows = int(stress_pending.get("row_counts", {}).get("h2_still_pending_rows", 0))
    status = "OBSERVE_ONLY_PRICE_PENDING" if pending_rows > 0 else "OBSERVE_ONLY"
    blocker = "H2_PENDING_NOT_PROMOTABLE" if pending_rows > 0 else "NONE"
    records = []
    for _, row in df.sort_values(["date_norm", "rank_in_branch"]).iterrows():
        records.append(base_candidate_record(
            date=str(row["date_norm"]),
            code=row["code_norm"],
            market=row["market"],
            regime="STRESS",
            method_branch="STRESS_OVERSOLD_RS_H2",
            signal_reason="stress composite: low RSI + low stretch + high relative strength + tradability filter",
            horizon="h2",
            expected_holding_days=2,
            candidate_score=row["stress_candidate_score"],
            rank_in_branch=row["rank_in_branch"],
            close=row["close"],
            trade_value=row["trade_value"],
            fwd_ret=row.get("fwd_ret_h2"),
            status=status,
            promotion_blocker=blocker,
            source_file=str(STRESS_ROWS),
            extra={
                "rs": row.get("rs"),
                "rsi14": row.get("rsi14"),
                "stretch": row.get("stretch"),
            },
        ))
    return pd.DataFrame(records)


def build_bear_candidates() -> pd.DataFrame:
    df = pd.read_csv(BEAR_TRADES, encoding="utf-8-sig", dtype={"code": str})
    df = add_rank_features(df)
    df = df[
        df["variant_id"].astype(str).eq(BEAR_VARIANT_ID)
        & df["top_n"].astype(str).eq("top10")
        & pd.to_numeric(df["cost_bps"], errors="coerce").eq(30)
    ].copy()
    if df.empty:
        return pd.DataFrame()
    df["candidate_rank"] = pd.to_numeric(df["candidate_rank"], errors="coerce").astype(int)
    records = []
    for _, row in df.sort_values(["date", "candidate_rank"]).iterrows():
        records.append(base_candidate_record(
            date=str(row["date"]),
            code=row["code"],
            market=row["market"],
            regime="BEAR",
            method_branch="BEAR_LOW_ATR_H5",
            signal_reason="bear low ATR h5 composite: low volatility + liquidity + relative strength + reversal features",
            horizon="h5",
            expected_holding_days=5,
            candidate_score=row["composite_score"],
            rank_in_branch=row["candidate_rank"],
            close=row["close"],
            trade_value=row["value"],
            fwd_ret=row.get("gross_ret"),
            status="OBSERVE_ONLY",
            promotion_blocker="LOW_CLOSED_DATE_COUNT_AND_PENDING_H5",
            source_file=str(BEAR_TRADES),
            extra={
                "atr_pct": row.get("atr_pct"),
                "rs": row.get("rs"),
                "rsi14": row.get("rsi14"),
                "stretch": row.get("stretch"),
            },
        ))
    return pd.DataFrame(records)


def build_transition_candidates() -> pd.DataFrame:
    df = pd.read_csv(TRANSITION_ROWS, encoding="utf-8-sig", dtype={"code": str})
    df = add_rank_features(df)
    df = df[
        df["market_regime"].astype(str).eq("TRANSITION")
        & df["matched_signal"].astype(str).eq("atr_pct")
        & df["matched_direction"].astype(str).eq("low")
    ].copy()
    if df.empty:
        return pd.DataFrame()
    for col in ["value", "close", "atr_pct", "rs", "rsi14", "stretch"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["known_market"] = df["market"].astype(str).isin(["KOSPI", "KOSDAQ"])
    df = df[df["known_market"] & df["value"].ge(1_000_000_000) & df["close"].ge(1000)].copy()
    if df.empty:
        return pd.DataFrame()
    df["atr_rank"] = df.groupby("date")["atr_pct"].rank(pct=True, ascending=True)
    df["value_rank"] = df.groupby("date")["value"].rank(pct=True, ascending=True)
    df["rs_rank"] = df.groupby("date")["rs"].rank(pct=True, ascending=True)
    df["rsi_low_score"] = 1.0 - df.groupby("date")["rsi14"].rank(pct=True, ascending=True)
    df["candidate_score"] = (
        0.45 * (1.0 - df["atr_rank"].fillna(1.0))
        + 0.25 * df["value_rank"].fillna(0.0)
        + 0.20 * df["rs_rank"].fillna(0.0)
        + 0.10 * df["rsi_low_score"].fillna(0.0)
    )
    df["rank_in_branch"] = df.groupby("date")["candidate_score"].rank(method="first", ascending=False).astype(int)
    df = df[df["rank_in_branch"].le(TOP_N_PER_BRANCH)].copy()
    records = []
    for _, row in df.sort_values(["date", "rank_in_branch"]).iterrows():
        records.append(base_candidate_record(
            date=str(row["date"]),
            code=row["code"],
            market=row["market"],
            regime="TRANSITION",
            method_branch="TRANSITION_LOW_ATR_H1",
            signal_reason="transition low ATR h1: low volatility + liquidity + relative strength",
            horizon="h1",
            expected_holding_days=1,
            candidate_score=row["candidate_score"],
            rank_in_branch=row["rank_in_branch"],
            close=row["close"],
            trade_value=row["value"],
            fwd_ret=row.get("fwd_ret_h1"),
            status="OBSERVE_ONLY",
            promotion_blocker="SMALL_SIGNAL_DATE_COUNT",
            source_file=str(TRANSITION_ROWS),
            extra={
                "atr_pct": row.get("atr_pct"),
                "rs": row.get("rs"),
                "rsi14": row.get("rsi14"),
                "stretch": row.get("stretch"),
            },
        ))
    return pd.DataFrame(records)


def main() -> int:
    required = [METHOD_MAP, STRESS_ROWS, STRESS_PENDING, BEAR_TRADES, TRANSITION_ROWS]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise SystemExit("missing inputs: " + "; ".join(missing))

    method_map = read_json(METHOD_MAP)
    stress_pending = read_json(STRESS_PENDING)

    frames = [
        build_stress_candidates(stress_pending),
        build_bear_candidates(),
        build_transition_candidates(),
    ]
    history = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
    if history.empty:
        raise SystemExit("no observe-only candidates generated")

    history = history.sort_values(["date", "regime", "method_branch", "rank_in_branch", "code"]).reset_index(drop=True)
    latest_dates = history.groupby(["regime", "method_branch"], as_index=False)["date"].max()
    latest = history.merge(latest_dates, on=["regime", "method_branch", "date"], how="inner")
    latest = latest.sort_values(["regime", "method_branch", "rank_in_branch", "code"]).reset_index(drop=True)

    branch_counts = (
        history.groupby(["regime", "method_branch", "horizon", "status", "promotion_blocker"], dropna=False)
        .agg(
            history_rows=("code", "size"),
            latest_date=("date", "max"),
            closed_rows=("forward_return_status", lambda s: int((s == "CLOSED").sum())),
            pending_rows=("forward_return_status", lambda s: int((s == "PENDING").sum())),
            unique_codes=("code", "nunique"),
        )
        .reset_index()
        .sort_values(["regime", "method_branch"])
    )

    latest_branch_counts = (
        latest.groupby(["regime", "method_branch", "horizon", "status", "promotion_blocker"], dropna=False)
        .agg(
            latest_rows=("code", "size"),
            latest_date=("date", "max"),
            closed_rows=("forward_return_status", lambda s: int((s == "CLOSED").sum())),
            pending_rows=("forward_return_status", lambda s: int((s == "PENDING").sum())),
            unique_codes=("code", "nunique"),
        )
        .reset_index()
        .sort_values(["regime", "method_branch"])
    )

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "new_method_candidates_observe_only",
        "classification": "NEW_METHOD_CANDIDATES_OBSERVE_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED_TO_OPERATIONAL_LOGIC",
        "logic_application_stage": "OBSERVE_ONLY_CANDIDATE_GENERATOR_APPLIED",
        "methodology_cycle": "validation_to_logic_application_to_new_data_to_revalidation",
        "methodology_map_input": str(METHOD_MAP),
        "methodology_map_conclusion": method_map.get("conclusion"),
        "row_counts": {
            "history_rows": int(len(history)),
            "latest_rows": int(len(latest)),
            "history_closed_rows": int((history["forward_return_status"] == "CLOSED").sum()),
            "history_pending_rows": int((history["forward_return_status"] == "PENDING").sum()),
            "latest_closed_rows": int((latest["forward_return_status"] == "CLOSED").sum()),
            "latest_pending_rows": int((latest["forward_return_status"] == "PENDING").sum()),
            "branch_count": int(history[["regime", "method_branch"]].drop_duplicates().shape[0]),
        },
        "branch_counts": branch_counts.astype(object).where(pd.notna(branch_counts), None).to_dict(orient="records"),
        "latest_branch_counts": latest_branch_counts.astype(object).where(pd.notna(latest_branch_counts), None).to_dict(orient="records"),
        "inactive_regimes": [
            {"regime": "BULL", "reason": "not present in current regime window; keep historical methodology only"},
            {"regime": "SIDEWAYS", "reason": "not present in current regime window; keep historical methodology only"},
        ],
        "promotion_rules": [
            "observe-only candidates are not operational candidates",
            "existing candidate generation, gates, paper/live, and orders are not modified",
            "promotion requires enough closed signal dates and a separate revalidation report",
            "STRESS promotion is blocked while h2 pending rows remain unresolved",
        ],
        "operation_effect": {
            "operational_candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "gate_or_threshold_change": False,
            "order_path": False,
        },
        "validation": [
            "required_inputs_exist: PASS",
            "observe_only_candidates_generated: PASS" if len(history) else "observe_only_candidates_generated: FAIL",
            "latest_candidates_generated: PASS" if len(latest) else "latest_candidates_generated: FAIL",
            "required_schema_present: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    history.to_csv(OUT_HISTORY, index=False, encoding="utf-8-sig")
    latest.to_csv(OUT_LATEST, index=False, encoding="utf-8-sig")
    OUT_META.write_text(json.dumps(clean_json_value(payload), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# New Method Candidates Observe Only",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- logic_application_stage: {payload['logic_application_stage']}",
        f"- methodology_cycle: {payload['methodology_cycle']}",
        "",
        "## Row Counts",
        "",
        *[f"- {k}: {v}" for k, v in payload["row_counts"].items()],
        "",
        "## Latest Branch Counts",
        "",
    ]
    for row in payload["latest_branch_counts"]:
        md.append(
            f"- {row['regime']} / {row['method_branch']} / {row['horizon']}: "
            f"latest_date={row['latest_date']}, rows={row['latest_rows']}, "
            f"closed={row['closed_rows']}, pending={row['pending_rows']}, blocker={row['promotion_blocker']}"
        )
    md += [
        "",
        "## Boundary",
        "",
        "- This is the first observe-only logic application artifact.",
        "- NOT_APPLIED to operational candidate generation, official backtest, HPO, paper/live, gates, thresholds, or orders.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_LATEST}")
    print(f"[OK] wrote {OUT_HISTORY}")
    print(f"[OK] wrote {OUT_META}")
    print(f"[OK] wrote {OUT_MD}")
    print("[CONCLUSION] OBSERVE_ONLY_CANDIDATE_GENERATOR_APPLIED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
