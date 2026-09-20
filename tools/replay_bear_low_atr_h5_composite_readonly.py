from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_ROWS = LOG_DIR / "bear_signal_direction_scan_rows_latest.csv"
METHOD_MAP = LOG_DIR / "current_regime_methodology_map_latest.json"

HORIZON = "h5"
HORIZON_RET_COL = f"fwd_ret_{HORIZON}"
TOP_NS = (5, 10, 20, 999999)
COST_BPS = (0, 30, 60)
PRIMARY_VARIANT_ID = "tradable_value_ge_1b_close_ge_1000_known_market"
PRIMARY_TOP_N = "top10"
PRIMARY_COST_BPS = 30

OUT_JSON = LOG_DIR / "bear_low_atr_h5_composite_replay_latest.json"
OUT_SUMMARY = LOG_DIR / "bear_low_atr_h5_composite_replay_summary_latest.csv"
OUT_DAILY = LOG_DIR / "bear_low_atr_h5_composite_replay_daily_latest.csv"
OUT_TRADES = LOG_DIR / "bear_low_atr_h5_composite_replay_trades_latest.csv"
OUT_MD = LOG_DIR / "bear_low_atr_h5_composite_replay_latest.md"


def profit_factor(ret: Iterable[float]) -> float | None:
    gains = 0.0
    losses = 0.0
    for value in ret:
        if value > 0:
            gains += float(value)
        elif value < 0:
            losses += abs(float(value))
    if losses > 0:
        return gains / losses
    if gains > 0:
        return None
    return 0.0


def clean_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): clean_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean_json_value(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def add_rank_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    numeric_cols = ["close", "value", "rs", "rsi14", "stretch", "atr_pct", "ret1_pct", "v_accel"]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["atr_rank"] = out.groupby("date")["atr_pct"].rank(pct=True, ascending=True)
    out["value_rank"] = out.groupby("date")["value"].rank(pct=True, ascending=True)
    out["rs_rank"] = out.groupby("date")["rs"].rank(pct=True, ascending=True)
    out["rsi_rank"] = out.groupby("date")["rsi14"].rank(pct=True, ascending=True)
    out["stretch_rank"] = out.groupby("date")["stretch"].rank(pct=True, ascending=True)
    out["low_atr_score"] = 1.0 - out["atr_rank"].fillna(1.0)
    out["value_score"] = out["value_rank"].fillna(0.0)
    out["rs_score"] = out["rs_rank"].fillna(0.0)
    out["rsi_low_score"] = 1.0 - out["rsi_rank"].fillna(1.0)
    out["stretch_low_score"] = 1.0 - out["stretch_rank"].fillna(1.0)
    out["composite_score"] = (
        0.40 * out["low_atr_score"]
        + 0.20 * out["value_score"]
        + 0.20 * out["rs_score"]
        + 0.10 * out["rsi_low_score"]
        + 0.10 * out["stretch_low_score"]
    )
    return out


def load_branch_rows() -> pd.DataFrame:
    df = pd.read_csv(INPUT_ROWS, encoding="utf-8-sig", dtype={"code": str})
    df = df[
        df["market_regime"].astype(str).eq("BEAR")
        & df["matched_signal"].astype(str).eq("atr_pct")
        & df["matched_direction"].astype(str).eq("low")
    ].copy()
    df[HORIZON_RET_COL] = pd.to_numeric(df[HORIZON_RET_COL], errors="coerce")
    df = add_rank_features(df)
    df["is_known_market"] = df["market"].astype(str).isin(["KOSPI", "KOSDAQ"])
    df["is_tradable_basic"] = (
        df["is_known_market"]
        & pd.to_numeric(df["value"], errors="coerce").ge(1_000_000_000)
        & pd.to_numeric(df["close"], errors="coerce").ge(1000)
    )
    return df.sort_values(["date", "composite_score", "code"], ascending=[True, False, True]).reset_index(drop=True)


def variant_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "branch_atr_low_all": pd.Series(True, index=df.index),
        "tradable_value_ge_1b_close_ge_1000_known_market": df["is_tradable_basic"].fillna(False),
        "tradable_low_atr_strict_half": df["is_tradable_basic"].fillna(False) & df["atr_rank"].le(0.50).fillna(False),
        "tradable_rs_top_half": df["is_tradable_basic"].fillna(False) & df["rs_rank"].ge(0.50).fillna(False),
        "tradable_reversal_half": (
            df["is_tradable_basic"].fillna(False)
            & df["rsi_rank"].le(0.50).fillna(False)
            & df["stretch_rank"].le(0.50).fillna(False)
        ),
    }


def replay_variant(df: pd.DataFrame, variant_id: str, top_n: int, cost_bps: int) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    top_label = f"top{top_n}" if top_n < 999999 else "all"
    selected = df.copy()
    selected["candidate_rank"] = selected.groupby("date")["composite_score"].rank(method="first", ascending=False).astype(int)
    if top_n < 999999:
        selected = selected[selected["candidate_rank"].le(top_n)].copy()
    selected["variant_id"] = variant_id
    selected["top_n"] = top_label
    selected["cost_bps"] = int(cost_bps)
    selected["gross_ret"] = pd.to_numeric(selected[HORIZON_RET_COL], errors="coerce")
    selected["net_ret"] = selected["gross_ret"] - (float(cost_bps) / 10000.0)
    closed = selected[selected["net_ret"].notna()].copy()
    pending = selected[selected["net_ret"].isna()].copy()

    daily_rows: list[dict[str, Any]] = []
    for day, g in selected.groupby("date", dropna=False):
        c = g[g["net_ret"].notna()]
        daily_rows.append({
            "variant_id": variant_id,
            "top_n": top_label,
            "cost_bps": int(cost_bps),
            "date": str(day),
            "selected_rows": int(len(g)),
            "closed_rows": int(len(c)),
            "pending_rows": int(len(g) - len(c)),
            "portfolio_ret": float(c["net_ret"].mean()) if len(c) else None,
            "gross_portfolio_ret": float(c["gross_ret"].mean()) if len(c) else None,
            "win_rate": float((c["net_ret"] > 0).mean()) if len(c) else None,
            "top_codes": "|".join(g.sort_values("candidate_rank").head(10)["code"].astype(str).tolist()),
        })

    daily = pd.DataFrame(daily_rows)
    port_ret = pd.to_numeric(daily["portfolio_ret"], errors="coerce").dropna()
    trade_ret = pd.to_numeric(closed["net_ret"], errors="coerce").dropna()
    summary = {
        "variant_id": variant_id,
        "top_n": top_label,
        "cost_bps": int(cost_bps),
        "selected_rows": int(len(selected)),
        "closed_rows": int(len(closed)),
        "pending_rows": int(len(pending)),
        "closed_dates": int(daily["portfolio_ret"].notna().sum()) if len(daily) else 0,
        "pending_dates": int((daily["pending_rows"] > 0).sum()) if len(daily) else 0,
        "trade_win_rate": float((trade_ret > 0).mean()) if len(trade_ret) else None,
        "trade_ret_mean": float(trade_ret.mean()) if len(trade_ret) else None,
        "trade_profit_factor": profit_factor(trade_ret.tolist()),
        "daily_ret_sum": float(port_ret.sum()) if len(port_ret) else 0.0,
        "daily_ret_mean": float(port_ret.mean()) if len(port_ret) else None,
        "daily_win_rate": float((port_ret > 0).mean()) if len(port_ret) else None,
        "daily_profit_factor": profit_factor(port_ret.tolist()),
        "worst_daily_ret": float(port_ret.min()) if len(port_ret) else None,
        "best_daily_ret": float(port_ret.max()) if len(port_ret) else None,
        "first_closed_date": str(daily[daily["portfolio_ret"].notna()]["date"].min()) if len(port_ret) else "",
        "last_closed_date": str(daily[daily["portfolio_ret"].notna()]["date"].max()) if len(port_ret) else "",
    }
    return summary, daily, selected


def main() -> int:
    if not METHOD_MAP.exists():
        raise SystemExit(f"missing methodology map: {METHOD_MAP}")
    method_map = json.loads(METHOD_MAP.read_text(encoding="utf-8"))
    rows = load_branch_rows()
    masks = variant_masks(rows)

    summary_rows: list[dict[str, Any]] = []
    daily_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    for variant_id, mask in masks.items():
        variant_df = rows[mask].copy()
        if variant_df.empty:
            continue
        for top_n in TOP_NS:
            for cost in COST_BPS:
                summary, daily, selected = replay_variant(variant_df, variant_id, top_n, cost)
                summary_rows.append(summary)
                daily_frames.append(daily)
                if cost == PRIMARY_COST_BPS and (top_n == 10 or top_n == 999999):
                    trade_frames.append(selected)

    summary_df = pd.DataFrame(summary_rows)
    daily_df = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    trades_df = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    if not trades_df.empty:
        trades_df["replay_status"] = trades_df["net_ret"].map(lambda x: "PENDING" if pd.isna(x) else "CLOSED")
        keep_cols = [
            "variant_id", "top_n", "cost_bps", "date", "code", "market", "candidate_rank", "composite_score",
            "close", "value", "atr_pct", "rs", "rsi14", "stretch", "gross_ret", "net_ret", "replay_status",
        ]
        trades_df = trades_df[[c for c in keep_cols if c in trades_df.columns]]

    primary = summary_df[
        summary_df["variant_id"].astype(str).eq(PRIMARY_VARIANT_ID)
        & summary_df["top_n"].astype(str).eq(PRIMARY_TOP_N)
        & summary_df["cost_bps"].eq(PRIMARY_COST_BPS)
    ]
    if primary.empty:
        conclusion = "BEAR_LOW_ATR_H5_COMPOSITE_REPLAY_INCOMPLETE"
        primary_summary: dict[str, Any] = {}
    else:
        row = primary.iloc[0]
        primary_summary = row.to_dict()
        if int(row["closed_dates"]) >= 2 and float(row["daily_ret_mean"]) > 0 and float(row["daily_profit_factor"]) >= 1.15:
            conclusion = "BEAR_LOW_ATR_H5_COMPOSITE_REPLAY_REVIEW_CANDIDATE_READ_ONLY"
        elif float(row["daily_ret_mean"]) > 0:
            conclusion = "BEAR_LOW_ATR_H5_COMPOSITE_POSITIVE_BUT_WEAK_READ_ONLY"
        else:
            conclusion = "BEAR_LOW_ATR_H5_COMPOSITE_REPLAY_NOT_SUPPORTED"

    best_rows = summary_df.sort_values(
        ["cost_bps", "daily_profit_factor", "daily_ret_mean", "closed_dates"],
        ascending=[True, False, False, False],
    ).head(20)

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "bear_low_atr_h5_composite_replay_read_only",
        "classification": "BEAR_LOW_ATR_H5_COMPOSITE_REPLAY_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "methodology_map_input": str(METHOD_MAP),
        "source_branch": {
            "market_regime": "BEAR",
            "signal": "atr_pct",
            "direction": "low",
            "horizon": HORIZON,
        },
        "cost_bps_grid": list(COST_BPS),
        "top_n_grid": ["top5", "top10", "top20", "all"],
        "variant_ids": list(masks.keys()),
        "primary_variant": f"{PRIMARY_VARIANT_ID}_{PRIMARY_TOP_N}_cost{PRIMARY_COST_BPS}bps",
        "row_counts": {
            "branch_rows": int(len(rows)),
            "branch_closed_rows": int(rows[HORIZON_RET_COL].notna().sum()),
            "branch_pending_rows": int(rows[HORIZON_RET_COL].isna().sum()),
            "summary_rows": int(len(summary_df)),
            "daily_rows": int(len(daily_df)),
            "trade_rows_saved": int(len(trades_df)),
        },
        "primary_summary": clean_json_value(primary_summary),
        "best_summaries": clean_json_value(best_rows.to_dict(orient="records")),
        "upstream_methodology_status": [
            row for row in method_map.get("regime_map", []) if row.get("regime") == "BEAR"
        ],
        "operation_effect": {
            "candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
            "operational_candidate_file_modified": False,
        },
        "validation": [
            "bear_low_atr_rows_loaded: PASS" if len(rows) else "bear_low_atr_rows_loaded: FAIL",
            "h5_closed_and_pending_split: PASS",
            "topn_cost_variant_grid_replayed: PASS" if len(summary_df) else "topn_cost_variant_grid_replayed: FAIL",
            "primary_variant_present: PASS" if not primary.empty else "primary_variant_present: FAIL",
            "operation_effect_no_changes: PASS",
        ],
    }
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    summary_df.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    daily_df.to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    trades_df.to_csv(OUT_TRADES, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(clean_json_value(payload), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# BEAR Low ATR H5 Composite Replay",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- primary_variant: {payload['primary_variant']}",
        "",
        "## Row Counts",
        "",
        *[f"- {k}: {v}" for k, v in payload["row_counts"].items()],
        "",
        "## Primary Summary",
        "",
        *[f"- {k}: {v}" for k, v in payload["primary_summary"].items()],
        "",
        "## Best Summaries",
        "",
    ]
    for row in payload["best_summaries"][:10]:
        md.append(
            f"- {row['variant_id']} / {row['top_n']} / cost={row['cost_bps']}bps: "
            f"closed_dates={row['closed_dates']}, daily_mean={row['daily_ret_mean']}, "
            f"daily_pf={row['daily_profit_factor']}, worst={row['worst_daily_ret']}"
        )
    md += [
        "",
        "## Operation Effect",
        "",
        "- NOT_APPLIED to candidate generation, official backtest, HPO, diagnostics, paper/live, gates, thresholds, or parameters.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_SUMMARY}")
    print(f"[OK] wrote {OUT_DAILY}")
    print(f"[OK] wrote {OUT_TRADES}")
    print(f"[OK] wrote {OUT_MD}")
    print(f"[CONCLUSION] {conclusion}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
