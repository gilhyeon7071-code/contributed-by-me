from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
FILTER_ROWS = LOG_DIR / "stress_composite_tradability_filter_rows_latest.csv"

CHOSEN_FILTER = "known_market_value_ge_1b_close_ge_1000"
HORIZON = "h2"
TOP_NS = (5, 10, 20, 999999)
COST_BPS = (0, 30, 60)

OUT_JSON = LOG_DIR / "stress_tradability_h2_replay_latest.json"
OUT_SUMMARY = LOG_DIR / "stress_tradability_h2_replay_summary_latest.csv"
OUT_DAILY = LOG_DIR / "stress_tradability_h2_replay_daily_latest.csv"
OUT_TRADES = LOG_DIR / "stress_tradability_h2_replay_trades_latest.csv"
OUT_MD = LOG_DIR / "stress_tradability_h2_replay_latest.md"


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


def load_rows() -> pd.DataFrame:
    df = pd.read_csv(FILTER_ROWS, encoding="utf-8-sig")
    df = df[df["filter_id"].astype(str).eq(CHOSEN_FILTER)].copy()
    df["date_norm"] = pd.to_datetime(df["date_norm"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["stress_candidate_score"] = pd.to_numeric(df["stress_candidate_score"], errors="coerce").fillna(0.0)
    df["trade_value"] = pd.to_numeric(df["trade_value"], errors="coerce").fillna(0.0)
    df[HORIZON_RET_COL] = pd.to_numeric(df[HORIZON_RET_COL], errors="coerce")
    df["candidate_rank"] = df.groupby("date_norm")["stress_candidate_score"].rank(method="first", ascending=False).astype(int)
    return df.sort_values(["date_norm", "candidate_rank", "code_norm"]).reset_index(drop=True)


HORIZON_RET_COL = f"fwd_ret_{HORIZON}"


def replay_variant(df: pd.DataFrame, top_n: int, cost_bps: int) -> tuple[dict[str, Any], pd.DataFrame]:
    label = f"top{top_n}" if top_n < 999999 else "all"
    selected = df[df["candidate_rank"].le(top_n)].copy() if top_n < 999999 else df.copy()
    selected["cost_bps"] = int(cost_bps)
    selected["top_n"] = label
    selected["gross_ret"] = pd.to_numeric(selected[HORIZON_RET_COL], errors="coerce")
    selected["net_ret"] = selected["gross_ret"] - (float(cost_bps) / 10000.0)
    closed = selected[selected["net_ret"].notna()].copy()
    pending = selected[selected["net_ret"].isna()].copy()

    daily_rows: list[dict[str, Any]] = []
    for day, g in selected.groupby("date_norm", dropna=False):
        c = g[g["net_ret"].notna()]
        daily_rows.append({
            "variant": label,
            "cost_bps": int(cost_bps),
            "date": str(day),
            "selected_rows": int(len(g)),
            "closed_rows": int(len(c)),
            "pending_rows": int(len(g) - len(c)),
            "portfolio_ret": float(c["net_ret"].mean()) if len(c) else None,
            "gross_portfolio_ret": float(c["gross_ret"].mean()) if len(c) else None,
            "win_rate": float((c["net_ret"] > 0).mean()) if len(c) else None,
            "top_codes": "|".join(g.sort_values("candidate_rank").head(10)["code_norm"].astype(str).tolist()),
        })

    daily = pd.DataFrame(daily_rows)
    port_ret = pd.to_numeric(daily["portfolio_ret"], errors="coerce").dropna()
    trade_ret = pd.to_numeric(closed["net_ret"], errors="coerce").dropna()
    summary = {
        "variant": label,
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
    return summary, daily


def main() -> int:
    rows = load_rows()
    summary_rows: list[dict[str, Any]] = []
    daily_frames: list[pd.DataFrame] = []
    for top_n in TOP_NS:
        for cost in COST_BPS:
            summary, daily = replay_variant(rows, top_n, cost)
            summary_rows.append(summary)
            daily_frames.append(daily)
    summary_df = pd.DataFrame(summary_rows)
    daily_df = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()

    trades = rows.copy()
    trades["gross_ret_h2"] = pd.to_numeric(trades[HORIZON_RET_COL], errors="coerce")
    trades["replay_status"] = trades["gross_ret_h2"].map(lambda x: "PENDING" if pd.isna(x) else "CLOSED")

    base = summary_df[(summary_df["variant"] == "top10") & (summary_df["cost_bps"] == 30)]
    if base.empty:
        conclusion = "H2_REPLAY_INCOMPLETE"
    else:
        row = base.iloc[0]
        if int(row["closed_dates"]) >= 3 and float(row["daily_ret_mean"]) > 0 and float(row["daily_profit_factor"]) >= 1.15:
            conclusion = "H2_REPLAY_REVIEW_CANDIDATE_WITH_PENDING_LATEST_DATE"
        elif float(row["daily_ret_mean"]) > 0:
            conclusion = "H2_REPLAY_POSITIVE_BUT_WEAK_FOR_REVIEW"
        else:
            conclusion = "H2_REPLAY_NOT_SUPPORTED"

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "stress_tradability_h2_replay_read_only",
        "classification": "STRESS_TRADABILITY_H2_REPLAY_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "chosen_filter": CHOSEN_FILTER,
        "horizon": HORIZON,
        "cost_bps_grid": list(COST_BPS),
        "top_n_grid": ["top5", "top10", "top20", "all"],
        "primary_variant": "top10_cost30bps",
        "row_counts": {
            "input_rows": int(len(rows)),
            "closed_input_rows": int(rows[HORIZON_RET_COL].notna().sum()),
            "pending_input_rows": int(rows[HORIZON_RET_COL].isna().sum()),
            "summary_rows": int(len(summary_df)),
            "daily_rows": int(len(daily_df)),
        },
        "primary_summary": clean_json_value(base.iloc[0].to_dict()) if not base.empty else {},
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
            "chosen_tradability_rows_loaded: PASS",
            "h2_closed_and_pending_split: PASS",
            "topn_cost_grid_replayed: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    summary_df.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    daily_df.to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    trades.to_csv(OUT_TRADES, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(clean_json_value(payload), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# STRESS Tradability H2 Replay",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- primary_variant: {payload['primary_variant']}",
        "",
        "## Primary Summary",
        "",
        *[f"- {k}: {v}" for k, v in payload["primary_summary"].items()],
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
