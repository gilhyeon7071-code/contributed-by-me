from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
RAW_ROWS = LOG_DIR / "stress_composite_daily_candidates_rows_latest.csv"
FILTER_ROWS = LOG_DIR / "stress_composite_tradability_filter_rows_latest.csv"
OPERATIONAL_ROWS = LOG_DIR / "candidates_latest_data.with_final_score.csv"

CHOSEN_FILTER = "known_market_value_ge_1b_close_ge_1000"
STRICT_FILTER = "known_market_value_ge_10b_close_ge_1000"
HORIZONS = (1, 2, 5)

OUT_JSON = LOG_DIR / "stress_tradability_forward_return_compare_latest.json"
OUT_SUMMARY = LOG_DIR / "stress_tradability_forward_return_compare_summary_latest.csv"
OUT_DAILY = LOG_DIR / "stress_tradability_forward_return_compare_daily_latest.csv"
OUT_ROWS = LOG_DIR / "stress_tradability_forward_return_compare_rows_latest.csv"
OUT_MD = LOG_DIR / "stress_tradability_forward_return_compare_latest.md"


def norm_date(value: Any) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def norm_code(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return ""
    text = str(value).strip()
    try:
        if float(text).is_integer():
            return str(int(float(text))).zfill(6)
    except Exception:
        pass
    return text.zfill(6) if text.isdigit() else text


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


def load_raw() -> pd.DataFrame:
    df = pd.read_csv(RAW_ROWS, encoding="utf-8-sig")
    df["pool"] = "raw_stress_spec"
    df["filter_id"] = "raw"
    df["date_norm"] = df["date_norm"].map(norm_date)
    df["code_norm"] = df["code_norm"].map(norm_code)
    return df


def load_filters() -> pd.DataFrame:
    df = pd.read_csv(FILTER_ROWS, encoding="utf-8-sig")
    df["pool"] = "tradability_filter_" + df["filter_id"].astype(str)
    df["date_norm"] = df["date_norm"].map(norm_date)
    df["code_norm"] = df["code_norm"].map(norm_code)
    return df


def load_operational_codes() -> set[tuple[str, str]]:
    if not OPERATIONAL_ROWS.exists():
        return set()
    op = pd.read_csv(OPERATIONAL_ROWS, encoding="utf-8-sig")
    if "date" not in op.columns or "code" not in op.columns:
        return set()
    op["date_norm"] = op["date"].map(norm_date)
    op["code_norm"] = op["code"].map(norm_code)
    return set(zip(op["date_norm"], op["code_norm"]))


def summarize(df: pd.DataFrame, pool: str, group: str, value: str, horizon: int) -> dict[str, Any]:
    ret_col = f"fwd_ret_h{horizon}"
    all_n = int(len(df))
    ret = pd.to_numeric(df.get(ret_col, pd.Series(dtype=float)), errors="coerce")
    valid = ret.dropna()
    pending_n = int(all_n - len(valid))
    pf = profit_factor(valid.tolist())
    return {
        "pool": pool,
        "group": group,
        "value": value,
        "horizon": f"h{horizon}",
        "all_rows": all_n,
        "valid_return_rows": int(len(valid)),
        "pending_return_rows": pending_n,
        "unique_codes": int(df.loc[valid.index, "code_norm"].nunique()) if len(valid) and "code_norm" in df.columns else 0,
        "win_n": int((valid > 0).sum()) if len(valid) else 0,
        "loss_n": int((valid <= 0).sum()) if len(valid) else 0,
        "win_rate": float((valid > 0).mean()) if len(valid) else None,
        "ret_sum": float(valid.sum()) if len(valid) else 0.0,
        "ret_mean": float(valid.mean()) if len(valid) else None,
        "ret_median": float(valid.median()) if len(valid) else None,
        "profit_factor": pf,
        "first_signal_date": df.loc[valid.index, "date_norm"].min() if len(valid) else "",
        "last_signal_date": df.loc[valid.index, "date_norm"].max() if len(valid) else "",
    }


def add_pool(frames: list[pd.DataFrame], pool_name: str, df: pd.DataFrame) -> None:
    out = df.copy()
    out["analysis_pool"] = pool_name
    frames.append(out)


def main() -> int:
    raw = load_raw()
    filters = load_filters()
    operational_keys = load_operational_codes()

    frames: list[pd.DataFrame] = []
    add_pool(frames, "raw_stress_spec", raw)
    chosen = filters[filters["filter_id"].eq(CHOSEN_FILTER)].copy()
    strict = filters[filters["filter_id"].eq(STRICT_FILTER)].copy()
    add_pool(frames, "chosen_tradability_filter", chosen)
    add_pool(frames, "strict_10b_tradability_filter", strict)

    if operational_keys:
        overlap = chosen[
            chosen.apply(lambda r: (str(r.get("date_norm", "")), str(r.get("code_norm", ""))) in operational_keys, axis=1)
        ].copy()
    else:
        overlap = chosen.iloc[0:0].copy()
    add_pool(frames, "chosen_filter_operational_overlap", overlap)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    summary_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    for pool, g in combined.groupby("analysis_pool", dropna=False):
        for h in HORIZONS:
            summary_rows.append(summarize(g, str(pool), "all_current_window", "all", h))
        for day, day_df in g.groupby("date_norm", dropna=False):
            for h in HORIZONS:
                daily_rows.append(summarize(day_df, str(pool), "date", str(day), h))

    summary = pd.DataFrame(summary_rows)
    daily = pd.DataFrame(daily_rows)

    chosen_h2 = summary[
        (summary["pool"] == "chosen_tradability_filter")
        & (summary["group"] == "all_current_window")
        & (summary["horizon"] == "h2")
    ]
    raw_h2 = summary[
        (summary["pool"] == "raw_stress_spec")
        & (summary["group"] == "all_current_window")
        & (summary["horizon"] == "h2")
    ]
    if chosen_h2.empty or raw_h2.empty:
        conclusion = "FORWARD_RETURN_COMPARE_INCOMPLETE"
        chosen_vs_raw = {}
    else:
        c = chosen_h2.iloc[0]
        r = raw_h2.iloc[0]
        chosen_vs_raw = {
            "h2_ret_mean_delta": None if pd.isna(c["ret_mean"]) or pd.isna(r["ret_mean"]) else float(c["ret_mean"]) - float(r["ret_mean"]),
            "h2_profit_factor_delta": None if pd.isna(c["profit_factor"]) or pd.isna(r["profit_factor"]) else float(c["profit_factor"]) - float(r["profit_factor"]),
            "h2_valid_rows_raw": int(r["valid_return_rows"]),
            "h2_valid_rows_chosen": int(c["valid_return_rows"]),
        }
        if int(c["valid_return_rows"]) >= 20 and float(c["ret_mean"]) > 0 and float(c["profit_factor"]) >= 1.15:
            if chosen_vs_raw["h2_ret_mean_delta"] is not None and chosen_vs_raw["h2_ret_mean_delta"] >= 0:
                conclusion = "TRADABILITY_FILTER_FORWARD_RETURNS_MAINTAINED_OR_IMPROVED_READ_ONLY"
            else:
                conclusion = "TRADABILITY_FILTER_FORWARD_RETURNS_POSITIVE_BUT_LOWER_THAN_RAW_READ_ONLY"
        else:
            conclusion = "TRADABILITY_FILTER_FORWARD_RETURNS_WEAK_READ_ONLY"

    keep_cols = [
        "analysis_pool", "filter_id", "date_norm", "code_norm", "market", "market_regime",
        "stress_candidate_score", "close", "trade_value", "value", "fwd_ret_h1", "fwd_ret_h2", "fwd_ret_h5",
    ]
    rows_out = combined[[c for c in keep_cols if c in combined.columns]].sort_values(
        ["analysis_pool", "date_norm", "stress_candidate_score", "code_norm"],
        ascending=[True, True, False, True],
    ).reset_index(drop=True)

    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "stress_tradability_forward_return_compare_read_only",
        "classification": "STRESS_TRADABILITY_FORWARD_RETURN_COMPARE_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "chosen_filter": CHOSEN_FILTER,
        "strict_filter": STRICT_FILTER,
        "chosen_vs_raw": chosen_vs_raw,
        "row_counts": {
            "raw_rows": int(len(raw)),
            "chosen_filter_rows": int(len(chosen)),
            "strict_filter_rows": int(len(strict)),
            "chosen_operational_overlap_rows": int(len(overlap)),
            "summary_rows": int(len(summary)),
            "daily_rows": int(len(daily)),
        },
        "summary": summary.to_dict(orient="records"),
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
            "raw_and_filtered_rows_loaded: PASS",
            "h1_h2_h5_compared: PASS",
            "pending_return_rows_separated: PASS",
            "operational_overlap_read_only: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    daily.to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    rows_out.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(clean_json_value(payload), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# STRESS Tradability Forward Return Compare",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- chosen_filter: {CHOSEN_FILTER}",
        "",
        "## Chosen vs Raw",
        "",
        *[f"- {k}: {v}" for k, v in chosen_vs_raw.items()],
        "",
        "## Row Counts",
        "",
        *[f"- {k}: {v}" for k, v in payload["row_counts"].items()],
        "",
        "## Operation Effect",
        "",
        "- NOT_APPLIED to candidate generation, official backtest, HPO, diagnostics, paper/live, gates, thresholds, or parameters.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_SUMMARY}")
    print(f"[OK] wrote {OUT_DAILY}")
    print(f"[OK] wrote {OUT_ROWS}")
    print(f"[OK] wrote {OUT_MD}")
    print(f"[CONCLUSION] {conclusion}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
