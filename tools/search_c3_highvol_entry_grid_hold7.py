from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_TRADES = LOG_DIR / "c3_highvol_exit_variant_compare_trades_latest.csv"

OUT_JSON = LOG_DIR / "c3_highvol_entry_grid_hold7_latest.json"
OUT_GRID = LOG_DIR / "c3_highvol_entry_grid_hold7_grid_latest.csv"
OUT_ROWS = LOG_DIR / "c3_highvol_entry_grid_hold7_rows_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_entry_grid_hold7_latest.md"

TARGET_EXIT_VARIANT = "hold7_stop6"


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _profit_factor(ret: pd.Series) -> float | None:
    gains = float(ret[ret > 0].sum())
    losses = float(-ret[ret < 0].sum())
    if losses == 0:
        return None if gains > 0 else 0.0
    return gains / losses


def _segment(df: pd.DataFrame, segment: str) -> pd.DataFrame:
    dt = pd.to_datetime(df["entry_date_norm"], errors="coerce")
    if segment == "all":
        return df.copy()
    if segment == "2024H1":
        return df[(dt >= "2024-01-01") & (dt <= "2024-06-30")].copy()
    if segment == "2025H1":
        return df[(dt >= "2025-01-01") & (dt <= "2025-06-30")].copy()
    if segment == "2025H2":
        return df[(dt >= "2025-07-01") & (dt <= "2025-12-31")].copy()
    if segment == "pre_2026":
        return df[dt < "2026-01-01"].copy()
    if segment == "2026H1":
        return df[(dt >= "2026-01-01") & (dt <= "2026-06-30")].copy()
    raise ValueError(segment)


def _summary_values(frame: pd.DataFrame) -> dict[str, Any]:
    ret = _num(frame["ret"]) if len(frame) else pd.Series(dtype=float)
    return {
        "n": int(len(frame)),
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret < 0).sum()) if len(ret) else 0,
        "stop_n": int((frame["exit_reason"].astype(str).str.upper() == "STOP").sum()) if len(frame) else 0,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "profit_factor": _profit_factor(ret),
        "unique_codes": int(frame["code"].nunique()) if len(frame) and "code" in frame.columns else 0,
    }


def _concentration(frame: pd.DataFrame) -> dict[str, Any]:
    ret = _num(frame["ret"]).dropna()
    wins = ret[ret > 0].sort_values(ascending=False)
    gross_profit = float(wins.sum())
    ret_sum = float(ret.sum())
    return {
        "gross_profit": gross_profit,
        "top1_positive_ret": float(wins.iloc[0]) if len(wins) else 0.0,
        "top3_positive_ret": float(wins.iloc[:3].sum()) if len(wins) else 0.0,
        "top5_positive_ret": float(wins.iloc[:5].sum()) if len(wins) else 0.0,
        "top3_share_of_gross_profit": float(wins.iloc[:3].sum() / gross_profit) if gross_profit > 0 and len(wins) else None,
        "top5_share_of_gross_profit": float(wins.iloc[:5].sum() / gross_profit) if gross_profit > 0 and len(wins) else None,
        "ret_sum_without_top5": float(ret_sum - wins.iloc[:5].sum()) if len(wins) else ret_sum,
    }


def _clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _classify(grid: pd.DataFrame) -> dict[str, Any]:
    viable = grid[
        (grid["n_all"] >= 20)
        & (grid["ret_all"] > 0)
        & (grid["ret_pre_2026"] > 0)
        & (grid["ret_2025H2"] > 0)
        & (grid["n_2026H1"] >= 1)
        & (grid["ret_2026H1"] > 0)
    ].sort_values(["ret_all", "n_all"], ascending=[False, False])
    reasons: list[str] = []
    if len(viable):
        best = viable.iloc[0].to_dict()
        reasons.append(f"viable_grid_count:{len(viable)}")
        reasons.append(f"best_rule:{best['rule_id']}")
        if int(best["n_all"]) < 30:
            reasons.append("best_sample_under_30")
        if float(best.get("top5_share_of_gross_profit") or 0) > 0.5:
            reasons.append("best_profit_concentrated_top5")
    else:
        best = {}
        reasons.append("no_viable_grid")
    return {
        "operational_decision": "NOT_APPROVED",
        "research_classification": "C3_HIGHVOL_ENTRY_GRID_HAS_SMALL_VIABLE_PROFILE",
        "confidence": "MEDIUM_RESEARCH_ONLY" if len(viable) else "LOW_RESEARCH_ONLY",
        "full_logic_application": "NOT_APPLIED",
        "best_rule_id": best.get("rule_id"),
        "reason": reasons,
    }


def main() -> int:
    if not INPUT_TRADES.exists():
        raise FileNotFoundError(INPUT_TRADES)
    df = pd.read_csv(INPUT_TRADES, dtype={"code": str})
    hold = df[df["exit_variant"].astype(str).eq(TARGET_EXIT_VARIANT)].copy()
    for col in ["entry_gap_pct", "signal_v_accel", "signal_atr_pct", "signal_value", "ret"]:
        hold[col] = _num(hold[col])
    base = hold[hold["market_regime"].astype(str).isin(["BULL", "SIDEWAYS"])].copy()

    gap_los = [0.0147299509]
    gap_his = [0.025, 0.03, 0.035, 0.04, 0.05]
    vacc_los = [1.255833911]
    vacc_his = [1.5, 1.75, 2.0, 2.5]
    atr_ranges = [(0.03, 0.04), (0.028, 0.04), (0.03, 0.045), (0.028, 0.045)]
    value_mins = [None, 500_000_000.0]
    segments = ["all", "2024H1", "2025H1", "2025H2", "pre_2026", "2026H1"]

    grid_rows: list[dict[str, Any]] = []
    selected_parts: list[pd.DataFrame] = []
    for gap_lo in gap_los:
        for gap_hi in gap_his:
            for vacc_lo in vacc_los:
                for vacc_hi in vacc_his:
                    for atr_lo, atr_hi in atr_ranges:
                        for value_min in value_mins:
                            mask = (
                                base["entry_gap_pct"].ge(gap_lo)
                                & base["entry_gap_pct"].lt(gap_hi)
                                & base["signal_v_accel"].ge(vacc_lo)
                                & base["signal_v_accel"].lt(vacc_hi)
                                & base["signal_atr_pct"].ge(atr_lo)
                                & base["signal_atr_pct"].lt(atr_hi)
                            )
                            if value_min is not None:
                                mask &= base["signal_value"].ge(value_min)
                            selected = base[mask.fillna(False)].copy()
                            rule_id = (
                                f"gap_{gap_lo:.4f}_{gap_hi:.3f}__vacc_{vacc_lo:.3f}_{vacc_hi:.2f}"
                                f"__atr_{atr_lo:.3f}_{atr_hi:.3f}__value_{'none' if value_min is None else '500m'}"
                            )
                            row: dict[str, Any] = {
                                "rule_id": rule_id,
                                "gap_lo": gap_lo,
                                "gap_hi": gap_hi,
                                "v_accel_lo": vacc_lo,
                                "v_accel_hi": vacc_hi,
                                "atr_lo": atr_lo,
                                "atr_hi": atr_hi,
                                "value_min": value_min,
                            }
                            for seg in segments:
                                vals = _summary_values(_segment(selected, seg))
                                row[f"n_{seg}"] = vals["n"]
                                row[f"ret_{seg}"] = vals["ret_sum"]
                                row[f"pf_{seg}"] = vals["profit_factor"]
                                row[f"stop_{seg}"] = vals["stop_n"]
                            row.update(_concentration(selected))
                            grid_rows.append(row)
                            if len(selected) >= 20:
                                part = selected.copy()
                                part.insert(0, "rule_id", rule_id)
                                selected_parts.append(part)

    grid = pd.DataFrame(grid_rows)
    grid = grid.sort_values(["ret_all", "n_all"], ascending=[False, False])
    selected_rows = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()
    classification = _classify(grid)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_hold7_entry_profile_grid",
        "input": str(INPUT_TRADES),
        "target_exit_variant": TARGET_EXIT_VARIANT,
        "base_rows_bull_sideways": int(len(base)),
        "grid_rows": int(len(grid)),
        "classification": classification,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_broker_changed": False,
            "gate_or_threshold_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "top_grid": _clean_records(grid.head(20)),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    grid.to_csv(OUT_GRID, index=False, encoding="utf-8-sig")
    selected_rows.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")

    viable = grid[
        (grid["n_all"] >= 20)
        & (grid["ret_all"] > 0)
        & (grid["ret_pre_2026"] > 0)
        & (grid["ret_2025H2"] > 0)
        & (grid["n_2026H1"] >= 1)
        & (grid["ret_2026H1"] > 0)
    ].sort_values(["ret_all", "n_all"], ascending=[False, False])
    lines = [
        "# C3 high-volatility hold7 entry grid",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Input: `{INPUT_TRADES}`",
        f"- Target exit variant: `{TARGET_EXIT_VARIANT}`",
        f"- Base rows BULL/SIDEWAYS: {len(base)}",
        f"- Grid rows: {len(grid)}",
        "- Scope: read-only grid search; no operating rule or gate changed.",
        "",
        "## Viable grids",
        "",
        "| rule_id | n_all | ret_all | PF_all | ret_2025H2 | n_2026H1 | ret_2026H1 | top5_share |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in viable.head(15).to_dict(orient="records"):
        pf = row["pf_all"]
        pf_text = "inf" if pd.isna(pf) and row["ret_all"] > 0 else f"{pf:.6f}" if not pd.isna(pf) else ""
        top5 = row.get("top5_share_of_gross_profit")
        top5_text = "" if pd.isna(top5) else f"{top5:.6f}"
        lines.append(
            f"| {row['rule_id']} | {row['n_all']} | {row['ret_all']:.6f} | {pf_text} | "
            f"{row['ret_2025H2']:.6f} | {row['n_2026H1']} | {row['ret_2026H1']:.6f} | {top5_text} |"
        )
    lines.extend(
        [
            "",
            "## Classification",
            "",
            f"- Research classification: `{classification['research_classification']}`",
            f"- Operational decision: `{classification['operational_decision']}`",
            f"- Full logic application: `{classification['full_logic_application']}`",
            f"- Confidence: `{classification['confidence']}`",
            f"- Best rule id: `{classification['best_rule_id']}`",
            f"- Reasons: {', '.join(classification['reason'])}",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "json": str(OUT_JSON),
                "grid": str(OUT_GRID),
                "rows": str(OUT_ROWS),
                "md": str(OUT_MD),
                "classification": classification,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
