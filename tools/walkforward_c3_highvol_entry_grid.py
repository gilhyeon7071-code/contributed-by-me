from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_TRADES = LOG_DIR / "c3_highvol_exit_variant_compare_trades_latest.csv"

OUT_JSON = LOG_DIR / "c3_highvol_walkforward_grid_latest.json"
OUT_GRID = LOG_DIR / "c3_highvol_walkforward_grid_candidates_latest.csv"
OUT_SELECTED = LOG_DIR / "c3_highvol_walkforward_grid_selected_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_walkforward_grid_latest.md"

TARGET_EXIT_VARIANT = "hold7_stop6"


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _profit_factor(ret: pd.Series) -> float | None:
    gains = float(ret[ret > 0].sum())
    losses = float(-ret[ret < 0].sum())
    if losses == 0:
        return None if gains > 0 else 0.0
    return gains / losses


def _mask_window(df: pd.DataFrame, window: str) -> pd.Series:
    dt = pd.to_datetime(df["entry_date_norm"], errors="coerce")
    if window == "pre_2025H2":
        return dt < "2025-07-01"
    if window == "2025H2":
        return (dt >= "2025-07-01") & (dt <= "2025-12-31")
    if window == "pre_2026":
        return dt < "2026-01-01"
    if window == "2026H1":
        return (dt >= "2026-01-01") & (dt <= "2026-06-30")
    if window == "all":
        return pd.Series(True, index=df.index)
    raise ValueError(window)


def _summary(frame: pd.DataFrame, prefix: str) -> dict[str, Any]:
    ret = _num(frame["ret"]) if len(frame) else pd.Series(dtype=float)
    return {
        f"{prefix}_n": int(len(frame)),
        f"{prefix}_win_n": int((ret > 0).sum()) if len(ret) else 0,
        f"{prefix}_loss_n": int((ret < 0).sum()) if len(ret) else 0,
        f"{prefix}_stop_n": int((frame["exit_reason"].astype(str).str.upper() == "STOP").sum()) if len(frame) else 0,
        f"{prefix}_ret": float(ret.sum()) if len(ret) else 0.0,
        f"{prefix}_pf": _profit_factor(ret),
    }


def _rule_mask(df: pd.DataFrame, row: dict[str, Any]) -> pd.Series:
    return (
        df["entry_gap_pct"].ge(float(row["gap_lo"]))
        & df["entry_gap_pct"].lt(float(row["gap_hi"]))
        & df["signal_v_accel"].ge(float(row["v_accel_lo"]))
        & df["signal_v_accel"].lt(float(row["v_accel_hi"]))
        & df["signal_atr_pct"].ge(float(row["atr_lo"]))
        & df["signal_atr_pct"].lt(float(row["atr_hi"]))
    )


def _clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _classify(selected: pd.DataFrame) -> dict[str, Any]:
    reasons: list[str] = []
    if selected.empty:
        return {
            "operational_decision": "NOT_APPROVED",
            "research_classification": "C3_HIGHVOL_WALKFORWARD_NO_SELECTION",
            "confidence": "LOW_RESEARCH_ONLY",
            "full_logic_application": "NOT_APPLIED",
            "reason": ["no_train_selection"],
        }
    positive_tests = int((selected["test_ret"] > 0).sum())
    total_tests = int(len(selected))
    reasons.append(f"positive_tests:{positive_tests}/{total_tests}")
    if positive_tests < total_tests:
        reasons.append("not_all_forward_tests_positive")
    if int(selected["test_n"].min()) < 3:
        reasons.append("small_forward_sample")
    if float(selected["train_ret"].sum()) > 0 and float(selected["test_ret"].sum()) > 0:
        reasons.append("aggregate_train_and_test_positive")
    return {
        "operational_decision": "NOT_APPROVED",
        "research_classification": "C3_HIGHVOL_WALKFORWARD_WEAK_POSITIVE",
        "confidence": "LOW_TO_MEDIUM_RESEARCH_ONLY",
        "full_logic_application": "NOT_APPLIED",
        "reason": reasons,
    }


def main() -> int:
    if not INPUT_TRADES.exists():
        raise FileNotFoundError(INPUT_TRADES)
    df = pd.read_csv(INPUT_TRADES, dtype={"code": str})
    hold = df[df["exit_variant"].astype(str).eq(TARGET_EXIT_VARIANT)].copy()
    for col in ["entry_gap_pct", "signal_v_accel", "signal_atr_pct", "ret"]:
        hold[col] = _num(hold[col])
    base = hold[hold["market_regime"].astype(str).isin(["BULL", "SIDEWAYS"])].copy()

    gap_his = [0.025, 0.03, 0.035, 0.04, 0.05]
    vacc_his = [1.5, 1.75, 2.0, 2.5]
    atr_ranges = [(0.03, 0.04), (0.028, 0.04), (0.03, 0.045), (0.028, 0.045)]
    windows = [
        {"walk_id": "train_pre_2025H2_test_2025H2", "train": "pre_2025H2", "test": "2025H2", "min_train_n": 5},
        {"walk_id": "train_pre_2026_test_2026H1", "train": "pre_2026", "test": "2026H1", "min_train_n": 10},
    ]

    candidate_rows: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    for win in windows:
        for gap_hi in gap_his:
            for vacc_hi in vacc_his:
                for atr_lo, atr_hi in atr_ranges:
                    rule = {
                        "gap_lo": 0.0147299509,
                        "gap_hi": gap_hi,
                        "v_accel_lo": 1.255833911,
                        "v_accel_hi": vacc_hi,
                        "atr_lo": atr_lo,
                        "atr_hi": atr_hi,
                    }
                    rule_id = (
                        f"gap_0.0147_{gap_hi:.3f}__vacc_1.256_{vacc_hi:.2f}"
                        f"__atr_{atr_lo:.3f}_{atr_hi:.3f}"
                    )
                    selected = base[_rule_mask(base, rule).fillna(False)].copy()
                    train = selected[_mask_window(selected, win["train"]).fillna(False)].copy()
                    test = selected[_mask_window(selected, win["test"]).fillna(False)].copy()
                    row: dict[str, Any] = {"walk_id": win["walk_id"], "train_window": win["train"], "test_window": win["test"], "rule_id": rule_id, **rule}
                    row.update(_summary(train, "train"))
                    row.update(_summary(test, "test"))
                    row.update(_summary(selected, "all"))
                    row["eligible"] = bool(
                        row["train_n"] >= int(win["min_train_n"])
                        and row["train_ret"] > 0
                        and (row["train_pf"] or 0) > 1
                    )
                    candidate_rows.append(row)

        candidates = pd.DataFrame([r for r in candidate_rows if r["walk_id"] == win["walk_id"]])
        eligible = candidates[candidates["eligible"]].sort_values(["train_ret", "train_n"], ascending=[False, False])
        if not eligible.empty:
            selected_rows.append(eligible.iloc[0].to_dict())

    candidates_df = pd.DataFrame(candidate_rows)
    selected_df = pd.DataFrame(selected_rows)
    classification = _classify(selected_df)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_walkforward_highvol_entry_grid",
        "input": str(INPUT_TRADES),
        "target_exit_variant": TARGET_EXIT_VARIANT,
        "base_rows_bull_sideways": int(len(base)),
        "walk_windows": windows,
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
        "selected": _clean_records(selected_df),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    candidates_df.to_csv(OUT_GRID, index=False, encoding="utf-8-sig")
    selected_df.to_csv(OUT_SELECTED, index=False, encoding="utf-8-sig")

    lines = [
        "# C3 high-volatility walk-forward grid",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Input: `{INPUT_TRADES}`",
        f"- Target exit variant: `{TARGET_EXIT_VARIANT}`",
        f"- Base rows BULL/SIDEWAYS: {len(base)}",
        "- Scope: read-only walk-forward grid; no operating rule or gate changed.",
        "",
        "## Selected rules",
        "",
        "| walk_id | rule_id | train_n | train_ret | train_pf | test_n | test_ret | test_pf |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in selected_df.to_dict(orient="records"):
        train_pf = row["train_pf"]
        test_pf = row["test_pf"]
        train_pf_text = "inf" if pd.isna(train_pf) and row["train_ret"] > 0 else f"{train_pf:.6f}" if not pd.isna(train_pf) else ""
        test_pf_text = "inf" if pd.isna(test_pf) and row["test_ret"] > 0 else f"{test_pf:.6f}" if not pd.isna(test_pf) else ""
        lines.append(
            f"| {row['walk_id']} | {row['rule_id']} | {row['train_n']} | {row['train_ret']:.6f} | "
            f"{train_pf_text} | {row['test_n']} | {row['test_ret']:.6f} | {test_pf_text} |"
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
            f"- Reasons: {', '.join(classification['reason'])}",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "json": str(OUT_JSON),
                "grid": str(OUT_GRID),
                "selected": str(OUT_SELECTED),
                "md": str(OUT_MD),
                "classification": classification,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
