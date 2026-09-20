#!/usr/bin/env python
"""Diagnose overlap between surge-immediate trades and overheat indicators."""

from __future__ import annotations

import itertools
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path("E:/1_Data")
LOG_DIR = ROOT / "2_Logs"
JOINED = LOG_DIR / "pnl_trade_signal_join_latest.csv"
OUT_JSON = LOG_DIR / "overheat_surge_overlap_latest.json"
OUT_CSV = LOG_DIR / "overheat_surge_overlap_ranked_latest.csv"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _to_float(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _to_bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.lower().isin(["true", "1", "yes", "y"])


def _profit_factor(pnl: pd.Series) -> float | None:
    wins = float(pnl[pnl > 0].sum())
    losses = float(-pnl[pnl < 0].sum())
    if losses <= 0:
        return None
    return wins / losses


def _eval_mask(df: pd.DataFrame, name: str, mask: pd.Series, deployable: bool, components: list[str]) -> dict[str, Any]:
    base = float(df["pnl_krw"].sum())
    affected = df.loc[mask].copy()
    reduce_50 = df["pnl_krw"].copy()
    reduce_50.loc[mask] = reduce_50.loc[mask] * 0.5
    skip = df["pnl_krw"].copy()
    skip.loc[mask] = 0.0
    pf = _profit_factor(affected["pnl_krw"]) if not affected.empty else None
    return {
        "condition": name,
        "components": components,
        "deployable": bool(deployable),
        "trades": int(mask.sum()),
        "affected_pnl_krw": int(round(float(affected["pnl_krw"].sum()))) if not affected.empty else 0,
        "affected_win_rate": round(float((affected["pnl_krw"] > 0).mean()) * 100.0, 4) if not affected.empty else None,
        "affected_profit_factor": round(float(pf), 4) if pf is not None else None,
        "reduce_50_improvement_krw": int(round(float(reduce_50.sum()) - base)),
        "skip_improvement_krw": int(round(float(skip.sum()) - base)),
        "share_of_total_loss_pct": round(
            float(-affected[affected["pnl_krw"] < 0]["pnl_krw"].sum()) / max(float(-df[df["pnl_krw"] < 0]["pnl_krw"].sum()), 1.0) * 100.0,
            4,
        )
        if not affected.empty
        else 0.0,
    }


def main() -> None:
    if not JOINED.exists():
        raise FileNotFoundError(str(JOINED))
    df = pd.read_csv(JOINED, dtype=str, encoding="utf-8-sig")
    for col in ["pnl_krw", "net_ret", "notional", "v_accel", "ret1_pct", "atr14_pct", "final_score"]:
        if col in df.columns:
            df[col] = _to_float(df[col])
    for col in ["surge_immediate", "split_entry", "signal_joined"]:
        if col in df.columns:
            df[col] = _to_bool(df[col])
    df["pnl_krw"] = df["pnl_krw"].fillna(0.0)
    df["net_ret"] = df["net_ret"].fillna(0.0)
    df["notional"] = df["notional"].fillna(0.0)

    metrics = {
        "v_accel": 0.80,
        "ret1_pct": 0.80,
        "atr14_pct": 0.80,
    }
    masks: dict[str, pd.Series] = {
        "급등 즉시진입": df.get("surge_immediate", pd.Series(False, index=df.index)).fillna(False),
    }
    thresholds: dict[str, float] = {}
    for col, q in metrics.items():
        if col in df.columns and df[col].notna().any():
            threshold = float(df[col].quantile(q))
            thresholds[f"{col} 과열"] = threshold
            masks[f"{col} 과열"] = df[col].ge(threshold)

    score_valid = pd.Series(False, index=df.index)
    if "final_score" in df.columns and df["final_score"].notna().any():
        score_valid = df["final_score"].notna() & df["final_score"].gt(0)
        if "signal_joined" in df.columns:
            score_valid = score_valid & df["signal_joined"]
        if score_valid.any():
            low_score_threshold = float(df.loc[score_valid, "final_score"].quantile(0.25))
            thresholds["final_score 저점"] = low_score_threshold
            masks["final_score 하위25"] = score_valid & df["final_score"].le(low_score_threshold)

    rows: list[dict[str, Any]] = []
    for name, mask in masks.items():
        rows.append(_eval_mask(df, name, mask, True, [name]))

    keys = list(masks.keys())
    for r in range(2, min(5, len(keys)) + 1):
        for combo in itertools.combinations(keys, r):
            mask = pd.Series(True, index=df.index)
            for key in combo:
                mask = mask & masks[key]
            if int(mask.sum()) <= 0:
                continue
            rows.append(_eval_mask(df, " + ".join(combo), mask, True, list(combo)))

    ranked = sorted(rows, key=lambda x: (x["reduce_50_improvement_krw"], x["skip_improvement_krw"]), reverse=True)
    pd.DataFrame(ranked).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    result = {
        "generated_at": _now(),
        "input": str(JOINED),
        "thresholds": thresholds,
        "baseline": {
            "trades": int(len(df)),
            "pnl_krw": int(round(float(df["pnl_krw"].sum()))),
            "loss_krw": int(round(float(df[df["pnl_krw"] < 0]["pnl_krw"].sum()))),
        },
        "ranked_overlap_conditions": ranked,
        "top_policy_candidates": [r for r in ranked if r["trades"] >= 5 and r["reduce_50_improvement_krw"] > 0][:8],
        "policy_status": "diagnostic_only_no_policy_change",
    }
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"latest": str(OUT_JSON), "csv": str(OUT_CSV), "top": result["top_policy_candidates"][:3]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
