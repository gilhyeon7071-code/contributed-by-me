#!/usr/bin/env python
"""Simulate current-data-fit policy candidates before changing live policy."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path("E:/1_Data")
LOG_DIR = ROOT / "2_Logs"
JOINED = LOG_DIR / "pnl_trade_signal_join_latest.csv"
OUT_JSON = LOG_DIR / "current_fit_policy_candidates_latest.json"
OUT_CSV = LOG_DIR / "current_fit_policy_candidates_latest.csv"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.lower().isin(["true", "1", "yes", "y"])


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _pf(pnl: pd.Series) -> float | None:
    wins = float(pnl[pnl > 0].sum())
    losses = float(-pnl[pnl < 0].sum())
    return wins / losses if losses > 0 else None


def _metrics(pnl: pd.Series, base: float) -> dict[str, Any]:
    pf = _pf(pnl)
    return {
        "total_pnl_krw": int(round(float(pnl.sum()))),
        "improvement_krw": int(round(float(pnl.sum()) - base)),
        "win_rate": round(float((pnl > 0).mean()) * 100.0, 4) if len(pnl) else None,
        "profit_factor": round(float(pf), 4) if pf is not None else None,
    }


def _scenario(df: pd.DataFrame, name: str, multipliers: pd.Series, base: float, note: str) -> dict[str, Any]:
    scenario_pnl = df["pnl_krw"] * multipliers
    affected = multipliers.ne(1.0)
    out = {
        "scenario": name,
        "affected_trades": int(affected.sum()),
        "affected_base_pnl_krw": int(round(float(df.loc[affected, "pnl_krw"].sum()))) if affected.any() else 0,
        "avg_multiplier": round(float(multipliers.mean()), 6),
        "note": note,
    }
    out.update(_metrics(scenario_pnl, base))
    return out


def main() -> None:
    if not JOINED.exists():
        raise FileNotFoundError(str(JOINED))
    df = pd.read_csv(JOINED, dtype=str, encoding="utf-8-sig")
    required = {"pnl_krw", "surge_immediate", "v_accel", "ret1_pct", "atr14_pct"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"missing columns: {missing}")
    for col in ["pnl_krw", "v_accel", "ret1_pct", "atr14_pct", "final_score"]:
        if col in df.columns:
            df[col] = _num(df[col])
    df["pnl_krw"] = df["pnl_krw"].fillna(0.0)
    df["surge_immediate"] = _bool(df["surge_immediate"])

    thresholds = {
        "v_accel_overheat": float(df["v_accel"].quantile(0.80)),
        "ret1_pct_overheat": float(df["ret1_pct"].quantile(0.80)),
        "atr14_pct_overheat": float(df["atr14_pct"].quantile(0.80)),
    }
    overheat_v = df["v_accel"].ge(thresholds["v_accel_overheat"])
    overheat_r = df["ret1_pct"].ge(thresholds["ret1_pct_overheat"])
    overheat_a = df["atr14_pct"].ge(thresholds["atr14_pct_overheat"])
    overheat_count = pd.concat([overheat_v, overheat_r, overheat_a], axis=1).sum(axis=1)
    surge = df["surge_immediate"]
    base = float(df["pnl_krw"].sum())

    ones = pd.Series(1.0, index=df.index)
    scenarios: list[dict[str, Any]] = []

    m = ones.copy()
    m.loc[surge] = 0.60
    scenarios.append(_scenario(df, "급등 1차비중 50%→30% 가정", m, base, "급등 split first_ratio를 0.5에서 0.3으로 낮추는 효과에 해당"))

    m = ones.copy()
    m.loc[overheat_v] *= 0.50
    scenarios.append(_scenario(df, "v_accel 과열 50% 축소", m, base, "거래량 가속 상위 20% 축소"))

    m = ones.copy()
    m.loc[overheat_r] *= 0.50
    scenarios.append(_scenario(df, "ret1_pct 과열 50% 축소", m, base, "당일 상승률 상위 20% 축소"))

    m = ones.copy()
    m.loc[overheat_a] *= 0.50
    scenarios.append(_scenario(df, "atr14_pct 과열 50% 축소", m, base, "변동성 상위 20% 축소"))

    m = ones.copy()
    m.loc[overheat_count.ge(2)] *= 0.50
    scenarios.append(_scenario(df, "과열 2개 이상 50% 축소", m, base, "v_accel/ret1_pct/atr14_pct 중 2개 이상 과열"))

    m = ones.copy()
    m.loc[surge] *= 0.60
    m.loc[overheat_count.ge(2)] *= 0.50
    scenarios.append(_scenario(df, "급등 30% + 과열2개 50% 축소", m, base, "급등 탐색진입 축소와 과열 복합 축소 결합"))

    m = ones.copy()
    m.loc[surge] *= 0.60
    m.loc[overheat_v] *= 0.50
    scenarios.append(_scenario(df, "급등 30% + v_accel 과열 50% 축소", m, base, "급등 탐색진입 축소와 최상위 손실 조건 결합"))

    ranked = sorted(scenarios, key=lambda x: x["improvement_krw"], reverse=True)
    pd.DataFrame(ranked).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    result = {
        "generated_at": _now(),
        "input": str(JOINED),
        "baseline": {
            "trades": int(len(df)),
            "pnl_krw": int(round(base)),
            "profit_factor": round(float(_pf(df["pnl_krw"])), 4) if _pf(df["pnl_krw"]) is not None else None,
        },
        "thresholds": thresholds,
        "ranked_scenarios": ranked,
        "recommended_first_policy": ranked[0] if ranked else {},
        "policy_status": "simulation_only_no_policy_change",
    }
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"latest": str(OUT_JSON), "csv": str(OUT_CSV), "recommended": result["recommended_first_policy"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
