#!/usr/bin/env python
"""Rank loss-heavy entry conditions from realized paper trades.

This script is read-only: it evaluates candidate filters/reductions and writes
diagnostic JSON/CSV files. It does not change trading policy.
"""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path("E:/1_Data")
LOG_DIR = ROOT / "2_Logs"
JOINED = LOG_DIR / "pnl_trade_signal_join_latest.csv"
OUT_JSON = LOG_DIR / "pnl_loss_conditions_latest.json"
OUT_CSV = LOG_DIR / "pnl_loss_conditions_ranked_latest.csv"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _num(s: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default)


def _bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.lower().isin(["true", "1", "yes", "y"])


def _safe_pf(pnl: pd.Series) -> float | None:
    wins = float(pnl[pnl > 0].sum())
    losses = float(-pnl[pnl < 0].sum())
    if losses <= 0:
        return None
    return wins / losses


def _segment(df: pd.DataFrame, mask: pd.Series) -> dict[str, Any]:
    part = df.loc[mask].copy()
    if part.empty:
        return {
            "trades": 0,
            "pnl_krw": 0,
            "win_rate": None,
            "avg_net_ret": None,
            "profit_factor": None,
            "avg_notional_krw": None,
        }
    pf = _safe_pf(part["pnl_krw"])
    return {
        "trades": int(len(part)),
        "pnl_krw": int(round(float(part["pnl_krw"].sum()))),
        "win_rate": round(float((part["pnl_krw"] > 0).mean()) * 100.0, 4),
        "avg_net_ret": round(float(part["net_ret"].mean()), 6),
        "profit_factor": round(float(pf), 4) if pf is not None else None,
        "avg_notional_krw": int(round(float(part["notional"].mean()))),
    }


def _condition_row(df: pd.DataFrame, name: str, mask: pd.Series, deployable: bool, note: str) -> dict[str, Any]:
    base = float(df["pnl_krw"].sum())
    affected = df.loc[mask]
    reduce_50 = df["pnl_krw"].copy()
    reduce_50.loc[mask] = reduce_50.loc[mask] * 0.5
    skip = df["pnl_krw"].copy()
    skip.loc[mask] = 0.0
    affected_pnl = float(affected["pnl_krw"].sum()) if not affected.empty else 0.0
    return {
        "condition": name,
        "deployable": bool(deployable),
        "trades": int(mask.sum()),
        "affected_pnl_krw": int(round(affected_pnl)),
        "affected_win_rate": round(float((affected["pnl_krw"] > 0).mean()) * 100.0, 4) if not affected.empty else None,
        "affected_profit_factor": round(float(_safe_pf(affected["pnl_krw"])), 4) if not affected.empty and _safe_pf(affected["pnl_krw"]) is not None else None,
        "reduce_50_total_pnl_krw": int(round(float(reduce_50.sum()))),
        "reduce_50_improvement_krw": int(round(float(reduce_50.sum()) - base)),
        "skip_total_pnl_krw": int(round(float(skip.sum()))),
        "skip_improvement_krw": int(round(float(skip.sum()) - base)),
        "note": note,
    }


def main() -> None:
    if not JOINED.exists():
        raise FileNotFoundError(str(JOINED))
    df = pd.read_csv(JOINED, dtype=str, encoding="utf-8-sig")
    required = {"pnl_krw", "net_ret", "notional", "surge_immediate", "split_entry"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"missing columns: {missing}")

    required_numeric = {"pnl_krw", "net_ret", "notional"}
    for col in ["pnl_krw", "net_ret", "notional", "final_score", "ret1_pct", "v_accel", "atr14_pct", "rsi14", "stoch_k", "adx14"]:
        if col in df.columns:
            if col in required_numeric:
                df[col] = _num(df[col])
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["surge_immediate", "split_entry", "sector_hrp_reduce", "signal_joined"]:
        if col in df.columns:
            df[col] = _bool(df[col])
    if "entry_dt" in df.columns:
        dt = pd.to_datetime(df["entry_dt"], errors="coerce")
        df["entry_hour"] = dt.dt.hour.fillna(-1).astype(int)
    else:
        df["entry_hour"] = -1
    if "entry_date" in df.columns and "exit_date" in df.columns:
        entry = df["entry_date"].astype(str).str.replace(r"[^0-9]", "", regex=True)
        exit_ = df["exit_date"].astype(str).str.replace(r"[^0-9]", "", regex=True)
        df["same_or_1d_exit"] = entry.eq(exit_) | (df.get("hold_days", pd.Series(0, index=df.index)).astype(str).eq("1"))
    else:
        df["same_or_1d_exit"] = False
    if "entry_month" not in df.columns and "entry_date" in df.columns:
        df["entry_month"] = df["entry_date"].astype(str).str[:6]

    conditions: list[dict[str, Any]] = []
    conditions.append(_condition_row(df, "급등 즉시진입 전체", df["surge_immediate"], True, "실전 적용 가능: 급등 즉시진입 금액 축소/허용 개수 축소"))
    conditions.append(_condition_row(df, "급등 즉시진입 + 분할진입", df["surge_immediate"] & df["split_entry"], True, "현재 손실이 유지되는 핵심 조합"))
    conditions.append(_condition_row(df, "분할진입 미적용", ~df["split_entry"], False, "대부분 과거 설정 구간 영향. 현재 정책 문제와 분리"))
    conditions.append(_condition_row(df, "진입 후 당일/1일 내 종료", df["same_or_1d_exit"], True, "짧은 보유 손실 방어 정책 후보"))
    conditions.append(_condition_row(df, "14시 이후 진입", df["entry_hour"].ge(14), True, "장후반 진입 축소 후보"))
    conditions.append(_condition_row(df, "점심 시간대 11~13시 진입", df["entry_hour"].between(11, 13), True, "시간대 품질 후보"))

    score_valid_mask = pd.Series(False, index=df.index)
    if "final_score" in df.columns and df["final_score"].notna().any():
        score_valid_mask = df["final_score"].notna() & df["final_score"].gt(0)
        if "signal_joined" in df.columns:
            score_valid_mask = score_valid_mask & df["signal_joined"]
        scored = df.loc[score_valid_mask].copy()
        if not scored.empty:
            q75 = float(scored["final_score"].quantile(0.75))
            q25 = float(scored["final_score"].quantile(0.25))
            conditions.append(_condition_row(df, f"final_score 상위 25% >= {q75:.4f}", score_valid_mask & df["final_score"].ge(q75), True, "높은 점수가 항상 우수하지 않은지 확인"))
            conditions.append(_condition_row(df, f"final_score 하위 25% <= {q25:.4f}", score_valid_mask & df["final_score"].le(q25), True, "낮은 점수 구간 품질 확인"))
            mid_mask = score_valid_mask & df["final_score"].between(q25, q75, inclusive="both")
            conditions.append(_condition_row(df, f"final_score 중간 50% {q25:.4f}~{q75:.4f}", mid_mask, True, "강화 후보 구간인지 확인"))

    for col in ["ret1_pct", "v_accel", "atr14_pct", "rsi14", "stoch_k"]:
        if col in df.columns and df[col].notna().any():
            q80 = float(df[col].quantile(0.80))
            conditions.append(_condition_row(df, f"{col} 상위 20% >= {q80:.4f}", df[col].ge(q80), True, f"{col} 과열 구간 후보"))

    ranked = sorted(conditions, key=lambda x: x["reduce_50_improvement_krw"], reverse=True)
    ranked_df = pd.DataFrame(ranked)
    ranked_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    deployable_ranked = [r for r in ranked if r["deployable"] and r["trades"] > 0 and r["reduce_50_improvement_krw"] > 0]
    result = {
        "generated_at": _now(),
        "input": str(JOINED),
        "baseline": _segment(df, pd.Series(True, index=df.index)),
        "ranked_conditions": ranked,
        "top_deployable_candidates": deployable_ranked[:5],
        "interpretation": {
            "primary": deployable_ranked[0]["condition"] if deployable_ranked else "",
            "policy_status": "diagnostic_only_no_policy_change",
        },
    }
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"latest": str(OUT_JSON), "csv": str(OUT_CSV), "primary": result["interpretation"]["primary"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
