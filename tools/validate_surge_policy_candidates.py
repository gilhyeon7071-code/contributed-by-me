from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


BASE = Path(r"E:\1_Data")
LOG_DIR = BASE / "2_Logs"
PAPER_DIR = BASE / "paper"

JOIN_PATH = LOG_DIR / "pnl_trade_signal_join_latest.csv"
FILLS_PATH = PAPER_DIR / "fills.csv"
OUT_PATH = LOG_DIR / "surge_policy_candidate_validation_latest.json"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_code(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = re.sub(r"\D", "", text)
    return digits.zfill(6) if digits else text


def _to_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes", "y"])


def _to_float(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


def _pct(value: float) -> float:
    return round(value * 100.0, 4)


def _won(value: float) -> int:
    return int(round(float(value)))


def _load_joined_trades() -> pd.DataFrame:
    if not JOIN_PATH.exists():
        raise FileNotFoundError(f"missing trade/signal join file: {JOIN_PATH}")
    df = pd.read_csv(JOIN_PATH, dtype=str, encoding="utf-8-sig")
    required = {"code", "pnl_krw", "net_ret", "notional", "surge_immediate"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"missing required columns in {JOIN_PATH}: {missing}")

    df["code"] = df["code"].map(_clean_code)
    for col in ["pnl_krw", "net_ret", "notional", "final_score"]:
        if col in df.columns:
            df[col] = _to_float(df[col])
    df["surge_immediate"] = _to_bool(df["surge_immediate"])
    if "split_entry" in df.columns:
        df["split_entry"] = _to_bool(df["split_entry"])
    return df


def _extract_note_value(note: str, key: str) -> str:
    if not note:
        return ""
    m = re.search(rf"(?:^|;){re.escape(key)}=([^;]*)", note)
    return m.group(1).strip() if m else ""


def _load_fill_exit_summary() -> dict[str, Any]:
    if not FILLS_PATH.exists():
        return {
            "available": False,
            "path": str(FILLS_PATH),
            "reason": "fills.csv 없음",
        }

    fills = pd.read_csv(FILLS_PATH, dtype=str, encoding="utf-8-sig")
    if fills.empty:
        return {"available": True, "rows": 0, "exit_reason_counts": {}}

    for col in ["code", "side", "note"]:
        if col not in fills.columns:
            fills[col] = ""
    fills["code"] = fills["code"].map(_clean_code)
    side = fills["side"].astype(str).str.upper()
    sells = fills[side.eq("SELL")].copy()
    if sells.empty:
        return {"available": True, "rows": int(len(fills)), "sell_rows": 0, "exit_reason_counts": {}}

    sells["exit_reason"] = sells["note"].map(lambda x: _extract_note_value(str(x), "exit_reason") or "UNKNOWN")
    sells["entry_order_id"] = sells["note"].map(lambda x: _extract_note_value(str(x), "entry_order_id"))
    sells["sell_ratio_pct"] = pd.to_numeric(
        sells["note"].map(lambda x: _extract_note_value(str(x), "sell_ratio_pct")),
        errors="coerce",
    )
    reason_counts = sells["exit_reason"].value_counts(dropna=False).to_dict()
    avg_ratio = (
        sells.groupby("exit_reason")["sell_ratio_pct"].mean().dropna().round(4).to_dict()
        if "sell_ratio_pct" in sells.columns
        else {}
    )
    return {
        "available": True,
        "path": str(FILLS_PATH),
        "rows": int(len(fills)),
        "sell_rows": int(len(sells)),
        "exit_reason_counts": {str(k): int(v) for k, v in reason_counts.items()},
        "avg_sell_ratio_pct_by_reason": {str(k): float(v) for k, v in avg_ratio.items()},
    }


def _scenario_size_scale(df: pd.DataFrame, mask: pd.Series, scale: float) -> dict[str, Any]:
    base = float(df["pnl_krw"].sum())
    affected = df.loc[mask].copy()
    scenario = df["pnl_krw"].copy()
    scenario.loc[mask] = scenario.loc[mask] * scale
    new_total = float(scenario.sum())
    return {
        "policy": f"급등 즉시진입 금액 {int(scale * 100)}% 적용",
        "deployable": True,
        "affected_trades": int(mask.sum()),
        "base_total_pnl_krw": _won(base),
        "scenario_total_pnl_krw": _won(new_total),
        "improvement_krw": _won(new_total - base),
        "affected_base_pnl_krw": _won(affected["pnl_krw"].sum()),
        "note": "체결 확률이나 신호 품질을 가정하지 않고 급등 즉시진입 금액만 축소한 보수적 시뮬레이션",
    }


def _scenario_loss_floor(df: pd.DataFrame, mask: pd.Series, floor_ret: float) -> dict[str, Any]:
    base = float(df["pnl_krw"].sum())
    scenario = df["pnl_krw"].copy()
    affected = df.loc[mask].copy()
    capped = affected.copy()
    capped["scenario_ret"] = capped["net_ret"].clip(lower=floor_ret)
    capped["scenario_pnl"] = capped["notional"] * capped["scenario_ret"]
    scenario.loc[mask] = capped["scenario_pnl"]
    new_total = float(scenario.sum())
    capped_count = int((affected["net_ret"] < floor_ret).sum())
    return {
        "policy": f"급등 즉시진입 손실률 {floor_ret:.0%} 하한 가정",
        "deployable": False,
        "affected_trades": int(mask.sum()),
        "capped_loss_trades": capped_count,
        "base_total_pnl_krw": _won(base),
        "scenario_total_pnl_krw": _won(new_total),
        "improvement_krw": _won(new_total - base),
        "note": "가격 경로 검증 전 손익 하한 가정이므로 원인 후보 검증용이며 즉시 정책값으로 쓰면 안 됨",
    }


def _scenario_size_and_floor(df: pd.DataFrame, mask: pd.Series, scale: float, floor_ret: float) -> dict[str, Any]:
    base = float(df["pnl_krw"].sum())
    scenario = df["pnl_krw"].copy()
    affected = df.loc[mask].copy()
    adjusted = affected.copy()
    adjusted["scenario_ret"] = adjusted["net_ret"].clip(lower=floor_ret)
    adjusted["scenario_pnl"] = adjusted["notional"] * adjusted["scenario_ret"] * scale
    scenario.loc[mask] = adjusted["scenario_pnl"]
    new_total = float(scenario.sum())
    return {
        "policy": f"급등 즉시진입 금액 {int(scale * 100)}% + 손실률 {floor_ret:.0%} 하한 가정",
        "deployable": False,
        "affected_trades": int(mask.sum()),
        "capped_loss_trades": int((affected["net_ret"] < floor_ret).sum()),
        "base_total_pnl_krw": _won(base),
        "scenario_total_pnl_krw": _won(new_total),
        "improvement_krw": _won(new_total - base),
        "note": "금액 축소는 실전 적용 가능하지만 손실률 하한은 체결 경로 검증 전까지 참고값",
    }


def _segment(df: pd.DataFrame, mask: pd.Series) -> dict[str, Any]:
    part = df.loc[mask].copy()
    if part.empty:
        return {
            "trades": 0,
            "pnl_krw": 0,
            "win_rate": None,
            "avg_net_ret": None,
            "profit_factor": None,
        }
    wins = part[part["pnl_krw"] > 0]["pnl_krw"].sum()
    losses = -part[part["pnl_krw"] < 0]["pnl_krw"].sum()
    pf = float(wins / losses) if losses > 0 else None
    return {
        "trades": int(len(part)),
        "pnl_krw": _won(part["pnl_krw"].sum()),
        "win_rate": _pct(float((part["pnl_krw"] > 0).mean())),
        "avg_net_ret": round(float(part["net_ret"].mean()), 6),
        "profit_factor": round(pf, 4) if pf is not None else None,
        "avg_notional_krw": _won(part["notional"].mean()),
    }


def main() -> None:
    df = _load_joined_trades()
    surge_mask = df["surge_immediate"]
    non_surge_mask = ~surge_mask

    scenarios = [
        _scenario_size_scale(df, surge_mask, 0.50),
        _scenario_size_scale(df, surge_mask, 0.25),
        _scenario_loss_floor(df, surge_mask, -0.05),
        _scenario_loss_floor(df, surge_mask, -0.04),
        _scenario_size_and_floor(df, surge_mask, 0.50, -0.05),
    ]
    scenarios = sorted(scenarios, key=lambda x: x["improvement_krw"], reverse=True)

    score_segments: dict[str, Any] = {}
    if "final_score" in df.columns and df["final_score"].notna().any():
        scored = df[df["final_score"].notna()].copy()
        if not scored.empty:
            scored["score_bucket"] = pd.qcut(scored["final_score"], q=4, duplicates="drop")
            score_segments = {
                str(bucket): _segment(scored, scored["score_bucket"].eq(bucket))
                for bucket in scored["score_bucket"].dropna().unique()
            }

    result = {
        "generated_at": _now(),
        "input": {
            "trade_signal_join": str(JOIN_PATH),
            "fills": str(FILLS_PATH),
        },
        "baseline": {
            "all_trades": _segment(df, pd.Series(True, index=df.index)),
            "surge_immediate": _segment(df, surge_mask),
            "non_surge": _segment(df, non_surge_mask),
        },
        "surge_exit_forensics": _load_fill_exit_summary(),
        "candidate_scenarios_ranked": scenarios,
        "final_score_segments": score_segments,
        "recommendation": {
            "primary": "급등 즉시진입 금액 50% 축소를 1순위 후보로 검증",
            "reason": "현재 데이터에서 급등 즉시진입은 손익과 PF가 크게 낮고, 금액 축소는 미래 정보 없이 적용 가능한 보수적 개선안입니다.",
            "do_not_apply_yet": [
                "손실률 하한 가정은 가격 경로·체결 가능성 검증 전까지 정책값으로 확정하지 않음",
                "재확인 진입은 사전 관찰 피처 저장이 부족해 별도 로그 수집 후 검증 필요",
            ],
        },
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result["recommendation"], ensure_ascii=False))
    print(str(OUT_PATH))


if __name__ == "__main__":
    main()
