from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_CSV = LOG_DIR / "normal_intraday_entry_context_latest.csv"
OUT_JSON = LOG_DIR / "normal_intraday_policy_relabel_latest.json"
OUT_CSV = LOG_DIR / "normal_intraday_policy_relabel_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "normal_intraday_policy_relabel_summary_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return default
        return val
    except Exception:
        return default


def _has_flag(row: pd.Series, flag: str) -> bool:
    flags = str(row.get("risk_flags", "") or "")
    return flag in {x.strip() for x in flags.split("|") if x.strip()}


def _candidate_rules(row: pd.Series) -> Dict[str, bool]:
    p1_decision = str(row.get("p1_entry_gate_decision_before_p1", "") or "").upper()
    net = _to_float(row.get("net_ret_sum"), 0.0)
    return {
        "R1_block_if_p1_not_allow": p1_decision in {"CAUTION", "REDUCE", "BLOCK"},
        "R2_block_if_p0_rolling_dd_ge_10pct": _has_flag(row, "p0_rolling_dd_ge_10pct"),
        "R3_block_if_daily_loss_negative": _has_flag(row, "p0_daily_loss_active_negative"),
        "R4_block_if_crash_proxy_dd_ge_12pct": _has_flag(row, "p0_crash_proxy_dd_ge_12pct"),
        "R5_block_if_p1_not_allow_and_daily_loss": (
            p1_decision in {"CAUTION", "REDUCE", "BLOCK"}
            and _has_flag(row, "p0_daily_loss_active_negative")
        ),
        "R6_block_if_p1_not_allow_and_rolling_dd": (
            p1_decision in {"CAUTION", "REDUCE", "BLOCK"}
            and _has_flag(row, "p0_rolling_dd_ge_10pct")
        ),
        "R7_block_if_two_or_more_context_flags": _to_float(row.get("risk_flag_count"), 0.0) >= 2,
        "BASE_actual_loss": net < 0,
    }


def _summarize_rule(df: pd.DataFrame, rule_col: str) -> Dict[str, Any]:
    net = pd.to_numeric(df["net_ret_sum"], errors="coerce").fillna(0.0)
    blocked = df[rule_col].astype(bool)
    allowed = ~blocked
    avoided = net[blocked & (net < 0)].abs().sum()
    opportunity_cost = net[blocked & (net > 0)].sum()
    allowed_net = net[allowed].sum()
    actual_net = net.sum()
    simulated_net = allowed_net
    return {
        "rule": rule_col,
        "blocked_entries": int(blocked.sum()),
        "allowed_entries": int(allowed.sum()),
        "blocked_losses": int((blocked & (net < 0)).sum()),
        "blocked_winners": int((blocked & (net > 0)).sum()),
        "loss_avoided_abs": float(avoided),
        "opportunity_cost_positive_ret": float(opportunity_cost),
        "actual_net_sum": float(actual_net),
        "simulated_allowed_net_sum": float(simulated_net),
        "net_improvement_vs_actual": float(simulated_net - actual_net),
    }


def run() -> Dict[str, Any]:
    ctx = _read_csv(INPUT_CSV)
    if ctx.empty:
        payload = {"generated_at": _now_ts(), "status": "NO_INPUT", "input": str(INPUT_CSV)}
        OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    work = ctx.copy()
    work["net_ret_sum"] = pd.to_numeric(work["net_ret_sum"], errors="coerce").fillna(0.0)
    for key in _candidate_rules(work.iloc[0]).keys():
        work[key] = False
    for idx, row in work.iterrows():
        rules = _candidate_rules(row)
        for key, value in rules.items():
            work.at[idx, key] = bool(value)

    rule_cols = [c for c in work.columns if c.startswith("R")]
    summary = pd.DataFrame([_summarize_rule(work, c) for c in rule_cols])
    summary = summary.sort_values(
        ["net_improvement_vs_actual", "blocked_entries"],
        ascending=[False, True],
        kind="mergesort",
    )

    best = summary.iloc[0].to_dict() if not summary.empty else {}
    detail_cols = [
        "entry_trace_id",
        "code",
        "entry_ts",
        "entry_ymd",
        "horizon",
        "net_ret_sum",
        "exit_reasons",
        "risk_flags",
        "risk_flag_count",
        "p1_entry_gate_decision_before_p1",
        "p0_last_day_ret",
        "p0_max_drawdown_pct",
        "p0_crash_max_dd",
    ] + rule_cols
    work[[c for c in detail_cols if c in work.columns]].to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": _now_ts(),
        "status": "OK",
        "input": str(INPUT_CSV),
        "outputs": {"detail_csv": str(OUT_CSV), "summary_csv": str(OUT_SUMMARY_CSV)},
        "counts": {
            "rows": int(len(work)),
            "loss_rows": int((work["net_ret_sum"] < 0).sum()),
            "winner_rows": int((work["net_ret_sum"] > 0).sum()),
        },
        "actual": {
            "net_sum": float(work["net_ret_sum"].sum()),
            "mean_net": float(work["net_ret_sum"].mean()) if len(work) else 0.0,
        },
        "best_rule": best,
        "summary": summary.to_dict(orient="records"),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    print(json.dumps(run(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
