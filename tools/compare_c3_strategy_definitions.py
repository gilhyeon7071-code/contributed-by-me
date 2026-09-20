from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
DETAIL = LOG_DIR / "c3_adapter_filter_breakdown_rows_latest.csv"
OVERLAP = LOG_DIR / "c3_strategy_param_replay_overlap_latest.json"

LATEST_JSON = LOG_DIR / "c3_strategy_definition_comparison_latest.json"
LATEST_SUMMARY_CSV = LOG_DIR / "c3_strategy_definition_comparison_summary_latest.csv"
LATEST_ROWS_CSV = LOG_DIR / "c3_strategy_definition_comparison_rows_latest.csv"
LATEST_MD = LOG_DIR / "c3_strategy_definition_comparison_latest.md"


def _summarize(df: pd.DataFrame) -> dict[str, Any]:
    ret = pd.to_numeric(df.get("ret", pd.Series(dtype=float)), errors="coerce").dropna()
    wins = ret[ret > 0]
    losses = ret[ret < 0]
    gross_profit = float(wins.sum())
    gross_loss = float(-losses.sum())
    if len(ret) == 0:
        pf: Any = None
    elif gross_loss > 0:
        pf = round(gross_profit / gross_loss, 6)
    elif gross_profit > 0:
        pf = "inf"
    else:
        pf = None
    return {
        "n": int(len(df)),
        "win_n": int((ret > 0).sum()),
        "loss_n": int((ret < 0).sum()),
        "win_rate": round(float((ret > 0).mean()), 6) if len(ret) else None,
        "ret_sum": round(float(ret.sum()), 6) if len(ret) else 0.0,
        "ret_mean": round(float(ret.mean()), 6) if len(ret) else None,
        "profit_factor": pf,
    }


def _bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(False, index=df.index)
    raw = df[col]
    if raw.dtype == bool:
        return raw.fillna(False)
    return raw.astype(str).str.lower().isin(["true", "1", "yes", "y"])


def main() -> int:
    detail = pd.read_csv(DETAIL, dtype={"code": str}, encoding="utf-8-sig")
    masks = {
        "A_native_c3_contract": pd.Series(True, index=detail.index),
        "B_native_plus_followthrough": _bool_series(detail, "followthrough_1d_eq_1"),
        "C_native_plus_market_after_recheck": _bool_series(detail, "market_after_recheck_nonempty"),
        "D_native_plus_c3_after_recheck": _bool_series(detail, "c3_after_recheck_eq_1"),
        "E_native_plus_followthrough_and_market": _bool_series(detail, "followthrough_1d_eq_1") & _bool_series(detail, "market_after_recheck_nonempty"),
        "F_full_current_adapter": _bool_series(detail, "adapter_all_conditions"),
    }
    descriptions = {
        "A_native_c3_contract": "native C3 selection contract only",
        "B_native_plus_followthrough": "native C3 plus entry-day followthrough",
        "C_native_plus_market_after_recheck": "native C3 plus market_after_recheck present",
        "D_native_plus_c3_after_recheck": "native C3 plus c3_after_recheck=1",
        "E_native_plus_followthrough_and_market": "native C3 plus followthrough and market_after_recheck",
        "F_full_current_adapter": "current full post-replay adapter",
    }
    summary_rows: list[dict[str, Any]] = []
    detail_rows: list[pd.DataFrame] = []
    for name, mask in masks.items():
        subset = detail[mask.fillna(False)].copy()
        row = {"definition": name, "description": descriptions[name]}
        row.update(_summarize(subset))
        summary_rows.append(row)
        if not subset.empty:
            tmp = subset.copy()
            tmp["definition"] = name
            detail_rows.append(tmp)

    summary = pd.DataFrame(summary_rows)
    combined_rows = pd.concat(detail_rows, ignore_index=True) if detail_rows else pd.DataFrame()
    overlap = json.loads(OVERLAP.read_text(encoding="utf-8-sig")) if OVERLAP.exists() else {}

    # Preference is not an operational decision. It is a research ranking by sample size + PF + not-over-narrow.
    candidates = summary[summary["definition"].isin(["A_native_c3_contract", "B_native_plus_followthrough", "C_native_plus_market_after_recheck", "E_native_plus_followthrough_and_market"])].copy()
    candidates["pf_numeric"] = pd.to_numeric(candidates["profit_factor"].replace("inf", float("inf")), errors="coerce")
    candidates["research_rank_score"] = candidates["ret_sum"].astype(float) + candidates["pf_numeric"].clip(upper=10).fillna(0) * 0.01 + candidates["n"].astype(float) * 0.001
    ranked = candidates.sort_values(["research_rank_score", "n"], ascending=False)
    suggested_research_definition = str(ranked.iloc[0]["definition"]) if not ranked.empty else ""

    payload = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "READ_ONLY_C3_STRATEGY_DEFINITION_COMPARISON_NOT_OPERATIONAL",
        "input_rows": int(len(detail)),
        "summary": summary.to_dict("records"),
        "suggested_research_definition": suggested_research_definition,
        "suggestion_boundary": "research ranking only; not an operational approval or policy change",
        "overlap_reference": {
            "old_rows": overlap.get("old_rows"),
            "old_in_native": overlap.get("old_in_native"),
            "old_in_adapter": overlap.get("old_in_adapter"),
        },
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "summary_csv": str(LATEST_SUMMARY_CSV),
        "rows_csv": str(LATEST_ROWS_CSV),
    }

    summary.to_csv(LATEST_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    combined_rows.to_csv(LATEST_ROWS_CSV, index=False, encoding="utf-8-sig")
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# C3 strategy definition comparison",
        "",
        f"- status: {payload['status']}",
        f"- input_rows: {payload['input_rows']}",
        f"- suggested_research_definition: {suggested_research_definition}",
        "",
        "## Definitions",
        "",
    ]
    for row in summary.to_dict("records"):
        lines.append(
            f"- {row['definition']}: n={row['n']}, win_rate={row['win_rate']}, ret_sum={row['ret_sum']}, pf={row['profit_factor']} - {row['description']}"
        )
    LATEST_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
