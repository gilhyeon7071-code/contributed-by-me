from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
NATIVE_CONTRACT = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
RESEARCH_PARAMS = LOG_DIR / "c3_strategy_replay_params_latest.json"
COMPARISON_JSON = LOG_DIR / "c3_strategy_definition_comparison_latest.json"
COMPARISON_ROWS = LOG_DIR / "c3_strategy_definition_comparison_rows_latest.csv"
ADAPTER_BREAKDOWN = LOG_DIR / "c3_adapter_filter_breakdown_latest.json"
REPLAY_JSON = LOG_DIR / "c3_strategy_param_replay_latest.json"

LATEST_CONTRACT_JSON = LOG_DIR / "c3_e_definition_contract_latest.json"
LATEST_REPORT_JSON = LOG_DIR / "c3_e_definition_report_latest.json"
LATEST_ROWS_CSV = LOG_DIR / "c3_e_definition_rows_latest.csv"
LATEST_MD = LOG_DIR / "c3_e_definition_contract_latest.md"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _summarize(df: pd.DataFrame) -> dict[str, Any]:
    ret = pd.to_numeric(df.get("ret", pd.Series(dtype=float)), errors="coerce").dropna()
    wins = ret[ret > 0]
    losses = ret[ret < 0]
    gross_profit = float(wins.sum())
    gross_loss = float(-losses.sum())
    if not len(ret):
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


def main() -> int:
    native_contract_payload = _read_json(NATIVE_CONTRACT)
    selection_contract = native_contract_payload.get("selection_contract", native_contract_payload)
    research_params = _read_json(RESEARCH_PARAMS)
    comparison = _read_json(COMPARISON_JSON)
    breakdown = _read_json(ADAPTER_BREAKDOWN)
    replay = _read_json(REPLAY_JSON)
    rows = pd.read_csv(COMPARISON_ROWS, dtype={"code": str}, encoding="utf-8-sig")
    e_rows = rows[rows["definition"].eq("E_native_plus_followthrough_and_market")].copy()
    e_summary = _summarize(e_rows)

    contract = {
        "contract_status": "READ_ONLY_C3_E_DEFINITION_CONTRACT_NOT_OPERATIONAL",
        "definition_id": "C3_E_NATIVE_FOLLOWTHROUGH_MARKET_RECHECK",
        "definition_label": "C3 native contract + followthrough + market_after_recheck",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "native_selection_contract": selection_contract,
        "research_params_path": str(RESEARCH_PARAMS),
        "research_params_boundary": {
            "promoted": bool(research_params.get("promoted", False)),
            "operational_use_allowed": False,
            "purpose": "read-only replay only; neutralizes generic stable value_min conflict for C3 research",
        },
        "post_replay_required_filters": [
            {"field": "followthrough_1d", "op": "==", "value": 1, "source": "report native trade field"},
            {"field": "market_after_recheck", "op": "nonempty", "value": True, "source": "cross-variant validation join"},
        ],
        "post_replay_diagnostic_filters": [
            {"field": "c3_after_recheck", "op": "==", "value": 1, "reason": "too narrow as mandatory in current evidence"},
            {"field": "score", "op": ">=", "value": 1.0, "reason": "diagnostic only in current evidence"},
            {"field": "signal_v_accel", "op": ">=", "value": 0.9, "reason": "already mostly retained; not selected as E mandatory beyond native contract"},
        ],
        "evidence": {
            "input_native_rows": int(comparison.get("input_rows", 0)),
            "definition_summary": e_summary,
            "comparison_summary": comparison.get("summary", []),
            "adapter_breakdown_status": breakdown.get("status"),
            "native_replay_status": replay.get("status"),
            "native_replay_run_dir": replay.get("run_dir"),
        },
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "next_required_before_integration": [
            "out-of-sample or later-window replay under the same definition",
            "strategy-scoped parameter contract approval path",
            "separate decision on whether market_after_recheck source is available at decision time",
        ],
    }
    report = {
        "created_at": contract["created_at"],
        "status": "READ_ONLY_C3_E_DEFINITION_REPORT_NOT_OPERATIONAL",
        "definition_id": contract["definition_id"],
        "summary": e_summary,
        "row_count": int(len(e_rows)),
        "rows_csv": str(LATEST_ROWS_CSV),
        "contract_json": str(LATEST_CONTRACT_JSON),
        "operation_effect": contract["operation_effect"],
        "full_logic_application": "NOT_APPLIED",
        "boundary": [
            "research definition artifact only",
            "not candidate generation",
            "not HPO",
            "not paper/live",
            "not parameter promotion",
        ],
    }

    e_rows.to_csv(LATEST_ROWS_CSV, index=False, encoding="utf-8-sig")
    LATEST_CONTRACT_JSON.write_text(json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8")
    LATEST_REPORT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# C3 E definition contract",
        "",
        f"- status: {contract['contract_status']}",
        f"- definition_id: {contract['definition_id']}",
        f"- row_count: {len(e_rows)}",
        f"- win_rate: {e_summary['win_rate']}",
        f"- ret_sum: {e_summary['ret_sum']}",
        f"- profit_factor: {e_summary['profit_factor']}",
        "",
        "## Required filters",
        "",
    ]
    for item in contract["post_replay_required_filters"]:
        lines.append(f"- {item['field']} {item['op']} {item['value']}")
    lines.extend(["", "## Diagnostic filters not mandatory", ""])
    for item in contract["post_replay_diagnostic_filters"]:
        lines.append(f"- {item['field']} {item['op']} {item['value']}: {item['reason']}")
    lines.extend(["", "## Boundary", ""])
    for item in report["boundary"]:
        lines.append(f"- {item}")
    LATEST_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
