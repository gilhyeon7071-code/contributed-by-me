from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SCAN_DIR = LOG_DIR / "research_report_entry_trigger_v2_transition_value2b_ret1_1_relaxation_scope_scan_20260713_132200"
OLD_CONTRACTS = SCAN_DIR / "relaxation_contracts.json"
OLD_BALANCED_TRADES = SCAN_DIR / "balanced_original_trades.csv"
CURRENT_SELECTION_CONTRACT = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
CURRENT_REPLAY_JSON = LOG_DIR / "c3_readonly_official_replay_latest.json"
CURRENT_NATIVE = LOG_DIR / "c3_readonly_official_replay_native_latest.csv"
CURRENT_ADAPTER = LOG_DIR / "c3_readonly_official_replay_adapter_latest.csv"
CURRENT_SUMMARY = LOG_DIR / "c3_readonly_official_replay_summary_latest.csv"

LATEST_JSON = LOG_DIR / "c3_replay_divergence_diagnosis_latest.json"
LATEST_COMPARISON_CSV = LOG_DIR / "c3_replay_divergence_diagnosis_comparison_latest.csv"
LATEST_OLD_TRADES_CSV = LOG_DIR / "c3_replay_divergence_diagnosis_old_balanced_latest.csv"
LATEST_MD = LOG_DIR / "c3_replay_divergence_diagnosis_latest.md"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype={"code": str}, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _norm_code(value: Any) -> str:
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6)[-6:]


def _norm_date(value: Any) -> str:
    if pd.isna(value):
        return ""
    s = str(value)[:10].replace("-", "")
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return str(value)[:10]


def _contract_core(contract: dict[str, Any]) -> dict[str, Any]:
    return {
        k: contract.get(k)
        for k in [
            "signal_date_start",
            "signal_date_end",
            "regimes",
            "required_signals",
            "numeric_filters",
            "entry_trigger",
            "exit_overrides",
        ]
    }


def _frame_summary(df: pd.DataFrame, date_col: str = "signal_date") -> dict[str, Any]:
    out = {"rows": int(len(df)), "codes": 0, "date_min": "", "date_max": "", "ret_sum": None, "profit_factor": None}
    if df.empty:
        return out
    if "code" in df.columns:
        out["codes"] = int(df["code"].map(_norm_code).nunique())
    if date_col in df.columns:
        dates = df[date_col].map(_norm_date)
        out["date_min"] = str(dates.min())
        out["date_max"] = str(dates.max())
    if "ret" in df.columns:
        ret = pd.to_numeric(df["ret"], errors="coerce").dropna()
        out["ret_sum"] = round(float(ret.sum()), 6) if len(ret) else None
        gross_profit = float(ret[ret > 0].sum())
        gross_loss = float(-ret[ret < 0].sum())
        if not len(ret):
            out["profit_factor"] = None
        elif gross_loss > 0:
            out["profit_factor"] = round(gross_profit / gross_loss, 6)
        elif gross_profit > 0:
            out["profit_factor"] = "inf"
        else:
            out["profit_factor"] = None
    return out


def _parse_price_integrity(stdout_path: Path) -> dict[str, Any]:
    if not stdout_path.exists():
        return {"found": False, "reason": "stdout_missing"}
    text = stdout_path.read_text(encoding="utf-8", errors="replace")
    m = re.search(
        r"PRICE_HISTORY_INTEGRITY\] contract=(\S+) rows_in=(\d+) rows_out=(\d+) invalid_rows=(\d+) gap_breaks=(\d+) extreme_breaks=(\d+) segments=(\d+)",
        text,
    )
    if not m:
        return {"found": False, "reason": "pattern_missing"}
    return {
        "found": True,
        "contract": m.group(1),
        "rows_in": int(m.group(2)),
        "rows_out": int(m.group(3)),
        "invalid_rows": int(m.group(4)),
        "gap_breaks": int(m.group(5)),
        "extreme_breaks": int(m.group(6)),
        "segments": int(m.group(7)),
    }


def _contract_diff_rows(old_core: dict[str, Any], current_core: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key in sorted(set(old_core) | set(current_core)):
        old_val = old_core.get(key)
        new_val = current_core.get(key)
        rows.append({
            "field": key,
            "same": old_val == new_val,
            "old": json.dumps(old_val, ensure_ascii=False, sort_keys=True),
            "current": json.dumps(new_val, ensure_ascii=False, sort_keys=True),
        })
    return rows


def main() -> int:
    old_contract = _read_json(OLD_CONTRACTS)["balanced_original"]
    current_contract_payload = _read_json(CURRENT_SELECTION_CONTRACT)
    current_contract = current_contract_payload.get("selection_contract", current_contract_payload)
    replay_payload = _read_json(CURRENT_REPLAY_JSON)
    old_trades = _read_csv(OLD_BALANCED_TRADES)
    current_native = _read_csv(CURRENT_NATIVE)
    current_adapter = _read_csv(CURRENT_ADAPTER)
    current_summary = _read_csv(CURRENT_SUMMARY)

    old_core = _contract_core(old_contract)
    current_core = _contract_core(current_contract)
    contract_rows = _contract_diff_rows(old_core, current_core)
    contract_equal_core = all(bool(r["same"]) for r in contract_rows)

    run_dir = Path(str(replay_payload.get("run_dir", "")))
    price_integrity = _parse_price_integrity(run_dir / "report_backtest_stdout.txt")

    comparison_rows = [
        {"artifact": "old_balanced_trades", **_frame_summary(old_trades)},
        {"artifact": "current_native_trades", **_frame_summary(current_native)},
        {"artifact": "current_c3_adapter", **_frame_summary(current_adapter)},
    ]
    comparison_df = pd.DataFrame(comparison_rows)
    contract_df = pd.DataFrame(contract_rows)
    old_export = old_trades.copy()
    if not old_export.empty:
        old_export["code"] = old_export["code"].map(_norm_code)
        old_export["signal_date"] = old_export["signal_date"].map(_norm_date)

    if int(len(current_native)) == 0 and contract_equal_core:
        divergence_point = "current_report_native_selection_or_upstream_data"
        diagnosis = "contract_core_same_but_current_native_zero"
    elif int(len(current_native)) == 0:
        divergence_point = "contract_or_current_report_native_selection"
        diagnosis = "contract_diff_and_current_native_zero"
    else:
        divergence_point = "post_native_c3_adapter"
        diagnosis = "native_rows_exist_check_adapter"

    payload = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "READ_ONLY_C3_REPLAY_DIVERGENCE_DIAGNOSIS_NOT_OPERATIONAL",
        "diagnosis": diagnosis,
        "divergence_point": divergence_point,
        "old_balanced": _frame_summary(old_trades),
        "current_native": _frame_summary(current_native),
        "current_adapter": _frame_summary(current_adapter),
        "contract_equal_core": bool(contract_equal_core),
        "contract_diff_rows": contract_rows,
        "price_integrity": price_integrity,
        "report_param_gate": replay_payload.get("report_param_gate", {}),
        "current_replay_status": replay_payload.get("status"),
        "current_replay_run_dir": replay_payload.get("run_dir"),
        "current_summary_rows": current_summary.to_dict(orient="records") if not current_summary.empty else [],
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "comparison_csv": str(LATEST_COMPARISON_CSV),
        "old_balanced_export_csv": str(LATEST_OLD_TRADES_CSV),
    }

    comparison_df.to_csv(LATEST_COMPARISON_CSV, index=False, encoding="utf-8-sig")
    old_export.to_csv(LATEST_OLD_TRADES_CSV, index=False, encoding="utf-8-sig")
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# C3 replay divergence diagnosis",
        "",
        f"- status: {payload['status']}",
        f"- diagnosis: {diagnosis}",
        f"- divergence_point: {divergence_point}",
        f"- old_balanced_rows: {payload['old_balanced']['rows']}",
        f"- current_native_rows: {payload['current_native']['rows']}",
        f"- current_adapter_rows: {payload['current_adapter']['rows']}",
        f"- contract_equal_core: {contract_equal_core}",
        f"- price_integrity: {json.dumps(price_integrity, ensure_ascii=False, sort_keys=True)}",
        "",
        "## Contract diff",
        "",
    ]
    for row in contract_rows:
        lines.append(f"- {row['field']}: same={row['same']}")
    lines.extend(["", "## Artifact comparison", ""])
    for row in comparison_rows:
        lines.append(f"- {row['artifact']}: rows={row['rows']}, codes={row['codes']}, date={row['date_min']}~{row['date_max']}, ret_sum={row['ret_sum']}, pf={row['profit_factor']}")
    LATEST_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())