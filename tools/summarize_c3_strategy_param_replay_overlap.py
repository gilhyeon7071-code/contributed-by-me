from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SCAN_DIR = LOG_DIR / "research_report_entry_trigger_v2_transition_value2b_ret1_1_relaxation_scope_scan_20260713_132200"
OLD_TRADES = SCAN_DIR / "balanced_original_trades.csv"
NATIVE = LOG_DIR / "c3_strategy_param_replay_native_latest.csv"
ADAPTER = LOG_DIR / "c3_strategy_param_replay_adapter_latest.csv"
SUMMARY = LOG_DIR / "c3_strategy_param_replay_summary_latest.csv"
REPLAY_JSON = LOG_DIR / "c3_strategy_param_replay_latest.json"

LATEST_JSON = LOG_DIR / "c3_strategy_param_replay_overlap_latest.json"
LATEST_CSV = LOG_DIR / "c3_strategy_param_replay_overlap_latest.csv"
LATEST_MD = LOG_DIR / "c3_strategy_param_replay_overlap_latest.md"


def _norm_code(value: Any) -> str:
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6)[-6:]


def _norm_date(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype={"code": str}, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["code"] = out["code"].map(_norm_code)
    out["signal_date"] = out["signal_date"].map(_norm_date)
    out["key"] = out["code"] + "|" + out["signal_date"]
    return out


def main() -> int:
    old = _prep(_read_csv(OLD_TRADES))
    native = _prep(_read_csv(NATIVE))
    adapter = _prep(_read_csv(ADAPTER))
    summary = _read_csv(SUMMARY)
    replay = json.loads(REPLAY_JSON.read_text(encoding="utf-8-sig")) if REPLAY_JSON.exists() else {}

    old_keys = set(old["key"]) if not old.empty else set()
    native_keys = set(native["key"]) if not native.empty else set()
    adapter_keys = set(adapter["key"]) if not adapter.empty else set()

    rows = []
    for _, row in old.sort_values(["signal_date", "code"]).iterrows():
        key = row["key"]
        rows.append(
            {
                "key": key,
                "code": row["code"],
                "signal_date": row["signal_date"],
                "entry_date": _norm_date(row.get("entry_date")),
                "old_ret": row.get("ret"),
                "in_strategy_param_native": bool(key in native_keys),
                "in_strategy_param_adapter": bool(key in adapter_keys),
            }
        )
    overlap = pd.DataFrame(rows)
    payload = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "READ_ONLY_C3_STRATEGY_PARAM_REPLAY_OVERLAP_NOT_OPERATIONAL",
        "old_rows": int(len(old)),
        "native_rows": int(len(native)),
        "adapter_rows": int(len(adapter)),
        "old_in_native": int(overlap["in_strategy_param_native"].sum()) if not overlap.empty else 0,
        "old_missing_native": int((~overlap["in_strategy_param_native"]).sum()) if not overlap.empty else 0,
        "old_in_adapter": int(overlap["in_strategy_param_adapter"].sum()) if not overlap.empty else 0,
        "native_summary": summary.to_dict("records") if not summary.empty else [],
        "replay_run_dir": replay.get("run_dir", ""),
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "overlap_csv": str(LATEST_CSV),
    }
    overlap.to_csv(LATEST_CSV, index=False, encoding="utf-8-sig")
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# C3 strategy-param replay overlap",
        "",
        f"- status: {payload['status']}",
        f"- old_rows: {payload['old_rows']}",
        f"- native_rows: {payload['native_rows']}",
        f"- adapter_rows: {payload['adapter_rows']}",
        f"- old_in_native: {payload['old_in_native']}",
        f"- old_missing_native: {payload['old_missing_native']}",
        f"- old_in_adapter: {payload['old_in_adapter']}",
        "",
        "## Native summary",
        "",
    ]
    for row in payload["native_summary"]:
        lines.append(f"- {row.get('layer')}: n={row.get('n')}, ret_sum={row.get('ret_sum')}, pf={row.get('profit_factor')}, win_rate={row.get('win_rate')}")
    LATEST_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
