from __future__ import annotations

import importlib.util
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"
REPLAY_TRADES = LOG_DIR / "stress_tradability_h2_replay_trades_latest.csv"

OUT_JSON = LOG_DIR / "stress_h2_pending_resolution_latest.json"
OUT_ROWS = LOG_DIR / "stress_h2_pending_resolution_rows_latest.csv"
OUT_MD = LOG_DIR / "stress_h2_pending_resolution_latest.md"

TARGET_PENDING_DATES = ("2026-07-10", "2026-07-13")


def clean_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): clean_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean_json_value(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_stress_h2_pending_audit", REPORT_BACKTEST)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {REPORT_BACKTEST}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def norm_date(value: Any) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def norm_code(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return ""
    text = str(value).strip()
    try:
        if float(text).is_integer():
            return str(int(float(text))).zfill(6)
    except Exception:
        pass
    return text.zfill(6) if text.isdigit() else text


def load_pending_trades() -> pd.DataFrame:
    trades = pd.read_csv(REPLAY_TRADES, encoding="utf-8-sig")
    trades["date_norm"] = trades["date_norm"].map(norm_date)
    trades["code_norm"] = trades["code_norm"].map(norm_code)
    trades["gross_ret_h2"] = pd.to_numeric(trades.get("gross_ret_h2"), errors="coerce")
    pending = trades[trades["gross_ret_h2"].isna()].copy()
    pending = pending[pending["date_norm"].isin(TARGET_PENDING_DATES)].copy()
    return pending


def build_availability(factors: pd.DataFrame, pending: pd.DataFrame) -> pd.DataFrame:
    if pending.empty:
        return pd.DataFrame()
    f = factors[["date", "code", "close", "price_session_index", "price_history_segment"]].copy()
    f["date_norm"] = pd.to_datetime(f["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    f["code_norm"] = f["code"].map(norm_code)
    key = f.set_index(["code_norm", "date_norm"])

    rows: list[dict[str, Any]] = []
    for _idx, row in pending.iterrows():
        code = str(row["code_norm"])
        signal_date = str(row["date_norm"])
        out = row.to_dict()
        out["audit_code_norm"] = code
        out["audit_signal_date"] = signal_date
        if (code, signal_date) not in key.index:
            out["pending_resolution_status"] = "SIGNAL_ROW_NOT_FOUND_IN_CURRENT_FACTORS"
            out["current_data_has_h2"] = False
            rows.append(out)
            continue
        base = key.loc[(code, signal_date)]
        if isinstance(base, pd.DataFrame):
            base = base.iloc[0]
        target_session = int(base["price_session_index"]) + 2
        code_rows = f[f["code_norm"].eq(code)]
        target = code_rows[
            (pd.to_numeric(code_rows["price_session_index"], errors="coerce") == target_session)
            & (code_rows["price_history_segment"].astype(str).eq(str(base["price_history_segment"])))
        ].copy()
        out["base_price_session_index"] = int(base["price_session_index"])
        out["target_price_session_index"] = target_session
        if target.empty:
            out["pending_resolution_status"] = "H2_TARGET_PRICE_NOT_AVAILABLE"
            out["current_data_has_h2"] = False
            out["target_date"] = ""
            out["target_close"] = None
            out["recomputed_h2_ret"] = None
        else:
            target = target.sort_values("date_norm").iloc[0]
            out["pending_resolution_status"] = "H2_TARGET_PRICE_AVAILABLE"
            out["current_data_has_h2"] = True
            out["target_date"] = str(target["date_norm"])
            out["target_close"] = float(target["close"])
            out["recomputed_h2_ret"] = float(target["close"] / base["close"] - 1.0)
        rows.append(out)
    return pd.DataFrame(rows)


def main() -> int:
    pending = load_pending_trades()
    report = load_report_module()
    data = report.load_data()
    factors = report.compute_factors(data)
    latest_data_date = pd.to_datetime(factors["date"], errors="coerce").max().strftime("%Y-%m-%d")
    availability = build_availability(factors, pending)

    if availability.empty:
        available_count = 0
        pending_count = 0
    else:
        available_count = int(availability["current_data_has_h2"].fillna(False).sum())
        pending_count = int((~availability["current_data_has_h2"].fillna(False)).sum())

    if len(pending) == 0:
        conclusion = "NO_TARGET_PENDING_H2_ROWS_FOUND"
    elif available_count == len(pending):
        conclusion = "ALL_TARGET_PENDING_H2_ROWS_RESOLVED_RERUN_REPLAY"
    elif available_count > 0:
        conclusion = "PARTIAL_TARGET_PENDING_H2_ROWS_RESOLVED_RERUN_NOT_YET_CLEAN"
    else:
        conclusion = "TARGET_PENDING_H2_ROWS_STILL_UNRESOLVED_NO_REPLAY_RERUN"

    by_date = (
        availability.groupby(["audit_signal_date", "pending_resolution_status"], dropna=False).size().reset_index(name="rows").to_dict(orient="records")
        if not availability.empty else []
    )
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "stress_h2_pending_resolution_read_only",
        "classification": "STRESS_H2_PENDING_RESOLUTION_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": conclusion,
        "latest_data_date": latest_data_date,
        "target_pending_dates": list(TARGET_PENDING_DATES),
        "row_counts": {
            "pending_target_rows": int(len(pending)),
            "h2_available_rows": available_count,
            "h2_still_pending_rows": pending_count,
            "data_rows": int(len(data)),
            "factor_rows": int(len(factors)),
        },
        "by_date": by_date,
        "operation_effect": {
            "candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
            "operational_candidate_file_modified": False,
            "replay_rerun": False,
        },
        "validation": [
            "pending_trades_loaded: PASS",
            "current_price_data_loaded: PASS",
            "h2_availability_checked_by_price_session_index: PASS",
            "operation_effect_no_changes: PASS",
        ],
    }

    availability.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(clean_json_value(payload), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    md = [
        "# STRESS H2 Pending Resolution",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- latest_data_date: {latest_data_date}",
        "",
        "## Row Counts",
        "",
        *[f"- {k}: {v}" for k, v in payload["row_counts"].items()],
        "",
        "## By Date",
        "",
    ]
    for row in by_date:
        md.append(f"- {row['audit_signal_date']} / {row['pending_resolution_status']}: {row['rows']}")
    if not by_date:
        md.append("- none")
    md += [
        "",
        "## Operation Effect",
        "",
        "- NOT_APPLIED to candidate generation, official backtest, HPO, diagnostics, paper/live, gates, thresholds, or parameters.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_ROWS}")
    print(f"[OK] wrote {OUT_MD}")
    print(f"[CONCLUSION] {conclusion}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
