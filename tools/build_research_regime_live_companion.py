"""Read-only companion: log the research-grade regime label (BULL/BEAR/SIDEWAYS/STRESS/
TRANSITION, report_backtest_v41_1.py:_assign_report_research_regime) alongside the live
paper_engine regime label (RALLY/NORMAL/BEAR) for the latest available trading date.

Scope: observation only. Does not read or write paper_engine.py, does not touch the P1
gate, sizing, orders, fills, ledger, or candidate generation. No BUY-eligibility change.

Why this exists: the two classifiers were confirmed today (2026-07-22) to disagree
substantially -- see .agent/PLANS.md 2026-07-22 entry. Live "BEAR" mapped to research
"STRESS" on 9/11 recent days, not research "BEAR". This tool accumulates the research
label going forward so a future confirmation round has genuine paired history to work
with, instead of retrofitting a mapping from a single day's snapshot.
"""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
REPORT_PATH = ROOT / "report_backtest_v41_1.py"
P1_GATE_STATUS_PATH = LOG_DIR / "p1_entry_gate_status_latest.json"
OUT_PREFIX = "research_regime_live_companion"


def load_report() -> Any:
    spec = importlib.util.spec_from_file_location("research_regime_live_companion_report", REPORT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {REPORT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    report = load_report()
    raw = report.load_data()
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    research_regime = report._assign_report_research_regime(factors)
    research_regime = research_regime.dropna()
    if research_regime.empty:
        raise RuntimeError("research regime series is empty")

    latest_date = research_regime.index.max()
    latest_research_regime = str(research_regime.loc[latest_date])

    live_regime = None
    live_as_of = None
    if P1_GATE_STATUS_PATH.exists():
        live = json.loads(P1_GATE_STATUS_PATH.read_text(encoding="utf-8-sig"))
        live_regime = live.get("market_regime")
        live_as_of = live.get("as_of_ymd")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_research_regime_live_companion",
        "policy_effect": "read_only_no_gate_or_order_change",
        "research_regime_source_date": latest_date.strftime("%Y-%m-%d"),
        "research_regime": latest_research_regime,
        "live_regime_as_of_ymd": live_as_of,
        "live_regime": live_regime,
        "note": (
            "research_regime is computed from the latest closed trading session available in "
            "price history, so it always lags live_as_of by at least one session on days the "
            "batch runs before that session's close is archived."
        ),
        "limitations": [
            "This pairs two independently defined regime classifiers for observation only.",
            "Live BEAR was found to correspond mostly to research STRESS in the 2026-07-06..07-21 sample (9/11 days), not research BEAR.",
            "No mapping, gate, sizing, or candidate-generation change is applied by this tool.",
        ],
    }

    out_json_dated = LOG_DIR / f"{OUT_PREFIX}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_json_latest = LOG_DIR / f"{OUT_PREFIX}_latest.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    out_json_dated.write_text(text, encoding="utf-8")
    out_json_latest.write_text(text, encoding="utf-8")

    history_csv = LOG_DIR / f"{OUT_PREFIX}_history.csv"
    row = pd.DataFrame([{
        "generated_at": payload["generated_at"],
        "research_regime_source_date": payload["research_regime_source_date"],
        "research_regime": payload["research_regime"],
        "live_regime_as_of_ymd": payload["live_regime_as_of_ymd"],
        "live_regime": payload["live_regime"],
    }])
    if history_csv.exists():
        existing = pd.read_csv(history_csv, encoding="utf-8-sig")
        combined = pd.concat([existing, row], ignore_index=True)
        combined = combined.drop_duplicates(subset=["research_regime_source_date"], keep="last")
    else:
        combined = row
    combined.to_csv(history_csv, index=False, encoding="utf-8-sig")

    print(json.dumps({
        "status": "OK",
        "research_regime_source_date": payload["research_regime_source_date"],
        "research_regime": payload["research_regime"],
        "live_regime_as_of_ymd": payload["live_regime_as_of_ymd"],
        "live_regime": payload["live_regime"],
        "history_rows": int(len(combined)),
        "json": str(out_json_latest),
        "history_csv": str(history_csv),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
