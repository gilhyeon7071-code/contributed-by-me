"""Build a versioned, observe-only candidate layer for deferred frozen rules."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
BASE_SCRIPT = ROOT / "tools" / "run_frozen_positive_next_open_cost_validation.py"
OUT_HISTORY = LOG / "deferred_observe_only_candidates_history_latest.csv"
OUT_LATEST = LOG / "deferred_observe_only_candidates_latest.csv"
OUT_SUMMARY = LOG / "deferred_observe_only_candidates_summary_latest.csv"
OUT_JSON = LOG / "deferred_observe_only_candidate_layer_latest.json"
OUT_MD = LOG / "deferred_observe_only_candidate_layer_latest.md"
START = pd.Timestamp("2025-06-01")
END = pd.Timestamp("2026-07-15")
GENERATOR_VERSION = "DEFERRED_OBSERVE_ONLY_V1"
PROTOCOL_VERSION = "FROZEN_SAMPLE_CONFIRMATION_V1"


def load_module():
    spec = importlib.util.spec_from_file_location("frozen_cost", BASE_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load frozen cost module")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def main() -> int:
    m = load_module()
    base = m.load_axis()
    report = base._load_report_module()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    f = report.compute_factors(raw)
    f["date"] = pd.to_datetime(f["date"], errors="coerce").dt.normalize()
    f["research_regime"] = f["date"].map(report._assign_report_research_regime(f))
    f = m.make_rules(f, base)
    f = m.add_next_open_returns(f)
    panel = f[f["date"].between(START, END) & f["market"].astype(str).str.upper().isin(["KOSPI", "KOSDAQ"]) & pd.to_numeric(f["close"], errors="coerce").gt(0) & pd.to_numeric(f["value"], errors="coerce").gt(0)].copy()
    frozen = pd.read_csv(LOG / "frozen_positive_next_open_cost_summary_latest.csv")
    frozen = frozen[frozen["cost_verdict"].eq("COST_NET_POSITIVE_TRAIN_HOLDOUT")].copy()
    frozen["strategy_family"] = frozen["strategy"]
    frozen["signal_name"] = frozen["signal"]
    frozen["research_regime"] = frozen["regime"]
    rows = []
    for _, rule in frozen.iterrows():
        mask = m.mask_for(rule, panel)
        ret_col = f"next_open_return_{rule['horizon']}"
        q = panel.loc[mask, ["date", "code", "market", "research_regime", "close", "value", ret_col]].copy()
        q = q.rename(columns={ret_col: "next_open_net_return"})
        if q.empty:
            continue
        q.insert(0, "generator_version", GENERATOR_VERSION)
        q.insert(1, "validation_protocol_version", PROTOCOL_VERSION)
        q.insert(2, "frozen_id", rule["frozen_id"])
        q.insert(3, "source", rule["source"])
        q.insert(4, "scope", rule["scope"])
        q.insert(5, "horizon", rule["horizon"])
        q.insert(6, "strategy", rule["strategy"])
        q.insert(7, "signal", rule["signal"])
        q.insert(8, "regime", rule["regime"])
        q["signal_date"] = q["date"].dt.strftime("%Y-%m-%d")
        q["next_open_net_return_bps"] = q["next_open_net_return"].astype(float) * 10000
        q["research_only"] = True
        q["operational_use"] = False
        q["paper_eligible"] = False
        q["sample_state"] = "DEFERRED_INSUFFICIENT_SAMPLE_OR_REPETITION"
        rows.append(q)
    out = pd.concat(rows, ignore_index=True, sort=False) if rows else pd.DataFrame()
    if not out.empty and out.duplicated(["generator_version", "frozen_id", "signal_date", "code", "horizon"]).any():
        raise SystemExit("duplicate observe-only candidate key")
    out = out.sort_values(["signal_date", "frozen_id", "code"], kind="mergesort") if not out.empty else out
    latest_date = out["signal_date"].max() if not out.empty else None
    latest = out[out["signal_date"].eq(latest_date)].copy() if latest_date else out.copy()
    summary = out.groupby(["frozen_id", "strategy", "signal", "regime", "horizon"], as_index=False).agg(candidate_rows=("code", "size"), unique_signal_dates=("signal_date", "nunique"), unique_codes=("code", "nunique"), mean_next_open_net_bps=("next_open_net_return_bps", "mean"), median_next_open_net_bps=("next_open_net_return_bps", "median")) if not out.empty else pd.DataFrame()
    out.to_csv(OUT_HISTORY, index=False, encoding="utf-8-sig")
    latest.to_csv(OUT_LATEST, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    code_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "OK", "scope": "deferred_observe_only_candidate_layer", "generator_version": GENERATOR_VERSION, "validation_protocol_version": PROTOCOL_VERSION, "code_hash": code_hash, "window": {"start": str(START.date()), "end": str(END.date())}, "frozen_rules": int(len(frozen)), "history_rows": int(len(out)), "history_dates": int(out["signal_date"].nunique()) if not out.empty else 0, "latest_date": latest_date, "latest_rows": int(len(latest)), "price_history_contract": integrity, "research_only": True, "operational_use": False, "paper_eligible": False, "outputs": {"history": str(OUT_HISTORY), "latest": str(OUT_LATEST), "summary": str(OUT_SUMMARY)}}
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Deferred Observe-Only Candidate Layer", "", f"- generator_version: {GENERATOR_VERSION}", f"- history_rows: {len(out)}", f"- history_dates: {payload['history_dates']}", f"- latest_date: {latest_date}", f"- latest_rows: {len(latest)}", "- research_only: true", "- operational_use: false", "- paper_eligible: false", "", "## Rules"]
    for _, r in frozen.iterrows():
        lines.append(f"- id={r['frozen_id']} {r['strategy']} + {r['signal']} / {r['regime']} / {r['horizon']}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
