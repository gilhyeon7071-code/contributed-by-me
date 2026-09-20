"""Read-only capacity and overlap audit for the deferred research paper ledger."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
INPUT = LOG / "deferred_research_paper_ledger_trades_latest.csv"
OUT = LOG / "deferred_research_paper_capacity_audit_latest.csv"
OUT_JSON = LOG / "deferred_research_paper_capacity_audit_latest.json"
OUT_MD = LOG / "deferred_research_paper_capacity_audit_latest.md"


def main() -> int:
    t = pd.read_csv(INPUT, dtype={"code": str, "frozen_id": str})
    t["signal_date"] = pd.to_datetime(t["signal_date"], errors="coerce")
    t["entry_date"] = pd.to_datetime(t["entry_date"], errors="coerce")
    t["exit_date"] = pd.to_datetime(t["exit_date"], errors="coerce")
    t = t.dropna(subset=["signal_date", "entry_date", "exit_date"])
    rows = []
    for key, g in t.groupby(["frozen_id", "strategy", "signal", "regime", "horizon"], dropna=False):
        daily = g.groupby("signal_date").size()
        sessions = pd.date_range(g["entry_date"].min(), g["exit_date"].max(), freq="B")
        concurrent = []
        for d in sessions:
            concurrent.append(int(((g["entry_date"] <= d) & (g["exit_date"] >= d)).sum()))
        overlap_signal_dates = int((daily > 1).sum())
        rec = {"frozen_id": key[0], "strategy": key[1], "signal": key[2], "regime": key[3], "horizon": key[4], "signal_dates": int(len(daily)), "candidate_rows": int(len(g)), "median_candidates_per_signal_date": float(daily.median()), "p95_candidates_per_signal_date": float(daily.quantile(0.95)), "max_candidates_per_signal_date": int(daily.max()), "overlap_signal_dates": overlap_signal_dates, "overlap_signal_date_rate": float(overlap_signal_dates / len(daily)) if len(daily) else None, "max_concurrent_positions": int(max(concurrent) if concurrent else 0), "median_concurrent_positions": float(pd.Series(concurrent).median()) if concurrent else None, "capacity_state": "HIGH_OVERLAP_OR_CAPACITY_REVIEW" if (max(concurrent) if concurrent else 0) > 20 or (daily.max() if len(daily) else 0) > 20 else "LOWER_OBSERVED_CONCURRENCY"}
        rows.append(rec)
    out = pd.DataFrame(rows).sort_values("frozen_id")
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "OK", "scope": "research_only_capacity_overlap_audit", "input_rows": int(len(t)), "rules": int(out["frozen_id"].nunique()), "high_overlap_or_capacity_review": int((out["capacity_state"] == "HIGH_OVERLAP_OR_CAPACITY_REVIEW").sum()), "policy_effect": False, "outputs": {"csv": str(OUT), "md": str(OUT_MD)}}
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Deferred Research Paper Capacity Audit", "", f"- input_rows: {len(t)}", f"- rules: {payload['rules']}", f"- high_overlap_or_capacity_review: {payload['high_overlap_or_capacity_review']}", "", "## Results"]
    for _, r in out.iterrows():
        lines.append(f"- id={r['frozen_id']} {r['strategy']} {r['signal']} {r['regime']} {r['horizon']}: median/day={r['median_candidates_per_signal_date']:.1f}, max/day={int(r['max_candidates_per_signal_date'])}, max_concurrent={int(r['max_concurrent_positions'])}, overlap_rate={r['overlap_signal_date_rate']:.2%}, state={r['capacity_state']}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
