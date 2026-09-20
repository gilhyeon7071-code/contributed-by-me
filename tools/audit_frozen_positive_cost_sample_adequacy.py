"""Read-only sample adequacy audit for cost-positive frozen combinations."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
SUMMARY = LOG / "frozen_positive_next_open_cost_summary_latest.csv"
TRADES = LOG / "frozen_positive_next_open_cost_trades_latest.csv"
OUT = LOG / "frozen_positive_cost_sample_adequacy_latest.csv"
OUT_JSON = LOG / "frozen_positive_cost_sample_adequacy_latest.json"
OUT_MD = LOG / "frozen_positive_cost_sample_adequacy_latest.md"
PERIODS = ["P1_202506_202509", "P2_202510_202512", "P3_202601_202603", "P4_202604_202606"]


def episodes(dates: pd.Series) -> int:
    x = pd.to_datetime(dates, errors="coerce").dropna().sort_values().drop_duplicates()
    if x.empty:
        return 0
    return int((x.diff().dt.days.gt(7).fillna(True)).sum())


def main() -> int:
    summary = pd.read_csv(SUMMARY)
    trades = pd.read_csv(TRADES)
    positive = summary[summary["cost_verdict"].eq("COST_NET_POSITIVE_TRAIN_HOLDOUT")].copy()
    if positive.empty:
        raise SystemExit("no cost-positive combinations")
    rows = []
    for _, r in positive.iterrows():
        t = trades[trades["frozen_id"].eq(r["frozen_id"])].copy()
        rec = {k: r.get(k) for k in ["frozen_id", "source", "scope", "horizon", "strategy", "signal", "regime"]}
        train_positive = 0
        for period in PERIODS:
            q = t[t["period"].eq(period)]
            ret_col = [c for c in q.columns if c.startswith("next_open_return_")]
            ret_col = ret_col[0] if ret_col else None
            mean_bps = float(pd.to_numeric(q[ret_col], errors="coerce").mean() * 10000) if ret_col and not q.empty else None
            dates = q["date"] if not q.empty else pd.Series(dtype=str)
            if mean_bps is not None and mean_bps > 0:
                train_positive += int(period != "P4_202604_202606")
            rec[f"{period}_rows"] = int(len(q))
            rec[f"{period}_dates"] = int(q["date"].nunique())
            rec[f"{period}_episodes"] = episodes(dates)
            rec[f"{period}_mean_net_bps"] = mean_bps
            rec[f"{period}_median_candidates_per_date"] = float(q.groupby("date").size().median()) if not q.empty else None
        rec["train_positive_periods"] = train_positive
        rec["train_unique_dates"] = int(sum(rec[f"{p}_dates"] or 0 for p in PERIODS[:3]))
        rec["holdout_unique_dates"] = int(rec["P4_202604_202606_dates"] or 0)
        rec["holdout_episodes"] = int(rec["P4_202604_202606_episodes"] or 0)
        flags = []
        if rec["holdout_unique_dates"] < 10:
            flags.append("HOLDOUT_DATES_LT_10_DIAGNOSTIC")
        if rec["holdout_episodes"] < 2:
            flags.append("HOLDOUT_EPISODES_LT_2_DIAGNOSTIC")
        if rec["train_positive_periods"] < 3:
            flags.append("TRAIN_NOT_POSITIVE_ALL_3_PERIODS_DIAGNOSTIC")
        rec["diagnostic_flags"] = "|".join(flags) if flags else "NONE"
        rec["sample_state"] = "DEFERRED_SAMPLE_ADEQUACY" if flags else "DESCRIPTIVELY_ADEQUATE_PENDING_PRE_REGISTERED_THRESHOLD"
        rows.append(rec)
    out = pd.DataFrame(rows).sort_values(["sample_state", "holdout_unique_dates", "frozen_id"], ascending=[True, False, True])
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "OK", "scope": "read_only_frozen_positive_sample_audit", "positive_combinations": int(len(out)), "deferred_diagnostic_count": int(out["sample_state"].eq("DEFERRED_SAMPLE_ADEQUACY").sum()), "adequacy_threshold_policy": "No pass/fail threshold introduced; flags are diagnostic only until pre-registered.", "outputs": {"csv": str(OUT), "md": str(OUT_MD)}}
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Frozen Positive Cost Sample Adequacy", "", f"- generated_at: {payload['generated_at']}", f"- positive_combinations: {len(out)}", f"- deferred_diagnostic_count: {payload['deferred_diagnostic_count']}", "- no new pass/fail threshold was introduced", "", "## Rows"]
    for _, r in out.iterrows():
        lines.append(f"- id={r['frozen_id']} {r['strategy']} {r['signal']} {r['regime']} {r['horizon']}: train_dates={r['train_unique_dates']}, holdout_dates={r['holdout_unique_dates']}, holdout_episodes={r['holdout_episodes']}, state={r['sample_state']}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
