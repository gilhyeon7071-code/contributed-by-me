"""Apply the pre-registered sample-adequacy rule to frozen cost-positive rows."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
INPUT = LOG / "frozen_positive_cost_sample_adequacy_latest.csv"
OUT = LOG / "frozen_positive_sample_confirmation_latest.csv"
OUT_JSON = LOG / "frozen_positive_sample_confirmation_latest.json"
OUT_MD = LOG / "frozen_positive_sample_confirmation_latest.md"

CRITERIA_VERSION = "FROZEN_SAMPLE_CONFIRMATION_V1"
MIN_DATES_PER_SPLIT = 20
MIN_EPISODES_PER_SPLIT = 2
MIN_TRAIN_POSITIVE_PERIODS = 3
TRAIN_PERIODS = ["P1_202506_202509", "P2_202510_202512", "P3_202601_202603"]
HOLDOUT = "P4_202604_202606"


def main() -> int:
    x = pd.read_csv(INPUT)
    rows = []
    for _, r in x.iterrows():
        checks = {}
        for p in TRAIN_PERIODS + [HOLDOUT]:
            checks[f"{p}_dates"] = int(r.get(f"{p}_dates", 0) or 0) >= MIN_DATES_PER_SPLIT
            checks[f"{p}_episodes"] = int(r.get(f"{p}_episodes", 0) or 0) >= MIN_EPISODES_PER_SPLIT
            mean = r.get(f"{p}_mean_net_bps")
            checks[f"{p}_positive"] = pd.notna(mean) and float(mean) > 0
        checks["train_positive_periods"] = int(r.get("train_positive_periods", 0) or 0) >= MIN_TRAIN_POSITIVE_PERIODS
        passed = all(checks.values())
        rec = {k: r.get(k) for k in ["frozen_id", "source", "scope", "horizon", "strategy", "signal", "regime"]}
        rec["criteria_version"] = CRITERIA_VERSION
        rec["passed_checks"] = int(sum(bool(v) for v in checks.values()))
        rec["total_checks"] = int(len(checks))
        rec["confirmation_status"] = "CONFIRMED_RESEARCH_CANDIDATE" if passed else "DEFERRED_INSUFFICIENT_SAMPLE_OR_REPETITION"
        rec["failed_checks"] = "|".join(k for k, v in checks.items() if not v)
        rows.append(rec)
    out = pd.DataFrame(rows)
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "criteria_version": CRITERIA_VERSION,
        "criteria": {
            "min_unique_signal_dates_per_split": MIN_DATES_PER_SPLIT,
            "min_noncontiguous_episodes_per_split": MIN_EPISODES_PER_SPLIT,
            "min_train_positive_periods": MIN_TRAIN_POSITIVE_PERIODS,
            "train_periods": TRAIN_PERIODS,
            "holdout": HOLDOUT,
        },
        "input_rows": int(len(x)),
        "confirmed_count": int((out["confirmation_status"] == "CONFIRMED_RESEARCH_CANDIDATE").sum()),
        "deferred_count": int((out["confirmation_status"] != "CONFIRMED_RESEARCH_CANDIDATE").sum()),
        "operational_change": False,
        "outputs": {"csv": str(OUT), "md": str(OUT_MD)},
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Frozen Positive Sample Confirmation", "", f"- criteria_version: {CRITERIA_VERSION}", f"- input_rows: {len(x)}", f"- confirmed: {payload['confirmed_count']}", f"- deferred: {payload['deferred_count']}", "", "## Criteria", f"- each split unique signal dates >= {MIN_DATES_PER_SPLIT}", f"- each split non-contiguous episodes >= {MIN_EPISODES_PER_SPLIT}", f"- all three Train periods net-positive", "", "## Results"]
    for _, r in out.iterrows():
        lines.append(f"- id={r['frozen_id']} {r['strategy']} {r['signal']} {r['regime']} {r['horizon']}: {r['confirmation_status']}; failed={r['failed_checks'] or 'none'}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
