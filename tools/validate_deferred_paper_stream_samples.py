"""Apply the frozen sample rule to realized rows in cap5/10/20 research streams."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
INPUT = LOG / "deferred_paper_streams_latest.csv"
OUT = LOG / "deferred_paper_stream_sample_validation_latest.csv"
OUT_JSON = LOG / "deferred_paper_stream_sample_validation_latest.json"
OUT_MD = LOG / "deferred_paper_stream_sample_validation_latest.md"
PERIODS = {
    "P1": (pd.Timestamp("2025-06-01"), pd.Timestamp("2025-09-30")),
    "P2": (pd.Timestamp("2025-10-01"), pd.Timestamp("2025-12-31")),
    "P3": (pd.Timestamp("2026-01-01"), pd.Timestamp("2026-03-31")),
    "P4": (pd.Timestamp("2026-04-01"), pd.Timestamp("2026-07-15")),
}
MIN_DATES = 20
MIN_EPISODES = 2


def episode_count(dates: pd.Series) -> int:
    x = pd.to_datetime(dates, errors="coerce").dropna().sort_values().drop_duplicates()
    return int(x.diff().dt.days.gt(7).fillna(True).sum()) if not x.empty else 0


def main() -> int:
    x = pd.read_csv(INPUT)
    x["signal_date"] = pd.to_datetime(x["signal_date"], errors="coerce")
    x["next_open_net_return"] = pd.to_numeric(x["next_open_net_return"], errors="coerce")
    x = x[x["next_open_net_return"].notna()].copy()
    rows = []
    for key, g in x.groupby(["paper_stream_version", "capacity_cap", "frozen_id", "strategy", "signal", "regime", "horizon"], dropna=False):
        rec = {"paper_stream_version": key[0], "capacity_cap": key[1], "frozen_id": key[2], "strategy": key[3], "signal": key[4], "regime": key[5], "horizon": key[6], "realized_rows": int(len(g)), "realized_dates": int(g["signal_date"].nunique())}
        checks = []
        for name, (start, end) in PERIODS.items():
            q = g[g["signal_date"].between(start, end)]
            daily = q.groupby("signal_date")["next_open_net_return"].mean()
            rec[f"{name}_dates"] = int(q["signal_date"].nunique())
            rec[f"{name}_episodes"] = episode_count(q["signal_date"])
            rec[f"{name}_mean_net_bps"] = float(daily.mean() * 10000) if not daily.empty else None
            checks += [rec[f"{name}_dates"] >= MIN_DATES, rec[f"{name}_episodes"] >= MIN_EPISODES]
        checks.append(all((rec[f"{name}_mean_net_bps"] is not None and rec[f"{name}_mean_net_bps"] > 0) for name in PERIODS))
        rec["confirmation_status"] = "CONFIRMED_RESEARCH_STREAM" if all(checks) else "DEFERRED_INSUFFICIENT_FORWARD_SAMPLE"
        rec["failed_checks"] = "|".join([f"{n}_dates" for n in PERIODS if rec[f"{n}_dates"] < MIN_DATES] + [f"{n}_episodes" for n in PERIODS if rec[f"{n}_episodes"] < MIN_EPISODES] + (["all_periods_positive"] if not checks[-1] else []))
        rows.append(rec)
    out = pd.DataFrame(rows).sort_values(["paper_stream_version", "frozen_id"])
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "OK", "criteria": {"min_realized_signal_dates_per_period": MIN_DATES, "min_episodes_per_period": MIN_EPISODES, "all_periods_positive": True}, "stream_rule_rows": int(len(out)), "confirmed": int((out["confirmation_status"] == "CONFIRMED_RESEARCH_STREAM").sum()), "deferred": int((out["confirmation_status"] != "CONFIRMED_RESEARCH_STREAM").sum()), "operational_change": False, "outputs": {"csv": str(OUT), "md": str(OUT_MD)}}
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Deferred Paper Stream Sample Validation", "", f"- stream_rule_rows: {len(out)}", f"- confirmed: {payload['confirmed']}", f"- deferred: {payload['deferred']}", "- operational_change: false", "", "## Results"]
    for _, r in out.iterrows():
        lines.append(f"- {r['paper_stream_version']} id={r['frozen_id']} {r['strategy']} {r['signal']} {r['regime']} {r['horizon']}: {r['confirmation_status']}; failed={r['failed_checks'] or 'none'}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
