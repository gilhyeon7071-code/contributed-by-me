"""Validate append/realization integrity of the three deferred paper streams."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
INPUT = LOG / "deferred_paper_streams_latest.csv"
OUT_JSON = LOG / "deferred_paper_stream_integrity_latest.json"
OUT_MD = LOG / "deferred_paper_stream_integrity_latest.md"


def main() -> int:
    x = pd.read_csv(INPUT, dtype={"code": str})
    x["next_open_net_return"] = pd.to_numeric(x["next_open_net_return"], errors="coerce")
    dup = int(x.duplicated(["paper_stream_version", "frozen_id", "signal_date", "code", "horizon"]).sum())
    max_violations = []
    for key, g in x.groupby(["paper_stream_version", "frozen_id", "signal_date"], dropna=False):
        cap = int(g["capacity_cap"].iloc[0])
        if len(g) > cap:
            max_violations.append({"key": [str(v) for v in key], "rows": int(len(g)), "cap": cap})
    pending_bad = int((x["paper_sample_state"].eq("PENDING_RESEARCH_RETURN") & x["next_open_net_return"].notna()).sum())
    realized_bad = int((x["paper_sample_state"].eq("REALIZED_RESEARCH_RETURN") & x["next_open_net_return"].isna()).sum())
    latest = x["signal_date"].max()
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "PASS" if dup == 0 and not max_violations and pending_bad == 0 and realized_bad == 0 else "FAIL", "rows": int(len(x)), "stream_versions": sorted(x["paper_stream_version"].dropna().unique().tolist()), "latest_signal_date": latest, "duplicate_keys": dup, "capacity_violations": len(max_violations), "pending_with_realized_return": pending_bad, "realized_without_return": realized_bad, "operational_change": False}
    if max_violations:
        payload["capacity_violation_examples"] = max_violations[:10]
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Deferred Paper Stream Integrity", "", f"- status: {payload['status']}", f"- rows: {len(x)}", f"- latest_signal_date: {latest}", f"- duplicate_keys: {dup}", f"- capacity_violations: {len(max_violations)}", f"- pending_with_realized_return: {pending_bad}", f"- realized_without_return: {realized_bad}", "- operational_change: false"]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
