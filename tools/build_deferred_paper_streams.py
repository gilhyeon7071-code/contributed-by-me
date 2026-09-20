"""Build separate research-only paper streams for cap 5/10/20."""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
BASE = ROOT / "tools" / "run_frozen_positive_next_open_cost_validation.py"
OBSERVE = LOG / "deferred_observe_only_candidates_history_latest.csv"
OUT = LOG / "deferred_paper_streams_latest.csv"
OUT_LATEST = LOG / "deferred_paper_streams_pending_latest.csv"
OUT_SUMMARY = LOG / "deferred_paper_streams_summary_latest.csv"
OUT_JSON = LOG / "deferred_paper_streams_latest.json"
OUT_MD = LOG / "deferred_paper_streams_latest.md"
CAPS = (5, 10, 20)


def load_module():
    spec = importlib.util.spec_from_file_location("frozen_cost", BASE)
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
    f = report.compute_factors(raw)
    f["date"] = pd.to_datetime(f["date"], errors="coerce").dt.normalize()
    factor = f[["code", "date", "rs_slope", "stretch"]].copy()
    factor["code"] = factor["code"].astype(str).str.zfill(6)
    factor = factor.drop_duplicates(["code", "date"])
    obs = pd.read_csv(OBSERVE, dtype={"code": str})
    obs["code"] = obs["code"].astype(str).str.zfill(6)
    obs["date"] = pd.to_datetime(obs["date"], errors="coerce").dt.normalize()
    obs["signal_date"] = pd.to_datetime(obs["signal_date"], errors="coerce")
    obs = obs.merge(factor, on=["code", "date"], how="left", validate="many_to_one")
    obs["rank_value"] = obs["rs_slope"].where(obs["signal"].eq("RS_SLOPE_Q5"), obs["stretch"])
    obs["rank_ascending"] = obs["signal"].eq("STRETCH_Q1")
    streams = []
    for cap in CAPS:
        selected = []
        for _, g in obs.groupby(["frozen_id", "signal_date"], dropna=False):
            asc = bool(g["rank_ascending"].iloc[0])
            q = g.sort_values(["rank_value", "code"], ascending=[asc, True], kind="mergesort").head(cap).copy()
            q["capacity_cap"] = cap
            q["paper_stream_version"] = f"DEFERRED_PAPER_STREAM_CAP{cap}_V1"
            q["stream_allocation_weight"] = 1.0 / len(q)
            q["paper_sample_state"] = q["next_open_net_return"].notna().map({True: "REALIZED_RESEARCH_RETURN", False: "PENDING_RESEARCH_RETURN"})
            q["operational_use"] = False
            selected.append(q)
        streams.append(pd.concat(selected, ignore_index=True))
    out = pd.concat(streams, ignore_index=True)
    if out.duplicated(["paper_stream_version", "frozen_id", "signal_date", "code", "horizon"]).any():
        raise SystemExit("duplicate paper stream key")
    latest_date = out["signal_date"].max()
    latest = out[out["signal_date"].eq(latest_date)].copy()
    summary = out.groupby(["paper_stream_version", "capacity_cap", "frozen_id", "strategy", "signal", "regime", "horizon"], as_index=False).agg(candidate_rows=("code", "size"), signal_dates=("signal_date", "nunique"), realized_rows=("next_open_net_return", lambda x: int(pd.to_numeric(x, errors="coerce").notna().sum())), pending_rows=("next_open_net_return", lambda x: int(pd.to_numeric(x, errors="coerce").isna().sum())), mean_realized_net_bps=("next_open_net_return", lambda x: float(pd.to_numeric(x, errors="coerce").mean() * 10000)))
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    latest.to_csv(OUT_LATEST, index=False, encoding="utf-8-sig")
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "OK", "scope": "research_only_deferred_paper_streams", "caps": list(CAPS), "stream_versions": [f"DEFERRED_PAPER_STREAM_CAP{c}_V1" for c in CAPS], "rows": int(len(out)), "latest_signal_date": str(latest_date.date()) if pd.notna(latest_date) else None, "latest_rows": int(len(latest)), "realized_rows": int(out["next_open_net_return"].notna().sum()), "pending_rows": int(out["next_open_net_return"].isna().sum()), "operational_change": False, "outputs": {"history": str(OUT), "latest_pending": str(OUT_LATEST), "summary": str(OUT_SUMMARY)}}
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Deferred Paper Streams", "", f"- streams: {', '.join(payload['stream_versions'])}", f"- rows: {len(out)}", f"- latest_signal_date: {payload['latest_signal_date']}", f"- latest_rows: {len(latest)}", f"- realized_rows: {payload['realized_rows']}", f"- pending_rows: {payload['pending_rows']}", "- operational_change: false"]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
