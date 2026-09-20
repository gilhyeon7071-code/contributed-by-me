"""Capacity sensitivity for the six deferred rules using their intrinsic signal rank."""
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
OUT = LOG / "deferred_capacity_sensitivity_latest.csv"
OUT_JSON = LOG / "deferred_capacity_sensitivity_latest.json"
OUT_MD = LOG / "deferred_capacity_sensitivity_latest.md"
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
    f["research_regime"] = f["date"].map(report._assign_report_research_regime(f))
    # Intrinsic ranking fields only; no new filter is introduced.
    factor = f[["code", "date", "rs_slope", "stretch"]].copy()
    factor["code"] = factor["code"].astype(str).str.zfill(6)
    factor = factor.drop_duplicates(["code", "date"])
    obs = pd.read_csv(OBSERVE, dtype={"code": str})
    obs["code"] = obs["code"].astype(str).str.zfill(6)
    obs["date"] = pd.to_datetime(obs["date"], errors="coerce").dt.normalize()
    obs = obs.merge(factor, on=["code", "date"], how="left", validate="many_to_one")
    obs = obs[obs["next_open_net_return"].notna()].copy()
    if obs.empty:
        raise SystemExit("no realized observe-only rows")
    obs["rank_value"] = obs["rs_slope"].where(obs["signal"].eq("RS_SLOPE_Q5"), obs["stretch"])
    obs["rank_ascending"] = obs["signal"].eq("STRETCH_Q1")
    rows = []
    for cap in CAPS:
        selected = []
        for key, g in obs.groupby(["frozen_id", "signal_date"], dropna=False):
            asc = bool(g["rank_ascending"].iloc[0])
            q = g.sort_values(["rank_value", "code"], ascending=[asc, True], kind="mergesort").head(cap).copy()
            q["capacity_cap"] = cap
            q["selected_weight"] = 1.0 / len(q)
            selected.append(q)
        sel = pd.concat(selected, ignore_index=True)
        daily = sel.groupby(["frozen_id", "strategy", "signal", "regime", "horizon", "signal_date"], as_index=False).agg(candidate_count=("code", "size"), basket_net_return=("next_open_net_return", "mean"))
        for key, g in daily.groupby(["frozen_id", "strategy", "signal", "regime", "horizon"], dropna=False):
            ret = pd.to_numeric(g["basket_net_return"], errors="coerce")
            rows.append({"capacity_cap": cap, "frozen_id": key[0], "strategy": key[1], "signal": key[2], "regime": key[3], "horizon": key[4], "signal_dates": int(len(g)), "mean_daily_net_bps": float(ret.mean() * 10000), "median_daily_net_bps": float(ret.median() * 10000), "positive_days": int((ret > 0).sum()), "max_selected_per_day": int(g["candidate_count"].max())})
    out = pd.DataFrame(rows).sort_values(["frozen_id", "capacity_cap"])
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "OK", "scope": "research_only_deferred_capacity_sensitivity", "caps": list(CAPS), "ranking_policy": {"RS_SLOPE_Q5": "rs_slope descending", "STRETCH_Q1": "stretch ascending", "tie_break": "code ascending"}, "rows": int(len(obs)), "result_rows": int(len(out)), "operational_change": False, "outputs": {"csv": str(OUT), "md": str(OUT_MD)}}
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Deferred Capacity Sensitivity", "", f"- caps: {', '.join(map(str, CAPS))}", "- RS_SLOPE_Q5 ranking: rs_slope descending", "- STRETCH_Q1 ranking: stretch ascending", "- tie-break: code ascending", "- operational_change: false", "", "## Results"]
    for _, r in out.iterrows():
        lines.append(f"- cap={int(r['capacity_cap'])} id={r['frozen_id']} {r['strategy']} {r['signal']} {r['regime']} {r['horizon']}: mean={float(r['mean_daily_net_bps']):.2f}bp, positive_days={int(r['positive_days'])}/{int(r['signal_dates'])}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
