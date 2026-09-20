"""Read-only long-history robustness audit of the six frozen cost-positive rules."""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
BASE_SCRIPT = ROOT / "tools" / "run_frozen_positive_next_open_cost_validation.py"
OUT = LOG / "frozen_positive_long_history_robustness_latest.csv"
OUT_JSON = LOG / "frozen_positive_long_history_robustness_latest.json"
OUT_MD = LOG / "frozen_positive_long_history_robustness_latest.md"
PERIODS = {
    "R1_202001_202112": (pd.Timestamp("2020-01-02"), pd.Timestamp("2021-12-31")),
    "R2_202201_202312": (pd.Timestamp("2022-01-01"), pd.Timestamp("2023-12-31")),
    "R3_202401_202512": (pd.Timestamp("2024-01-01"), pd.Timestamp("2025-12-31")),
    "R4_202601_202607": (pd.Timestamp("2026-01-01"), pd.Timestamp("2026-07-15")),
}


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
    panel = f[f["date"].between(pd.Timestamp("2020-01-02"), pd.Timestamp("2026-07-15")) & f["market"].astype(str).str.upper().isin(["KOSPI", "KOSDAQ"]) & pd.to_numeric(f["close"], errors="coerce").gt(0) & pd.to_numeric(f["value"], errors="coerce").gt(0)].copy()
    frozen = pd.read_csv(LOG / "frozen_positive_next_open_cost_summary_latest.csv")
    frozen = frozen[frozen["cost_verdict"].eq("COST_NET_POSITIVE_TRAIN_HOLDOUT")].copy()
    frozen["strategy_family"] = frozen["strategy"]
    frozen["strategy_name"] = frozen["strategy"]
    frozen["signal_name"] = frozen["signal"]
    frozen["research_regime"] = frozen["regime"]
    rows = []
    for i, rule in frozen.reset_index(drop=True).iterrows():
        mask = m.mask_for(rule, panel)
        ret_col = f"next_open_return_{rule['horizon']}"
        q = panel.loc[mask & panel[ret_col].notna(), ["date", "code", ret_col]].copy()
        period = pd.Series(pd.NA, index=q.index, dtype="object")
        for name, (start, end) in PERIODS.items():
            period.loc[q["date"].between(start, end)] = name
        q["period"] = period
        means = {}
        counts = {}
        dates = {}
        for name in PERIODS:
            x = q[q["period"].eq(name)]
            means[name] = float(pd.to_numeric(x[ret_col], errors="coerce").mean() * 10000) if not x.empty else None
            counts[name] = int(len(x))
            dates[name] = int(x["date"].nunique())
        valid_means = [v for v in means.values() if v is not None]
        rec = {"frozen_id": i, "source": rule.get("source"), "scope": rule.get("scope"), "horizon": rule.get("horizon"), "strategy": rule.get("strategy_family") if pd.notna(rule.get("strategy_family")) else rule.get("strategy_name"), "signal": rule.get("signal_name"), "regime": rule.get("research_regime"), "total_rows": int(len(q)), "total_dates": int(q["date"].nunique()), "positive_periods": int(sum(v is not None and v > 0 for v in means.values())), "periods_with_data": int(len(valid_means))}
        for name in PERIODS:
            rec[f"{name}_mean_net_bps"] = means[name]
            rec[f"{name}_rows"] = counts[name]
            rec[f"{name}_dates"] = dates[name]
        rec["robustness_state"] = "ALL_FOUR_PERIODS_POSITIVE_DESCRIPTIVE" if len(valid_means) == 4 and all(v > 0 for v in valid_means) else "MIXED_OR_NEGATIVE_DESCRIPTIVE"
        rows.append(rec)
    out = pd.DataFrame(rows)
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "status": "OK", "scope": "read_only_frozen_positive_long_history_robustness", "window": {"start": "2020-01-02", "end": "2026-07-15"}, "periods": list(PERIODS), "frozen_combinations": int(len(frozen)), "panel_rows": int(len(panel)), "all_four_positive_descriptive": int((out["robustness_state"] == "ALL_FOUR_PERIODS_POSITIVE_DESCRIPTIVE").sum()), "price_history_contract": integrity, "operational_change": False, "limitations": ["Rules were selected using a later window, so this is robustness context, not independent confirmation.", "No new rule search or multiple-comparison correction was performed.", "No capacity, fill rejection, or paper-runtime evidence was modeled."], "outputs": {"csv": str(OUT), "md": str(OUT_MD)}}
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    lines = ["# Frozen Positive Long-History Robustness", "", f"- frozen_combinations: {len(frozen)}", f"- panel_rows: {len(panel)}", f"- all_four_positive_descriptive: {payload['all_four_positive_descriptive']}", "", "## Results"]
    for _, r in out.iterrows():
        lines.append(f"- id={r['frozen_id']} {r['strategy']} {r['signal']} {r['regime']} {r['horizon']}: positive_periods={r['positive_periods']}/4, rows={r['total_rows']}, dates={r['total_dates']}, state={r['robustness_state']}")
    lines.extend(["", "## Limitations", *[f"- {x}" for x in payload["limitations"]]])
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
