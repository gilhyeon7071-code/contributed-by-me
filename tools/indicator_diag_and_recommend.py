#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Run multi-horizon indicator diagnostics (1/2/5d) and generate
parameter candidate sets (conservative/neutral/aggressive).

Outputs:
- E:\\1_Data\\2_Logs\\indicator_diag_*_latest_h{h}.(csv|json)
- E:\\1_Data\\12_Risk_Controlled\\param_candidates_v41_1_YYYYMMDD_HHMMSS.json
- E:\\1_Data\\12_Risk_Controlled\\param_candidates_v41_1_latest.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Dict, List

import numpy as np
import pandas as pd


ROOT = Path(r"E:\1_Data")
TOOLS = ROOT / "tools"
LOG_DIR = ROOT / "2_Logs"
RISK_DIR = ROOT / "12_Risk_Controlled"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import generate_candidates_v41_1 as gen  # noqa: E402


def _now_ts() -> str:
    return pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")


def _load_json(p: Path) -> Dict:
    return json.loads(p.read_text(encoding="utf-8"))


def _run_diag(py_exe: str, lookback_days: int, min_universe: int, h: int) -> None:
    tag = f"h{h}"
    script = TOOLS / "indicator_factor_diagnostic.py"
    cmd = [
        py_exe,
        str(script),
        "--lookback-days",
        str(int(lookback_days)),
        "--min-universe",
        str(int(min_universe)),
        "--target-horizon",
        str(int(h)),
        "--tag",
        tag,
    ]
    print("[RUN]", " ".join(cmd))
    cp = subprocess.run(cmd, capture_output=True, text=True)
    if cp.stdout:
        print(cp.stdout.strip())
    if cp.stderr:
        # keep warnings visible, but not fatal unless return code non-zero
        print(cp.stderr.strip())
    if cp.returncode != 0:
        raise RuntimeError(f"indicator_factor_diagnostic failed (h={h}, rc={cp.returncode})")


def _latest_diag_paths(h: int) -> Dict[str, Path]:
    tag = f"h{h}"
    return {
        "summary": LOG_DIR / f"indicator_diag_summary_latest_{tag}.json",
        "binary": LOG_DIR / f"indicator_diag_binary_latest_{tag}.csv",
        "continuous": LOG_DIR / f"indicator_diag_continuous_latest_{tag}.csv",
        "combo": LOG_DIR / f"indicator_diag_daily_combo_latest_{tag}.csv",
    }


def _safe_float(x, default=np.nan) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _build_profile(base: Dict, profile: str, signals: Dict) -> Dict:
    p = dict(base)
    sparse = signals["h1_avg_selected_n"] < 3.0 or signals["h1_selected_ratio"] < 0.8
    weak_combo = signals["h1_avg_alpha_bps"] < 0

    volcorr_bad = signals["volcorr_delta_bps"] < -20.0
    high52_bad = signals["high52_delta_bps"] < -20.0

    if profile == "conservative":
        p["rs_lim"] = _safe_float(base.get("rs_lim", 1.7)) + (0.02 if not sparse else 0.0)
        p["v_accel_lim"] = _safe_float(base.get("v_accel_lim", 2.5)) + (0.10 if not sparse else 0.0)
        p["stretch_max"] = _safe_float(base.get("stretch_max", 1.19)) - (0.01 if not sparse else 0.0)
        p["atr_max"] = _safe_float(base.get("atr_max", 0.12)) - 0.005
        p["value_min"] = _safe_float(base.get("value_min", 1e9)) * (1.00 if sparse else 1.10)
        p["rsi_max"] = min(70.0, _safe_float(base.get("rsi_max", 70.0)))
        p["min_listing_days"] = _safe_float(base.get("min_listing_days", 126.0)) + 20.0
        if volcorr_bad:
            p["vol_close_corr_min"] = _safe_float(base.get("vol_close_corr_min", 0.0)) - 0.05
        if high52_bad:
            p["near_52w_high_gap_max"] = _safe_float(base.get("near_52w_high_gap_max", 0.05)) + 0.02

    elif profile == "neutral":
        p["rs_lim"] = _safe_float(base.get("rs_lim", 1.7)) - (0.05 if sparse else 0.02)
        p["v_accel_lim"] = _safe_float(base.get("v_accel_lim", 2.5)) - (0.30 if sparse else 0.15)
        p["stretch_max"] = _safe_float(base.get("stretch_max", 1.19)) + 0.02
        p["atr_max"] = _safe_float(base.get("atr_max", 0.12)) + 0.01
        p["value_min"] = _safe_float(base.get("value_min", 1e9)) * 0.85
        p["rsi_max"] = max(68.0, _safe_float(base.get("rsi_max", 70.0)))
        if volcorr_bad:
            p["vol_close_corr_min"] = _safe_float(base.get("vol_close_corr_min", 0.0)) - 0.12
        if high52_bad:
            p["near_52w_high_gap_max"] = _safe_float(base.get("near_52w_high_gap_max", 0.05)) + 0.05

    elif profile == "aggressive":
        p["rs_lim"] = _safe_float(base.get("rs_lim", 1.7)) - (0.12 if weak_combo else 0.08)
        p["v_accel_lim"] = _safe_float(base.get("v_accel_lim", 2.5)) - 0.55
        p["stretch_max"] = _safe_float(base.get("stretch_max", 1.19)) + 0.04
        p["atr_max"] = _safe_float(base.get("atr_max", 0.12)) + 0.02
        p["value_min"] = _safe_float(base.get("value_min", 1e9)) * 0.65
        p["rsi_max"] = max(72.0, _safe_float(base.get("rsi_max", 70.0)))
        p["min_listing_days"] = max(90.0, _safe_float(base.get("min_listing_days", 126.0)) - 24.0)
        p["vol_close_corr_min"] = _safe_float(base.get("vol_close_corr_min", 0.0)) - 0.20
        p["near_52w_high_gap_max"] = _safe_float(base.get("near_52w_high_gap_max", 0.05)) + 0.08

    else:
        raise ValueError(profile)

    # Normalize with project bounds.
    n = gen._normalize_params(p)
    n["profile"] = profile
    return n


def _pick_recommended(sigs: Dict) -> str:
    avg_alpha = np.nanmean([sigs["h1_avg_alpha_bps"], sigs["h2_avg_alpha_bps"], sigs["h5_avg_alpha_bps"]])
    sparse = sigs["h1_avg_selected_n"] < 2.0
    if sparse and avg_alpha < 0:
        return "neutral"
    if avg_alpha > 0 and sigs["h1_selected_ratio"] > 0.8:
        return "conservative"
    return "neutral"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--min-universe", type=int, default=2000)
    ap.add_argument("--horizons", type=str, default="1,2,5")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    RISK_DIR.mkdir(parents=True, exist_ok=True)

    py_exe = sys.executable
    horizons = [int(x.strip()) for x in str(args.horizons).split(",") if x.strip()]
    for h in horizons:
        if h not in (1, 2, 5):
            raise ValueError(f"unsupported horizon: {h}")

    for h in horizons:
        _run_diag(py_exe, lookback_days=args.lookback_days, min_universe=args.min_universe, h=h)

    summaries = {}
    binaries = {}
    for h in horizons:
        paths = _latest_diag_paths(h)
        for k, p in paths.items():
            if not p.exists():
                raise FileNotFoundError(f"missing diagnostic artifact: {p}")
        summaries[h] = _load_json(paths["summary"])
        binaries[h] = pd.read_csv(paths["binary"])

    h1_summary = summaries[1]
    h2_summary = summaries[2]
    h5_summary = summaries[5]
    h1_bin = binaries[1]

    def _delta_of(filter_name: str) -> float:
        x = h1_bin[h1_bin["filter"] == filter_name]
        if x.empty:
            return float("nan")
        return _safe_float(x.iloc[0].get("pass_minus_fail_bps"), np.nan)

    sigs = {
        "h1_avg_alpha_bps": _safe_float(h1_summary.get("combo", {}).get("avg_alpha_bps"), np.nan),
        "h2_avg_alpha_bps": _safe_float(h2_summary.get("combo", {}).get("avg_alpha_bps"), np.nan),
        "h5_avg_alpha_bps": _safe_float(h5_summary.get("combo", {}).get("avg_alpha_bps"), np.nan),
        "h1_avg_selected_n": _safe_float(h1_summary.get("combo", {}).get("avg_selected_n"), np.nan),
        "h1_selected_ratio": _safe_float(h1_summary.get("combo", {}).get("selected_day_ratio"), np.nan),
        "volcorr_delta_bps": _delta_of("volcorr_pass"),
        "high52_delta_bps": _delta_of("high52_pass"),
        "all_pass_delta_bps": _delta_of("all_pass"),
    }

    stable_path = RISK_DIR / "stable_params_v41_1.json"
    stable = _load_json(stable_path)
    base = gen._normalize_params(stable)

    profiles = {
        "conservative": _build_profile(base, "conservative", sigs),
        "neutral": _build_profile(base, "neutral", sigs),
        "aggressive": _build_profile(base, "aggressive", sigs),
    }

    recommended = _pick_recommended(sigs)

    ts = _now_ts()
    out_path = RISK_DIR / f"param_candidates_v41_1_{ts}.json"
    latest_path = RISK_DIR / "param_candidates_v41_1_latest.json"
    md_path = ROOT / "docs" / f"PARAM_CANDIDATES_V41_1_{ts}.md"

    payload = {
        "as_of": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "indicator_diag_and_recommend",
        "lookback_days": int(args.lookback_days),
        "min_universe": int(args.min_universe),
        "horizons": horizons,
        "signals": sigs,
        "recommended_profile": recommended,
        "profiles": profiles,
        "notes": [
            "Use paper_engine_config_lock.py for controlled apply.",
            "Recommendation is data-driven heuristic, not auto-apply.",
        ],
        "diag_refs": {
            "h1": str(_latest_diag_paths(1)["summary"]),
            "h2": str(_latest_diag_paths(2)["summary"]),
            "h5": str(_latest_diag_paths(5)["summary"]),
        },
    }

    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    shutil.copyfile(out_path, latest_path)

    ROOT.joinpath("docs").mkdir(parents=True, exist_ok=True)
    md_lines = [
        f"# Param Candidates v41.1 ({ts})",
        "",
        f"- Recommended: `{recommended}`",
        f"- h1 avg_alpha_bps: `{sigs['h1_avg_alpha_bps']:.2f}`",
        f"- h2 avg_alpha_bps: `{sigs['h2_avg_alpha_bps']:.2f}`",
        f"- h5 avg_alpha_bps: `{sigs['h5_avg_alpha_bps']:.2f}`",
        f"- h1 avg_selected_n: `{sigs['h1_avg_selected_n']:.2f}`",
        "",
        "## Profiles",
    ]
    for k in ("conservative", "neutral", "aggressive"):
        p = profiles[k]
        md_lines.append(f"### {k}")
        md_lines.append(f"- rs_lim: `{p['rs_lim']}`")
        md_lines.append(f"- v_accel_lim: `{p['v_accel_lim']}`")
        md_lines.append(f"- stretch_max: `{p['stretch_max']}`")
        md_lines.append(f"- value_min: `{p['value_min']}`")
        md_lines.append(f"- atr_max: `{p['atr_max']}`")
        md_lines.append(f"- rsi_max: `{p['rsi_max']}`")
        md_lines.append(f"- vol_close_corr_min: `{p['vol_close_corr_min']}`")
        md_lines.append(f"- near_52w_high_gap_max: `{p['near_52w_high_gap_max']}`")
        md_lines.append(f"- min_listing_days: `{p['min_listing_days']}`")
        md_lines.append("")

    md_lines += [
        "## Artifacts",
        f"- `{out_path}`",
        f"- `{latest_path}`",
    ]
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    print(f"[OK] candidates={out_path}")
    print(f"[OK] latest={latest_path}")
    print(f"[OK] report={md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

