"""max_pos sweep using actual KRX historical data + full compute_factors."""
from __future__ import annotations
import json, sys, types
from pathlib import Path
import numpy as np
import pandas as pd

BASE_DIR = Path(r"E:\1_Data")

# ─── register module in sys.modules BEFORE exec so dataclass works ───────────
mod_path = BASE_DIR / "optimize_params_v41_1.py"
mod = types.ModuleType("optimize_params_v41_1")
mod.__file__ = str(mod_path)
mod.__spec__ = None
sys.modules["optimize_params_v41_1"] = mod
with open(mod_path, encoding="utf-8-sig") as f:
    code = compile(f.read(), str(mod_path), "exec")
exec(code, mod.__dict__)

# ─── aliases ─────────────────────────────────────────────────────────────────
load_data       = mod.load_data
compute_factors = mod.compute_factors
build_windows   = mod.build_windows
eval_params     = mod.eval_params
FROZEN_KEYS     = mod.FROZEN_KEYS
BOUNDS          = mod.BOUNDS
TRAIN_END       = mod.TRAIN_END
VAL_END         = mod.VAL_END

# ─── load KRX historical data ────────────────────────────────────────────────
print("Loading KRX data...", flush=True)
df = load_data(BASE_DIR)
print(f"  raw rows={len(df):,}  codes={df['code'].nunique():,}  "
      f"dates={df['date'].min().date()} ~ {df['date'].max().date()}", flush=True)

print("Computing factors...", flush=True)
df = compute_factors(df)
df = df.dropna(subset=["rs","v_accel","stretch","value","atr14_pct","rsi14"]).copy()
print(f"  after factors: {len(df):,} rows", flush=True)

# ─── base params from current stable ─────────────────────────────────────────
stable = json.load(open(r"E:\1_Data\12_Risk_Controlled\stable_params_v41_1.json", encoding="utf-8-sig"))
base = {}
for k in list(BOUNDS.keys()) + list(FROZEN_KEYS):
    if k in stable:
        base[k] = stable[k]
# [2026-09-10] fee 는 시뮬에 넘기는 **왕복** 값이다. DEFAULT_FEE 와 같은 0.00400 으로.
base.setdefault("fee", 0.00400)
base["w_rs"]        = stable["w_rs"]
base["w_rs_slope"]  = stable["w_rs_slope"]
base["w_v_accel"]   = stable["w_v_accel"]
base["use_relax_ladder"] = stable.get("use_relax_ladder", 1.0)
base["gap_up_max_pct"]   = stable.get("gap_up_max_pct", 0.03)
base["entry_gap_down_stop_pct"] = stable.get("entry_gap_down_stop_pct", 0.03)
for k in ["rsi_max","require_macd_golden","vol_close_corr_min",
          "near_52w_high_gap_max","min_listing_days","gap_limit"]:
    base[k] = stable.get(k, base.get(k, 0.0))

windows = build_windows(df, years_back=10)
oos_windows = [(s,e) for s,e in windows if e > VAL_END]
val_windows = [(s,e) for s,e in windows if TRAIN_END < e <= VAL_END]
print(f"\nWindows: total={len(windows)}, IS={len(windows)-len(val_windows)-len(oos_windows)}, "
      f"VAL={len(val_windows)}, OOS={len(oos_windows)}")
print(f"Base: hold={base['hold']} stop={base['stop_loss']} rs_lim={base['rs_lim']} "
      f"v_accel={base['v_accel_lim']} max_pos={base['max_pos']}\n")

# ─── sweep ────────────────────────────────────────────────────────────────────
sweep_vals = [4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 20]

print(f"{'mp':>5} {'score':>9} {'oos_pf':>7} {'oos_ret%':>9} {'oos_n':>6} "
      f"{'val_pf':>7} {'val_n':>6} {'is_pf':>7}")
print("─" * 65)

rows = []
for mp in sweep_vals:
    p = dict(base)
    p["max_pos"] = mp
    mod._PRICE_CACHE = None
    score, results = eval_params(df, windows, p)

    def _avg(lst, key): return float(np.mean([r.__dict__[key] if hasattr(r,'__dict__') else r[key] for r in lst])) if lst else 0.0
    def _sum(lst, key): return int(sum(r.__dict__[key] if hasattr(r,'__dict__') else r[key] for r in lst))

    # results may be dataclass or dict depending on version
    def _get(r, k):
        return getattr(r, k) if hasattr(r, k) else r[k]

    oos = [r for r in results if _get(r, "split") == "OOS"]
    val = [r for r in results if _get(r, "split") == "VAL"]
    is_ = [r for r in results if _get(r, "split") == "IS"]

    oos_pf  = float(np.mean([_get(r,"pf") for r in oos])) if oos else 0.0
    oos_ret = float(np.mean([_get(r,"mean_ret") for r in oos])) * 100 if oos else 0.0
    oos_n   = sum(_get(r,"n_trades") for r in oos) if oos else 0
    val_pf  = float(np.mean([_get(r,"pf") for r in val])) if val else 0.0
    val_n   = sum(_get(r,"n_trades") for r in val) if val else 0
    is_pf   = float(np.mean([_get(r,"pf") for r in is_])) if is_ else 0.0
    marker  = "  <-- current" if mp == stable.get("max_pos", 8) else ""

    print(f"{mp:>5} {score:>9.3f} {oos_pf:>7.3f} {oos_ret:>9.4f} {oos_n:>6} "
          f"{val_pf:>7.3f} {val_n:>6} {is_pf:>7.3f}{marker}", flush=True)
    rows.append({"mp": mp, "score": score, "oos_pf": oos_pf,
                 "oos_ret": oos_ret, "oos_n": oos_n, "val_pf": val_pf})

best = max(rows, key=lambda r: r["score"])
cur  = next(r for r in rows if r["mp"] == stable.get("max_pos", 8))
print()
print(f"Best:    max_pos={best['mp']:>2}  score={best['score']:>9.3f}  "
      f"oos_pf={best['oos_pf']:.3f}  oos_ret={best['oos_ret']:.4f}%  oos_n={best['oos_n']}")
print(f"Current: max_pos={cur['mp']:>2}  score={cur['score']:>9.3f}  "
      f"oos_pf={cur['oos_pf']:.3f}  oos_ret={cur['oos_ret']:.4f}%  oos_n={cur['oos_n']}")

if best["mp"] != cur["mp"]:
    diff = best["score"] - cur["score"]
    print(f"\n→ 최적값 max_pos={best['mp']} (현재 대비 score +{diff:.3f})")
else:
    print(f"\n→ 현재 max_pos={cur['mp']}가 스윕 범위 내 최적값")
