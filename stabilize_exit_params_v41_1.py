# -*- coding: utf-8 -*-
"""
Exit parameter stabilizer.

Problem
- Positive stop_loss values such as 0.02 must be interpreted as invalid
  for this config because stop loss is stored as a negative return threshold.

Policy
- Force take_profit and trail_pct to OFF(None).
- stop_loss:
  - None or value >= 0 -> DEFAULT_SL (-0.05)
  - value < MIN_SL_CAP (-0.30) -> MIN_SL_CAP

Run:
  python stabilize_exit_params_v41_1.py
"""

import json
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RC_DIR = BASE_DIR / "12_Risk_Controlled"
STABLE = RC_DIR / "stable_params_v41_1.json"
BEST = RC_DIR / "best_params_v41_1.json"

DEFAULT_SL = -0.05
MIN_SL_CAP = -0.30  # Lower bound to prevent excessive stop loss.

def _backup(path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = path.with_suffix(path.suffix + f".bak_{ts}")
    bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return bak

def _fix_stop_loss(val):
    if val is None:
        return DEFAULT_SL
    try:
        v = float(val)
    except Exception:
        return DEFAULT_SL
    if v >= 0:
        return DEFAULT_SL
    if v < MIN_SL_CAP:
        return MIN_SL_CAP
    return v

def _patch(p: dict) -> dict:
    # TP/TRAIL OFF
    p["take_profit"] = None
    p["trail_pct"] = None

    # Normalize stop_loss to a negative bounded threshold.
    p["stop_loss"] = _fix_stop_loss(p.get("stop_loss"))

    # Human-readable policy metadata.
    p["exit_policy"] = {
        "take_profit": None,
        "trail_pct": None,
        "stop_loss": p["stop_loss"],
        "note": "TP/TRAIL forced OFF; stop_loss forced negative with safe bounds"
    }
    return p

def patch_one(path: Path) -> None:
    if not path.exists():
        return
    _backup(path)
    p = json.loads(path.read_text(encoding="utf-8"))
    p = _patch(p)
    path.write_text(json.dumps(p, ensure_ascii=False, indent=2), encoding="utf-8")

    v = json.loads(path.read_text(encoding="utf-8"))
    print(f"[OK] stabilized: {path}")
    print(f"     take_profit={v.get('take_profit')} trail_pct={v.get('trail_pct')} stop_loss={v.get('stop_loss')}")

def main() -> int:
    RC_DIR.mkdir(parents=True, exist_ok=True)
    if not STABLE.exists():
        print(f"[SKIP] missing: {STABLE}")
        return 0
    patch_one(STABLE)
    patch_one(BEST)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

