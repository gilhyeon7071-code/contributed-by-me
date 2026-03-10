from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Optional


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CFG_PATH = ROOT / "paper" / "paper_engine_config.json"


def _latest(pattern: str) -> Optional[Path]:
    files = sorted(LOG_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def main() -> int:
    ap = argparse.ArgumentParser(description="Suggest/apply risk limit recalibration from latest pnl summary")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    pnl_path = _latest("paper_pnl_summary_*.json")
    if pnl_path is None:
        print("[STOP] no paper_pnl_summary found")
        return 2

    if not CFG_PATH.exists():
        print(f"[STOP] missing config: {CFG_PATH}")
        return 2

    pnl = json.loads(pnl_path.read_text(encoding="utf-8"))
    cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))

    eq = pnl.get("equity", {}) if isinstance(pnl, dict) else {}
    max_dd = abs(float(eq.get("max_drawdown_pct", 0.0) or 0.0))
    last_day_ret = float(eq.get("last_day_ret", 0.0) or 0.0)

    cur_max_new = int(cfg.get("max_new_trades_per_day", 3) or 3)
    cur_gross = float(cfg.get("max_gross_exposure_pct", 1.0) or 1.0)
    cur_daily = float(cfg.get("max_daily_new_exposure_pct", 1.0) or 1.0)

    mult = 1.0
    reason = "NEUTRAL"
    if max_dd >= 0.25:
        mult = 0.6
        reason = "HIGH_DRAWDOWN"
    elif max_dd >= 0.18:
        mult = 0.75
        reason = "ELEVATED_DRAWDOWN"
    elif max_dd <= 0.08 and last_day_ret > 0:
        mult = 1.1
        reason = "LOW_DRAWDOWN_POSITIVE"

    sug_max_new = max(1, int(round(cur_max_new * mult)))
    sug_gross = _clamp(cur_gross * mult, 0.20, 1.00)
    sug_daily = _clamp(cur_daily * mult, 0.10, 1.00)

    suggestion = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "source_pnl": str(pnl_path),
        "reason": reason,
        "metrics": {"max_drawdown_pct": max_dd, "last_day_ret": last_day_ret},
        "current": {
            "max_new_trades_per_day": cur_max_new,
            "max_gross_exposure_pct": cur_gross,
            "max_daily_new_exposure_pct": cur_daily,
        },
        "suggested": {
            "max_new_trades_per_day": sug_max_new,
            "max_gross_exposure_pct": sug_gross,
            "max_daily_new_exposure_pct": sug_daily,
        },
        "applied": False,
    }

    if args.apply:
        cfg["max_new_trades_per_day"] = int(sug_max_new)
        cfg["max_gross_exposure_pct"] = float(sug_gross)
        cfg["max_daily_new_exposure_pct"] = float(sug_daily)
        CFG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        suggestion["applied"] = True

    out_json = LOG_DIR / "risk_recalibration_latest.json"
    out_json.write_text(json.dumps(suggestion, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] suggestion={out_json}")
    print(f"[OK] applied={bool(suggestion['applied'])} reason={reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
