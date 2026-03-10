# -*- coding: utf-8 -*-
import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
RC_DIR = BASE_DIR / "12_Risk_Controlled"
STABLE = RC_DIR / "stable_params_v41_1.json"
BACKTEST_SUMMARY = RC_DIR / "report_backtest_summary_v41_1.json"
LOG_DIR = BASE_DIR / "2_Logs"

FRESH_DAYS = 7  # refresh window for periodic optimize


def _mtime_days(path: Path) -> float:
    if not path.exists():
        return 1e9
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    return age.total_seconds() / 86400.0


def _jread(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def should_optimize() -> tuple[bool, str]:
    if not STABLE.exists():
        return True, "stable_params missing"
    age_days = _mtime_days(STABLE)
    if age_days >= FRESH_DAYS:
        return True, f"stable_params stale ({age_days:.1f}d >= {FRESH_DAYS}d)"
    return False, f"stable_params fresh ({age_days:.1f}d)"


def enforce_exit_stability() -> int:
    script = BASE_DIR / "stabilize_exit_params_v41_1.py"
    if not script.exists():
        print(f"[WARN] missing stabilizer: {script}")
        return 0
    r = subprocess.run([sys.executable, str(script)], cwd=str(BASE_DIR))
    return r.returncode


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run optimizer when due (or force-run).")
    ap.add_argument("--force", action="store_true", help="Force optimizer regardless of staleness.")
    ap.add_argument("--reason", default="", help="Optional reason for force run (for logs).")

    # quality gate
    ap.add_argument("--skip-quality-gate", action="store_true", help="Skip backtest quality/freshness gate")
    ap.add_argument("--max-backtest-age-days", type=float, default=7.0, help="Max age for report_backtest_summary")
    ap.add_argument("--min-oos-trades", type=int, default=20, help="Minimum OOS trades for optimize")
    ap.add_argument("--min-oos-pf", type=float, default=0.80, help="Minimum OOS PF for optimize")
    ap.add_argument("--min-stable-score", type=float, default=-1e9, help="Minimum stable best_score for optimize")

    # rollback gate
    ap.add_argument("--no-auto-rollback", action="store_true", help="Disable automatic rollback after optimize")
    ap.add_argument("--rollback-drop-pct", type=float, default=0.10, help="Rollback if score drops more than this fraction")
    ap.add_argument("--rollback-score-floor", type=float, default=-1e8, help="Rollback if new stable score is below/equal this floor")
    return ap.parse_args()


def _write_status(payload: dict) -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        p_ts = LOG_DIR / f"optimize_if_due_{ts}.json"
        p_last = LOG_DIR / "optimize_if_due_last.json"
        txt = json.dumps(payload, ensure_ascii=False, indent=2)
        p_ts.write_text(txt, encoding="utf-8")
        p_last.write_text(txt, encoding="utf-8")
    except Exception:
        pass


def _quality_gate(args: argparse.Namespace) -> Tuple[bool, Dict[str, Any]]:
    out: Dict[str, Any] = {
        "summary_path": str(BACKTEST_SUMMARY),
        "stable_path": str(STABLE),
        "max_backtest_age_days": float(args.max_backtest_age_days),
        "min_oos_trades": int(args.min_oos_trades),
        "min_oos_pf": float(args.min_oos_pf),
        "min_stable_score": float(args.min_stable_score),
        "age_days": None,
        "oos_n": None,
        "oos_pf": None,
        "stable_score": None,
        "ok": False,
        "reasons": [],
    }

    if not BACKTEST_SUMMARY.exists():
        out["reasons"].append("missing_backtest_summary")
        return False, out
    if not STABLE.exists():
        out["reasons"].append("missing_stable_params")
        return False, out

    age = _mtime_days(BACKTEST_SUMMARY)
    out["age_days"] = float(age)
    if age > float(args.max_backtest_age_days):
        out["reasons"].append(f"stale_backtest_summary({age:.2f}d>{float(args.max_backtest_age_days):.2f}d)")

    summary = _jread(BACKTEST_SUMMARY)
    oos = (summary.get("splits") or {}).get("OOS") or {}
    oos_n = int(oos.get("n") or 0)
    oos_pf = float(oos.get("pf") or 0.0)
    out["oos_n"] = oos_n
    out["oos_pf"] = oos_pf

    stable = _jread(STABLE)
    stable_score = float(stable.get("best_score") or 0.0)
    out["stable_score"] = stable_score

    if oos_n < int(args.min_oos_trades):
        out["reasons"].append(f"oos_trades_low({oos_n}<{int(args.min_oos_trades)})")
    if oos_pf < float(args.min_oos_pf):
        out["reasons"].append(f"oos_pf_low({oos_pf:.4f}<{float(args.min_oos_pf):.4f})")
    if stable_score < float(args.min_stable_score):
        out["reasons"].append(f"stable_score_low({stable_score:.4f}<{float(args.min_stable_score):.4f})")

    out["ok"] = len(out["reasons"]) == 0
    return bool(out["ok"]), out


def _backup_stable() -> Optional[Path]:
    if not STABLE.exists():
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = RC_DIR / f"stable_params_v41_1.pre_opt_{ts}.json"
    shutil.copy2(STABLE, bak)
    return bak


def _score_from_stable(path: Path) -> Optional[float]:
    try:
        j = _jread(path)
        if not j:
            return None
        return float(j.get("best_score"))
    except Exception:
        return None


def _maybe_rollback(backup_path: Optional[Path], args: argparse.Namespace) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "enabled": not bool(args.no_auto_rollback),
        "backup_path": str(backup_path) if backup_path else None,
        "applied": False,
        "old_score": None,
        "new_score": None,
        "drop_pct": None,
        "rollback_drop_pct": float(args.rollback_drop_pct),
        "rollback_score_floor": float(args.rollback_score_floor),
        "reason": None,
        "error": None,
    }

    if args.no_auto_rollback:
        out["reason"] = "disabled_by_flag"
        return out
    if backup_path is None or not backup_path.exists():
        out["reason"] = "no_backup"
        return out
    if not STABLE.exists():
        out["reason"] = "stable_missing_after_optimize"
        return out

    old_score = _score_from_stable(backup_path)
    new_score = _score_from_stable(STABLE)
    out["old_score"] = old_score
    out["new_score"] = new_score

    if old_score is None or new_score is None:
        out["reason"] = "score_unavailable"
        return out

    drop_pct = 0.0
    if abs(float(old_score)) > 1e-12:
        drop_pct = (float(old_score) - float(new_score)) / abs(float(old_score))
    out["drop_pct"] = float(drop_pct)

    need_rollback = False
    if float(new_score) < float(args.rollback_score_floor):
        need_rollback = True
        out["reason"] = "score_floor_breach"
    elif float(drop_pct) > float(args.rollback_drop_pct):
        need_rollback = True
        out["reason"] = "excessive_score_drop"

    if not need_rollback:
        out["reason"] = "keep_new_stable"
        return out

    try:
        shutil.copy2(backup_path, STABLE)
        out["applied"] = True
    except Exception as e:
        out["error"] = f"rollback_copy_fail:{type(e).__name__}:{e}"
    return out


def main() -> int:
    args = parse_args()
    RC_DIR.mkdir(parents=True, exist_ok=True)

    do_opt, reason = should_optimize()
    if args.force:
        do_opt = True
        reason = f"forced: {args.reason}" if args.reason else "forced"
    print(f"[OPT] decision={do_opt} reason={reason}")

    out = {
        "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "force": bool(args.force),
        "reason": reason,
        "decision": bool(do_opt),
        "quality_gate": None,
        "stable_backup": None,
        "rollback": None,
        "optimize_rc": 0,
        "stabilizer_rc": 0,
        "status": "START",
    }

    if do_opt:
        if not args.skip_quality_gate:
            gate_ok, gate = _quality_gate(args)
            out["quality_gate"] = gate
            if not gate_ok:
                print(f"[ERROR] quality gate failed: {gate.get('reasons')}")
                out["status"] = "FAIL_QUALITY_GATE"
                out["optimize_rc"] = 3
                _write_status(out)
                return 3
        else:
            out["quality_gate"] = {"ok": True, "skipped": True}

        script = BASE_DIR / "optimize_params_v41_1.py"
        if not script.exists():
            print(f"[ERROR] missing: {script}")
            out["status"] = "FAIL_MISSING_OPT_SCRIPT"
            out["optimize_rc"] = 1
            _write_status(out)
            return 1

        bak = _backup_stable()
        out["stable_backup"] = str(bak) if bak else None

        print("[OPT] running optimize ...")
        r = subprocess.run([sys.executable, str(script)], cwd=str(BASE_DIR))
        if r.returncode != 0:
            print(f"[ERROR] optimize failed: rc={r.returncode}")
            out["status"] = "FAIL_OPTIMIZE"
            out["optimize_rc"] = int(r.returncode)
            _write_status(out)
            return r.returncode

        rb = _maybe_rollback(bak, args)
        out["rollback"] = rb
        if rb.get("error"):
            print(f"[ERROR] rollback failed: {rb.get('error')}")
            out["status"] = "FAIL_ROLLBACK"
            out["optimize_rc"] = 4
            _write_status(out)
            return 4
        if rb.get("applied"):
            print(f"[WARN] rollback applied: reason={rb.get('reason')}")

        out["optimize_rc"] = 0

    rc = enforce_exit_stability()
    if rc != 0:
        print(f"[ERROR] stabilizer failed: rc={rc}")
        out["status"] = "FAIL_STABILIZER"
        out["stabilizer_rc"] = int(rc)
        _write_status(out)
        return rc

    out["stabilizer_rc"] = 0
    if isinstance(out.get("rollback"), dict) and bool((out.get("rollback") or {}).get("applied")):
        out["status"] = "OK_ROLLBACK_APPLIED"
    else:
        out["status"] = "OK"
    _write_status(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



