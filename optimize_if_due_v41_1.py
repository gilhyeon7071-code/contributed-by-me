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
BACKTEST_VALIDATION = LOG_DIR / "backtest_validation_latest.json"
BACKTEST_CHECKLIST = LOG_DIR / "backtest_validation_checklist_latest.json"

FRESH_DAYS = 7  # refresh window for periodic optimize


def _mtime_days(path: Path) -> float:
    if not path.exists():
        return 1e9
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    return age.total_seconds() / 86400.0


def _jread(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}

def _normalize_ymd(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        if hasattr(x, "strftime"):
            return x.strftime("%Y%m%d")
    except Exception:
        pass
    s = str(x).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10].replace("-", "")
    if len(s) >= 8 and s[:8].isdigit():
        return s[:8]
    return None

def _extract_summary_asof_ymd(summary: Dict[str, Any]) -> Optional[str]:
    if not isinstance(summary, dict):
        return None
    cand = _normalize_ymd(summary.get("as_of_ymd") or summary.get("as_of") or summary.get("input_asof_ymd"))
    if cand:
        return cand
    g = str(summary.get("generated_at") or "").strip()
    if len(g) >= 10 and g[4] == "-" and g[7] == "-":
        return _normalize_ymd(g[:10])
    return None


def should_optimize() -> tuple[bool, str]:
    if not STABLE.exists():
        return True, "stable_params missing"
    stable = _jread(STABLE)
    issues: list[str] = []
    try:
        best_score = float(stable.get("best_score") or 0.0)
        if best_score <= -1e8:
            issues.append(f"stable_score_floor({best_score:.0f})")
    except Exception:
        issues.append("stable_score_parse_error")
    windows = stable.get("windows") if isinstance(stable.get("windows"), list) else []
    oos_windows = [w for w in windows if str((w or {}).get("split") or "").upper() == "OOS"]
    oos_n_total = int(sum(int((w or {}).get("n_trades") or 0) for w in oos_windows)) if oos_windows else 0
    if oos_n_total <= 0:
        issues.append("stable_oos_n_zero")
    as_of = str(stable.get("as_of") or "").strip()
    if as_of:
        try:
            as_of_dt = datetime.strptime(as_of[:10], "%Y-%m-%d")
            as_of_age_days = (datetime.now() - as_of_dt).total_seconds() / 86400.0
            if as_of_age_days >= FRESH_DAYS:
                issues.append(f"stable_asof_stale({as_of_age_days:.1f}d)")
        except Exception:
            issues.append("stable_asof_parse_error")
    if issues:
        return True, "stable_params unhealthy: " + ",".join(issues)
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
    ap.add_argument("--min-oos-pf", type=float, default=0.75, help="Minimum OOS PF for optimize")
    ap.add_argument("--min-stable-score", type=float, default=-20.0, help="Minimum stable best_score for optimize")
    ap.add_argument("--skip-validation-gate", action="store_true", help="Skip backtest validation checklist gate")
    ap.add_argument(
        "--allow-early-logic-check-optimize",
        action="store_true",
        help="Allow optimization during paper_early_logic_check when hard optimizer prerequisites pass.",
    )
    ap.add_argument("--max-validation-age-days", type=float, default=14.0, help="Max age for backtest validation checklist/json")
    ap.add_argument("--min-robust-ratio", type=float, default=0.60, help="Minimum robust_ratio from strategy_parameter_validation")

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

    _append_firing_ledger(payload)


def _append_firing_ledger(payload: dict) -> None:
    """HPO 자동 발동을 **append-only 원장**에 한 줄 남긴다.

    [2026-09-10] 신설. 어제(2026-09-09) 이 결함을 찾아 적어두고 못 닫았던 것이다.

    `optimize_if_due_<ts>.json` 은 이름에 날짜가 있어 `tools/log_cleanup_30d.py` 가
    30일 뒤 지운다. `_last.json` 만 날짜가 없어 살아남는다.
    -> **이력이 지워지고 마지막 한 건만 남는 구조**였다.
    2026-09-09 실측: 타임스탬프본 **0개**, `_last` 만 1개(2026-05-21).

    왜 중요한가: **자동 발동 1회 = HPO 라운드 1회 = 다중검정 시도 1건**이다.
    못 세면 `2_Logs/research_trial_ledger.json` 의 누적 시도 수가 거짓이 되고
    나중에 찾은 것의 유의성이 부풀려진다
    (`project_1data_multiple_testing_correction_broken`: n_trials 가 4 였던 그 결함).

    이 파일은 **이름에 날짜가 없어** 보관 정책에 지워지지 않는다. 그것이 설계다.
    """
    try:
        led = LOG_DIR / "optimize_if_due_ledger.jsonl"
        line = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "as_of": payload.get("as_of"),
            "decision": payload.get("decision"),
            "force": payload.get("force"),
            "reason": payload.get("reason"),
            "status": payload.get("status"),
            "optimize_rc": payload.get("optimize_rc"),
            "stabilizer_rc": payload.get("stabilizer_rc"),
            "stable_backup": payload.get("stable_backup"),
            "rollback": payload.get("rollback"),
        }
        with open(led, "a", encoding="utf-8") as f:
            f.write(json.dumps(line, ensure_ascii=False) + chr(10))
    except Exception as exc:
        print("[FIRING_LEDGER] 기록 실패 (%s: %s)" % (type(exc).__name__, exc))


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
        "oos_source": "summary",
        "summary_asof_ymd": None,
        "stable_asof_ymd": None,
        "stable_oos_n": None,
        "stable_oos_pf": None,
        "source_switched_to_stable": False,
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
    out["summary_asof_ymd"] = _extract_summary_asof_ymd(summary)

    stable = _jread(STABLE)
    stable_score = float(stable.get("best_score") or 0.0)
    out["stable_score"] = stable_score
    out["stable_asof_ymd"] = _normalize_ymd(stable.get("as_of"))
    sw = [w for w in (stable.get("windows") or []) if str((w or {}).get("split") or "").upper() == "OOS"]
    sw = [w for w in sw if int((w or {}).get("n_trades") or 0) > 0]
    if sw:
        sw_sorted = sorted(sw, key=lambda w: str((w or {}).get("end") or ""))
        latest = sw_sorted[-1] if sw_sorted else sw[0]
        out["stable_oos_n"] = int((latest or {}).get("n_trades") or 0)
        try:
            out["stable_oos_pf"] = float((latest or {}).get("pf"))
        except Exception:
            out["stable_oos_pf"] = None

    sum_asof = str(out.get("summary_asof_ymd") or "")
    st_asof = str(out.get("stable_asof_ymd") or "")
    st_oos_n = int(out.get("stable_oos_n") or 0)
    st_oos_pf = out.get("stable_oos_pf")
    if sum_asof and st_asof and sum_asof < st_asof and st_oos_n > 0 and st_oos_pf is not None:
        out["oos_n"] = st_oos_n
        out["oos_pf"] = float(st_oos_pf)
        out["oos_source"] = "stable_windows"
        out["source_switched_to_stable"] = True

    if int(out["oos_n"] or 0) < int(args.min_oos_trades):
        out["reasons"].append(f"oos_trades_low({int(out['oos_n'] or 0)}<{int(args.min_oos_trades)})")
    if float(out["oos_pf"] or 0.0) < float(args.min_oos_pf):
        out["reasons"].append(f"oos_pf_low({float(out['oos_pf'] or 0.0):.4f}<{float(args.min_oos_pf):.4f})")
    if stable_score < float(args.min_stable_score):
        out["reasons"].append(f"stable_score_low({stable_score:.4f}<{float(args.min_stable_score):.4f})")

    out["ok"] = len(out["reasons"]) == 0
    return bool(out["ok"]), out


def _validation_gate(args: argparse.Namespace) -> Tuple[bool, Dict[str, Any]]:
    out: Dict[str, Any] = {
        "checklist_path": str(BACKTEST_CHECKLIST),
        "validation_path": str(BACKTEST_VALIDATION),
        "max_validation_age_days": float(args.max_validation_age_days),
        "min_robust_ratio": float(args.min_robust_ratio),
        "checklist_age_days": None,
        "validation_age_days": None,
        "checklist_passed": None,
        "operation_judgment": None,
        "strategy_parameter_status": None,
        "robust_ratio": None,
        "module_optimizer_ready": None,
        "module_optimizer_blockers": [],
        "ok": False,
        "reasons": [],
    }

    if not BACKTEST_CHECKLIST.exists():
        out["reasons"].append("missing_backtest_validation_checklist")
        return False, out
    if not BACKTEST_VALIDATION.exists():
        out["reasons"].append("missing_backtest_validation")
        return False, out

    c_age = _mtime_days(BACKTEST_CHECKLIST)
    v_age = _mtime_days(BACKTEST_VALIDATION)
    out["checklist_age_days"] = float(c_age)
    out["validation_age_days"] = float(v_age)
    if c_age > float(args.max_validation_age_days):
        out["reasons"].append(f"stale_checklist({c_age:.2f}d>{float(args.max_validation_age_days):.2f}d)")
    if v_age > float(args.max_validation_age_days):
        out["reasons"].append(f"stale_validation({v_age:.2f}d>{float(args.max_validation_age_days):.2f}d)")

    checklist = _jread(BACKTEST_CHECKLIST)
    validation = _jread(BACKTEST_VALIDATION)
    module_progress = checklist.get("module_progress") if isinstance(checklist.get("module_progress"), dict) else {}
    evaluation_scope = checklist.get("evaluation_scope") if isinstance(checklist.get("evaluation_scope"), dict) else {}
    out["evaluation_scope"] = evaluation_scope
    out["allow_early_logic_check_optimize"] = bool(args.allow_early_logic_check_optimize)
    out["readiness_reasons_ignored"] = []

    c_passed = bool(checklist.get("passed", False))
    out["checklist_passed"] = c_passed
    if not c_passed:
        out["reasons"].append("checklist_not_passed")

    op_judgment = str(checklist.get("operation_judgment") or "")
    out["operation_judgment"] = op_judgment
    if op_judgment not in {"운영가능", "조건부운영"}:
        out["reasons"].append(f"operation_judgment_not_ready({op_judgment or '-'})")

    items = checklist.get("items") or []
    spv = next((x for x in items if str(x.get("name")) == "strategy_parameter_validation"), None)
    spv_status = str((spv or {}).get("status") or "")
    out["strategy_parameter_status"] = spv_status or None
    if spv_status != "PASS":
        out["reasons"].append(f"strategy_parameter_validation_not_pass({spv_status or '-'})")

    gates = validation.get("gate_results") or []
    spv_gate = next((g for g in gates if str(g.get("name")) == "strategy_parameter_validation"), None)
    robust_ratio = None
    try:
        robust_ratio = float(((spv_gate or {}).get("details") or {}).get("robust_ratio"))
    except Exception:
        robust_ratio = None
    out["robust_ratio"] = robust_ratio

    if robust_ratio is None:
        out["reasons"].append("robust_ratio_missing")
    elif robust_ratio < float(args.min_robust_ratio):
        out["reasons"].append(f"robust_ratio_low({robust_ratio:.4f}<{float(args.min_robust_ratio):.4f})")

    if module_progress:
        module_optimizer_ready = bool(module_progress.get("optimizer_ready", False))
        module_optimizer_blockers = [str(x) for x in (module_progress.get("optimizer_blockers") or []) if str(x)]
        out["module_optimizer_ready"] = module_optimizer_ready
        out["module_optimizer_blockers"] = module_optimizer_blockers
        if not module_optimizer_ready:
            blockers_txt = ",".join(module_optimizer_blockers) or "-"
            out["reasons"].append(f"module_progress_not_ready({blockers_txt})")

    early_logic_scope = str(evaluation_scope.get("scope") or "") == "paper_early_logic_check"
    strategy_conclusion_allowed = bool(evaluation_scope.get("strategy_conclusion_allowed", True))
    if bool(args.allow_early_logic_check_optimize) and early_logic_scope and not strategy_conclusion_allowed:
        readiness_prefixes = (
            "checklist_not_passed",
            "operation_judgment_not_ready(",
            "module_progress_not_ready(",
        )
        kept_reasons = []
        ignored_reasons = []
        for reason in out["reasons"]:
            if any(str(reason).startswith(prefix) for prefix in readiness_prefixes):
                ignored_reasons.append(reason)
            else:
                kept_reasons.append(reason)
        out["reasons"] = kept_reasons
        out["readiness_reasons_ignored"] = ignored_reasons
        out["early_logic_check_optimize_allowed"] = bool(ignored_reasons)
    else:
        out["early_logic_check_optimize_allowed"] = False

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
        "validation_gate": None,
        "quality_gate": None,
        "stable_backup": None,
        "rollback": None,
        "optimize_rc": 0,
        "stabilizer_rc": 0,
        "status": "START",
    }

    if do_opt:
        if not args.skip_validation_gate:
            v_ok, v_gate = _validation_gate(args)
            out["validation_gate"] = v_gate
            if not v_ok:
                print(f"[ERROR] validation gate failed: {v_gate.get('reasons')}")
                out["status"] = "FAIL_VALIDATION_GATE"
                out["optimize_rc"] = 5
                _write_status(out)
                return 5
        else:
            out["validation_gate"] = {"ok": True, "skipped": True}

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

