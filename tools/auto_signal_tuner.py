from __future__ import annotations

import argparse
import glob
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple
import logging


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"
CFG_PATH = PAPER_DIR / "paper_engine_config.json"
POLICY_PATH = PAPER_DIR / "signal_autotune_policy.json"
LOCK_TOOL = ROOT / "tools" / "paper_engine_config_lock.py"
SNAPSHOT_TOOL = ROOT / "tools" / "build_integrated_ops_snapshot.py"
CANDIDATE_LATEST = LOGS / "auto_signal_tune_candidate_latest.json"




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _get_nested(obj: Dict[str, Any], dotted: str, default: Any = None) -> Any:
    cur: Any = obj
    for k in dotted.split("."):
        if isinstance(cur, list):
            if not k.isdigit():
                return default
            idx = int(k)
            if idx < 0 or idx >= len(cur):
                return default
            cur = cur[idx]
            continue
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _set_nested(obj: Dict[str, Any], dotted: str, value: Any) -> None:
    cur: Any = obj
    keys = dotted.split(".")
    for k in keys[:-1]:
        if k not in cur or not isinstance(cur[k], dict):
            cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = value


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(v)))


def _to_float(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(d)


def _latest_after_close() -> Dict[str, Any]:
    files = sorted(glob.glob(str(LOGS / "after_close_summary_*.json")))
    return _load_json(Path(files[-1])) if files else {}


def _read_run_log() -> Dict[str, Any]:
    p = LOGS / "run_paper_daily_last.txt"
    if not p.exists():
        return {}
    txt = p.read_text(encoding="utf-8", errors="replace")
    m = re.search(r"\[ENTRY_GATE\] decision=([A-Z_]+).*?engine=([A-Z_]+).*?fx=([A-Z_]+)", txt)
    return {
        "batch_finished": "[OK] finished" in txt,
        "entry_decision": m.group(1) if m else None,
        "engine_regime": m.group(2) if m else None,
        "fx_status": m.group(3) if m else None,
    }


def build_candidate() -> Dict[str, Any]:
    policy = _load_json(POLICY_PATH) if POLICY_PATH.exists() else {}
    cfg = _load_json(CFG_PATH)
    macro = _load_json(LOGS / "macro_signal_latest.json") if (LOGS / "macro_signal_latest.json").exists() else {}
    pending = _load_json(LOGS / "pending_entry_status_latest.json") if (LOGS / "pending_entry_status_latest.json").exists() else {}
    meta = _load_json(LOGS / "candidates_latest_meta.json") if (LOGS / "candidates_latest_meta.json").exists() else {}
    run = _read_run_log()
    after_close = _latest_after_close()

    source = str(macro.get("source") or _get_nested(macro, "sources.macro", "")).strip()
    ret1 = _to_float(_get_nested(macro, "market_metrics.ret1", 0.0), 0.0)
    all_pass = int(_get_nested(meta, "attempts.0.diag.all_pass", 0) or 0)

    patch: Dict[str, Any] = {}
    signal_rows = []

    # regime signal
    sig_regime = _get_nested(policy, "signals.regime", {})
    if bool(sig_regime.get("enabled", False)):
        key = str(_get_nested(sig_regime, "keys.rally_day_ret_min_proxy", "regime_entry_policy.rally_day_ret_min_proxy"))
        cur = _to_float(_get_nested(cfg, key, 0.02), 0.02)
        lo = _to_float(_get_nested(sig_regime, "bounds.rally_day_ret_min_proxy.min", 0.015), 0.015)
        hi = _to_float(_get_nested(sig_regime, "bounds.rally_day_ret_min_proxy.max", 0.03), 0.03)
        step = _to_float(_get_nested(sig_regime, "bounds.rally_day_ret_min_proxy.step", 0.0025), 0.0025)
        proxy_sources = set(_get_nested(sig_regime, "trigger.proxy_sources", []))
        high_ret = _to_float(_get_nested(sig_regime, "trigger.high_ret_threshold", 0.02), 0.02)
        proposed = cur
        reason = "no_change"
        if source in proxy_sources and ret1 >= high_ret:
            proposed = _clamp(cur - step, lo, hi)
            reason = f"proxy_high_ret({ret1:.4f})"
        if abs(proposed - cur) > 1e-12:
            _set_nested(patch, key, round(proposed, 6))
        signal_rows.append({"signal": "regime", "key": key, "current": cur, "proposed": proposed, "reason": reason})

    # fx signal
    sig_fx = _get_nested(policy, "signals.fx", {})
    if bool(sig_fx.get("enabled", False)):
        key = str(_get_nested(sig_fx, "keys.daily_abs_change_block_level", "fx_entry_policy.daily_abs_change_block_level"))
        cur = _to_float(_get_nested(cfg, key, 18.0), 18.0)
        lo = _to_float(_get_nested(sig_fx, "bounds.daily_abs_change_block_level.min", 15.0), 15.0)
        hi = _to_float(_get_nested(sig_fx, "bounds.daily_abs_change_block_level.max", 22.0), 22.0)
        step = _to_float(_get_nested(sig_fx, "bounds.daily_abs_change_block_level.step", 1.0), 1.0)
        decision = str(run.get("entry_decision") or "").upper()
        proposed = cur
        reason = "no_change"
        if ret1 >= _to_float(_get_nested(sig_fx, "trigger.ret_threshold", 0.02), 0.02):
            if decision in {"REDUCE", "CAUTION"} and bool(_get_nested(sig_fx, "trigger.only_if_decision_reduce_or_caution", True)):
                proposed = _clamp(cur + step, lo, hi)
                reason = f"ret_high_and_decision_{decision}"
        if abs(proposed - cur) > 1e-12:
            _set_nested(patch, key, round(proposed, 6))
        signal_rows.append({"signal": "fx", "key": key, "current": cur, "proposed": proposed, "reason": reason})

    # entry pool signal
    sig_entry = _get_nested(policy, "signals.entry_pool", {})
    if bool(sig_entry.get("enabled", False)):
        key = str(_get_nested(sig_entry, "keys.union_entry_strength_min", "union_entry_strength_min"))
        cur = _to_float(_get_nested(cfg, key, 0.65), 0.65)
        lo = _to_float(_get_nested(sig_entry, "bounds.union_entry_strength_min.min", 0.55), 0.55)
        hi = _to_float(_get_nested(sig_entry, "bounds.union_entry_strength_min.max", 0.75), 0.75)
        step = _to_float(_get_nested(sig_entry, "bounds.union_entry_strength_min.step", 0.02), 0.02)
        all_pass_low = int(_get_nested(sig_entry, "trigger.all_pass_low", 3))
        proposed = cur
        reason = "no_change"
        if all_pass <= all_pass_low:
            proposed = _clamp(cur - step, lo, hi)
            reason = f"all_pass_low({all_pass})"
        if abs(proposed - cur) > 1e-12:
            _set_nested(patch, key, round(proposed, 6))
        signal_rows.append({"signal": "entry_pool", "key": key, "current": cur, "proposed": proposed, "reason": reason})

    # observe-only signals
    signal_rows.append({"signal": "sector", "mode": "observe_only", "reason": "policy_observe_only"})
    signal_rows.append({"signal": "news", "mode": "observe_only", "reason": "policy_observe_only"})

    execs = (after_close.get("executions") or {}) if isinstance(after_close, dict) else {}
    candidate = {
        "generated_at": _now(),
        "policy_path": str(POLICY_PATH),
        "context": {
            "macro_source": source,
            "macro_ret1": ret1,
            "entry_decision": run.get("entry_decision"),
            "engine_regime": run.get("engine_regime"),
            "fx_status": run.get("fx_status"),
            "all_pass": all_pass,
            "pending_max_new": pending.get("max_new"),
            "pending_entry_ready": pending.get("entry_ready"),
            "today_buy_count": execs.get("buy_count"),
        },
        "signals": signal_rows,
        "patch": patch,
        "apply_recommended": bool(patch),
    }
    return candidate


def _run(cmd: list[str]) -> Tuple[int, str, str]:
    cp = subprocess.run(cmd, capture_output=True, text=True)
    return int(cp.returncode), cp.stdout or "", cp.stderr or ""


def _validate(policy: Dict[str, Any]) -> Dict[str, Any]:
    rc, out, err = _run([sys.executable, str(SNAPSHOT_TOOL)])
    snapshot_ok = (rc == 0)
    snap = _load_json(LOGS / "integrated_ops_snapshot_latest.json") if (LOGS / "integrated_ops_snapshot_latest.json").exists() else {}
    issues = sum(1 for r in (snap.get("calc_issue_rows") or []) if str(r.get("calc_issue_state")) == "ISSUE")
    run_state = _read_run_log()
    pending = _load_json(LOGS / "pending_entry_status_latest.json") if (LOGS / "pending_entry_status_latest.json").exists() else {}
    max_new = int(pending.get("max_new", 0) or 0)
    batch_finished = bool(run_state.get("batch_finished"))

    max_issue_count = int(_get_nested(policy, "validation.max_issue_count", 0))
    require_batch_finished = bool(_get_nested(policy, "validation.require_batch_finished", True))
    require_max_new = int(_get_nested(policy, "validation.require_max_new_at_least", 1))

    ok = snapshot_ok and (issues <= max_issue_count) and (max_new >= require_max_new)
    if require_batch_finished:
        ok = ok and batch_finished

    return {
        "ok": ok,
        "snapshot_rc": rc,
        "issue_count": issues,
        "max_new": max_new,
        "batch_finished": batch_finished,
        "snapshot_stdout": out.strip(),
        "snapshot_stderr": err.strip(),
    }


def _rollback_from_change_log(change_log_path: Path) -> Tuple[bool, str]:
    if not change_log_path.exists():
        return False, f"missing change log: {change_log_path}"
    log_obj = _load_json(change_log_path)
    before_cfg = log_obj.get("before_config")
    if not isinstance(before_cfg, dict):
        return False, "before_config missing in change log"
    tmp = LOGS / f"auto_signal_tune_rollback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    _save_json(tmp, before_cfg)
    rc, out, err = _run([sys.executable, str(LOCK_TOOL), "set", "--patch-file", str(tmp)])
    return rc == 0, (out + "\n" + err).strip()


def apply_candidate(candidate_path: Path) -> int:
    if not candidate_path.exists():
        _log_print(f"[FAIL] candidate missing: {candidate_path}")
        return 2
    candidate = _load_json(candidate_path)
    patch = candidate.get("patch") or {}
    if not isinstance(patch, dict) or not patch:
        _log_print("[SKIP] empty patch")
        return 0

    policy = _load_json(POLICY_PATH)
    if not bool(policy.get("enabled", False)) or not bool(policy.get("apply_enabled", False)):
        _log_print("[FAIL] apply blocked by policy (enabled/apply_enabled)")
        return 3

    tmp_patch = LOGS / f"auto_signal_tune_patch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    _save_json(tmp_patch, patch)

    before_logs = set(glob.glob(str(LOGS / "paper_engine_config.change_*.json")))
    rc, out, err = _run([sys.executable, str(LOCK_TOOL), "set", "--patch-file", str(tmp_patch)])
    _log_print(out.strip())
    if err.strip():
        _log_print(err.strip())
    if rc != 0:
        _log_print(f"[FAIL] lock apply rc={rc}")
        return 4

    after_logs = sorted(set(glob.glob(str(LOGS / "paper_engine_config.change_*.json"))) - before_logs)
    change_log = Path(after_logs[-1]) if after_logs else None

    val = _validate(policy)
    _log_print(f"[VALIDATION] ok={val['ok']} issues={val['issue_count']} max_new={val['max_new']} batch_finished={val['batch_finished']}")

    if val["ok"]:
        return 0

    if change_log is not None:
        ok, msg = _rollback_from_change_log(change_log)
        _log_print(f"[ROLLBACK] ok={ok}")
        if msg:
            _log_print(msg)
    return 5


def main() -> int:
    ap = argparse.ArgumentParser(description="Auto tune signal thresholds with gated apply/rollback.")
    ap.add_argument("--mode", choices=["propose", "apply"], default="propose")
    ap.add_argument("--candidate", default=str(CANDIDATE_LATEST))
    args = ap.parse_args()

    if args.mode == "propose":
        cand = build_candidate()
        ts_path = LOGS / f"auto_signal_tune_candidate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        _save_json(ts_path, cand)
        _save_json(CANDIDATE_LATEST, cand)
        _log_print(f"[OK] wrote {ts_path}")
        _log_print(f"[OK] wrote {CANDIDATE_LATEST}")
        _log_print(f"[INFO] apply_recommended={cand.get('apply_recommended')}")
        return 0

    return apply_candidate(Path(args.candidate))


if __name__ == "__main__":
    raise SystemExit(main())

