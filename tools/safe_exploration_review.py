from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

DEFAULT_CANDIDATE = LOG_DIR / "auto_signal_tune_candidate_latest.json"
DEFAULT_CALIBRATION = LOG_DIR / "calibration_stream_latest.json"
DEFAULT_ARL = LOG_DIR / "arl_auto_intraday_validation_latest.json"
DEFAULT_PENDING = LOG_DIR / "pending_entry_status_latest.json"
DEFAULT_OUTPUT = LOG_DIR / "safe_exploration_review_latest.json"


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
        except Exception:
            return {}
    return {}


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return int(default)
        return int(float(v))
    except Exception:
        return int(default)


def _nested(obj: Dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    cur: Any = obj
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _candidate_step_pressure(candidate: Dict[str, Any]) -> Tuple[float, List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    pressures: List[float] = []
    for row in candidate.get("signals") or []:
        if not isinstance(row, dict):
            continue
        if "current" not in row or "proposed" not in row:
            continue
        current = _to_float(row.get("current"))
        proposed = _to_float(row.get("proposed"))
        denom = max(abs(current), 1.0)
        pressure = min(1.0, abs(proposed - current) / denom)
        pressures.append(pressure)
        rows.append(
            {
                "signal": str(row.get("signal") or ""),
                "current": current,
                "proposed": proposed,
                "step_pressure": round(pressure, 6),
                "reason": str(row.get("reason") or ""),
            }
        )
    return (max(pressures) if pressures else 0.0), rows


def build_review(
    candidate: Dict[str, Any],
    calibration: Dict[str, Any],
    arl: Dict[str, Any],
    pending: Dict[str, Any],
    *,
    beta: float,
    gamma: float,
    eta0: float,
    min_k: int,
    max_safe_pct: float,
    safe_threshold: float,
    bandit_dual_limit: float,
) -> Dict[str, Any]:
    calib_rows = _to_int(_nested(calibration, ("metrics", "rows"), 0), 0)
    arl_rows = _to_int(_nested(arl, ("history", "rows"), 0), 0)
    k = max(calib_rows, arl_rows)
    t = max(1, k)
    eta_t = float(eta0) / (float(t) ** float(gamma))

    step_pressure, signal_rows = _candidate_step_pressure(candidate)
    candidate_has_patch = bool(candidate.get("patch"))
    calibration_status = str(calibration.get("status") or "UNKNOWN").upper()
    arl_status = str(arl.get("status") or "UNKNOWN").upper()
    entry_decision = str(_nested(candidate, ("context", "entry_decision"), "") or "").upper()
    pending_max_new = _to_int(_nested(candidate, ("context", "pending_max_new"), pending.get("max_new")), 0)

    violation_components: Dict[str, float] = {
        "calibration_fail": 1.0 if calibration_status == "FAIL" else 0.0,
        "arl_fail": 1.0 if arl_status == "FAIL" else 0.0,
        "current_gate_block": 1.0 if entry_decision == "BLOCK" or pending_max_new <= 0 else 0.0,
        "sample_deficit": max(0.0, (float(min_k) - float(k)) / max(1.0, float(min_k))),
        "step_pressure": float(step_pressure),
    }
    bandit_dual = sum(violation_components.values())

    reward_proxy = 0.15 if candidate_has_patch else 0.0
    uncertainty = 1.0 / math.sqrt(max(1.0, float(k)))
    risk_penalty = 0.25 * violation_components["current_gate_block"] + 0.10 * step_pressure
    safe_bo_lower = reward_proxy - (float(beta) * uncertainty) - risk_penalty

    reasons: List[str] = []
    if k < int(min_k):
        reasons.append(f"K_BELOW_MIN({k}<{int(min_k)})")
    if safe_bo_lower < float(safe_threshold):
        reasons.append(f"SAFE_BO_LOWER_LOW({safe_bo_lower:.6f}<{float(safe_threshold):.6f})")
    if bandit_dual > float(bandit_dual_limit):
        reasons.append(f"BANDIT_DUAL_HIGH({bandit_dual:.6f}>{float(bandit_dual_limit):.6f})")
    if entry_decision == "BLOCK" or pending_max_new <= 0:
        reasons.append("CURRENT_GATE_NOT_OPEN")
    if calibration_status == "FAIL":
        reasons.append("CALIBRATION_FAIL")
    if arl_status == "FAIL":
        reasons.append("ARL_FAIL")

    safe_pass = not reasons
    live_pct = 0.0
    traffic_action = "HOLD_APPROVAL"
    if safe_pass:
        traffic_action = "ELIGIBLE_SHADOW_REVIEW_ONLY"

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "SAFE_PASS" if safe_pass else "SAFE_FAIL",
        "apply_status": "WAITING_APPROVAL",
        "orders_modified": False,
        "fills_modified": False,
        "ledger_modified": False,
        "stats_modified": False,
        "gate_modified": False,
        "risk_lock_modified": False,
        "runtime_config_modified": False,
        "model": {
            "method": "ADVISORY_SAFE_LOWER_BOUND_PROXY",
            "note": "No live apply. Lower bound is a conservative advisory proxy until a GP/SafeOpt model is wired.",
            "beta": float(beta),
            "gamma": float(gamma),
            "eta0": float(eta0),
            "eta_t": round(eta_t, 8),
        },
        "policy": {
            "min_k": int(min_k),
            "max_safe_pct": float(max_safe_pct),
            "safe_threshold": float(safe_threshold),
            "bandit_dual_limit": float(bandit_dual_limit),
            "promotion_gate": "safe_bo_lower >= safe_threshold AND bandit_dual <= limit AND K >= min_k AND live_pct <= max_safe_pct",
        },
        "evidence": {
            "K": int(k),
            "calibration_rows": int(calib_rows),
            "arl_rows": int(arl_rows),
            "calibration_status": calibration_status,
            "arl_status": arl_status,
            "entry_decision": entry_decision or "UNKNOWN",
            "pending_max_new": int(pending_max_new),
            "candidate_has_patch": candidate_has_patch,
            "candidate_patch_keys": sorted([str(k) for k in (candidate.get("patch") or {}).keys()]),
            "signals": signal_rows,
        },
        "safe_bo_lower": round(float(safe_bo_lower), 8),
        "bandit_dual": round(float(bandit_dual), 8),
        "bandit_dual_components": {k: round(v, 8) for k, v in violation_components.items()},
        "live_pct": live_pct,
        "traffic_action": traffic_action,
        "traffic_cap_ok": live_pct <= float(max_safe_pct),
        "blocked_for_auto_apply": [
            "User requested implementation verification only; apply is waiting for explicit approval.",
            "This review writes evidence JSON/text only and does not modify trading state.",
        ],
        "reasons": reasons,
    }


def _summary_lines(review: Dict[str, Any]) -> List[str]:
    evidence = review.get("evidence") if isinstance(review.get("evidence"), dict) else {}
    model = review.get("model") if isinstance(review.get("model"), dict) else {}
    policy = review.get("policy") if isinstance(review.get("policy"), dict) else {}
    return [
        f"[FINAL] {review.get('status')} apply_status={review.get('apply_status')}",
        f"K={evidence.get('K')} min_k={policy.get('min_k')} live_pct={review.get('live_pct')} max_safe_pct={policy.get('max_safe_pct')}",
        f"safe_bo_lower={review.get('safe_bo_lower')} safe_threshold={policy.get('safe_threshold')} beta={model.get('beta')}",
        f"bandit_dual={review.get('bandit_dual')} dual_limit={policy.get('bandit_dual_limit')}",
        f"eta_t={model.get('eta_t')} gamma={model.get('gamma')}",
        f"calibration_status={evidence.get('calibration_status')} calibration_rows={evidence.get('calibration_rows')}",
        f"arl_status={evidence.get('arl_status')} arl_rows={evidence.get('arl_rows')}",
        f"entry_decision={evidence.get('entry_decision')} pending_max_new={evidence.get('pending_max_new')}",
        f"candidate_patch_keys={','.join(evidence.get('candidate_patch_keys') or []) or '-'}",
        f"reasons={','.join(review.get('reasons') or []) or '-'}",
    ]


def write_review(review: Dict[str, Any], output_json: Path) -> Dict[str, str]:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    ts = _now_ts()
    dated_json = output_json.with_name(f"safe_exploration_review_{ts}.json")
    txt = json.dumps(review, ensure_ascii=False, indent=2) + "\n"
    output_json.write_text(txt, encoding="utf-8")
    dated_json.write_text(txt, encoding="utf-8")

    summary = "\n".join(_summary_lines(review)) + "\n"
    latest_txt = output_json.with_suffix(".txt")
    dated_txt = output_json.with_name(f"safe_exploration_review_{ts}.txt")
    latest_txt.write_text(summary, encoding="utf-8")
    dated_txt.write_text(summary, encoding="utf-8")
    return {
        "latest_json": str(output_json),
        "dated_json": str(dated_json),
        "latest_txt": str(latest_txt),
        "dated_txt": str(dated_txt),
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Review safe exploration gates without applying changes.")
    p.add_argument("--candidate", default=str(DEFAULT_CANDIDATE))
    p.add_argument("--calibration", default=str(DEFAULT_CALIBRATION))
    p.add_argument("--arl", default=str(DEFAULT_ARL))
    p.add_argument("--pending", default=str(DEFAULT_PENDING))
    p.add_argument("--output-json", default=str(DEFAULT_OUTPUT))
    p.add_argument("--beta", type=float, default=3.0)
    p.add_argument("--gamma", type=float, default=0.6)
    p.add_argument("--eta0", type=float, default=1.0)
    p.add_argument("--min-k", type=int, default=50)
    p.add_argument("--max-safe-pct", type=float, default=0.10)
    p.add_argument("--safe-threshold", type=float, default=0.0)
    p.add_argument("--bandit-dual-limit", type=float, default=1.0)
    return p


def main() -> int:
    args = build_parser().parse_args()
    review = build_review(
        _read_json(Path(args.candidate)),
        _read_json(Path(args.calibration)),
        _read_json(Path(args.arl)),
        _read_json(Path(args.pending)),
        beta=float(args.beta),
        gamma=float(args.gamma),
        eta0=float(args.eta0),
        min_k=int(args.min_k),
        max_safe_pct=float(args.max_safe_pct),
        safe_threshold=float(args.safe_threshold),
        bandit_dual_limit=float(args.bandit_dual_limit),
    )
    paths = write_review(review, Path(args.output_json))
    for line in _summary_lines(review):
        print(line)
    print(f"[SAFE_EXPLORATION] latest_json={paths['latest_json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
