"""Shared quality gate for v41 stable parameter artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from utils.gate_audit import log_gate_event

def _gate_value(gate: Dict[str, Any], key: str, default: Any) -> Any:
    val = gate.get(key, default)
    if val is None:
        return default
    if isinstance(val, str) and val.strip() == "":
        return default
    return val



def _row_metric_value(row: Dict[str, Any], key: str, default: Any) -> Any:
    val = row.get(key, default)
    if val is None:
        return default
    if isinstance(val, str) and val.strip() == "":
        return default
    return val

def load_stable_quality_gate(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"missing: {config_path}")
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    gate = cfg.get("stable_params_quality_gate")
    if not isinstance(gate, dict):
        raise RuntimeError("missing stable_params_quality_gate")
    return gate


def evaluate_stable_params(stable: Dict[str, Any], gate: Dict[str, Any]) -> Dict[str, Any]:
    require_promoted = bool(gate.get("require_promoted", True))
    min_oos_trades = int(_gate_value(gate, "min_oos_trades", 20))
    min_oos_pf = float(_gate_value(gate, "min_oos_pf", 0.75))
    min_stable_score = float(_gate_value(gate, "min_stable_score", 0.0))
    # [2026-07-27] Profitability condition. The gate previously checked a tail-risk
    # floor (worst-fold, upstream) and a sub-breakeven OOS bar, so a configuration
    # losing money in every fold could still be certified. `min_mean_pf` closes that
    # hole. Absent from config -> not enforced (backward compatible).
    _mmp_raw = gate.get("min_mean_pf")
    min_mean_pf = None if _mmp_raw is None else float(_mmp_raw)
    promoted = bool(stable.get("promoted", False))
    try:
        best_score = float(stable.get("best_score"))
    except Exception:
        best_score = float("-inf")

    windows = stable.get("windows") if isinstance(stable.get("windows"), list) else []
    oos_n_total = 0
    oos_pf_weighted_num = 0.0
    oos_pf_weighted_den = 0
    all_n_total = 0
    all_pf_weighted_num = 0.0
    all_pf_weighted_den = 0
    malformed_rows = 0
    for row in windows:
        if not isinstance(row, dict):
            malformed_rows += 1
            continue
        try:
            n_trades = int(_row_metric_value(row, "n_trades", 0))
            pf = float(_row_metric_value(row, "pf", 0.0))
        except Exception:
            malformed_rows += 1
            continue
        if n_trades <= 0:
            continue
        # every split contributes to the overall profitability measure
        all_n_total += n_trades
        all_pf_weighted_num += pf * n_trades
        all_pf_weighted_den += n_trades
        if str(row.get("split", "")).upper() != "OOS":
            continue
        oos_n_total += n_trades
        oos_pf_weighted_num += pf * n_trades
        oos_pf_weighted_den += n_trades

    oos_pf_weighted = oos_pf_weighted_num / oos_pf_weighted_den if oos_pf_weighted_den else 0.0
    mean_pf_weighted = all_pf_weighted_num / all_pf_weighted_den if all_pf_weighted_den else 0.0
    reasons = []
    if require_promoted and not promoted:
        reasons.append("not_promoted")
    if best_score < min_stable_score:
        reasons.append(f"stable_score_low({best_score:.4f}<{min_stable_score:.4f})")
    if oos_n_total < min_oos_trades:
        reasons.append(f"oos_trades_low({oos_n_total}<{min_oos_trades})")
    if oos_pf_weighted_den <= 0:
        reasons.append("oos_trades_missing")
    elif oos_pf_weighted < min_oos_pf:
        reasons.append(f"oos_pf_low({oos_pf_weighted:.4f}<{min_oos_pf:.4f})")
    if min_mean_pf is not None:
        if all_pf_weighted_den <= 0:
            reasons.append("mean_pf_missing")
        elif mean_pf_weighted < min_mean_pf:
            reasons.append(f"mean_pf_low({mean_pf_weighted:.4f}<{min_mean_pf:.4f})")
    # malformed rows must not silently shrink the evidence base (fail-closed)
    if malformed_rows > 0:
        reasons.append(f"malformed_window_rows({malformed_rows})")

    _log_stable_gate(
        reasons=reasons,
        threshold_map={
            "not_promoted": (None, None),
            "stable_score_low": (min_stable_score, best_score),
            "oos_trades_low": (min_oos_trades, oos_n_total),
            "oos_trades_missing": (None, None),
            "oos_pf_low": (min_oos_pf, oos_pf_weighted),
            "mean_pf_missing": (None, None),
            "mean_pf_low": (min_mean_pf, mean_pf_weighted),
            "malformed_window_rows": (None, None),
        },
    )

    return {
        "ok": not reasons,
        "reason": ";".join(reasons) if reasons else "ok",
        "reasons": reasons,
        "promoted": promoted,
        "best_score": best_score,
        "oos_n_total": oos_n_total,
        "oos_pf_weighted": oos_pf_weighted,
        "mean_pf_weighted": mean_pf_weighted,
        "all_n_total": all_n_total,
        "malformed_window_rows": malformed_rows,
        "thresholds": {
            "require_promoted": require_promoted,
            "min_oos_trades": min_oos_trades,
            "min_oos_pf": min_oos_pf,
            "min_stable_score": min_stable_score,
            "min_mean_pf": min_mean_pf,
        },
    }


def _reason_code(reason: str) -> str:
    s = str(reason).split(":", 1)[0].strip().lower()
    # strip trailing parenthetical values so code stays stable
    if "(" in s:
        s = s.split("(", 1)[0].strip()
    return s


def _log_stable_gate(
    reasons: list[str],
    threshold_map: Dict[str, tuple[Any, Any]],
) -> None:
    if not reasons:
        log_gate_event(
            gate_layer="stable",
            gate_name="stable_params_quality_gate",
            decision="PASS",
            source_file="utils/stable_params_gate.py",
            source_key="evaluate_stable_params",
            recoverable=False,
        )
        return
    for reason in reasons:
        code = _reason_code(reason)
        threshold, actual = threshold_map.get(code, (None, None))
        log_gate_event(
            gate_layer="stable",
            gate_name="stable_params_quality_gate",
            decision="BLOCK",
            reason_code=code,
            reason_detail=reason,
            source_file="utils/stable_params_gate.py",
            source_key="evaluate_stable_params",
            threshold=threshold,
            actual_value=actual,
            recoverable=True,
            recovery_hint="HPO 또는 gate 임계값 조정",
        )