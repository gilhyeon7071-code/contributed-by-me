"""Shared quality gate for v41 stable parameter artifacts."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pyarrow.parquet as pq

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

def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _find_krx_parquets(base_dir: Path) -> list[Path]:
    """Mirror optimizer's canonical parquet discovery for provenance hashing."""
    out: list[Path] = []
    seen: set[str] = set()
    for d in (base_dir / "_krx_manual", base_dir / "krx_daily_archive", base_dir):
        if not d.exists() or not d.is_dir():
            continue
        for p in d.glob("krx_daily_*_clean.parquet"):
            key = str(p.resolve())
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    if out:
        return sorted(out)
    for d in (base_dir / "_krx_manual", base_dir / "krx_daily_archive", base_dir):
        if not d.exists() or not d.is_dir():
            continue
        for p in d.glob("krx_daily_*.parquet"):
            key = str(p.resolve())
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    return sorted(out)


def _compute_data_source_hash(base_dir: Path) -> str:
    """Content-aware hash of the canonical parquet source set.

    Uses file size and parquet metadata (row count, row groups, schema) instead
    of mtime so that backups, copies, or normal nightly updates with identical
    content do not invalidate the provenance stamp.
    """
    files = _find_krx_parquets(base_dir)
    h = hashlib.sha256()
    for p in files:
        try:
            st = p.stat()
            meta = pq.read_metadata(p)
            token = (
                f"{p.resolve()}|"
                f"size={st.st_size}|"
                f"rows={meta.num_rows}|"
                f"row_groups={meta.num_row_groups}|"
                f"schema={meta.schema.to_string()}\n"
            )
        except Exception:
            token = f"{p.resolve()}|ERROR\n"
        h.update(token.encode("utf-8"))
    return h.hexdigest()


def compute_provenance_metadata(base_dir: Path, config_path: Path) -> dict:
    """Capture code/config/data fingerprints at stable-generation time."""
    this_file = Path(__file__).resolve()
    optimize_params = (base_dir / "optimize_params_v41_1.py").resolve()
    gen_candidates = (base_dir / "generate_candidates_v41_1.py").resolve()
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "code_hashes": {
            "utils/stable_params_gate.py": _sha256_file(this_file),
            "optimize_params_v41_1.py": _sha256_file(optimize_params) if optimize_params.exists() else "",
            "generate_candidates_v41_1.py": _sha256_file(gen_candidates) if gen_candidates.exists() else "",
        },
        "exec_policy_hash": _sha256_file(config_path) if config_path.exists() else "",
        "data_source_hash": _compute_data_source_hash(base_dir),
    }


def _check_provenance(stable: Dict[str, Any], base_dir: Path, config_path: Path) -> list[str]:
    """Return mismatch reasons between stored provenance and current environment."""
    stored = stable.get("meta", {}).get("provenance")
    if not isinstance(stored, dict):
        return ["provenance_missing"]

    current = compute_provenance_metadata(base_dir, config_path)
    reasons: list[str] = []

    stored_code = stored.get("code_hashes", {})
    current_code = current.get("code_hashes", {})
    for key in set(stored_code) | set(current_code):
        if stored_code.get(key) != current_code.get(key):
            reasons.append(f"provenance_code_hash_mismatch:{key}")

    if stored.get("exec_policy_hash") != current.get("exec_policy_hash"):
        reasons.append("provenance_exec_policy_hash_mismatch")

    if stored.get("data_source_hash") != current.get("data_source_hash"):
        reasons.append("provenance_data_source_hash_mismatch")

    return reasons


def load_stable_quality_gate(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"missing: {config_path}")
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    gate = cfg.get("stable_params_quality_gate")
    if not isinstance(gate, dict):
        raise RuntimeError("missing stable_params_quality_gate")
    gate["_config_path"] = str(config_path)
    gate["_base_dir"] = str(config_path.parent.parent)
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
    # [2026-08-15] Per-fold OOS tail-risk floor. This used to live upstream, in
    # optimize_params_v41_1.py::_fold_selection_metrics()'s worst-fold hurdle.
    # Once OOS folds were reserved as a true holdout they left the HPO objective
    # entirely, so nothing checked an individual OOS fold any more -- only the
    # n_trades-weighted OOS average, where a good fold can mask a bad one.
    # Measured 2026-08-15 against the live thresholds: the 2025-08~2026-08 fold
    # could fall to pf 0.6838 and still pass. This restores the floor here.
    # `min_oos_worst_fold_trades` mirrors the optimizer's MIN_TRADES_PER_WINDOW
    # so that thin folds are skipped rather than hard-failing the gate on their
    # pf=0.0 sentinel -- the upstream hurdle excluded them the same way.
    min_oos_worst_fold_pf = float(_gate_value(gate, "min_oos_worst_fold_pf", 0.75))
    min_oos_worst_fold_trades = int(_gate_value(gate, "min_oos_worst_fold_trades", 15))
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
    oos_worst_pf = None
    oos_worst_window = None
    oos_scored_folds = 0
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
        if n_trades >= min_oos_worst_fold_trades:
            oos_scored_folds += 1
            if oos_worst_pf is None or pf < oos_worst_pf:
                oos_worst_pf = pf
                oos_worst_window = {
                    "start": str(row.get("start", "")),
                    "end": str(row.get("end", "")),
                    "n_trades": n_trades,
                    "pf": pf,
                }

    oos_pf_weighted = oos_pf_weighted_num / oos_pf_weighted_den if oos_pf_weighted_den else 0.0
    mean_pf_weighted = all_pf_weighted_num / all_pf_weighted_den if all_pf_weighted_den else 0.0
    reasons = []

    # Provenance check: detect stale artifacts caused by code/config/data drift.
    # Advisory only — does not block certification.
    provenance_warnings: list[str] = []
    _cfg_path_str = gate.get("_config_path")
    _base_dir_str = gate.get("_base_dir")
    if _cfg_path_str and _base_dir_str:
        provenance_warnings = _check_provenance(
            stable,
            Path(_base_dir_str),
            Path(_cfg_path_str),
        )

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
    # Per-fold OOS floor. Fail-closed: if the OOS evidence exists but no fold is
    # thick enough to score, the weighted average alone is not enough to certify.
    if oos_pf_weighted_den > 0:
        if oos_worst_pf is None:
            reasons.append(
                f"oos_worst_fold_missing(no OOS fold with n_trades>={min_oos_worst_fold_trades})"
            )
        elif oos_worst_pf < min_oos_worst_fold_pf:
            reasons.append(
                f"oos_worst_pf_low({oos_worst_pf:.4f}<{min_oos_worst_fold_pf:.4f})"
            )
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
        warnings=provenance_warnings,
        threshold_map={
            "not_promoted": (None, None),
            "stable_score_low": (min_stable_score, best_score),
            "oos_trades_low": (min_oos_trades, oos_n_total),
            "oos_trades_missing": (None, None),
            "oos_pf_low": (min_oos_pf, oos_pf_weighted),
            "oos_worst_pf_low": (min_oos_worst_fold_pf, oos_worst_pf),
            "oos_worst_fold_missing": (min_oos_worst_fold_trades, oos_scored_folds),
            "mean_pf_missing": (None, None),
            "mean_pf_low": (min_mean_pf, mean_pf_weighted),
            "malformed_window_rows": (None, None),
            "provenance_missing": (None, None),
            "provenance_code_hash_mismatch": (None, None),
            "provenance_exec_policy_hash_mismatch": (None, None),
            "provenance_data_source_hash_mismatch": (None, None),
        },
    )

    # Provenance mismatches are advisory: they warn about stale artifacts but do
    # not block certification, so that legacy stable artifacts keep working while
    # new ones are stamped automatically.
    critical_reasons = [r for r in reasons if not r.startswith("provenance_")]

    return {
        "ok": not critical_reasons,
        "reason": ";".join(critical_reasons) if critical_reasons else "ok",
        "reasons": critical_reasons,
        "warnings": provenance_warnings,
        "promoted": promoted,
        "best_score": best_score,
        "oos_n_total": oos_n_total,
        "oos_pf_weighted": oos_pf_weighted,
        "oos_worst_pf": oos_worst_pf,
        "oos_worst_window": oos_worst_window,
        "oos_scored_folds": oos_scored_folds,
        "mean_pf_weighted": mean_pf_weighted,
        "all_n_total": all_n_total,
        "malformed_window_rows": malformed_rows,
        "thresholds": {
            "require_promoted": require_promoted,
            "min_oos_trades": min_oos_trades,
            "min_oos_pf": min_oos_pf,
            "min_stable_score": min_stable_score,
            "min_mean_pf": min_mean_pf,
            "min_oos_worst_fold_pf": min_oos_worst_fold_pf,
            "min_oos_worst_fold_trades": min_oos_worst_fold_trades,
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
    warnings: list[str],
    threshold_map: Dict[str, tuple[Any, Any]],
) -> None:
    if not reasons and not warnings:
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
    for warning in warnings:
        code = _reason_code(warning)
        threshold, actual = threshold_map.get(code, (None, None))
        log_gate_event(
            gate_layer="stable",
            gate_name="stable_params_quality_gate",
            decision="WARN",
            reason_code=code,
            reason_detail=warning,
            source_file="utils/stable_params_gate.py",
            source_key="evaluate_stable_params",
            threshold=threshold,
            actual_value=actual,
            recoverable=False,
        )