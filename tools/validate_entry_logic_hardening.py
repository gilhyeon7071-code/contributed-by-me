from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import paper_engine as pe  # noqa: E402
# [2026-09-13] The 2026-08-07 package split stopped re-exporting these from
#   paper_engine/__init__; every reference below raised AttributeError at import
#   or first call. Import from the module that defines them instead.
from paper_engine.common import _norm_ymd_text, _to_int
from paper_engine.entry import _check_positive_entry_criteria, _load_signal_date_top_codes_by_score, _merge_guard_decision


LOG_DIR = BASE_DIR / "2_Logs"
OUT_JSON = LOG_DIR / "entry_logic_hardening_validation_latest.json"
OUT_CSV = LOG_DIR / "entry_logic_independent_relabel_latest.csv"


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig")


def _truthy_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})


def _latest_by_code(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "code" not in df.columns:
        return df
    work = df.copy()
    work["code"] = work["code"].astype(str).str.zfill(6)
    if "ts" in work.columns:
        work = work.sort_values("ts", kind="mergesort")
    return work.groupby("code", as_index=False, group_keys=False).tail(1)


def _build_independent_pool(cfg: Dict[str, Any]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    cand = _read_csv(LOG_DIR / "candidates_latest_data.with_final_score.csv")
    if not cand.empty:
        cand["_independent_source"] = "latest_final_score_candidates"
        frames.append(cand)

    obs = _latest_by_code(_read_csv(LOG_DIR / "followthrough_observation_tracker.csv"))
    if not obs.empty:
        obs["_independent_source"] = "followthrough_observation_latest_by_code"
        frames.append(obs)

    if not frames:
        return pd.DataFrame()

    pool = pd.concat(frames, ignore_index=True, sort=False)
    if "code" in pool.columns:
        pool["code"] = pool["code"].astype(str).str.zfill(6)
        pool = pool.drop_duplicates(subset=["code", "_independent_source"], keep="last")

    checks = []
    for _, row in pool.iterrows():
        try:
            checks.append(_check_positive_entry_criteria(row, cfg))
        except Exception as exc:
            checks.append({"ok": False, "reason": f"check_error:{type(exc).__name__}:{exc}"})
    pool["relabel_positive_entry_ok"] = [bool(x.get("ok", False)) for x in checks]
    pool["relabel_positive_entry_reason"] = [str(x.get("reason", "")) for x in checks]
    return pool


def _guard_policy_report() -> Dict[str, Any]:
    cases = {
        "execution": ("hard", "BLOCK"),
        "outlier": ("soft", "BLOCK"),
        "sigma": ("soft", "BLOCK"),
        "macro_news": ("soft", "BLOCK"),
        "integrity": ("advisory", "BLOCK"),
        "backtest_validation": ("advisory", "BLOCK"),
    }
    result: Dict[str, Any] = {}
    for name, (severity, decision) in cases.items():
        merged_decision, merged_reason = _merge_guard_decision(
            "ALLOW",
            "",
            guard_tag=name,
            guard_decision=decision,
            guard_reason="probe",
            guard_severity=severity,
        )
        result[name] = {
            "severity": severity,
            "input_decision": decision,
            "merged_decision": merged_decision,
            "merged_reason": merged_reason,
        }
    return result


def _cap_report(cfg: Dict[str, Any]) -> Dict[str, Any]:
    cap = cfg.get("entry_signal_date_top_score_cap", {}) if isinstance(cfg.get("entry_signal_date_top_score_cap"), dict) else {}
    signal_date = ""
    cand = _read_csv(LOG_DIR / "candidates_latest_data.with_final_score.csv")
    if "date" in cand.columns and not cand.empty:
        signal_date = _norm_ymd_text(cand["date"].dropna().astype(str).iloc[0])
    top_n = max(1, int(_to_int(cap.get("top_n", 5), 5) or 5))
    top_codes, top_msg = _load_signal_date_top_codes_by_score(signal_date, top_n=top_n) if signal_date else (None, "no_signal_date")
    return {
        "config": cap,
        "signal_date": signal_date,
        "top_n": top_n,
        "top_codes_count": len(top_codes or []),
        "top_codes": sorted(list(top_codes or [])),
        "loader_message": top_msg,
        "fallback_if_pool_lt_n_effective": bool(cap.get("fallback_if_pool_lt_n", True)) and len(top_codes or []) < top_n,
    }


def _validation_reduce_report(cfg: Dict[str, Any]) -> Dict[str, Any]:
    ro = cfg.get("risk_orchestration", {}) if isinstance(cfg.get("risk_orchestration"), dict) else {}
    val = ro.get("dd_stop_validation", {}) if isinstance(ro.get("dd_stop_validation"), dict) else {}
    p1 = {}
    risk = {}
    for path, target in [
        (LOG_DIR / "p1_entry_gate_status_latest.json", p1),
        (LOG_DIR / "risk_orchestration_latest.json", risk),
    ]:
        if path.exists():
            try:
                target.update(json.loads(path.read_text(encoding="utf-8-sig")))
            except Exception:
                pass
    rt = p1.get("risk_gate_runtime", {}) if isinstance(p1.get("risk_gate_runtime"), dict) else {}
    return {
        "config": val,
        "runtime_position_size_multiplier": rt.get("position_size_multiplier"),
        "runtime_entry_gate": rt.get("entry_gate"),
        "runtime_risk_orchestration": {
            k: (rt.get("risk_orchestration", {}) or {}).get(k)
            for k in ["scale", "scale_after_kelly_tc", "dd_stop_triggered", "scale_after_dd", "scale_zero_causes"]
        },
        "latest_risk_orchestration_file": {
            k: (risk.get("risk_orchestration", {}) or {}).get(k)
            for k in ["scale", "scale_after_kelly_tc", "dd_stop_triggered", "scale_after_dd", "scale_zero_causes"]
        },
    }


def _split2_report() -> Dict[str, Any]:
    pending = _read_csv(LOG_DIR / "pending_entry_signals_latest.csv")
    snapshot = _read_csv(LOG_DIR / "entry_signal_snapshot_latest.csv")
    split2_pending = 0
    if not pending.empty and "split_entry_2nd" in pending.columns:
        split2_pending = int(_truthy_series(pending["split_entry_2nd"]).sum())
    normal_snapshot = 0
    split2_snapshot = 0
    if not snapshot.empty:
        if "is_split_entry_2nd" in snapshot.columns:
            split2_snapshot = int(_truthy_series(snapshot["is_split_entry_2nd"]).sum())
            normal_snapshot = int(len(snapshot) - split2_snapshot)
        else:
            normal_snapshot = int(len(snapshot))
    return {
        "pending_rows": int(len(pending)),
        "pending_split2_rows": split2_pending,
        "snapshot_rows": int(len(snapshot)),
        "snapshot_split2_rows": split2_snapshot,
        "snapshot_normal_rows": normal_snapshot,
        "split2_blocks_new_entries": False,
        "note": "selection policy retains split2 priority rows and normal first-entry rows together",
    }


def main() -> int:
    cfg = pe.load_config()
    pool = _build_independent_pool(cfg)
    if not pool.empty:
        keep_cols = [
            c for c in [
                "code",
                "name",
                "_independent_source",
                "final_score",
                "score",
                "sector_entry_allowed",
                "execution_pool",
                "buy_signal",
                "entry_allowed_validation",
                "block_reasons",
                "relabel_positive_entry_ok",
                "relabel_positive_entry_reason",
            ]
            if c in pool.columns
        ]
        pool[keep_cols].to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "general_entry_logic_hardening_1_6",
        "independent_relabel": {
            "out_csv": str(OUT_CSV),
            "rows": int(len(pool)),
            "unique_codes": int(pool["code"].nunique()) if "code" in pool.columns and not pool.empty else 0,
            "positive_rows": int(pool["relabel_positive_entry_ok"].sum()) if "relabel_positive_entry_ok" in pool.columns else 0,
            "positive_unique_codes": int(pool.loc[pool["relabel_positive_entry_ok"], "code"].nunique()) if "relabel_positive_entry_ok" in pool.columns and "code" in pool.columns else 0,
        },
        "guard_policy": _guard_policy_report(),
        "validation_reduce": _validation_reduce_report(cfg),
        "cap_policy": _cap_report(cfg),
        "split2_policy": _split2_report(),
    }
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
