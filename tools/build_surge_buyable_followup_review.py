from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

BUYABLE_CSV = LOG_DIR / "surge_buyable_candidate_layer_latest.csv"
LOB_CSV = LOG_DIR / "surge_lob_latest.csv"
RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
MICRO_SIM_CSV = LOG_DIR / "surge_ev_micro_probe_simulation_latest.csv"

OUT_JSON = LOG_DIR / "surge_buyable_followup_review_latest.json"
OUT_CSV = LOG_DIR / "surge_buyable_followup_review_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _latest_by_code(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = str(row.get("code") or "").zfill(6)
        ts = str(row.get("ts") or row.get("lob_confirm_ts") or "")
        if code not in out or ts >= str(out[code].get("ts") or out[code].get("lob_confirm_ts") or ""):
            out[code] = row
    return out


def _sim_summary(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("code") or "").zfill(6), []).append(row)
    out: dict[str, dict[str, Any]] = {}
    for code, items in grouped.items():
        ret_vals = [_float(row.get("primary_ret_pct"), None) for row in items]
        ret_vals = [float(v) for v in ret_vals if v is not None]
        ret5 = [_float(row.get("ret_5m_pct"), None) for row in items]
        ret5 = [float(v) for v in ret5 if v is not None]
        out[code] = {
            "sim_rows": len(items),
            "sim_avg_primary_ret_pct": round(sum(ret_vals) / len(ret_vals), 6) if ret_vals else "",
            "sim_best_primary_ret_pct": round(max(ret_vals), 6) if ret_vals else "",
            "sim_best_5m_pct": round(max(ret5), 6) if ret5 else "",
            "sim_stop_rows": sum(1 for row in items if str(row.get("exit_reason") or "") == "STOP_LOSS_HIT"),
        }
    return out


def _followup_decision(row: dict[str, Any]) -> tuple[str, str]:
    lob_status = str(row.get("followup_lob_status") or "").strip().upper()
    lob_available = _truthy(row.get("followup_lob_available"))
    spread = _float(row.get("followup_spread_bps"), 9999.0) or 9999.0
    ask_depth = _float(row.get("followup_ask_depth_levels"), 0.0) or 0.0
    rvol = _float(row.get("rvol20"), 0.0) or 0.0
    sim_rows = int(_float(row.get("sim_rows"), 0) or 0)
    sim_avg = _float(row.get("sim_avg_primary_ret_pct"), None)
    sim_stop = int(_float(row.get("sim_stop_rows"), 0) or 0)
    reasons = str(row.get("entry_reason") or "")

    if lob_status == "BID_ONLY_LIMIT":
        return "KEEP_WATCH", "bid_only_limit_needs_fill_quality_observation"
    if not (lob_available and lob_status == "OK"):
        if rvol > 5.0:
            return "DOWNGRADE_NO_TOUCH", "no_lob_and_rvol_overheat"
        return "KEEP_WATCH", "waiting_for_usable_lob"
    if spread > 30:
        return "DOWNGRADE_NO_TOUCH", "lob_ok_but_spread_too_wide"
    if ask_depth < 3:
        return "KEEP_WATCH", "lob_ok_but_ask_depth_thin"
    if sim_rows and sim_avg is not None and sim_avg <= -1.2 and sim_stop >= 1:
        return "DOWNGRADE_NO_TOUCH", "negative_forward_markout_confirmed"
    if "HIGH_REJECTION_ENTRY_BLOCK" in reasons and sim_rows == 0:
        return "KEEP_WATCH", "high_rejection_needs_forward_markout"
    return "PROMOTE_READY_SHADOW", "lob_recovered_and_no_negative_markout_evidence"


def build() -> dict[str, Any]:
    buyable = [row for row in _read_csv(BUYABLE_CSV) if str(row.get("buyable_grade") or "") == "B_MICRO_PROBE_WATCH"]
    lob_by_code = _latest_by_code(_read_csv(LOB_CSV))
    recheck_by_code = _latest_by_code(_read_csv(RECHECK_CSV))
    sim_by_code = _sim_summary(_read_csv(MICRO_SIM_CSV))

    rows: list[dict[str, Any]] = []
    for row in buyable:
        code = str(row.get("code") or "").zfill(6)
        lob = lob_by_code.get(code, {})
        recheck = recheck_by_code.get(code, {})
        sim = sim_by_code.get(code, {})
        followup = {
            **row,
            "followup_lob_ts": lob.get("ts", ""),
            "followup_lob_status": lob.get("lob_status", ""),
            "followup_lob_available": lob.get("lob_available", ""),
            "followup_spread_bps": lob.get("spread_bps", ""),
            "followup_ask_depth_levels": lob.get("ask_depth_levels", ""),
            "followup_lob_evidence_reason": lob.get("lob_evidence_reason", ""),
            "recheck_ts": recheck.get("ts", ""),
            "recheck_lob_confirm_ts": recheck.get("lob_confirm_ts", ""),
            "recheck_lob_status": recheck.get("lob_status", ""),
            "recheck_probe_class": recheck.get("probe_class", ""),
            "recheck_reason": recheck.get("recheck_reason", ""),
            "sim_rows": sim.get("sim_rows", 0),
            "sim_avg_primary_ret_pct": sim.get("sim_avg_primary_ret_pct", ""),
            "sim_best_primary_ret_pct": sim.get("sim_best_primary_ret_pct", ""),
            "sim_best_5m_pct": sim.get("sim_best_5m_pct", ""),
            "sim_stop_rows": sim.get("sim_stop_rows", 0),
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
        }
        decision, reason = _followup_decision(followup)
        followup["followup_decision"] = decision
        followup["followup_reason"] = reason
        rows.append(followup)

    decision_counts = Counter(row["followup_decision"] for row in rows)
    result = {
        "generated_at": _now_ts(),
        "status": "OK",
        "scope": "surge_buyable_followup_review",
        "source_files": {
            "buyable_candidate_layer": str(BUYABLE_CSV),
            "surge_lob": str(LOB_CSV),
            "no_lob_recheck_queue": str(RECHECK_CSV),
            "micro_probe_simulation": str(MICRO_SIM_CSV),
        },
        "summary": {
            "b_watch_rows": len(buyable),
            "output_rows": len(rows),
            "decision_counts": dict(sorted(decision_counts.items())),
            "promote_ready_shadow_rows": decision_counts.get("PROMOTE_READY_SHADOW", 0),
            "entry_approval_changed": False,
            "policy_change": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
        },
        "interpretation": (
            "Follow-up review checks whether B micro-probe watch rows recovered usable LOB. "
            "PROMOTE_READY_SHADOW is still research-only and not an order route."
        ),
        "artifacts": {"csv": str(OUT_CSV)},
    }
    fields = [
        "ts", "date", "code", "detected_surge_type", "entry_reason", "buyable_grade",
        "suggested_action", "grade_reason", "change_pct", "rvol20", "surge_score_final",
        "followup_decision", "followup_reason", "followup_lob_ts", "followup_lob_status",
        "followup_lob_available", "followup_spread_bps", "followup_ask_depth_levels",
        "followup_lob_evidence_reason", "recheck_ts", "recheck_lob_confirm_ts",
        "recheck_lob_status", "recheck_probe_class", "recheck_reason", "sim_rows",
        "sim_avg_primary_ret_pct", "sim_best_primary_ret_pct", "sim_best_5m_pct",
        "sim_stop_rows", "policy_change", "entry_approval_changed", "paper_order_route",
        "broker_order_route", "trading_route", "research_only",
    ]
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows, fields)
    return result


def main() -> int:
    result = build()
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
