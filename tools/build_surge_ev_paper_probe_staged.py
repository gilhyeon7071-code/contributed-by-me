"""Build isolated paper-only staged candidates for surge EV micro-probes.

The output is a review/staging artifact. It is intentionally not consumed by
orders_exec, broker dispatch, or the normal candidate action plan.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

READINESS_JSON = LOG_DIR / "surge_ev_probe_readiness_latest.json"
SIMULATION_JSON = LOG_DIR / "surge_ev_shadow_simulation_latest.json"
OUT_JSON = LOG_DIR / "surge_ev_paper_probe_staged_latest.json"
OUT_CSV = LOG_DIR / "surge_ev_paper_probe_staged_latest.csv"

MIN_LOT_QTY = 1
UNKNOWN_DEPTH_HAIRCUT = 0.50
ASK_DEPTH_LT2_HAIRCUT = 0.25
ASK_DEPTH_LT3_HAIRCUT = 0.50
SPREAD_GT20_HAIRCUT = 0.50


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    if not fields:
        fields = ["stage_id"]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _matches_rule(row: Dict[str, Any], rule: str) -> bool:
    if str(row.get("status") or "") != "EVALUATED":
        return False
    if rule == "SCORE_RVOL_12_16_ALL":
        return (
            row.get("source") == "SCORE_RVOL_LOB_CONFIRMED_CLEAN"
            and 0.12 <= _to_float(row.get("change_pct")) < 0.16
        )
    if rule == "WATCH_SCORE_RVOL_12_16_SPREAD_LE15_RVOL_3_4_2":
        return (
            row.get("source") == "SCORE_RVOL_LOB_CONFIRMED_CLEAN"
            and 0.12 <= _to_float(row.get("change_pct")) < 0.16
            and _to_float(row.get("spread_bps")) <= 15.0
            and 3.0 <= _to_float(row.get("rvol20")) <= 4.2
        )
    return False


def _qty_for(notional: float, entry_price: float) -> int:
    if notional <= 0 or entry_price <= 0:
        return 0
    return max(MIN_LOT_QTY, int(notional // entry_price))


def _lob_quality_haircut(row: Dict[str, Any]) -> tuple[float, str]:
    lob_status = str(row.get("lob_status") or "").upper()
    ask_depth = _to_float(row.get("ask_depth_levels"), -1.0)
    spread_bps = _to_float(row.get("spread_bps"))
    reasons: List[str] = []
    factor = 1.0
    if lob_status == "BID_ONLY_LIMIT":
        return 0.0, "BID_ONLY_LIMIT_NO_ASK_FILL"
    if lob_status and lob_status != "OK":
        return 0.0, f"LOB_STATUS_NOT_OK:{lob_status}"
    if ask_depth < 0:
        factor = min(factor, UNKNOWN_DEPTH_HAIRCUT)
        reasons.append("ASK_DEPTH_UNKNOWN_50PCT_HAIRCUT")
    elif ask_depth < 2.0:
        factor = min(factor, ASK_DEPTH_LT2_HAIRCUT)
        reasons.append("ASK_DEPTH_LT2_75PCT_HAIRCUT")
    elif ask_depth < 3.0:
        factor = min(factor, ASK_DEPTH_LT3_HAIRCUT)
        reasons.append("ASK_DEPTH_LT3_50PCT_HAIRCUT")
    if spread_bps > 20.0:
        factor = min(factor, SPREAD_GT20_HAIRCUT)
        reasons.append("SPREAD_GT20_50PCT_HAIRCUT")
    if not reasons:
        reasons.append("LOB_QUALITY_NO_HAIRCUT")
    return factor, "|".join(reasons)


def _stage_rows(readiness_row: Dict[str, Any], bucket_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rule = str(readiness_row.get("rule") or "")
    notional = _to_float(readiness_row.get("proposed_probe_notional_krw"))
    stop_pct = _to_float(readiness_row.get("probe_stop_loss_pct"))
    timebox_min = int(_to_float(readiness_row.get("probe_timebox_min")))
    out: List[Dict[str, Any]] = []
    if str(readiness_row.get("readiness_status") or "") != "SHADOW_PAPER_PROBE_READY_NOT_ROUTED":
        return out
    for row in bucket_rows:
        if not _matches_rule(row, rule):
            continue
        code = str(row.get("code") or "").zfill(6)
        entry_price = _to_float(row.get("entry_price"))
        base_qty = _qty_for(notional, entry_price)
        haircut_factor, haircut_reasons = _lob_quality_haircut(row)
        qty = int(base_qty * haircut_factor)
        if base_qty > 0 and haircut_factor > 0 and qty < MIN_LOT_QTY:
            qty = MIN_LOT_QTY
        estimated_notional = qty * entry_price
        estimated_loss = estimated_notional * stop_pct
        out.append({
            "stage_id": f"SURGE_EV_PROBE_{rule}_{code}_{str(row.get('entry_ts') or '').replace(':', '').replace('-', '')}",
            "stage_status": "PAPER_ONLY_STAGED_NOT_DISPATCHED",
            "rule": rule,
            "code": code,
            "source": row.get("source", ""),
            "signal_ts": row.get("signal_ts", ""),
            "entry_reference_ts": row.get("entry_ts", ""),
            "entry_reference_price": entry_price,
            "base_suggested_qty_before_lob_haircut": base_qty,
            "suggested_qty": qty,
            "suggested_notional_krw": round(estimated_notional, 2),
            "proposed_probe_notional_cap_krw": round(notional, 2),
            "lob_quality_haircut_factor": round(haircut_factor, 6),
            "lob_quality_haircut_reasons": haircut_reasons,
            "probe_stop_loss_pct": stop_pct,
            "estimated_loss_at_stop_krw": round(estimated_loss, 2),
            "probe_timebox_min": timebox_min,
            "surge_score_final": row.get("surge_score_final", ""),
            "change_pct": row.get("change_pct", ""),
            "rvol20": row.get("rvol20", ""),
            "lob_status": row.get("lob_status", ""),
            "spread_bps": row.get("spread_bps", ""),
            "ask_depth_levels": row.get("ask_depth_levels", ""),
            "primary_ret_pct": row.get("primary_ret_pct", ""),
            "exit_reason": row.get("exit_reason", ""),
            "requires_lob_ok": True,
            "requires_no_hard_blocker": True,
            "requires_same_day_not_reentered": True,
            "same_day_reentry_allowed": False,
            "scale_up_allowed": False,
            "paper_probe_stage": True,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "entry_approval_changed": False,
            "policy_change": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
        })
    return out


def _rule_priority(rule: str) -> int:
    if rule == "WATCH_SCORE_RVOL_12_16_SPREAD_LE15_RVOL_3_4_2":
        return 0
    if rule == "SCORE_RVOL_12_16_ALL":
        return 1
    return 9


def _select_stage_slots(rows: List[Dict[str, Any]], max_slots: int) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    seen_codes: set[str] = set()
    ranked = sorted(
        rows,
        key=lambda row: (
            _rule_priority(str(row.get("rule") or "")),
            -_to_float(row.get("primary_ret_pct")),
            str(row.get("entry_reference_ts") or ""),
            str(row.get("code") or ""),
        ),
    )
    for row in ranked:
        code = str(row.get("code") or "")
        if not code or code in seen_codes:
            continue
        selected.append(row)
        seen_codes.add(code)
        if len(selected) >= max_slots:
            break
    return selected


def main() -> int:
    readiness = _read_json(READINESS_JSON)
    simulation = _read_json(SIMULATION_JSON)
    simulation_rows = list(simulation.get("rows") or [])
    all_rows: List[Dict[str, Any]] = []
    max_slots = 0
    for readiness_row in readiness.get("rows", []):
        max_slots = max(max_slots, int(_to_float(readiness_row.get("max_daily_probe_slots"), 0.0)))
        all_rows.extend(_stage_rows(readiness_row, simulation_rows))
    rows = _select_stage_slots(all_rows, max_slots=max_slots or 0)
    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "surge_ev_paper_probe_staged",
        "source_readiness_json": str(READINESS_JSON),
        "source_simulation_json": str(SIMULATION_JSON),
        "risk_contract": {
            "isolated_artifact": True,
            "orders_exec_write": False,
            "candidate_action_plan_write": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "entry_approval_changed": False,
            "policy_change": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "summary": {
            "rows": len(rows),
            "source_stage_rows": len(all_rows),
            "max_daily_probe_slots": int(max_slots),
            "unique_codes": sorted({row.get("code") for row in rows}),
            "paper_probe_stage_rows": sum(1 for row in rows if row.get("paper_probe_stage") is True),
            "lob_quality_haircut_rows": sum(1 for row in rows if _to_float(row.get("lob_quality_haircut_factor"), 1.0) < 1.0),
            "zero_qty_after_lob_haircut_rows": sum(1 for row in rows if int(_to_float(row.get("suggested_qty"), 0.0)) <= 0),
            "dispatch_enabled_rows": sum(1 for row in rows if row.get("dispatch_enabled") is True),
            "broker_order_route_rows": sum(1 for row in rows if row.get("broker_order_route") is True),
            "trading_allowed_rows": sum(1 for row in rows if row.get("trading_allowed") is True),
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", "rows": len(rows), "unique_codes": payload["summary"]["unique_codes"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
