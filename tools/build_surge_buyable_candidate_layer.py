from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SURGE_REALTIME_CSV = LOG_DIR / "surge_realtime_latest.csv"
EV_BUCKET_CSV = LOG_DIR / "surge_ev_bucket_decomposition_latest.csv"
MICRO_SIM_CSV = LOG_DIR / "surge_ev_micro_probe_simulation_latest.csv"

OUT_JSON = LOG_DIR / "surge_buyable_candidate_layer_latest.json"
OUT_CSV = LOG_DIR / "surge_buyable_candidate_layer_latest.csv"


HARD_NO_TOUCH_REASONS = {
    "KRX_ADMIN",
    "KRX_WARNING",
    "KRX_RISK",
    "NEWS_NEGATIVE",
    "NEWS_IMPLICATION_BLOCK",
    "ORDERFLOW_PAUSE",
    "ORDERFLOW_RISK_BLOCK",
    "TRADING_VALUE_FLOOR",
}


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


def _split_reasons(*values: Any) -> set[str]:
    out: set[str] = set()
    for value in values:
        text = str(value or "")
        for token in text.replace("|", ";").replace(",", ";").split(";"):
            token = token.strip().upper()
            if token:
                out.add(token)
    return out


def _ev_by_code(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("code") or "").zfill(6)].append(row)
    out: dict[str, dict[str, Any]] = {}
    for code, items in grouped.items():
        evaluated = [row for row in items if str(row.get("status") or "") == "EVALUATED"]
        ret_vals = [_float(row.get("primary_ret_pct"), None) for row in evaluated]
        ret_vals = [float(v) for v in ret_vals if v is not None]
        fav_vals = [_float(row.get("max_favorable_pct"), None) for row in items]
        fav_vals = [float(v) for v in fav_vals if v is not None]
        adv_vals = [_float(row.get("max_adverse_pct"), None) for row in items]
        adv_vals = [float(v) for v in adv_vals if v is not None]
        flags = Counter()
        for row in items:
            for flag in str(row.get("cause_flags") or "").split("|"):
                if flag:
                    flags[flag] += 1
        out[code] = {
            "ev_rows": len(items),
            "ev_evaluated_rows": len(evaluated),
            "ev_avg_primary_ret_pct": round(sum(ret_vals) / len(ret_vals), 6) if ret_vals else "",
            "ev_best_primary_ret_pct": round(max(ret_vals), 6) if ret_vals else "",
            "ev_worst_primary_ret_pct": round(min(ret_vals), 6) if ret_vals else "",
            "ev_best_favorable_pct": round(max(fav_vals), 6) if fav_vals else "",
            "ev_worst_adverse_pct": round(min(adv_vals), 6) if adv_vals else "",
            "ev_stop_rows": sum(1 for row in items if str(row.get("exit_reason") or "") == "STOP_LOSS_HIT"),
            "ev_positive_rows": sum(1 for v in ret_vals if v > 0),
            "ev_cause_flags": "|".join(flag for flag, _ in flags.most_common()),
        }
    return out


def _micro_by_code(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("code") or "").zfill(6)].append(row)
    out: dict[str, dict[str, Any]] = {}
    for code, items in grouped.items():
        ret5 = [_float(row.get("ret_5m_pct"), None) for row in items]
        ret15 = [_float(row.get("ret_15m_pct"), None) for row in items]
        ret30 = [_float(row.get("ret_30m_pct"), None) for row in items]
        ret5 = [float(v) for v in ret5 if v is not None]
        ret15 = [float(v) for v in ret15 if v is not None]
        ret30 = [float(v) for v in ret30 if v is not None]
        out[code] = {
            "markout_rows": len(items),
            "ret_5m_best_pct": round(max(ret5), 6) if ret5 else "",
            "ret_15m_best_pct": round(max(ret15), 6) if ret15 else "",
            "ret_30m_best_pct": round(max(ret30), 6) if ret30 else "",
        }
    return out


def _classify(row: dict[str, Any]) -> tuple[str, str, float, list[str]]:
    reasons = _split_reasons(row.get("entry_reason"), row.get("exclude_reasons"), row.get("paper_probe_block_reasons"))
    hard_reasons = sorted(reasons & HARD_NO_TOUCH_REASONS)
    change = _float(row.get("change_pct"), 0.0) or 0.0
    rvol = _float(row.get("rvol20"), 0.0) or 0.0
    score = _float(row.get("surge_score_final"), 0.0) or 0.0
    trading_value = _float(row.get("trading_value"), 0.0) or 0.0
    drawdown = _float(row.get("intraday_high_drawdown_pct"), 0.0) or 0.0
    spread = _float(row.get("spread_bps"), 9999.0) or 9999.0
    ask_depth = _float(row.get("ask_depth_levels"), 0.0) or 0.0
    lob_ok = str(row.get("lob_status") or "").strip().upper() == "OK" and _truthy(row.get("lob_available"))
    ev_rows = int(_float(row.get("ev_rows"), 0) or 0)
    ev_avg = _float(row.get("ev_avg_primary_ret_pct"), None)
    ev_best = _float(row.get("ev_best_primary_ret_pct"), None)
    ev_stops = int(_float(row.get("ev_stop_rows"), 0) or 0)
    ev_flags = _split_reasons(row.get("ev_cause_flags"))

    notes: list[str] = []
    score_points = 0.0

    if hard_reasons:
        notes.append("hard_no_touch_reason=" + "|".join(hard_reasons))
        return "C_NO_TOUCH", "hard_policy_or_liquidity_block", 0.0, notes

    if trading_value < 1_000_000_000:
        notes.append("trading_value_below_1b")
        return "C_NO_TOUCH", "insufficient_trading_value", 0.0, notes

    if "NO_LOB_BLOCK" in reasons and not lob_ok:
        if ev_rows and ev_avg is not None and ev_avg <= -1.2 and ev_stops >= 1:
            notes.append("no_lob_with_negative_ev_markout")
            return "C_NO_TOUCH", "observed_adverse_markout_after_no_lob", 0.0, notes
        if rvol > 5.0:
            notes.append("no_lob_with_rvol_overheat_gt_5")
            return "C_NO_TOUCH", "no_lob_rvol_overheat", 0.0, notes
        if score >= 80 and trading_value >= 1_000_000_000:
            notes.append("no_lob_but_recheck_observation_value")
            return "B_MICRO_PROBE_WATCH", "lob_recheck_watch_only", 0.25, notes
        return "C_NO_TOUCH", "no_lob_without_observation_edge", 0.0, notes

    if lob_ok:
        score_points += 1.0
    if spread <= 15:
        score_points += 1.0
    elif spread > 30:
        score_points -= 1.0
        notes.append("wide_spread_gt_30bps")
    if ask_depth >= 3:
        score_points += 0.5
    if score >= 80:
        score_points += 1.0
    if 1.5 <= rvol <= 5.0:
        score_points += 0.75
    elif rvol > 5.0:
        score_points -= 1.0
        notes.append("rvol_overheat_gt_5")
    else:
        notes.append("weak_rvol_lt_1_5")
    if drawdown >= -0.025:
        score_points += 0.75
    elif drawdown >= -0.08:
        score_points -= 0.25
        notes.append("gray_high_rejection_drawdown")
    else:
        score_points -= 1.0
        notes.append("deep_high_rejection_drawdown")
    if change <= 0.16:
        score_points += 0.5
    elif change <= 0.24:
        notes.append("entry_change_16_24_watch")
    else:
        score_points -= 0.25
        notes.append("late_chase_ge_24")

    if ev_rows:
        if ev_avg is not None and ev_avg > 0:
            score_points += 1.0
        if ev_best is not None and ev_best > 0:
            score_points += 0.5
        if ev_stops >= 1 and ev_avg is not None and ev_avg <= -1.2:
            score_points -= 1.0
            notes.append("ev_stop_loss_negative")
        if {"FAST_ADVERSE_MOVE", "LOW_FOLLOWTHROUGH"} & ev_flags:
            score_points -= 0.5
            notes.append("ev_adverse_or_low_followthrough")
    else:
        notes.append("no_forward_markout_yet")

    if score_points >= 4.5 and lob_ok:
        return "A_BUYABLE_WATCH", "clean_buyable_watch_not_order_approval", 0.5, notes
    if score_points >= 2.0:
        return "B_MICRO_PROBE_WATCH", "observation_or_micro_probe_candidate", 0.25, notes
    return "C_NO_TOUCH", "risk_reward_not_supported_current_layer", 0.0, notes


def build() -> dict[str, Any]:
    realtime_rows = _read_csv(SURGE_REALTIME_CSV)
    ev = _ev_by_code(_read_csv(EV_BUCKET_CSV))
    micro = _micro_by_code(_read_csv(MICRO_SIM_CSV))
    candidates = [
        row for row in realtime_rows
        if _truthy(row.get("detected_surge_flag")) or _truthy(row.get("is_realtime_surge"))
    ]

    out_rows: list[dict[str, Any]] = []
    for row in candidates:
        code = str(row.get("code") or "").zfill(6)
        enriched: dict[str, Any] = dict(row)
        enriched.update(ev.get(code, {
            "ev_rows": 0,
            "ev_evaluated_rows": 0,
            "ev_avg_primary_ret_pct": "",
            "ev_best_primary_ret_pct": "",
            "ev_worst_primary_ret_pct": "",
            "ev_best_favorable_pct": "",
            "ev_worst_adverse_pct": "",
            "ev_stop_rows": 0,
            "ev_positive_rows": 0,
            "ev_cause_flags": "",
        }))
        enriched.update(micro.get(code, {
            "markout_rows": 0,
            "ret_5m_best_pct": "",
            "ret_15m_best_pct": "",
            "ret_30m_best_pct": "",
        }))
        grade, action, size_factor, notes = _classify(enriched)
        out_rows.append({
            "ts": row.get("ts", ""),
            "date": row.get("date", ""),
            "code": code,
            "detected_surge_type": row.get("detected_surge_type", ""),
            "entry_decision": row.get("entry_decision", ""),
            "entry_reason": row.get("entry_reason", ""),
            "exclude_reasons": row.get("exclude_reasons", ""),
            "buyable_grade": grade,
            "suggested_action": action,
            "suggested_size_factor": size_factor,
            "grade_reason": "|".join(notes),
            "change_pct": row.get("change_pct", ""),
            "rvol20": row.get("rvol20", ""),
            "trading_value": row.get("trading_value", ""),
            "intraday_high_drawdown_pct": row.get("intraday_high_drawdown_pct", ""),
            "surge_score_final": row.get("surge_score_final", ""),
            "lob_status": row.get("lob_status", ""),
            "lob_available": row.get("lob_available", ""),
            "spread_bps": row.get("spread_bps", ""),
            "ask_depth_levels": row.get("ask_depth_levels", ""),
            "paper_probe_allowed": row.get("paper_probe_allowed", ""),
            "ev_rows": enriched.get("ev_rows", 0),
            "ev_evaluated_rows": enriched.get("ev_evaluated_rows", 0),
            "ev_avg_primary_ret_pct": enriched.get("ev_avg_primary_ret_pct", ""),
            "ev_best_primary_ret_pct": enriched.get("ev_best_primary_ret_pct", ""),
            "ev_worst_primary_ret_pct": enriched.get("ev_worst_primary_ret_pct", ""),
            "ev_stop_rows": enriched.get("ev_stop_rows", 0),
            "ev_positive_rows": enriched.get("ev_positive_rows", 0),
            "ev_cause_flags": enriched.get("ev_cause_flags", ""),
            "ret_5m_best_pct": enriched.get("ret_5m_best_pct", ""),
            "ret_15m_best_pct": enriched.get("ret_15m_best_pct", ""),
            "ret_30m_best_pct": enriched.get("ret_30m_best_pct", ""),
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
        })

    grade_counts = Counter(row["buyable_grade"] for row in out_rows)
    action_counts = Counter(row["suggested_action"] for row in out_rows)
    by_type = Counter(row["detected_surge_type"] for row in out_rows)
    result = {
        "generated_at": _now_ts(),
        "status": "OK",
        "scope": "surge_buyable_candidate_layer",
        "source_files": {
            "surge_realtime": str(SURGE_REALTIME_CSV),
            "ev_bucket_decomposition": str(EV_BUCKET_CSV),
            "micro_probe_simulation": str(MICRO_SIM_CSV),
        },
        "summary": {
            "realtime_rows": len(realtime_rows),
            "detected_surge_rows": len(candidates),
            "output_rows": len(out_rows),
            "grade_counts": dict(sorted(grade_counts.items())),
            "action_counts": dict(sorted(action_counts.items())),
            "detected_type_counts": dict(sorted(by_type.items())),
            "entry_approval_changed": False,
            "policy_change": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
            "research_only": True,
        },
        "interpretation": (
            "This layer separates buy-candidate discovery from existing entry approval. "
            "It does not relax trading policy; it preserves surge candidates as A/B/C research layers."
        ),
        "artifacts": {
            "csv": str(OUT_CSV),
        },
    }
    fields = [
        "ts", "date", "code", "detected_surge_type", "entry_decision", "entry_reason",
        "exclude_reasons", "buyable_grade", "suggested_action", "suggested_size_factor",
        "grade_reason", "change_pct", "rvol20", "trading_value", "intraday_high_drawdown_pct",
        "surge_score_final", "lob_status", "lob_available", "spread_bps", "ask_depth_levels",
        "paper_probe_allowed", "ev_rows", "ev_evaluated_rows", "ev_avg_primary_ret_pct",
        "ev_best_primary_ret_pct", "ev_worst_primary_ret_pct", "ev_stop_rows",
        "ev_positive_rows", "ev_cause_flags", "ret_5m_best_pct", "ret_15m_best_pct",
        "ret_30m_best_pct", "policy_change", "entry_approval_changed", "paper_order_route",
        "broker_order_route", "trading_route", "research_only",
    ]
    OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, out_rows, fields)
    return result


def main() -> int:
    result = build()
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
