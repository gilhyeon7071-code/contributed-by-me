from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

CROSS_JSON = LOG_DIR / "report_entry_trigger_v2_transition_value2b_ret1_1_c3_cross_variant_validation_latest.json"
OUTCOME_JSON = LOG_DIR / "report_entry_trigger_v2_transition_value2b_ret1_1_2025_2026_axis_c3_outcome_latest.json"

OUT_CSV = LOG_DIR / "c3_research_adapter_latest.csv"
OUT_JSON = LOG_DIR / "c3_research_adapter_latest.json"
OUT_MD = LOG_DIR / "c3_research_adapter_latest.md"

METHOD_FAMILY = "C3_TRANSITION_PULLBACK_REACCEL_NORMAL"
ROUTE_CLASS = "NORMAL_RESEARCH"


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"json root is not object: {path}")
    return obj


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def _key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(row.get("code", "") or "").zfill(6),
        str(row.get("signal_date", "") or ""),
        str(row.get("entry_date", "") or ""),
    )


def _year(date_text: Any) -> str:
    s = str(date_text or "")
    return s[:4] if len(s) >= 4 else ""


def _outcome_label(row: Dict[str, Any]) -> str:
    exit_reason = str(row.get("exit_reason", "") or "").upper()
    ret = _safe_float(row.get("ret"), 0.0) or 0.0
    if "STOP" in exit_reason:
        return "STOP_LOSS"
    if ret > 0:
        return "WIN"
    return "LOSS_OTHER"


def _contract_pass(row: Dict[str, Any]) -> bool:
    score = _safe_float(row.get("score"), 0.0) or 0.0
    follow = _safe_int(row.get("followthrough_1d"), 0)
    v_accel = _safe_float(row.get("signal_v_accel"), 0.0) or 0.0
    market = str(row.get("market_after_recheck") or row.get("market_raw") or "").strip()
    return bool(score >= 1.0 and follow == 1 and v_accel >= 0.9 and market)


def _risk_tags(row: Dict[str, Any], outcome_extra: Optional[Dict[str, Any]]) -> List[str]:
    tags: List[str] = []
    exit_reason = str(row.get("exit_reason", "") or "").upper()
    signal_ret1 = _safe_float(row.get("signal_ret1_pct"), 0.0) or 0.0
    entry_gap = _safe_float(row.get("entry_gap_pct"), 0.0) or 0.0
    entry_oc = _safe_float(row.get("entry_day_open_to_close_pct"), 0.0) or 0.0
    max_high = _safe_float((outcome_extra or {}).get("max_high_pct_to_exit"), None)
    min_low = _safe_float((outcome_extra or {}).get("min_low_pct_to_exit"), None)

    if "STOP" in exit_reason and max_high is not None and max_high >= 5.0:
        tags.append("PROFIT_THEN_STOP_EXIT_REPAIR")
    if signal_ret1 >= 0.85 and entry_gap <= 0.7:
        tags.append("HOT_SIGNAL_LOW_GAP_RETRACE_WATCH")
    if signal_ret1 >= 0.85 and entry_oc > 2.0:
        tags.append("ENTRY_DAY_CHASE_WATCH")
    if min_low is not None and min_low <= -10.0:
        tags.append("DEEP_ADVERSE_PATH_WATCH")
    if not tags:
        tags.append("NO_SPECIAL_RISK_TAG")
    return tags


def _entry_variant_notes(row: Dict[str, Any], risk_tags: List[str]) -> str:
    notes = ["NEXT_OPEN_BASELINE"]
    if "PROFIT_THEN_STOP_EXIT_REPAIR" in risk_tags:
        notes.append("PARTIAL_TP_OR_EARLY_EXIT_RESEARCH")
    if "ENTRY_DAY_CHASE_WATCH" in risk_tags:
        notes.append("CHASE_FILTER_RESEARCH_ONLY")
    if "HOT_SIGNAL_LOW_GAP_RETRACE_WATCH" in risk_tags:
        notes.append("RETRACE_CONFIRMATION_RESEARCH_ONLY")
    return "|".join(notes)


def _build_rows(cross: Dict[str, Any], outcome: Dict[str, Any]) -> List[Dict[str, Any]]:
    dedup_rows = cross.get("dedup_rows") if isinstance(cross.get("dedup_rows"), list) else []
    outcome_rows = outcome.get("rows") if isinstance(outcome.get("rows"), list) else []
    outcome_by_key = {_key(r): r for r in outcome_rows if isinstance(r, dict)}

    rows: List[Dict[str, Any]] = []
    for raw in dedup_rows:
        if not isinstance(raw, dict):
            continue
        if _safe_int(raw.get("c3_after_recheck"), 0) != 1:
            continue
        extra = outcome_by_key.get(_key(raw), {})
        risk_tags = _risk_tags(raw, extra)
        out = {
            "methodology_family": METHOD_FAMILY,
            "route_class": ROUTE_CLASS,
            "surge_route": 0,
            "operational_candidate": 0,
            "research_only": 1,
            "code": str(raw.get("code", "") or "").zfill(6),
            "signal_date": str(raw.get("signal_date", "") or ""),
            "entry_date": str(raw.get("entry_date", "") or ""),
            "exit_date": str(raw.get("exit_date", "") or ""),
            "year": _year(raw.get("entry_date")),
            "market": str(raw.get("market_after_recheck") or raw.get("market_raw") or ""),
            "market_rechecked": _safe_int(extra.get("market_rechecked"), 0),
            "setup_family": str(raw.get("setup_family", "") or ""),
            "market_regime": str(raw.get("market_regime", "") or ""),
            "ret": _safe_float(raw.get("ret"), 0.0),
            "ret_pct": _safe_float(raw.get("ret_pct"), 0.0),
            "exit_reason": str(raw.get("exit_reason", "") or ""),
            "outcome": _outcome_label(raw),
            "score": _safe_float(raw.get("score"), 0.0),
            "followthrough_1d": _safe_int(raw.get("followthrough_1d"), 0),
            "signal_v_accel": _safe_float(raw.get("signal_v_accel"), 0.0),
            "signal_rs": _safe_float(raw.get("signal_rs"), 0.0),
            "signal_ret1_pct": _safe_float(raw.get("signal_ret1_pct"), 0.0),
            "entry_gap_pct": _safe_float(raw.get("entry_gap_pct"), 0.0),
            "entry_day_open_to_close_pct": _safe_float(raw.get("entry_day_open_to_close_pct"), 0.0),
            "max_high_pct_to_exit": _safe_float(extra.get("max_high_pct_to_exit"), None),
            "min_low_pct_to_exit": _safe_float(extra.get("min_low_pct_to_exit"), None),
            "salvage_candidate": str(extra.get("salvage_candidate", "") or ""),
            "profit_stop_pattern": str(extra.get("profit_stop_pattern", "") or ""),
            "candidate_contract_pass": 1 if _contract_pass(raw) else 0,
            "risk_tags": "|".join(risk_tags),
            "entry_variant_notes": _entry_variant_notes(raw, risk_tags),
            "selected_in_variants": str(raw.get("selected_in_variants", "") or ""),
            "variant_count": _safe_int(raw.get("variant_count"), 0),
        }
        rows.append(out)
    rows.sort(key=lambda r: (str(r["entry_date"]), str(r["code"])))
    return rows


def _profit_factor(rows: Iterable[Dict[str, Any]]) -> Optional[float]:
    gains = 0.0
    losses = 0.0
    for r in rows:
        ret = _safe_float(r.get("ret"), 0.0) or 0.0
        if ret > 0:
            gains += ret
        elif ret < 0:
            losses += abs(ret)
    if losses <= 0:
        return None if gains <= 0 else float("inf")
    return round(gains / losses, 6)


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_year: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_year[str(r.get("year") or "")].append(r)
    return {
        "rows": len(rows),
        "codes": ",".join([str(r["code"]) for r in rows]),
        "win_n": sum(1 for r in rows if r.get("outcome") == "WIN"),
        "stop_n": sum(1 for r in rows if r.get("outcome") == "STOP_LOSS"),
        "loss_other_n": sum(1 for r in rows if r.get("outcome") == "LOSS_OTHER"),
        "ret_sum": round(sum((_safe_float(r.get("ret"), 0.0) or 0.0) for r in rows), 6),
        "ret_mean": round(sum((_safe_float(r.get("ret"), 0.0) or 0.0) for r in rows) / len(rows), 6) if rows else 0.0,
        "pf": _profit_factor(rows),
        "contract_pass_n": sum(1 for r in rows if _safe_int(r.get("candidate_contract_pass"), 0) == 1),
        "risk_tag_counts": dict(Counter(tag for r in rows for tag in str(r.get("risk_tags", "")).split("|") if tag)),
        "by_year": {
            y: {
                "n": len(yr),
                "win_n": sum(1 for r in yr if r.get("outcome") == "WIN"),
                "stop_n": sum(1 for r in yr if r.get("outcome") == "STOP_LOSS"),
                "ret_sum": round(sum((_safe_float(r.get("ret"), 0.0) or 0.0) for r in yr), 6),
                "pf": _profit_factor(yr),
            }
            for y, yr in sorted(by_year.items())
        },
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "methodology_family",
        "route_class",
        "surge_route",
        "operational_candidate",
        "research_only",
        "code",
        "signal_date",
        "entry_date",
        "exit_date",
        "year",
        "market",
        "market_rechecked",
        "setup_family",
        "market_regime",
        "ret",
        "ret_pct",
        "exit_reason",
        "outcome",
        "score",
        "followthrough_1d",
        "signal_v_accel",
        "signal_rs",
        "signal_ret1_pct",
        "entry_gap_pct",
        "entry_day_open_to_close_pct",
        "max_high_pct_to_exit",
        "min_low_pct_to_exit",
        "salvage_candidate",
        "profit_stop_pattern",
        "candidate_contract_pass",
        "risk_tags",
        "entry_variant_notes",
        "selected_in_variants",
        "variant_count",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def _write_md(path: Path, payload: Dict[str, Any]) -> None:
    summary = payload["summary"]
    lines = [
        "# C3 read-only 연구 어댑터",
        "",
        "## 판정",
        "",
        "- C3는 급등 실시간 전략이 아니라 일반/전환형 일봉 연구 후보로 분리한다.",
        "- 운영 후보가 아니라 research-only 표본이다.",
        "- 다음 단계는 이 표본으로 진입/청산 변형을 검증하는 것이다.",
        "",
        "## 핵심 값",
        "",
        f"- rows: `{summary['rows']}`",
        f"- win_n: `{summary['win_n']}`",
        f"- stop_n: `{summary['stop_n']}`",
        f"- loss_other_n: `{summary['loss_other_n']}`",
        f"- ret_sum: `{summary['ret_sum']}`",
        f"- ret_mean: `{summary['ret_mean']}`",
        f"- pf: `{summary['pf']}`",
        f"- contract_pass_n: `{summary['contract_pass_n']}`",
        "",
        "## 리스크 태그",
        "",
    ]
    for k, v in sorted(summary["risk_tag_counts"].items()):
        lines.append(f"- `{k}`: `{v}`")
    lines.extend(
        [
            "",
            "## 공식/운영 영향",
            "",
            "- stable params 변경 없음",
            "- 후보 생성기 변경 없음",
            "- HPO 변경 없음",
            "- paper engine 변경 없음",
            "- order/fill/ledger 변경 없음",
            "- 전체로직 미적용",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    cross = _load_json(CROSS_JSON)
    outcome = _load_json(OUTCOME_JSON)
    rows = _build_rows(cross, outcome)
    summary = _summarize(rows)
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "READ_ONLY_C3_RESEARCH_ADAPTER_NOT_OPERATIONAL",
        "methodology_family": METHOD_FAMILY,
        "route_class": ROUTE_CLASS,
        "operational_applied": False,
        "full_logic_application": "NOT_APPLIED",
        "source_files": {
            "cross_variant": str(CROSS_JSON),
            "axis_outcome": str(OUTCOME_JSON),
        },
        "summary": summary,
        "operation_effect": {
            "stable_params": False,
            "candidate_generator": False,
            "hpo": False,
            "paper_engine": False,
            "paper_config": False,
            "orders": False,
            "fills": False,
            "ledger": False,
        },
        "next": {
            "recommended": "validate_entry_exit_variants_on_c3_research_sample",
            "do_not": "do_not_promote_to_operational_rule_from_this_adapter",
        },
        "rows": rows,
        "output_files": {
            "csv": str(OUT_CSV),
            "json": str(OUT_JSON),
            "md": str(OUT_MD),
        },
    }
    _write_csv(OUT_CSV, rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({k: payload[k] for k in ("status", "methodology_family", "route_class", "summary", "output_files")}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
