from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SCREENER_CSV = LOG_DIR / "surge_event_money_pullback_screener_latest.csv"
CURRENT_CANDIDATE_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
SURGE_REALTIME_CSV = LOG_DIR / "surge_realtime_latest.csv"
ENTRY_DECISION_CSV = LOG_DIR / "entry_decision_layers_runtime_latest.csv"
PENDING_SIGNALS_CSV = LOG_DIR / "pending_entry_signals_latest.csv"
PENDING_STATUS_JSON = LOG_DIR / "pending_entry_status_latest.json"

OUT_JSON = LOG_DIR / "surge_event_money_pullback_vs_current_logic_latest.json"
OUT_CSV = LOG_DIR / "surge_event_money_pullback_vs_current_logic_latest.csv"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def _code(row: dict[str, Any]) -> str:
    value = str(row.get("code") or "").strip()
    return value.zfill(6) if value.isdigit() else value


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = "" if value is None else str(value).strip()
        if text and text.lower() not in {"nan", "<na>", "none"}:
            return text
    return ""


def _boolish(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _float_or_zero(value: Any) -> float:
    try:
        text = str(value).strip()
        if not text or text.lower() in {"nan", "<na>", "none"}:
            return 0.0
        return float(text)
    except Exception:
        return 0.0


def _is_surge_realtime(row: dict[str, str]) -> bool:
    if _boolish(row.get("detected_surge_flag")):
        return True
    if _boolish(row.get("is_realtime_surge")):
        return True
    if _boolish(row.get("surge_flag")):
        return True
    return bool(_first_nonempty(row.get("detected_surge_type"), row.get("surge_type")))


def _source_add(
    sources: dict[str, set[str]],
    rows_by_code: dict[str, dict[str, str]],
    code: str,
    source: str,
    row: dict[str, str],
) -> None:
    if not code:
        return
    sources[code].add(source)
    rows_by_code.setdefault(code, row)


def _screener_watch(row: dict[str, str]) -> bool:
    bucket = _first_nonempty(row.get("criteria_first_bucket"))
    return bool(bucket) and bucket != "C_TRUE_EXCLUDE"


def _entry_status_rows(status: dict[str, Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for item in status.get("entry_decision_rows") or []:
        if isinstance(item, dict):
            out.append({str(k): "" if v is None else str(v) for k, v in item.items()})
    return out


def _difference_reason(
    overlap_type: str,
    screener_row: dict[str, str] | None,
    current_sources: set[str],
    current_rows: dict[str, dict[str, str]],
) -> str:
    if overlap_type == "BOTH":
        source_text = "+".join(sorted(current_sources))
        entry_row = current_rows.get("current_entry_decision") or current_rows.get("current_pending_status_decision") or {}
        entry_signal = _first_nonempty(entry_row.get("signal"), entry_row.get("next_action"))
        entry_reason = _first_nonempty(entry_row.get("reason"), entry_row.get("execution_reason"), entry_row.get("pending_reason"))
        if entry_signal or entry_reason:
            return f"same_current_logic; sources={source_text}; current_signal={entry_signal}; current_reason={entry_reason}"
        return f"same_current_logic; sources={source_text}"

    if overlap_type == "SCREENER_ONLY":
        row = screener_row or {}
        notes = ["criteria_first_watch_not_in_current_logic_outputs"]
        if "current_surge_realtime" not in current_sources:
            notes.append("not_in_current_realtime_surge")
        if "current_candidate_final" not in current_sources:
            notes.append("not_in_current_final_candidate_pool")
        risk = _float_or_zero(row.get("risk_score"))
        event = _float_or_zero(row.get("event_score"))
        money = _float_or_zero(row.get("money_score"))
        pullback = _float_or_zero(row.get("pullback_score"))
        if risk > 0:
            notes.append(f"screener_risk_score={risk:g}")
        if event < 2:
            notes.append(f"event_score_below_strong={event:g}")
        if pullback <= 0:
            notes.append("pullback_missing_or_not_ready")
        if money >= 3:
            notes.append(f"money_axis_positive={money:g}")
        return "; ".join(notes)

    row = screener_row or {}
    bucket = _first_nonempty(row.get("criteria_first_bucket"), "NO_SCREENER_ROW")
    reason = _first_nonempty(row.get("bucket_reason"), "not_a_criteria_first_watch")
    risk = _float_or_zero(row.get("risk_score"))
    event = _float_or_zero(row.get("event_score"))
    money = _float_or_zero(row.get("money_score"))
    pullback = _float_or_zero(row.get("pullback_score"))
    return (
        f"current_logic_only; screener_bucket={bucket}; bucket_reason={reason}; "
        f"event={event:g}; money={money:g}; pullback={pullback:g}; risk={risk:g}"
    )


def main() -> int:
    screener_rows = _read_csv(SCREENER_CSV)
    candidate_rows = _read_csv(CURRENT_CANDIDATE_CSV)
    surge_rows = _read_csv(SURGE_REALTIME_CSV)
    entry_rows = _read_csv(ENTRY_DECISION_CSV)
    pending_rows = _read_csv(PENDING_SIGNALS_CSV)
    status = _read_json(PENDING_STATUS_JSON)
    status_entry_rows = _entry_status_rows(status)

    screener_by_code: dict[str, dict[str, str]] = {}
    for row in screener_rows:
        code = _code(row)
        if code:
            screener_by_code.setdefault(code, row)

    current_sources: dict[str, set[str]] = defaultdict(set)
    current_rows_by_source: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)

    for row in candidate_rows:
        code = _code(row)
        _source_add(current_sources, current_rows_by_source[code], code, "current_candidate_final", row)

    for row in surge_rows:
        if not _is_surge_realtime(row):
            continue
        code = _code(row)
        _source_add(current_sources, current_rows_by_source[code], code, "current_surge_realtime", row)

    for row in entry_rows:
        code = _code(row)
        _source_add(current_sources, current_rows_by_source[code], code, "current_entry_decision", row)

    for row in pending_rows:
        code = _code(row)
        _source_add(current_sources, current_rows_by_source[code], code, "current_pending_signal", row)

    for row in status_entry_rows:
        code = _code(row)
        _source_add(current_sources, current_rows_by_source[code], code, "current_pending_status_decision", row)

    screener_watch_codes = {code for code, row in screener_by_code.items() if _screener_watch(row)}
    current_codes = set(current_sources)
    compare_codes = sorted(screener_watch_codes | current_codes)

    output_rows: list[dict[str, Any]] = []
    for code in compare_codes:
        srow = screener_by_code.get(code)
        is_watch = code in screener_watch_codes
        is_current = code in current_codes
        if is_watch and is_current:
            overlap_type = "BOTH"
        elif is_watch:
            overlap_type = "SCREENER_ONLY"
        else:
            overlap_type = "CURRENT_ONLY"

        crow_by_source = current_rows_by_source.get(code, {})
        candidate_row = crow_by_source.get("current_candidate_final") or {}
        surge_row = crow_by_source.get("current_surge_realtime") or {}
        entry_row = crow_by_source.get("current_entry_decision") or {}
        pending_row = crow_by_source.get("current_pending_signal") or {}
        status_row = crow_by_source.get("current_pending_status_decision") or {}
        current_signal = _first_nonempty(
            entry_row.get("signal"),
            status_row.get("signal"),
            surge_row.get("entry_decision"),
            pending_row.get("signal"),
        )
        current_reason = _first_nonempty(
            entry_row.get("execution_reason"),
            entry_row.get("pending_reason"),
            status_row.get("reason"),
            surge_row.get("entry_reason"),
            surge_row.get("exclude_reasons"),
            pending_row.get("reason"),
        )
        current_rank_score = _first_nonempty(
            entry_row.get("rank_score"),
            status_row.get("rank_score"),
            pending_row.get("final_score"),
            candidate_row.get("final_score"),
            surge_row.get("surge_score_final"),
            surge_row.get("surge_score"),
        )

        row = {
            "code": code,
            "name": _first_nonempty(
                (srow or {}).get("name"),
                candidate_row.get("name"),
                entry_row.get("name"),
                pending_row.get("name"),
                surge_row.get("name"),
            ),
            "overlap_type": overlap_type,
            "screener_watch": is_watch,
            "current_logic_present": is_current,
            "screener_bucket": _first_nonempty((srow or {}).get("criteria_first_bucket"), "NO_SCREENER_ROW"),
            "screener_bucket_display": _first_nonempty((srow or {}).get("criteria_first_bucket_display")),
            "bucket_contract": _first_nonempty((srow or {}).get("bucket_contract")),
            "bucket_reason": _first_nonempty((srow or {}).get("bucket_reason")),
            "watch_subtype": _first_nonempty((srow or {}).get("watch_subtype")),
            "quality_label": _first_nonempty((srow or {}).get("quality_label")),
            "total_structure_score": _first_nonempty((srow or {}).get("total_structure_score")),
            "event_score": _first_nonempty((srow or {}).get("event_score")),
            "money_score": _first_nonempty((srow or {}).get("money_score")),
            "pullback_score": _first_nonempty((srow or {}).get("pullback_score")),
            "risk_score": _first_nonempty((srow or {}).get("risk_score")),
            "risk_evidence": _first_nonempty((srow or {}).get("risk_evidence")),
            "current_logic_sources": "|".join(sorted(current_sources.get(code, set()))),
            "current_signal": current_signal,
            "current_reason": current_reason,
            "current_rank_score": current_rank_score,
            "current_entry_allowed": _first_nonempty(surge_row.get("entry_allowed")),
            "current_entry_blocked": _first_nonempty(surge_row.get("entry_blocked")),
            "current_surge_type": _first_nonempty(surge_row.get("detected_surge_type"), surge_row.get("surge_type")),
            "current_exclude_reasons": _first_nonempty(surge_row.get("exclude_reasons")),
            "connection_level": "SIGNAL_QUALITY_ONLY",
            "trading_connection": False,
            "signal_connection": False,
            "execution_connection": False,
            "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
            "difference_reason": _difference_reason(
                overlap_type,
                srow,
                current_sources.get(code, set()),
                crow_by_source,
            ),
        }
        output_rows.append(row)

    counts = Counter(row["overlap_type"] for row in output_rows)
    source_counts = Counter()
    for sources in current_sources.values():
        for source in sources:
            source_counts[source] += 1

    summary = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "connection_level": "SIGNAL_QUALITY_ONLY",
        "trading_connection": False,
        "signal_connection": False,
        "execution_connection": False,
        "connection_note": "Signal quality or observation only; not entry approval, order, fill, or ledger.",
        "inputs": {
            "screener_csv": str(SCREENER_CSV),
            "current_candidate_csv": str(CURRENT_CANDIDATE_CSV),
            "surge_realtime_csv": str(SURGE_REALTIME_CSV),
            "entry_decision_csv": str(ENTRY_DECISION_CSV),
            "pending_signals_csv": str(PENDING_SIGNALS_CSV),
            "pending_status_json": str(PENDING_STATUS_JSON),
        },
        "input_rows": {
            "screener": len(screener_rows),
            "current_candidate_final": len(candidate_rows),
            "current_surge_realtime_raw": len(surge_rows),
            "current_entry_decision": len(entry_rows),
            "current_pending_signal": len(pending_rows),
            "current_pending_status_decision": len(status_entry_rows),
        },
        "sets": {
            "screener_watch_codes": len(screener_watch_codes),
            "current_logic_codes": len(current_codes),
            "compared_codes": len(compare_codes),
        },
        "overlap_counts": dict(counts),
        "current_source_counts": dict(source_counts),
        "status_snapshot": {
            "pending_status_generated_at": status.get("generated_at", ""),
            "pending_status_status": status.get("status", ""),
            "candidates_after_caps": status.get("candidates_after_caps", ""),
            "entry_ready": status.get("entry_ready", ""),
            "filled": status.get("filled", ""),
            "pending_queue_len_raw": status.get("pending_queue_len_raw", ""),
        },
        "rows": output_rows,
    }

    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = [
        "code",
        "name",
        "overlap_type",
        "screener_watch",
        "current_logic_present",
        "screener_bucket",
        "screener_bucket_display",
        "bucket_contract",
        "bucket_reason",
        "watch_subtype",
        "quality_label",
        "total_structure_score",
        "event_score",
        "money_score",
        "pullback_score",
        "risk_score",
        "risk_evidence",
        "current_logic_sources",
        "current_signal",
        "current_reason",
        "current_rank_score",
        "current_entry_allowed",
        "current_entry_blocked",
        "current_surge_type",
        "current_exclude_reasons",
        "connection_level",
        "trading_connection",
        "signal_connection",
        "execution_connection",
        "connection_note",
        "difference_reason",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(json.dumps({k: summary[k] for k in ("sets", "overlap_counts", "current_source_counts")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
