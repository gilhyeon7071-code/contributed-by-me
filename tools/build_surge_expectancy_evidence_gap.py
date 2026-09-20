from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

INTENT_CSV = LOG_DIR / "surge_probe_intent_split_latest.csv"
OUTCOME_CSV = LOG_DIR / "surge_path_validation_outcome_latest.csv"
SANITY_CSV = LOG_DIR / "surge_sanity_labeled_latest.csv"
QUEUE_CSV = LOG_DIR / "candidate_action_queue_latest.csv"
OUT_JSON = LOG_DIR / "surge_expectancy_evidence_gap_latest.json"
OUT_CSV = LOG_DIR / "surge_expectancy_evidence_gap_latest.csv"


REQUIRED_EVIDENCE = {
    "trade_tick": "trade_fill evidence or executable tick/print evidence",
    "orderbook_or_probe_policy": "LOB OK, or explicitly capped no-LOB paper/mock probe policy",
    "followthrough": "forward markout must show continuation rather than immediate adverse move",
    "upside_room_or_nextday_thesis": "intraday room remains, or next-day gap/lock thesis is measured",
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").replace(",", "").strip()
        return float(text) if text else float(default)
    except Exception:
        return float(default)


def _by_code(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _queue_by_code(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        if str(row.get("source") or "") != "surge_realtime":
            continue
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _mtime(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def _extract_missing_sanity_reasons(row: dict[str, str]) -> list[str]:
    raw = str(row.get("surge_sanity_reasons") or "")
    return [part.split(":", 1)[0] + ":" + part.split(":", 1)[1].split("|", 1)[0] if ":" in part else part for part in raw.split("|") if part]


def _evaluate(intent: dict[str, str], outcome: dict[str, str], sanity: dict[str, str], queue: dict[str, str]) -> dict[str, Any]:
    code = _code(intent.get("code"))
    missing: list[str] = []
    blockers: list[str] = []
    no_lob_probe = _truthy(intent.get("no_lob_probe_allowed"))
    lob_status = str(intent.get("lob_status") or "").strip().upper()
    remaining_to_30 = _float(intent.get("remaining_to_30pct"))
    outcome_class = str(outcome.get("outcome_class") or "NOT_EVALUATED")
    sanity_status = str(sanity.get("surge_sanity_status") or intent.get("sanity_status") or "").upper()
    queue_action_state = str(queue.get("action_state") or "").strip().upper()
    active_response_reason = str(queue.get("active_response_reason") or "").strip()
    surge_after_state = str(queue.get("surge_after_state") or "").strip().upper()

    if "trade_fill:missing_trade_tick_evidence" in str(sanity.get("surge_sanity_reasons") or ""):
        missing.append("trade_tick")
    if not no_lob_probe and lob_status != "OK":
        blockers.append("orderbook_missing_without_probe_policy")
    if outcome_class == "PATH_TEST_NEGATIVE":
        blockers.append("followthrough_negative")
    elif outcome_class in {"NOT_EVALUABLE", "NOT_EVALUATED"}:
        missing.append("followthrough")
    elif outcome_class != "PATH_TEST_POSITIVE":
        missing.append("followthrough_positive_not_observed")
    if remaining_to_30 <= 0.005:
        missing.append("upside_room_or_nextday_thesis")
    if sanity_status != "PASS":
        missing.extend(_extract_missing_sanity_reasons(sanity))
    if lob_status == "OK":
        missing = [item for item in missing if item != "orderbook:missing_lob"]
    if surge_after_state in {"WAIT_RECLAIM", "WAIT_LOB", "HARD_EXCLUDE"} and active_response_reason:
        blockers.append(f"active_response:{surge_after_state}:{active_response_reason}")
    elif queue_action_state == "TRADABLE" and active_response_reason:
        blockers.append(f"queue_active_response:{active_response_reason}")

    missing = sorted(set(x for x in missing if x))
    blockers = sorted(set(x for x in blockers if x))
    expectancy_ready = bool(not blockers and not missing and outcome_class == "PATH_TEST_POSITIVE")
    if expectancy_ready:
        state = "EXPECTANCY_READY"
    elif blockers:
        state = "EXPECTANCY_BLOCKED"
    else:
        state = "EXPECTANCY_NOT_PROVEN"
    return {
        "code": code,
        "name": intent.get("name", ""),
        "intent_class": intent.get("intent_class", ""),
        "current_expectancy_buy_ready": _truthy(intent.get("expectancy_buy_ready")),
        "expectancy_evidence_state": state,
        "expectancy_ready_after_gap_review": expectancy_ready,
        "entry_allowed": _truthy(intent.get("entry_allowed")),
        "queue_action_state": queue_action_state,
        "queue_trading_allowed": queue.get("trading_allowed", ""),
        "active_response_reason": active_response_reason,
        "surge_after_state": queue.get("surge_after_state", ""),
        "would_block_surge": queue.get("would_block_surge", ""),
        "defense_signal_score": queue.get("defense_signal_score", ""),
        "defense_reasons": queue.get("defense_reasons", ""),
        "missing_evidence": "|".join(missing),
        "blocking_evidence": "|".join(blockers),
        "outcome_class": outcome_class,
        "outcome_reason": outcome.get("outcome_reason", ""),
        "outcome_signal_ts": outcome.get("signal_ts", ""),
        "outcome_signal_ts_source": outcome.get("signal_ts_source", ""),
        "history_points_after_signal": outcome.get("history_points_after_signal", ""),
        "max_favorable_pct": outcome.get("max_favorable_pct", ""),
        "max_adverse_pct": outcome.get("max_adverse_pct", ""),
        "close_like_ret_pct": outcome.get("close_like_ret_pct", ""),
        "remaining_to_30pct": remaining_to_30,
        "change_pct": intent.get("change_pct", ""),
        "rvol20": intent.get("rvol20", ""),
        "lob_status": intent.get("lob_status", ""),
        "sanity_status": sanity_status,
        "sanity_actionable": sanity.get("surge_sanity_actionable") or intent.get("sanity_actionable", ""),
        "sanity_reasons": sanity.get("surge_sanity_reasons", ""),
        "required_evidence_contract": "; ".join(f"{k}={v}" for k, v in REQUIRED_EVIDENCE.items()),
        "paper_order_route": False,
        "broker_order_route": False,
        "orders_modified": False,
        "fills_modified": False,
        "research_only": True,
    }


def build() -> dict[str, Any]:
    intents = [row for row in _read_csv(INTENT_CSV) if row.get("intent_class") in {"PATH_VALIDATION_PROBE", "REVIEW_ONLY_CANDIDATE", "EXPECTANCY_BUY_CANDIDATE"}]
    outcomes = _by_code(_read_csv(OUTCOME_CSV))
    sanity = _by_code(_read_csv(SANITY_CSV))
    queue = _queue_by_code(_read_csv(QUEUE_CSV))
    rows = [
        _evaluate(
            row,
            outcomes.get(_code(row.get("code")), {}),
            sanity.get(_code(row.get("code")), {}),
            queue.get(_code(row.get("code")), {}),
        )
        for row in intents
    ]
    counts = Counter(row["expectancy_evidence_state"] for row in rows)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "surge_expectancy_evidence_gap",
        "summary": {
            "review_rows": len(rows),
            "state_counts": dict(sorted(counts.items())),
            "expectancy_ready_rows": sum(1 for row in rows if row["expectancy_ready_after_gap_review"] is True),
            "paper_order_route": False,
            "broker_order_route": False,
            "orders_modified": False,
            "fills_modified": False,
            "research_only": True,
        },
        "required_evidence": REQUIRED_EVIDENCE,
        "source_files": {
            "intent": {"path": str(INTENT_CSV), "mtime": _mtime(INTENT_CSV)},
            "outcome": {"path": str(OUTCOME_CSV), "mtime": _mtime(OUTCOME_CSV)},
            "sanity": {"path": str(SANITY_CSV), "mtime": _mtime(SANITY_CSV)},
            "queue": {"path": str(QUEUE_CSV), "mtime": _mtime(QUEUE_CSV)},
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    fields = [
        "code",
        "name",
        "intent_class",
        "current_expectancy_buy_ready",
        "expectancy_evidence_state",
        "expectancy_ready_after_gap_review",
        "entry_allowed",
        "queue_action_state",
        "queue_trading_allowed",
        "active_response_reason",
        "surge_after_state",
        "would_block_surge",
        "defense_signal_score",
        "defense_reasons",
        "missing_evidence",
        "blocking_evidence",
        "outcome_class",
        "outcome_reason",
        "outcome_signal_ts",
        "outcome_signal_ts_source",
        "history_points_after_signal",
        "max_favorable_pct",
        "max_adverse_pct",
        "close_like_ret_pct",
        "remaining_to_30pct",
        "change_pct",
        "rvol20",
        "lob_status",
        "sanity_status",
        "sanity_actionable",
        "sanity_reasons",
        "required_evidence_contract",
        "paper_order_route",
        "broker_order_route",
        "orders_modified",
        "fills_modified",
        "research_only",
    ]
    _write_csv(OUT_CSV, rows, fields)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
