from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
WIKI_INDEX = ROOT / "docs" / "llm_wiki" / "wiki_index_latest.md"

ACTIVE_LAYER_JSON = LOG_DIR / "surge_active_response_layer_latest.json"
ACTIVE_LAYER_CSV = LOG_DIR / "surge_active_response_layer_latest.csv"
QUEUE_JSON = LOG_DIR / "surge_active_response_queue_latest.json"
LOOP_STATUS_JSON = LOG_DIR / "intraday_loop_status_latest.json"
LEDGER_DRY_RUN_JSON = LOG_DIR / "ledger_live_fills_dry_run_latest.json"

OUT_JSON = LOG_DIR / "surge_live_readiness_audit_latest.json"
OUT_CSV = LOG_DIR / "surge_live_readiness_audit_latest.csv"
HISTORY_JSONL = LOG_DIR / "surge_live_readiness_audit_history.jsonl"

REVIEW_LABELS = {"ACTIVE_ENTRY_READY", "PROBE_READY", "WAIT_LOB", "WAIT_RECLAIM"}


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size <= 0:
        return {}
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except UnicodeDecodeError:
            continue
        except json.JSONDecodeError:
            return {}
    return {}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def _wiki_field(text: str, key: str) -> str:
    prefix = f"- {key}:"
    for line in text.splitlines():
        if line.strip().startswith(prefix):
            return line.split(":", 1)[1].strip().strip("`")
    return ""


def _loop_labels(loop_status: dict[str, Any]) -> set[str]:
    return {str(step.get("label") or "") for step in loop_status.get("steps") or []}


def _global_blockers(
    wiki_text: str,
    loop_status: dict[str, Any],
    ledger_dry_run: dict[str, Any],
) -> list[str]:
    blockers: list[str] = []
    date_status = _wiki_field(wiki_text, "date_asof_interpretation_status")
    strict_match = _wiki_field(wiki_text, "strict_asof_matches_D")
    score_match = _wiki_field(wiki_text, "score_asof_matches_expected")
    intraday_steps = _wiki_field(wiki_text, "intraday_steps")
    if date_status and date_status not in {"OK", "PASS"}:
        blockers.append(f"DATE_ASOF_STATUS:{date_status}")
    if strict_match.lower() == "false":
        blockers.append("STRICT_ASOF_NOT_MATCH_D")
    if score_match.lower() == "false":
        blockers.append("SCORE_ASOF_NOT_MATCH_EXPECTED")
    if intraday_steps and "/" in intraday_steps:
        left, right = intraday_steps.split("/", 1)
        if left.strip() != right.strip():
            blockers.append(f"INTRADAY_STEPS_NOT_ALL_OK:{intraday_steps}")
    if loop_status.get("steps_ok") != loop_status.get("steps_total"):
        blockers.append(f"LOOP_STEPS_NOT_ALL_OK:{loop_status.get('steps_ok')}/{loop_status.get('steps_total')}")
    if ledger_dry_run.get("status") == "FAIL":
        blockers.append(f"LEDGER_LIVE_FILLS_DRY_RUN_FAIL:{ledger_dry_run.get('missing_rows', '')}")
    return blockers


def _candidate_blockers(row: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    entry_decision = str(row.get("entry_decision") or "")
    label = str(row.get("active_response_label") or "")
    if entry_decision != "ENTRY_ALLOWED":
        blockers.append(f"ENTRY_NOT_ALLOWED:{entry_decision or 'unknown'}")
    if label == "WAIT_LOB":
        blockers.append("LOB_RECHECK_REQUIRED")
    elif label == "WAIT_RECLAIM":
        blockers.append("VWAP_OR_HIGH_RECLAIM_REQUIRED")
    elif label == "PROBE_READY":
        blockers.append("PROBE_REVIEW_ONLY")
    elif label == "ACTIVE_ENTRY_READY":
        blockers.append("ENTRY_ROUTE_NOT_APPROVED")
    if str(row.get("broker_order_route") or "").lower() != "true":
        blockers.append("BROKER_ROUTE_FALSE")
    if str(row.get("trading_route") or "").lower() != "true":
        blockers.append("TRADING_ROUTE_FALSE")
    if str(row.get("paper_order_route") or "").lower() != "true":
        blockers.append("PAPER_ROUTE_FALSE")
    exclude = str(row.get("exclude_reasons") or "")
    if exclude:
        blockers.append(f"EXCLUDE_REASONS:{exclude}")
    return blockers


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if len(text) == 8 and text.isdigit():
            text = f"{text[:4]}-{text[4:6]}-{text[6:8]}T00:00:00"
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        return None


def _paper_global_blockers(
    active_meta: dict[str, Any],
    active_rows: list[dict[str, str]],
    ledger_dry_run: dict[str, Any],
    *,
    max_source_age_minutes: float = 10.0,
) -> list[str]:
    blockers: list[str] = []
    if not active_rows:
        blockers.append("ACTIVE_RESPONSE_ROWS_MISSING")
    source_ts = active_meta.get("source_ts") or active_meta.get("generated_at") or ""
    parsed_ts = _parse_ts(source_ts)
    if not parsed_ts:
        blockers.append("ACTIVE_RESPONSE_SOURCE_TS_MISSING")
    elif max_source_age_minutes > 0:
        age_min = (datetime.now() - parsed_ts).total_seconds() / 60.0
        if age_min > max_source_age_minutes:
            blockers.append(f"ACTIVE_RESPONSE_STALE:{age_min:.1f}min>{max_source_age_minutes:.1f}min")
        elif age_min < -1.0:
            blockers.append(f"ACTIVE_RESPONSE_FUTURE_TS:{age_min:.1f}min")
    if ledger_dry_run.get("status") == "FAIL":
        blockers.append(f"LEDGER_LIVE_FILLS_DRY_RUN_FAIL:{ledger_dry_run.get('missing_rows', '')}")
    return blockers


def _reason_keys(text: Any) -> set[str]:
    keys: set[str] = set()
    for part in str(text or "").split("|"):
        key = part.split(":", 1)[0].strip()
        if key:
            keys.add(key)
    return keys


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _paper_probe_override_allowed(row: dict[str, Any]) -> bool:
    if str(row.get("active_response_label") or "") != "PROBE_READY":
        return False
    if str(row.get("paper_probe_allowed") or "").strip().lower() not in {"1", "true", "yes", "y", "on"}:
        return False
    if "PAPER_PROBE" not in str(row.get("paper_policy_relaxations") or ""):
        return False

    exclude_keys = _reason_keys(row.get("exclude_reasons"))
    probe_keys = _reason_keys(row.get("paper_probe_block_reasons"))
    hard_keys = {
        "KRX_WARNING",
        "TRADING_VALUE_FLOOR",
        "NO_LOB_BLOCK",
        "SPREAD_BLOCK",
        "MARKOUT_NEGATIVE_BLOCK",
        "INSUFFICIENT_FEATURES",
    }
    if exclude_keys & hard_keys:
        return False
    if "ORDER_IMBALANCE_EXTREME" in exclude_keys:
        if _to_float(row.get("order_imbalance_l1")) <= 0:
            return False
        if _to_float(row.get("markout_1step_bps")) < 0:
            return False
    if exclude_keys and not exclude_keys.issubset(probe_keys):
        return False
    return True


def _paper_candidate_blockers(row: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    label = str(row.get("active_response_label") or "")
    entry_decision = str(row.get("entry_decision") or "")
    paper_probe_override = _paper_probe_override_allowed(row)
    if label not in {"ACTIVE_ENTRY_READY", "PROBE_READY"}:
        blockers.append(f"ACTIVE_RESPONSE_NOT_READY:{label or 'unknown'}")
    if entry_decision == "ENTRY_BLOCKED":
        if not paper_probe_override:
            blockers.append("ENTRY_DECISION_BLOCKED")
    elif entry_decision and entry_decision not in {"ENTRY_ALLOWED", "WAIT_EXECUTION"}:
        blockers.append(f"ENTRY_DECISION_NOT_APPROVABLE:{entry_decision}")
    exclude = str(row.get("exclude_reasons") or "")
    if exclude and not paper_probe_override:
        blockers.append(f"EXCLUDE_REASONS:{exclude}")
    return blockers


def build() -> dict[str, Any]:
    generated_at = _now_ts()
    active_meta = _read_json(ACTIVE_LAYER_JSON)
    queue_meta = _read_json(QUEUE_JSON)
    loop_status = _read_json(LOOP_STATUS_JSON)
    ledger_dry_run = _read_json(LEDGER_DRY_RUN_JSON)
    wiki_text = WIKI_INDEX.read_text(encoding="utf-8", errors="replace") if WIKI_INDEX.exists() else ""
    active_rows = _read_csv(ACTIVE_LAYER_CSV)

    global_blockers = _global_blockers(wiki_text, loop_status, ledger_dry_run)
    paper_global_blockers = _paper_global_blockers(active_meta, active_rows, ledger_dry_run)
    labels_seen = _loop_labels(loop_status)
    loop_has_active_steps = {
        "surge_active_response_layer": "surge_active_response_layer" in labels_seen,
        "surge_active_response_queue": "surge_active_response_queue" in labels_seen,
    }

    audit_rows: list[dict[str, Any]] = []
    for row in active_rows:
        label = str(row.get("active_response_label") or "")
        if label not in REVIEW_LABELS:
            continue
        blockers = global_blockers + _candidate_blockers(row)
        paper_blockers = paper_global_blockers + _paper_candidate_blockers(row)
        paper_ready = not paper_blockers
        out = {
            "generated_at": generated_at,
            "source_ts": active_meta.get("source_ts") or "",
            "code": row.get("code", ""),
            "active_response_label": label,
            "active_response_reason": row.get("active_response_reason", ""),
            "active_response_next_check": row.get("active_response_next_check", ""),
            "entry_decision": row.get("entry_decision", ""),
            "entry_reason": row.get("entry_reason", ""),
            "change_pct": row.get("change_pct", ""),
            "rvol20": row.get("rvol20", ""),
            "trading_value": row.get("trading_value", ""),
            "lob_status": row.get("lob_status", ""),
            "exclude_reasons": row.get("exclude_reasons", ""),
            "reclaim_probe_transition": row.get("reclaim_probe_transition", ""),
            "reclaim_check_status": row.get("reclaim_check_status", ""),
            "reclaim_evidence": row.get("reclaim_evidence", ""),
            "live_trade_readiness": "BLOCKED",
            "live_trade_blockers": "|".join(blockers),
            "paper_order_readiness": "PAPER_READY" if paper_ready else "BLOCKED",
            "paper_order_blockers": "|".join(paper_blockers),
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": bool(paper_ready),
            "paper_order_route": bool(paper_ready),
            "broker_order_route": False,
            "trading_route": False,
        }
        audit_rows.append(out)

    fields = [
        "generated_at",
        "source_ts",
        "code",
        "active_response_label",
        "active_response_reason",
        "active_response_next_check",
        "entry_decision",
        "entry_reason",
        "change_pct",
        "rvol20",
        "trading_value",
        "lob_status",
        "exclude_reasons",
        "reclaim_probe_transition",
        "reclaim_check_status",
        "reclaim_evidence",
        "live_trade_readiness",
        "live_trade_blockers",
        "paper_order_readiness",
        "paper_order_blockers",
        "research_only",
        "policy_change",
        "entry_approval_changed",
        "paper_order_route",
        "broker_order_route",
        "trading_route",
    ]
    _write_csv(OUT_CSV, audit_rows, fields)

    label_counts = dict(Counter(row["active_response_label"] for row in audit_rows))
    readiness_counts = dict(Counter(row["live_trade_readiness"] for row in audit_rows))
    paper_readiness_counts = dict(Counter(row["paper_order_readiness"] for row in audit_rows))
    paper_ready_rows = [row for row in audit_rows if row.get("paper_order_readiness") == "PAPER_READY"]
    payload = {
        "status": "PASS",
        "reason": "ok",
        "generated_at": generated_at,
        "source_ts": active_meta.get("source_ts") or "",
        "scope": "read_only_surge_live_readiness_audit",
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": bool(paper_ready_rows),
        "paper_order_route": bool(paper_ready_rows),
        "broker_order_route": False,
        "trading_route": False,
        "source_files": {
            "active_layer_json": str(ACTIVE_LAYER_JSON),
            "active_layer_csv": str(ACTIVE_LAYER_CSV),
            "queue_json": str(QUEUE_JSON),
            "loop_status_json": str(LOOP_STATUS_JSON),
            "ledger_dry_run_json": str(LEDGER_DRY_RUN_JSON),
            "wiki_index": str(WIKI_INDEX),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV), "history_jsonl": str(HISTORY_JSONL)},
        "global_context": {
            "wiki_generated_at": _wiki_field(wiki_text, "generated_at"),
            "D": _wiki_field(wiki_text, "D"),
            "orders_exec_exists": _wiki_field(wiki_text, "orders_exec_exists"),
            "asof_ymd": _wiki_field(wiki_text, "asof_ymd"),
            "score_expected_ymd": _wiki_field(wiki_text, "score_expected_ymd"),
            "score_asof_matches_expected": _wiki_field(wiki_text, "score_asof_matches_expected"),
            "strict_asof_matches_D": _wiki_field(wiki_text, "strict_asof_matches_D"),
            "date_asof_interpretation_status": _wiki_field(wiki_text, "date_asof_interpretation_status"),
            "intraday_steps": _wiki_field(wiki_text, "intraday_steps"),
            "loop_steps_ok": loop_status.get("steps_ok"),
            "loop_steps_total": loop_status.get("steps_total"),
            "ledger_live_fills_dry_run_status": ledger_dry_run.get("status"),
            "ledger_live_fills_dry_run_missing_rows": ledger_dry_run.get("missing_rows"),
            "loop_has_active_response_steps": loop_has_active_steps,
        },
        "global_blockers": global_blockers,
        "paper_global_blockers": paper_global_blockers,
        "source_counts": {
            "active_response_rows": len(active_rows),
            "review_rows": len(audit_rows),
            "queue_rows": (queue_meta.get("source_counts") or {}).get("queue_rows"),
        },
        "active_response_label_counts": label_counts,
        "live_trade_readiness_counts": readiness_counts,
        "paper_order_readiness_counts": paper_readiness_counts,
        "paper_ready_codes": [row["code"] for row in paper_ready_rows],
        "probe_ready_codes": [row["code"] for row in audit_rows if row["active_response_label"] == "PROBE_READY"],
        "active_entry_ready_codes": [row["code"] for row in audit_rows if row["active_response_label"] == "ACTIVE_ENTRY_READY"],
        "wait_lob_codes": [row["code"] for row in audit_rows if row["active_response_label"] == "WAIT_LOB"],
        "wait_reclaim_codes": [row["code"] for row in audit_rows if row["active_response_label"] == "WAIT_RECLAIM"],
        "rows": audit_rows[:100],
        "access_issues": [
            "live readiness audit does not approve live broker orders",
            "paper_order_readiness is a paper-engine candidate approval signal only",
            "global STOP/date/as-of/ledger blockers are reported before candidate-level readiness",
            "PROBE_READY can only reach paper order review when paper_order_readiness=PAPER_READY",
        ],
    }
    _write_json(OUT_JSON, payload)
    _append_jsonl(HISTORY_JSONL, payload)
    return payload


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))

