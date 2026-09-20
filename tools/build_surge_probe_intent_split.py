from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
SANITY_CSV = LOG_DIR / "surge_sanity_labeled_latest.csv"
QUEUE_CSV = LOG_DIR / "candidate_action_queue_latest.csv"
MARKET_RISING_CSV = LOG_DIR / "market_rising_latest.csv"

OUT_JSON = LOG_DIR / "surge_probe_intent_split_latest.json"
OUT_CSV = LOG_DIR / "surge_probe_intent_split_latest.csv"
READ_ONLY_BLOCKED_PATH_CLASS = "READ_ONLY_BLOCKED_PATH_VALIDATION"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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
        code = _code(row.get("code") or row.get("stck_shrn_iscd"))
        if code:
            out[code] = row
    return out


def _market_rank(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def _queue_state(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        if str(row.get("source") or "") != "surge_realtime":
            continue
        code = _code(row.get("code"))
        if code and str(row.get("action_state") or "") == "TRADABLE":
            out[code] = row
    return out


def _classify(row: dict[str, str], sanity: dict[str, str], queue: dict[str, str]) -> tuple[str, str, bool]:
    entry_allowed = _truthy(row.get("entry_allowed"))
    no_lob_probe = _truthy(row.get("no_lob_probe_allowed"))
    surge_type = str(row.get("surge_type") or row.get("detected_surge_type") or "").strip().upper()
    sanity_status = str(sanity.get("surge_sanity_status") or "").strip().upper()
    sanity_actionable = _truthy(sanity.get("surge_sanity_actionable"))
    queue_tradable = str(queue.get("action_state") or "").strip().upper() == "TRADABLE"
    change_pct = _float(row.get("change_pct"))
    remaining_to_limit_pct = max(0.0, 0.30 - change_pct)

    if not entry_allowed:
        lob_ok = str(row.get("lob_status") or "").strip().upper() == "OK" or _truthy(row.get("lob_available"))
        range_upper = _float(row.get("intraday_range_position_pct"), -1.0) >= 0.75
        low_rebound = _float(row.get("intraday_low_rebound_pct"), 0.0) >= 0.05
        limit_near = surge_type == "LIMIT_UP_NEAR" or change_pct >= 0.22
        if lob_ok or limit_near or range_upper or low_rebound:
            return (
                READ_ONLY_BLOCKED_PATH_CLASS,
                "detector_blocked_but_path_observation_only",
                False,
            )
        return "NOT_BUY_CANDIDATE", "detector_entry_not_allowed", False

    if no_lob_probe:
        return (
            "PATH_VALIDATION_PROBE",
            "no_lob_probe_allowed_requires_path_validation_not_expectancy_buy",
            False,
        )

    if surge_type == "LIMIT_UP_NEAR" and remaining_to_limit_pct <= 0.005:
        return (
            "PATH_VALIDATION_PROBE",
            "limit_up_near_has_little_intraday_upside_without_gap_or_lock_evidence",
            False,
        )

    if sanity_status == "PASS" and sanity_actionable and queue_tradable:
        return "EXPECTANCY_BUY_CANDIDATE", "sanity_pass_and_queue_tradable", True

    return "REVIEW_ONLY_CANDIDATE", "entry_allowed_but_expectancy_evidence_not_complete", False


def build() -> dict[str, Any]:
    surge_rows = _read_csv(SURGE_CSV)
    sanity_by_code = _by_code(_read_csv(SANITY_CSV))
    queue_by_code = _queue_state(_read_csv(QUEUE_CSV))
    market_by_code = _market_rank(_read_csv(MARKET_RISING_CSV))
    rows: list[dict[str, Any]] = []

    for row in surge_rows:
        if not _truthy(row.get("detected_surge_flag")):
            continue
        code = _code(row.get("code"))
        sanity = sanity_by_code.get(code, {})
        queue = queue_by_code.get(code, {})
        market = market_by_code.get(code, {})
        intent, reason, expectancy_buy_ready = _classify(row, sanity, queue)
        change_pct = _float(row.get("change_pct"))
        rows.append(
            {
                "ts": row.get("ts", ""),
                "date": row.get("date", ""),
                "code": code,
                "name": market.get("name", ""),
                "market_rank": market.get("rank", ""),
                "intent_class": intent,
                "intent_reason": reason,
                "expectancy_buy_ready": expectancy_buy_ready,
                "entry_allowed": _truthy(row.get("entry_allowed")),
                "no_lob_probe_allowed": _truthy(row.get("no_lob_probe_allowed")),
                "queue_action_state": queue.get("action_state", ""),
                "queue_trading_allowed": queue.get("trading_allowed", ""),
                "sanity_status": sanity.get("surge_sanity_status", ""),
                "sanity_actionable": sanity.get("surge_sanity_actionable", ""),
                "surge_type": row.get("surge_type", row.get("detected_surge_type", "")),
                "change_pct": change_pct,
                "remaining_to_30pct": round(max(0.0, 0.30 - change_pct), 6),
                "rvol20": _float(row.get("rvol20")),
                "trading_value": _float(row.get("trading_value")),
                "surge_score_final": _float(row.get("surge_score_final")),
                "lob_status": row.get("lob_status", ""),
                "lob_available": _truthy(row.get("lob_available")),
                "exclude_reasons": row.get("exclude_reasons", ""),
                "paper_policy_relaxations": row.get("paper_policy_relaxations", ""),
                "paper_order_route": False,
                "broker_order_route": False,
                "orders_modified": False,
                "fills_modified": False,
                "research_only": True,
            }
        )

    counts = Counter(str(row["intent_class"]) for row in rows)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "surge_probe_intent_split",
        "summary": {
            "rows": len(rows),
            "intent_counts": dict(sorted(counts.items())),
            "expectancy_buy_ready_rows": sum(1 for row in rows if row["expectancy_buy_ready"] is True),
            "path_validation_probe_rows": sum(1 for row in rows if row["intent_class"] == "PATH_VALIDATION_PROBE"),
            "read_only_blocked_path_validation_rows": sum(
                1 for row in rows if row["intent_class"] == READ_ONLY_BLOCKED_PATH_CLASS
            ),
            "paper_order_route": False,
            "broker_order_route": False,
            "orders_modified": False,
            "fills_modified": False,
            "research_only": True,
        },
        "source_files": {
            "surge": str(SURGE_CSV),
            "sanity": str(SANITY_CSV),
            "queue": str(QUEUE_CSV),
            "market_rising": str(MARKET_RISING_CSV),
        },
        "interpretation": (
            "This artifact separates path-validation probes from expectancy-based buy candidates. "
            "It does not approve orders or change routing."
        ),
        "rows": rows,
    }
    fields = [
        "ts",
        "date",
        "code",
        "name",
        "market_rank",
        "intent_class",
        "intent_reason",
        "expectancy_buy_ready",
        "entry_allowed",
        "no_lob_probe_allowed",
        "queue_action_state",
        "queue_trading_allowed",
        "sanity_status",
        "sanity_actionable",
        "surge_type",
        "change_pct",
        "remaining_to_30pct",
        "rvol20",
        "trading_value",
        "surge_score_final",
        "lob_status",
        "lob_available",
        "exclude_reasons",
        "paper_policy_relaxations",
        "paper_order_route",
        "broker_order_route",
        "orders_modified",
        "fills_modified",
        "research_only",
    ]
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows, fields)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
