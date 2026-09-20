from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

INTENT_CSV = LOG_DIR / "surge_probe_intent_split_latest.csv"
OUTCOME_CSV = LOG_DIR / "surge_path_validation_outcome_latest.csv"
OUT_JSON = LOG_DIR / "surge_blocked_path_observation_review_latest.json"
OUT_CSV = LOG_DIR / "surge_blocked_path_observation_review_latest.csv"
HISTORY_CSV = LOG_DIR / "surge_blocked_path_observation_review_history.csv"
HISTORY_JSONL = LOG_DIR / "surge_blocked_path_observation_review_history.jsonl"
HISTORY_SUMMARY_JSON = LOG_DIR / "surge_blocked_path_observation_review_history_summary_latest.json"

OBSERVATION_CLASSES = {"PATH_VALIDATION_PROBE", "READ_ONLY_BLOCKED_PATH_VALIDATION"}
MIN_BLOCKER_EVALUABLE_N = 20
REVIEW_POSITIVE_WITH_V_RATE = 0.30
REVIEW_NEGATIVE_RATE_MAX = 0.25
KEEP_NEGATIVE_RATE_MIN = 0.35


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


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _append_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 0:
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                out.append(parsed)
    return out


def _source_version(*paths: Path) -> str:
    parts: list[str] = []
    for path in paths:
        try:
            stat = path.stat()
        except FileNotFoundError:
            parts.append(f"{path.name}:missing")
            continue
        parts.append(f"{path.name}:{stat.st_mtime_ns}:{stat.st_size}")
    return "|".join(parts)


def _code(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text.zfill(6)[-6:] if text else ""


def _bool(value: Any) -> bool:
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


def _blocker_key(raw: Any) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return ["NO_DETECTOR_BLOCKER"]
    keys: list[str] = []
    for part in text.split("|"):
        part = part.strip()
        if not part:
            continue
        keys.append(part.split(":", 1)[0])
    return sorted(set(keys)) or ["NO_DETECTOR_BLOCKER"]


def _path_signal(row: dict[str, str]) -> str:
    outcome = str(row.get("outcome_class") or "").strip().upper()
    v_status = str(row.get("v_rebound_path_status") or "").strip().upper()
    if outcome == "PATH_TEST_POSITIVE" and v_status == "V_REBOUND_HELD_5M":
        return "POSITIVE_WITH_V_REBOUND"
    if outcome == "PATH_TEST_POSITIVE":
        return "POSITIVE_NO_V_REBOUND"
    if outcome == "PATH_TEST_NEGATIVE" and v_status == "V_REBOUND_HELD_5M":
        return "NEGATIVE_DESPITE_V_REBOUND"
    if outcome == "PATH_TEST_NEGATIVE":
        return "NEGATIVE_NO_V_REBOUND"
    if outcome == "PATH_TEST_NEUTRAL":
        return "NEUTRAL"
    return "NOT_EVALUABLE"


def _observation_verdict(intent: dict[str, str], outcome: dict[str, str]) -> tuple[str, str]:
    intent_class = str(intent.get("intent_class") or "")
    outcome_class = str(outcome.get("outcome_class") or "")
    v_status = str(outcome.get("v_rebound_path_status") or "")
    blockers = _blocker_key(intent.get("exclude_reasons"))
    lob_status = str(intent.get("lob_status") or "").upper()

    if intent_class == "PATH_VALIDATION_PROBE":
        return "DIRECT_PROBE_ONLY", "direct_path_probe_still_not_buy_ready"
    if outcome_class == "NOT_EVALUABLE" or not outcome_class:
        return "INSUFFICIENT_HISTORY", "path_outcome_not_evaluable"
    if outcome_class == "PATH_TEST_POSITIVE" and v_status == "V_REBOUND_HELD_5M":
        if "HIGH_REJECTION_ENTRY_BLOCK" in blockers:
            return "REVIEW_HIGH_REJECTION_RULE", "blocked_by_high_rejection_but_recovered_and_held_5m"
        if "ENTRY_CHANGE_BLOCK" in blockers:
            return "REVIEW_ENTRY_CHANGE_RULE", "blocked_by_entry_change_but_recovered_and_held_5m"
        if lob_status == "OK":
            return "REVIEW_LOB_OK_BLOCKED_CASE", "lob_ok_blocked_case_showed_positive_v_rebound"
        return "OBSERVE_MORE", "positive_v_rebound_without_direct_rule_change_basis"
    if outcome_class == "PATH_TEST_POSITIVE":
        return "OBSERVE_MORE", "positive_path_without_v_rebound_confirmation"
    if outcome_class == "PATH_TEST_NEGATIVE":
        return "KEEP_BLOCKER_EVIDENCE", "blocked_case_showed_negative_path"
    return "NO_ACTION_FROM_SINGLE_ROW", "neutral_path_or_no_positive_followthrough"


def _summary_counts(rows: list[dict[str, Any]], *keys: str) -> list[dict[str, Any]]:
    counts: dict[tuple[str, ...], int] = defaultdict(int)
    for row in rows:
        counts[tuple(str(row.get(key, "")) for key in keys)] += 1
    out: list[dict[str, Any]] = []
    for values, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        rec = {key: value for key, value in zip(keys, values)}
        rec["count"] = count
        out.append(rec)
    return out


def _avg(rows: list[dict[str, Any]], key: str) -> Any:
    values: list[float] = []
    for row in rows:
        text = str(row.get(key) or "").strip()
        if not text:
            continue
        try:
            values.append(float(text))
        except ValueError:
            continue
    if not values:
        return ""
    return round(sum(values) / len(values), 6)


def _safe_rate(num: int, den: int) -> Any:
    if den <= 0:
        return ""
    return round(num / den, 6)


def _blocker_review_status(stats: dict[str, Any]) -> tuple[str, str]:
    evaluable_n = int(stats.get("evaluable_n") or 0)
    negative_rate = float(stats.get("negative_rate_evaluable") or 0.0)
    positive_v_rate = float(stats.get("positive_with_v_rate_evaluable") or 0.0)
    positive_rate = float(stats.get("positive_rate_evaluable") or 0.0)
    if evaluable_n < MIN_BLOCKER_EVALUABLE_N:
        return "SAMPLE_TOO_SMALL", f"evaluable_n<{MIN_BLOCKER_EVALUABLE_N}"
    if positive_v_rate >= REVIEW_POSITIVE_WITH_V_RATE and negative_rate <= REVIEW_NEGATIVE_RATE_MAX:
        return "REVIEW_CONDITIONAL_RELAXATION", "positive_v_rebound_rate_high_and_negative_rate_limited"
    if negative_rate >= KEEP_NEGATIVE_RATE_MIN and positive_v_rate < REVIEW_POSITIVE_WITH_V_RATE:
        return "KEEP_BLOCKER_EVIDENCE", "negative_rate_high_without_enough_positive_v_rebound"
    if positive_rate >= 0.50:
        return "OBSERVE_MORE_POSITIVE_MIXED", "positive_rate_high_but_v_or_negative_filter_not_enough"
    return "MIXED_NEEDS_MORE", "mixed_outcomes_without_policy_basis"


def _blocker_review(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        blockers = str(row.get("blockers") or "NO_DETECTOR_BLOCKER").split("|")
        for blocker in blockers:
            grouped[blocker or "NO_DETECTOR_BLOCKER"].append(row)

    out: list[dict[str, Any]] = []
    for blocker, bucket in grouped.items():
        positive = [row for row in bucket if row.get("outcome_class") == "PATH_TEST_POSITIVE"]
        negative = [row for row in bucket if row.get("outcome_class") == "PATH_TEST_NEGATIVE"]
        neutral = [row for row in bucket if row.get("outcome_class") == "PATH_TEST_NEUTRAL"]
        not_eval = [row for row in bucket if row.get("outcome_class") == "NOT_EVALUABLE"]
        positive_v = [row for row in bucket if row.get("path_signal") == "POSITIVE_WITH_V_REBOUND"]
        evaluable_n = len(positive) + len(negative) + len(neutral)
        stats: dict[str, Any] = {
            "blocker": blocker,
            "total_n": len(bucket),
            "evaluable_n": evaluable_n,
            "positive_n": len(positive),
            "positive_with_v_n": len(positive_v),
            "negative_n": len(negative),
            "neutral_n": len(neutral),
            "not_evaluable_n": len(not_eval),
            "positive_rate_evaluable": _safe_rate(len(positive), evaluable_n),
            "positive_with_v_rate_evaluable": _safe_rate(len(positive_v), evaluable_n),
            "negative_rate_evaluable": _safe_rate(len(negative), evaluable_n),
            "avg_ret_3m_pct": _avg(bucket, "ret_3m_pct"),
            "avg_ret_5m_pct": _avg(bucket, "ret_5m_pct"),
            "avg_ret_10m_pct": _avg(bucket, "ret_10m_pct"),
            "avg_max_favorable_pct": _avg(bucket, "max_favorable_pct"),
            "avg_max_adverse_pct": _avg(bucket, "max_adverse_pct"),
            "avg_close_like_ret_pct": _avg(bucket, "close_like_ret_pct"),
            "paper_order_route": False,
            "broker_order_route": False,
            "orders_modified": False,
            "fills_modified": False,
            "research_only": True,
        }
        status, reason = _blocker_review_status(stats)
        stats["review_status"] = status
        stats["review_reason"] = reason
        out.append(stats)
    return sorted(out, key=lambda row: (-int(row["total_n"]), str(row["blocker"])))


def _bucket_stats(label: str, bucket: list[dict[str, Any]]) -> dict[str, Any]:
    positive = [row for row in bucket if row.get("outcome_class") == "PATH_TEST_POSITIVE"]
    negative = [row for row in bucket if row.get("outcome_class") == "PATH_TEST_NEGATIVE"]
    neutral = [row for row in bucket if row.get("outcome_class") == "PATH_TEST_NEUTRAL"]
    not_eval = [row for row in bucket if row.get("outcome_class") == "NOT_EVALUABLE"]
    positive_v = [row for row in bucket if row.get("path_signal") == "POSITIVE_WITH_V_REBOUND"]
    evaluable_n = len(positive) + len(negative) + len(neutral)
    stats: dict[str, Any] = {
        "condition": label,
        "total_n": len(bucket),
        "evaluable_n": evaluable_n,
        "positive_n": len(positive),
        "positive_with_v_n": len(positive_v),
        "negative_n": len(negative),
        "neutral_n": len(neutral),
        "not_evaluable_n": len(not_eval),
        "positive_rate_evaluable": _safe_rate(len(positive), evaluable_n),
        "positive_with_v_rate_evaluable": _safe_rate(len(positive_v), evaluable_n),
        "negative_rate_evaluable": _safe_rate(len(negative), evaluable_n),
        "avg_ret_3m_pct": _avg(bucket, "ret_3m_pct"),
        "avg_ret_5m_pct": _avg(bucket, "ret_5m_pct"),
        "avg_ret_10m_pct": _avg(bucket, "ret_10m_pct"),
        "avg_max_favorable_pct": _avg(bucket, "max_favorable_pct"),
        "avg_max_adverse_pct": _avg(bucket, "max_adverse_pct"),
        "avg_close_like_ret_pct": _avg(bucket, "close_like_ret_pct"),
        "paper_order_route": False,
        "broker_order_route": False,
        "orders_modified": False,
        "fills_modified": False,
        "research_only": True,
    }
    status, reason = _blocker_review_status(stats)
    stats["review_status"] = status
    stats["review_reason"] = reason
    return stats


def _has_blocker(row: dict[str, Any], blocker: str) -> bool:
    return blocker in set(str(row.get("blockers") or "").split("|"))


def _exact_blockers(row: dict[str, Any], blockers: set[str]) -> bool:
    actual = {part for part in str(row.get("blockers") or "").split("|") if part}
    return actual == blockers


def _conditional_review(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    conditions: list[tuple[str, Any]] = [
        ("ENTRY_CHANGE_ONLY", lambda r: _exact_blockers(r, {"ENTRY_CHANGE_BLOCK"})),
        ("ENTRY_CHANGE_WITH_LOB_OK", lambda r: _has_blocker(r, "ENTRY_CHANGE_BLOCK") and str(r.get("lob_status")) == "OK"),
        (
            "ENTRY_CHANGE_WITH_POSITIVE_V",
            lambda r: _has_blocker(r, "ENTRY_CHANGE_BLOCK") and r.get("path_signal") == "POSITIVE_WITH_V_REBOUND",
        ),
        ("HIGH_REJECTION_ONLY", lambda r: _exact_blockers(r, {"HIGH_REJECTION_ENTRY_BLOCK"})),
        ("HIGH_REJECTION_WITH_LOB_OK", lambda r: _has_blocker(r, "HIGH_REJECTION_ENTRY_BLOCK") and str(r.get("lob_status")) == "OK"),
        (
            "HIGH_REJECTION_WITH_POSITIVE_V",
            lambda r: _has_blocker(r, "HIGH_REJECTION_ENTRY_BLOCK") and r.get("path_signal") == "POSITIVE_WITH_V_REBOUND",
        ),
        ("NO_LOB_ONLY", lambda r: _exact_blockers(r, {"NO_LOB_BLOCK"})),
        (
            "NO_LOB_WITH_POSITIVE_V",
            lambda r: _has_blocker(r, "NO_LOB_BLOCK") and r.get("path_signal") == "POSITIVE_WITH_V_REBOUND",
        ),
        ("RVOL_OVERHEAT_ONLY", lambda r: _exact_blockers(r, {"RVOL_OVERHEAT_BLOCK"})),
        ("RVOL_OVERHEAT_WITH_LOB_OK", lambda r: _has_blocker(r, "RVOL_OVERHEAT_BLOCK") and str(r.get("lob_status")) == "OK"),
        (
            "RVOL_OVERHEAT_WITH_POSITIVE_V",
            lambda r: _has_blocker(r, "RVOL_OVERHEAT_BLOCK") and r.get("path_signal") == "POSITIVE_WITH_V_REBOUND",
        ),
        ("SCORE_RVOL_OVERHEAT_ONLY", lambda r: _exact_blockers(r, {"SCORE_RVOL_OVERHEAT_BLOCK"})),
        (
            "SCORE_RVOL_OVERHEAT_WITH_POSITIVE_V",
            lambda r: _has_blocker(r, "SCORE_RVOL_OVERHEAT_BLOCK") and r.get("path_signal") == "POSITIVE_WITH_V_REBOUND",
        ),
        ("LOB_OK_BLOCKED_ANY", lambda r: str(r.get("lob_status")) == "OK" and str(r.get("blockers") or "") not in {"", "NO_DETECTOR_BLOCKER"}),
        ("NO_LOB_BLOCKED_ANY", lambda r: str(r.get("lob_status")) == "NO_LOB" and _has_blocker(r, "NO_LOB_BLOCK")),
    ]
    out = []
    for label, predicate in conditions:
        bucket = [row for row in rows if predicate(row)]
        out.append(_bucket_stats(label, bucket))
    return sorted(out, key=lambda row: (-int(row["evaluable_n"]), str(row["condition"])))


def _history_summary(history_rows: list[dict[str, Any]]) -> dict[str, Any]:
    unique: dict[str, dict[str, Any]] = {}
    for row in history_rows:
        key = str(row.get("observation_key") or "")
        if not key:
            continue
        unique[key] = row
    rows = list(unique.values())
    blocker_rows: list[dict[str, Any]] = []
    for row in rows:
        for blocker in str(row.get("blockers") or "").split("|"):
            blocker_rows.append(
                {
                    "blocker": blocker,
                    "outcome_class": row.get("outcome_class", ""),
                    "path_signal": row.get("path_signal", ""),
                    "observation_verdict": row.get("observation_verdict", ""),
                }
            )
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "surge_blocked_path_observation_history_summary",
        "source_history_jsonl": str(HISTORY_JSONL),
        "summary": {
            "raw_history_rows": len(history_rows),
            "unique_observation_rows": len(rows),
            "paper_order_route": False,
            "broker_order_route": False,
            "orders_modified": False,
            "fills_modified": False,
            "research_only": True,
        },
        "breakdowns": {
            "by_intent_outcome": _summary_counts(rows, "intent_class", "outcome_class"),
            "by_path_signal": _summary_counts(rows, "path_signal"),
            "by_observation_verdict": _summary_counts(rows, "observation_verdict", "observation_reason"),
            "by_blocker_outcome": _summary_counts(blocker_rows, "blocker", "outcome_class"),
            "by_blocker_path_signal": _summary_counts(blocker_rows, "blocker", "path_signal"),
            "by_blocker_review_status": _summary_counts(_blocker_review(rows), "review_status"),
            "blocker_review": _blocker_review(rows),
            "by_conditional_review_status": _summary_counts(_conditional_review(rows), "review_status"),
            "conditional_review": _conditional_review(rows),
        },
        "interpretation": (
            "Append-only read-only accumulation for repeated-sample review. "
            "This is not an entry approval or threshold change."
        ),
    }


def _sync_history_csv_from_jsonl(fields: list[str]) -> int:
    history_rows = _read_jsonl(HISTORY_JSONL)
    csv_rows = _read_csv(HISTORY_CSV)
    if len(csv_rows) >= len(history_rows):
        return len(csv_rows)
    _write_csv(HISTORY_CSV, history_rows, fields)
    return len(history_rows)


def build() -> dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    source_version = _source_version(INTENT_CSV, OUTCOME_CSV)
    intents = [row for row in _read_csv(INTENT_CSV) if row.get("intent_class") in OBSERVATION_CLASSES]
    outcomes = _by_code(_read_csv(OUTCOME_CSV))
    rows: list[dict[str, Any]] = []

    for intent in intents:
        code = _code(intent.get("code"))
        outcome = outcomes.get(code, {})
        blockers = _blocker_key(intent.get("exclude_reasons"))
        verdict, reason = _observation_verdict(intent, outcome)
        rows.append(
            {
                "code": code,
                "name": intent.get("name", ""),
                "intent_class": intent.get("intent_class", ""),
                "entry_allowed": _bool(intent.get("entry_allowed")),
                "expectancy_buy_ready": _bool(intent.get("expectancy_buy_ready")),
                "lob_status": intent.get("lob_status", ""),
                "blocker_count": len([b for b in blockers if b != "NO_DETECTOR_BLOCKER"]),
                "blockers": "|".join(blockers),
                "outcome_class": outcome.get("outcome_class", "NOT_EVALUATED"),
                "v_rebound_path_status": outcome.get("v_rebound_path_status", ""),
                "path_signal": _path_signal(outcome),
                "ret_3m_pct": outcome.get("ret_3m_pct", ""),
                "ret_5m_pct": outcome.get("ret_5m_pct", ""),
                "ret_10m_pct": outcome.get("ret_10m_pct", ""),
                "max_favorable_pct": outcome.get("max_favorable_pct", ""),
                "max_adverse_pct": outcome.get("max_adverse_pct", ""),
                "close_like_ret_pct": outcome.get("close_like_ret_pct", ""),
                "history_points_after_signal": outcome.get("history_points_after_signal", ""),
                "change_pct": _float(intent.get("change_pct")),
                "remaining_to_30pct": _float(intent.get("remaining_to_30pct")),
                "rvol20": _float(intent.get("rvol20")),
                "trading_value": _float(intent.get("trading_value")),
                "observation_verdict": verdict,
                "observation_reason": reason,
                "paper_order_route": False,
                "broker_order_route": False,
                "orders_modified": False,
                "fills_modified": False,
                "research_only": True,
            }
        )

    blocker_outcomes: list[dict[str, Any]] = []
    for row in rows:
        for blocker in str(row.get("blockers") or "").split("|"):
            blocker_outcomes.append(
                {
                    "blocker": blocker,
                    "outcome_class": row.get("outcome_class", ""),
                    "path_signal": row.get("path_signal", ""),
                    "lob_status": row.get("lob_status", ""),
                }
            )

    payload = {
        "generated_at": generated_at,
        "status": "OK",
        "scope": "surge_blocked_path_observation_review",
        "source_files": {
            "intent_split": str(INTENT_CSV),
            "path_outcome": str(OUTCOME_CSV),
            "source_version": source_version,
            "history_csv": str(HISTORY_CSV),
            "history_jsonl": str(HISTORY_JSONL),
            "history_summary_json": str(HISTORY_SUMMARY_JSON),
        },
        "summary": {
            "observation_rows": len(rows),
            "intent_class_counts": dict(sorted(Counter(str(row["intent_class"]) for row in rows).items())),
            "outcome_class_counts": dict(sorted(Counter(str(row["outcome_class"]) for row in rows).items())),
            "path_signal_counts": dict(sorted(Counter(str(row["path_signal"]) for row in rows).items())),
            "observation_verdict_counts": dict(
                sorted(Counter(str(row["observation_verdict"]) for row in rows).items())
            ),
            "expectancy_buy_ready_rows": sum(1 for row in rows if row["expectancy_buy_ready"] is True),
            "entry_allowed_rows": sum(1 for row in rows if row["entry_allowed"] is True),
            "paper_order_route": False,
            "broker_order_route": False,
            "orders_modified": False,
            "fills_modified": False,
            "research_only": True,
        },
        "breakdowns": {
            "by_intent_outcome": _summary_counts(rows, "intent_class", "outcome_class"),
            "by_lob_outcome": _summary_counts(rows, "lob_status", "outcome_class"),
            "by_blocker_outcome": _summary_counts(blocker_outcomes, "blocker", "outcome_class"),
            "by_blocker_path_signal": _summary_counts(blocker_outcomes, "blocker", "path_signal"),
            "by_observation_verdict": _summary_counts(rows, "observation_verdict", "observation_reason"),
        },
        "interpretation": (
            "Read-only blocker/path observation review. It is evidence for future rule review, "
            "not an entry approval or threshold change."
        ),
        "rows": rows,
    }
    fields = [
        "code",
        "name",
        "intent_class",
        "entry_allowed",
        "expectancy_buy_ready",
        "lob_status",
        "blocker_count",
        "blockers",
        "outcome_class",
        "v_rebound_path_status",
        "path_signal",
        "ret_3m_pct",
        "ret_5m_pct",
        "ret_10m_pct",
        "max_favorable_pct",
        "max_adverse_pct",
        "close_like_ret_pct",
        "history_points_after_signal",
        "change_pct",
        "remaining_to_30pct",
        "rvol20",
        "trading_value",
        "observation_verdict",
        "observation_reason",
        "paper_order_route",
        "broker_order_route",
        "orders_modified",
        "fills_modified",
        "research_only",
    ]
    _write_csv(OUT_CSV, rows, fields)
    history_rows: list[dict[str, Any]] = []
    for row in rows:
        history_row = {
            "review_generated_at": generated_at,
            "source_version": source_version,
            **row,
        }
        history_row["observation_key"] = "|".join(
            [
                source_version,
                str(row.get("code") or ""),
                str(row.get("intent_class") or ""),
                str(row.get("outcome_class") or ""),
                str(row.get("path_signal") or ""),
            ]
        )
        history_rows.append(history_row)
    _append_jsonl(HISTORY_JSONL, history_rows)
    history_fields = ["review_generated_at", "source_version", "observation_key", *fields]
    _append_csv(HISTORY_CSV, history_rows, history_fields)
    history_csv_rows = _sync_history_csv_from_jsonl(history_fields)
    history_payload = _history_summary(_read_jsonl(HISTORY_JSONL))
    HISTORY_SUMMARY_JSON.write_text(json.dumps(history_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["summary"]["raw_history_rows"] = history_payload["summary"]["raw_history_rows"]
    payload["summary"]["unique_observation_rows"] = history_payload["summary"]["unique_observation_rows"]
    payload["summary"]["history_csv_rows"] = history_csv_rows
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    payload = build()
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
