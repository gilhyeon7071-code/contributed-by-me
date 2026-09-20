from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

HISTORY_CSV = LOG_DIR / "promotion_observe_history.csv"
OUT_JSON = LOG_DIR / "promotion_observe_history_writer_latest.json"
OUT_MD = LOG_DIR / "promotion_observe_history_writer_latest.md"

FIELDNAMES = [
    "observed_at",
    "sample_family",
    "unique_key",
    "code",
    "source_state",
    "output_state",
    "evidence_status",
    "source_file",
    "source_generated_at",
    "details_json",
    "policy_change_applied",
    "trading_logic_changed",
    "order_connection",
]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _safe(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _history_rows() -> list[dict[str, str]]:
    return _read_csv(HISTORY_CSV)


def _existing_keys(rows: list[dict[str, str]]) -> set[str]:
    return {row.get("unique_key", "") for row in rows if row.get("unique_key")}


def _event(
    *,
    observed_at: str,
    sample_family: str,
    unique_key: str,
    code: str = "",
    source_state: str,
    output_state: str,
    evidence_status: str,
    source_file: str,
    source_generated_at: str = "",
    details: dict[str, Any] | None = None,
) -> dict[str, str]:
    return {
        "observed_at": observed_at,
        "sample_family": sample_family,
        "unique_key": unique_key,
        "code": code,
        "source_state": source_state,
        "output_state": output_state,
        "evidence_status": evidence_status,
        "source_file": source_file,
        "source_generated_at": source_generated_at,
        "details_json": json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
        "policy_change_applied": "False",
        "trading_logic_changed": "False",
        "order_connection": "NONE",
    }


def _collect_defense_reclaim(now: str) -> list[dict[str, str]]:
    path = LOG_DIR / "reclaim_observe_sampler_history.csv"
    rows = _read_csv(path)
    events: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        code = _safe(row.get("code"))
        status = _safe(row.get("checklist_status"))
        source_class = _safe(row.get("source_review_class"))
        observed_date = _safe(row.get("sampled_at"))[:10]
        if not code or not status:
            continue
        unique_key = f"defense_reclaim|{observed_date}|{code}|{source_class}|{status}"
        if unique_key in seen:
            continue
        seen.add(unique_key)
        events.append(
            _event(
                observed_at=now,
                sample_family="defense_reclaim",
                unique_key=unique_key,
                code=code,
                source_state=status,
                output_state="OBSERVE_HISTORY_ONLY",
                evidence_status="OBSERVED",
                source_file=str(path),
                details={
                    "source_review_class": source_class,
                    "checklist_stage": row.get("checklist_stage", ""),
                    "defense_signal_score": row.get("defense_signal_score", ""),
                    "surge_active_response_label": row.get("surge_active_response_label", ""),
                    "order_connection": row.get("order_connection", ""),
                },
            )
        )
    return events


def _collect_surge(now: str) -> list[dict[str, str]]:
    path = LOG_DIR / "surge_active_response_policy_audit_latest.json"
    payload = _read_json(path)
    source_generated_at = _safe(payload.get("generated_at"))
    events: list[dict[str, str]] = []
    for row in payload.get("rows", []):
        code = _safe(row.get("code"))
        review_bucket = _safe(row.get("review_bucket"))
        label = _safe(row.get("active_response_label"))
        entry = _safe(row.get("entry_decision"))
        if not code or not review_bucket:
            continue
        family = "surge_wait_reclaim" if review_bucket == "WAIT_RECLAIM_ALLOWED" else "surge_blocked_transition"
        if review_bucket == "HARD_EXCLUDE":
            family = "surge_hard_exclude"
        unique_key = f"{family}|{source_generated_at[:10]}|{code}|{label}|{entry}|{review_bucket}"
        events.append(
            _event(
                observed_at=now,
                sample_family=family,
                unique_key=unique_key,
                code=code,
                source_state=review_bucket,
                output_state="OBSERVE_HISTORY_ONLY",
                evidence_status="OBSERVED",
                source_file=str(path),
                source_generated_at=source_generated_at,
                details={
                    "active_response_reason": row.get("active_response_reason", ""),
                    "entry_reason": row.get("entry_reason", ""),
                    "change_pct": row.get("change_pct", ""),
                    "rvol20": row.get("rvol20", ""),
                    "lob_status": row.get("lob_status", ""),
                    "exclude_reasons": row.get("exclude_reasons", ""),
                },
            )
        )
    return events


def _collect_risk(now: str) -> list[dict[str, str]]:
    path = LOG_DIR / "production_risk_policy_audit_latest.json"
    payload = _read_json(path)
    source_generated_at = _safe(payload.get("generated_at"))
    events: list[dict[str, str]] = []
    for row in payload.get("signal_rows", []):
        name = _safe(row.get("name"))
        source = _safe(row.get("source"))
        effect = _safe(row.get("effect"))
        if not name:
            continue
        unique_key = f"risk_measurement|{source_generated_at[:10]}|{name}|{source}|{effect}"
        events.append(
            _event(
                observed_at=now,
                sample_family="risk_measurement",
                unique_key=unique_key,
                source_state=effect,
                output_state="OBSERVE_HISTORY_ONLY",
                evidence_status="OBSERVED" if row.get("sample_ok") else "EVIDENCE_GAP",
                source_file=str(path),
                source_generated_at=source_generated_at,
                details={
                    "stage": row.get("stage", ""),
                    "advisory": row.get("advisory", ""),
                    "sample_ok": row.get("sample_ok", ""),
                    "reason": row.get("reason", ""),
                    "source": source,
                },
            )
        )
    return events


def _write_history(rows: list[dict[str, str]]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with HISTORY_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def build() -> dict[str, Any]:
    now = datetime.now().isoformat(timespec="seconds")
    before_rows = _history_rows()
    before_keys = _existing_keys(before_rows)

    candidates = []
    candidates.extend(_collect_defense_reclaim(now))
    candidates.extend(_collect_surge(now))
    candidates.extend(_collect_risk(now))

    new_rows: list[dict[str, str]] = []
    skipped_duplicates = 0
    seen_this_run: set[str] = set()
    for row in candidates:
        key = row["unique_key"]
        if key in before_keys or key in seen_this_run:
            skipped_duplicates += 1
            continue
        seen_this_run.add(key)
        new_rows.append(row)

    all_rows = before_rows + new_rows
    _write_history(all_rows)

    family_counts: dict[str, int] = {}
    for row in all_rows:
        family = row.get("sample_family", "")
        family_counts[family] = family_counts.get(family, 0) + 1

    summary = {
        "history_rows_before": len(before_rows),
        "candidate_rows": len(candidates),
        "new_rows_appended": len(new_rows),
        "duplicate_rows_skipped": skipped_duplicates,
        "history_rows_after": len(all_rows),
        "family_counts_after": family_counts,
        "policy_change_applied_rows": 0,
        "trading_logic_changed_rows": 0,
        "order_connected_rows": 0,
    }

    payload = {
        "generated_at": now,
        "status": "PASS",
        "schema_version": "promotion_observe_history_writer_v1",
        "source_files": {
            "reclaim_observe_sampler_history": str(LOG_DIR / "reclaim_observe_sampler_history.csv"),
            "surge_active_response_policy_audit": str(LOG_DIR / "surge_active_response_policy_audit_latest.json"),
            "production_risk_policy_audit": str(LOG_DIR / "production_risk_policy_audit_latest.json"),
        },
        "outputs": {
            "history_csv": str(HISTORY_CSV),
            "json": str(OUT_JSON),
            "md": str(OUT_MD),
        },
        "scope": {
            "policy_effect": "observe_only_history_writer",
            "full_logic_application": "NOT_APPLIED",
            "order_connection": "NONE",
        },
        "summary": summary,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_md(payload)
    return payload


def _write_md(payload: dict[str, Any]) -> None:
    summary = payload["summary"]
    lines = [
        "# Promotion Observe History Writer",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- policy_effect: {payload['scope']['policy_effect']}",
        f"- full_logic_application: {payload['scope']['full_logic_application']}",
        f"- order_connection: {payload['scope']['order_connection']}",
        f"- history_rows_before: {summary['history_rows_before']}",
        f"- candidate_rows: {summary['candidate_rows']}",
        f"- new_rows_appended: {summary['new_rows_appended']}",
        f"- duplicate_rows_skipped: {summary['duplicate_rows_skipped']}",
        f"- history_rows_after: {summary['history_rows_after']}",
        f"- policy_change_applied_rows: {summary['policy_change_applied_rows']}",
        f"- trading_logic_changed_rows: {summary['trading_logic_changed_rows']}",
        f"- order_connected_rows: {summary['order_connected_rows']}",
        "",
        "## Family Counts",
    ]
    for family, count in sorted(summary["family_counts_after"].items()):
        lines.append(f"- {family}: {count}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    payload = build()
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
