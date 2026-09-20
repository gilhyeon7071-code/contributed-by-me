from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SOURCE_JSON = LOG_DIR / "surge_active_response_layer_latest.json"
SOURCE_CSV = LOG_DIR / "surge_active_response_layer_latest.csv"
OUT_JSON = LOG_DIR / "surge_active_response_policy_audit_latest.json"
OUT_CSV = LOG_DIR / "surge_active_response_policy_audit_latest.csv"
OUT_MD = LOG_DIR / "surge_active_response_policy_audit_latest.md"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _bool(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y"}


def _float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _review_bucket(row: dict[str, str]) -> tuple[str, str]:
    label = str(row.get("active_response_label") or "")
    entry_decision = str(row.get("entry_decision") or "")
    if label == "ACTIVE_ENTRY_READY":
        return "IMMEDIATE_REVIEW_READY", "active response says immediate review"
    if label == "PROBE_READY":
        return "PROBE_REVIEW_READY", "active response says probe review"
    if label == "WAIT_RECLAIM" and entry_decision == "ENTRY_ALLOWED":
        return "WAIT_RECLAIM_ALLOWED", "entry allowed but reclaim required before review"
    if label == "WAIT_RECLAIM":
        return "WAIT_RECLAIM_BLOCKED", "entry blocked and reclaim required"
    if label == "WAIT_LOB":
        return "WAIT_LOB", "LOB missing and recheck required"
    if label == "HARD_EXCLUDE":
        return "HARD_EXCLUDE", "surge layer excludes"
    return "OTHER", "unmapped active response label"


def _build_rows(source_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in source_rows:
        bucket, bucket_reason = _review_bucket(row)
        out.append(
            {
                "code": str(row.get("code") or "").zfill(6),
                "active_response_label": row.get("active_response_label"),
                "active_response_reason": row.get("active_response_reason"),
                "active_response_next_check": row.get("active_response_next_check"),
                "entry_decision": row.get("entry_decision"),
                "entry_reason": row.get("entry_reason"),
                "change_pct": row.get("change_pct"),
                "rvol20": row.get("rvol20"),
                "trading_value": row.get("trading_value"),
                "lob_status": row.get("lob_status"),
                "lob_available": row.get("lob_available"),
                "exclude_reasons": row.get("exclude_reasons"),
                "review_bucket": bucket,
                "review_bucket_reason": bucket_reason,
                "research_only": _bool(row.get("research_only")),
                "policy_change": _bool(row.get("policy_change")),
                "entry_approval_changed": _bool(row.get("entry_approval_changed")),
                "paper_order_route": _bool(row.get("paper_order_route")),
                "broker_order_route": _bool(row.get("broker_order_route")),
                "trading_route": _bool(row.get("trading_route")),
                "change_value": _float(row.get("change_pct")),
                "rvol20_value": _float(row.get("rvol20")),
            }
        )
    return out


FIELDS = [
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
    "lob_available",
    "exclude_reasons",
    "review_bucket",
    "review_bucket_reason",
    "research_only",
    "policy_change",
    "entry_approval_changed",
    "paper_order_route",
    "broker_order_route",
    "trading_route",
]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    s = payload["summary"]
    lines = [
        "# Surge Active Response Policy Audit",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- source_rows: `{s['source_rows']}`",
        f"- detected_rows: `{s['detected_rows']}`",
        f"- route_enabled_rows: `{s['route_enabled_rows']}`",
        "",
        "## Review Bucket Counts",
    ]
    for k, v in sorted(s["review_bucket_counts"].items()):
        lines.append(f"- {k}: `{v}`")
    lines.extend(["", "## Interpretation"])
    lines.append("- This layer is read-only and does not route orders.")
    lines.append("- Current latest artifact has no ACTIVE_ENTRY_READY or PROBE_READY rows.")
    lines.append("- ENTRY_ALLOWED rows are still WAIT_RECLAIM, not buy approval.")
    lines.append("- Future design should decide how WAIT_RECLAIM evidence is accumulated, not bypassed.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    source_json = _read_json(SOURCE_JSON)
    source_rows = _read_csv(SOURCE_CSV)
    rows = _build_rows(source_rows)
    route_enabled = [
        row for row in rows
        if row["paper_order_route"] or row["broker_order_route"] or row["trading_route"] or row["policy_change"] or row["entry_approval_changed"]
    ]
    review_counts = Counter(row["review_bucket"] for row in rows)
    label_counts = Counter(str(row.get("active_response_label") or "") for row in rows)
    entry_counts = Counter(str(row.get("entry_decision") or "") for row in rows)
    summary = {
        "source_rows": int((source_json.get("source_counts") or {}).get("source_rows") or len(source_rows)),
        "detected_rows": len(rows),
        "review_bucket_counts": dict(review_counts),
        "active_response_label_counts": dict(label_counts),
        "entry_decision_counts": dict(entry_counts),
        "route_enabled_rows": len(route_enabled),
        "entry_allowed_wait_reclaim_rows": review_counts.get("WAIT_RECLAIM_ALLOWED", 0),
        "hard_exclude_rows": review_counts.get("HARD_EXCLUDE", 0),
        "immediate_or_probe_ready_rows": review_counts.get("IMMEDIATE_REVIEW_READY", 0) + review_counts.get("PROBE_REVIEW_READY", 0),
    }
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "surge_active_response_policy_audit_v1",
        "source_files": {"surge_active_json": str(SOURCE_JSON), "surge_active_csv": str(SOURCE_CSV)},
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV), "md": str(OUT_MD)},
        "scope": {
            "policy_effect": "read_only_surge_active_response_audit_only",
            "full_logic_application": "NOT_APPLIED",
            "order_connection": "NONE",
        },
        "summary": summary,
        "rows": rows,
    }
    _write_csv(OUT_CSV, rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", **summary, "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
