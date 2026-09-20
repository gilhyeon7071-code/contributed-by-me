from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCE_CSV = LOG_DIR / "surge_recovery_reentry_candidates_latest.csv"
OUT_JSON = LOG_DIR / "surge_recovery_reentry_outcome_tracking_latest.json"
OUT_CSV = LOG_DIR / "surge_recovery_reentry_outcome_tracking_latest.csv"


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
            writer.writerow({field: row.get(field, "") for field in fields})


def _float(value: Any) -> float | None:
    text = str(value or "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _avg(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [_float(row.get(key)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 6)


def _min(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [_float(row.get(key)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round(min(clean), 6)


def _max(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [_float(row.get(key)) for row in rows]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round(max(clean), 6)


def _classify_quality(row: dict[str, Any]) -> str:
    path_signal = str(row.get("path_signal") or "")
    outcome = str(row.get("outcome_class") or "")
    adverse = _float(row.get("max_adverse_pct"))
    favorable = _float(row.get("max_favorable_pct"))
    ret_10m = _float(row.get("ret_10m_pct"))
    if outcome != "PATH_TEST_POSITIVE":
        return "NOT_POSITIVE_OUTCOME"
    if path_signal == "POSITIVE_WITH_V_REBOUND" and (adverse is None or adverse >= -0.015):
        return "POSITIVE_V_REBOUND_CONTROLLED_ADVERSE"
    if favorable is not None and favorable > 0 and ret_10m is None:
        return "EARLY_POSITIVE_INCOMPLETE_10M"
    return "POSITIVE_BUT_NEEDS_REVIEW"


def build() -> dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    source_rows = _read_csv(SOURCE_CSV)
    recovery_rows = [
        row for row in source_rows if str(row.get("candidate_class") or "") == "RECOVERY_REENTRY_CANDIDATE"
    ]
    output_rows: list[dict[str, Any]] = []
    for row in recovery_rows:
        output_rows.append(
            {
                "generated_at": generated_at,
                "code": row.get("code", ""),
                "name": row.get("name", ""),
                "candidate_class": row.get("candidate_class", ""),
                "quality_bucket": _classify_quality(row),
                "matched_conditions": row.get("matched_conditions", ""),
                "blockers": row.get("blockers", ""),
                "outcome_class": row.get("outcome_class", ""),
                "path_signal": row.get("path_signal", ""),
                "v_rebound_path_status": row.get("v_rebound_path_status", ""),
                "ret_3m_pct": row.get("ret_3m_pct", ""),
                "ret_5m_pct": row.get("ret_5m_pct", ""),
                "ret_10m_pct": row.get("ret_10m_pct", ""),
                "max_favorable_pct": row.get("max_favorable_pct", ""),
                "max_adverse_pct": row.get("max_adverse_pct", ""),
                "close_like_ret_pct": row.get("close_like_ret_pct", ""),
                "change_pct": row.get("change_pct", ""),
                "remaining_to_30pct": row.get("remaining_to_30pct", ""),
                "rvol20": row.get("rvol20", ""),
                "trading_value": row.get("trading_value", ""),
                "paper_virtual_candidate": row.get("paper_virtual_candidate", ""),
                "entry_allowed": row.get("entry_allowed", ""),
                "expectancy_buy_ready": row.get("expectancy_buy_ready", ""),
                "paper_order_route": row.get("paper_order_route", ""),
                "broker_order_route": row.get("broker_order_route", ""),
                "orders_modified": row.get("orders_modified", ""),
                "fills_modified": row.get("fills_modified", ""),
                "research_only": row.get("research_only", ""),
            }
        )

    quality_counts = Counter(str(row.get("quality_bucket") or "") for row in output_rows)
    route_flags = {
        "entry_allowed_rows": sum(1 for row in output_rows if _bool(row.get("entry_allowed"))),
        "expectancy_buy_ready_rows": sum(1 for row in output_rows if _bool(row.get("expectancy_buy_ready"))),
        "paper_order_route": any(_bool(row.get("paper_order_route")) for row in output_rows),
        "broker_order_route": any(_bool(row.get("broker_order_route")) for row in output_rows),
        "orders_modified": any(_bool(row.get("orders_modified")) for row in output_rows),
        "fills_modified": any(_bool(row.get("fills_modified")) for row in output_rows),
        "research_only": all(_bool(row.get("research_only")) for row in output_rows) if output_rows else True,
    }
    status = "OK"
    if not source_rows:
        status = "NO_SOURCE_ROWS"
    elif not output_rows:
        status = "NO_CURRENT_RECOVERY_REENTRY_CANDIDATES"

    report = {
        "generated_at": generated_at,
        "status": status,
        "scope": "surge_recovery_reentry_outcome_tracking",
        "source_files": {"recovery_reentry_candidates_csv": str(SOURCE_CSV)},
        "summary": {
            "source_rows": len(source_rows),
            "recovery_reentry_candidate_rows": len(output_rows),
            "quality_bucket_counts": dict(sorted(quality_counts.items())),
            "avg_ret_3m_pct": _avg(output_rows, "ret_3m_pct"),
            "avg_ret_5m_pct": _avg(output_rows, "ret_5m_pct"),
            "avg_ret_10m_pct": _avg(output_rows, "ret_10m_pct"),
            "avg_max_favorable_pct": _avg(output_rows, "max_favorable_pct"),
            "avg_max_adverse_pct": _avg(output_rows, "max_adverse_pct"),
            "worst_max_adverse_pct": _min(output_rows, "max_adverse_pct"),
            "best_max_favorable_pct": _max(output_rows, "max_favorable_pct"),
            **route_flags,
        },
        "interpretation": (
            "Read-only virtual outcome tracking for RECOVERY_REENTRY_CANDIDATE rows. "
            "This does not approve entries, route orders, change thresholds, or modify fills."
        ),
        "rows": output_rows,
    }
    fields = [
        "generated_at",
        "code",
        "name",
        "candidate_class",
        "quality_bucket",
        "matched_conditions",
        "blockers",
        "outcome_class",
        "path_signal",
        "v_rebound_path_status",
        "ret_3m_pct",
        "ret_5m_pct",
        "ret_10m_pct",
        "max_favorable_pct",
        "max_adverse_pct",
        "close_like_ret_pct",
        "change_pct",
        "remaining_to_30pct",
        "rvol20",
        "trading_value",
        "paper_virtual_candidate",
        "entry_allowed",
        "expectancy_buy_ready",
        "paper_order_route",
        "broker_order_route",
        "orders_modified",
        "fills_modified",
        "research_only",
    ]
    _write_csv(OUT_CSV, output_rows, fields)
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> int:
    report = build()
    print(json.dumps({"status": report.get("status"), "summary": report.get("summary")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
