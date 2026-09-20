from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

OUT_JSON = LOG_DIR / "input_publish_contract_audit_latest.json"
OUT_CSV = LOG_DIR / "input_publish_contract_audit_latest.csv"
OUT_MD = LOG_DIR / "input_publish_contract_audit_latest.md"


COMPONENTS: list[dict[str, Any]] = [
    {
        "component": "price_history_integrity",
        "priority": 1,
        "role": "price input quality contract for candidate/backtest/HPO diagnostics",
        "source_files": ["tools/price_integrity.py", "tools/build_candidate_price_history.py"],
        "artifacts": [
            "2_Logs/candidate_price_history_status_latest.json",
            "2_Logs/candidate_price_history_latest.csv",
        ],
        "required_artifact_keys": ["generated_at", "status"],
        "boundary_keys": [],
        "code_keywords": {
            "schema_validation": ["reasons.append", "negative_volume", "zero_volume_price_move"],
            "raw_evidence": ["raw_close", "raw_move_abs", "raw_evidence"],
            "volume_anomaly_gate": ["negative_volume", "zero_volume_price_move"],
            "rollback": [],
            "atomic_publish": ["_atomic_write_csv", "_atomic_write_text"],
            "provenance": ["provenance", "price_source"],
        },
    },
    {
        "component": "naver_news_collect",
        "priority": 1,
        "role": "external news API ingestion into local news DB",
        "source_files": ["tools/news_collect_naver_daily.py"],
        "artifacts": ["news_trading/data/trading.db"],
        "required_artifact_keys": [],
        "boundary_keys": [],
        "code_keywords": {
            "schema_validation": ["CREATE TABLE", "article_key", "published_at"],
            "raw_evidence": ["originallink", "description", "pubDate"],
            "volume_anomaly_gate": ["quota", "daily_used", "hourly_used"],
            "rollback": ["rollback"],
            "atomic_publish": [],
            "provenance": ["source", "originallink", "fetched_at"],
        },
    },
    {
        "component": "auto_news_implications",
        "priority": 1,
        "role": "news implication promotion with direct candidate append forbidden",
        "source_files": ["tools/auto_news_implications_daily.py"],
        "artifacts": ["2_Logs/auto_news_implications_status_latest.json"],
        "required_artifact_keys": [
            "generated_at",
            "asof_ymd",
            "quality",
            "direct_candidate_policy",
            "candidate_append",
        ],
        "boundary_keys": [
            ["direct_candidate_policy", "forbidden"],
            ["candidate_append", False],
        ],
        "code_keywords": {
            "schema_validation": ["rows_raw", "quality", "reject_counts"],
            "raw_evidence": ["rows_with_body_evidence", "content_sidecar"],
            "volume_anomaly_gate": ["limit", "rows_raw"],
            "rollback": [],
            "atomic_publish": ["_atomic_write_text"],
            "provenance": ["db", "output", "version"],
        },
    },
    {
        "component": "news_score_daily",
        "priority": 1,
        "role": "news scoring and publisher trust shadow boundary",
        "source_files": ["tools/news_score_daily.py"],
        "artifacts": ["2_Logs/news_score_status_latest.json"],
        "required_artifact_keys": [],
        "boundary_keys": [],
        "code_keywords": {
            "schema_validation": ["rows_raw", "publisher_gate_state", "missing_metrics"],
            "raw_evidence": ["source_signal_raw_id", "event_url", "event_title"],
            "volume_anomaly_gate": ["publisher_duplicate_rate", "publisher_p95_latency_ms"],
            "rollback": [],
            "atomic_publish": ["_atomic_write_text"],
            "provenance": ["_provenance_fields", "independent_source_count", "publisher_source"],
        },
    },
    {
        "component": "candidate_generation",
        "priority": 1,
        "role": "candidate generation from validated market/fundamental snapshots",
        "source_files": ["generate_candidates_v41_1.py"],
        "artifacts": [
            "2_Logs/candidates_latest_data.csv",
            "2_Logs/candidates_latest_meta.json",
            "2_Logs/candidates_latest.csv",
            "2_Logs/candidates_latest_data.with_final_score.csv",
        ],
        "required_artifact_keys": [],
        "boundary_keys": [],
        "code_keywords": {
            "schema_validation": ["mandatory_cols", "errors=\"coerce\"", "empty output"],
            "raw_evidence": ["as_of_select", "latest_raw_max", "codes_today_raw"],
            "volume_anomaly_gate": ["volume", "value"],
            "rollback": [],
            "atomic_publish": ["_atomic_write_csv", "_atomic_write_text", "_atomic_write_json"],
            "provenance": ["_src_priority", "_src_mtime", "snapshot"],
        },
    },
    {
        "component": "candidate_bridge_observation",
        "priority": 1,
        "role": "read-only candidate bridge observation before policy judgment",
        "source_files": ["tools/build_candidate_bridge_daily_status_report.py"],
        "artifacts": ["2_Logs/candidate_bridge_daily_status_latest.json"],
        "required_artifact_keys": [
            "generated_at",
            "scope",
            "policy_effect",
            "candidate_rows",
            "alert_status",
            "price_max_ymd",
        ],
        "boundary_keys": [
            ["policy_effect", "read_only_no_candidate_order_fill_ledger_stat_change"],
            ["entry_eligible_read_only_count", 0],
        ],
        "code_keywords": {
            "schema_validation": ["decision_counts", "outcome_status_counts"],
            "raw_evidence": ["source_ledger", "source_simulation"],
            "volume_anomaly_gate": ["alert_status"],
            "rollback": [],
            "atomic_publish": ["_atomic_write_text"],
            "provenance": ["source_ledger", "source_simulation"],
        },
    },
    {
        "component": "surge_ev_probe_readiness",
        "priority": 1,
        "role": "risk-budgeted surge probe readiness without order route",
        "source_files": ["tools/build_surge_ev_probe_readiness.py"],
        "artifacts": ["2_Logs/surge_ev_probe_readiness_latest.json"],
        "required_artifact_keys": ["ts", "status", "scope", "risk_contract"],
        "boundary_keys": [
            ["risk_contract.trading_allowed", False],
            ["risk_contract.research_only", True],
            ["risk_contract.must_not_dispatch", True],
            ["risk_contract.paper_order_route", False],
            ["risk_contract.broker_order_route", False],
        ],
        "code_keywords": {
            "schema_validation": ["status", "risk_contract", "sizing_contract"],
            "raw_evidence": ["source_verdict_json", "source_bucket_csv", "source_capital_json"],
            "volume_anomaly_gate": [],
            "rollback": [],
            "atomic_publish": [],
            "provenance": ["source_verdict_json", "source_bucket_csv", "source_capital_json"],
        },
    },
    {
        "component": "paper_engine_dispatch_boundary",
        "priority": 1,
        "role": "final paper entry path boundary that must remain fail-closed",
        "source_files": ["paper_engine.py"],
        "artifacts": [
            "2_Logs/p1_entry_gate_status_latest.json",
            "2_Logs/pending_entry_status_latest.json",
        ],
        "required_artifact_keys": [],
        "boundary_keys": [],
        "code_keywords": {
            "schema_validation": ["detect_schema", "schema"],
            "raw_evidence": ["snapshot", "entry_decision"],
            "volume_anomaly_gate": [],
            "rollback": [],
            "atomic_publish": [],
            "provenance": ["p0_snapshot", "gate_snapshot", "macro_snapshot"],
        },
    },
]


def _now_ts() -> str:
    return dt.datetime.now().replace(microsecond=0).isoformat()


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    for enc in ("utf-8-sig", "utf-8", "cp949", "mbcs"):
        try:
            return path.read_text(encoding=enc)
        except Exception:
            continue
    return ""


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    text = _read_text(path)
    if not text:
        return {}
    try:
        obj = json.loads(text)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _get_path(payload: dict[str, Any], dotted: str) -> Any:
    cur: Any = payload
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _artifact_snapshot(rel_path: str) -> dict[str, Any]:
    path = ROOT / rel_path
    item: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "mtime": "",
        "size_bytes": 0,
        "json_keys": [],
    }
    if not path.exists():
        return item
    stat = path.stat()
    item["mtime"] = dt.datetime.fromtimestamp(stat.st_mtime).replace(microsecond=0).isoformat()
    item["size_bytes"] = int(stat.st_size)
    if path.suffix.lower() == ".json":
        payload = _read_json(path)
        item["json_keys"] = sorted(str(k) for k in payload.keys())
    return item


def _has_all_required_keys(payload: dict[str, Any], keys: list[str]) -> bool:
    return all(_get_path(payload, key) is not None for key in keys)


def _boundary_status(payload: dict[str, Any], boundary_keys: list[list[Any]]) -> tuple[str, list[str]]:
    if not boundary_keys:
        return "NA", []
    misses: list[str] = []
    for dotted, expected in boundary_keys:
        actual = _get_path(payload, str(dotted))
        if actual != expected:
            misses.append(f"{dotted}: expected={expected!r} actual={actual!r}")
    return ("PASS" if not misses else "FAIL"), misses


def _keyword_status(source_text: str, keywords: list[str]) -> str:
    if not keywords:
        return "NA"
    return "PASS" if all(keyword in source_text for keyword in keywords) else "WARN"


def _component_row(spec: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    source_text = "\n".join(_read_text(ROOT / p) for p in spec["source_files"])
    source_exists = all((ROOT / p).exists() for p in spec["source_files"])
    artifact_items = [_artifact_snapshot(p) for p in spec["artifacts"]]
    artifact_exists = all(bool(item["exists"]) for item in artifact_items)
    primary_payload = _read_json(ROOT / spec["artifacts"][0]) if spec["artifacts"] else {}

    required_keys_status = (
        "PASS"
        if _has_all_required_keys(primary_payload, list(spec.get("required_artifact_keys") or []))
        else ("NA" if not spec.get("required_artifact_keys") else "WARN")
    )
    boundary_status, boundary_misses = _boundary_status(primary_payload, list(spec.get("boundary_keys") or []))
    keyword_statuses = {
        name: _keyword_status(source_text, list(words))
        for name, words in dict(spec.get("code_keywords") or {}).items()
    }

    warn_count = int(not source_exists) + int(not artifact_exists)
    warn_count += sum(1 for v in keyword_statuses.values() if v == "WARN")
    warn_count += int(required_keys_status == "WARN")
    fail_count = int(boundary_status == "FAIL")
    status = "FAIL" if fail_count else ("WARN" if warn_count else "PASS")

    row = {
        "priority": spec["priority"],
        "component": spec["component"],
        "status": status,
        "source_exists": source_exists,
        "artifact_exists": artifact_exists,
        "required_keys_status": required_keys_status,
        "boundary_status": boundary_status,
        "schema_validation": keyword_statuses.get("schema_validation", "NA"),
        "raw_evidence": keyword_statuses.get("raw_evidence", "NA"),
        "volume_anomaly_gate": keyword_statuses.get("volume_anomaly_gate", "NA"),
        "atomic_publish": keyword_statuses.get("atomic_publish", "NA"),
        "rollback": keyword_statuses.get("rollback", "NA"),
        "provenance": keyword_statuses.get("provenance", "NA"),
        "role": spec["role"],
        "boundary_misses": "; ".join(boundary_misses),
    }
    detail = {
        **row,
        "source_files": [str(ROOT / p) for p in spec["source_files"]],
        "artifacts": artifact_items,
        "required_artifact_keys": list(spec.get("required_artifact_keys") or []),
        "boundary_expectations": list(spec.get("boundary_keys") or []),
    }
    return row, detail


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "priority",
        "component",
        "status",
        "source_exists",
        "artifact_exists",
        "required_keys_status",
        "boundary_status",
        "schema_validation",
        "raw_evidence",
        "volume_anomaly_gate",
        "atomic_publish",
        "rollback",
        "provenance",
        "role",
        "boundary_misses",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _render_md(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines: list[str] = [
        "# Input Publish Contract Audit",
        "",
        f"- generated_at: `{s['generated_at']}`",
        f"- scope: `{s['scope']}`",
        f"- status: `{s['status']}`",
        f"- policy_effect: `{s['policy_effect']}`",
        f"- trading_effect: `{s['trading_effect']}`",
        "",
        "## Component Status",
        "",
        "| priority | component | status | boundary | schema | raw | volume | atomic | rollback | provenance |",
        "|---:|---|---|---|---|---|---|---|---|---|",
    ]
    for row in report["rows"]:
        lines.append(
            "| {priority} | {component} | {status} | {boundary_status} | {schema_validation} | "
            "{raw_evidence} | {volume_anomaly_gate} | {atomic_publish} | {rollback} | {provenance} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Priority Finding",
            "",
            "- P1 current need: consolidate external input contracts before publish.",
            "- Keep news, candidate bridge, and surge probe layers read-only unless separately approved.",
            "- Treat WARN rows as implementation gaps, not runtime failures.",
            "",
            "## No Operational Effect",
            "",
            "- No Gate, LOCK, order, fill, ledger, broker, score, or threshold change is applied.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_report() -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    for spec in COMPONENTS:
        row, detail = _component_row(spec)
        rows.append(row)
        details.append(detail)
    status_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
    status = "FAIL" if status_counts.get("FAIL") else ("WARN" if status_counts.get("WARN") else "PASS")
    return {
        "summary": {
            "generated_at": _now_ts(),
            "scope": "read_only_input_publish_contract_audit",
            "status": status,
            "status_counts": dict(sorted(status_counts.items())),
            "components": len(rows),
            "policy_effect": False,
            "trading_effect": False,
            "policy_change_applied": False,
            "orders_fills_ledger_changed": False,
        },
        "rows": rows,
        "details": details,
    }


def main() -> int:
    report = build_report()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, list(report["rows"]))
    OUT_MD.write_text(_render_md(report), encoding="utf-8")
    print(f"[FINAL] input publish contract audit -> {OUT_JSON} status={report['summary']['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
