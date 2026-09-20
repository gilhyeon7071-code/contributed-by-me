from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
DEFAULT_REQUIREMENTS = LOG_DIR / "promotion_gate_surge_stop_gap_future_evidence_requirements_20260620_123942.json"
TEMPLATE_GLOB = "future_surge_stop_gap_candidate_evidence_template_*.csv"

REQUIRED_AXIS_STATUS_COLUMNS = {
    "identity_alignment": "identity_alignment_status",
    "pre_entry_lob": "pre_entry_lob_status",
    "pre_entry_orderflow": "pre_entry_orderflow_status",
    "surge_quality": "surge_quality_status",
    "followthrough_markout": "followthrough_markout_status",
    "sizing_cap": "sizing_cap_status",
    "recurrence_sample": "recurrence_sample_status",
    "non_routing_boundary": "non_routing_boundary_status",
    "evidence_bundle": "evidence_bundle_status",
}

ROUTE_FALSE_COLUMNS = [
    "paper_order_route",
    "broker_order_route",
    "trading_allowed",
    "entry_approval_changed",
    "policy_change",
]


def _now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"json root is not an object: {path}")
    return data


def _read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [dict(row) for row in reader]
        return rows, list(reader.fieldnames or [])


def _latest_template() -> Path:
    files = sorted(LOG_DIR.glob(TEMPLATE_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"no template found: {LOG_DIR / TEMPLATE_GLOB}")
    return files[0]


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool | None:
    text = _norm(value).lower()
    if text in {"true", "1", "yes", "y", "pass", "ok"}:
        return True
    if text in {"false", "0", "no", "n", "fail"}:
        return False
    return None


def _is_pass_status(value: Any) -> bool:
    text = _norm(value).upper()
    return text in {"OK", "READY", "PRESENT"} or text == "PASS" or text.startswith("PASS_")


def _is_reference_or_template(row: dict[str, str]) -> bool:
    role = _norm(row.get("row_role")).upper()
    review = _norm(row.get("review_status")).upper()
    return role.startswith("TEMPLATE") or "REFERENCE_ONLY" in role or "REFERENCE_ONLY" in review


def _check_schema(fieldnames: list[str], req_ids: list[str]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    present = set(fieldnames)
    for req_id in req_ids:
        col = REQUIRED_AXIS_STATUS_COLUMNS.get(req_id)
        status = "PASS" if col in present else "FAIL"
        checks.append(
            {
                "name": f"required_axis_column:{req_id}",
                "status": status,
                "observed_value": col if col in present else "missing",
                "expected_value": col,
                "reason": "required axis status column present" if status == "PASS" else "required axis status column missing",
            }
        )
    for col in ROUTE_FALSE_COLUMNS + ["must_not_dispatch", "review_status", "row_role"]:
        status = "PASS" if col in present else "FAIL"
        checks.append(
            {
                "name": f"required_control_column:{col}",
                "status": status,
                "observed_value": col if col in present else "missing",
                "expected_value": col,
                "reason": "required control column present" if status == "PASS" else "required control column missing",
            }
        )
    return checks


def _validate_row(row: dict[str, str], req_ids: list[str], index: int) -> dict[str, Any]:
    failures: list[str] = []
    unknowns: list[str] = []
    passes: list[str] = []

    if _is_reference_or_template(row):
        role = _norm(row.get("row_role")) or "UNKNOWN_ROLE"
        review = _norm(row.get("review_status")) or "UNKNOWN_REVIEW"
        if role.upper().startswith("TEMPLATE"):
            unknowns.append(f"template row is not candidate evidence: {review}")
        else:
            failures.append(f"reference-only row is not promotable: {review}")

    for req_id in req_ids:
        col = REQUIRED_AXIS_STATUS_COLUMNS.get(req_id)
        value = row.get(col or "", "")
        if not col:
            failures.append(f"unsupported required axis: {req_id}")
        elif _is_pass_status(value):
            passes.append(req_id)
        elif _norm(value):
            failures.append(f"{col}={_norm(value)}")
        else:
            unknowns.append(f"{col}=MISSING")

    for col in ROUTE_FALSE_COLUMNS:
        value = _truthy(row.get(col))
        if value is False:
            passes.append(col)
        elif value is True:
            failures.append(f"{col}=true")
        else:
            unknowns.append(f"{col}=UNKNOWN")

    must_not_dispatch = _truthy(row.get("must_not_dispatch"))
    if must_not_dispatch is True:
        passes.append("must_not_dispatch")
    elif must_not_dispatch is False:
        failures.append("must_not_dispatch=false")
    else:
        unknowns.append("must_not_dispatch=UNKNOWN")

    if failures:
        status = "FAIL"
    elif unknowns:
        status = "UNKNOWN"
    else:
        status = "PASS"

    return {
        "row_index": index,
        "candidate_code": _norm(row.get("candidate_code")),
        "row_role": _norm(row.get("row_role")),
        "review_status": _norm(row.get("review_status")),
        "status": status,
        "ready_for_promotion_gate": status == "PASS",
        "pass_count": len(passes),
        "failures": failures,
        "unknowns": unknowns,
    }


def build_result(template_csv: Path, requirements_json: Path) -> dict[str, Any]:
    requirements = _read_json(requirements_json)
    rows, fieldnames = _read_csv(template_csv)
    req_ids = [str(item.get("id")) for item in requirements.get("requirements", []) if isinstance(item, dict) and item.get("id")]
    if not req_ids:
        req_ids = list(REQUIRED_AXIS_STATUS_COLUMNS)

    schema_checks = _check_schema(fieldnames, req_ids)
    row_results = [_validate_row(row, req_ids, idx + 1) for idx, row in enumerate(rows)]
    schema_failed = any(item["status"] == "FAIL" for item in schema_checks)
    row_statuses = {item["status"] for item in row_results}

    if schema_failed or "FAIL" in row_statuses:
        overall = "FAIL"
    elif "UNKNOWN" in row_statuses or not row_results:
        overall = "UNKNOWN"
    else:
        overall = "PASS"

    ready_rows = [row for row in row_results if row["ready_for_promotion_gate"]]
    return {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "scope": "readonly_future_surge_stop_gap_candidate_evidence_validator",
        "trading_effect": False,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_allowed": False,
        "must_not_dispatch": True,
        "source_paths": {
            "template_csv": str(template_csv),
            "requirements_json": str(requirements_json),
        },
        "overall_status": overall,
        "ready_candidate_count": len(ready_rows),
        "promotion_allowed": overall == "PASS" and len(ready_rows) > 0,
        "fail_closed_reason": "PASS requires schema PASS and at least one fully PASS non-routing candidate row",
        "schema_checks": schema_checks,
        "row_results": row_results,
        "untested": [
            "real order generation",
            "paper route",
            "broker route",
            "policy relaxation",
            "dashboard reflection",
        ],
    }


def _write_outputs(result: dict[str, Any], out_json: Path, out_csv: Path) -> None:
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["row_index", "candidate_code", "row_role", "review_status", "status", "ready_for_promotion_gate", "failures", "unknowns"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in result.get("row_results", []):
            writer.writerow(
                {
                    "row_index": row.get("row_index"),
                    "candidate_code": row.get("candidate_code"),
                    "row_role": row.get("row_role"),
                    "review_status": row.get("review_status"),
                    "status": row.get("status"),
                    "ready_for_promotion_gate": row.get("ready_for_promotion_gate"),
                    "failures": "|".join(row.get("failures", [])),
                    "unknowns": "|".join(row.get("unknowns", [])),
                }
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate future surge STOP_GAP evidence template rows read-only.")
    parser.add_argument("--template-csv", default="", help="Candidate evidence CSV. Defaults to newest future_surge_stop_gap template.")
    parser.add_argument("--requirements-json", default=str(DEFAULT_REQUIREMENTS))
    parser.add_argument("--out-json", default="")
    parser.add_argument("--out-csv", default="")
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--ci-fail-on-block", action="store_true")
    args = parser.parse_args()

    template_csv = Path(args.template_csv) if args.template_csv else _latest_template()
    requirements_json = Path(args.requirements_json)
    result = build_result(template_csv, requirements_json)

    stamp = _now_stamp()
    out_json = Path(args.out_json) if args.out_json else LOG_DIR / f"future_surge_stop_gap_candidate_evidence_validation_{stamp}.json"
    out_csv = Path(args.out_csv) if args.out_csv else LOG_DIR / f"future_surge_stop_gap_candidate_evidence_validation_{stamp}.csv"
    if not args.no_write:
        _write_outputs(result, out_json, out_csv)
        result["output_paths"] = {"json": str(out_json), "csv": str(out_csv)}

    print(
        "[FUTURE_SURGE_STOP_GAP_EVIDENCE] "
        f"overall={result['overall_status']} ready_candidate_count={result['ready_candidate_count']} "
        f"promotion_allowed={result['promotion_allowed']}"
    )
    if not args.no_write:
        print(f"[FUTURE_SURGE_STOP_GAP_EVIDENCE] json={out_json}")
        print(f"[FUTURE_SURGE_STOP_GAP_EVIDENCE] csv={out_csv}")

    if args.ci_fail_on_block and result["overall_status"] != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
