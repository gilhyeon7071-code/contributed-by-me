from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    _date_after_label,
    _document_text,
    load_corporate_action_adapter_contract,
)
from tools import analyze_kospi_mcap_quarterly_c8_dart_viewer_policy_scope as policy_scope
from tools import validate_kospi_mcap_quarterly_c8_dart_viewer_fallback as fallback


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
LINEAGE_REPORT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/reports/"
    "c8_lineage_authority_validation_latest.json"
)
DATE_RESIDUAL_REPORT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/reports/c8_date_residual_scope_latest.json"
)
FALLBACK_CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_dart_viewer_fallback_validation_contract_v1.json"
)
REPORT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/reports/"
    "c8_completion_lineage_scope_latest.json"
)


class CompletionLineageError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CompletionLineageError("JSON_READ_FAILED", str(path)) from exc
    if not isinstance(payload, dict):
        raise CompletionLineageError("JSON_OBJECT_REQUIRED", str(path))
    return payload


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def match_completion_to_family(
    *,
    completion_receipt_date: str,
    actual_effective_date: str,
    family_events: Mapping[str, Sequence[Mapping[str, str]]],
    unresolved_decision_dates: Sequence[str] = (),
) -> dict[str, Any]:
    if not actual_effective_date:
        return {
            "authority_resolved": False,
            "initial_receipt": "",
            "classification": "COMPLETION_ACTUAL_DATE_MISSING",
            "candidate_initial_receipts": [],
        }
    if any(date <= completion_receipt_date for date in unresolved_decision_dates):
        return {
            "authority_resolved": False,
            "initial_receipt": "",
            "classification": "UNRESOLVED_DECISION_STATE_PRESENT",
            "candidate_initial_receipts": [],
        }
    candidates: list[str] = []
    state_rows: list[dict[str, Any]] = []
    for initial_receipt, events in sorted(family_events.items()):
        active = True
        planned_date = ""
        applied: list[str] = []
        for event in sorted(
            events, key=lambda item: (str(item["receipt_date"]), str(item["receipt_no"]))
        ):
            if str(event["receipt_date"]) > completion_receipt_date:
                continue
            applied.append(str(event["receipt_no"]))
            if event.get("event_kind") == "CANCELLATION":
                active = False
            elif event.get("event_kind") in {"INITIAL_DECISION", "REVISION"}:
                active = True
                if event.get("planned_effective_date"):
                    planned_date = str(event["planned_effective_date"])
        if active and planned_date == actual_effective_date:
            candidates.append(initial_receipt)
        state_rows.append(
            {
                "initial_receipt": initial_receipt,
                "active": active,
                "planned_effective_date": planned_date,
                "applied_receipts": applied,
            }
        )
    resolved = len(candidates) == 1
    return {
        "authority_resolved": resolved,
        "initial_receipt": candidates[0] if resolved else "",
        "classification": (
            "UNIQUE_ACTIVE_FAMILY_EXACT_EFFECTIVE_DATE"
            if resolved
            else "NO_ACTIVE_FAMILY_DATE_MATCH"
            if not candidates
            else "AMBIGUOUS_ACTIVE_FAMILY_DATE_MATCH"
        ),
        "candidate_initial_receipts": candidates,
        "family_states": state_rows,
    }


def analyze_scope(*, root: Path = ROOT) -> dict[str, Any]:
    root = root.resolve()
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        lineage = _load_json(root / LINEAGE_REPORT_RELATIVE)
        residual = _load_json(root / DATE_RESIDUAL_REPORT_RELATIVE)
        if lineage.get("status") != "PASS" or residual.get("status") != "PASS":
            raise CompletionLineageError("INPUT_ANALYSIS_NOT_PASS")
        if lineage.get("selection_as_of") != residual.get("selection_as_of"):
            raise CompletionLineageError("INPUT_AS_OF_MISMATCH")
        fallback_contract = fallback.load_validation_contract(
            root, root / FALLBACK_CONTRACT_RELATIVE
        )
        manifest, capture, status_by_receipt = fallback._validate_capture_context(
            root=root, contract=fallback_contract
        )
        _, rows_by_receipt, _ = fallback._load_list_pages(
            root=root, contract=fallback_contract, manifest=manifest
        )
        document_payloads, _, _ = fallback._load_document_payloads(
            root=root,
            contract=fallback_contract,
            manifest=manifest,
            status_by_receipt=status_by_receipt,
        )
        adapter_contract = load_corporate_action_adapter_contract(root)
        diagnostics, _ = policy_scope._document_diagnostics(
            rows_by_receipt=rows_by_receipt,
            document_payloads=document_payloads,
            status_by_receipt=status_by_receipt,
            adapter_contract=adapter_contract,
        )
        lineage_rows = {
            str(row["receipt_no"]): row for row in lineage.get("rows", [])
        }
        residual_dates = {
            str(row["receipt_no"]): str(row.get("candidate_actual_effective_date") or "")
            for row in residual.get("receipts", [])
            if row.get("classification")
            in {
                "SCHEDULE_ROW_EXACT_DATE_CANDIDATE",
                "OFFICIAL_SCHEDULE_NODE_EXACT_DATE_CANDIDATE",
            }
        }
        rules = adapter_contract["document_rules"]
        family_events_by_stock: dict[
            str, dict[str, list[dict[str, str]]]
        ] = defaultdict(lambda: defaultdict(list))
        unresolved_decision_dates_by_stock: dict[str, list[str]] = defaultdict(list)
        for receipt, diagnostic in sorted(diagnostics.items()):
            if diagnostic["series"] != "MERGER_DECISION_HISTORY":
                continue
            lineage_row = lineage_rows.get(receipt, {})
            override = str(lineage_row.get("opendart_event_kind_override") or "")
            event_kind = override or str(diagnostic["event_kind"])
            if event_kind == "INITIAL_DECISION":
                initial_receipt = receipt
            elif lineage_row.get("authority_resolved"):
                initial_receipt = str(lineage_row.get("initial_receipt") or "")
            else:
                unresolved_decision_dates_by_stock[diagnostic["stock_code"]].append(
                    diagnostic["receipt_date"]
                )
                continue
            if lineage_row.get("non_merger_attachment_noop"):
                continue
            text, text_reasons = _document_text(document_payloads[receipt])
            if text_reasons or "합병" not in text:
                continue
            planned_date = _date_after_label(
                text, rules["actual_effective_date_labels"], rules["date_patterns"]
            )
            family_events_by_stock[diagnostic["stock_code"]][initial_receipt].append(
                {
                    "receipt_no": receipt,
                    "receipt_date": diagnostic["receipt_date"],
                    "event_kind": event_kind,
                    "planned_effective_date": planned_date,
                }
            )

        rows: list[dict[str, Any]] = []
        classes: Counter[str] = Counter()
        for receipt, diagnostic in sorted(diagnostics.items()):
            if diagnostic["event_kind"] != "COMPLETION":
                continue
            actual_date = str(diagnostic.get("current_actual_date") or "") or residual_dates.get(
                receipt, ""
            )
            if not diagnostic.get("contains_merger"):
                result = {
                    "authority_resolved": False,
                    "initial_receipt": "",
                    "classification": "NON_MERGER_COMPLETION_CONTENT",
                    "candidate_initial_receipts": [],
                }
            else:
                result = match_completion_to_family(
                    completion_receipt_date=diagnostic["receipt_date"],
                    actual_effective_date=actual_date,
                    family_events=family_events_by_stock.get(diagnostic["stock_code"], {}),
                    unresolved_decision_dates=unresolved_decision_dates_by_stock.get(
                        diagnostic["stock_code"], []
                    ),
                )
            classes[result["classification"]] += 1
            rows.append(
                {
                    "receipt_no": receipt,
                    "source": diagnostic["source"],
                    "stock_code": diagnostic["stock_code"],
                    "receipt_date": diagnostic["receipt_date"],
                    "actual_effective_date": actual_date,
                    "actual_effective_date_source": (
                        "CURRENT_LABEL" if diagnostic.get("current_actual_date") else "STRUCTURED_FALLBACK"
                        if actual_date
                        else ""
                    ),
                    **result,
                }
            )
        resolved = sum(int(row["authority_resolved"]) for row in rows)
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_COMPLETION_TO_INITIAL_DECISION_LINEAGE_SCOPE",
            "status": "PASS",
            "verdict": "MEASURED_COMPLETION_LINEAGE_SCOPE",
            "generated_at": generated_at,
            "selection_as_of": manifest["selection_as_of"],
            "capture_status_before": capture.get("status"),
            "capture_full_capture_complete_before": capture.get("full_capture_complete"),
            "completion_count": len(rows),
            "resolved_count": resolved,
            "unresolved_count": len(rows) - resolved,
            "classification_counts": dict(sorted(classes.items())),
            "unresolved_decision_state_count": sum(
                len(values) for values in unresolved_decision_dates_by_stock.values()
            ),
            "rows": rows,
            "promotion_allowed": False,
            "adapter_changed": False,
            "capture_status_modified": False,
            "downstream_opened": False,
        }
    except (CompletionLineageError, OSError, ValueError, TypeError, KeyError) as exc:
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_COMPLETION_TO_INITIAL_DECISION_LINEAGE_SCOPE",
            "status": "FAIL",
            "verdict": "FAIL_COMPLETION_LINEAGE_SCOPE",
            "reason_codes": [getattr(exc, "code", type(exc).__name__)],
            "failure_detail": getattr(exc, "detail", str(exc)),
            "generated_at": generated_at,
            "promotion_allowed": False,
            "adapter_changed": False,
            "capture_status_modified": False,
            "downstream_opened": False,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze completion-to-decision lineage")
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    report = analyze_scope(root=args.root)
    _atomic_write(args.root.resolve() / REPORT_RELATIVE, _json_bytes(report))
    print(json.dumps({key: value for key, value in report.items() if key != "rows"}, ensure_ascii=False, indent=2))
    return 0 if report.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
