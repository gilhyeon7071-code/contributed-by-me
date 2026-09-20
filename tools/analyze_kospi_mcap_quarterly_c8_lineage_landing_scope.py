from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    load_corporate_action_adapter_contract,
)
from tools import analyze_kospi_mcap_quarterly_c8_dart_viewer_policy_scope as policy_scope
from tools import collect_kospi_mcap_quarterly_c8_lineage_landings as collector
from tools import validate_kospi_mcap_quarterly_c8_dart_viewer_fallback as fallback


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
LINEAGE_CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_lineage_authority_validation_contract_v1.json"
)
LANDING_CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_lineage_landing_collection_contract_v1.json"
)
REPORT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/reports/"
    "c8_lineage_landing_scope_latest.json"
)


class LandingScopeError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LandingScopeError("JSON_READ_FAILED", str(path)) from exc
    if not isinstance(payload, dict):
        raise LandingScopeError("JSON_OBJECT_REQUIRED", str(path))
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


def classify_family_authority(
    *,
    receipt: str,
    current: Mapping[str, Any],
    family: Sequence[Mapping[str, Any]],
    diagnostics: Mapping[str, Mapping[str, Any]],
    allowed_event_kinds: Sequence[str] = ("REVISION", "CANCELLATION"),
    attachment_original_receipts: Sequence[str] = (),
) -> dict[str, Any]:
    attachment_originals = set(attachment_original_receipts)
    selected = sorted(
        {str(item.get("receipt_no") or "") for item in family if item.get("selected")}
    )
    originals = sorted(
        {
            str(item.get("receipt_no") or "")
            for item in family
            if not item.get("is_correction")
        }
    )
    root_candidates = []
    for candidate in originals:
        candidate_diag = diagnostics.get(candidate)
        if (
            candidate_diag
            and candidate_diag.get("stock_code") == current.get("stock_code")
            and candidate_diag.get("series") == current.get("series")
        ):
            root_candidates.append(candidate)
    root_candidates = sorted(set(root_candidates))
    reasons: list[str] = []
    if selected != [receipt]:
        reasons.append("FAMILY_SELECTED_RECEIPT_NOT_CURRENT")
    if len(originals) != 1:
        reasons.append("FAMILY_ORIGINAL_OPTION_NOT_EXACT_ONE")
    if len(root_candidates) != 1:
        reasons.append("FAMILY_ROOT_NOT_ACCEPTED_SAME_STOCK_SERIES")
    root_receipt = root_candidates[0] if len(root_candidates) == 1 else ""
    root = diagnostics.get(root_receipt) if root_receipt else None
    if root and str(root.get("receipt_date") or "") > str(current.get("receipt_date") or ""):
        reasons.append("FAMILY_ROOT_AFTER_CURRENT_RECEIPT")
    if root_receipt == receipt and receipt not in attachment_originals:
        reasons.append("NON_INITIAL_FAMILY_ROOT_IS_SELF")
    current_is_attachment_original = receipt in attachment_originals
    if (
        current.get("event_kind") not in set(allowed_event_kinds)
        and not current_is_attachment_original
    ):
        reasons.append("EVENT_KIND_NOT_AUTHORITY_ELIGIBLE")
    root_is_effective_initial = bool(
        root
        and (
            root.get("event_kind") == "INITIAL_DECISION"
            or root_receipt in attachment_originals
        )
    )
    if root and not root_is_effective_initial:
        reasons.append("FAMILY_ROOT_NOT_INITIAL_DECISION")
    if root and not root.get("contains_merger"):
        reasons.append("FAMILY_ROOT_NOT_MERGER_CONTENT")
    resolved = not reasons and root_is_effective_initial
    if resolved and current_is_attachment_original:
        classification = "DART_OFFICIAL_ATTACHMENT_ORIGINAL_INITIAL"
        event_kind_override = "INITIAL_DECISION"
    elif resolved:
        classification = "DART_OFFICIAL_FAMILY_INITIAL_DECISION"
        event_kind_override = ""
    else:
        classification = "OFFICIAL_FAMILY_UNRESOLVED"
        event_kind_override = ""
    return {
        "classification": classification,
        "authority_resolved": resolved,
        "initial_receipt": root_receipt if resolved else "",
        "event_kind_override": event_kind_override,
        "official_family_selected_receipts": selected,
        "official_family_original_receipts": originals,
        "accepted_same_stock_series_root_candidates": root_candidates,
        "root_event_kind": str(root.get("event_kind") or "") if root else "",
        "reason_codes": sorted(set(reasons)),
    }


def analyze_scope(*, root: Path = ROOT) -> dict[str, Any]:
    root = root.resolve()
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    try:
        lineage_contract = _load_json(root / LINEAGE_CONTRACT_RELATIVE)
        fallback_contract_path = fallback._resolve(
            root, lineage_contract["input"]["fallback_validation_contract_path"]
        )
        fallback_contract = fallback.load_validation_contract(root, fallback_contract_path)
        manifest, capture, status_by_receipt = fallback._validate_capture_context(
            root=root, contract=fallback_contract
        )
        _, rows_by_receipt, _list_evidence = fallback._load_list_pages(
            root=root, contract=fallback_contract, manifest=manifest
        )
        document_payloads, _, _document_evidence = fallback._load_document_payloads(
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
        landing_contract_path = root / LANDING_CONTRACT_RELATIVE
        landing_contract, _source_report, receipts, source_hash = collector._load_context(
            root, landing_contract_path
        )
        landing_paths = collector._paths(root, landing_contract)
        completed, integrity_issues = collector._verify_completed(
            root, landing_paths["pass"], receipts
        )
        if integrity_issues or completed != receipts:
            raise LandingScopeError(
                "LINEAGE_LANDING_COLLECTION_NOT_COMPLETE",
                f"completed={len(completed)}, issues={len(integrity_issues)}",
            )

        classes: Counter[str] = Counter()
        reasons: Counter[str] = Counter()
        event_counts: Counter[str] = Counter()
        rows: list[dict[str, Any]] = []
        family_by_receipt: dict[str, list[dict[str, Any]]] = {}
        journal_by_receipt: dict[str, dict[str, Any]] = {}
        attachment_originals: set[str] = set()
        for receipt in receipts:
            journal = _load_json(landing_paths["pass"] / f"{receipt}.json")
            journal_by_receipt[receipt] = journal
            family = list(journal.get("official_family_references") or [])
            family_by_receipt[receipt] = family
            current = diagnostics.get(receipt) or {}
            selected = [
                str(item.get("receipt_no") or "")
                for item in family
                if item.get("selected")
            ]
            originals = [
                str(item.get("receipt_no") or "")
                for item in family
                if not item.get("is_correction")
            ]
            report_name = "".join(str(current.get("report_name") or "").split())
            if (
                current.get("series") == "MERGER_DECISION_HISTORY"
                and current.get("event_kind") != "CANCELLATION"
                and current.get("contains_merger") is True
                and report_name.startswith(("[첨부추가]", "[첨부정정]"))
                and selected == [receipt]
                and originals == [receipt]
            ):
                attachment_originals.add(receipt)
        for receipt in receipts:
            current = diagnostics.get(receipt)
            if not current or current.get("source") != "OPENDART_PASS":
                raise LandingScopeError("TARGET_DIAGNOSTIC_INVALID", receipt)
            journal = journal_by_receipt[receipt]
            result = classify_family_authority(
                receipt=receipt,
                current=current,
                family=family_by_receipt[receipt],
                diagnostics=diagnostics,
                attachment_original_receipts=attachment_originals,
            )
            classes[result["classification"]] += 1
            event_counts[str(current["event_kind"])] += 1
            reasons.update(result["reason_codes"])
            rows.append(
                {
                    "receipt_no": receipt,
                    "stock_code": current["stock_code"],
                    "receipt_date": current["receipt_date"],
                    "event_kind": current["event_kind"],
                    "landing_sha256": journal["landing"]["sha256"],
                    **result,
                }
            )
        resolved = sum(int(row["authority_resolved"]) for row in rows)
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_OPENDART_OFFICIAL_LANDING_LINEAGE_SCOPE",
            "status": "PASS",
            "verdict": "MEASURED_OFFICIAL_FAMILY_AUTHORITY_SCOPE",
            "generated_at": generated_at,
            "selection_as_of": manifest["selection_as_of"],
            "input_lineage_report_sha256": source_hash,
            "capture_status_before": capture.get("status"),
            "capture_full_capture_complete_before": capture.get("full_capture_complete"),
            "target_receipt_count": len(receipts),
            "landing_integrity_pass": True,
            "resolved_count": resolved,
            "unresolved_count": len(rows) - resolved,
            "classification_counts": dict(sorted(classes.items())),
            "event_kind_counts": dict(sorted(event_counts.items())),
            "unresolved_reason_counts": dict(sorted(reasons.items())),
            "attachment_original_initial_count": len(attachment_originals),
            "attachment_original_initial_receipts": sorted(attachment_originals),
            "rows": rows,
            "promotion_allowed": False,
            "adapter_changed": False,
            "capture_status_modified": False,
            "downstream_opened": False,
        }
    except (LandingScopeError, OSError, ValueError, TypeError, KeyError) as exc:
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_OPENDART_OFFICIAL_LANDING_LINEAGE_SCOPE",
            "status": "FAIL",
            "verdict": "FAIL_LINEAGE_LANDING_SCOPE",
            "reason_codes": [getattr(exc, "code", type(exc).__name__)],
            "failure_detail": getattr(exc, "detail", str(exc)),
            "generated_at": generated_at,
            "promotion_allowed": False,
            "adapter_changed": False,
            "capture_status_modified": False,
            "downstream_opened": False,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze official DART landing lineage scope")
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    result = analyze_scope(root=args.root)
    output = args.root.resolve() / REPORT_RELATIVE
    _atomic_write(output, _json_bytes(result))
    print(json.dumps({key: value for key, value in result.items() if key != "rows"}, ensure_ascii=False, indent=2))
    return 0 if result.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
