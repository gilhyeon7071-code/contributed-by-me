from __future__ import annotations

import argparse
import hashlib
import html
from html.parser import HTMLParser
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from paper.strategies.kospi_mcap_quarterly_v1.src.c8_corporate_action_adapter import (
    _decode,
    _document_text,
    _extract_package_members,
    load_corporate_action_adapter_contract,
)
from paper.strategies.kospi_mcap_quarterly_v1.src.contracts import ContractError, normalize_ymd
from tools import analyze_kospi_mcap_quarterly_c8_dart_viewer_policy_scope as policy_scope
from tools import collect_kospi_mcap_quarterly_c8_dart_viewer_shadow as shadow
from tools import validate_kospi_mcap_quarterly_c8_dart_viewer_fallback as fallback


STRATEGY_ID = "KOSPI_MCAP_QUARTERLY_V1"
CONTRACT_RELATIVE = Path(
    "paper/strategies/kospi_mcap_quarterly_v1/config/"
    "c8_date_residual_scope_contract_v1.json"
)
REPORT_DIR_RELATIVE = Path("paper/strategies/kospi_mcap_quarterly_v1/reports")
MEMBER_ELE_ID_PATTERN = re.compile(r"^[0-9]+_[0-9]+_(?P<ele_id>[0-9]+)\.[^.]+$")


class ResidualScopeError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


class _TableRowParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        tag = tag.lower()
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []
        elif tag == "br" and self._cell is not None:
            self._cell.append(" ")

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._row is not None and self._cell is not None:
            self._row.append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None
            self._cell = None


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResidualScopeError("JSON_READ_FAILED", str(path)) from exc
    if not isinstance(payload, dict):
        raise ResidualScopeError("JSON_OBJECT_REQUIRED", str(path))
    return payload


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def load_residual_scope_contract(
    root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    path = contract_path or root / CONTRACT_RELATIVE
    contract = _load_json(path)
    if contract.get("strategy_id") != STRATEGY_ID or contract.get("mode") != "VALIDATION_ONLY":
        raise ResidualScopeError("RESIDUAL_SCOPE_CONTRACT_INVALID")
    permissions = contract.get("execution_permissions")
    if not isinstance(permissions, Mapping) or any(value is not False for value in permissions.values()):
        raise ResidualScopeError("RESIDUAL_SCOPE_PERMISSIONS_INVALID")
    return contract


def parse_table_rows(payload: bytes) -> list[list[str]]:
    parser = _TableRowParser()
    parser.feed(_decode(payload))
    parser.close()
    return parser.rows


def _normalized(value: object) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def _full_dates(values: Iterable[str], patterns: Iterable[str]) -> list[str]:
    dates: set[str] = set()
    for value in values:
        for pattern in patterns:
            for match in re.finditer(pattern, value):
                try:
                    dates.add(
                        normalize_ymd(
                            f"{int(match.group(1)):04d}{int(match.group(2)):02d}"
                            f"{int(match.group(3)):02d}"
                        )
                    )
                except (ContractError, IndexError, ValueError):
                    continue
    return sorted(dates)


def _partial_dates(values: Iterable[str], pattern: str) -> list[str]:
    partials: set[str] = set()
    for value in values:
        for match in re.finditer(pattern, value):
            month = int(match.group(1))
            day = int(match.group(2))
            if 1 <= month <= 12 and 1 <= day <= 31:
                partials.add(f"{month:02d}-{day:02d}")
    return sorted(partials)


def extract_schedule_row_evidence(
    payload: bytes,
    *,
    label_prefix: str,
    date_patterns: Iterable[str],
    partial_date_pattern: str,
) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for member_name, member_payload in _extract_package_members(payload):
        match = MEMBER_ELE_ID_PATTERN.fullmatch(member_name)
        ele_id = match.group("ele_id") if match else ""
        for cells in parse_table_rows(member_payload):
            label_index = next(
                (
                    index
                    for index, cell in enumerate(cells)
                    if _normalized(cell).startswith(label_prefix)
                ),
                -1,
            )
            if label_index < 0:
                continue
            values = cells[label_index + 1 :]
            evidence.append(
                {
                    "member_name": member_name,
                    "member_ele_id": ele_id,
                    "cells": cells,
                    "full_dates": _full_dates(values, date_patterns),
                    "partial_dates": _partial_dates(values, partial_date_pattern),
                }
            )
    return evidence


def parse_viewer_node_titles(payload: bytes) -> dict[str, str]:
    text = html.unescape(shadow._decode_html(payload))
    titles: dict[str, str] = {}
    for block in shadow.NODE_BLOCK_PATTERN.finditer(text):
        name = re.escape(block.group("name"))
        body = block.group("body")
        ele_id_match = re.search(
            rf"{name}\[['\"]eleId['\"]\]\s*=\s*['\"](?P<value>[0-9]+)['\"]",
            body,
            re.IGNORECASE,
        )
        title_match = re.search(
            rf"{name}\[['\"]text['\"]\]\s*=\s*['\"](?P<value>[^'\"]+)['\"]",
            body,
            re.IGNORECASE,
        )
        if ele_id_match and title_match:
            titles[ele_id_match.group("value")] = " ".join(
                html.unescape(title_match.group("value")).split()
            )
    return titles


def _viewer_schedule_ele_ids(
    *,
    root: Path,
    shadow_dir: Path,
    receipt: str,
    required_title: str,
) -> tuple[list[str], str]:
    journal = _load_json(shadow_dir / f"{receipt}.json")
    if journal.get("status") != "SHADOW_PASS" or journal.get("receipt_no") != receipt:
        raise ResidualScopeError("SHADOW_JOURNAL_IDENTITY_INVALID", receipt)
    landing = journal.get("landing")
    if not isinstance(landing, Mapping):
        raise ResidualScopeError("SHADOW_LANDING_MISSING", receipt)
    landing_path = _resolve(root, str(landing.get("path") or ""))
    payload = landing_path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if digest != landing.get("sha256"):
        raise ResidualScopeError("SHADOW_LANDING_HASH_MISMATCH", receipt)
    titles = parse_viewer_node_titles(payload)
    schedule_ids = sorted(
        ele_id for ele_id, title in titles.items() if _normalized(title) == required_title
    )
    return schedule_ids, digest


def classify_residual(
    *,
    source: str,
    schedule_row_dates: list[str],
    schedule_node_dates: list[str],
    partial_dates: list[str],
    non_merger_content: bool,
) -> tuple[str, str, str]:
    if source == "VIEWER_FALLBACK" and len(schedule_node_dates) == 1:
        return (
            "OFFICIAL_SCHEDULE_NODE_EXACT_DATE_CANDIDATE",
            schedule_node_dates[0],
            "DART_VIEWER_TREE_SCHEDULE_NODE_TABLE_ROW",
        )
    if len(schedule_row_dates) == 1:
        return (
            "SCHEDULE_ROW_EXACT_DATE_CANDIDATE",
            schedule_row_dates[0],
            "DART_COMPLETION_SCHEDULE_TABLE_ROW",
        )
    if partial_dates and not schedule_row_dates:
        return "PARTIAL_DATE_YEAR_REQUIRED_BLOCKED", "", ""
    if non_merger_content and not schedule_row_dates:
        return "NON_MERGER_CONTENT_BLOCKED", "", ""
    return "AMBIGUOUS_OR_UNSUPPORTED_BLOCKED", "", ""


def analyze_date_residuals(
    *, root: Path = ROOT, contract_path: Path | None = None
) -> dict[str, Any]:
    generated_at = datetime.now().astimezone().isoformat()
    try:
        root = root.resolve()
        contract = load_residual_scope_contract(root, contract_path)
        fallback_contract = fallback.load_validation_contract(
            root, _resolve(root, contract["fallback_validation_contract_path"])
        )
        manifest, capture, status_by_receipt = fallback._validate_capture_context(
            root=root, contract=fallback_contract
        )
        if capture.get("status") != contract["input"]["required_capture_status"]:
            raise ResidualScopeError("CAPTURE_STATUS_CHANGED")
        if manifest.get("full_capture_complete") is not contract["input"][
            "required_full_capture_complete"
        ]:
            raise ResidualScopeError("FULL_CAPTURE_STATE_CHANGED")
        _, rows_by_receipt, list_evidence = fallback._load_list_pages(
            root=root, contract=fallback_contract, manifest=manifest
        )
        document_payloads, _, document_evidence = fallback._load_document_payloads(
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
        residual_receipts = sorted(
            receipt
            for receipt, item in diagnostics.items()
            if item["event_kind"] == "COMPLETION" and not item["current_actual_date"]
        )
        expected_count = int(contract["input"]["required_residual_completion_count"])
        if len(residual_receipts) != expected_count:
            raise ResidualScopeError(
                "RESIDUAL_RECEIPT_COUNT_MISMATCH",
                f"expected={expected_count},actual={len(residual_receipts)}",
            )

        rules = contract["authority_rules"]
        adapter_rules = adapter_contract["document_rules"]
        shadow_dir = _resolve(
            root, fallback_contract["input"]["shadow_pass_journal_dir"]
        )
        classification_counts: Counter[str] = Counter()
        results: list[dict[str, Any]] = []
        for receipt in residual_receipts:
            payload = document_payloads[receipt]
            diagnostic = diagnostics[receipt]
            row_evidence = extract_schedule_row_evidence(
                payload,
                label_prefix=rules["schedule_row_label_normalized_prefix"],
                date_patterns=adapter_rules["date_patterns"],
                partial_date_pattern=rules["partial_date_pattern"],
            )
            schedule_row_dates = sorted(
                {date for item in row_evidence for date in item["full_dates"]}
            )
            partial_dates = sorted(
                {date for item in row_evidence for date in item["partial_dates"]}
            )
            schedule_ele_ids: list[str] = []
            landing_sha256 = ""
            if diagnostic["source"] == "VIEWER_FALLBACK":
                schedule_ele_ids, landing_sha256 = _viewer_schedule_ele_ids(
                    root=root,
                    shadow_dir=shadow_dir,
                    receipt=receipt,
                    required_title=rules["viewer_schedule_node_title_normalized"],
                )
            schedule_node_dates = sorted(
                {
                    date
                    for item in row_evidence
                    if item["member_ele_id"] in schedule_ele_ids
                    for date in item["full_dates"]
                }
            )
            text, _ = _document_text(payload)
            non_merger_content = any(
                marker in text for marker in rules["non_merger_content_markers"]
            )
            classification, candidate_date, authority = classify_residual(
                source=diagnostic["source"],
                schedule_row_dates=schedule_row_dates,
                schedule_node_dates=schedule_node_dates,
                partial_dates=partial_dates,
                non_merger_content=non_merger_content,
            )
            classification_counts[classification] += 1
            results.append(
                {
                    "receipt_no": receipt,
                    "stock_code": diagnostic["stock_code"],
                    "source": diagnostic["source"],
                    "report_name": diagnostic["report_name"],
                    "receipt_date": diagnostic["receipt_date"],
                    "document_sha256": hashlib.sha256(payload).hexdigest(),
                    "schedule_row_dates": schedule_row_dates,
                    "schedule_node_ele_ids": schedule_ele_ids,
                    "schedule_node_dates": schedule_node_dates,
                    "partial_dates": partial_dates,
                    "non_merger_content": non_merger_content,
                    "landing_sha256": landing_sha256,
                    "schedule_row_evidence": row_evidence,
                    "classification": classification,
                    "candidate_actual_effective_date": candidate_date,
                    "candidate_authority": authority,
                    "promotion_allowed": False,
                }
            )
        candidate_classes = {
            "SCHEDULE_ROW_EXACT_DATE_CANDIDATE",
            "OFFICIAL_SCHEDULE_NODE_EXACT_DATE_CANDIDATE",
        }
        candidate_count = sum(
            count
            for name, count in classification_counts.items()
            if name in candidate_classes
        )
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_COMPLETION_DATE_RESIDUAL_OFFICIAL_STRUCTURE_SCOPE",
            "status": "PASS",
            "verdict": "RESIDUAL_SCOPE_ANALYSIS_COMPLETE_ADAPTER_CHANGE_NOT_ALLOWED",
            "generated_at": generated_at,
            "selection_as_of": manifest.get("selection_as_of"),
            "capture_status_before": capture.get("status"),
            "capture_full_capture_complete_before": capture.get("full_capture_complete"),
            "input_integrity": {**list_evidence, **document_evidence},
            "residual_receipt_count": len(results),
            "classification_counts": dict(sorted(classification_counts.items())),
            "exact_date_candidate_count": candidate_count,
            "still_blocked_count": len(results) - candidate_count,
            "receipts": results,
            "adapter_change_allowed": False,
            "capture_status_changed": False,
            "network_requests": 0,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
        }
    except (ResidualScopeError, fallback.FallbackValidationError) as exc:
        return {
            "strategy_id": STRATEGY_ID,
            "scope": "C8_COMPLETION_DATE_RESIDUAL_OFFICIAL_STRUCTURE_SCOPE",
            "status": "FAIL",
            "verdict": "FAIL_RESIDUAL_SCOPE_ANALYSIS",
            "reason_codes": [getattr(exc, "code", "RESIDUAL_SCOPE_ANALYSIS_FAILED")],
            "failure_detail": getattr(exc, "detail", str(exc)),
            "generated_at": generated_at,
            "adapter_change_allowed": False,
            "capture_status_changed": False,
            "network_requests": 0,
            "canonical_publish_allowed": False,
            "m2_allowed": False,
            "candidate_selection_allowed": False,
            "orders_allowed": False,
            "operational_change": False,
        }


def write_residual_scope_report(
    report: Mapping[str, Any], *, root: Path = ROOT, contract_path: Path | None = None
) -> tuple[Path, Path]:
    contract = load_residual_scope_contract(root.resolve(), contract_path)
    generated_at = str(report.get("generated_at") or "")
    stamp = "".join(character for character in generated_at if character.isdigit())[:14]
    if len(stamp) != 14:
        raise ResidualScopeError("REPORT_TIMESTAMP_INVALID")
    report_dir = root.resolve() / REPORT_DIR_RELATIVE
    output = contract["output"]
    versioned = report_dir / f"{output['versioned_report_prefix']}{stamp}.json"
    latest = _resolve(root.resolve(), output["latest_report_path"])
    payload = _json_bytes(report)
    _atomic_write(versioned, payload)
    _atomic_write(latest, payload)
    return versioned, latest


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze unresolved C8 completion dates using official document structure."
    )
    parser.add_argument("--contract", type=Path, default=ROOT / CONTRACT_RELATIVE)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    report = analyze_date_residuals(root=ROOT, contract_path=args.contract)
    if args.write_report:
        paths = write_residual_scope_report(report, root=ROOT, contract_path=args.contract)
        report = {**report, "report_paths": [str(path) for path in paths]}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
