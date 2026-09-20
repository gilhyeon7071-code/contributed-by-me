"""Build a conservative V3 research-observation ledger from known local artifacts.

This is a read-only auditor. It never runs a validation model and never reads or
writes Gate, orders, fills, ledger, broker, or paper-runtime state. A saved prior
research result quarantines its calendar overlap; lack of a scanned artifact is
reported as unverified and is never treated as proof that a period is unseen.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


@dataclass(frozen=True)
class EvidenceSpec:
    relative_path: str
    expected_scope: str
    role: str


JSON_EVIDENCE = (
    EvidenceSpec(
        "2_Logs/new_method_historical_axis_matrix_latest.json",
        "read_only_new_method_historical_axis_matrix",
        "PRIOR_RESEARCH_RESULT",
    ),
    EvidenceSpec(
        "2_Logs/new_method_strategy_axis_matrix_latest.json",
        "read_only_new_method_strategy_axis_matrix",
        "PRIOR_RESEARCH_RESULT",
    ),
    EvidenceSpec(
        "2_Logs/new_method_price_structure_latest.json",
        "read_only_price_structure_strategy_validation",
        "PRIOR_RESEARCH_RESULT",
    ),
)

KNOWN_TRAIN_EVIDENCE = (
    {
        "start": "2020-01-01",
        "end": "2023-12-31",
        "role": "PRIOR_RESEARCH_TRAIN_RESULT",
        "path": "docs/03-operations/strategy_validation_round1_train_result_20260720.md",
        "reason": "Round 1 final artifact is explicitly Train-only through 2023.",
    },
    {
        "start": "2020-01-01",
        "end": "2023-12-31",
        "role": "PRIOR_RESEARCH_TRAIN_RESULT",
        "path": "docs/03-operations/strategy_validation_round2_train_result_20260720.md",
        "reason": "Round 2 final artifact is explicitly Train-only through 2023.",
    },
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def overlap(start: str, end: str, target_start: str, target_end: str) -> bool:
    return not (end < target_start or start > target_end)


def read_json_evidence(spec: EvidenceSpec) -> dict[str, Any]:
    path = ROOT / spec.relative_path
    if not path.exists():
        return {
            "path": spec.relative_path,
            "exists": False,
            "role": spec.role,
            "status": "MISSING_EVIDENCE_FILE",
        }
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    window = payload.get("window") or {}
    scope = payload.get("scope")
    start, end = window.get("start"), window.get("end")
    status = "VALID_PRIOR_RESEARCH_EVIDENCE"
    if scope != spec.expected_scope or not start or not end:
        status = "INVALID_OR_INCOMPLETE_EVIDENCE"
    return {
        "path": spec.relative_path,
        "exists": True,
        "role": spec.role,
        "status": status,
        "scope": scope,
        "generated_at": payload.get("generated_at"),
        "start": start,
        "end": end,
        "sha256": sha256(path),
    }


def row(
    start: str,
    end: str,
    status: str,
    role: str,
    evidence_paths: list[str],
    reason: str,
) -> dict[str, Any]:
    return {
        "calendar_start": start,
        "calendar_end": end,
        "observation_status": status,
        "evidence_role": role,
        "evidence_paths": " | ".join(evidence_paths),
        "first_calculation_at": "unknown",
        "first_human_view_at": "unknown",
        "reason": reason,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", default=datetime.now().strftime("%Y%m%d"))
    args = parser.parse_args()
    generated_at = datetime.now().isoformat(timespec="seconds")

    json_evidence = [read_json_evidence(spec) for spec in JSON_EVIDENCE]
    valid_windows = [
        evidence
        for evidence in json_evidence
        if evidence.get("status") == "VALID_PRIOR_RESEARCH_EVIDENCE"
    ]
    rows: list[dict[str, Any]] = []
    for evidence in KNOWN_TRAIN_EVIDENCE:
        source = ROOT / evidence["path"]
        if source.exists():
            rows.append(
                row(
                    evidence["start"],
                    evidence["end"],
                    "QUARANTINED_PRIOR_RESEARCH_ARTIFACT",
                    evidence["role"],
                    [evidence["path"]],
                    evidence["reason"],
                )
            )

    for evidence in valid_windows:
        rows.append(
            row(
                evidence["start"],
                evidence["end"],
                "QUARANTINED_PRIOR_RESEARCH_ARTIFACT",
                evidence["role"],
                [evidence["path"]],
                "Saved prior-research result artifact covers this interval; it cannot be claimed as a new blind period.",
            )
        )

    candidate_start, candidate_end = "2025-01-01", "2025-12-31"
    candidate_2025_overlap = [
        evidence
        for evidence in valid_windows
        if overlap(evidence["start"], evidence["end"], candidate_start, candidate_end)
    ]
    if candidate_2025_overlap:
        rows.append(
            row(
                candidate_start,
                candidate_end,
                "QUARANTINED_PRIOR_RESEARCH_ARTIFACT",
                "SEALED_OOS_ELIGIBILITY",
                [str(evidence["path"]) for evidence in candidate_2025_overlap],
                "At least one saved prior-research result overlaps 2025. A partially observed calendar year is not an eligible sealed OOS.",
            )
        )
        sealed_oos_2025 = "NOT_ELIGIBLE_QUARANTINED"
    else:
        rows.append(
            row(
                candidate_start,
                candidate_end,
                "UNVERIFIED_NO_SCANNED_EVIDENCE",
                "SEALED_OOS_ELIGIBILITY",
                [],
                "No configured evidence overlapped 2025. This is not proof that the period is unseen; a broader attestation is required.",
            )
        )
        sealed_oos_2025 = "UNVERIFIED_NOT_ELIGIBLE"

    rows.append(
        row(
            "2024-01-01",
            "2024-12-31",
            "UNVERIFIED_NO_SCANNED_EVIDENCE",
            "POTENTIAL_DEVELOPMENT_VALIDATION",
            [],
            "No configured artifact proves blind status. This row does not authorize use as a blind validation period.",
        )
    )

    payload = {
        "generated_at": generated_at,
        "as_of": args.as_of,
        "scope": "read_only_v3_observation_ledger_bootstrap",
        "inputs": json_evidence,
        "known_train_evidence": KNOWN_TRAIN_EVIDENCE,
        "ledger_rows": rows,
        "sealed_oos_2025_eligibility": sealed_oos_2025,
        "final_confirmation_status": "DEFERRED_NO_LEDGER_QUALIFIED_COMPLETE_SEALED_OOS",
        "limitations": [
            "This audit inventories configured local research artifacts only.",
            "The absence of an artifact is not proof that a period was unseen.",
            "First human-result-view times cannot be reconstructed from filesystem artifacts and remain unknown.",
        ],
        "operational_change": False,
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = LOG_DIR / f"v3_observation_ledger_bootstrap_{stamp}"
    csv_path = stem.with_suffix(".csv")
    json_path = stem.with_suffix(".json")
    md_path = stem.with_suffix(".md")
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# V3 Observation Ledger Bootstrap",
        "",
        f"- generated_at: {generated_at}",
        f"- scope: {payload['scope']}",
        f"- sealed_oos_2025_eligibility: {sealed_oos_2025}",
        f"- final_confirmation_status: {payload['final_confirmation_status']}",
        "",
        "## Ledger",
        "",
        "| interval | status | role | evidence |",
        "|---|---|---|---|",
    ]
    for item in rows:
        lines.append(
            f"| {item['calendar_start']} ~ {item['calendar_end']} | {item['observation_status']} | {item['evidence_role']} | {item['evidence_paths'] or 'none'} |"
        )
    lines.extend(["", "## Limitations", *[f"- {item}" for item in payload["limitations"]], ""])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps({"status": "OK", "csv": str(csv_path), "json": str(json_path), "md": str(md_path), "sealed_oos_2025_eligibility": sealed_oos_2025}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
