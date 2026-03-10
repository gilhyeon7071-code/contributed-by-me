"""Checkpoint gate evaluation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from .base import VerificationStatus


CHECKPOINTS = [
    "pre_open",
    "signal_to_order",
    "pre_order_send",
    "post_close",
]


@dataclass
class CheckpointDecision:
    checkpoint: str
    action: str
    reasons: List[str]
    critical_failed_items: List[str]
    failed_items: List[str]
    skipped_items: List[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "checkpoint": self.checkpoint,
            "action": self.action,
            "reasons": self.reasons,
            "critical_failed_items": self.critical_failed_items,
            "failed_items": self.failed_items,
            "skipped_items": self.skipped_items,
        }


def evaluate_checkpoint_gate(report, checkpoint: str) -> CheckpointDecision:
    if checkpoint not in CHECKPOINTS:
        raise ValueError(f"Unsupported checkpoint: {checkpoint}")

    scoped = []
    for phase_report in report.phase_reports.values():
        for result in phase_report.results:
            cp = str(result.metadata.get("checkpoint", ""))
            if cp == checkpoint:
                scoped.append(result)

    failed = [r for r in scoped if r.status == VerificationStatus.FAILED]
    skipped = [r for r in scoped if r.status == VerificationStatus.SKIPPED]
    critical_failed = [r for r in failed if bool(r.metadata.get("critical", False))]

    if critical_failed:
        action = "BLOCK"
    elif failed:
        action = "REDUCE"
    elif skipped:
        action = "DEFERRED"
    else:
        action = "PASS"

    reasons = []
    if critical_failed:
        reasons.append(f"critical failures={len(critical_failed)}")
    if failed and not critical_failed:
        reasons.append(f"non-critical failures={len(failed)}")
    if skipped:
        reasons.append(f"skipped checks={len(skipped)}")
    if not reasons:
        reasons.append("all scoped checks passed")

    return CheckpointDecision(
        checkpoint=checkpoint,
        action=action,
        reasons=reasons,
        critical_failed_items=[r.item_name for r in critical_failed],
        failed_items=[r.item_name for r in failed],
        skipped_items=[r.item_name for r in skipped],
    )
