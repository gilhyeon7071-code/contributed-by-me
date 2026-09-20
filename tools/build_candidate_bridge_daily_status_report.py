"""Build a read-only daily status report for candidate bridge validation.

This report is intentionally descriptive. It does not infer approval, change
candidate eligibility, or modify paper_engine, orders, fills, gates, sizing,
operational ledgers, or stats.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _line_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(text, encoding=encoding)
    tmp_path.replace(path)


def build_report(root: Path) -> dict[str, Any]:
    log_dir = root / "2_Logs"
    ledger_path = log_dir / "candidate_bridge_shadow_ledger_latest.json"
    sim_path = log_dir / "candidate_bridge_policy_simulation_latest.json"
    out_json = log_dir / "candidate_bridge_daily_status_latest.json"
    out_md = log_dir / "candidate_bridge_daily_status_latest.md"
    ledger = _read_json(ledger_path)
    sim = _read_json(sim_path)

    summary = ledger.get("summary") if isinstance(ledger.get("summary"), dict) else {}
    validation = ledger.get("validation_summary") if isinstance(ledger.get("validation_summary"), dict) else {}
    decision_counts = summary.get("decision_counts") if isinstance(summary.get("decision_counts"), dict) else {}
    outcome_counts = summary.get("outcome_status_counts") if isinstance(summary.get("outcome_status_counts"), dict) else {}

    generated_at = datetime.now().isoformat(timespec="seconds")
    status = {
        "generated_at": generated_at,
        "scope": "read_only_candidate_bridge_daily_status",
        "policy_effect": "read_only_no_candidate_order_fill_ledger_stat_change",
        "source_ledger": str(ledger_path),
        "source_simulation": str(sim_path),
        "ledger_generated_at": ledger.get("generated_at", ""),
        "simulation_generated_at": sim.get("generated_at", ""),
        "candidate_rows": sim.get("candidate_rows", ledger.get("rows_total", 0)),
        "entry_eligible_read_only_count": sim.get(
            "entry_eligible_read_only_count",
            summary.get("entry_eligible_read_only_rows", 0),
        ),
        "decision_counts": decision_counts,
        "outcome_status_counts": outcome_counts,
        "price_max_ymd": ledger.get("price_max_ymd", ""),
        "alert_status": ledger.get("alert_status", ""),
        "alert_message": ledger.get("alert_message", ""),
        "completed_d1_rows": validation.get("completed_d1_rows", 0),
        "completed_d3_rows": validation.get("completed_d3_rows", 0),
        "completed_d5_rows": validation.get("completed_d5_rows", 0),
        "allow_minus_block_d1_mean_pct": validation.get("allow_minus_block_d1_mean_pct"),
        "allow_minus_block_d3_mean_pct": validation.get("allow_minus_block_d3_mean_pct"),
        "allow_minus_block_d5_mean_pct": validation.get("allow_minus_block_d5_mean_pct"),
        "report_mode": "daily_10am_previous_day_status",
        "completion_note": "Two weeks is an initial observation window, not validation completion.",
    }

    md = [
        "# Candidate Bridge Validation Daily Status",
        "",
        f"- generated_at: {generated_at}",
        f"- report_mode: {status['report_mode']}",
        f"- alert_status: {status['alert_status']}",
        f"- alert_message: {status['alert_message']}",
        f"- price_max_ymd: {status['price_max_ymd']}",
        f"- candidate_rows: {status['candidate_rows']}",
        f"- entry_eligible_read_only_count: {status['entry_eligible_read_only_count']}",
        "",
        "## Counts",
        f"- decision_counts: {json.dumps(decision_counts, ensure_ascii=False)}",
        f"- outcome_status_counts: {json.dumps(outcome_counts, ensure_ascii=False)}",
        "",
        "## Completed Outcomes",
        f"- completed_d1_rows: {status['completed_d1_rows']}",
        f"- completed_d3_rows: {status['completed_d3_rows']}",
        f"- completed_d5_rows: {status['completed_d5_rows']}",
        "",
        "## Relative Performance",
        f"- allow_minus_block_d1_mean_pct: {_line_value(status['allow_minus_block_d1_mean_pct'])}",
        f"- allow_minus_block_d3_mean_pct: {_line_value(status['allow_minus_block_d3_mean_pct'])}",
        f"- allow_minus_block_d5_mean_pct: {_line_value(status['allow_minus_block_d5_mean_pct'])}",
        "",
        "## Note",
        "- This is read-only status reporting.",
        "- Two weeks is an initial observation window, not validation completion.",
        "- No candidate eligibility, paper_engine entry path, orders, fills, gates, sizing, operational ledger, or stats are changed.",
    ]

    _atomic_write_text(out_json, json.dumps(status, ensure_ascii=True, indent=2), encoding="utf-8")
    _atomic_write_text(out_md, "\n".join(md) + "\n", encoding="utf-8")
    print(json.dumps(status, ensure_ascii=True, indent=2))
    return status


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="RootA path")
    args = parser.parse_args()
    build_report(Path(args.root).resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
