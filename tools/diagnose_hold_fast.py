from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"

TRVAL = LOG / "trading_stage_validation_latest.json"
PAPER_FIX = LOG / "paper_fix_cycle_latest.json"
PENDING = LOG / "pending_entry_status_latest.json"
SNAP = LOG / "integrated_ops_snapshot_latest.json"
RUNLOG = LOG / "run_paper_daily_last.txt"

OUT_JSON = LOG / "hold_diagnose_fast_latest.json"
OUT_MD = LOG / "hold_diagnose_fast_latest.md"


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _required_blockers(stage: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in (stage.get("items") or []):
        if not isinstance(item, dict):
            continue
        if not bool(item.get("required", True)):
            continue
        status = str(item.get("status") or "")
        if status in {"FAIL", "NOT_EVALUABLE"}:
            out.append(
                {
                    "name": str(item.get("name") or "-"),
                    "status": status,
                    "issue": str(item.get("issue") or "-"),
                    "metric": str(item.get("metric") or "-"),
                    "action": str(item.get("action") or "-"),
                }
            )
    return out


def _tail_gate_lines(path: Path, limit: int = 12) -> List[str]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    keep: List[str] = []
    for ln in lines:
        if any(k in ln for k in ("OUTLIER_GATE", "MACRO_NEWS_GUARD", "ENTRY_GATE")):
            keep.append(ln.strip())
    return keep[-limit:]


def _to_md(rep: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# Hold Fast Diagnose ({rep.get('generated_at')})")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- overall: **{rep.get('overall_judgment', '-')}**")
    lines.append(f"- next_step: `{rep.get('next_step', '-')}`")
    lines.append(f"- paper: **{rep.get('paper_judgment', '-')}**")
    lines.append(f"- live: **{rep.get('live_judgment', '-')}**")
    lines.append(f"- pending_status_reason: `{rep.get('pending_status_reason', '-')}`")
    lines.append(f"- pending_max_new: `{rep.get('pending_max_new', '-')}`")
    lines.append(f"- top_blocker_display: `{rep.get('top_blocker_display', '-')}`")
    lines.append(f"- top_blocker_effective: `{rep.get('top_blocker_effective', '-')}`")
    lines.append("")

    lines.append(f"## Paper Required Blockers ({len(rep.get('paper_required_blockers', []))})")
    if not rep.get("paper_required_blockers"):
        lines.append("- none")
    else:
        for b in rep["paper_required_blockers"]:
            lines.append(f"- {b.get('name')} [{b.get('status')}] {b.get('issue')} / {b.get('metric')}")
    lines.append("")

    lines.append(f"## Live Required Blockers ({len(rep.get('live_required_blockers', []))})")
    if not rep.get("live_required_blockers"):
        lines.append("- none")
    else:
        for b in rep["live_required_blockers"]:
            lines.append(f"- {b.get('name')} [{b.get('status')}] {b.get('issue')} / {b.get('metric')}")
    lines.append("")

    lines.append("## Latest Gate Lines")
    for ln in rep.get("runlog_gate_lines", []):
        lines.append(f"- {ln}")
    if not rep.get("runlog_gate_lines"):
        lines.append("- none")
    lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    tr = _read_json(TRVAL)
    pf = _read_json(PAPER_FIX)
    pd = _read_json(PENDING)
    sn = _read_json(SNAP)

    paper = tr.get("paper") if isinstance(tr.get("paper"), dict) else {}
    live = tr.get("live") if isinstance(tr.get("live"), dict) else {}
    overall = tr.get("overall") if isinstance(tr.get("overall"), dict) else {}
    summary = sn.get("summary") if isinstance(sn.get("summary"), dict) else {}

    rep: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_judgment": str(overall.get("judgment") or pf.get("overall_judgment") or "-"),
        "next_step": str(overall.get("next_step") or pf.get("next_step") or "-"),
        "paper_judgment": str(paper.get("judgment") or pf.get("paper_judgment") or "-"),
        "live_judgment": str(live.get("judgment") or pf.get("live_judgment") or "-"),
        "paper_required_blockers": _required_blockers(paper),
        "live_required_blockers": _required_blockers(live),
        "pending_status_reason": str(pd.get("status_reason") or "-"),
        "pending_max_new": pd.get("max_new"),
        "pending_queue_len": pd.get("pending_queue_len"),
        "top_blocker_display": str(summary.get("top_blocker") or "-"),
        "top_blocker_effective": str(summary.get("top_blocker_effective") or "-"),
        "runlog_gate_lines": _tail_gate_lines(RUNLOG),
        "sources": {
            "trading_stage_validation": str(TRVAL),
            "paper_fix_cycle": str(PAPER_FIX),
            "pending_entry_status": str(PENDING),
            "integrated_ops_snapshot": str(SNAP),
            "run_paper_daily_last": str(RUNLOG),
        },
    }

    OUT_JSON.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    OUT_MD.write_text(_to_md(rep), encoding="utf-8-sig")

    print(f"[HOLD_FAST] wrote {OUT_JSON}")
    print(f"[HOLD_FAST] wrote {OUT_MD}")
    print(
        f"[HOLD_FAST] overall={rep['overall_judgment']} next={rep['next_step']} "
        f"paper={rep['paper_judgment']} live={rep['live_judgment']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
