from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


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
    for it in stage.get("items", []) or []:
        if not bool(it.get("required", True)):
            continue
        st = str(it.get("status", ""))
        if st in {"FAIL", "NOT_EVALUABLE"}:
            out.append(
                {
                    "name": it.get("name"),
                    "status": st,
                    "metric": it.get("metric"),
                    "threshold": it.get("threshold"),
                    "issue": it.get("issue"),
                    "action": it.get("action"),
                }
            )
    return out


def _to_md(rep: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# Paper Fix Cycle Report ({rep.get('generated_at')})")
    lines.append("")
    lines.append(f"- cycle_index: {rep.get('cycle_index')}")
    lines.append(f"- overall_judgment: **{rep.get('overall_judgment')}**")
    lines.append(f"- next_step: `{rep.get('next_step')}`")
    lines.append(f"- paper_judgment: **{rep.get('paper_judgment')}**")
    lines.append(f"- live_judgment: **{rep.get('live_judgment')}**")
    lines.append("")

    blocks = rep.get("paper_required_blockers", []) or []
    lines.append(f"## Paper Required Blockers ({len(blocks)})")
    lines.append("")
    if not blocks:
        lines.append("- none")
    else:
        lines.append("| 항목 | 상태 | 지표 | 기준 | 이슈 | 조치 |")
        lines.append("|---|---|---|---|---|---|")
        for b in blocks:
            lines.append(
                f"| {b.get('name')} | {b.get('status')} | {b.get('metric')} | {b.get('threshold')} | {b.get('issue')} | {b.get('action')} |"
            )

    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Build concise paper-fix cycle report from trading_stage_validation_latest")
    ap.add_argument("--cycle-index", type=int, default=1)
    args = ap.parse_args()

    src = LOG_DIR / "trading_stage_validation_latest.json"
    ts = _read_json(src)
    if not ts:
        print(f"[STOP] missing_or_empty: {src}")
        return 2

    paper = ts.get("paper", {}) if isinstance(ts.get("paper"), dict) else {}
    live = ts.get("live", {}) if isinstance(ts.get("live"), dict) else {}
    overall = ts.get("overall", {}) if isinstance(ts.get("overall"), dict) else {}

    blockers = _required_blockers(paper)
    rep: Dict[str, Any] = {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cycle_index": int(args.cycle_index),
        "source": str(src),
        "overall_judgment": str(overall.get("judgment", "-")),
        "next_step": str(overall.get("next_step", "-")),
        "paper_judgment": str(paper.get("judgment", "-")),
        "live_judgment": str(live.get("judgment", "-")),
        "paper_required_blockers": blockers,
    }

    latest_json = LOG_DIR / "paper_fix_cycle_latest.json"
    latest_md = LOG_DIR / "paper_fix_cycle_latest.md"

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / f"paper_fix_cycle_{stamp}.json"
    out_md = LOG_DIR / f"paper_fix_cycle_{stamp}.md"

    payload = json.dumps(rep, ensure_ascii=False, indent=2)
    out_json.write_text(payload, encoding="utf-8-sig")
    latest_json.write_text(payload, encoding="utf-8-sig")

    md = _to_md(rep)
    out_md.write_text(md, encoding="utf-8-sig")
    latest_md.write_text(md, encoding="utf-8-sig")

    print(f"[PAPER_FIX] cycle={rep['cycle_index']} overall={rep['overall_judgment']} next={rep['next_step']}")
    print(f"[PAPER_FIX] blockers={len(blockers)} paper={rep['paper_judgment']} live={rep['live_judgment']}")
    print(f"[PAPER_FIX] latest_json={latest_json}")
    print(f"[PAPER_FIX] latest_md={latest_md}")

    # 0: paper_fix cleared, 10: still paper_fix, 11: paper_recheck/conditional
    nxt = rep["next_step"]
    if nxt == "paper_fix":
        return 10
    if nxt in {"paper_recheck"}:
        return 11
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
