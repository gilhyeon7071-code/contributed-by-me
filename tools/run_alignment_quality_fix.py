from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import subprocess
import sys
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


def _tail(text: str, n: int = 20) -> str:
    lines = (text or "").strip().splitlines()
    return "\n".join(lines[-n:])


def _run(cmd: List[str], label: str, soft: bool = True) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "label": label,
        "cmd": cmd,
        "returncode": None,
        "ok": False,
        "stdout_tail": "",
        "stderr_tail": "",
    }
    try:
        p = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
        out["returncode"] = int(p.returncode)
        out["ok"] = int(p.returncode) == 0
        out["stdout_tail"] = _tail(p.stdout or "")
        out["stderr_tail"] = _tail(p.stderr or "")
    except Exception as e:
        out["returncode"] = -1
        out["stderr_tail"] = f"{type(e).__name__}:{e}"
    if (not soft) and (not bool(out["ok"])):
        raise RuntimeError(f"step failed: {label} rc={out.get('returncode')}")
    return out


def _read_latest_blockers() -> List[str]:
    p = LOG_DIR / "trading_stage_validation_latest.json"
    j = _read_json(p)
    paper = j.get("paper", {}) if isinstance(j.get("paper"), dict) else {}
    names: List[str] = []
    for it in paper.get("items", []) or []:
        if not bool(it.get("required", True)):
            continue
        st = str(it.get("status", ""))
        if st not in {"FAIL", "NOT_EVALUABLE"}:
            continue
        nm = str(it.get("name", "")).strip()
        if nm:
            names.append(nm)
    return names


def _derive_run_ymd() -> str:
    fills = ROOT / "paper" / "fills.csv"
    if not fills.exists():
        return dt.datetime.now().strftime("%Y%m%d")

    latest_any = ""
    latest_buy = ""
    for enc in ("utf-8-sig", "utf-8"):
        try:
            with fills.open("r", encoding=enc, newline="") as f:
                r = csv.DictReader(f)
                for row in r:
                    ymd = str(row.get("datetime", "")).strip()[:8]
                    if not (len(ymd) == 8 and ymd.isdigit()):
                        continue
                    if ymd > latest_any:
                        latest_any = ymd
                    side = str(row.get("side", "")).strip().upper()
                    if side == "BUY" and ymd > latest_buy:
                        latest_buy = ymd
            break
        except Exception:
            continue

    return latest_buy or latest_any or dt.datetime.now().strftime("%Y%m%d")


def _render_md(rep: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# Alignment/Quality Fix Report ({rep.get('generated_at')})")
    lines.append("")
    lines.append(f"- cycle_index: {rep.get('cycle_index')}")
    lines.append(f"- run_ymd: `{rep.get('run_ymd')}`")
    lines.append(f"- need_alignment_refresh: {rep.get('need_alignment_refresh')}")
    lines.append(f"- need_quality_refresh: {rep.get('need_quality_refresh')}")
    lines.append(f"- prev_min_shared: {rep.get('prev_min_shared')}")
    lines.append(f"- primary_min_shared: {rep.get('primary_min_shared')}")
    lines.append(f"- before_blockers: {', '.join(rep.get('before_blockers', [])) or 'none'}")
    lines.append(f"- after_blockers: {', '.join(rep.get('after_blockers', [])) or 'none'}")
    lines.append(f"- alignment_ready_after: {rep.get('alignment_ready_after')}")
    lines.append(f"- quality_gate_ok_after: {rep.get('quality_gate_ok_after')}")
    lines.append(f"- relaxed_alignment_applied: {rep.get('relaxed_alignment_applied')}")
    lines.append("")
    lines.append("## Steps")
    lines.append("")
    lines.append("| step | rc | ok |")
    lines.append("|---|---:|---|")
    for s in rep.get("steps", []) or []:
        lines.append(f"| {s.get('label')} | {s.get('returncode')} | {s.get('ok')} |")
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run focused paper_fix alignment/quality refresh routine.")
    ap.add_argument("--cycle-index", type=int, default=1)
    ap.add_argument("--align-window-trades", type=int, default=30)
    ap.add_argument("--min-shared-trades-strict", type=int, default=10)
    ap.add_argument("--min-shared-trades-relaxed", type=int, default=5)
    ap.add_argument("--allow-summary-fallback", action="store_true")
    ap.add_argument("--force-optimize", action="store_true")
    ap.add_argument("--max-backtest-age-days", type=float, default=7.0)
    ap.add_argument("--min-oos-trades", type=int, default=20)
    ap.add_argument("--min-oos-pf", type=float, default=0.80)
    ap.add_argument("--min-stable-score", type=float, default=-1e9)
    return ap.parse_args()


def _run_live_vs_bt(py: str, run_ymd: str, args: argparse.Namespace, min_shared: int) -> Dict[str, Any]:
    cmd = [
        py,
        str(ROOT / "live_vs_bt_paper_daily.py"),
        "--date",
        run_ymd,
        "--auto-optimize",
        "--align-window-trades",
        str(int(args.align_window_trades)),
        "--min-shared-trades",
        str(int(min_shared)),
        "--max-backtest-age-days",
        str(float(args.max_backtest_age_days)),
        "--min-oos-trades",
        str(int(args.min_oos_trades)),
        "--min-oos-pf",
        str(float(args.min_oos_pf)),
        "--min-stable-score",
        str(float(args.min_stable_score)),
    ]
    if bool(args.allow_summary_fallback):
        cmd.append("--allow-summary-fallback")
    return _run(cmd, label=f"live_vs_bt(min_shared={int(min_shared)})", soft=True)


def main() -> int:
    args = parse_args()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    py = sys.executable or "python"
    run_ymd = _derive_run_ymd()
    before = _read_latest_blockers()
    fb_before = _read_json(LOG_DIR / "live_vs_bt_feedback_latest.json")

    need_alignment = any(x in {"paper_bt_alignment", "paper_return_divergence"} for x in before)
    need_quality = "paper_quality_gate" in before
    need = bool(need_alignment or need_quality)

    prev_thr = (fb_before.get("thresholds") or {}) if isinstance(fb_before.get("thresholds"), dict) else {}
    strict_shared = max(1, int(args.min_shared_trades_strict))
    relaxed_shared = max(1, int(args.min_shared_trades_relaxed))
    prev_min_shared = max(1, int(prev_thr.get("min_shared_trades") or strict_shared))

    steps: List[Dict[str, Any]] = []
    relaxed_applied = False
    primary_min_shared = strict_shared if need_alignment else prev_min_shared

    if need:
        steps.append(_run([py, str(ROOT / "report_if_due_v41_1.py"), "--force"], label="report_if_due(force)", soft=True))

        if bool(args.force_optimize) and need_quality:
            steps.append(
                _run(
                    [
                        py,
                        str(ROOT / "optimize_if_due_v41_1.py"),
                        "--force",
                        "--reason",
                        f"paper_fix_cycle_{int(args.cycle_index)}_quality",
                        "--skip-quality-gate",
                    ],
                    label="optimize_if_due(force,skip_gate)",
                    soft=True,
                )
            )
            steps.append(_run([py, str(ROOT / "report_if_due_v41_1.py"), "--force"], label="report_if_due(post_opt)", soft=True))

        steps.append(_run_live_vs_bt(py=py, run_ymd=run_ymd, args=args, min_shared=primary_min_shared))

        fb = _read_json(LOG_DIR / "live_vs_bt_feedback_latest.json")
        cmpo = fb.get("comparison", {}) if isinstance(fb.get("comparison"), dict) else {}
        opt = fb.get("optimize", {}) if isinstance(fb.get("optimize"), dict) else {}
        aln = opt.get("alignment", {}) if isinstance(opt.get("alignment"), dict) else {}
        aln_ready = bool(cmpo.get("alignment_ready"))
        aln_reason = str(cmpo.get("alignment_reason") or "")
        bt_n = int(aln.get("bt_n") or 0)

        if need_alignment and (not aln_ready):
            if (aln_reason == "bt_too_few_trades_in_window") and (relaxed_shared < primary_min_shared) and (bt_n >= relaxed_shared):
                steps.append(_run_live_vs_bt(py=py, run_ymd=run_ymd, args=args, min_shared=relaxed_shared))
                relaxed_applied = True

        steps.append(_run([py, str(ROOT / "tools" / "build_trading_stage_validation_report.py")], label="build_trading_stage_validation", soft=True))

    after = _read_latest_blockers()
    fb_after = _read_json(LOG_DIR / "live_vs_bt_feedback_latest.json")
    cmp_after = fb_after.get("comparison", {}) if isinstance(fb_after.get("comparison"), dict) else {}
    opt_after = fb_after.get("optimize", {}) if isinstance(fb_after.get("optimize"), dict) else {}

    rep: Dict[str, Any] = {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cycle_index": int(args.cycle_index),
        "run_ymd": run_ymd,
        "need_alignment_refresh": bool(need_alignment),
        "need_quality_refresh": bool(need_quality),
        "prev_min_shared": int(prev_min_shared),
        "primary_min_shared": int(primary_min_shared),
        "before_blockers": before,
        "after_blockers": after,
        "alignment_ready_after": bool(cmp_after.get("alignment_ready")),
        "alignment_reason_after": cmp_after.get("alignment_reason"),
        "quality_gate_ok_after": bool(opt_after.get("gate_ok")) if isinstance(opt_after, dict) else None,
        "quality_gate_reasons_after": (opt_after.get("gate_reasons") or []) if isinstance(opt_after, dict) else [],
        "relaxed_alignment_applied": bool(relaxed_applied),
        "ran_routine": bool(need),
        "steps": steps,
    }

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / f"paper_fix_alignment_quality_{stamp}.json"
    out_md = LOG_DIR / f"paper_fix_alignment_quality_{stamp}.md"
    latest_json = LOG_DIR / "paper_fix_alignment_quality_latest.json"
    latest_md = LOG_DIR / "paper_fix_alignment_quality_latest.md"

    payload = json.dumps(rep, ensure_ascii=False, indent=2)
    out_json.write_text(payload, encoding="utf-8-sig")
    latest_json.write_text(payload, encoding="utf-8-sig")

    md = _render_md(rep)
    out_md.write_text(md, encoding="utf-8-sig")
    latest_md.write_text(md, encoding="utf-8-sig")

    print(f"[ALIGN_QUALITY] ran={need} cycle={int(args.cycle_index)} run_ymd={run_ymd}")
    print(f"[ALIGN_QUALITY] before={len(before)} after={len(after)} primary_min_shared={int(primary_min_shared)} relaxed={bool(relaxed_applied)}")
    print(f"[ALIGN_QUALITY] latest_json={latest_json}")
    print(f"[ALIGN_QUALITY] latest_md={latest_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
