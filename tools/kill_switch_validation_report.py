#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kill-switch validation score report (shadow mode).

Purpose
- Score logical consistency and auditability of kill-switch MDD handling.
- Produce a shadow auto-score action without affecting trading decisions.

Outputs
- 2_Logs/kill_switch_validation_report_latest.json
- 2_Logs/kill_switch_validation_report_<YYYYMMDD_HHMMSS>.json
- 2_Logs/kill_switch_validation_daily_latest.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
OUT_LATEST = LOG_DIR / "kill_switch_validation_report_latest.json"
OUT_DAILY_CSV = LOG_DIR / "kill_switch_validation_daily_latest.csv"


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _now_compact() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    return {}


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _safe_ratio(n: int, d: int, default: float = 1.0) -> float:
    return float(default) if d <= 0 else float(n) / float(d)


def _clip01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _grade(score: float) -> str:
    if score >= 85.0:
        return "GOOD"
    if score >= 70.0:
        return "WATCH"
    return "REVIEW"


def _parse_ymd_from_name(name: str, prefix: str) -> Optional[str]:
    m = re.match(rf"^{re.escape(prefix)}_(\d{{8}})(?:_(\d{{6}}))?\.json$", name)
    if not m:
        return None
    return m.group(1)


def _load_latest_by_day(prefix: str) -> Dict[str, Tuple[Path, Dict[str, Any]]]:
    out: Dict[str, Tuple[Path, Dict[str, Any]]] = {}
    files = sorted(LOG_DIR.glob(f"{prefix}_*.json"), key=lambda p: p.name)
    for p in files:
        ymd = _parse_ymd_from_name(p.name, prefix)
        if not ymd:
            continue
        obj = _read_json(p)
        if not isinstance(obj, dict):
            continue
        prev = out.get(ymd)
        if prev is None or p.name > prev[0].name:
            out[ymd] = (p, obj)
    return out


def _shadow_score(max_dd: Optional[float], dd_lim: Optional[float], day_ret: Optional[float], day_lim: Optional[float]) -> float:
    dd_ratio = 0.0
    day_ratio = 0.0
    if max_dd is not None and dd_lim is not None and dd_lim > 0:
        dd_ratio = abs(max_dd) / abs(dd_lim)
    if day_ret is not None and day_lim is not None and day_lim > 0 and day_ret < 0:
        day_ratio = abs(day_ret) / abs(day_lim)
    return min(100.0, max(0.0, (70.0 * dd_ratio) + (30.0 * day_ratio)))


def _shadow_action(score: float, block_cut: float = 85.0, reduce_cut: float = 60.0) -> str:
    if score >= block_cut:
        return "BLOCK"
    if score >= reduce_cut:
        return "REDUCE"
    return "ALLOW"


def _build_daily_rows() -> List[Dict[str, Any]]:
    p0_by_day = _load_latest_by_day("p0_daily_check")
    gate_by_day = _load_latest_by_day("gate_daily")

    rows: List[Dict[str, Any]] = []
    for ymd in sorted(p0_by_day.keys()):
        p0_path, p0 = p0_by_day[ymd]
        gate_path, gate = gate_by_day.get(ymd, (None, {}))

        ks = p0.get("kill_switch") if isinstance(p0.get("kill_switch"), dict) else {}
        metrics = ks.get("metrics") if isinstance(ks.get("metrics"), dict) else {}
        limits = ks.get("limits") if isinstance(ks.get("limits"), dict) else {}

        triggered = bool(ks.get("triggered"))
        reasons = [str(x) for x in (ks.get("reasons") or [])]

        max_dd = _to_float(metrics.get("max_drawdown_pct"))
        day_ret = _to_float(metrics.get("last_day_ret"))
        dd_lim = abs(_to_float(limits.get("max_drawdown_pct")) or 0.25)
        day_lim = abs(_to_float(limits.get("max_daily_loss_pct")) or 0.08)

        has_required = (max_dd is not None) and (day_ret is not None) and (dd_lim > 0) and (day_lim > 0)

        expect_dd = bool(max_dd is not None and dd_lim > 0 and max_dd <= -abs(dd_lim))
        expect_day = bool(day_ret is not None and day_lim > 0 and day_ret <= -abs(day_lim))
        expected_trigger = expect_dd or expect_day

        mismatch = bool(has_required and (triggered != expected_trigger))

        dd_source = metrics.get("dd_source") if isinstance(metrics.get("dd_source"), dict) else {}
        calc = dd_source.get("calc") if isinstance(dd_source.get("calc"), dict) else {}
        art = dd_source.get("artifact") if isinstance(dd_source.get("artifact"), dict) else {}
        curve = art.get("dd_curve_csv") if isinstance(art.get("dd_curve_csv"), dict) else {}

        match_fmt4 = bool(calc.get("match_fmt4"))
        curve_path = Path(str(curve.get("path"))) if curve.get("path") else None
        curve_exists = bool(curve_path and curve_path.exists())
        provenance_ok = bool(match_fmt4 and curve_exists)
        has_dd_source = bool(dd_source)

        lifetime_dd = _to_float(metrics.get("debug_lifetime_max_drawdown_pct"))
        gap = None
        if max_dd is not None and lifetime_dd is not None:
            gap = abs(float(lifetime_dd) - float(max_dd))

        gate_action = "NA"
        gate2 = "NA"
        if isinstance(gate, dict) and gate:
            gate_action = str(((gate.get("engine_action") or {}).get("action") or "NA")).upper()
            gate2 = str(((gate.get("gate2") or {}).get("status") or "NA")).upper()

        baseline_score = _shadow_score(max_dd, dd_lim, day_ret, day_lim)
        baseline_action = _shadow_action(baseline_score, block_cut=85.0, reduce_cut=60.0)
        actual_action_simple = "ALLOW" if gate_action not in {"REDUCE", "BLOCK"} else gate_action

        rows.append(
            {
                "ymd": ymd,
                "p0_file": str(p0_path),
                "gate_file": str(gate_path) if gate_path else "",
                "kill_triggered": triggered,
                "expected_trigger": expected_trigger,
                "expect_dd": expect_dd,
                "expect_day_loss": expect_day,
                "mismatch": mismatch,
                "has_required": has_required,
                "max_dd": max_dd,
                "max_dd_limit": dd_lim,
                "last_day_ret": day_ret,
                "day_loss_limit": day_lim,
                "kill_reasons": ";".join(reasons),
                "provenance_ok": provenance_ok,
                "dd_source_match_fmt4": match_fmt4,
                "dd_curve_exists": curve_exists,
                "has_dd_source": has_dd_source,
                "rolling_vs_lifetime_gap": gap,
                "gate_action": gate_action,
                "gate2_status": gate2,
                "actual_action_simple": actual_action_simple,
                "shadow_score": round(baseline_score, 4),
                "shadow_action": baseline_action,
                "shadow_agree_actual": (baseline_action == actual_action_simple) if gate_action != "NA" else None,
            }
        )
    return rows


def _summarize(rows: List[Dict[str, Any]], block_cut: float = 85.0, reduce_cut: float = 60.0, provenance_start_ymd: Optional[str] = None) -> Dict[str, Any]:
    n = len(rows)

    required_count = 0
    mismatch_count = 0
    false_pos = 0
    false_neg = 0

    trigger_days = 0
    provenance_scope_days = 0
    provenance_ok_days = 0

    gate_scope = 0
    gate_align = 0

    shadow_scope = 0
    shadow_agree = 0

    gaps: List[float] = []

    for r in rows:
        has_required = bool(r.get("has_required"))
        triggered = bool(r.get("kill_triggered"))
        expected_trigger = bool(r.get("expected_trigger"))

        if has_required:
            required_count += 1
        if bool(r.get("mismatch")):
            mismatch_count += 1
        if has_required and triggered and (not expected_trigger):
            false_pos += 1
        if has_required and expected_trigger and (not triggered):
            false_neg += 1

        if triggered:
            trigger_days += 1
        prov_in_scope = bool(triggered and ((provenance_start_ymd is None) or (str(r.get("ymd") or "") >= str(provenance_start_ymd))))
        if prov_in_scope:
            provenance_scope_days += 1
            if bool(r.get("provenance_ok")):
                provenance_ok_days += 1

        gap = _to_float(r.get("rolling_vs_lifetime_gap"))
        if gap is not None:
            gaps.append(float(gap))

        gate2 = str(r.get("gate2_status") or "NA").upper()
        if gate2 != "NA":
            gate_scope += 1
            aligned = (gate2 == "FAIL") if triggered else (gate2 == "PASS")
            if aligned:
                gate_align += 1

        score = _to_float(r.get("shadow_score"))
        gate_action = str(r.get("gate_action") or "NA").upper()
        actual_action_simple = str(r.get("actual_action_simple") or "ALLOW").upper()
        if score is not None and gate_action != "NA":
            pred = _shadow_action(score, block_cut=block_cut, reduce_cut=reduce_cut)
            shadow_scope += 1
            if pred == actual_action_simple:
                shadow_agree += 1

    consistency_ratio = _safe_ratio(required_count - mismatch_count, required_count, default=1.0)
    coverage_ratio = _safe_ratio(required_count, n, default=0.0)
    provenance_ratio = _safe_ratio(provenance_ok_days, provenance_scope_days, default=1.0)
    gate_align_ratio = _safe_ratio(gate_align, gate_scope, default=1.0)
    shadow_agree_ratio = _safe_ratio(shadow_agree, shadow_scope, default=0.0)

    gap_mean = (sum(gaps) / len(gaps)) if gaps else None
    gap_max = max(gaps) if gaps else None

    total_score = 100.0 * (
        0.35 * _clip01(consistency_ratio)
        + 0.25 * _clip01(coverage_ratio)
        + 0.20 * _clip01(provenance_ratio)
        + 0.20 * _clip01(gate_align_ratio)
    )

    return {
        "window": {
            "days": n,
            "from": rows[0]["ymd"] if rows else None,
            "to": rows[-1]["ymd"] if rows else None,
        },
        "score": {
            "total": round(total_score, 2),
            "grade": _grade(total_score),
            "components": {
                "consistency_ratio": round(consistency_ratio, 6),
                "coverage_ratio": round(coverage_ratio, 6),
                "provenance_ratio": round(provenance_ratio, 6),
                "gate_alignment_ratio": round(gate_align_ratio, 6),
            },
        },
        "stats": {
            "required_count": required_count,
            "mismatch_count": mismatch_count,
            "false_positive": false_pos,
            "false_negative": false_neg,
            "trigger_days": trigger_days,
            "provenance_scope_days": provenance_scope_days,
            "provenance_start_ymd": provenance_start_ymd,
            "provenance_ok_days": provenance_ok_days,
            "gate_scope_days": gate_scope,
            "gate_aligned_days": gate_align,
            "rolling_vs_lifetime_gap_mean": gap_mean,
            "rolling_vs_lifetime_gap_max": gap_max,
        },
        "shadow_auto_score": {
            "enabled": True,
            "note": "shadow only (non-execution)",
            "thresholds": {
                "block_cut": float(block_cut),
                "reduce_cut": float(reduce_cut),
            },
            "agreement_with_actual_action_ratio": round(shadow_agree_ratio, 6),
            "scope_days": shadow_scope,
            "agree_days": shadow_agree,
        },
    }


def _tune_thresholds(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    scope_rows = [r for r in rows if str(r.get("gate_action") or "NA").upper() != "NA" and _to_float(r.get("shadow_score")) is not None]

    if not scope_rows:
        return {
            "scope_days": 0,
            "best": {
                "block_cut": 85.0,
                "reduce_cut": 60.0,
                "agreement_ratio": 0.0,
                "agree_days": 0,
            },
            "baseline": {
                "block_cut": 85.0,
                "reduce_cut": 60.0,
                "agreement_ratio": 0.0,
                "agree_days": 0,
            },
            "improvement": 0.0,
        }

    def _eval(block_cut: float, reduce_cut: float) -> Tuple[int, float]:
        agree = 0
        for r in scope_rows:
            score = float(r["shadow_score"])
            pred = _shadow_action(score, block_cut=block_cut, reduce_cut=reduce_cut)
            actual = str(r.get("actual_action_simple") or "ALLOW").upper()
            if pred == actual:
                agree += 1
        ratio = _safe_ratio(agree, len(scope_rows), default=0.0)
        return agree, ratio

    base_agree, base_ratio = _eval(85.0, 60.0)

    best_block = 85.0
    best_reduce = 60.0
    best_agree = base_agree
    best_ratio = base_ratio

    for reduce_cut in range(40, 81, 5):
        for block_cut in range(reduce_cut + 10, 96, 5):
            agree, ratio = _eval(float(block_cut), float(reduce_cut))
            if ratio > best_ratio:
                best_block, best_reduce, best_agree, best_ratio = float(block_cut), float(reduce_cut), agree, ratio
            elif ratio == best_ratio:
                cur_dist = abs(best_block - 85.0) + abs(best_reduce - 60.0)
                new_dist = abs(float(block_cut) - 85.0) + abs(float(reduce_cut) - 60.0)
                if new_dist < cur_dist:
                    best_block, best_reduce, best_agree, best_ratio = float(block_cut), float(reduce_cut), agree, ratio

    return {
        "scope_days": len(scope_rows),
        "baseline": {
            "block_cut": 85.0,
            "reduce_cut": 60.0,
            "agreement_ratio": round(base_ratio, 6),
            "agree_days": int(base_agree),
        },
        "best": {
            "block_cut": best_block,
            "reduce_cut": best_reduce,
            "agreement_ratio": round(best_ratio, 6),
            "agree_days": int(best_agree),
        },
        "improvement": round(best_ratio - base_ratio, 6),
    }


def _attach_tuned_actions(rows: List[Dict[str, Any]], block_cut: float, reduce_cut: float) -> None:
    for r in rows:
        score = _to_float(r.get("shadow_score"))
        if score is None:
            r["shadow_action_tuned"] = "NA"
            r["shadow_agree_actual_tuned"] = None
            continue
        pred = _shadow_action(score, block_cut=block_cut, reduce_cut=reduce_cut)
        r["shadow_action_tuned"] = pred
        gate_action = str(r.get("gate_action") or "NA").upper()
        actual = str(r.get("actual_action_simple") or "ALLOW").upper()
        r["shadow_agree_actual_tuned"] = (pred == actual) if gate_action != "NA" else None


def _build_recommendation(full_summary: Dict[str, Any], recent_summary: Dict[str, Any], full_tune: Dict[str, Any], recent_tune: Dict[str, Any]) -> Dict[str, Any]:
    rec = {
        "policy": "KEEP_BASELINE",
        "reason": "no clear gain",
        "use_thresholds": {"block_cut": 85.0, "reduce_cut": 60.0},
    }

    recent_improve = float(recent_tune.get("improvement") or 0.0)
    recent_scope = int(recent_tune.get("scope_days") or 0)
    full_improve = float(full_tune.get("improvement") or 0.0)

    # Promote tuned thresholds only when recent scope is sufficient and gain is meaningful.
    if recent_scope >= 8 and recent_improve >= 0.05:
        b = (recent_tune.get("best") or {})
        rec = {
            "policy": "TRIAL_TUNED_SHADOW",
            "reason": f"recent agreement improved by {recent_improve:.3f}",
            "use_thresholds": {
                "block_cut": float(b.get("block_cut") or 85.0),
                "reduce_cut": float(b.get("reduce_cut") or 60.0),
            },
        }
    elif full_improve >= 0.05 and int(full_tune.get("scope_days") or 0) >= 12:
        b = (full_tune.get("best") or {})
        rec = {
            "policy": "TRIAL_TUNED_SHADOW",
            "reason": f"full-window agreement improved by {full_improve:.3f}",
            "use_thresholds": {
                "block_cut": float(b.get("block_cut") or 85.0),
                "reduce_cut": float(b.get("reduce_cut") or 60.0),
            },
        }

    # Guardrail: if validation grade is REVIEW, keep baseline even with agreement gain.
    grade_recent = str((recent_summary.get("score") or {}).get("grade") or "")
    if grade_recent == "REVIEW":
        rec = {
            "policy": "KEEP_BASELINE",
            "reason": "recent validation grade is REVIEW",
            "use_thresholds": {"block_cut": 85.0, "reduce_cut": 60.0},
        }

    return rec


def build_report(recent_days: int = 20) -> Dict[str, Any]:
    rows = _build_daily_rows()
    if recent_days <= 0:
        recent_days = 20
    recent_rows = rows[-recent_days:] if len(rows) > recent_days else list(rows)

    prov_candidates = [str(r.get("ymd") or "") for r in rows if bool(r.get("has_dd_source"))]
    prov_candidates = [x for x in prov_candidates if len(x) == 8]
    provenance_start_ymd = min(prov_candidates) if prov_candidates else None

    full_summary = _summarize(rows, block_cut=85.0, reduce_cut=60.0, provenance_start_ymd=provenance_start_ymd)
    recent_summary = _summarize(recent_rows, block_cut=85.0, reduce_cut=60.0, provenance_start_ymd=provenance_start_ymd)

    tune_full = _tune_thresholds(rows)
    tune_recent = _tune_thresholds(recent_rows)

    recommendation = _build_recommendation(full_summary, recent_summary, tune_full, tune_recent)
    rec_cuts = recommendation.get("use_thresholds") or {"block_cut": 85.0, "reduce_cut": 60.0}
    rec_block = float(rec_cuts.get("block_cut") or 85.0)
    rec_reduce = float(rec_cuts.get("reduce_cut") or 60.0)

    _attach_tuned_actions(rows, block_cut=rec_block, reduce_cut=rec_reduce)

    issues: List[str] = []
    fstats = full_summary.get("stats") or {}
    rstats = recent_summary.get("stats") or {}
    if int(fstats.get("false_negative") or 0) > 0:
        issues.append(f"false_negative_trigger={int(fstats.get('false_negative') or 0)}")
    if int(fstats.get("false_positive") or 0) > 0:
        issues.append(f"false_positive_trigger={int(fstats.get('false_positive') or 0)}")
    if int(fstats.get("provenance_scope_days") or 0) > 0:
        prv = float((full_summary.get("score") or {}).get("components", {}).get("provenance_ratio") or 0.0)
        if prv < 1.0:
            issues.append("triggered day without full dd_source provenance")
    gate_align = float((full_summary.get("score") or {}).get("components", {}).get("gate_alignment_ratio") or 1.0)
    if gate_align < 1.0:
        issues.append("gate2 alignment mismatch exists")

    recs: List[str] = []
    if int(fstats.get("false_negative") or 0) > 0:
        recs.append("check threshold compare path and NaN handling in p0 kill-switch branch")
    if int(fstats.get("false_positive") or 0) > 0:
        recs.append("verify kill reasons include only threshold-based triggers")
    if int(fstats.get("provenance_scope_days") or 0) > 0:
        prv = float((full_summary.get("score") or {}).get("components", {}).get("provenance_ratio") or 0.0)
        if prv < 1.0:
            recs.append("enforce dd_source artifact write as fail-closed when kill-switch triggers")
    r_gap_mean = _to_float(rstats.get("rolling_vs_lifetime_gap_mean"))
    if r_gap_mean is not None and r_gap_mean > 0.20:
        recs.append("review rolling(mean) vs lifetime(product) method gap and governance threshold")
    if recommendation.get("policy") == "TRIAL_TUNED_SHADOW":
        recs.append("keep tuned thresholds in shadow mode for 2-4 weeks before execution use")
    if not recs:
        recs.append("current logic is consistent; keep shadow monitoring for drift")

    report = {
        "generated_at": _now_ts(),
        "window": full_summary.get("window"),
        "score": full_summary.get("score"),
        "stats": full_summary.get("stats"),
        "shadow_auto_score": full_summary.get("shadow_auto_score"),
        "recent_window_days": int(recent_days),
        "recent": {
            "window": recent_summary.get("window"),
            "score": recent_summary.get("score"),
            "stats": recent_summary.get("stats"),
            "shadow_auto_score": recent_summary.get("shadow_auto_score"),
        },
        "provenance_policy": {
            "start_ymd": provenance_start_ymd,
            "rule": "provenance_ratio scores only triggered days on/after start_ymd",
        },
        "threshold_tuning": {
            "full_window": tune_full,
            "recent_window": tune_recent,
            "recommended": recommendation,
        },
        "issues": issues,
        "recommendations": recs,
        "files": {
            "log_dir": str(LOG_DIR),
            "daily_csv": str(OUT_DAILY_CSV),
            "latest_json": str(OUT_LATEST),
        },
        "daily": rows,
    }
    return report


def _write_daily_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding="utf-8", newline="") as f:
            f.write("ymd\n")
        return
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser(description="Kill-switch validation score report (shadow mode)")
    ap.add_argument("--recent-days", type=int, default=20, help="recent window days for focused summary")
    args = ap.parse_args()

    report = build_report(recent_days=int(args.recent_days))
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    OUT_LATEST.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    ts = _now_compact()
    out_ts = LOG_DIR / f"kill_switch_validation_report_{ts}.json"
    out_ts.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    _write_daily_csv(report.get("daily") or [], OUT_DAILY_CSV)

    sc = report.get("score", {})
    st = report.get("stats", {})
    sh = report.get("shadow_auto_score", {})
    recent = report.get("recent", {})
    recent_sh = recent.get("shadow_auto_score", {}) if isinstance(recent, dict) else {}
    rec = (report.get("threshold_tuning") or {}).get("recommended") or {}
    rec_thr = rec.get("use_thresholds") or {}

    print(
        "[KILL_VALID] "
        f"score={sc.get('total')} grade={sc.get('grade')} "
        f"days={report.get('window', {}).get('days')} "
        f"mismatch={st.get('mismatch_count')} fn={st.get('false_negative')} fp={st.get('false_positive')} "
        f"shadow_agree_full={sh.get('agreement_with_actual_action_ratio')} "
        f"shadow_agree_recent={recent_sh.get('agreement_with_actual_action_ratio')}"
    )
    print(
        "[KILL_VALID_TUNE] "
        f"policy={rec.get('policy')} reason={rec.get('reason')} "
        f"block_cut={rec_thr.get('block_cut')} reduce_cut={rec_thr.get('reduce_cut')}"
    )
    print(f"[OK] wrote: {OUT_LATEST}")
    print(f"[OK] wrote: {out_ts}")
    print(f"[OK] wrote: {OUT_DAILY_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



