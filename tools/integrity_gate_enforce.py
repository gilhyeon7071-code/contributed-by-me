from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
DOC_DIR = ROOT / "docs" / "03-operations"
DEFAULT_OVERLAY = LOG_DIR / "integrity_overlay_latest.json"

TARGETS: Dict[str, Dict[str, Any]] = {
    "sampling_validation": {
        "label": "샘플링검증",
        "min_pass_rate": 65.0,
        "max_fail": 0,
        "max_warn_skip": 2,
    },
    "business_rule_validation": {
        "label": "비즈니스규칙검증",
        "min_pass_rate": 75.0,
        "max_fail": 0,
        "max_warn_skip": 6,
    },
    "domain_integrity": {
        "label": "도메인무결성",
        "min_pass_rate": 80.0,
        "max_fail": 0,
        "max_warn_skip": 4,
    },
}


def _now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _safe_int(v: Any) -> int:
    try:
        return int(v)
    except Exception:
        return 0


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _phase_breakdown(items: List[Dict[str, Any]], category_label: str) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for it in items:
        cats = it.get("integrity_categories", []) or []
        if category_label not in cats:
            continue
        phase = str(it.get("phase", "UNKNOWN") or "UNKNOWN")
        st = str(it.get("status", "")).upper()
        if phase not in out:
            out[phase] = {"total": 0, "pass": 0, "warn": 0, "fail": 0, "skip": 0}
        out[phase]["total"] += 1
        if st == "PASSED":
            out[phase]["pass"] += 1
        elif st == "WARNING":
            out[phase]["warn"] += 1
        elif st in {"FAILED", "ERROR"}:
            out[phase]["fail"] += 1
        elif st == "SKIPPED":
            out[phase]["skip"] += 1
    return out


def _issue_items(items: List[Dict[str, Any]], category_label: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        cats = it.get("integrity_categories", []) or []
        if category_label not in cats:
            continue
        st = str(it.get("status", "")).upper()
        if st == "PASSED":
            continue
        out.append(
            {
                "phase": str(it.get("phase", "")),
                "item_name": str(it.get("item_name", "")),
                "status": st,
                "message": str(it.get("message", "")),
            }
        )
    return out


def evaluate(overlay: Dict[str, Any]) -> Dict[str, Any]:
    cat_stats = overlay.get("category_stats", {}) or {}
    items = overlay.get("items", []) or []

    results: List[Dict[str, Any]] = []
    pass_n = 0
    fail_n = 0

    for key, rule in TARGETS.items():
        label = str(rule["label"])
        stat = cat_stats.get(key, {}) or {}
        pass_rate = _safe_float(stat.get("pass_rate_pct", 0.0))
        fail_count = _safe_int(stat.get("fail", 0))
        warn_count = _safe_int(stat.get("warn", 0))
        skip_count = _safe_int(stat.get("skip", 0))
        total = _safe_int(stat.get("total", 0))
        warn_skip = warn_count + skip_count

        checks = [
            {
                "name": "pass_rate",
                "ok": pass_rate >= float(rule["min_pass_rate"]),
                "actual": pass_rate,
                "expected": f">= {float(rule['min_pass_rate']):.1f}",
            },
            {
                "name": "fail_count",
                "ok": fail_count <= int(rule["max_fail"]),
                "actual": fail_count,
                "expected": f"<= {int(rule['max_fail'])}",
            },
            {
                "name": "warn_skip_count",
                "ok": warn_skip <= int(rule["max_warn_skip"]),
                "actual": warn_skip,
                "expected": f"<= {int(rule['max_warn_skip'])}",
            },
        ]

        ok = all(bool(c["ok"]) for c in checks)
        if ok:
            pass_n += 1
        else:
            fail_n += 1

        results.append(
            {
                "category_key": key,
                "category_label": label,
                "ok": ok,
                "metrics": {
                    "total": total,
                    "pass_rate_pct": pass_rate,
                    "pass": _safe_int(stat.get("pass", 0)),
                    "warn": warn_count,
                    "fail": fail_count,
                    "skip": skip_count,
                },
                "checks": checks,
                "phase_breakdown": _phase_breakdown(items, label),
                "issue_items": _issue_items(items, label),
            }
        )

    return {
        "generated_at": _now(),
        "source_overlay": str(DEFAULT_OVERLAY),
        "ok": fail_n == 0,
        "pass_n": pass_n,
        "fail_n": fail_n,
        "targets": results,
    }


def render_md(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# Integrity Gate Report ({dt.datetime.now().strftime('%Y-%m-%d')})")
    lines.append("")
    lines.append(f"- source_overlay: `{payload.get('source_overlay','')}`")
    lines.append(f"- gate_ok: **{payload.get('ok', False)}**")
    lines.append(f"- pass_n/fail_n: **{payload.get('pass_n',0)} / {payload.get('fail_n',0)}**")
    lines.append("")

    for t in payload.get("targets", []) or []:
        m = t.get("metrics", {}) or {}
        lines.append(f"## {t.get('category_label')} ({'PASS' if t.get('ok') else 'FAIL'})")
        lines.append("")
        lines.append(
            f"- total={m.get('total',0)} pass={m.get('pass',0)} warn={m.get('warn',0)} fail={m.get('fail',0)} skip={m.get('skip',0)} pass_rate={m.get('pass_rate_pct',0)}%"
        )
        lines.append("- checks")
        for c in t.get("checks", []) or []:
            lines.append(
                f"  - {c.get('name')}: {'OK' if c.get('ok') else 'FAIL'} (actual={c.get('actual')} expected={c.get('expected')})"
            )

        issues = t.get("issue_items", []) or []
        if issues:
            lines.append("- issue_items")
            for it in issues[:20]:
                lines.append(
                    f"  - [{it.get('phase')}] {it.get('item_name')} / {it.get('status')} / {it.get('message')}"
                )

        lines.append("")

    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Enforce integrity gate for weak categories")
    ap.add_argument("--overlay-json", default=str(DEFAULT_OVERLAY))
    ap.add_argument("--out-json", default="")
    ap.add_argument("--out-md", default="")
    ap.add_argument("--warn-only", action="store_true", help="do not fail exit code even if gate fails")
    args = ap.parse_args()

    overlay_path = Path(args.overlay_json)
    if not overlay_path.exists():
        raise SystemExit(f"overlay not found: {overlay_path}")

    overlay = json.loads(overlay_path.read_text(encoding="utf-8-sig"))
    result = evaluate(overlay)
    result["source_overlay"] = str(overlay_path)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    DOC_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"integrity_gate_{stamp}.json")
    out_latest = LOG_DIR / "integrity_gate_latest.json"
    out_md = Path(args.out_md) if args.out_md else (DOC_DIR / f"integrity_gate_{dt.datetime.now().strftime('%Y%m%d')}.md")

    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    out_latest.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    out_md.write_text(render_md(result), encoding="utf-8-sig")

    print(f"[GATE] ok={result['ok']} pass={result['pass_n']} fail={result['fail_n']}")
    print(f"[GATE] json={out_json}")
    print(f"[GATE] latest={out_latest}")
    print(f"[GATE] md={out_md}")

    if args.warn_only:
        return 0
    return 0 if bool(result.get("ok", False)) else 2


if __name__ == "__main__":
    raise SystemExit(main())

