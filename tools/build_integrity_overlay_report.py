from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT_GLOB = str(ROOT / "checkfile" / "outputs" / "verification_report_*.json")
LOG_DIR = ROOT / "2_Logs"
DOC_DIR = ROOT / "docs" / "03-operations"

CATEGORIES = {
    "entity_integrity": "개체무결성",
    "referential_integrity": "참조무결성",
    "domain_integrity": "도메인무결성",
    "business_rule_validation": "비즈니스규칙검증",
    "batch_validation": "배치검증",
    "checksum_validation": "체크섬",
    "sampling_validation": "샘플링검증",
    "input_validation": "입력값검증",
    "state_transition_validation": "상태전이검증",
}

# Primary mapping for current 55 verification items.
ITEM_CATEGORY_MAP: Dict[str, List[str]] = {
    "OS compatibility": ["input_validation"],
    "Cost structure": ["domain_integrity", "business_rule_validation"],
    "Architecture suitability": ["business_rule_validation"],
    "API rate-limit compliance": ["domain_integrity", "business_rule_validation"],
    "Market calendar alignment": ["domain_integrity", "business_rule_validation"],
    "Process isolation": ["state_transition_validation"],
    "Message queue performance": ["domain_integrity", "batch_validation"],
    "DB schema efficiency": ["entity_integrity"],
    "Module separation": ["entity_integrity"],
    "Exactly-once idempotency": ["entity_integrity", "state_transition_validation", "business_rule_validation"],
    "Draft check": ["input_validation"],
    "Flowcharting": ["state_transition_validation"],
    "Dry run": ["state_transition_validation", "sampling_validation"],
    "Edge case test": ["domain_integrity", "business_rule_validation"],
    "Cross-check": ["referential_integrity", "sampling_validation"],
    "Stress test": ["batch_validation"],
    "Survivorship bias": ["sampling_validation", "business_rule_validation"],
    "Look-ahead bias": ["business_rule_validation", "state_transition_validation"],
    "Point-in-time snapshot": ["referential_integrity", "state_transition_validation"],
    "Data integrity": ["entity_integrity", "referential_integrity"],
    "Adjusted price handling": ["domain_integrity"],
    "Overfitting": ["sampling_validation", "business_rule_validation"],
    "Walk-forward regime robustness": ["sampling_validation", "state_transition_validation"],
    "Randomness test": ["sampling_validation"],
    "Max drawdown": ["domain_integrity", "business_rule_validation"],
    "Slippage modeling": ["domain_integrity"],
    "Liquidity constraints": ["domain_integrity", "business_rule_validation"],
    "Partial-fill handling": ["state_transition_validation", "business_rule_validation"],
    "Order rejection handling": ["state_transition_validation", "business_rule_validation"],
    "Network disconnection handling": ["state_transition_validation"],
    "Order state machine": ["state_transition_validation"],
    "Event sequence integrity": ["entity_integrity", "state_transition_validation"],
    "API error-code handling": ["input_validation", "state_transition_validation"],
    "Order limits": ["business_rule_validation", "domain_integrity"],
    "Portfolio exposure limits": ["business_rule_validation", "domain_integrity"],
    "Kill switch": ["business_rule_validation", "state_transition_validation"],
    "Duplicate-order prevention": ["entity_integrity", "business_rule_validation"],
    "Price deviation guard": ["domain_integrity", "business_rule_validation"],
    "Cost optimization": ["business_rule_validation"],
    "API connection stability": ["batch_validation", "state_transition_validation"],
    "Execution consistency": ["referential_integrity", "state_transition_validation"],
    "Broker statement reconciliation": ["referential_integrity", "sampling_validation"],
    "Tax/fee calculation": ["domain_integrity", "business_rule_validation"],
    "Paper-trading awareness": ["business_rule_validation", "state_transition_validation"],
    "Canary readiness": ["state_transition_validation", "business_rule_validation"],
    "Realtime websocket pipeline": ["state_transition_validation", "batch_validation"],
    "Emergency full liquidation": ["state_transition_validation", "business_rule_validation"],
    "Alert channel delivery": ["batch_validation", "state_transition_validation"],
    "Soak test automation": ["batch_validation", "state_transition_validation"],
    "Auto reconnection": ["state_transition_validation", "batch_validation"],
    "Log integrity": ["checksum_validation", "entity_integrity"],
    "Data backup": ["checksum_validation", "batch_validation"],
    "Backup drill runbook": ["batch_validation", "state_transition_validation"],
    "Operations scheduler": ["batch_validation"],
    "Monitoring alerts": ["batch_validation", "state_transition_validation"],
}

STATUS_PASS = {"PASSED"}
STATUS_WARN = {"WARNING"}
STATUS_FAIL = {"FAILED", "ERROR"}
STATUS_SKIP = {"SKIPPED"}


def _now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _pick_latest_report(glob_pat: str) -> Path:
    paths = sorted(glob.glob(glob_pat))
    if not paths:
        raise FileNotFoundError(f"no verification report found: {glob_pat}")
    return Path(paths[-1])


def _status_bucket(status: str) -> str:
    s = str(status or "").upper().strip()
    if s in STATUS_PASS:
        return "pass"
    if s in STATUS_WARN:
        return "warn"
    if s in STATUS_FAIL:
        return "fail"
    if s in STATUS_SKIP:
        return "skip"
    return "unknown"


def _flatten_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    phases = payload.get("phases", {}) or {}
    for phase_name, phase_obj in phases.items():
        for r in phase_obj.get("results", []) or []:
            out.append(
                {
                    "phase": phase_name,
                    "item_name": str(r.get("item_name", "")).strip(),
                    "status": str(r.get("status", "")).strip().upper(),
                    "message": str(r.get("message", "")).strip(),
                    "criteria": str(r.get("criteria", "")).strip(),
                }
            )
    return out


def build_overlay(report_path: Path) -> Dict[str, Any]:
    data = json.loads(report_path.read_text(encoding="utf-8-sig"))
    items = _flatten_items(data)

    category_stats: Dict[str, Dict[str, Any]] = {}
    for key, label in CATEGORIES.items():
        category_stats[key] = {
            "label": label,
            "total": 0,
            "pass": 0,
            "warn": 0,
            "fail": 0,
            "skip": 0,
            "unknown": 0,
            "coverage": [],
        }

    uncovered: List[str] = []
    per_item: List[Dict[str, Any]] = []

    for it in items:
        name = it["item_name"]
        cats = ITEM_CATEGORY_MAP.get(name, [])
        if not cats:
            uncovered.append(name)
        bucket = _status_bucket(it["status"])

        for c in cats:
            st = category_stats[c]
            st["total"] += 1
            st[bucket] += 1
            st["coverage"].append(name)

        per_item.append({**it, "integrity_categories": [CATEGORIES[c] for c in cats]})

    for key in list(category_stats.keys()):
        st = category_stats[key]
        total = int(st["total"])
        st["pass_rate_pct"] = round((st["pass"] / total) * 100.0, 1) if total > 0 else 0.0
        st["unique_items"] = sorted(set(st["coverage"]))
        st.pop("coverage", None)

    summary = {
        "generated_at": _now(),
        "source_report": str(report_path),
        "source_total_items": len(items),
        "mapped_items": len([x for x in items if x["item_name"] in ITEM_CATEGORY_MAP]),
        "unmapped_items": sorted(set(uncovered)),
        "categories_covered": len([k for k, v in category_stats.items() if int(v["total"]) > 0]),
        "categories_total": len(CATEGORIES),
    }

    return {
        "summary": summary,
        "category_stats": category_stats,
        "items": per_item,
    }


def _render_md(report: Dict[str, Any]) -> str:
    s = report["summary"]
    lines: List[str] = []
    lines.append(f"# 정합성 오버레이 리포트 ({dt.datetime.now().strftime('%Y-%m-%d')})")
    lines.append("")
    lines.append(f"- source_report: `{s['source_report']}`")
    lines.append(f"- source_total_items: **{s['source_total_items']}**")
    lines.append(f"- mapped_items: **{s['mapped_items']}**")
    lines.append(f"- categories_covered: **{s['categories_covered']} / {s['categories_total']}**")
    lines.append("")
    lines.append("## 카테고리 집계")
    lines.append("")
    lines.append("| 카테고리 | total | pass | warn | fail | skip | pass_rate |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for key, st in report["category_stats"].items():
        lines.append(
            f"| {st['label']} | {st['total']} | {st['pass']} | {st['warn']} | {st['fail']} | {st['skip']} | {st['pass_rate_pct']}% |"
        )

    if s.get("unmapped_items"):
        lines.append("")
        lines.append("## 미매핑 항목")
        lines.append("")
        for name in s["unmapped_items"]:
            lines.append(f"- {name}")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Build integrity overlay report from verification report")
    ap.add_argument("--report-json", default="", help="input verification report JSON (default: latest)")
    ap.add_argument("--out-json", default="", help="output overlay JSON path")
    ap.add_argument("--out-md", default="", help="output overlay Markdown path")
    args = ap.parse_args()

    report_path = Path(args.report_json) if args.report_json else _pick_latest_report(DEFAULT_REPORT_GLOB)

    overlay = build_overlay(report_path)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    DOC_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"integrity_overlay_{stamp}.json")
    out_latest = LOG_DIR / "integrity_overlay_latest.json"
    out_md = Path(args.out_md) if args.out_md else (DOC_DIR / f"integrity_overlay_{dt.datetime.now().strftime('%Y%m%d')}.md")

    out_json.write_text(json.dumps(overlay, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    out_latest.write_text(json.dumps(overlay, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    out_md.write_text(_render_md(overlay), encoding="utf-8-sig")

    print(f"[OK] source={report_path}")
    print(f"[OK] json={out_json}")
    print(f"[OK] latest={out_latest}")
    print(f"[OK] md={out_md}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

