from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
DEFAULT_REVIEW = LOG_DIR / "safe_exploration_review_latest.json"
DEFAULT_OUTPUT = LOG_DIR / "safe_exploration_apply_gate_latest.json"


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
        except Exception:
            return {}
    return {}


def build_apply_gate(review: Dict[str, Any], *, approve: bool) -> Dict[str, Any]:
    status = str(review.get("status") or "UNKNOWN").upper()
    apply_status = str(review.get("apply_status") or "UNKNOWN").upper()
    live_pct = float(review.get("live_pct") or 0.0)
    reasons: List[str] = []

    if not approve:
        reasons.append("APPROVAL_FLAG_MISSING")
    if status != "SAFE_PASS":
        reasons.append(f"REVIEW_NOT_SAFE_PASS({status})")
    if apply_status != "WAITING_APPROVAL":
        reasons.append(f"REVIEW_NOT_WAITING_APPROVAL({apply_status})")
    if live_pct != 0.0:
        reasons.append(f"LIVE_PCT_NOT_ZERO({live_pct})")

    allowed = not reasons
    action = "WOULD_APPLY_AFTER_SEPARATE_IMPLEMENTATION" if allowed else "BLOCKED"
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "APPLY_GATE_PASS" if allowed else "APPLY_GATE_BLOCK",
        "action": action,
        "approval_flag": bool(approve),
        "review_status": status,
        "review_apply_status": apply_status,
        "review_live_pct": live_pct,
        "orders_modified": False,
        "fills_modified": False,
        "ledger_modified": False,
        "stats_modified": False,
        "gate_modified": False,
        "risk_lock_modified": False,
        "runtime_config_modified": False,
        "reasons": reasons,
        "blocked_for_auto_apply": [
            "This gate is evidence-only and does not call config lock, order, fill, ledger, stats, Gate, or broker paths.",
            "A separate implementation is required before any real apply operation.",
        ],
    }


def write_gate(payload: Dict[str, Any], output_json: Path) -> Dict[str, str]:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    ts = _now_ts()
    dated_json = output_json.with_name(f"safe_exploration_apply_gate_{ts}.json")
    txt = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    output_json.write_text(txt, encoding="utf-8")
    dated_json.write_text(txt, encoding="utf-8")

    lines = [
        f"[FINAL] {payload.get('status')} action={payload.get('action')}",
        f"approval_flag={payload.get('approval_flag')}",
        f"review_status={payload.get('review_status')} review_apply_status={payload.get('review_apply_status')} review_live_pct={payload.get('review_live_pct')}",
        f"orders_modified={payload.get('orders_modified')} fills_modified={payload.get('fills_modified')} ledger_modified={payload.get('ledger_modified')} stats_modified={payload.get('stats_modified')}",
        f"gate_modified={payload.get('gate_modified')} risk_lock_modified={payload.get('risk_lock_modified')} runtime_config_modified={payload.get('runtime_config_modified')}",
        f"reasons={','.join(payload.get('reasons') or []) or '-'}",
    ]
    summary = "\n".join(lines) + "\n"
    latest_txt = output_json.with_suffix(".txt")
    dated_txt = output_json.with_name(f"safe_exploration_apply_gate_{ts}.txt")
    latest_txt.write_text(summary, encoding="utf-8")
    dated_txt.write_text(summary, encoding="utf-8")
    return {
        "latest_json": str(output_json),
        "dated_json": str(dated_json),
        "latest_txt": str(latest_txt),
        "dated_txt": str(dated_txt),
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Evidence-only apply gate for Safe Exploration review.")
    p.add_argument("--review-json", default=str(DEFAULT_REVIEW))
    p.add_argument("--output-json", default=str(DEFAULT_OUTPUT))
    p.add_argument("--approve", action="store_true")
    return p


def main() -> int:
    args = build_parser().parse_args()
    payload = build_apply_gate(_read_json(Path(args.review_json)), approve=bool(args.approve))
    paths = write_gate(payload, Path(args.output_json))
    print(f"[FINAL] {payload['status']} action={payload['action']}")
    print(f"[SAFE_EXPLORATION_APPLY_GATE] latest_json={paths['latest_json']}")
    return 0 if payload["status"] == "APPLY_GATE_PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
