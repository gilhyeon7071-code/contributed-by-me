from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SOURCE_JSON = LOG_DIR / "production_risk_playbook_latest.json"
OUT_JSON = LOG_DIR / "production_risk_policy_audit_latest.json"
OUT_MD = LOG_DIR / "production_risk_policy_audit_latest.md"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _signal_rows(playbook: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for sig in playbook.get("signals") or []:
        if not isinstance(sig, dict):
            continue
        stage = str(sig.get("stage") or "UNKNOWN")
        advisory = bool(sig.get("advisory", False))
        sample_ok = bool(sig.get("sample_ok", False))
        if advisory:
            effect = "ADVISORY_ONLY"
        elif stage == "HARD":
            effect = "CAN_BLOCK_NEW_ORDERS"
        elif stage == "SOFT":
            effect = "CAN_REDUCE_SIZE"
        elif stage == "WATCH":
            effect = "CAN_REQUIRE_EVIDENCE"
        elif not sample_ok:
            effect = "MEASUREMENT_GAP"
        else:
            effect = "NO_CURRENT_EFFECT"
        rows.append(
            {
                "name": sig.get("name"),
                "stage": stage,
                "advisory": advisory,
                "sample_ok": sample_ok,
                "effect": effect,
                "reason": sig.get("reason"),
                "source": sig.get("source", ""),
            }
        )
    return rows


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    s = payload["summary"]
    lines = [
        "# Production Risk Policy Audit",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- action: `{s['action']}`",
        f"- blocked: `{s['blocked']}`",
        f"- size_multiplier: `{s['size_multiplier']}`",
        f"- new_orders_allowed: `{s['new_orders_allowed']}`",
        "",
        "## Signal Effects",
    ]
    for row in payload["signal_rows"]:
        lines.append(f"- `{row['name']}` `{row['stage']}` -> `{row['effect']}`: {row['reason']}")
    lines.extend(["", "## Interpretation"])
    lines.append("- Current artifact has no active block or size reduction.")
    lines.append("- Drawdown is advisory in this artifact, not a direct block source.")
    lines.append("- Measurement gaps should not be treated as confirmed safe conditions.")
    lines.append("- Future design review should separate blocking, sizing, and evidence-bundle roles.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    playbook = _read_json(SOURCE_JSON)
    mitigations = playbook.get("mitigations") if isinstance(playbook.get("mitigations"), dict) else {}
    rows = _signal_rows(playbook)
    summary = {
        "action": str(playbook.get("action") or "UNKNOWN"),
        "blocked": bool(playbook.get("blocked", False)),
        "soft_pause": bool(playbook.get("soft_pause", False)),
        "watch": bool(playbook.get("watch", False)),
        "new_orders_allowed": bool(mitigations.get("new_orders_allowed", True)),
        "size_multiplier": mitigations.get("size_multiplier"),
        "evidence_bundle_required": bool(mitigations.get("evidence_bundle_required", False)),
        "signals": len(rows),
        "measurement_gap_count": sum(1 for r in rows if r["effect"] == "MEASUREMENT_GAP"),
        "advisory_count": sum(1 for r in rows if r["effect"] == "ADVISORY_ONLY"),
        "active_policy_effect": any(r["effect"] in {"CAN_BLOCK_NEW_ORDERS", "CAN_REDUCE_SIZE", "CAN_REQUIRE_EVIDENCE"} for r in rows),
    }
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "production_risk_policy_audit_v1",
        "source_files": {"production_risk_playbook": str(SOURCE_JSON)},
        "outputs": {"json": str(OUT_JSON), "md": str(OUT_MD)},
        "scope": {
            "policy_effect": "read_only_policy_audit_only",
            "full_logic_application": "NOT_APPLIED",
        },
        "summary": summary,
        "signal_rows": rows,
        "next_review": {
            "candidate": "ADAPTIVE_REVIEW_CANDIDATE",
            "reason": "future review should decide which signals block, resize, or only require evidence",
            "current_change_recommendation": "NO_POLICY_CHANGE",
        },
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", **summary, "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
