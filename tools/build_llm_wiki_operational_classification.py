from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT_A = Path("E:/1_Data")
ROOT_B = Path("E:/vibe/buffett")
OUT_ROOT = ROOT_A / "docs" / "llm_wiki" / "05_Logs"
REPORT_MD = OUT_ROOT / "current_operational_classification_latest.md"
REPORT_JSON = OUT_ROOT / "current_operational_classification_latest.json"


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return ""


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for encoding in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=encoding))
        except UnicodeDecodeError:
            continue
    return {}


def _extract_md_value(text: str, key: str) -> str:
    pattern = re.compile(rf"^\|\s*{re.escape(key)}\s*\|\s*`?([^`|]+)`?\s*\|", re.MULTILINE)
    match = pattern.search(text)
    return match.group(1).strip() if match else "unknown"


def _bool_text(value: str) -> bool:
    return value.strip().lower() == "true"


def _status_row(area: str, status: str, reason: str, evidence: str) -> dict[str, str]:
    return {
        "area": area,
        "status": status,
        "reason": reason,
        "evidence": evidence,
    }


def _json_status(payload: dict[str, Any], *path: str) -> str:
    value: Any = payload
    for key in path:
        if not isinstance(value, dict):
            return "unknown"
        value = value.get(key)
    return str(value) if value is not None else "unknown"


def _ledger_repair_is_effectively_clean(payload: dict[str, Any]) -> bool:
    status = _json_status(payload, "status")
    if status in {"PASS", "NOOP"}:
        return True
    if status != "APPLIED_METADATA":
        return False
    return (
        payload.get("missing_rows") == 0
        and payload.get("pure_add_rows") == 0
        and payload.get("replaced_rows") == 0
    )


def _intraday_loop_is_effectively_clean(payload: dict[str, Any]) -> bool:
    steps = payload.get("steps")
    if not isinstance(steps, list) or not steps:
        return False
    for step in steps:
        if not isinstance(step, dict):
            return False
        if step.get("ok") is True:
            continue
        if step.get("advisory_only") is True:
            continue
        if step.get("skipped") is True:
            continue
        return False
    return True


def main() -> int:
    roota_state_path = ROOT_A / "docs" / "llm_wiki" / "00_Current_State" / "system_state_latest.md"
    rootb_state_path = ROOT_B / "docs" / "llm_wiki" / "00_Current_State" / "dashboard_state_latest.md"
    readiness_path = ROOT_A / "docs" / "llm_wiki" / "05_Logs" / "external_readiness_latest.json"
    ssot_health_path = ROOT_A / "2_Logs" / "ssot_health_card_latest.json"
    ledger_dry_run_path = ROOT_A / "2_Logs" / "ledger_live_fills_dry_run_latest.json"
    ledger_repair_path = ROOT_A / "2_Logs" / "ledger_live_fills_repair_latest.json"
    reconcile_state_path = ROOT_A / "2_Logs" / "reconcile_paper_state_from_fills_latest.json"
    live_vs_bt_path = ROOT_B / "data" / "stats" / "live_vs_bt.json"
    intraday_loop_path = ROOT_A / "2_Logs" / "intraday_loop_status_latest.json"

    roota_text = _read_text(roota_state_path)
    rootb_text = _read_text(rootb_state_path)
    readiness = _read_json(readiness_path)
    ssot_health = _read_json(ssot_health_path)
    ledger_dry_run = _read_json(ledger_dry_run_path)
    ledger_repair = _read_json(ledger_repair_path)
    reconcile_state = _read_json(reconcile_state_path)
    live_vs_bt = _read_json(live_vs_bt_path)
    intraday_loop = _read_json(intraday_loop_path)

    date_status = _extract_md_value(roota_text, "date_asof_interpretation_status")
    strict_asof_matches_d = _bool_text(_extract_md_value(roota_text, "strict_asof_matches_D"))
    score_asof_matches_expected = _bool_text(_extract_md_value(roota_text, "score_asof_matches_expected"))
    orders_exec_exists = _bool_text(_extract_md_value(roota_text, "orders_exec_exists"))
    intraday_steps = _extract_md_value(roota_text, "intraday_steps")
    rootb_status = _extract_md_value(rootb_text, "status_overall")
    rootb_alerts = _extract_md_value(rootb_text, "health_alerts_count")
    external_api = readiness.get("external_api") if isinstance(readiness.get("external_api"), dict) else {}
    external_blockers = external_api.get("blockers") if isinstance(external_api.get("blockers"), list) else []
    ssot_status = _json_status(ssot_health, "overall", "status")
    ledger_dry_status = _json_status(ledger_dry_run, "status")
    ledger_repair_status = _json_status(ledger_repair, "status")
    ledger_repair_effective_clean = _ledger_repair_is_effectively_clean(ledger_repair)
    reconcile_status = _json_status(reconcile_state, "status")
    live_vs_bt_status = _json_status(live_vs_bt, "status")
    intraday_effective_clean = _intraday_loop_is_effectively_clean(intraday_loop)
    trade_chain_active = (
        ssot_status == "PASS"
        and ledger_dry_status == "PASS"
        and ledger_repair_effective_clean
        and reconcile_status == "PASS"
        and live_vs_bt_status == "PASS"
    )

    rows: list[dict[str, str]] = []
    rows.append(
        _status_row(
            "RootA score context",
            "ACTIVE" if score_asof_matches_expected and orders_exec_exists else "BLOCKED",
            f"orders_exec_exists={orders_exec_exists}, score_asof_matches_expected={score_asof_matches_expected}",
            str(roota_state_path),
        )
    )
    rows.append(
        _status_row(
            "RootA order/fill/ledger/stat interpretation",
            "ACTIVE" if trade_chain_active else "REVIEW",
            (
                f"ssot={ssot_status}, ledger_dry={ledger_dry_status}, "
                f"ledger_repair={ledger_repair_status}, reconcile_state={reconcile_status}, "
                f"live_vs_bt={live_vs_bt_status}, date_note={date_status}, "
                f"ledger_repair_effective_clean={ledger_repair_effective_clean}"
            ),
            str(ssot_health_path),
        )
    )
    rows.append(
        _status_row(
            "RootA score date strict-D note",
            "NOTE" if not strict_asof_matches_d else "ACTIVE",
            f"strict_asof_matches_D={strict_asof_matches_d}, date_asof_interpretation_status={date_status}",
            str(ROOT_A / "docs" / "llm_wiki" / "01_Policies" / "date_asof_interpretation.md"),
        )
    )
    rows.append(
        _status_row(
            "RootA intraday loop summary",
            "ACTIVE" if intraday_effective_clean else "REVIEW",
            f"intraday_steps={intraday_steps}, intraday_effective_clean={intraday_effective_clean}",
            str(roota_state_path),
        )
    )
    rows.append(
        _status_row(
            "RootB dashboard context",
            "ACTIVE" if rootb_status == "PASS" and rootb_alerts in ("0", "0.0") else "REVIEW",
            f"status_overall={rootb_status}, health_alerts_count={rootb_alerts}",
            str(rootb_state_path),
        )
    )
    rows.append(
        _status_row(
            "Slack and meeting external API",
            "DEFERRED" if external_blockers else "ACTIVE",
            "blockers=" + ",".join(str(x) for x in external_blockers),
            str(readiness_path),
        )
    )
    rows.append(
        _status_row(
            "Trading logic behavior",
            "NOT_CHANGED",
            "Wiki/reporting layer only; no Gate, STOP, LOCK, risk, score, order, fill, ledger, stats behavior changed.",
            str(ROOT_A / "docs" / "llm_wiki" / "wiki_index_latest.md"),
        )
    )

    generated_at = datetime.now().isoformat(timespec="seconds")
    payload = {
        "generated_at": generated_at,
        "scope": "llm_wiki_operational_classification",
        "policy_effect": False,
        "trading_effect": False,
        "summary_status": (
            "BLOCKED"
            if any(row["status"] == "BLOCKED" for row in rows)
            else "REVIEW"
            if any(row["status"] == "REVIEW" for row in rows)
            else "ACTIVE"
        ),
        "rows": rows,
    }

    md_lines = [
        "# Current Operational Classification",
        "",
        f"generated_at: {generated_at}",
        "scope: `llm_wiki_operational_classification`",
        "policy_effect: `false`",
        "trading_effect: `false`",
        f"summary_status: `{payload['summary_status']}`",
        "",
        "| area | status | reason | evidence |",
        "|---|---|---|---|",
    ]
    for row in rows:
        md_lines.append(
            f"| {row['area']} | `{row['status']}` | {row['reason']} | `{row['evidence']}` |"
        )
    md_lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This report classifies current Wiki/operations context for Codex reading.",
            "- It does not approve trading or change runtime behavior.",
            "- `REVIEW` means inspect canonical artifacts before interpreting the related area.",
            "- `NOTE` means the condition should be visible in reporting but is not currently a blocker.",
            "- `DEFERRED` means intentionally not connected or not configured.",
            "",
        ]
    )

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    REPORT_MD.write_text("\n".join(md_lines), encoding="utf-8", newline="\n")
    print(str(REPORT_MD))
    print(str(REPORT_JSON))
    print(f"summary_status={payload['summary_status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
