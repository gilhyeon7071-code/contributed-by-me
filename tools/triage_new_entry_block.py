from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

ROOTB = Path(r"E:\vibe\buffett")
ROOTA = Path(r"E:\1_Data")
STATE_PATH = ROOTB / "runs" / "dashboard_state_latest.json"
SSOT_HEALTH_PATH = ROOTA / "2_Logs" / "ssot_health_card_latest.json"
PORT = 8501


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _fmt_age_minutes(path: Path) -> str:
    if not path.exists():
        return "missing"
    age_sec = max(0.0, datetime.now().timestamp() - path.stat().st_mtime)
    return f"{age_sec / 60.0:.1f}m"


def _listening_pids(port: int) -> list[int]:
    try:
        out = subprocess.check_output(["netstat", "-ano"], text=True, encoding="utf-8", errors="ignore")
    except Exception:
        return []
    pids: list[int] = []
    token = f":{port}"
    for line in out.splitlines():
        s = " ".join(line.split())
        if not s:
            continue
        if token not in s:
            continue
        parts = s.split(" ")
        if len(parts) < 5:
            continue
        state = parts[3].upper()
        if state != "LISTENING":
            continue
        try:
            pids.append(int(parts[4]))
        except Exception:
            continue
    return sorted(set(pids))


def main() -> int:
    state = _read_json(STATE_PATH)
    health = state.get("health") if isinstance(state.get("health"), dict) else {}
    gate_summary = state.get("gate_summary") if isinstance(state.get("gate_summary"), dict) else {}
    fg = health.get("freshness_guard") if isinstance(health.get("freshness_guard"), dict) else {}
    if not fg and isinstance(state.get("freshness_guard"), dict):
        fg = state.get("freshness_guard")  # type: ignore[assignment]
    if not fg and isinstance(gate_summary.get("freshness_guard"), dict):
        fg = gate_summary.get("freshness_guard")  # type: ignore[assignment]

    blocked = fg.get("blocked")
    realtime_issues = fg.get("realtime_issues") if isinstance(fg.get("realtime_issues"), list) else []
    daily_mismatch = fg.get("daily_mismatch") if isinstance(fg.get("daily_mismatch"), list) else []
    reasons = fg.get("reasons") if isinstance(fg.get("reasons"), list) else []
    action = gate_summary.get("action")
    state_asof = state.get("as_of_ymd")
    state_overall = state.get("overall")
    state_mtime = datetime.fromtimestamp(STATE_PATH.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if STATE_PATH.exists() else "missing"

    ssot_mtime = datetime.fromtimestamp(SSOT_HEALTH_PATH.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if SSOT_HEALTH_PATH.exists() else "missing"
    ssot_age = _fmt_age_minutes(SSOT_HEALTH_PATH)
    pids = _listening_pids(PORT)

    print("[TRIAGE] new_entry_block")
    print(f"- state.path: {STATE_PATH}")
    print(f"- state.mtime: {state_mtime}")
    print(f"- state.as_of_ymd: {state_asof}")
    print(f"- state.overall: {state_overall}")
    print(f"- gate_summary.action: {action}")
    print(f"- freshness_guard.blocked: {blocked}")
    print(f"- freshness_guard.realtime_issues: {len(realtime_issues)}")
    print(f"- freshness_guard.daily_mismatch: {len(daily_mismatch)}")
    print(f"- freshness_guard.reasons: {len(reasons)}")
    print(f"- ssot_health_card.path: {SSOT_HEALTH_PATH}")
    print(f"- ssot_health_card.mtime: {ssot_mtime}")
    print(f"- ssot_health_card.age: {ssot_age}")
    print(f"- dashboard.port: {PORT}")
    print(f"- dashboard.listening_pids: {pids if pids else 'none'}")
    if len(pids) > 1:
        print("- diagnosis: DUPLICATE_DASHBOARD_PROCESS")
    elif blocked in (True, "true", "True", 1, "1"):
        print("- diagnosis: FRESHNESS_GUARD_BLOCKED")
    elif action and str(action).upper() not in {"ALLOW", "PASS", "OPEN", "-"}:
        print("- diagnosis: GATE_BLOCKED")
    else:
        print("- diagnosis: NO_BLOCK_ROOT_DETECTED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
