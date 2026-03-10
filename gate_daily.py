# gate_daily.py
# - Summarize daily gates based on latest p0_daily_check_*.json
# - Writes: 2_Logs/gate_daily_YYYYMMDD.json

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# 濡쒓퉭 ?ㅼ젙
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("gate_daily")

# 怨듯넻 ?좏떥由ы떚 紐⑤뱢 import
from utils.common import now_ymd, read_json, latest_file


@dataclass
class GateResult:
    ok: bool
    code: str
    msg: str

# _now_tag, read_json, latest_file -> utils.common?쇰줈 ?대룞??


def _gate(ok: bool, msg_ok: str, msg_fail: str) -> GateResult:
    return GateResult(ok=ok, code="PASS" if ok else "FAIL", msg=msg_ok if ok else msg_fail)


def _fmt(g: GateResult) -> str:
    return "PASS" if g.ok else "FAIL"


def _risk_off_engine_action(p0: Dict[str, Any], reasons: List[str]) -> Dict[str, Any]:
    """Decide what paper_engine should do when risk_off is enabled.

    Returns:
      {"action": "ALLOW"|"BLOCK"|"REDUCE", "modes": {...}, "hard_block": [...]}
    """
    modes: Dict[str, str] = {}
    for r in (reasons or []):
        mode = ""
        if r == "crash_risk_off":
            mode = (((p0.get("crash_risk_off") or {}).get("limits") or {}).get("mode") or "")
        elif r == "kill_switch":
            mode = (((p0.get("kill_switch") or {}).get("limits") or {}).get("mode") or "")
        mode = str(mode).upper().strip()
        if mode:
            modes[r] = mode

    # Prefer REDUCE for kill_switch when mode is missing/BLOCK for operation continuity.
    # Hard data-integrity reasons still override to BLOCK below.
    if "kill_switch" in [str(x) for x in (reasons or [])]:
        ks_mode = str(modes.get("kill_switch") or "").upper()
        if ks_mode in {"", "BLOCK"}:
            modes["kill_switch"] = "REDUCE"

    if any(v == "BLOCK" for v in modes.values()):
        action = "BLOCK"
    elif any(v == "REDUCE" for v in modes.values()):
        action = "REDUCE"
    elif "kill_switch" in [str(x) for x in (reasons or [])]:
        action = "REDUCE"
    else:
        action = "BLOCK"

    # HARD BLOCK: data integrity issues (universe shrink/date mismatch) must block.
    hard_block = [
        r for r in (reasons or [])
        if str(r).startswith("krx_clean_universe_degraded")
        or str(r).startswith("cand_latest_date")
        or ("prices_date_max(" in str(r) and "meta_latest_date(" in str(r))
    ]

    if hard_block:
        action = "BLOCK"
        for r in hard_block:
            modes[str(r)] = "BLOCK"

    return {"action": action, "modes": modes, "hard_block": hard_block}

def main() -> int:
    root = Path(__file__).resolve().parent
    logs_dir = root / "2_Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    p0_path = latest_file(logs_dir, "p0_daily_check_*.json")
    p0 = read_json(p0_path) if p0_path else None

    gate0 = _gate(
        ok=bool(p0),
        msg_ok="ok",
        msg_fail="missing_p0_daily_check_json",
    )

    risk_off = (p0 or {}).get("risk_off") or {}
    reasons = list((risk_off or {}).get("reasons") or [])
    risk_off_enabled = bool((risk_off or {}).get("enabled"))

    gate1 = _gate(
        ok=(not risk_off_enabled),
        msg_ok="ok",
        msg_fail=("risk_off_enabled" + (":" + ",".join(reasons) if reasons else "")),
    )

    kill_switch = (p0 or {}).get("kill_switch") or {}
    ks_triggered = bool(kill_switch.get("triggered"))

    gate2 = _gate(
        ok=(not ks_triggered),
        msg_ok="ok",
        msg_fail="kill_switch_triggered",
    )

    # Always produce structured engine_action (so callers can read it without parsing 'note')
    engine_action: Dict[str, Any] = {"action": "ALLOW", "modes": {}, "hard_block": []}
    note_msg: Optional[str] = None

    if risk_off_enabled:
        engine_action = _risk_off_engine_action(p0 or {}, reasons)
        action = str(engine_action.get("action") or "BLOCK").upper()
        modes = engine_action.get("modes") or {}
        modes_s = ",".join([f"{k}:{v}" for k, v in modes.items()]) if isinstance(modes, dict) else ""

        if action == "REDUCE":
            note_msg = f"risk_off=ON -> paper_engine will reduce new entries. reasons={','.join(reasons)}"
            if modes_s:
                note_msg = note_msg + f" modes={modes_s}"
        else:
            note_msg = f"risk_off=ON -> paper_engine will block new entries. reasons={','.join(reasons)}"
            if modes_s:
                note_msg = note_msg + f" modes={modes_s}"

    flags = {
        "risk_off_enabled": risk_off_enabled,
        "kill_switch_triggered": ks_triggered,
    }


    # --- gate_macro (Phase1) ---
    macro_path = latest_file(logs_dir, "macro_signal_latest.json")
    macro = read_json(macro_path) if macro_path else None

    # Macro gate is advisory (soft): never hard-fail trading by itself.
    # Hard blocking remains the responsibility of risk_off / kill_switch.
    macro_asof = str((macro or {}).get("as_of_ymd") or "")
    macro_is_today = (macro_asof == now_ymd())
    macro_risk_on = bool((macro or {}).get("risk_on", True))

    if (not macro) or (not macro_is_today):
        gate_macro = _gate(ok=True, msg_ok="macro_missing_or_stale", msg_fail="macro_unused")
    else:
        regime = str((macro or {}).get("regime") or "unknown")
        msg = "macro_risk_on" if macro_risk_on else f"macro_risk_off_soft:{regime}"
        gate_macro = _gate(ok=True, msg_ok=msg, msg_fail=msg)

    out = logs_dir / f"gate_daily_{now_ymd()}.json"
    payload: Dict[str, Any] = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "p0_daily_check": str(p0_path) if p0_path else None,
        # Backward-compatible top-level fields
        "gate0": {"status": _fmt(gate0), "msg": gate0.msg},
        "gate1": {"status": _fmt(gate1), "msg": gate1.msg},
        "gate2": {"status": _fmt(gate2), "msg": gate2.msg},
        "gate_macro": {"status": _fmt(gate_macro), "msg": gate_macro.msg},
        "note": note_msg,
        # NEW: structured engine action
        "engine_action": engine_action,
        # New structured fields
        "gates": {
            "gate0": {"status": _fmt(gate0), "msg": gate0.msg},
            "gate1": {"status": _fmt(gate1), "msg": gate1.msg},
            "gate2": {"status": _fmt(gate2), "msg": gate2.msg},
            "gate_macro": {"status": _fmt(gate_macro), "msg": gate_macro.msg},
        },
        "snapshot": {
            "risk_off": risk_off,
            "reasons": reasons,
            "flags": flags,
        },
    }

    # ASOF_B: gate.as_of_ymd := p0_daily_check.as_of_ymd (auto)
    try:
        import os, json
        _p = payload.get('p0_daily_check')
        if _p and os.path.exists(_p):
            _d = json.load(open(_p,'r',encoding='utf-8'))
            _a = _d.get('as_of_ymd')
            if _a is not None:
                payload['as_of_ymd'] = str(_a)
    except Exception:
        pass

    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[GATE] wrote: {out}")
    print(f"[GATE] gate0={_fmt(gate0)} ({gate0.msg})  gate1={_fmt(gate1)} ({gate1.msg})")
    print(f"[GATE] gate2={_fmt(gate2)} ({gate2.msg})")
    print(f"[GATE] gate_macro={_fmt(gate_macro)} ({gate_macro.msg})")
    if note_msg:
        print(f"[GATE] note: {note_msg}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


