# gate_daily.py
# - Summarize daily gates based on latest p0_daily_check_*.json
# - Writes: 2_Logs/gate_daily_YYYYMMDD.json

from __future__ import annotations

import logging
import os
import json
import re
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
from utils.common import (
    is_daily_loss_reason,
    is_risk_off_hard_block_reason,
    latest_file,
    now_ymd,
    read_json,
)


@dataclass
class GateResult:
    ok: bool
    code: str
    msg: str

# _now_tag, read_json, latest_file -> utils.common?쇰줈 ?대룞??


_P0_DAILY_CHECK_REPORT_RE = re.compile(r"^p0_daily_check_\d{8}_\d{6}\.json$")


def _latest_p0_daily_check_report(logs_dir: Path) -> Optional[Path]:
    try:
        files = [
            p
            for p in logs_dir.glob("p0_daily_check_*.json")
            if _P0_DAILY_CHECK_REPORT_RE.match(p.name)
        ]
        files = sorted(files, key=lambda p: p.stat().st_mtime)
        return files[-1] if files else None
    except Exception:
        return None


def _gate(ok: bool, msg_ok: str, msg_fail: str) -> GateResult:
    return GateResult(ok=ok, code="PASS" if ok else "FAIL", msg=msg_ok if ok else msg_fail)


def _fmt(g: GateResult) -> str:
    return "PASS" if g.ok else "FAIL"


def _load_macro_gate_policy(root: Path) -> Dict[str, Any]:
    policy: Dict[str, Any] = {
        "mode": "SOFT",
        "hard_block_regimes": ["CRASH", "STAGFLATION"],
        "hard_block_when_risk_on_false": True,
    }
    cfg_path = Path(str(os.environ.get("PAPER_CONFIG_PATH", "")).strip() or (root / "paper" / "paper_engine_config.json"))
    if not cfg_path.exists():
        return policy
    try:
        obj = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return policy
    macro_pol = obj.get("macro_gate_policy") if isinstance(obj.get("macro_gate_policy"), dict) else {}
    if not isinstance(macro_pol, dict):
        return policy
    mode = str(macro_pol.get("mode") or policy["mode"]).strip().upper()
    regimes = macro_pol.get("hard_block_regimes")
    if not isinstance(regimes, list):
        regimes = policy["hard_block_regimes"]
    return {
        "mode": mode if mode in {"SOFT", "HARD"} else "SOFT",
        "hard_block_regimes": [str(x).upper().strip() for x in regimes if str(x).strip()],
        "hard_block_when_risk_on_false": bool(macro_pol.get("hard_block_when_risk_on_false", policy["hard_block_when_risk_on_false"])),
    }


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

    reason_list = [str(x) for x in (reasons or [])]

    # Prefer REDUCE for kill_switch when mode is missing/BLOCK for operation continuity.
    # Hard data-integrity reasons and daily-loss triggers still override to BLOCK below.
    if "kill_switch" in reason_list:
        modes["kill_switch"] = "BLOCK"

    if any(v == "BLOCK" for v in modes.values()):
        action = "BLOCK"
    else:
        action = "BLOCK"

    # HARD BLOCK: data integrity issues only.
    # DAILY_LOSS는 paper_engine의 adaptive/kill_switch mode(REDUCE/BLOCK)에 위임한다.
    hard_block = [r for r in reason_list if is_risk_off_hard_block_reason(r)]

    if hard_block:
        action = "BLOCK"
        for r in hard_block:
            modes[str(r)] = "BLOCK"

    return {"action": action, "modes": modes, "hard_block": hard_block}

def main() -> int:
    root = Path(__file__).resolve().parent
    logs_dir = root / "2_Logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    p0_path = _latest_p0_daily_check_report(logs_dir)
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
    macro_policy = _load_macro_gate_policy(root)
    macro_policy_mode = str(macro_policy.get("mode") or "SOFT").upper()
    hard_block_regimes = {str(x).upper() for x in list(macro_policy.get("hard_block_regimes") or [])}
    hard_block_when_risk_on_false = bool(macro_policy.get("hard_block_when_risk_on_false", True))

    # Macro gate is advisory (soft): never hard-fail trading by itself.
    # Hard blocking remains the responsibility of risk_off / kill_switch.
    macro_asof = str((macro or {}).get("as_of_ymd") or "")
    macro_is_today = (macro_asof == now_ymd())
    macro_risk_on = bool((macro or {}).get("risk_on", True))
    macro_regime = None
    if (not macro) or (not macro_is_today):
        gate_macro = _gate(ok=True, msg_ok="macro_missing_or_stale", msg_fail="macro_unused")
    else:
        macro_regime = str((macro or {}).get("regime") or "unknown").upper()
        macro_hard_block = (
            macro_policy_mode == "HARD"
            and (not macro_risk_on if hard_block_when_risk_on_false else True)
            and (macro_regime in hard_block_regimes if hard_block_regimes else False)
        )
        if macro_hard_block:
            msg = f"macro_risk_off_hard:{macro_regime}"
            gate_macro = _gate(ok=False, msg_ok=msg, msg_fail=msg)
        else:
            msg = "macro_risk_on" if macro_risk_on else f"macro_risk_off_soft:{macro_regime}"
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
        "macro": {
            "as_of_ymd": macro_asof or None,
            "is_today": bool(macro_is_today),
            "regime": macro_regime,
            "risk_on": bool(macro_risk_on) if (macro and macro_is_today) else None,
        },
        "macro_policy": {
            "mode": macro_policy_mode,
            "hard_block_regimes": sorted(list(hard_block_regimes)),
            "hard_block_when_risk_on_false": bool(hard_block_when_risk_on_false),
        },
        "regime": macro_regime,
    }

    # ASOF_B: gate.as_of_ymd := p0_daily_check.as_of_ymd (auto)
    try:
        import os
        import json
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

