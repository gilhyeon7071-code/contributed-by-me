from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
# [2026-09-13] The 2026-08-07 package split stopped re-exporting these from
#   paper_engine/__init__; every reference below raised AttributeError at import
#   or first call. Import from the module that defines them instead.
from paper_engine.config import DEFAULT_CONFIG
from paper_engine.entry import _normal_intraday_realtime_block_reason

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
OUT_JSON = LOG_DIR / "normal_intraday_policy_validation_latest.json"

def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()

def _case(
    name: str,
    *,
    decision: str,
    realtime: bool = True,
    surge: bool = False,
    split2: bool = False,
    replay: bool = False,
    entry_gate_reason: str = "",
    position_size_multiplier: float = 0.0,
    p0_rolling_dd_abs: float | None = None,
    cfg_patch: Dict[str, Any] | None = None,
    expected_block: bool,
) -> Dict[str, Any]:
    

    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if isinstance(cfg_patch, dict):
        for key, value in cfg_patch.items():
            if isinstance(value, dict) and isinstance(cfg.get(key), dict):
                cfg[key].update(value)
            else:
                cfg[key] = value
    reason = _normal_intraday_realtime_block_reason(
        cfg=cfg,
        is_intraday_realtime_entry=realtime,
        is_surge_immediate=surge,
        is_split_2nd=split2,
        is_open_order_replay=replay,
        entry_gate_decision=decision,
        entry_gate_reason=entry_gate_reason,
        position_size_multiplier=position_size_multiplier,
        p0_rolling_dd_abs=p0_rolling_dd_abs,
        p0_rolling_dd_source="test",
    )
    blocked = bool(reason)
    return {
        "name": name,
        "decision": decision,
        "realtime": bool(realtime),
        "surge": bool(surge),
        "split2": bool(split2),
        "replay": bool(replay),
        "entry_gate_reason": entry_gate_reason,
        "position_size_multiplier": float(position_size_multiplier),
        "p0_rolling_dd_abs": p0_rolling_dd_abs,
        "expected_block": bool(expected_block),
        "actual_block": bool(blocked),
        "reason": reason,
        "status": "PASS" if blocked == bool(expected_block) else "FAIL",
    }

def run() -> Dict[str, Any]:
    cases: List[Dict[str, Any]] = [
        _case("normal_intraday_allow", decision="ALLOW", expected_block=False),
        _case("normal_intraday_caution", decision="CAUTION", expected_block=False),
        _case("normal_intraday_reduce", decision="REDUCE", expected_block=False),
        _case("normal_intraday_block", decision="BLOCK", expected_block=True),
        _case("next_open_reduce", decision="REDUCE", realtime=False, expected_block=False),
        _case("surge_intraday_reduce", decision="REDUCE", surge=True, expected_block=False),
        _case("split2_intraday_reduce", decision="REDUCE", split2=True, expected_block=False),
        _case("replay_intraday_reduce", decision="REDUCE", replay=True, expected_block=False),
        _case(
            "normal_intraday_validation_reduce_tightened",
            decision="REDUCE",
            entry_gate_reason="validation_reduce",
            position_size_multiplier=0.25,
            expected_block=False,
        ),
        _case(
            "normal_intraday_p0_rolling_dd_default_off",
            decision="ALLOW",
            p0_rolling_dd_abs=0.12,
            expected_block=False,
        ),
        _case(
            "normal_intraday_p0_rolling_dd_threshold_on",
            decision="ALLOW",
            p0_rolling_dd_abs=0.12,
            cfg_patch={
                "normal_intraday_realtime_policy": {
                    "block_when_p0_rolling_dd_ge_threshold": True,
                    "p0_rolling_dd_block_pct": 0.10,
                }
            },
            expected_block=True,
        ),
        _case(
            "surge_intraday_p0_rolling_dd_threshold_bypass",
            decision="ALLOW",
            surge=True,
            p0_rolling_dd_abs=0.12,
            cfg_patch={
                "normal_intraday_realtime_policy": {
                    "block_when_p0_rolling_dd_ge_threshold": True,
                    "p0_rolling_dd_block_pct": 0.10,
                }
            },
            expected_block=False,
        ),
    ]
    status = "PASS" if all(c["status"] == "PASS" for c in cases) else "FAIL"
    payload = {
        "generated_at": _now_ts(),
        "status": status,
        "cases": cases,
        "summary": {
            "total": len(cases),
            "pass": sum(1 for c in cases if c["status"] == "PASS"),
            "fail": sum(1 for c in cases if c["status"] != "PASS"),
        },
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload

def main() -> int:
    payload = run()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") == "PASS" else 1

if __name__ == "__main__":
    raise SystemExit(main())
