"""Market regime resolution helpers split from paper_engine.py.

These functions were extracted during the Phase 3c modularisation to isolate
the market-regime decision tree from the main engine body.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure utils import works regardless of cwd.
ROOT = Path(os.environ.get("STOC_BASE_DIR", r"E:\1_Data"))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.common import latest_file, now_ymd  # noqa: E402
from paper_engine.common import _to_float, _get_dict  # noqa: E402


def load_latest_gate_snapshot(
    log_dir: Path,
    max_age_days: int = 2,
    latest_p0_path: Optional[Path] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "path": None,
        "as_of_ymd": None,
        "regime": None,
        "risk_on": None,
        "gate_status": None,
        "age_days": None,
        "fresh": False,
        "p0_daily_check": None,
        "p0_aligned": None,
        "p0_alignment_reason": "not_evaluated",
    }
    try:
        p = latest_file(log_dir, "gate_daily_*.json")
        if not p:
            return out

        obj = json.loads(p.read_text(encoding="utf-8"))
        out["path"] = str(p)
        gate_p0_path = str(obj.get("p0_daily_check") or "").strip() or None
        out["p0_daily_check"] = gate_p0_path

        macro = obj.get("macro") if isinstance(obj.get("macro"), dict) else {}
        regime = str((macro.get("regime") or obj.get("regime") or "")).upper() or None
        risk_on = macro.get("risk_on") if ("risk_on" in macro) else None
        as_of_ymd = str((obj.get("as_of_ymd") or macro.get("as_of_ymd") or "")).strip() or None

        if regime is None:
            gm = None
            gates = obj.get("gates") if isinstance(obj.get("gates"), dict) else {}
            if isinstance(gates.get("gate_macro"), dict):
                gm = gates.get("gate_macro", {}).get("msg")
            if gm is None and isinstance(obj.get("gate_macro"), dict):
                gm = obj.get("gate_macro", {}).get("msg")
            gm = str(gm or "")
            if gm.startswith("macro_risk_off_soft:"):
                regime = gm.split(":", 1)[1].strip().upper() or None
                risk_on = False
            elif gm.startswith("macro_risk_off_hard:"):
                regime = gm.split(":", 1)[1].strip().upper() or None
                risk_on = False
            elif gm == "macro_risk_on":
                regime = "NORMAL"
                risk_on = True

        age_days = None
        if as_of_ymd and re.fullmatch(r"\d{8}", as_of_ymd):
            try:
                d0 = datetime.strptime(now_ymd(), "%Y%m%d")
                d1 = datetime.strptime(as_of_ymd, "%Y%m%d")
                age_days = float((d0 - d1).days)
            except Exception:
                age_days = None

        out["as_of_ymd"] = as_of_ymd
        out["regime"] = regime
        out["risk_on"] = risk_on
        gate_states: List[str] = []
        gates = obj.get("gates") if isinstance(obj.get("gates"), dict) else {}
        for key in ("gate0", "gate1", "gate2", "gate_macro"):
            node = obj.get(key) if isinstance(obj.get(key), dict) else gates.get(key)
            status = str((node or {}).get("status") or "").upper()
            if status:
                gate_states.append(status)
        if gate_states:
            out["gate_status"] = "PASS" if all(s == "PASS" for s in gate_states) else "BLOCK"
        out["age_days"] = age_days
        out["fresh"] = bool(age_days is not None and age_days <= float(max_age_days))
        if latest_p0_path:
            try:
                latest_p0_resolved = str(Path(latest_p0_path).resolve())
                gate_p0_resolved = str(Path(gate_p0_path).resolve()) if gate_p0_path else ""
                p0_aligned = bool(gate_p0_resolved and gate_p0_resolved == latest_p0_resolved)
            except Exception:
                latest_p0_resolved = str(latest_p0_path)
                gate_p0_resolved = str(gate_p0_path or "")
                p0_aligned = bool(gate_p0_resolved and gate_p0_resolved == latest_p0_resolved)
            out["p0_aligned"] = bool(p0_aligned)
            out["p0_alignment_reason"] = "aligned" if p0_aligned else "gate_daily_not_built_from_latest_p0"
            if not p0_aligned:
                out["gate_status"] = "BLOCK"
                out["fresh"] = False
        else:
            out["p0_alignment_reason"] = "latest_p0_path_missing"
        return out
    except Exception:
        return out


def resolve_bear_sizing_confirmation(log_dir: Path, market_regime: str) -> Dict[str, Any]:
    """Conservative multi-signal confirmation gate for BEAR-regime sizing overrides.

    A single BEAR regime label is not enough evidence to shrink live position
    sizing/exposure (regime detection itself has known blind spots). Require
    entry_gate=CAUTION and risk_orchestration scale<0.5 to agree with the
    BEAR label, read from the same p1_entry_gate_status snapshot, before the
    reduced BEAR capital_budget_policy is allowed to apply. If unconfirmed,
    callers should fall back to the NORMAL override instead of BEAR.
    """
    out: Dict[str, Any] = {
        "confirmed": False,
        "market_regime": str(market_regime or "").upper(),
        "entry_gate_decision": None,
        "risk_orch_scale": None,
        "reason": "not_evaluated",
        "path": None,
    }
    if str(market_regime or "").upper() != "BEAR":
        out["reason"] = "regime_not_bear"
        return out
    try:
        p = latest_file(log_dir, "p1_entry_gate_status_latest.json")
        if not p or not p.exists():
            out["reason"] = "p1_entry_gate_status_missing"
            return out
        obj = json.loads(p.read_text(encoding="utf-8"))
        out["path"] = str(p)
        entry_gate_decision = str(obj.get("entry_gate_decision_before_p1") or "").upper()
        out["entry_gate_decision"] = entry_gate_decision
        risk_orch_scale = None
        basis = obj.get("entry_risk_basis") if isinstance(obj.get("entry_risk_basis"), dict) else {}
        for comp in (basis.get("basis_components") or []):
            if isinstance(comp, dict) and str(comp.get("layer") or "").lower() == "risk_orchestration":
                risk_orch_scale = _to_float(comp.get("scale"), None)
                break
        out["risk_orch_scale"] = risk_orch_scale
        gate_ok = entry_gate_decision == "CAUTION"
        scale_ok = risk_orch_scale is not None and risk_orch_scale < 0.5
        out["confirmed"] = bool(gate_ok and scale_ok)
        if not gate_ok and not scale_ok:
            out["reason"] = "entry_gate_and_risk_scale_not_confirmed"
        elif not gate_ok:
            out["reason"] = "entry_gate_not_caution"
        elif not scale_ok:
            out["reason"] = "risk_orch_scale_not_below_0.5"
        else:
            out["reason"] = "confirmed"
        return out
    except Exception as exc:
        out["reason"] = f"error:{type(exc).__name__}"
        return out


def resolve_market_regime(
    cfg: Dict[str, Any],
    p0_snapshot: Dict[str, Any],
    macro_snapshot: Dict[str, Any],
    gate_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    pol = cfg.get("regime_entry_policy") if isinstance(cfg, dict) else {}
    if not isinstance(pol, dict):
        pol = {}

    enabled = bool(pol.get("enabled", False))
    out: Dict[str, Any] = {
        "enabled": enabled,
        "regime": "NORMAL",
        "day_ret": None,
        "p0_market_regime": str(p0_snapshot.get("market_regime") or "").upper(),
        "macro_regime": str(macro_snapshot.get("regime") or "").upper(),
        "macro_risk_on": macro_snapshot.get("risk_on"),
        "regime_source": "macro_snapshot",
        "reasons": [],
    }
    if not enabled:
        out["reasons"].append("policy_disabled")
        return out

    def _pct_to_decimal(v: Any, d: float) -> float:
        x = _to_float(v, d)
        if abs(x) > 1.0:
            x = x / 100.0
        return x

    def _resolve_rate_hike_fear_regime(
        macro_regime: str,
        daily_return: Any,
        gate_daily: str,
        risk_off: bool,
    ) -> str:
        if str(macro_regime or "").upper() != "RATE_HIKE_FEAR":
            return str(macro_regime or "").upper()

        cond = pol.get("rate_hike_fear_crash_conditions") if isinstance(pol.get("rate_hike_fear_crash_conditions"), dict) else {}
        daily_return_threshold = _pct_to_decimal((cond or {}).get("daily_return_threshold", -1.50), -1.50)
        require_gate_block = bool((cond or {}).get("require_gate_block", True))
        require_risk_off = bool((cond or {}).get("require_risk_off", True))

        market_falling = False
        try:
            market_falling = float(daily_return) < float(daily_return_threshold)
        except Exception:
            market_falling = False

        gate_blocked = str(gate_daily or "").upper() != "PASS"
        market_risk_off = bool(risk_off)

        if market_falling:
            return "CRASH"
        if require_gate_block and gate_blocked:
            return "CRASH"
        if require_risk_off and market_risk_off:
            return "CRASH"
        return "RATE_HIKE_FEAR"

    rally_day_ret_min = _to_float(pol.get("rally_day_ret_min", 0.025), 0.025)
    rally_day_ret_min_proxy = _to_float(pol.get("rally_day_ret_min_proxy", rally_day_ret_min), rally_day_ret_min)
    crash_day_ret_max = _to_float(pol.get("crash_day_ret_max", -0.025), -0.025)
    p0_bear_promote_enabled = bool(pol.get("p0_bear_promote_enabled", False))
    p0_bear_allowed_macro_regimes = {
        str(x or "").upper()
        for x in (pol.get("p0_bear_allowed_macro_regimes") or ["NORMAL", "RECOVERY", "SIDEWAYS", "VOLATILE"])
    }
    allow_rally_on_volatile = bool(pol.get("allow_rally_on_macro_volatile", True))
    allow_rally_when_macro_risk_off = bool(pol.get("allow_rally_when_macro_risk_off", True))
    gate_daily_prefer = bool(pol.get("gate_daily_prefer", True))

    if gate_daily_prefer and isinstance(gate_snapshot, dict):
        gate_regime = str(gate_snapshot.get("regime") or "").upper()
        gate_risk_on = gate_snapshot.get("risk_on")
        gate_fresh = bool(gate_snapshot.get("fresh", False))
        if gate_regime and gate_fresh:
            out["macro_regime"] = gate_regime
            out["macro_risk_on"] = gate_risk_on
            out["regime_source"] = "gate_daily"
            out["reasons"].append("regime_source=gate_daily")
        else:
            out["reasons"].append("regime_source=macro_snapshot_fallback")

    day_ret = None
    # Prefer macro ret1 (market-level) over p0 crash fallback proxy.
    try:
        mm = _get_dict(macro_snapshot, "market_metrics")
        if "ret1" in mm:
            day_ret = float(mm.get("ret1"))
    except Exception:
        day_ret = None
    if day_ret is None:
        try:
            cro = _get_dict(p0_snapshot, "crash_risk_off")
            metrics = _get_dict(cro, "metrics")
            if "day_ret" in metrics:
                day_ret = float(metrics.get("day_ret"))
        except Exception:
            day_ret = None
    out["day_ret"] = day_ret
    macro_source = str(macro_snapshot.get("source") or "").lower()
    if macro_source in {"krx_clean_proxy", "local_proxy_prices"}:
        rally_day_ret_threshold = rally_day_ret_min_proxy
        out["reasons"].append(f"rally_threshold_proxy={rally_day_ret_threshold:.4f}")
    else:
        rally_day_ret_threshold = rally_day_ret_min

    macro_regime = out["macro_regime"]
    macro_risk_on = out["macro_risk_on"]
    gate_daily_status = str((gate_snapshot or {}).get("gate_status") or "").upper() if isinstance(gate_snapshot, dict) else ""
    risk_off_enabled = bool(((p0_snapshot.get("risk_off") if isinstance(p0_snapshot, dict) else {}) or {}).get("enabled", False))
    p0_market_regime = str(
        p0_snapshot.get("market_regime")
        or ((p0_snapshot.get("meta") if isinstance(p0_snapshot.get("meta"), dict) else {}) or {}).get("market_regime")
        or ""
    ).upper()
    out["p0_market_regime"] = p0_market_regime

    resolved_macro_regime = _resolve_rate_hike_fear_regime(
        macro_regime=macro_regime,
        daily_return=day_ret,
        gate_daily=gate_daily_status,
        risk_off=risk_off_enabled,
    )
    if resolved_macro_regime == "CRASH" and macro_regime == "RATE_HIKE_FEAR":
        out["macro_regime"] = "CRASH"
        out["reasons"].append("rate_hike_fear_promoted_to_crash")
    elif resolved_macro_regime == "RATE_HIKE_FEAR":
        if day_ret is not None and day_ret >= rally_day_ret_threshold:
            if macro_risk_on is False and (not allow_rally_when_macro_risk_off):
                out["reasons"].append("macro_risk_off_blocks_rally")
            else:
                out["regime"] = "RALLY"
                out["reasons"].append("rate_hike_fear_soft_dayret_rally_override")
                return out
        out["regime"] = "RATE_HIKE_FEAR"
        out["reasons"].append("rate_hike_fear_soft")
        return out

    macro_regime = out["macro_regime"]

    # Treat RATE_HIKE_FEAR as crash only when explicitly opted in.
    rate_hike_fear_as_crash = bool(pol.get("rate_hike_fear_as_crash", False))
    is_macro_crash = (macro_regime == "CRASH") or (rate_hike_fear_as_crash and macro_regime == "RATE_HIKE_FEAR")
    is_macro_volatile = macro_regime == "VOLATILE"

    if is_macro_crash or (day_ret is not None and day_ret <= crash_day_ret_max):
        out["regime"] = "CRASH"
        out["reasons"].append("macro_or_dayret_crash")
        return out

    if day_ret is not None and day_ret >= rally_day_ret_threshold:
        if macro_risk_on is False and (not allow_rally_when_macro_risk_off):
            out["reasons"].append("macro_risk_off_blocks_rally")
        elif is_macro_volatile and not allow_rally_on_volatile:
            out["reasons"].append("macro_volatile_blocks_rally")
        else:
            out["regime"] = "RALLY"
            out["reasons"].append("dayret_rally")
            return out

    if p0_bear_promote_enabled and p0_market_regime == "BEAR":
        if (not p0_bear_allowed_macro_regimes) or (macro_regime in p0_bear_allowed_macro_regimes):
            out["regime"] = "BEAR"
            out["reasons"].append("p0_bear_promoted")
            return out
        out["reasons"].append(f"p0_bear_not_promoted:macro={macro_regime or 'UNKNOWN'}")

    out["reasons"].append("normal_by_default")
    return out
