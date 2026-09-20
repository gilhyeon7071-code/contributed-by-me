#!/usr/bin/env python
"""Refresh risk_orchestration dd_stop policy validation artifacts."""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path("E:/1_Data")
LOG_DIR = ROOT / "2_Logs"
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
TRADES_CALC_PATH = ROOT / "paper" / "trades_calc.csv"
RISK_ORCH_PATH = LOG_DIR / "risk_orchestration_latest.json"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if not math.isfinite(out):
            return default
        return out
    except Exception:
        return default


def _latest_kill_switch_curve() -> Path | None:
    paths = sorted(LOG_DIR.glob("kill_switch_dd_curve_*.csv"), key=lambda p: p.stat().st_mtime)
    return paths[-1] if paths else None


def _dd_curve_stats() -> dict[str, Any]:
    path = _latest_kill_switch_curve()
    if not path or not path.exists():
        return {"rows": 0, "stats": {}, "curve_path": None}
    df = pd.read_csv(path)
    if df.empty:
        return {"rows": int(len(df)), "stats": {}, "curve_path": None}
    dd_col = "dd_mean" if "dd_mean" in df.columns else "drawdown" if "drawdown" in df.columns else ""
    if not dd_col:
        return {"rows": int(len(df)), "stats": {}, "curve_path": str(path)}
    dd = pd.to_numeric(df[dd_col], errors="coerce").dropna()
    abs_dd = dd.abs()
    stats = {
        "rows": int(len(df)),
        "dd_column": dd_col,
        "max_abs": float(abs_dd.max()) if len(abs_dd) else 0.0,
        "latest_abs": float(abs_dd.iloc[-1]) if len(abs_dd) else 0.0,
        "p50_abs": float(abs_dd.quantile(0.50)) if len(abs_dd) else 0.0,
        "p75_abs": float(abs_dd.quantile(0.75)) if len(abs_dd) else 0.0,
        "p90_abs": float(abs_dd.quantile(0.90)) if len(abs_dd) else 0.0,
        "p95_abs": float(abs_dd.quantile(0.95)) if len(abs_dd) else 0.0,
        "days_ge_10": int((abs_dd >= 0.10).sum()),
        "days_ge_15": int((abs_dd >= 0.15).sum()),
        "days_ge_18": int((abs_dd >= 0.18).sum()),
        "days_ge_20": int((abs_dd >= 0.20).sum()),
    }
    return {"rows": int(len(df)), "stats": stats, "curve_path": str(path)}


def _scenario(base_scale: float, dd_current: float, es_triggered: bool, es_reduction: float, dd_cap: float, dd_stop: float) -> dict[str, Any]:
    scale = max(0.0, base_scale)
    reasons: list[str] = []
    if dd_stop > 0 and dd_current >= dd_stop:
        scale = 0.0
        reasons.append("dd_stop")
    elif dd_stop > dd_cap and dd_current >= dd_cap:
        taper = (dd_stop - dd_current) / max(dd_stop - dd_cap, 1e-9)
        scale *= max(0.0, min(1.0, taper))
        reasons.append("dd_taper")
    if es_triggered:
        scale *= max(0.0, min(1.0, es_reduction))
        reasons.append("es_reduction")
    decision = "BLOCK" if scale <= 0 else "REDUCE" if scale < base_scale else "ALLOW"
    return {
        "dd_cap": round(dd_cap, 6),
        "dd_stop": round(dd_stop, 6),
        "decision": decision,
        "scale_after_dd_es": round(scale, 6),
        "reasons": reasons,
    }


def main() -> int:
    cfg = _read_json(CONFIG_PATH)
    risk_doc = _read_json(RISK_ORCH_PATH)
    ro = risk_doc.get("risk_orchestration") if isinstance(risk_doc.get("risk_orchestration"), dict) else {}
    ro_cfg = cfg.get("risk_orchestration") if isinstance(cfg.get("risk_orchestration"), dict) else {}
    dcfg = ro_cfg.get("dd_taper") if isinstance(ro_cfg.get("dd_taper"), dict) else {}

    dd_cap = _to_float(ro.get("dd_cap", dcfg.get("dd_cap", 0.10)), 0.10)
    dd_stop = _to_float(ro.get("dd_stop", dcfg.get("dd_stop", 0.15)), 0.15)
    dd_current = _to_float(ro.get("dd_current"), 0.0)
    es_triggered = bool(ro.get("es_triggered", False))
    es_reduction = _to_float(ro.get("es_reduction_factor"), 1.0)
    base_scale = _to_float(ro.get("f_kelly"), 0.0)
    est_vol = _to_float(ro.get("est_vol"), 0.0)
    target_vol = _to_float(ro.get("target_vol"), 0.03)
    if est_vol > 0:
        base_scale = min(base_scale, target_vol / est_vol)
    base_scale = max(0.0, base_scale * _to_float(ro.get("c"), 0.0))

    curve = _dd_curve_stats()
    thresholds = [
        (0.067, 0.10),
        (0.10, 0.12),
        (0.10, 0.15),
        (0.10, 0.18),
        (0.10, 0.20),
        (0.10, 0.25),
        (0.10, 0.30),
        (0.10, 0.35),
    ]
    scenarios = [
        _scenario(base_scale, dd_current, es_triggered, es_reduction, cap, stop)
        for cap, stop in thresholds
    ]

    if dd_current >= dd_stop and _to_float(ro.get("es"), 0.0) <= -abs(_to_float(ro.get("es_limit"), 0.05)):
        recommendation = "KEEP_CURRENT_DD_STOP_0_15"
        rationale = ["current_dd_exceeds_stop", "tail_loss_es_exceeds_normal_limit"]
    elif dd_current >= dd_stop:
        recommendation = "KEEP_CURRENT_DD_STOP_0_15"
        rationale = ["current_dd_exceeds_stop"]
    else:
        recommendation = "NO_DD_STOP_RELAXATION_NEEDED"
        rationale = ["current_dd_below_stop"]

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "risk_orchestration.dd_stop policy validation only; no config change",
        "inputs": {
            "config": str(CONFIG_PATH),
            "trades_calc": str(TRADES_CALC_PATH),
            "risk_orchestration_latest": str(RISK_ORCH_PATH),
            "dd_curve": curve.get("curve_path"),
        },
        "current_policy": {
            "dd_cap": dd_cap,
            "dd_stop": dd_stop,
            "target_vol": _to_float(ro.get("target_vol"), 0.03),
            "lookback_trades": int(_to_float(ro.get("lookback_trades"), 0)),
        },
        "current_metrics": {
            "dd_current": dd_current,
            "edge": _to_float(ro.get("edge"), 0.0),
            "variance": _to_float(ro.get("variance"), 0.0),
            "est_vol": est_vol,
            "expected_shortfall": _to_float(ro.get("es"), 0.0),
            "f_kelly_raw": _to_float(ro.get("f_kelly_raw"), 0.0),
            "f_kelly": _to_float(ro.get("f_kelly"), 0.0),
            "base_scale_before_dd_es": base_scale,
            "es_limit": _to_float(ro.get("es_limit"), 0.05),
            "es_reduction_factor": es_reduction,
            "es_triggered": es_triggered,
        },
        "dd_curve_stats": curve.get("stats", {}),
        "threshold_scenarios": scenarios,
        "assessment": {
            "recommendation": recommendation,
            "rationale": rationale,
            "interpretation": (
                "dd_stop is a live risk-defense trigger. Raising it would be a policy relaxation, "
                "not a bug fix."
            ),
        },
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dated_path = LOG_DIR / f"risk_orch_dd_stop_validation_{stamp}.json"
    latest_path = LOG_DIR / "risk_orch_dd_stop_validation_latest.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    dated_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    json.loads(latest_path.read_text(encoding="utf-8"))
    print(f"[OK] wrote {dated_path}")
    print(f"[OK] wrote {latest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
