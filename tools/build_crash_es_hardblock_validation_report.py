from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CFG_PATH = ROOT / "paper" / "paper_engine_config.json"
TRADES_CALC = ROOT / "paper" / "trades_calc.csv"


def _read_json(path: Path) -> Dict[str, Any]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    return {}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).replace(",", "").strip()
        if not text:
            return default
        out = float(text)
        if out != out:
            return default
        return out
    except Exception:
        return default


def _pct01(value: Any, default: float) -> float:
    out = _to_float(value, default)
    if abs(out) > 1.0:
        out /= 100.0
    return out


def _latest_gate_daily() -> Path | None:
    files = sorted(LOG_DIR.glob("gate_daily_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _recompute_recent_es(lookback: int) -> Dict[str, Any]:
    df = pd.read_csv(TRADES_CALC, dtype=str, encoding="utf-8-sig").fillna("")
    if "net_ret" not in df.columns:
        return {"status": "FAIL", "reason": "net_ret_missing"}
    ret = pd.to_numeric(df["net_ret"], errors="coerce").dropna()
    sample = ret.tail(max(10, int(lookback or 60)))
    if len(sample) <= 1:
        return {"status": "NOT_EVALUABLE", "reason": "insufficient_recent_returns", "ret_n": int(len(sample))}
    q10 = float(sample.quantile(0.10))
    tail = sample[sample <= q10]
    return {
        "status": "PASS",
        "source": str(TRADES_CALC),
        "rows_total": int(len(df)),
        "ret_n_total": int(ret.shape[0]),
        "lookback": int(len(sample)),
        "edge": float(sample.mean()),
        "variance": float(sample.var(ddof=0)),
        "est_vol": float(sample.std(ddof=0)),
        "q10": q10,
        "tail_n": int(len(tail)),
        "es": float(tail.mean()) if len(tail) else 0.0,
        "tail_values": [float(x) for x in tail.tolist()],
    }


def build_report() -> Dict[str, Any]:
    cfg = _read_json(CFG_PATH)
    macro = _read_json(LOG_DIR / "macro_signal_latest.json")
    p0 = _read_json(LOG_DIR / "p0_daily_check_20260528_135743.json")
    gate_path = _latest_gate_daily()
    gate = _read_json(gate_path) if gate_path else {}
    risk = _read_json(LOG_DIR / "risk_orchestration_latest.json")
    p1 = _read_json(LOG_DIR / "p1_entry_gate_status_latest.json")

    ro_cfg = cfg.get("risk_orchestration") if isinstance(cfg.get("risk_orchestration"), dict) else {}
    regime_policy = cfg.get("regime_entry_policy") if isinstance(cfg.get("regime_entry_policy"), dict) else {}
    ro_detail = risk.get("risk_orchestration") if isinstance(risk.get("risk_orchestration"), dict) else {}
    es_cfg = ro_cfg.get("es_gate") if isinstance(ro_cfg.get("es_gate"), dict) else {}
    by_regime = es_cfg.get("by_regime") if isinstance(es_cfg.get("by_regime"), dict) else {}
    crash_es_cfg = by_regime.get("CRASH") if isinstance(by_regime.get("CRASH"), dict) else {}

    macro_metrics = macro.get("market_metrics") if isinstance(macro.get("market_metrics"), dict) else {}
    macro_ret1 = _to_float(macro_metrics.get("ret1"), 0.0)
    crash_day_ret_max = _pct01(regime_policy.get("crash_day_ret_max"), -0.025)
    macro_regime = str(macro.get("regime") or "").upper()
    gate_regime = str(gate.get("regime") or ((gate.get("macro") or {}).get("regime") if isinstance(gate.get("macro"), dict) else "") or "").upper()
    gate_statuses: List[str] = []
    gates = gate.get("gates") if isinstance(gate.get("gates"), dict) else {}
    for key in ("gate0", "gate1", "gate2", "gate_macro"):
        node = gates.get(key) if isinstance(gates.get(key), dict) else gate.get(key)
        if isinstance(node, dict) and str(node.get("status") or "").strip():
            gate_statuses.append(str(node.get("status")).upper())
    gate_daily_status = "PASS" if gate_statuses and all(x == "PASS" for x in gate_statuses) else ("BLOCK" if gate_statuses else "UNKNOWN")

    expected_regime = "CRASH" if (macro_regime == "CRASH" or macro_ret1 <= crash_day_ret_max) else macro_regime or "NORMAL"
    runtime_regime = str(risk.get("market_regime") or p1.get("market_regime") or "").upper()
    crash_verdict = "PASS" if runtime_regime == expected_regime == "CRASH" else "ISSUE_FOUND"
    crash_reason = (
        f"macro_ret1={macro_ret1:.6f} <= crash_day_ret_max={crash_day_ret_max:.6f}"
        if expected_regime == "CRASH"
        else "macro_ret1_above_crash_threshold"
    )

    lookback = int(_to_float(ro_cfg.get("lookback_trades"), 60))
    es_recalc = _recompute_recent_es(lookback)
    runtime_es = _to_float(ro_detail.get("es"), 0.0)
    es_diff = abs(_to_float(es_recalc.get("es"), 0.0) - runtime_es)
    es_calc_verdict = "PASS" if es_recalc.get("status") == "PASS" and es_diff <= 1e-12 else "ISSUE_FOUND"

    runtime_es_limit = abs(_to_float(ro_detail.get("es_limit"), 0.0))
    cfg_es_limit = abs(_to_float(crash_es_cfg.get("limit"), _to_float(es_cfg.get("limit"), 0.05)))
    cfg_hard_block = bool(crash_es_cfg.get("hard_block", es_cfg.get("hard_block", False)))
    cfg_reduction_factor = _to_float(crash_es_cfg.get("reduction_factor", es_cfg.get("reduction_factor", 0.5)))
    es_trigger_expected = bool(abs(runtime_es) >= cfg_es_limit > 0)
    es_policy_verdict = (
        "PASS"
        if runtime_regime == "CRASH"
        and abs(runtime_es_limit - cfg_es_limit) <= 1e-12
        and bool(ro_detail.get("es_hard_block")) == cfg_hard_block
        and bool(ro_detail.get("es_triggered")) == es_trigger_expected
        and ("es_hard_block" in list(ro_detail.get("scale_zero_causes") or []))
        else "ISSUE_FOUND"
    )

    rows = [
        {"check": "crash_regime", "status": crash_verdict, "value": runtime_regime, "expected": expected_regime, "note": crash_reason},
        {"check": "gate_daily", "status": "INFO", "value": gate_daily_status, "expected": "PASS", "note": f"gate_regime={gate_regime}"},
        {"check": "macro_regime", "status": "INFO", "value": macro_regime, "expected": "NORMAL_OR_CRASH", "note": f"macro_source={macro.get('source') or (macro.get('sources') or {}).get('macro')}"},
        {"check": "macro_ret1", "status": "PASS" if macro_ret1 <= crash_day_ret_max else "INFO", "value": macro_ret1, "expected": f"<= {crash_day_ret_max}", "note": "daily market return drives crash override"},
        {"check": "es_recompute", "status": es_calc_verdict, "value": _to_float(es_recalc.get("es"), 0.0), "expected": runtime_es, "note": f"lookback={lookback}; tail_n={es_recalc.get('tail_n')}"},
        {"check": "es_limit", "status": "PASS" if abs(runtime_es_limit - cfg_es_limit) <= 1e-12 else "ISSUE_FOUND", "value": runtime_es_limit, "expected": cfg_es_limit, "note": "CRASH regime override"},
        {"check": "es_trigger", "status": es_policy_verdict, "value": bool(ro_detail.get("es_triggered")), "expected": es_trigger_expected, "note": f"abs_es={abs(runtime_es):.6f}, limit={cfg_es_limit:.6f}"},
        {"check": "hard_block", "status": es_policy_verdict, "value": bool(ro_detail.get("es_hard_block")), "expected": cfg_hard_block, "note": f"reduction_factor={cfg_reduction_factor}"},
        {"check": "position_size_multiplier", "status": "PASS" if _to_float(risk.get("position_size_multiplier"), 1.0) == 0.0 else "ISSUE_FOUND", "value": _to_float(risk.get("position_size_multiplier"), 1.0), "expected": 0.0, "note": ",".join(list(ro_detail.get("scale_zero_causes") or []))},
    ]

    final_status = "PASS" if crash_verdict == "PASS" and es_calc_verdict == "PASS" and es_policy_verdict == "PASS" else "ISSUE_FOUND"
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
        "schema_version": "crash_es_hardblock_validation_v1",
        "scope": "read_only_crash_regime_and_es_hardblock_validation",
        "status": final_status,
        "policy_change": False,
        "trading_effect": False,
        "sources": {
            "config": str(CFG_PATH),
            "macro_signal": str(LOG_DIR / "macro_signal_latest.json"),
            "gate_daily": str(gate_path) if gate_path else None,
            "p0_daily_check": str(LOG_DIR / "p0_daily_check_20260528_135743.json"),
            "risk_orchestration": str(LOG_DIR / "risk_orchestration_latest.json"),
            "p1_entry_gate": str(LOG_DIR / "p1_entry_gate_status_latest.json"),
            "trades_calc": str(TRADES_CALC),
        },
        "source_generated_at": {
            "macro_signal": macro.get("generated_at") or macro.get("as_of_ymd"),
            "gate_daily": gate.get("generated_at"),
            "p0_daily_check": p0.get("generated_at"),
            "risk_orchestration": risk.get("generated_at"),
        },
        "crash_validation": {
            "status": crash_verdict,
            "runtime_regime": runtime_regime,
            "expected_regime": expected_regime,
            "macro_regime": macro_regime,
            "gate_daily_status": gate_daily_status,
            "gate_regime": gate_regime,
            "macro_ret1": macro_ret1,
            "crash_day_ret_max": crash_day_ret_max,
            "reason": crash_reason,
        },
        "es_validation": {
            "calculation_status": es_calc_verdict,
            "policy_status": es_policy_verdict,
            "runtime_es": runtime_es,
            "recomputed_es": _to_float(es_recalc.get("es"), 0.0),
            "es_diff": es_diff,
            "es_alpha": _to_float(ro_detail.get("es_alpha"), 0.10),
            "lookback_trades": lookback,
            "tail_n": es_recalc.get("tail_n"),
            "q10": es_recalc.get("q10"),
            "edge": es_recalc.get("edge"),
            "variance": es_recalc.get("variance"),
            "est_vol": es_recalc.get("est_vol"),
            "tail_values": es_recalc.get("tail_values"),
        },
        "hardblock_validation": {
            "status": es_policy_verdict,
            "runtime_es_limit": runtime_es_limit,
            "configured_crash_es_limit": cfg_es_limit,
            "runtime_es_triggered": bool(ro_detail.get("es_triggered")),
            "expected_es_triggered": es_trigger_expected,
            "runtime_es_hard_block": bool(ro_detail.get("es_hard_block")),
            "configured_crash_hard_block": cfg_hard_block,
            "configured_crash_reduction_factor": cfg_reduction_factor,
            "position_size_multiplier": _to_float(risk.get("position_size_multiplier"), 1.0),
            "scale_zero_causes": list(ro_detail.get("scale_zero_causes") or []),
        },
        "rows": rows,
    }


def main() -> int:
    payload = build_report()
    out_json = LOG_DIR / "crash_es_hardblock_validation_latest.json"
    out_csv = LOG_DIR / "crash_es_hardblock_validation_latest.csv"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "status", "value", "expected", "note"])
        writer.writeheader()
        writer.writerows(payload["rows"])
    print(
        json.dumps(
            {
                "status": payload["status"],
                "crash_status": payload["crash_validation"]["status"],
                "es_calc_status": payload["es_validation"]["calculation_status"],
                "hardblock_status": payload["hardblock_validation"]["status"],
                "out_json": str(out_json),
                "out_csv": str(out_csv),
            },
            ensure_ascii=False,
        )
    )
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
