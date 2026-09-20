from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"
CONFIG_PATH = PAPER_DIR / "paper_engine_config.json"


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == "":
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _norm_ymd(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())[:8]


def _generated_today(value: Any) -> bool:
    return _norm_ymd(value) == datetime.now().strftime("%Y%m%d")


def _latest(pattern: str) -> Path | None:
    files = sorted(LOG_DIR.glob(pattern), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    return files[0] if files else None


def _sample_ok(nfills: int, ess: float, min_fills: int, min_ess: float) -> bool:
    return int(nfills) >= int(min_fills) or float(ess) >= float(min_ess)


def _tier(value: float, watch: float, soft: float, hard: float) -> str:
    if value >= hard:
        return "HARD"
    if value >= soft:
        return "SOFT"
    if value >= watch:
        return "WATCH"
    return "NORMAL"


def _max_stage(stages: List[str]) -> str:
    order = {"NORMAL": 0, "WATCH": 1, "SOFT": 2, "HARD": 3}
    return max(stages or ["NORMAL"], key=lambda x: order.get(str(x), 0))


def _dd_signal(cfg: Dict[str, Any]) -> Dict[str, Any]:
    p0 = _read_json(LOG_DIR / "p0_daily_check_latest.json")
    if not p0:
        p0_path = _latest("p0_daily_check_*.json")
        p0 = _read_json(p0_path) if p0_path else {}
    ro = _read_json(LOG_DIR / "risk_orchestration_latest.json")
    ro_is_current = _generated_today(ro.get("generated_at"))
    ro_doc = ro.get("risk_orchestration") if ro_is_current and isinstance(ro.get("risk_orchestration"), dict) else {}
    ks = p0.get("kill_switch") if isinstance(p0.get("kill_switch"), dict) else {}
    kmet = ks.get("metrics") if isinstance(ks.get("metrics"), dict) else {}
    dd_pct = abs(_to_float(ro_doc.get("dd_current", kmet.get("max_drawdown_pct", 0.0)), 0.0)) * 100.0
    th = cfg.get("dd_pct", {}) if isinstance(cfg.get("dd_pct"), dict) else {}
    stage = _tier(dd_pct, _to_float(th.get("watch"), 0.25), _to_float(th.get("soft"), 0.5), _to_float(th.get("hard"), 1.0))
    role = str(cfg.get("drawdown_role") or "blocking").strip().lower()
    advisory = role in {"advisory", "confirm_only", "ddm_owned", "ddm_owner"}
    return {
        "name": "drawdown",
        "stage": stage,
        "value": dd_pct,
        "unit": "pct",
        "role": role,
        "advisory": advisory,
        "sample_ok": True,
        "reason": f"dd_pct={dd_pct:.4f}",
        "source": "risk_orchestration_latest.json" if ro_doc else "p0_daily_check",
        "risk_orchestration_current": bool(ro_doc),
    }


def _slippage_signal(cfg: Dict[str, Any]) -> Dict[str, Any]:
    min_fills = _to_int(cfg.get("min_fills"), 50)
    min_ess = _to_float(cfg.get("min_ess"), 150.0)
    path = PAPER_DIR / "fills_ledger.csv"
    if not path.exists():
        path = PAPER_DIR / "fills.csv"
    if not path.exists():
        return {"name": "slippage", "stage": "NORMAL", "sample_ok": False, "reason": "fills_missing"}
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception as exc:
        return {"name": "slippage", "stage": "NORMAL", "sample_ok": False, "reason": f"read_fail:{type(exc).__name__}"}
    col = "slippage_bps_model" if "slippage_bps_model" in df.columns else ""
    if not col and {"slippage", "price"}.issubset(set(df.columns)):
        col = "slippage"
    if not col:
        return {"name": "slippage", "stage": "NORMAL", "sample_ok": False, "reason": "slippage_column_missing"}
    vals = pd.to_numeric(df[col], errors="coerce").dropna().abs()
    nfills = int(len(vals))
    ess = float(nfills)
    if not _sample_ok(nfills, ess, min_fills, min_ess):
        return {"name": "slippage", "stage": "NORMAL", "sample_ok": False, "nfills": nfills, "ess": ess, "reason": "sample_below_floor"}
    recent_n = max(min_fills, min(200, nfills))
    baseline_n = min(max(30 * min_fills, recent_n), nfills)
    recent = vals.tail(recent_n)
    baseline = vals.tail(baseline_n).head(max(0, baseline_n - recent_n))
    baseline_med = float(baseline.median()) if not baseline.empty else float(vals.median())
    recent_med = float(recent.median()) if not recent.empty else 0.0
    delta = recent_med - baseline_med
    th = cfg.get("slip_bps_delta", {}) if isinstance(cfg.get("slip_bps_delta"), dict) else {}
    stage = _tier(delta, _to_float(th.get("watch"), 5.0), _to_float(th.get("soft"), 15.0), _to_float(th.get("hard"), 40.0))
    return {
        "name": "slippage",
        "stage": stage,
        "value": delta,
        "unit": "bps_delta",
        "recent_median_bps": recent_med,
        "baseline_median_bps": baseline_med,
        "nfills": nfills,
        "ess": ess,
        "sample_ok": True,
        "reason": f"slip_delta_bps={delta:.4f}",
    }


def _reject_signal(cfg: Dict[str, Any]) -> Dict[str, Any]:
    path = _latest("kis_order_dispatch_*.json")
    if not path:
        return {"name": "reject", "stage": "NORMAL", "sample_ok": False, "reason": "dispatch_log_missing"}
    obj = _read_json(path)
    records = obj.get("records") if isinstance(obj.get("records"), list) else []
    submitted = len(records)
    rejected = 0
    has_5xx = False
    harmless = {"SKIP_ALREADY_DISPATCHED", "PRECHECK_DUPLICATE_IN_BATCH"}
    for rec in records:
        if not isinstance(rec, dict):
            continue
        status = str(rec.get("dispatch_status") or "").upper()
        code = str(rec.get("error_status_code") or "")
        if code.startswith("5"):
            has_5xx = True
        if status in harmless:
            continue
        if status.startswith("PRECHECK_REJECT") or status.startswith("PRECHECK_ERROR") or str(rec.get("error") or ""):
            rejected += 1
    rate = (rejected / submitted) if submitted > 0 else 0.0
    th = cfg.get("reject_rate", {}) if isinstance(cfg.get("reject_rate"), dict) else {}
    stage = "HARD" if has_5xx else _tier(rate, _to_float(th.get("watch"), 0.01), _to_float(th.get("soft"), 0.03), _to_float(th.get("hard"), 0.07))
    return {
        "name": "reject",
        "stage": stage,
        "value": rate,
        "unit": "ratio",
        "submitted": submitted,
        "rejected": rejected,
        "has_exchange_5xx": has_5xx,
        "sample_ok": submitted > 0,
        "reason": f"reject_rate={rate:.4f}" + (";exchange_5xx" if has_5xx else ""),
        "source": str(path),
    }


def _liquidity_signal(cfg: Dict[str, Any]) -> Dict[str, Any]:
    path = LOG_DIR / "surge_lob_latest.csv"
    if not path.exists():
        return {"name": "liquidity", "stage": "NORMAL", "sample_ok": False, "reason": "lob_missing"}
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception as exc:
        return {"name": "liquidity", "stage": "NORMAL", "sample_ok": False, "reason": f"read_fail:{type(exc).__name__}"}
    if df.empty:
        return {"name": "liquidity", "stage": "NORMAL", "sample_ok": False, "reason": "lob_empty"}
    spread = pd.to_numeric(df.get("spread_bps", pd.Series(dtype=float)), errors="coerce").dropna()
    spread_now = float(spread.median()) if not spread.empty else 0.0
    depth_cols = [c for c in ["askq1", "askq2", "askq3", "askq4", "askq5", "bidq1", "bidq2", "bidq3", "bidq4", "bidq5"] if c in df.columns]
    depth_now = 0.0
    depth_drop = 0.0
    if depth_cols:
        depth = df[depth_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).sum(axis=1)
        depth_now = float(depth.tail(max(1, min(5, len(depth)))).median())
        baseline = float(depth.median()) if len(depth) > 0 else 0.0
        if baseline > 0:
            depth_drop = max(0.0, 1.0 - (depth_now / baseline))
    th_depth = cfg.get("depth_drop", {}) if isinstance(cfg.get("depth_drop"), dict) else {}
    th_spread = cfg.get("spread_bps_delta", {}) if isinstance(cfg.get("spread_bps_delta"), dict) else {}
    depth_stage = _tier(depth_drop, _to_float(th_depth.get("watch"), 0.30), _to_float(th_depth.get("soft"), 0.60), _to_float(th_depth.get("hard"), 0.80))
    spread_stage = _tier(spread_now, _to_float(th_spread.get("watch"), 5.0), _to_float(th_spread.get("soft"), 15.0), _to_float(th_spread.get("hard"), 50.0))
    stage = _max_stage([depth_stage, spread_stage])
    return {
        "name": "liquidity",
        "stage": stage,
        "depth_drop": depth_drop,
        "spread_bps_delta": spread_now,
        "depth_now_top5": depth_now,
        "sample_ok": True,
        "reason": f"depth_drop={depth_drop:.4f};spread_bps={spread_now:.4f}",
        "source": str(path),
    }


def build_payload(cfg: Dict[str, Any]) -> Dict[str, Any]:
    signals = [_dd_signal(cfg), _liquidity_signal(cfg), _slippage_signal(cfg), _reject_signal(cfg)]
    action_signals = [s for s in signals if not bool(s.get("advisory", False))]
    advisory_signals = [s for s in signals if bool(s.get("advisory", False))]
    hard_count = sum(1 for s in action_signals if s.get("stage") == "HARD")
    soft_count = sum(1 for s in action_signals if s.get("stage") == "SOFT")
    watch_count = sum(1 for s in action_signals if s.get("stage") == "WATCH")
    concurrent_hard = hard_count >= 2
    any_hard = hard_count >= 1
    if concurrent_hard or any_hard:
        action = "HARD"
    elif soft_count > 0:
        action = "SOFT"
    elif watch_count > 0:
        action = "WATCH"
    else:
        action = "NORMAL"
    reasons = [f"{s.get('name')}:{s.get('stage')}:{s.get('reason')}" for s in action_signals if s.get("stage") != "NORMAL"]
    advisory_reasons = [f"{s.get('name')}:{s.get('stage')}:{s.get('reason')}" for s in advisory_signals if s.get("stage") != "NORMAL"]
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "as_of_ymd": datetime.now().strftime("%Y%m%d"),
        "schema_version": "production_risk_playbook_v1",
        "source_fresh": True,
        "action": action,
        "blocked": action == "HARD",
        "soft_pause": action == "SOFT",
        "watch": action == "WATCH",
        "reason": ";".join(reasons) if reasons else "ok",
        "advisory_reason": ";".join(advisory_reasons),
        "signals": signals,
        "advisory_signals": advisory_signals,
        "rules": {
            "hard_count": hard_count,
            "soft_count": soft_count,
            "watch_count": watch_count,
            "concurrent_hard": concurrent_hard,
            "advisory_count": len(advisory_signals),
        },
        "mitigations": {
            "new_orders_allowed": action != "HARD",
            "size_multiplier": 0.0 if action == "HARD" else 0.5 if action == "SOFT" else 1.0,
            "evidence_bundle_required": action in {"WATCH", "SOFT", "HARD"},
        },
        "evidence_paths": {
            "risk_orchestration": str(LOG_DIR / "risk_orchestration_latest.json"),
            "p1_entry_gate": str(LOG_DIR / "p1_entry_gate_status_latest.json"),
            "market_ops_alert": str(LOG_DIR / "market_ops_alert_latest.json"),
            "lob_snapshot": str(LOG_DIR / "surge_lob_latest.csv"),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build production Watch/Soft/Hard risk playbook artifact.")
    ap.add_argument("--out", default=str(LOG_DIR / "production_risk_playbook_latest.json"))
    args = ap.parse_args()
    engine_cfg = _read_json(CONFIG_PATH)
    prod_cfg = engine_cfg.get("production_risk_playbook") if isinstance(engine_cfg.get("production_risk_playbook"), dict) else {}
    cfg = {
        "min_fills": 50,
        "min_ess": 150,
        "dd_pct": {"watch": 0.25, "soft": 0.5, "hard": 1.0},
        "slip_bps_delta": {"watch": 5.0, "soft": 15.0, "hard": 40.0},
        "reject_rate": {"watch": 0.01, "soft": 0.03, "hard": 0.07},
        "depth_drop": {"watch": 0.30, "soft": 0.60, "hard": 0.80},
        "spread_bps_delta": {"watch": 5.0, "soft": 15.0, "hard": 50.0},
    }
    if isinstance(prod_cfg, dict):
        cfg.update({k: v for k, v in prod_cfg.items() if k not in {"enabled", "artifact_path"}})
    payload = build_payload(cfg)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] production_risk_playbook={out} action={payload['action']} reason={payload['reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
