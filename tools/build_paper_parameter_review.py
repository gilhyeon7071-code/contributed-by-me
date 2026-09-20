from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
RISK_DIR = ROOT / "12_Risk_Controlled"
PAPER_DIR = ROOT / "paper"

PAPER_PNL_PATH = LOG_DIR / "paper_pnl_summary_last.json"
LVB_PATH = LOG_DIR / "live_vs_bt_feedback_latest.json"
TRVAL_PATH = LOG_DIR / "trading_stage_validation_latest.json"
BACKTEST_FINAL_PATH = LOG_DIR / "backtest_final_output_latest.json"
STABLE_PATH = RISK_DIR / "stable_params_v41_1.json"
PAPER_TRADES_PATH = PAPER_DIR / "trades.csv"

DEFAULT_WARMUP_TRADES = 50




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-", "None"):
            return None
        return float(v)
    except Exception:
        return None


def _load_trade_exit_mix(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"rows": 0, "counts": {}, "ratios": {}}
    counts: Dict[str, int] = {}
    rows = 0
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows += 1
                reason = str((row or {}).get("exit_reason") or "-").strip() or "-"
                counts[reason] = counts.get(reason, 0) + 1
    except Exception:
        return {"rows": 0, "counts": {}, "ratios": {}}
    ratios = {k: (v / rows if rows > 0 else 0.0) for k, v in counts.items()}
    return {"rows": rows, "counts": counts, "ratios": ratios}


def _paper_item(stage: Dict[str, Any], name: str) -> Dict[str, Any]:
    for item in stage.get("items", []) or []:
        if isinstance(item, dict) and str(item.get("name")) == name:
            return item
    return {}


def _paper_review_status(enough_sample: bool, value: Any = None) -> str:
    if not enough_sample:
        return "blocked"
    if value is None:
        return "monitor"
    return "monitor"


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _round_param(name: str, value: float) -> float | int:
    if name in {"hold", "max_pos"}:
        return int(round(value))
    if name == "value_min":
        return float(round(value, -8))
    return round(value, 4)


def _proposed_zone(current: Any, proposed: Any) -> Optional[Dict[str, Any]]:
    current_f = _to_float(current)
    proposed_f = _to_float(proposed)
    if current_f is None or proposed_f is None:
        return None
    return {
        "lower": min(current_f, proposed_f),
        "upper": max(current_f, proposed_f),
    }


def _candidate_payload(
    name: str,
    current_value: Any,
    enough_sample: bool,
    evidence: Dict[str, Any],
) -> Dict[str, Any]:
    if not enough_sample:
        return {
            "review_status": "blocked",
            "reason": "insufficient_sample",
            "proposed_value": None,
            "proposed_zone": None,
            "confidence": "low",
            "requires_retest": True,
        }

    current_f = _to_float(current_value)
    if current_f is None:
        return {
            "review_status": "monitor",
            "reason": "missing_current_value",
            "proposed_value": None,
            "proposed_zone": None,
            "confidence": "low",
            "requires_retest": True,
        }

    paper_mdd = _to_float(evidence.get("paper_mdd"))
    last_day_ret = _to_float(evidence.get("last_day_ret"))
    gross_pf = _to_float(evidence.get("gross_pf"))
    abs_diff = _to_float(evidence.get("return_divergence"))
    stop_gap_ratio = _to_float(evidence.get("stop_gap_ratio"))
    time_exit_ratio = _to_float(evidence.get("time_exit_ratio"))
    alignment_ready = bool(evidence.get("alignment_ready"))
    quality_gate_ok = bool(evidence.get("quality_gate_ok"))

    risk_pressure = (paper_mdd is not None and paper_mdd <= -0.12) or (last_day_ret is not None and last_day_ret <= -0.06)
    stop_gap_pressure = stop_gap_ratio is not None and stop_gap_ratio >= 0.25
    time_exit_pressure = time_exit_ratio is not None and time_exit_ratio >= 0.40
    divergence_pressure = abs_diff is not None and abs_diff >= 0.15
    quality_pressure = not quality_gate_ok
    alignment_pressure = not alignment_ready
    profitability_soft = gross_pf is not None and gross_pf < 1.2

    proposed_value: Any = None
    proposed_zone: Optional[Dict[str, Any]] = None
    review_status = "monitor"
    reason = "monitor_only"
    confidence = "low"

    if name == "stop_loss" and (risk_pressure or stop_gap_pressure):
        proposed_value = _round_param(name, -_clamp(abs(current_f) * 0.9, 0.02, 0.12))
        review_status = "candidate"
        reason = "risk_pressure"
        confidence = "medium" if risk_pressure and stop_gap_pressure else "low"
    elif name == "hold" and time_exit_pressure:
        step = 2 if time_exit_ratio is not None and time_exit_ratio >= 0.50 else 1
        proposed_value = _round_param(name, _clamp(current_f - step, 3, 20))
        review_status = "candidate"
        reason = "time_exit_pressure"
        confidence = "medium"
    elif name == "gap_up_max_pct" and (divergence_pressure or quality_pressure or alignment_pressure):
        proposed_value = _round_param(name, _clamp(current_f * 0.9, 0.01, 0.08))
        review_status = "candidate"
        reason = "entry_divergence_pressure"
        confidence = "low"
    elif name == "entry_gap_down_stop_pct" and (stop_gap_pressure or risk_pressure):
        proposed_value = _round_param(name, _clamp(current_f * 0.9, 0.01, 0.08))
        review_status = "candidate"
        reason = "gap_stop_pressure"
        confidence = "medium" if stop_gap_pressure else "low"
    elif name == "value_min" and (divergence_pressure or profitability_soft):
        multiplier = 1.2 if divergence_pressure else 1.1
        proposed_value = _round_param(name, _clamp(current_f * multiplier, 1_000_000_000.0, 1_000_000_000_000.0))
        review_status = "candidate"
        reason = "liquidity_filter_tighten"
        confidence = "low"
    elif name == "atr_max" and (divergence_pressure or risk_pressure):
        proposed_value = _round_param(name, _clamp(current_f * 0.9, 0.5, 10.0))
        review_status = "candidate"
        reason = "volatility_cap_tighten"
        confidence = "low"
    elif name == "max_pos" and risk_pressure:
        proposed_value = _round_param(name, _clamp(current_f - 1, 1, 20))
        review_status = "candidate"
        reason = "position_risk_tighten"
        confidence = "medium"

    if proposed_value is not None and _to_float(proposed_value) == current_f:
        proposed_value = None
        review_status = "monitor"
        reason = "no_adjustable_headroom"
        confidence = "low"

    if proposed_value is not None:
        proposed_zone = _proposed_zone(current_f, proposed_value)

    return {
        "review_status": review_status,
        "reason": reason,
        "proposed_value": proposed_value,
        "proposed_zone": proposed_zone,
        "confidence": confidence,
        "requires_retest": True,
    }


def build_review(warmup_trades: int = DEFAULT_WARMUP_TRADES) -> Dict[str, Any]:
    paper_pnl = _read_json(PAPER_PNL_PATH)
    lvb = _read_json(LVB_PATH)
    trval = _read_json(TRVAL_PATH)
    backtest_final = _read_json(BACKTEST_FINAL_PATH)
    stable = _read_json(STABLE_PATH)
    exit_mix = _load_trade_exit_mix(PAPER_TRADES_PATH)

    paper = trval.get("paper", {}) if isinstance(trval.get("paper"), dict) else {}
    trades_used = int(paper_pnl.get("trades_used", 0) or 0)
    enough_sample = trades_used >= int(warmup_trades)

    paper_mdd = _to_float(((paper_pnl.get("equity") or {}).get("max_drawdown_pct")))
    last_day_ret = _to_float(((paper_pnl.get("equity") or {}).get("last_day_ret")))
    avg_ret = _to_float(paper_pnl.get("avg_ret"))
    gross_pf = _to_float(paper_pnl.get("gross_pf"))
    abs_diff = _to_float(((lvb.get("divergence") or {}).get("abs_diff")))
    alignment_ready = bool(((lvb.get("comparison") or {}).get("alignment_ready")))
    quality_gate_ok = bool(((lvb.get("quality_gate") or {}).get("ok")))

    stop_gap_ratio = _to_float((exit_mix.get("ratios") or {}).get("STOP_GAP"))
    time_ratio = _to_float((exit_mix.get("ratios") or {}).get("TIME"))

    operating = ((backtest_final.get("operating_parameters") or {}).get("params") or {})
    parameter_names = [
        "stop_loss",
        "hold",
        "gap_up_max_pct",
        "entry_gap_down_stop_pct",
        "value_min",
        "atr_max",
        "max_pos",
    ]

    parameters: List[Dict[str, Any]] = []
    for name in parameter_names:
        current_value = stable.get(name, operating.get(name))
        evidence = {
            "paper_mdd": paper_mdd,
            "last_day_ret": last_day_ret,
            "avg_ret": avg_ret,
            "gross_pf": gross_pf,
            "return_divergence": abs_diff,
            "alignment_ready": alignment_ready,
            "quality_gate_ok": quality_gate_ok,
            "stop_gap_ratio": stop_gap_ratio,
            "time_exit_ratio": time_ratio,
        }
        candidate = _candidate_payload(name, current_value, enough_sample, evidence)
        parameters.append(
            {
                "name": name,
                "current_value": current_value,
                "source_priority": [
                    "stable_params_v41_1.json",
                    "backtest_final_output_latest.json.operating_parameters.params",
                ],
                "paper_review": {
                    "status": candidate["review_status"],
                    "reason": candidate["reason"],
                    "evidence": evidence,
                },
                "candidate_update": {
                    "proposed_value": candidate["proposed_value"],
                    "proposed_zone": candidate["proposed_zone"],
                    "confidence": candidate["confidence"],
                    "requires_retest": bool(candidate["requires_retest"]),
                },
            }
        )

    return {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": {
            "paper_pnl_summary": str(PAPER_PNL_PATH),
            "live_vs_bt_feedback": str(LVB_PATH),
            "trading_stage_validation": str(TRVAL_PATH),
            "backtest_final_output": str(BACKTEST_FINAL_PATH),
            "stable_params": str(STABLE_PATH),
            "paper_trades": str(PAPER_TRADES_PATH),
        },
        "sample_gate": {
            "paper_trades_used": trades_used,
            "warmup_trades": int(warmup_trades),
            "enough_for_feedback": enough_sample,
        },
        "paper_status": {
            "judgment": paper.get("judgment"),
            "paper_mdd": _paper_item(paper, "paper_mdd"),
            "paper_bt_alignment": _paper_item(paper, "paper_bt_alignment"),
            "paper_return_divergence": _paper_item(paper, "paper_return_divergence"),
            "paper_quality_gate": _paper_item(paper, "paper_quality_gate"),
        },
        "paper_observations": {
            "risk": {
                "paper_mdd": paper_mdd,
                "last_day_ret": last_day_ret,
                "avg_ret": avg_ret,
                "gross_pf": gross_pf,
            },
            "comparison": {
                "alignment_ready": alignment_ready,
                "return_divergence": abs_diff,
                "quality_gate_ok": quality_gate_ok,
            },
            "exit_mix": exit_mix,
        },
        "parameters": parameters,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build paper parameter review snapshot from latest paper/backtest artifacts")
    ap.add_argument("--warmup-trades", type=int, default=DEFAULT_WARMUP_TRADES)
    args = ap.parse_args()

    review = build_review(warmup_trades=int(args.warmup_trades))

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / f"paper_parameter_review_{stamp}.json"
    latest_json = LOG_DIR / "paper_parameter_review_latest.json"

    payload = json.dumps(review, ensure_ascii=False, indent=2)
    out_json.write_text(payload, encoding="utf-8-sig")
    latest_json.write_text(payload, encoding="utf-8-sig")

    _log_print(f"[PAPER_PARAM_REVIEW] sample={review['sample_gate']['paper_trades_used']}/{review['sample_gate']['warmup_trades']}")
    _log_print(f"[PAPER_PARAM_REVIEW] latest={latest_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
