from __future__ import annotations

import csv
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
VALIDATION_JSON = LOG_DIR / "backtest_validation_latest.json"
FAILURE_JSON = LOG_DIR / "backtest_acceptance_failure_diagnostic_latest.json"
SENSITIVITY_JSON = LOG_DIR / "backtest_acceptance_sample_sensitivity_latest.json"
CANDIDATE_SCREEN_JSON = LOG_DIR / "backtest_acceptance_candidate_screen_latest.json"


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _finite_float(value: Any, default: float = math.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _returns_from_validation(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    artifacts = doc.get("artifacts") if isinstance(doc.get("artifacts"), dict) else {}
    base_series = artifacts.get("base_series") if isinstance(artifacts.get("base_series"), dict) else {}
    rows = base_series.get("returns") if isinstance(base_series.get("returns"), list) else []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ret = _finite_float(row.get("return"))
        if math.isnan(ret):
            continue
        out.append({"date": str(row.get("date") or row.get("timestamp") or ""), "return": ret})
    return out


def _acceptance_gate(doc: Dict[str, Any]) -> Dict[str, Any]:
    for gate in doc.get("gate_results", []):
        if isinstance(gate, dict) and gate.get("name") == "acceptance_pnl_turnover":
            return gate
    return {}


def _bootstrap_totals(returns: Iterable[float], *, n_boot: int = 1200, block_size: int = 21) -> np.ndarray:
    rr = np.asarray([float(v) for v in returns if math.isfinite(float(v))], dtype=float)
    n = len(rr)
    if n == 0:
        return np.asarray([], dtype=float)
    if n == 1:
        return np.asarray([float((1.0 + rr[0]) - 1.0)], dtype=float)
    bs = max(2, min(int(block_size), n))
    rng = np.random.default_rng(42)
    totals: List[float] = []
    for _ in range(max(100, int(n_boot))):
        chunks: List[np.ndarray] = []
        while sum(len(c) for c in chunks) < n:
            start = int(rng.integers(0, n))
            end = start + bs
            if end <= n:
                block = rr[start:end]
            else:
                block = np.concatenate([rr[start:n], rr[0 : end - n]])
            chunks.append(block)
        sample = np.concatenate(chunks)[:n]
        totals.append(float(np.prod(1.0 + sample) - 1.0))
    return np.asarray(totals, dtype=float)


def _ci95(returns: Iterable[float]) -> Dict[str, float]:
    totals = _bootstrap_totals(returns)
    if len(totals) == 0:
        return {"low": math.nan, "med": math.nan, "high": math.nan, "negative_bootstrap_rate": math.nan}
    return {
        "low": float(np.percentile(totals, 2.5)),
        "med": float(np.percentile(totals, 50.0)),
        "high": float(np.percentile(totals, 97.5)),
        "negative_bootstrap_rate": float(np.mean(totals <= 0.0)),
    }


def _compound(returns: Iterable[float]) -> float:
    total = 1.0
    for ret in returns:
        total *= 1.0 + float(ret)
    return float(total - 1.0)


def _window_blocks(rows: List[Dict[str, Any]], window: int = 21) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if len(rows) < window:
        return out
    for idx in range(0, len(rows) - window + 1):
        part = rows[idx : idx + window]
        vals = [float(r["return"]) for r in part]
        out.append(
            {
                "start": part[0]["date"],
                "end": part[-1]["date"],
                "total_return": _compound(vals),
                "mean_return": float(np.mean(vals)),
                "loss_days": int(sum(1 for v in vals if v < 0)),
            }
        )
    return out


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except Exception:
        return []


def _row_date(row: Dict[str, str]) -> str:
    for key in ("date", "ymd", "entry_ts", "exit_ts", "timestamp", "datetime"):
        raw = str(row.get(key, "") or "")
        digits = "".join(ch for ch in raw if ch.isdigit())
        if len(digits) >= 8:
            return digits[:8]
    return ""


def _trade_return(row: Dict[str, str]) -> float:
    for key in ("net_ret", "return", "ret", "gross_ret"):
        val = _finite_float(row.get(key))
        if not math.isnan(val):
            return val
    return math.nan


def _worst_day_links(worst_days: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    bt_rows = _read_csv_rows(ROOT / "12_Risk_Controlled" / "report_backtest_trades_v41_1.csv")
    paper_rows = _read_csv_rows(ROOT / "paper" / "trades_calc.csv")
    out: List[Dict[str, Any]] = []
    for day in worst_days:
        ymd = str(day.get("date", ""))
        bt_matches = [r for r in bt_rows if _row_date(r) == ymd]
        paper_matches = [r for r in paper_rows if _row_date(r) == ymd]
        paper_rets = [_trade_return(r) for r in paper_matches]
        paper_rets = [v for v in paper_rets if math.isfinite(v)]
        out.append(
            {
                "date": ymd,
                "validation_daily_return": day.get("return"),
                "backtest_trade_rows": len(bt_matches),
                "paper_trades_calc_rows": len(paper_matches),
                "paper_trades_calc_net_sum": float(sum(paper_rets)) if paper_rets else 0.0,
                "paper_trades_calc_worst_rows": sorted(
                    paper_matches,
                    key=lambda r: _trade_return(r) if math.isfinite(_trade_return(r)) else 999.0,
                )[:5],
            }
        )
    return out


def _scenario(name: str, rows: List[Dict[str, Any]], replacements: Dict[str, float]) -> Dict[str, Any]:
    vals = [float(replacements.get(str(row["date"]), row["return"])) for row in rows]
    ci = _ci95(vals)
    impacted = [
        {"date": row["date"], "old_return": row["return"], "new_return": replacements[row["date"]]}
        for row in rows
        if row["date"] in replacements
    ]
    return {
        "name": name,
        "type": "hypothetical_read_only",
        "impacted_days": impacted,
        "ci95_low": ci["low"],
        "ci95_med": ci["med"],
        "ci95_high": ci["high"],
        "negative_bootstrap_rate": ci["negative_bootstrap_rate"],
        "would_pass": bool(ci["low"] > 0.0),
    }


def build_payloads() -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    doc = _read_json(VALIDATION_JSON)
    gate = _acceptance_gate(doc)
    details = gate.get("details") if isinstance(gate.get("details"), dict) else {}
    rows = _returns_from_validation(doc)
    values = [float(row["return"]) for row in rows]
    ci = _ci95(values)
    worst = sorted(rows, key=lambda r: r["return"])[:10]
    best = sorted(rows, key=lambda r: r["return"], reverse=True)[:10]
    worst_blocks = sorted(_window_blocks(rows), key=lambda r: r["total_return"])[:5]
    best_blocks = sorted(_window_blocks(rows), key=lambda r: r["total_return"], reverse=True)[:5]
    last10 = rows[-10:]

    leave_one = []
    for row in worst[:5]:
        vals = [float(r["return"]) for r in rows if r is not row]
        row_ci = _ci95(vals)
        leave_one.append(
            {
                "removed_date": row["date"],
                "removed_return": row["return"],
                "ci95_low": row_ci["low"],
                "ci95_med": row_ci["med"],
                "ci95_high": row_ci["high"],
                "would_pass_low_gt_0": bool(row_ci["low"] > 0.0),
            }
        )

    pnl_low = _finite_float(details.get("pnl_ci95_low"))
    turnover = _finite_float(details.get("turnover_monthly"))
    turnover_limit = _finite_float(details.get("turnover_limit_monthly"))
    failure = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "source_json": str(VALIDATION_JSON),
        "status": "FAIL_CONFIRMED" if not bool(gate.get("passed", False)) else "PASS",
        "failed_gate": "acceptance_pnl_turnover",
        "gate_details": details,
        "recomputed_ci95": ci,
        "sample": {
            "n_days": len(values),
            "compound_total_return": _compound(values),
            "mean_daily": float(np.mean(values)) if values else math.nan,
            "median_daily": float(np.median(values)) if values else math.nan,
            "loss_days": int(sum(1 for v in values if v < 0)),
            "zero_days": int(sum(1 for v in values if v == 0)),
            "positive_days": int(sum(1 for v in values if v > 0)),
            "negative_bootstrap_rate": ci["negative_bootstrap_rate"],
        },
        "turnover_pass": bool(math.isfinite(turnover) and math.isfinite(turnover_limit) and turnover <= turnover_limit),
        "pnl_low_gap_to_pass": float(max(0.0, -pnl_low)) if math.isfinite(pnl_low) else math.nan,
        "worst_days": worst,
        "best_days": best,
        "worst_21d_blocks": worst_blocks,
        "best_21d_blocks": best_blocks,
        "last10_days": last10,
        "leave_one_worst_day_sensitivity": leave_one,
        "interpretation": {
            "direct_cause": "pnl_ci95_low <= 0" if not gate.get("passed", False) and pnl_low <= 0 else "not_failed_or_unknown",
            "not_data_integrity_failure": True,
            "not_turnover_failure": bool(math.isfinite(turnover) and math.isfinite(turnover_limit) and turnover <= turnover_limit),
            "likely_resolution": "more operating samples or demonstrable entry-quality improvement; no threshold relaxation is applied here",
        },
    }

    scenarios = []
    for add_days in (1, 2, 3, 5, 10, 21):
        for add_ret in (0.0, 0.0001, 0.0002, 0.0005, 0.001, 0.002):
            vals = values + [add_ret] * add_days
            add_ci = _ci95(vals)
            scenarios.append(
                {
                    "added_days": add_days,
                    "added_daily_return": add_ret,
                    "ci95_low": add_ci["low"],
                    "ci95_med": add_ci["med"],
                    "ci95_high": add_ci["high"],
                    "negative_bootstrap_rate": add_ci["negative_bootstrap_rate"],
                    "would_pass": bool(add_ci["low"] > 0.0),
                }
            )
    required: Dict[str, Any] = {"return": None, "ci95_low": None, "would_pass": False}
    lo, hi = 0.0, 0.05
    for _ in range(24):
        mid = (lo + hi) / 2.0
        mid_ci = _ci95(values + [mid])
        if mid_ci["low"] > 0.0:
            hi = mid
            required = {"return": mid, **mid_ci, "would_pass": True}
        else:
            lo = mid
    sensitivity = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "source": "backtest_validation_latest.json base_series.returns",
        "status": "OK",
        "baseline_ci95": ci,
        "hypothetical_added_sample_scenarios": scenarios,
        "one_day_required_return_to_pass_ci95_low_gt_0": required,
        "note": "Hypothetical sensitivity only; no operational data, policy, threshold, score, gate, order, fill, ledger, or stats artifact was changed.",
    }

    recent_losses = {row["date"]: 0.0 for row in last10 if row["return"] < 0.0}
    worst_one = {worst[0]["date"]: 0.0} if worst else {}
    worst_three = {row["date"]: 0.0 for row in worst[:3]}
    candidate = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_current_backtest_acceptance_candidate_screen",
        "source_validation_json": str(VALIDATION_JSON),
        "baseline": {
            "ci95_low": ci["low"],
            "ci95_med": ci["med"],
            "ci95_high": ci["high"],
            "negative_bootstrap_rate": ci["negative_bootstrap_rate"],
            "n_days": len(values),
        },
        "implemented_guards": {
            "policy_change_applied": False,
            "threshold_relaxation_applied": False,
            "entry_approval_changed": False,
        },
        "worst_day_links": _worst_day_links(worst),
        "scenarios_ranked": sorted(
            [
                _scenario("zero_recent_loss_days_last10", rows, recent_losses),
                _scenario("zero_worst_day", rows, worst_one),
                _scenario("zero_worst_3_days", rows, worst_three),
                _scenario("add_5_flat_days", rows + [{"date": f"ADDED_FLAT_{i}", "return": 0.0} for i in range(5)], {}),
                _scenario("add_5_small_positive_days", rows + [{"date": f"ADDED_POS_{i}", "return": 0.0005} for i in range(5)], {}),
            ],
            key=lambda r: (not r.get("would_pass", False), -float(r.get("ci95_low") or -999)),
        ),
        "interpretation": {
            "direct_failure": "pnl_ci95_low <= 0",
            "current_margin_to_pass": float(max(0.0, -pnl_low)) if math.isfinite(pnl_low) else math.nan,
            "policy_change_applied": False,
            "trading_effect": False,
            "note": "Read-only screen. Use as evidence for what to observe next, not as approval to loosen FAIL-CLOSED gates.",
        },
    }
    return failure, sensitivity, candidate


def main() -> int:
    failure, sensitivity, candidate = build_payloads()
    _write_json(FAILURE_JSON, failure)
    _write_json(SENSITIVITY_JSON, sensitivity)
    _write_json(CANDIDATE_SCREEN_JSON, candidate)
    status = "PASS" if failure.get("status") in {"FAIL_CONFIRMED", "PASS"} and sensitivity.get("status") == "OK" and candidate.get("status") == "OK" else "FAIL"
    print(json.dumps({"status": status, "failure": str(FAILURE_JSON), "sensitivity": str(SENSITIVITY_JSON), "candidate_screen": str(CANDIDATE_SCREEN_JSON)}, ensure_ascii=False))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
