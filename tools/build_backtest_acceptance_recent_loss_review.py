from __future__ import annotations

import csv
import datetime as dt
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
DIAG_JSON = LOG_DIR / "backtest_acceptance_candidate_screen_latest.json"
VALIDATION_JSON = LOG_DIR / "backtest_validation_latest.json"
CONFIG_JSON = ROOT / "paper" / "paper_engine_config.json"
TRADES_CALC_CSV = ROOT / "paper" / "trades_calc.csv"
OUT_JSON = LOG_DIR / "backtest_acceptance_recent_loss_review_latest.json"
OUT_CSV = LOG_DIR / "backtest_acceptance_recent_loss_review_latest.csv"


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "date",
        "validation_daily_return",
        "entry_trade_rows",
        "exit_trade_rows",
        "entry_net_sum",
        "exit_net_sum",
        "top_codes",
        "top_horizons",
        "top_entry_timings",
        "top_fallback_stages",
        "has_intraday_realtime",
        "has_split_entry",
        "has_sector_hrp_reduce",
        "has_sector_corr_reduce",
        "review_bucket",
    ]
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})
    tmp.replace(path)


def _float(value: Any, default: float = math.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _date8(value: Any) -> str:
    raw = str(value or "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _returns_by_date() -> Dict[str, float]:
    doc = _read_json(VALIDATION_JSON)
    artifacts = doc.get("artifacts") if isinstance(doc.get("artifacts"), dict) else {}
    series = artifacts.get("base_series") if isinstance(artifacts.get("base_series"), dict) else {}
    out: Dict[str, float] = {}
    for row in series.get("returns", []) if isinstance(series.get("returns"), list) else []:
        if not isinstance(row, dict):
            continue
        ymd = _date8(row.get("date") or row.get("timestamp"))
        ret = _float(row.get("return"))
        if ymd and math.isfinite(ret):
            out[ymd] = ret
    return out


def _recent_loss_dates(candidate_doc: Dict[str, Any], returns: Dict[str, float]) -> List[str]:
    for scenario in candidate_doc.get("scenarios_ranked", []):
        if isinstance(scenario, dict) and scenario.get("name") == "zero_recent_loss_days_last10":
            dates = [_date8(row.get("date")) for row in scenario.get("impacted_days", []) if isinstance(row, dict)]
            return [d for d in dates if d]
    ordered = sorted(returns.keys())
    return [d for d in ordered[-10:] if returns.get(d, 0.0) < 0.0]


def _note_token(note: str, key: str) -> str:
    match = re.search(rf"(?:^|[;| ]){re.escape(key)}=([^;| ]+)", note)
    return match.group(1) if match else ""


def _count(values: List[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for value in values:
        if not value:
            continue
        out[value] = out.get(value, 0) + 1
    return dict(sorted(out.items(), key=lambda kv: (-kv[1], kv[0])))


def _top_counts(counts: Dict[str, int], limit: int = 5) -> str:
    return ";".join(f"{k}:{v}" for k, v in list(counts.items())[:limit])


def _sum_net(rows: List[Dict[str, str]]) -> float:
    vals = [_float(row.get("net_ret")) for row in rows]
    vals = [v for v in vals if math.isfinite(v)]
    return float(sum(vals)) if vals else 0.0


def _bucket(row: Dict[str, Any]) -> str:
    if row["entry_trade_rows"] == 0 and row["exit_trade_rows"] == 0:
        return "NO_PAPER_TRADE_LINK_CURRENT_FILES"
    if row["has_intraday_realtime"] and row["has_split_entry"]:
        return "INTRADAY_SPLIT_ENTRY_LOSS_REVIEW"
    if row["has_intraday_realtime"]:
        return "INTRADAY_ENTRY_LOSS_REVIEW"
    return "LEGACY_OR_NON_INTRADAY_LOSS_REVIEW"


def build_review() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    candidate = _read_json(DIAG_JSON)
    returns = _returns_by_date()
    recent_dates = _recent_loss_dates(candidate, returns)
    trades = _read_csv(TRADES_CALC_CSV)
    config = _read_json(CONFIG_JSON) if CONFIG_JSON.exists() else {}

    rows: List[Dict[str, Any]] = []
    for ymd in recent_dates:
        entry_rows = [r for r in trades if _date8(r.get("entry_ts")) == ymd]
        exit_rows = [r for r in trades if _date8(r.get("exit_ts")) == ymd]
        linked = entry_rows + [r for r in exit_rows if r not in entry_rows]
        notes = [str(r.get("note", "")) for r in linked]
        codes = _count([str(r.get("code", "")).zfill(6) for r in linked])
        horizons = _count([_note_token(n, "horizon") for n in notes])
        timings = _count([_note_token(n, "entry_timing") for n in notes])
        stages = _count([_note_token(n, "fallback_stage") for n in notes])
        row = {
            "date": ymd,
            "validation_daily_return": returns.get(ymd),
            "entry_trade_rows": len(entry_rows),
            "exit_trade_rows": len(exit_rows),
            "entry_net_sum": _sum_net(entry_rows),
            "exit_net_sum": _sum_net(exit_rows),
            "top_codes": _top_counts(codes),
            "top_horizons": _top_counts(horizons),
            "top_entry_timings": _top_counts(timings),
            "top_fallback_stages": _top_counts(stages),
            "has_intraday_realtime": any("entry_timing=intraday_realtime" in n for n in notes),
            "has_split_entry": any("split_entry=" in n for n in notes),
            "has_sector_hrp_reduce": any("sector_hrp_reduce=1" in n for n in notes),
            "has_sector_corr_reduce": any("sector_corr_reduce=1" in n for n in notes),
        }
        row["review_bucket"] = _bucket(row)
        rows.append(row)

    bucket_counts = _count([str(r.get("review_bucket", "")) for r in rows])
    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "scope": "read_only_recent_loss_entry_quality_review",
        "source_candidate_screen": str(DIAG_JSON),
        "source_validation_json": str(VALIDATION_JSON),
        "source_trades_calc": str(TRADES_CALC_CSV),
        "policy_change": False,
        "entry_approval_changed": False,
        "trading_effect": False,
        "current_guard_state": {
            "hold_close_drop_guard": config.get("hold_close_drop_guard"),
            "entry_gap_down_stop_pct": config.get("entry_gap_down_stop_pct"),
            "entry_gap_risk_guard": config.get("entry_gap_risk_guard"),
            "intraday_residual_overnight_guard": config.get("intraday_residual_overnight_guard"),
            "entry_gap_up_reduce": config.get("entry_gap_up_reduce"),
        },
        "recent_loss_dates": recent_dates,
        "bucket_counts": bucket_counts,
        "rows": rows,
        "interpretation": {
            "primary_review_bucket": next(iter(bucket_counts.keys()), ""),
            "action_boundary": "diagnostic_only_no_gate_or_threshold_change",
            "next_evidence_need": "collect or verify whether recent intraday split-entry losses are prevented by existing active guards in subsequent operating samples",
        },
    }
    return payload, rows


def main() -> int:
    payload, rows = build_review()
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": payload["status"], "rows": len(rows), "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
