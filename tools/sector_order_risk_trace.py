from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"
ENGINE = ROOT / "paper_engine.py"
SECTOR_CORR = LOG_DIR / "sector_correlation_latest.json"
OUT_JSON = LOG_DIR / "sector_order_risk_trace_latest.json"
OUT_CSV = LOG_DIR / "sector_order_risk_trace_latest.csv"


def _now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _date8() -> str:
    return datetime.now().strftime("%Y%m%d")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    return obj if isinstance(obj, dict) else {}


def _latest_order_file() -> Path | None:
    files = sorted(PAPER_DIR.glob("orders_*_broker_submit_mock.csv"), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def _read_orders(path: Path | None) -> List[Dict[str, str]]:
    if path is None or not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _source_checks() -> Dict[str, Any]:
    text = _read_text(ENGINE)
    surge_budget_pos = text.find("qty = int(qty_from_budget)")
    note_pos = text.find("note_parts.extend(sector_risk_note_parts)")
    return {
        "paper_engine_exists": ENGINE.exists(),
        "has_sector_risk_note_parts": "sector_risk_note_parts" in text,
        "has_note_trace_fields": all(
            token in text
            for token in [
                "sector_hrp_reduce=1",
                "sector_hrp_mult=",
                "sector_hrp_qty=",
                "sector_corr_reduce=1",
            ]
        ),
        "surge_budget_before_note_trace": surge_budget_pos >= 0 and note_pos > surge_budget_pos,
    }


def _artifact_checks() -> Dict[str, Any]:
    obj = _read_json(SECTOR_CORR)
    mults = obj.get("sector_hrp_qty_multiplier") if isinstance(obj.get("sector_hrp_qty_multiplier"), dict) else {}
    reduced = {
        str(k): float(v)
        for k, v in mults.items()
        if isinstance(v, (int, float)) and 0.0 < float(v) < 1.0
    }
    corr_scores = obj.get("sector_corr_score") if isinstance(obj.get("sector_corr_score"), dict) else {}
    thresholds = obj.get("thresholds") if isinstance(obj.get("thresholds"), dict) else {}
    reduce_thr = float(thresholds.get("reduce_threshold_abs_corr", 0.85) or 0.85)
    corr_reduced = {
        str(k): float(v)
        for k, v in corr_scores.items()
        if isinstance(v, (int, float)) and float(v) >= reduce_thr
    }
    return {
        "path": str(SECTOR_CORR),
        "exists": SECTOR_CORR.exists(),
        "enabled": bool(obj.get("enabled", False)),
        "risk_budget_engine": str(obj.get("risk_budget_engine", "")),
        "hrp_reduced_sector_count": len(reduced),
        "corr_reduced_sector_count": len(corr_reduced),
        "hrp_reduced_sectors": reduced,
        "corr_reduced_sectors": corr_reduced,
    }


def _order_checks(order_file: Path | None, orders: List[Dict[str, str]]) -> Dict[str, Any]:
    buys = [r for r in orders if str(r.get("side", "")).upper() == "BUY"]
    accepted = [r for r in buys if str(r.get("dispatch_status", "")).upper() == "ACCEPTED"]
    traced = []
    for row in buys:
        note = str(row.get("note", "") or "")
        if "sector_hrp_reduce=1" in note or "sector_corr_reduce=1" in note:
            traced.append(row)
    return {
        "path": str(order_file) if order_file else None,
        "exists": bool(order_file and order_file.exists()),
        "buy_rows": len(buys),
        "accepted_buy_rows": len(accepted),
        "sector_trace_rows": len(traced),
        "historical_trace_available": len(traced) > 0,
    }


def _flatten(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for section in ["source_code", "artifact", "orders"]:
        data = result.get(section, {})
        if not isinstance(data, dict):
            continue
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False, sort_keys=True)
            rows.append(
                {
                    "generated_at": result.get("generated_at", ""),
                    "status": result.get("status", ""),
                    "section": section,
                    "metric": key,
                    "value": value,
                }
            )
    return rows


def main() -> int:
    source = _source_checks()
    artifact = _artifact_checks()
    order_file = _latest_order_file()
    orders = _order_checks(order_file, _read_orders(order_file))

    issues = []
    if not all(source.values()):
        issues.append("source_code_trace_path_incomplete")
    if not artifact.get("enabled"):
        issues.append("sector_correlation_artifact_disabled_or_missing")
    if int(artifact.get("hrp_reduced_sector_count", 0) or 0) <= 0 and int(artifact.get("corr_reduced_sector_count", 0) or 0) <= 0:
        issues.append("no_sector_reduction_candidate_in_artifact")
    if orders.get("buy_rows", 0) > 0 and not orders.get("historical_trace_available"):
        issues.append("historical_order_note_has_no_sector_trace")

    status = "PASS"
    if any(x in issues for x in ["source_code_trace_path_incomplete", "sector_correlation_artifact_disabled_or_missing"]):
        status = "FAIL"
    elif issues:
        status = "WARN"

    result = {
        "generated_at": _now_ts(),
        "date8": _date8(),
        "status": status,
        "issues": issues,
        "source_code": source,
        "artifact": artifact,
        "orders": orders,
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    dated_json = LOG_DIR / f"sector_order_risk_trace_{_date8()}.json"
    dated_csv = LOG_DIR / f"sector_order_risk_trace_{_date8()}.csv"
    text = json.dumps(result, ensure_ascii=True, indent=2)
    OUT_JSON.write_text(text + "\n", encoding="utf-8")
    dated_json.write_text(text + "\n", encoding="utf-8")

    rows = _flatten(result)
    for path in [OUT_CSV, dated_csv]:
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["generated_at", "status", "section", "metric", "value"])
            writer.writeheader()
            writer.writerows(rows)

    print(f"WROTE {OUT_JSON}")
    print(f"WROTE {dated_json}")
    print(f"WROTE {OUT_CSV} rows={len(rows)}")
    print(f"status={status}")
    return 0 if status in {"PASS", "WARN"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
