from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
STAGED_JSON = LOG_DIR / "surge_ev_paper_probe_staged_latest.json"
SIM_JSON = LOG_DIR / "surge_ev_shadow_simulation_latest.json"
OUT_JSON = LOG_DIR / "surge_lob_quality_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "surge_lob_quality_diagnostic_latest.csv"


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except Exception:
        return default


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _depth_bucket(value: Any) -> str:
    depth = _to_float(value, -1.0)
    if depth < 0:
        return "UNKNOWN"
    if depth < 2:
        return "LT2"
    if depth < 3:
        return "LT3"
    return "GE3"


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    if not fields:
        fields = ["code"]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def main() -> int:
    now = datetime.now().isoformat(timespec="seconds")
    staged = _read_json(STAGED_JSON)
    sim = _read_json(SIM_JSON)
    rows: List[Dict[str, Any]] = []
    for row in staged.get("rows") or []:
        base_qty = int(_to_float(row.get("base_suggested_qty_before_lob_haircut"), 0.0))
        qty = int(_to_float(row.get("suggested_qty"), 0.0))
        rows.append({
            "code": row.get("code", ""),
            "rule": row.get("rule", ""),
            "lob_status": row.get("lob_status", ""),
            "ask_depth_levels": row.get("ask_depth_levels", ""),
            "ask_depth_bucket": _depth_bucket(row.get("ask_depth_levels")),
            "spread_bps": row.get("spread_bps", ""),
            "base_qty": base_qty,
            "suggested_qty": qty,
            "qty_delta": qty - base_qty,
            "haircut_factor": row.get("lob_quality_haircut_factor", ""),
            "haircut_reasons": row.get("lob_quality_haircut_reasons", ""),
            "paper_order_route": row.get("paper_order_route", False),
            "broker_order_route": row.get("broker_order_route", False),
            "dispatch_enabled": row.get("dispatch_enabled", False),
            "trading_allowed": row.get("trading_allowed", False),
        })
    depth_counts = Counter(_depth_bucket(row.get("ask_depth_levels")) for row in sim.get("rows") or [])
    staged_depth_counts = Counter(row.get("ask_depth_bucket") for row in rows)
    route_violations = [
        row for row in rows
        if str(row.get("paper_order_route")).lower() == "true"
        or str(row.get("broker_order_route")).lower() == "true"
        or str(row.get("dispatch_enabled")).lower() == "true"
        or str(row.get("trading_allowed")).lower() == "true"
    ]
    payload = {
        "ts": now,
        "status": "OK",
        "scope": "surge_lob_quality_diagnostic",
        "source_staged_json": str(STAGED_JSON),
        "source_simulation_json": str(SIM_JSON),
        "summary": {
            "simulation_rows": len(sim.get("rows") or []),
            "simulation_depth_counts": dict(depth_counts),
            "staged_rows": len(rows),
            "staged_depth_counts": dict(staged_depth_counts),
            "haircut_rows": sum(1 for row in rows if _to_float(row.get("haircut_factor"), 1.0) < 1.0),
            "zero_qty_rows": sum(1 for row in rows if int(_to_float(row.get("suggested_qty"), 0.0)) <= 0),
            "route_violations": len(route_violations),
        },
        "risk_contract": {
            "policy_change": False,
            "entry_approval_changed": False,
            "orders_exec_write": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", "staged_rows": len(rows), "route_violations": len(route_violations), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
