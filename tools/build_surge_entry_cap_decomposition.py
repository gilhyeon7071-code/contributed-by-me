from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SURGE_CSV = LOG_DIR / "surge_realtime_latest.csv"
SURGE_JSON = LOG_DIR / "surge_realtime_latest.json"
OUT_JSON = LOG_DIR / "surge_entry_cap_decomposition_latest.json"
OUT_CSV = LOG_DIR / "surge_entry_cap_decomposition_latest.csv"


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


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _reason_keys(row: Dict[str, Any]) -> Set[str]:
    keys: Set[str] = set()
    for field in ("exclude_reasons", "entry_reason", "paper_probe_block_reasons"):
        for part in str(row.get(field) or "").split("|"):
            token = part.strip().split(":", 1)[0].strip()
            if token:
                keys.add(token)
    return keys


def _cap_class(row: Dict[str, Any], keys: Set[str]) -> str:
    entry = "ENTRY_CHANGE_BLOCK" in keys
    atr = "ENTRY_ATR_CAP" in keys
    limit_near = str(row.get("cond_limit_near") or "").lower() == "true"
    if limit_near and not entry and not atr:
        return "LIMIT_NEAR_CAP_BYPASS_OR_NO_CAP"
    if entry and atr:
        return "ENTRY_CHANGE_AND_ATR_CAP"
    if entry:
        return "ENTRY_CHANGE_ONLY"
    if atr:
        return "ATR_CAP_ONLY"
    return "NO_ENTRY_CAP_BLOCK"


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
    surge = _read_json(SURGE_JSON)
    rows: List[Dict[str, Any]] = []
    for row in _read_csv(SURGE_CSV):
        keys = _reason_keys(row)
        cap_class = _cap_class(row, keys)
        if cap_class == "NO_ENTRY_CAP_BLOCK":
            continue
        rows.append({
            "code": str(row.get("code") or "").zfill(6),
            "name": row.get("name", ""),
            "surge_score_final": row.get("surge_score_final", ""),
            "change_pct": row.get("change_pct", ""),
            "max_entry_change_pct": row.get("max_entry_change_pct", ""),
            "atr_entry_cap_pct": row.get("atr_entry_cap_pct", ""),
            "cond_limit_near": row.get("cond_limit_near", ""),
            "rvol20": row.get("rvol20", ""),
            "spread_bps": row.get("spread_bps", ""),
            "intraday_high_drawdown_pct": row.get("intraday_high_drawdown_pct", ""),
            "cap_class": cap_class,
            "entry_change_block": "ENTRY_CHANGE_BLOCK" in keys,
            "atr_cap_block": "ENTRY_ATR_CAP" in keys,
            "reason_keys": "|".join(sorted(keys)),
            "policy_change": False,
            "entry_approval_changed": False,
            "research_only": True,
        })
    counts = Counter(row["cap_class"] for row in rows)
    payload = {
        "ts": now,
        "status": "OK",
        "scope": "surge_entry_cap_decomposition",
        "source_surge_json": str(SURGE_JSON),
        "source_surge_csv": str(SURGE_CSV),
        "surge_ts": surge.get("ts", ""),
        "evaluated_rows": surge.get("evaluated_rows", ""),
        "summary": {
            "rows": len(rows),
            "class_counts": [{"cap_class": key, "count": int(value)} for key, value in counts.most_common()],
            "entry_change_only": counts.get("ENTRY_CHANGE_ONLY", 0),
            "atr_cap_only": counts.get("ATR_CAP_ONLY", 0),
            "entry_change_and_atr_cap": counts.get("ENTRY_CHANGE_AND_ATR_CAP", 0),
            "limit_near_bypass_or_no_cap": counts.get("LIMIT_NEAR_CAP_BYPASS_OR_NO_CAP", 0),
        },
        "risk_contract": {
            "policy_change": False,
            "entry_approval_changed": False,
            "live_order_allowed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "research_only": True,
            "must_not_dispatch": True,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": "OK", "rows": len(rows), "class_counts": payload["summary"]["class_counts"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
