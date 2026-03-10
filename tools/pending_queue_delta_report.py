#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build pending queue day-over-day delta report.

Inputs:
- 2_Logs/pending_entry_status_latest.json
- 2_Logs/pending_entry_signals_latest.csv (fallback row count)

Outputs:
- 2_Logs/pending_entry_queue_history.csv
- 2_Logs/pending_entry_queue_delta_latest.json
- 2_Logs/pending_entry_queue_delta_<YYYYMMDD_HHMMSS>.json
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
STATUS_PATH = LOG_DIR / "pending_entry_status_latest.json"
SIGNALS_PATH = LOG_DIR / "pending_entry_signals_latest.csv"
HISTORY_PATH = LOG_DIR / "pending_entry_queue_history.csv"
LATEST_PATH = LOG_DIR / "pending_entry_queue_delta_latest.json"

HISTORY_FIELDS = [
    "recorded_at",
    "source_generated_at",
    "status",
    "market_regime",
    "pending_queue_len",
    "no_next_day",
    "entry_ready",
    "filled",
    "signals_rows",
]


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _now_compact() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            raw = path.read_text(encoding=enc)
            obj = json.loads(raw)
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    return {}


def _count_csv_rows(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header is None:
                    return 0
                cnt = sum(1 for _ in reader)
                return int(cnt)
        except Exception:
            continue
    return None


def _read_history(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                rows = list(csv.DictReader(f))
            return [{k: str(v or "") for k, v in r.items()} for r in rows]
        except Exception:
            continue
    return []


def _write_history(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HISTORY_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: str(r.get(k, "")) for k in HISTORY_FIELDS})


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_row(status: Dict[str, Any], signals_rows: Optional[int]) -> Dict[str, Any]:
    queue_status = _to_int(status.get("pending_queue_len"))
    queue_effective = queue_status if queue_status is not None else _to_int(signals_rows)
    if queue_effective is None:
        queue_effective = 0
    return {
        "recorded_at": _now_ts(),
        "source_generated_at": str(status.get("generated_at") or ""),
        "status": str(status.get("status") or ""),
        "market_regime": str(status.get("market_regime") or ""),
        "pending_queue_len": int(queue_effective),
        "no_next_day": _to_int(status.get("no_next_day")) or 0,
        "entry_ready": _to_int(status.get("entry_ready")) or 0,
        "filled": _to_int(status.get("filled")) or 0,
        "signals_rows": _to_int(signals_rows) if signals_rows is not None else "",
    }


def _upsert_history(rows: List[Dict[str, str]], cur: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = [dict(r) for r in rows]
    src_ts = str(cur.get("source_generated_at") or "")
    if src_ts:
        same = [i for i, r in enumerate(out) if str(r.get("source_generated_at") or "") == src_ts]
        if same:
            out[same[-1]] = cur
            return out
    out.append(cur)
    return out


def _to_row_payload(r: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if r is None:
        return None
    return {
        "recorded_at": str(r.get("recorded_at") or ""),
        "source_generated_at": str(r.get("source_generated_at") or ""),
        "status": str(r.get("status") or ""),
        "market_regime": str(r.get("market_regime") or ""),
        "pending_queue_len": _to_int(r.get("pending_queue_len")) or 0,
        "no_next_day": _to_int(r.get("no_next_day")) or 0,
        "entry_ready": _to_int(r.get("entry_ready")) or 0,
        "filled": _to_int(r.get("filled")) or 0,
        "signals_rows": _to_int(r.get("signals_rows")),
    }


def _trend(delta: Optional[int]) -> str:
    if delta is None:
        return "NA"
    if delta < 0:
        return "DECREASE"
    if delta > 0:
        return "INCREASE"
    return "UNCHANGED"


def main() -> int:
    status = _read_json(STATUS_PATH)
    signals_rows = _count_csv_rows(SIGNALS_PATH)
    cur = _build_row(status, signals_rows)

    old_rows = _read_history(HISTORY_PATH)
    new_rows = _upsert_history(old_rows, cur)
    _write_history(HISTORY_PATH, new_rows)

    prev = new_rows[-2] if len(new_rows) >= 2 else None
    cur_q = _to_int(cur.get("pending_queue_len")) or 0
    prev_q = _to_int(prev.get("pending_queue_len")) if isinstance(prev, dict) else None
    delta = (cur_q - prev_q) if prev_q is not None else None

    report = {
        "generated_at": _now_ts(),
        "status_file": str(STATUS_PATH),
        "signals_file": str(SIGNALS_PATH),
        "history_file": str(HISTORY_PATH),
        "history_rows": len(new_rows),
        "current": _to_row_payload(cur),
        "previous": _to_row_payload(prev if isinstance(prev, dict) else None),
        "delta": {
            "pending_queue_len": delta,
            "trend": _trend(delta),
            "decreased": bool(delta is not None and delta < 0),
            "increased": bool(delta is not None and delta > 0),
            "abs_change": abs(delta) if delta is not None else None,
        },
    }

    _write_json(LATEST_PATH, report)
    ts_path = LOG_DIR / f"pending_entry_queue_delta_{_now_compact()}.json"
    _write_json(ts_path, report)

    prev_txt = "NA" if prev_q is None else str(prev_q)
    delta_txt = "NA" if delta is None else f"{delta:+d}"
    print(
        "[PENDING_QUEUE_DELTA] "
        f"current={cur_q} prev={prev_txt} delta={delta_txt} trend={report['delta']['trend']} "
        f"status={cur.get('status') or 'NA'}"
    )
    print(f"[OK] wrote: {LATEST_PATH}")
    print(f"[OK] wrote: {ts_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
