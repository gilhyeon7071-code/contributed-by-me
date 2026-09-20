"""Summarize accumulated no-trade blocker markout history.

Read-only diagnostic layer. It summarizes the history produced by
``build_no_trade_blocker_markout_archive.py`` and never changes trading policy,
orders, fills, ledger state, or gates.
"""
from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

HISTORY_CSV = LOG_DIR / "no_trade_blocker_markout_archive_history.csv"
OUT_JSON = LOG_DIR / "no_trade_blocker_markout_summary_latest.json"
OUT_CSV = LOG_DIR / "no_trade_blocker_markout_summary_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _f(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(str(value).strip())
    except Exception:
        return None


def _summarize(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    vals = [_f(row.get("return_to_latest_pct")) for row in rows]
    vals = [val for val in vals if val is not None]
    observed_days = sorted({str(row.get("d_ref") or "") for row in rows if str(row.get("d_ref") or "").strip()})
    observed_runs = sorted({str(row.get("observed_at") or "") for row in rows if str(row.get("observed_at") or "").strip()})
    return {
        "rows": len(rows),
        "priced_rows": len(vals),
        "observed_day_count": len(observed_days),
        "observed_days": observed_days,
        "observed_run_count": len(observed_runs),
        "avg_return_to_latest_pct": round(sum(vals) / len(vals), 6) if vals else None,
        "validation_read_counts": dict(Counter(str(row.get("validation_read") or "") for row in rows)),
        "missed_gain_ge_1pct_n": sum(1 for val in vals if val >= 1.0),
        "avoided_loss_le_minus_1pct_n": sum(1 for val in vals if val <= -1.0),
        "positive_n": sum(1 for val in vals if val > 0),
        "negative_n": sum(1 for val in vals if val < 0),
    }


def _bucket_rows(rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    by_bucket: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_bucket[str(row.get("blocker_bucket") or "UNKNOWN")].append(row)
    out: List[Dict[str, Any]] = []
    for bucket, bucket_rows in sorted(by_bucket.items()):
        summary = _summarize(bucket_rows)
        out.append(
            {
                "blocker_bucket": bucket,
                **{k: v for k, v in summary.items() if k not in {"observed_days"}},
                "observed_days": "|".join(summary.get("observed_days") or []),
                "read_only_for_trading": True,
                "trading_effect": False,
                "policy_effect": False,
                "policy_change_applied": False,
            }
        )
    return out


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else ["blocker_bucket", "rows"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    rows = _read_csv(HISTORY_CSV)
    bucket_rows = _bucket_rows(rows)
    overall = _summarize(rows)
    payload = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "scope": "read_only_no_trade_blocker_markout_summary",
        "source_history_csv": str(HISTORY_CSV),
        "overall": overall,
        "by_bucket": {row["blocker_bucket"]: row for row in bucket_rows},
        "judgment": {
            "frequency_bottleneck_confirmed": bool(rows),
            "policy_relaxation_supported_now": False,
            "reason": "Summary is observational. Policy relaxation requires independent multi-day evidence by blocker bucket.",
        },
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, bucket_rows)
    print(json.dumps({"status": "PASS", "out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "overall": overall}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
