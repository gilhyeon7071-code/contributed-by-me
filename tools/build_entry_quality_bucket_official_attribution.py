from __future__ import annotations

import csv
import glob
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
BUCKET_SUMMARY_JSON = LOG_DIR / "entry_quality_cause_bucket_report_latest.json"
OUT_JSON = LOG_DIR / "entry_quality_bucket_official_attribution_latest.json"
OUT_CSV = LOG_DIR / "entry_quality_bucket_official_attribution_latest.csv"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def main() -> int:
    bucket_report = load_json(BUCKET_SUMMARY_JSON)
    bucket_pnl = {
        str(r.get("cause_bucket", "")).lower(): r
        for r in bucket_report.get("bucket_summary", [])
        if isinstance(r, dict)
    }
    active_buckets = set(bucket_pnl)
    rows = []
    stale_bucket_results = []
    for raw in sorted(glob.glob(str(LOG_DIR / "backtest_validation_bucket_*.json"))):
        path = Path(raw)
        data = load_json(path)
        bucket = path.stem.replace("backtest_validation_bucket_", "")
        bucket_key = bucket.lower()
        if bucket_key not in active_buckets:
            stale_bucket_results.append(str(path))
            continue
        meta = data.get("artifacts", {}).get("base_meta", {}).get("entry_quality_audit_validation_scenario", {})
        acceptance = data.get("artifacts", {}).get("acceptance", {})
        failures = [g.get("name", "") for g in data.get("gate_results", []) if isinstance(g, dict) and not g.get("passed")]
        summary = bucket_pnl.get(bucket_key, {})
        rows.append(
            {
                "bucket": bucket,
                "official_passed": bool(data.get("passed")),
                "failure_gates": "|".join(f for f in failures if f),
                "bucket_orders": summary.get("orders"),
                "bucket_pnl_sum": summary.get("pnl_sum"),
                "bucket_loss_orders": summary.get("loss_orders"),
                "target_entry_order_ids": meta.get("target_entry_order_ids"),
                "removed_rows": meta.get("removed_rows"),
                "removed_buy_rows": meta.get("removed_buy_rows"),
                "removed_sell_rows": meta.get("removed_sell_rows"),
                "pnl_ci95_low": acceptance.get("pnl_ci95_low"),
                "turnover_monthly": acceptance.get("turnover_monthly"),
                "audit_path": meta.get("audit_path", ""),
                "result_json": str(path),
            }
        )

    rows.sort(key=lambda r: (not bool(r["official_passed"]), str(r["bucket"])))
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if rows else "WARN",
        "scope": "read_only_entry_quality_bucket_official_attribution",
        "inputs": {
            "bucket_summary_json": str(BUCKET_SUMMARY_JSON),
            "bucket_validation_glob": str(LOG_DIR / "backtest_validation_bucket_*.json"),
        },
        "summary": {
            "bucket_scenarios": len(rows),
            "official_pass_buckets": sum(1 for r in rows if r["official_passed"]),
            "official_fail_buckets": sum(1 for r in rows if not r["official_passed"]),
            "stale_bucket_results_ignored": len(stale_bucket_results),
        },
        "stale_bucket_results_ignored": stale_bucket_results,
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if rows:
        with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    print(json.dumps({"status": payload["status"], "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0 if rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
