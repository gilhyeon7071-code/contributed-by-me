from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SOURCE_AUDIT = LOG_DIR / "entry_quality_cross_section_audit_latest.json"
BUCKET_REPORT = LOG_DIR / "entry_quality_cause_bucket_report_latest.json"
OUT_DIR = LOG_DIR / "entry_quality_bucket_audits"


def main() -> int:
    source = json.loads(SOURCE_AUDIT.read_text(encoding="utf-8"))
    bucket_report = json.loads(BUCKET_REPORT.read_text(encoding="utf-8"))
    source_rows = source.get("rows") if isinstance(source.get("rows"), list) else []
    bucket_rows = bucket_report.get("rows") if isinstance(bucket_report.get("rows"), list) else []
    bucket_by_order = {str(r.get("entry_order_id", "")): str(r.get("cause_bucket", "")) for r in bucket_rows}
    grouped: dict[str, list[dict]] = {}
    for row in source_rows:
        order_id = str(row.get("entry_order_id", ""))
        bucket = bucket_by_order.get(order_id, "UNCLASSIFIED_ENTRY_QUALITY")
        grouped.setdefault(bucket, []).append(row)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    outputs = []
    for bucket, rows in sorted(grouped.items()):
        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": "PASS" if rows else "WARN",
            "scope": "read_only_entry_quality_bucket_audit",
            "bucket": bucket,
            "source_audit": str(SOURCE_AUDIT),
            "source_bucket_report": str(BUCKET_REPORT),
            "summary": {
                "flagged_entry_orders": len(rows),
                "ledger_realized_pnl_sum": sum(float(r.get("ledger_realized_pnl_sum") or 0.0) for r in rows),
            },
            "rows": rows,
        }
        out_path = OUT_DIR / f"{bucket.lower()}_audit.json"
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        outputs.append(str(out_path))
    print(json.dumps({"status": "PASS", "output_count": len(outputs), "outputs": outputs}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
