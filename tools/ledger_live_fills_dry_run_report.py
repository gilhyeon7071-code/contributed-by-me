from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set

sys.path.insert(0, str(Path(__file__).resolve().parent))
from repair_rootb_ledger_missing_live_fills import build_repair


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
ROOTB = Path(r"E:\vibe\buffett")
LIVE_PATH = ROOTB / "data" / "live" / "live_fills.csv"
LEDGER_PATH = ROOTB / "data" / "ledger" / "paper_fills_ledger.csv"


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _norm_ymd(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _fill_id_set(rows: List[Dict[str, str]]) -> Set[str]:
    return {str(row.get("fill_id") or "").strip() for row in rows if str(row.get("fill_id") or "").strip()}


def _norm_code(v: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(v or ""))
    return digits[-6:].zfill(6) if digits else str(v or "").strip()


def _norm_qty(v: Any) -> str:
    try:
        return str(round(float(str(v or "0").replace(",", "")), 6))
    except Exception:
        return "0.0"


def _coverage_key(row: Dict[str, str]) -> str:
    source_order = str(row.get("source_order_id") or "").strip()
    if not source_order:
        note = str(row.get("note") or "")
        for part in note.split(";"):
            if part.startswith("source_order_id="):
                source_order = part.split("=", 1)[1].strip()
                break
    return "|".join(
        [
            _norm_ymd(row.get("date") or row.get("datetime")),
            _norm_code(row.get("code")),
            str(row.get("side") or "").strip().upper(),
            _norm_qty(row.get("fill_qty") or row.get("qty")),
            source_order,
        ]
    )


def _covered_by_ledger(row: Dict[str, str], ledger_keys: Set[str]) -> bool:
    source = str(row.get("source") or "").strip().upper()
    side = str(row.get("side") or "").strip().upper()
    if source not in {"EXEC_SYNC", "PAPER_ENGINE_BRIDGE"} or side != "SELL":
        return False
    key = _coverage_key(row)
    return bool(key.rsplit("|", 1)[-1]) and key in ledger_keys


def _oper_start_ymd() -> str:
    raw = str(os.getenv("PAPER_OPER_START_YMD", "20260301") or "").strip()
    ymd = _norm_ymd(raw)
    return ymd if len(ymd) == 8 else "20260301"


def build_report(all_dates: bool = True) -> Dict[str, Any]:
    start_ymd = _oper_start_ymd()
    repair_aligned = build_repair(apply=False, start_ymd=start_ymd)
    missing_ids = list(repair_aligned.get("missing_fill_ids") or [])
    missing_keys = list(repair_aligned.get("missing_repair_keys") or [])
    missing_sample = [
        {
            "fill_id": str(fill_id),
            "repair_key": str(missing_keys[idx]) if idx < len(missing_keys) else "",
        }
        for idx, fill_id in enumerate(missing_ids[:20])
    ]
    issue_rows = (
        int(repair_aligned.get("missing_rows") or 0)
        + int(repair_aligned.get("roota_missing_rows") or 0)
        + int(repair_aligned.get("roota_bridge_extra_rows") or 0)
        + int(repair_aligned.get("paper_closed_exec_sync_buy_rows") or 0)
        + int(repair_aligned.get("live_absent_duplicate_rows") or 0)
        + int(repair_aligned.get("fill_id_drift_rows") or 0)
    )
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "dry_run",
        "apply": False,
        "alignment": "repair_rootb_ledger_missing_live_fills.build_repair",
        "live_path": str(LIVE_PATH),
        "ledger_path": str(LEDGER_PATH),
        "live_exists": LIVE_PATH.exists(),
        "ledger_exists": LEDGER_PATH.exists(),
        "oper_start_ymd": start_ymd,
        "live_rows": int(repair_aligned.get("live_rows") or 0),
        "ledger_rows": int(repair_aligned.get("ledger_rows_before") or 0),
        "missing_rows": int(repair_aligned.get("missing_rows") or 0),
        "missing_fill_ids": missing_ids,
        "missing_repair_keys": missing_keys,
        "missing_sample": missing_sample,
        "covered_duplicate_rows": int(repair_aligned.get("redundant_exec_sync_missing_rows") or 0),
        "covered_exec_sync_buy_rows": int(repair_aligned.get("redundant_exec_sync_buy_missing_rows") or 0),
        "covered_roota_absent_bridge_rows": int(repair_aligned.get("roota_absent_bridge_missing_rows") or 0),
        "covered_duplicate_fill_ids": [],
        "roota_missing_rows": int(repair_aligned.get("roota_missing_rows") or 0),
        "roota_bridge_extra_rows": int(repair_aligned.get("roota_bridge_extra_rows") or 0),
        "metadata_blank_rows": int(repair_aligned.get("metadata_blank_rows") or 0),
        "exec_sync_identity_dedupe_rows": int(repair_aligned.get("exec_sync_identity_dedupe_rows") or 0),
        "exec_sync_entry_lineage_dedupe_rows": int(repair_aligned.get("exec_sync_entry_lineage_dedupe_rows") or 0),
        "paper_closed_exec_sync_buy_rows": int(repair_aligned.get("paper_closed_exec_sync_buy_rows") or 0),
        "live_absent_duplicate_rows": int(repair_aligned.get("live_absent_duplicate_rows") or 0),
        "fill_id_drift_rows": int(repair_aligned.get("fill_id_drift_rows") or 0),
        "status": "PASS" if issue_rows == 0 else "FAIL",
    }

    live = _read_csv(LIVE_PATH)
    ledger = _read_csv(LEDGER_PATH)
    ledger_ids = _fill_id_set(ledger)
    ledger_keys = {_coverage_key(row) for row in ledger}
    start_ymd = _oper_start_ymd()
    covered_duplicates = []
    missing = []
    for row in live:
        fill_id = str(row.get("fill_id") or "").strip()
        if not fill_id or fill_id in ledger_ids:
            continue
        if _covered_by_ledger(row, ledger_keys):
            covered_duplicates.append(fill_id)
            continue
        ymd = _norm_ymd(row.get("date"))
        if ymd and ymd < start_ymd:
            continue
        if not all_dates and ymd != datetime.now().strftime("%Y%m%d"):
            continue
        missing.append(
            {
                "fill_id": fill_id,
                "date": ymd,
                "code": str(row.get("code") or "").strip(),
                "side": str(row.get("side") or "").strip(),
            }
        )
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "dry_run",
        "apply": False,
        "live_path": str(LIVE_PATH),
        "ledger_path": str(LEDGER_PATH),
        "live_exists": LIVE_PATH.exists(),
        "ledger_exists": LEDGER_PATH.exists(),
        "oper_start_ymd": start_ymd,
        "live_rows": int(len(live)),
        "ledger_rows": int(len(ledger)),
        "missing_rows": int(len(missing)),
        "missing_fill_ids": [x["fill_id"] for x in missing],
        "missing_sample": missing[:20],
        "covered_duplicate_rows": int(len(covered_duplicates)),
        "covered_duplicate_fill_ids": covered_duplicates,
        "status": "PASS" if len(missing) == 0 else "FAIL",
    }


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    report = build_report(all_dates=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = LOG_DIR / f"ledger_live_fills_dry_run_{stamp}.json"
    latest = LOG_DIR / "ledger_live_fills_dry_run_latest.json"
    text = json.dumps(report, ensure_ascii=False, indent=2)
    out.write_text(text, encoding="utf-8")
    latest.write_text(text, encoding="utf-8")
    print(f"[LEDGER_LIVE_DRY_RUN] status={report['status']} missing_rows={report['missing_rows']}")
    print(f"[LEDGER_LIVE_DRY_RUN] json={out}")
    print(f"[LEDGER_LIVE_DRY_RUN] latest={latest}")
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
