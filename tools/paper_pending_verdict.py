from __future__ import annotations

import glob
import json
import os
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def main() -> int:
    files = glob.glob(str(LOGS / "paper_pending_report_*.json"))
    if not files:
        print(f"LATEST - pending_report_missing ok False")
        return 3
    latest = Path(max(files, key=os.path.getmtime))
    report = _read_json(latest)
    verdict = report.get("verdict") if isinstance(report.get("verdict"), dict) else {}
    pending = report.get("pending") if isinstance(report.get("pending"), list) else []
    active = report.get("active") if isinstance(report.get("active"), list) else []
    ok = bool(verdict.get("ok"))
    print(
        "LATEST",
        str(latest),
        "prices_date_max",
        report.get("prices_date_max"),
        "eod_prices_date_max",
        report.get("eod_prices_date_max"),
        "intraday_prices_date_max",
        report.get("intraday_prices_date_max"),
        "pending",
        len(pending),
        "active",
        len(active),
        "pending_blocking_count",
        verdict.get("pending_blocking_count"),
        "verdict_reason",
        verdict.get("reason"),
        "ok",
        ok,
    )
    return 0 if ok else 3


if __name__ == "__main__":
    raise SystemExit(main())
