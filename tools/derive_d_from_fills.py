from __future__ import annotations

import argparse
import csv
from pathlib import Path


def _ymd8(raw: str) -> str:
    s = "".join(ch for ch in str(raw or "") if ch.isdigit())
    return s[:8] if len(s) >= 8 else ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, dest="csv_path")
    ns = ap.parse_args()

    p = Path(ns.csv_path)
    if not p.exists():
        print("")
        return 0

    max_buy = ""
    max_any = ""
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            ymd = _ymd8(row.get("datetime", ""))
            if len(ymd) != 8:
                continue
            if ymd > max_any:
                max_any = ymd
            side = str(row.get("side", "")).strip().upper()
            if side == "BUY" and ymd > max_buy:
                max_buy = ymd

    print(max_buy or max_any)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
