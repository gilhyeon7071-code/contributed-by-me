from __future__ import annotations

import argparse
import csv
import datetime as dt
import time
from pathlib import Path
from typing import List, Optional

from kis_order_client import KISApiError, KISOrderClient


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


def _parse_codes(raw: str) -> List[str]:
    vals: List[str] = []
    for x in str(raw or "").split(","):
        t = str(x).strip()
        if t:
            vals.append(t.zfill(6))
    return vals


def _ensure_csv_header(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "code", "price", "volume", "ask1", "bid1", "mode", "status", "error"])


def _append_row(path: Path, row: List[object]) -> None:
    with path.open("a", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser(description="Poll KIS quote snapshots and store to CSV")
    ap.add_argument("--codes", required=True, help="Comma-separated 6-digit codes")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--interval-sec", type=float, default=1.0)
    ap.add_argument("--iterations", type=int, default=0, help="0 means infinite")
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--retry-sleep-sec", type=float, default=0.8)
    ap.add_argument("--out-csv", default="")
    args = ap.parse_args()

    codes = _parse_codes(args.codes)
    if not codes:
        print("[STOP] no valid codes")
        return 2

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = None
    else:
        mock_opt = args.mock == "true"

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    today = dt.datetime.now().strftime("%Y%m%d")
    out_csv = Path(args.out_csv) if args.out_csv else (LOG_DIR / f"kis_quote_ticks_{today}.csv")
    _ensure_csv_header(out_csv)

    try:
        client = KISOrderClient.from_env(mock=mock_opt)
    except Exception as e:
        print(f"[STOP] KIS env/config failed: {e}")
        return 2

    mode = "mock" if client.cfg.mock else "prod"
    print(f"[INFO] mode={mode} codes={','.join(codes)} out={out_csv}")

    loop = 0
    while True:
        loop += 1
        for code in codes:
            ts = dt.datetime.now().isoformat(timespec="seconds")
            status = "OK"
            err = ""
            price = ""
            volume = ""
            ask1 = ""
            bid1 = ""

            ok = False
            for attempt in range(1, int(args.max_retries) + 1):
                try:
                    rsp = client.inquire_price(code=code)
                    out = rsp.get("output", {}) or {}
                    price = str(out.get("stck_prpr", ""))
                    volume = str(out.get("acml_vol", ""))
                    ask1 = str(out.get("askp1", ""))
                    bid1 = str(out.get("bidp1", ""))
                    ok = True
                    break
                except (KISApiError, Exception) as e:
                    err = str(e)
                    status = f"ERR_RETRY_{attempt}"
                    if attempt < int(args.max_retries):
                        time.sleep(float(args.retry_sleep_sec))

            if not ok:
                status = "ERR_FINAL"

            _append_row(out_csv, [ts, code, price, volume, ask1, bid1, mode, status, err])

        if args.iterations > 0 and loop >= int(args.iterations):
            break
        time.sleep(max(0.1, float(args.interval_sec)))

    print(f"[OK] saved={out_csv} loops={loop}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
