from __future__ import annotations

import argparse
import csv
import datetime as dt
import logging
import time
from pathlib import Path
from typing import List, Optional

# [2026-08-26] embed 런타임 대응.
#   _runtime/python312-embed/python312._pth 에 E:\1_Data 는 있어도 tools 는 없고,
#   ._pth 가 있으면 스크립트 디렉터리가 sys.path 에 안 들어간다.
#   그래서 맨이름 임포트는 embed 에서 ModuleNotFoundError 로 죽는다
#   (2026-08-26 11:00 첫 자동 스프레드 측정이 이걸로 실패).
#   surge_lob_ingest.py 가 쓰는 패키지 경로 형태를 폴백으로 둔다.
import sys as _sys
from pathlib import Path as _Path

_ROOT = _Path(__file__).resolve().parents[1]
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))
try:
    from kis_order_client import KISApiError, KISOrderClient
    from market_data_adapter import KISMarketDataAdapter
except ModuleNotFoundError:
    from tools.kis_order_client import KISApiError, KISOrderClient
    from tools.market_data_adapter import KISMarketDataAdapter


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
logger = logging.getLogger("kis_quote_poll")


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
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

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
        logger.error("[STOP] no valid codes")
        return 2

    # [2026-09-08] --mock 은 **받기만 하고 쓰이지 않는다.** 아래에서 시세는 항상 실계좌
    #   자격증명으로 조회한다(모의 시세는 지연·결측이 있어 의도된 선택이다).
    #   그런데 인자를 조용히 무시하니 `--mock true` 를 주고도 로그가 mode=prod 로 찍혀
    #   "플래그가 안 먹는다"는 오진을 부른다(실측: 2026-09-08 조사 중 걸렸다).
    #   동작은 그대로 두고, 무시한다는 사실만 소리내어 말한다.
    if args.mock != "auto":
        logger.warning(
            "[IGNORED] --mock %s 은 이 도구에서 무시된다. 시세는 항상 실계좌로 조회한다"
            " (모의 시세 품질 때문). 계좌·주문과 무관한 읽기 전용 조회다", args.mock
        )

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    today = dt.datetime.now().strftime("%Y%m%d")
    out_csv = Path(args.out_csv) if args.out_csv else (LOG_DIR / f"kis_quote_ticks_{today}.csv")
    _ensure_csv_header(out_csv)

    try:
        client = KISOrderClient.from_env(mock=False)  # FORCED PROD FOR MARKET DATA
    except Exception as e:
        logger.error("[STOP] KIS env/config failed: %s", e)
        return 2

    mode = "mock" if client.cfg.mock else "prod"
    logger.info("mode=%s codes=%s out=%s", mode, ",".join(codes), out_csv)
    adapter = KISMarketDataAdapter()

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
                    rsp = adapter.fetch_ticker(client, code)
                    quote = adapter.parse_ticker(rsp)
                    if int(quote.get("ask1", 0) or 0) <= 0 or int(quote.get("bid1", 0) or 0) <= 0:
                        try:
                            hoga_rsp = adapter.fetch_orderbook(client, code)
                            book = adapter.parse_orderbook(hoga_rsp)
                            quote["ask1"] = max(int(quote.get("ask1", 0) or 0), int(book.get("ask1", 0) or 0))
                            quote["bid1"] = max(int(quote.get("bid1", 0) or 0), int(book.get("bid1", 0) or 0))
                        except Exception:
                            pass
                    price = str(quote.get("current_price", ""))
                    volume = str(quote.get("volume", ""))
                    ask1 = str(quote.get("ask1", ""))
                    bid1 = str(quote.get("bid1", ""))
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

    logger.info("[OK] saved=%s loops=%s", out_csv, loop)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
