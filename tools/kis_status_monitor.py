from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import time
from pathlib import Path
from typing import Dict, Optional

from kis_order_client import KISOrderClient
from market_data_adapter import KISMarketDataAdapter


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
logger = logging.getLogger("kis_status_monitor")


def _load_json(path: Path) -> Dict[str, object]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _ws_status_path() -> Path:
    # kis_realtime_ws.py writes kis_ws_status_latest{_worker}.json, and
    # kis_ws_multiplexer.py always assigns a worker id, so the unsuffixed
    # name stops being updated. Pick whichever worker file is newest.
    files = [p for p in LOG_DIR.glob("kis_ws_status_latest*.json") if p.is_file()]
    if not files:
        return LOG_DIR / "kis_ws_status_latest.json"
    return max(files, key=lambda p: p.stat().st_mtime)


def _health(client: KISOrderClient, adapter: KISMarketDataAdapter, code: str) -> Dict[str, object]:
    out: Dict[str, object] = {"ok": False, "error": ""}
    try:
        _ = client._ensure_token()  # noqa: SLF001
        q = adapter.fetch_ticker(client, code)
        qo = adapter.parse_ticker(q)
        if int(qo.get("ask1", 0) or 0) <= 0 or int(qo.get("bid1", 0) or 0) <= 0:
            try:
                hoga = adapter.fetch_orderbook(client, code)
                hob = adapter.parse_orderbook(hoga)
                qo["ask1"] = max(int(qo.get("ask1", 0) or 0), int(hob.get("ask1", 0) or 0))
                qo["bid1"] = max(int(qo.get("bid1", 0) or 0), int(hob.get("bid1", 0) or 0))
            except Exception:
                pass
        out = {
            "ok": True,
            "code": str(code).zfill(6),
            "price": str(qo.get("current_price", "")),
            "volume": str(qo.get("volume", "")),
            "ask1": str(qo.get("ask1", "")),
            "bid1": str(qo.get("bid1", "")),
        }
    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)
    return out


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

    ap = argparse.ArgumentParser(description="Realtime status monitor writer")
    ap.add_argument("--code", default="005930")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--interval-sec", type=float, default=15.0)
    ap.add_argument("--duration-sec", type=int, default=0, help="0 means infinite")
    args = ap.parse_args()

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = None
    else:
        mock_opt = args.mock == "true"

    try:
        client = KISOrderClient.from_env(mock=mock_opt)
    except Exception as e:
        logger.error("[STOP] KIS env/config failed: %s", e)
        return 2

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    latest_path = LOG_DIR / "kis_realtime_status_latest.json"
    adapter = KISMarketDataAdapter()

    started = time.time()
    stop_at = (started + int(args.duration_sec)) if int(args.duration_sec) > 0 else 0.0

    while True:
        if stop_at > 0 and time.time() >= stop_at:
            break

        now = dt.datetime.now().isoformat(timespec="seconds")
        account = _load_json(LOG_DIR / "kis_account_snapshot_latest.json")
        ws = _load_json(_ws_status_path())
        soak = _load_json(LOG_DIR / "kis_soak_latest.json")

        payload = {
            "ts": now,
            "mode": "mock" if client.cfg.mock else "prod",
            "health": _health(client, adapter, str(args.code)),
            "account": account.get("summary", {}),
            "ws": ws,
            "soak": soak,
        }
        latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("[MON] %s ok=%s latest=%s", now, payload["health"].get("ok"), latest_path)

        time.sleep(max(1.0, float(args.interval_sec)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
