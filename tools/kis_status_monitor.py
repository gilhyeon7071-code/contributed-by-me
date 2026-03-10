from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path
from typing import Dict, Optional

from kis_order_client import KISOrderClient


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


def _load_json(path: Path) -> Dict[str, object]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _health(client: KISOrderClient, code: str) -> Dict[str, object]:
    out: Dict[str, object] = {"ok": False, "error": ""}
    try:
        _ = client._ensure_token()  # noqa: SLF001
        q = client.inquire_price(code=code)
        qo = q.get("output", {}) or {}
        out = {
            "ok": True,
            "code": str(code).zfill(6),
            "price": str(qo.get("stck_prpr", "")),
            "volume": str(qo.get("acml_vol", "")),
            "ask1": str(qo.get("askp1", "")),
            "bid1": str(qo.get("bidp1", "")),
        }
    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)
    return out


def main() -> int:
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
        print(f"[STOP] KIS env/config failed: {e}")
        return 2

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    latest_path = LOG_DIR / "kis_realtime_status_latest.json"

    started = time.time()
    stop_at = (started + int(args.duration_sec)) if int(args.duration_sec) > 0 else 0.0

    while True:
        if stop_at > 0 and time.time() >= stop_at:
            break

        now = dt.datetime.now().isoformat(timespec="seconds")
        account = _load_json(LOG_DIR / "kis_account_snapshot_latest.json")
        ws = _load_json(LOG_DIR / "kis_ws_status_latest.json")
        soak = _load_json(LOG_DIR / "kis_soak_latest.json")

        payload = {
            "ts": now,
            "mode": "mock" if client.cfg.mock else "prod",
            "health": _health(client, str(args.code)),
            "account": account.get("summary", {}),
            "ws": ws,
            "soak": soak,
        }
        latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[MON] {now} ok={payload['health'].get('ok')} latest={latest_path}")

        time.sleep(max(1.0, float(args.interval_sec)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
