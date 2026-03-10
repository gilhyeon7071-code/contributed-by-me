from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

from kis_order_client import KISApiError, KISOrderClient
from notify_channels import send_alert


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

PROD_WS_URL = "ws://ops.koreainvestment.com:21000"
MOCK_WS_URL = "ws://ops.koreainvestment.com:31000"


def _now_ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _parse_codes(raw: str) -> List[str]:
    vals: List[str] = []
    for x in str(raw or "").split(","):
        t = str(x).strip()
        if not t:
            continue
        vals.append(t.zfill(6))
    return sorted(set(vals))


def _channel_tr_ids(raw: str) -> List[str]:
    req: List[str] = []
    mapping = {
        "trade": "H0STCNT0",
        "hoga": "H0STASP0",
        "quote": "H0STCNT0",
    }
    for x in str(raw or "trade,hoga").split(","):
        k = str(x).strip().lower()
        if not k:
            continue
        if k in mapping:
            req.append(mapping[k])
        else:
            req.append(k)
    out: List[str] = []
    for tr in req:
        if tr not in out:
            out.append(tr)
    return out


def _pick_ws_url(mock: bool) -> str:
    env_url = str(os.getenv("KIS_WS_URL", "")).strip()
    if env_url:
        return env_url
    return MOCK_WS_URL if mock else PROD_WS_URL


def _append_jsonl(path: Path, obj: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _write_latest(path: Path, obj: Dict[str, object]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _try_parse_raw(msg: str) -> Dict[str, object]:
    s = str(msg or "")
    out: Dict[str, object] = {"raw": s}

    if s.startswith("{"):
        try:
            j = json.loads(s)
            out["json"] = j
            return out
        except Exception:
            return out

    if "|" in s:
        parts = s.split("|")
        out["frame_kind"] = parts[0] if len(parts) > 0 else ""
        out["tr_id"] = parts[1] if len(parts) > 1 else ""
        out["payload_count"] = parts[2] if len(parts) > 2 else ""
        payload = parts[-1] if len(parts) >= 4 else ""
        out["payload"] = payload
        if "^" in payload:
            fields = payload.split("^")
            out["fields"] = fields[:20]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="KIS real-time websocket stream collector")
    ap.add_argument("--codes", required=True, help="Comma-separated 6-digit symbols")
    ap.add_argument("--channels", default="trade,hoga", help="trade,hoga or raw tr_id list")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--duration-sec", type=int, default=0, help="0 means infinite")
    ap.add_argument("--reconnect-max", type=int, default=0, help="0 means infinite")
    ap.add_argument("--backoff-sec", type=float, default=2.0)
    ap.add_argument("--notify-on-error", action="store_true")
    ap.add_argument("--out-jsonl", default="")
    args = ap.parse_args()

    try:
        import websocket  # type: ignore
    except Exception:
        print("[STOP] websocket-client package is required. install: pip install websocket-client")
        return 2

    codes = _parse_codes(args.codes)
    if not codes:
        print("[STOP] no valid codes")
        return 2

    tr_ids = _channel_tr_ids(args.channels)
    if not tr_ids:
        print("[STOP] no valid channels")
        return 2

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = None
    else:
        mock_opt = args.mock == "true"

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ymd = dt.datetime.now().strftime("%Y%m%d")
    out_jsonl = Path(args.out_jsonl) if args.out_jsonl else (LOG_DIR / f"kis_ws_ticks_{ymd}.jsonl")
    status_latest = LOG_DIR / "kis_ws_status_latest.json"

    try:
        client = KISOrderClient.from_env(mock=mock_opt)
    except Exception as e:
        print(f"[STOP] KIS env/config failed: {e}")
        return 2

    ws_url = _pick_ws_url(bool(client.cfg.mock))
    started = time.time()
    stop_at = (started + int(args.duration_sec)) if int(args.duration_sec) > 0 else 0.0

    reconnects = 0
    total_msgs = 0

    while True:
        if stop_at > 0 and time.time() >= stop_at:
            break
        if int(args.reconnect_max) > 0 and reconnects > int(args.reconnect_max):
            break

        reconnects += 1
        state = {
            "ts": _now_ts(),
            "status": "CONNECTING",
            "reconnect": reconnects,
            "ws_url": ws_url,
            "mock": bool(client.cfg.mock),
            "codes": codes,
            "tr_ids": tr_ids,
            "total_msgs": total_msgs,
        }
        _write_latest(status_latest, state)

        try:
            approval_key = client.issue_ws_approval_key()
        except Exception as e:
            state.update({"status": "APPROVAL_FAIL", "error": str(e)})
            _write_latest(status_latest, state)
            if args.notify_on_error:
                send_alert(f"[KIS_WS] approval fail: {e}", level="error", extra=state)
            time.sleep(max(1.0, float(args.backoff_sec)))
            continue

        subscribed = {"value": False}

        def _on_open(ws) -> None:  # noqa: ANN001
            subscribed["value"] = True
            for tr_id in tr_ids:
                for code in codes:
                    msg = {
                        "header": {
                            "approval_key": approval_key,
                            "custtype": "P",
                            "tr_type": "1",
                            "content-type": "utf-8",
                        },
                        "body": {"input": {"tr_id": tr_id, "tr_key": code}},
                    }
                    ws.send(json.dumps(msg, ensure_ascii=False))
            _append_jsonl(out_jsonl, {"ts": _now_ts(), "event": "subscribed", "tr_ids": tr_ids, "codes": codes})

        def _on_message(ws, message: str) -> None:  # noqa: ANN001
            nonlocal total_msgs
            total_msgs += 1
            rec = {
                "ts": _now_ts(),
                "event": "message",
                "seq": total_msgs,
            }
            rec.update(_try_parse_raw(message))
            _append_jsonl(out_jsonl, rec)
            if total_msgs % 50 == 0:
                st = {
                    "ts": _now_ts(),
                    "status": "STREAMING",
                    "reconnect": reconnects,
                    "ws_url": ws_url,
                    "mock": bool(client.cfg.mock),
                    "codes": codes,
                    "tr_ids": tr_ids,
                    "total_msgs": total_msgs,
                    "subscribed": bool(subscribed["value"]),
                }
                _write_latest(status_latest, st)

        def _on_error(ws, error: object) -> None:  # noqa: ANN001
            err = str(error)
            _append_jsonl(out_jsonl, {"ts": _now_ts(), "event": "error", "error": err})

        def _on_close(ws, code: object, reason: object) -> None:  # noqa: ANN001
            _append_jsonl(
                out_jsonl,
                {"ts": _now_ts(), "event": "closed", "code": str(code), "reason": str(reason), "reconnect": reconnects},
            )

        app = websocket.WebSocketApp(
            ws_url,
            on_open=_on_open,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
        )

        st2 = {
            "ts": _now_ts(),
            "status": "RUN_FOREVER",
            "reconnect": reconnects,
            "ws_url": ws_url,
            "mock": bool(client.cfg.mock),
            "codes": codes,
            "tr_ids": tr_ids,
            "total_msgs": total_msgs,
        }
        _write_latest(status_latest, st2)

        try:
            app.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            _append_jsonl(out_jsonl, {"ts": _now_ts(), "event": "run_forever_exception", "error": str(e)})
            if args.notify_on_error:
                send_alert(f"[KIS_WS] stream error: {e}", level="error", extra={"reconnect": reconnects})

        if stop_at > 0 and time.time() >= stop_at:
            break
        time.sleep(max(1.0, float(args.backoff_sec)))

    final = {
        "ts": _now_ts(),
        "status": "DONE",
        "ws_url": ws_url,
        "mock": bool(client.cfg.mock),
        "codes": codes,
        "tr_ids": tr_ids,
        "reconnects": reconnects,
        "total_msgs": total_msgs,
        "out_jsonl": str(out_jsonl),
    }
    _write_latest(status_latest, final)
    print(f"[OK] done total_msgs={total_msgs} reconnects={reconnects} out={out_jsonl}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
