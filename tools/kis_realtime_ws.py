from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

TOOLS_DIR = Path(__file__).resolve().parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from kis_order_client import KISApiError, KISOrderClient
from notify_channels import send_alert


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SUBSCRIPTION_STATE_PATH = LOG_DIR / "kis_ws_subscriptions_latest.json"
MARKET_EVENT_LATEST_PATH = LOG_DIR / "kis_ws_market_event_latest.json"
logger = logging.getLogger("kis_realtime_ws")

PROD_WS_URL = "ws://ops.koreainvestment.com:21000"
MOCK_WS_URL = "ws://ops.koreainvestment.com:31000"
MAX_SUB_CODES = 40
DEFAULT_MAX_SUBSCRIPTIONS = 40


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


def _in_krx_regular_session(now: Optional[dt.datetime] = None) -> bool:
    cur = now or dt.datetime.now()
    if cur.weekday() >= 5:
        return False
    hhmm = cur.hour * 100 + cur.minute
    return 900 <= hhmm <= 1530


def _iso_from_epoch(epoch: float) -> str:
    if float(epoch or 0.0) <= 0.0:
        return ""
    return dt.datetime.fromtimestamp(epoch).isoformat(timespec="seconds")


def _build_ws_message(approval_key: str, tr_id: str, code: str, tr_type: str) -> Dict[str, object]:
    return {
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": tr_type,
            "content-type": "utf-8",
        },
        "body": {
            "input": {
                "tr_id": tr_id,
                "tr_key": code,
            }
        },
    }


def _send_subscribe(ws: object, approval_key: str, tr_id: str, code: str) -> None:
    ws.send(json.dumps(_build_ws_message(approval_key, tr_id, code, "1"), ensure_ascii=False))


def _send_unsubscribe(ws: object, approval_key: str, tr_id: str, code: str) -> None:
    ws.send(json.dumps(_build_ws_message(approval_key, tr_id, code, "2"), ensure_ascii=False))


def _build_subscription_plan(tr_ids: List[str], codes: List[str], max_subscriptions: int) -> List[Dict[str, str]]:
    limit = max(1, int(max_subscriptions))
    plan: List[Dict[str, str]] = []
    for tr_id in tr_ids:
        for code in codes:
            if len(plan) >= limit:
                return plan
            plan.append({"tr_id": str(tr_id), "code": str(code)})
    return plan


def _parse_extra_subscriptions(raw: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in str(raw or "").split(","):
        token = item.strip()
        if not token or ":" not in token:
            continue
        tr_id, code = [x.strip() for x in token.split(":", 1)]
        if not tr_id or not code:
            continue
        key = (tr_id, code)
        if key in seen:
            continue
        seen.add(key)
        out.append({"tr_id": tr_id, "code": code})
    return out


def _append_jsonl(path: Path, obj: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _write_latest(path: Path, obj: Dict[str, object]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _status_payload(status: str, **extra: object) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "ts": _now_ts(),
        "status": str(status or "").strip().upper(),
    }
    for key, value in extra.items():
        if value is not None:
            payload[str(key)] = value
    return payload


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


def _normalize_market_frame(msg: str) -> Optional[Dict[str, object]]:
    parsed = _try_parse_raw(msg)
    tr_id = str(parsed.get("tr_id") or "").strip()
    payload = str(parsed.get("payload") or "").strip()
    if not tr_id or not payload or payload.startswith("{"):
        return None
    code = ""
    fields: List[str] = []
    if "^" in payload:
        fields = [str(x) for x in payload.split("^")]
        if fields:
            code = str(fields[0]).strip().zfill(6)
    event_type = {
        "H0STCNT0": "trade",
        "H0STASP0": "hoga",
        "H0UPCNT0": "index_trade",
        "H0UPASP0": "index_hoga",
    }.get(tr_id, "raw")
    normalized: Dict[str, object] = {
        "event_type": event_type,
        "tr_id": tr_id,
        "code": code,
        "raw_payload": payload,
    }
    if fields:
        normalized["fields"] = fields[:20]
    return normalized


def _load_subscription_state() -> Dict[str, object]:
    try:
        if SUBSCRIPTION_STATE_PATH.exists():
            return json.loads(SUBSCRIPTION_STATE_PATH.read_text(encoding="utf-8-sig"))
    except Exception:
        pass
    return {}


def _save_subscription_state(*, ws_url: str, mock: bool, codes: List[str], tr_ids: List[str]) -> None:
    payload = {
        "ts": _now_ts(),
        "ws_url": ws_url,
        "mock": bool(mock),
        "codes": list(codes),
        "tr_ids": list(tr_ids),
    }
    _write_latest(SUBSCRIPTION_STATE_PATH, payload)


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

    ap = argparse.ArgumentParser(description="KIS real-time websocket stream collector")
    ap.add_argument("--codes", default="", help="Comma-separated 6-digit symbols")
    ap.add_argument("--channels", default="trade,hoga", help="trade,hoga or raw tr_id list")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--restore-last", action="store_true", help="Restore last saved subscription set when codes are omitted")
    ap.add_argument("--duration-sec", type=int, default=0, help="0 means infinite")
    ap.add_argument("--reconnect-max", type=int, default=0, help="0 means infinite")
    ap.add_argument("--backoff-sec", type=float, default=2.0)
    ap.add_argument("--session-max-sec", type=int, default=int(float(os.getenv("KIS_WS_SESSION_MAX_SEC", "3300") or 3300)))
    ap.add_argument("--stale-frame-sec", type=int, default=int(float(os.getenv("KIS_WS_STALE_FRAME_SEC", "90") or 90)))
    ap.add_argument(
        "--stale-market-data-sec",
        type=int,
        default=int(float(os.getenv("KIS_WS_STALE_MARKET_DATA_SEC", "60") or 60)),
    )
    ap.add_argument("--ping-interval-sec", type=float, default=float(os.getenv("KIS_WS_PING_INTERVAL_SEC", "0") or 0))
    ap.add_argument("--ping-timeout-sec", type=float, default=float(os.getenv("KIS_WS_PING_TIMEOUT_SEC", "10") or 10))
    ap.add_argument(
        "--max-subscriptions",
        type=int,
        default=int(float(os.getenv("KIS_WS_MAX_SUBSCRIPTIONS", str(DEFAULT_MAX_SUBSCRIPTIONS)) or DEFAULT_MAX_SUBSCRIPTIONS)),
    )
    ap.add_argument("--notify-on-error", action="store_true")
    ap.add_argument("--out-jsonl", default="")
    ap.add_argument("--worker-id", default="", help="Optional worker ID to segregate state files")
    ap.add_argument(
        "--extra-subscriptions",
        default="",
        help="Optional comma-separated TR:code subscriptions appended to the stock plan, e.g. H0UPCNT0:0001",
    )
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    global SUBSCRIPTION_STATE_PATH
    global MARKET_EVENT_LATEST_PATH
    
    worker_suffix = f"_{args.worker_id}" if str(args.worker_id).strip() else ""
    if worker_suffix:
        SUBSCRIPTION_STATE_PATH = LOG_DIR / f"kis_ws_subscriptions_latest{worker_suffix}.json"
        MARKET_EVENT_LATEST_PATH = LOG_DIR / f"kis_ws_market_event_latest{worker_suffix}.json"

    status_latest = LOG_DIR / f"kis_ws_status_latest{worker_suffix}.json"

    try:
        import websocket  # type: ignore
    except Exception:
        state = _status_payload(
            "DEPENDENCY_FAIL",
            error="websocket-client package is required",
            notify_on_error=bool(args.notify_on_error),
        )
        _write_latest(status_latest, state)
        logger.error("[STOP] websocket-client package is required. install: pip install websocket-client")
        return 2

    requested_codes = _parse_codes(args.codes)
    restored = _load_subscription_state() if bool(args.restore_last) else {}
    restored_codes = _parse_codes(",".join(restored.get("codes", []) if isinstance(restored.get("codes"), list) else []))
    codes = requested_codes or restored_codes
    if not codes:
        _write_latest(status_latest, _status_payload("INVALID_ARGUMENT", error="no valid codes"))
        logger.error("[STOP] no valid codes")
        return 2
    if len(codes) > MAX_SUB_CODES:
        _write_latest(
            status_latest,
            _status_payload(
                "INVALID_ARGUMENT",
                error=f"subscription code limit exceeded: {len(codes)} > {MAX_SUB_CODES}",
                codes=codes,
                max_sub_codes=MAX_SUB_CODES,
            ),
        )
        logger.error("[STOP] subscription code limit exceeded: %s > %s", len(codes), MAX_SUB_CODES)
        return 2

    restored_tr_ids = [str(x).strip() for x in (restored.get("tr_ids", []) if isinstance(restored.get("tr_ids"), list) else []) if str(x).strip()]
    tr_ids = _channel_tr_ids(args.channels) if str(args.channels or "").strip() else restored_tr_ids
    if not tr_ids:
        _write_latest(status_latest, _status_payload("INVALID_ARGUMENT", error="no valid channels", codes=codes))
        logger.error("[STOP] no valid channels")
        return 2

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = None
    else:
        mock_opt = args.mock == "true"

    reconnect_max = max(0, int(args.reconnect_max))
    stop_at = (time.time() + float(args.duration_sec)) if float(args.duration_sec) > 0 else 0.0

    ymd = dt.datetime.now().strftime("%Y%m%d")
    out_jsonl = Path(args.out_jsonl) if args.out_jsonl else (LOG_DIR / f"kis_ws_ticks{worker_suffix}_{ymd}.jsonl")

    try:
        client = KISOrderClient.from_env(mock=False)  # FORCED PROD FOR MARKET DATA
    except Exception as e:
        _write_latest(
            status_latest,
            _status_payload("CONFIG_FAIL", error=str(e), codes=codes, tr_ids=tr_ids, mock=args.mock),
        )
        logger.error("[STOP] KIS env/config failed: %s", e)
        return 2

    ws_url = _pick_ws_url(bool(client.cfg.mock))
    _save_subscription_state(ws_url=ws_url, mock=bool(client.cfg.mock), codes=codes, tr_ids=tr_ids)
    started = time.time()
    duration_sec = max(0, int(args.duration_sec))
    stop_at = (started + duration_sec) if duration_sec > 0 else 0.0

    reconnect_max = max(0, int(args.reconnect_max))
    reconnects = 0
    total_msgs = 0
    session_max_sec = max(0, int(args.session_max_sec))
    stale_frame_sec = max(0, int(args.stale_frame_sec))
    stale_market_data_sec = max(0, int(args.stale_market_data_sec))
    ping_interval_sec = max(0.0, float(args.ping_interval_sec))
    ping_timeout_sec = max(0.0, float(args.ping_timeout_sec))
    max_subscriptions = max(1, int(args.max_subscriptions))
    subscription_plan = _build_subscription_plan(tr_ids, codes, max_subscriptions)
    extra_subscription_plan = _parse_extra_subscriptions(args.extra_subscriptions)
    for item in extra_subscription_plan:
        if len(subscription_plan) >= max_subscriptions:
            break
        if item not in subscription_plan:
            subscription_plan.append(item)
    requested_subscription_count = int(len(tr_ids) * len(codes) + len(extra_subscription_plan))
    effective_tr_ids = list(tr_ids)
    for item in extra_subscription_plan:
        extra_tr = str(item.get("tr_id") or "")
        if extra_tr and extra_tr not in effective_tr_ids:
            effective_tr_ids.append(extra_tr)

    while True:
        if stop_at > 0 and time.time() >= stop_at:
            break
        if reconnect_max > 0 and reconnects >= reconnect_max:
            exhausted = _status_payload(
                "RECONNECT_EXHAUSTED",
                reconnects=reconnects,
                ws_url=ws_url,
                mock=bool(client.cfg.mock),
                codes=codes,
                tr_ids=tr_ids,
                total_msgs=total_msgs,
            )
            _write_latest(status_latest, exhausted)
            if args.notify_on_error:
                send_alert("[KIS_WS] reconnect exhausted", level="error", extra=exhausted)
            break

        reconnects += 1
        state = _status_payload(
            "CONNECTING",
            reconnect=reconnects,
            ws_url=ws_url,
            mock=bool(client.cfg.mock),
            codes=codes,
            code_count=len(codes),
            max_sub_codes=MAX_SUB_CODES,
            max_subscriptions=max_subscriptions,
            requested_subscription_count=requested_subscription_count,
            subscription_plan_count=len(subscription_plan),
            tr_ids=effective_tr_ids,
            total_msgs=total_msgs,
            restored=bool(args.restore_last and not requested_codes),
        )
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
        stream_state = {
            "approval_issued_at": time.time(),
            "session_started_at": 0.0,
            "last_frame_at": 0.0,
            "last_market_data_at": 0.0,
        }

        def _on_open(ws) -> None:  # noqa: ANN001
            subscribed["value"] = True
            now_epoch = time.time()
            stream_state["session_started_at"] = now_epoch
            stream_state["last_frame_at"] = now_epoch
            for item in subscription_plan:
                _send_subscribe(ws, approval_key, item["tr_id"], item["code"])
            _append_jsonl(
                out_jsonl,
                {
                    "ts": _now_ts(),
                    "event": "subscribed",
                    "tr_ids": effective_tr_ids,
                    "codes": codes,
                    "code_count": len(codes),
                    "max_sub_codes": MAX_SUB_CODES,
                    "max_subscriptions": max_subscriptions,
                    "requested_subscription_count": requested_subscription_count,
                    "subscription_plan_count": len(subscription_plan),
                    "subscription_plan": subscription_plan,
                },
            )
            _write_latest(
                status_latest,
                _status_payload(
                    "SUBSCRIBED",
                    reconnect=reconnects,
                    ws_url=ws_url,
                    mock=bool(client.cfg.mock),
                    codes=codes,
                    code_count=len(codes),
                    max_sub_codes=MAX_SUB_CODES,
                    max_subscriptions=max_subscriptions,
                    requested_subscription_count=requested_subscription_count,
                    subscription_plan_count=len(subscription_plan),
                    tr_ids=effective_tr_ids,
                    total_msgs=total_msgs,
                    subscribed=True,
                    approval_issued_at=_iso_from_epoch(stream_state["approval_issued_at"]),
                    session_started_at=_iso_from_epoch(stream_state["session_started_at"]),
                ),
            )

        def _on_message(ws, message: str) -> None:  # noqa: ANN001
            nonlocal total_msgs
            stream_state["last_frame_at"] = time.time()
            if "PINGPONG" in str(message or ""):
                try:
                    ws.send(message)
                    _append_jsonl(out_jsonl, {"ts": _now_ts(), "event": "pingpong_echo", "raw": str(message)})
                    _write_latest(
                        status_latest,
                        _status_payload(
                            "PINGPONG_ECHO",
                            reconnect=reconnects,
                            ws_url=ws_url,
                            mock=bool(client.cfg.mock),
                            codes=codes,
                            code_count=len(codes),
                            max_sub_codes=MAX_SUB_CODES,
                            max_subscriptions=max_subscriptions,
                            requested_subscription_count=requested_subscription_count,
                            subscription_plan_count=len(subscription_plan),
                            tr_ids=effective_tr_ids,
                            total_msgs=total_msgs,
                            subscribed=bool(subscribed["value"]),
                            approval_issued_at=_iso_from_epoch(stream_state["approval_issued_at"]),
                            session_started_at=_iso_from_epoch(stream_state["session_started_at"]),
                            last_frame_at=_iso_from_epoch(stream_state["last_frame_at"]),
                            last_market_data_at=_iso_from_epoch(stream_state["last_market_data_at"]),
                        ),
                    )
                except Exception as e:
                    _append_jsonl(out_jsonl, {"ts": _now_ts(), "event": "pingpong_echo_error", "error": str(e)})
                return
            total_msgs += 1
            if not str(message or "").startswith("{"):
                stream_state["last_market_data_at"] = time.time()
            rec = {
                "ts": _now_ts(),
                "event": "message",
                "seq": total_msgs,
            }
            rec.update(_try_parse_raw(message))
            normalized = _normalize_market_frame(message)
            if normalized:
                rec["normalized"] = normalized
                _write_latest(
                    MARKET_EVENT_LATEST_PATH,
                    {
                        "ts": _now_ts(),
                        "seq": total_msgs,
                        **normalized,
                    },
                )
                if str(normalized.get("tr_id") or "").startswith("H0UP"):
                    _write_latest(
                        LOG_DIR / "kis_ws_index_latest.json",
                        {
                            "ts": _now_ts(),
                            "seq": total_msgs,
                            **normalized,
                        },
                    )
            _append_jsonl(out_jsonl, rec)
            if total_msgs % 50 == 0:
                st = {
                    "ts": _now_ts(),
                    "status": "STREAMING",
                    "reconnect": reconnects,
                    "ws_url": ws_url,
                    "mock": bool(client.cfg.mock),
                    "codes": codes,
                    "code_count": len(codes),
                    "max_sub_codes": MAX_SUB_CODES,
                    "max_subscriptions": max_subscriptions,
                    "requested_subscription_count": requested_subscription_count,
                    "subscription_plan_count": len(subscription_plan),
                    "tr_ids": effective_tr_ids,
                    "total_msgs": total_msgs,
                    "subscribed": bool(subscribed["value"]),
                    "approval_issued_at": _iso_from_epoch(stream_state["approval_issued_at"]),
                    "session_started_at": _iso_from_epoch(stream_state["session_started_at"]),
                    "last_frame_at": _iso_from_epoch(stream_state["last_frame_at"]),
                    "last_market_data_at": _iso_from_epoch(stream_state["last_market_data_at"]),
                }
                _write_latest(status_latest, st)
                if "H0UPCNT0" in effective_tr_ids:
                    index_st = dict(st)
                    index_st["status_source"] = "combined_stock_ws"
                    index_st["source_worker_id"] = str(args.worker_id or "")
                    _write_latest(LOG_DIR / "kis_ws_status_latest_index.json", index_st)

        def _on_error(ws, error: object) -> None:  # noqa: ANN001
            err = str(error)
            _append_jsonl(out_jsonl, {"ts": _now_ts(), "event": "error", "error": err})
            _write_latest(
                status_latest,
                _status_payload(
                    "STREAM_ERROR",
                    reconnect=reconnects,
                    ws_url=ws_url,
                    mock=bool(client.cfg.mock),
                    codes=codes,
                    code_count=len(codes),
                    max_sub_codes=MAX_SUB_CODES,
                    max_subscriptions=max_subscriptions,
                    requested_subscription_count=requested_subscription_count,
                    subscription_plan_count=len(subscription_plan),
                    tr_ids=effective_tr_ids,
                    total_msgs=total_msgs,
                    subscribed=bool(subscribed["value"]),
                    approval_issued_at=_iso_from_epoch(stream_state["approval_issued_at"]),
                    session_started_at=_iso_from_epoch(stream_state["session_started_at"]),
                    last_frame_at=_iso_from_epoch(stream_state["last_frame_at"]),
                    last_market_data_at=_iso_from_epoch(stream_state["last_market_data_at"]),
                    error=err,
                ),
            )

        def _on_close(ws, code: object, reason: object) -> None:  # noqa: ANN001
            _append_jsonl(
                out_jsonl,
                {"ts": _now_ts(), "event": "closed", "code": str(code), "reason": str(reason), "reconnect": reconnects},
            )
            _write_latest(
                status_latest,
                _status_payload(
                    "CLOSED",
                    reconnect=reconnects,
                    ws_url=ws_url,
                    mock=bool(client.cfg.mock),
                    codes=codes,
                    code_count=len(codes),
                    max_sub_codes=MAX_SUB_CODES,
                    max_subscriptions=max_subscriptions,
                    requested_subscription_count=requested_subscription_count,
                    subscription_plan_count=len(subscription_plan),
                    tr_ids=effective_tr_ids,
                    total_msgs=total_msgs,
                    subscribed=bool(subscribed["value"]),
                    approval_issued_at=_iso_from_epoch(stream_state["approval_issued_at"]),
                    session_started_at=_iso_from_epoch(stream_state["session_started_at"]),
                    last_frame_at=_iso_from_epoch(stream_state["last_frame_at"]),
                    last_market_data_at=_iso_from_epoch(stream_state["last_market_data_at"]),
                    code=str(code),
                    reason=str(reason),
                ),
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
            "code_count": len(codes),
            "max_sub_codes": MAX_SUB_CODES,
            "max_subscriptions": max_subscriptions,
            "requested_subscription_count": requested_subscription_count,
            "subscription_plan_count": len(subscription_plan),
            "tr_ids": tr_ids,
            "total_msgs": total_msgs,
        }
        _write_latest(status_latest, st2)

        try:
            stop_event = threading.Event()

            def _close_with_reason(status: str, event: str, **extra: object) -> None:
                _append_jsonl(out_jsonl, {"ts": _now_ts(), "event": event, "reconnect": reconnects, **extra})
                _write_latest(
                    status_latest,
                    _status_payload(
                        status,
                        reconnect=reconnects,
                        ws_url=ws_url,
                        mock=bool(client.cfg.mock),
                        codes=codes,
                        code_count=len(codes),
                        max_sub_codes=MAX_SUB_CODES,
                        max_subscriptions=max_subscriptions,
                        requested_subscription_count=requested_subscription_count,
                        subscription_plan_count=len(subscription_plan),
                        tr_ids=effective_tr_ids,
                        total_msgs=total_msgs,
                        subscribed=bool(subscribed["value"]),
                        approval_issued_at=_iso_from_epoch(stream_state["approval_issued_at"]),
                        session_started_at=_iso_from_epoch(stream_state["session_started_at"]),
                        last_frame_at=_iso_from_epoch(stream_state["last_frame_at"]),
                        last_market_data_at=_iso_from_epoch(stream_state["last_market_data_at"]),
                        **extra,
                    ),
                )
                try:
                    app.close()
                except Exception:
                    pass

            def _watchdog() -> None:
                while not stop_event.is_set():
                    now_epoch = time.time()
                    session_started_at = float(stream_state["session_started_at"] or 0.0)
                    frame_anchor = float(stream_state["last_frame_at"] or session_started_at or 0.0)
                    market_anchor = float(stream_state["last_market_data_at"] or session_started_at or 0.0)

                    if stop_at > 0 and now_epoch >= stop_at:
                        _close_with_reason("DURATION_TIMEOUT", "duration_timeout")
                        return
                    if session_max_sec > 0 and session_started_at > 0 and (now_epoch - session_started_at) >= session_max_sec:
                        _close_with_reason("SESSION_ROTATE", "session_rotate", session_max_sec=session_max_sec)
                        return
                    if stale_frame_sec > 0 and bool(subscribed["value"]) and frame_anchor > 0 and (now_epoch - frame_anchor) >= stale_frame_sec:
                        _close_with_reason("STALE_FRAME", "stale_frame", stale_frame_sec=stale_frame_sec)
                        return
                    if (
                        stale_market_data_sec > 0
                        and bool(subscribed["value"])
                        and _in_krx_regular_session()
                        and market_anchor > 0
                        and (now_epoch - market_anchor) >= stale_market_data_sec
                    ):
                        _close_with_reason(
                            "STALE_MARKET_DATA",
                            "stale_market_data",
                            stale_market_data_sec=stale_market_data_sec,
                        )
                        return
                    time.sleep(1.0)

            watchdog = threading.Thread(target=_watchdog, daemon=True)
            watchdog.start()
            run_forever_kwargs: Dict[str, object] = {}
            if ping_interval_sec > 0:
                run_forever_kwargs["ping_interval"] = ping_interval_sec
                if ping_timeout_sec > 0:
                    run_forever_kwargs["ping_timeout"] = ping_timeout_sec
            app.run_forever(**run_forever_kwargs)
        except Exception as e:
            _append_jsonl(out_jsonl, {"ts": _now_ts(), "event": "run_forever_exception", "error": str(e)})
            exc_state = _status_payload(
                "RUN_FOREVER_EXCEPTION",
                reconnect=reconnects,
                ws_url=ws_url,
                mock=bool(client.cfg.mock),
                codes=codes,
                code_count=len(codes),
                max_sub_codes=MAX_SUB_CODES,
                max_subscriptions=max_subscriptions,
                requested_subscription_count=requested_subscription_count,
                subscription_plan_count=len(subscription_plan),
                tr_ids=effective_tr_ids,
                total_msgs=total_msgs,
                subscribed=bool(subscribed["value"]),
                approval_issued_at=_iso_from_epoch(stream_state["approval_issued_at"]),
                session_started_at=_iso_from_epoch(stream_state["session_started_at"]),
                last_frame_at=_iso_from_epoch(stream_state["last_frame_at"]),
                last_market_data_at=_iso_from_epoch(stream_state["last_market_data_at"]),
                error=str(e),
            )
            _write_latest(status_latest, exc_state)
            if args.notify_on_error:
                send_alert(f"[KIS_WS] stream error: {e}", level="error", extra=exc_state)
        finally:
            stop_event.set()

        if stop_at > 0 and time.time() >= stop_at:
            break
        time.sleep(max(1.0, float(args.backoff_sec)))

    try:
        prev_status = json.loads(status_latest.read_text(encoding="utf-8-sig")) if status_latest.exists() else {}
    except Exception:
        prev_status = {}
    prev_status_name = str(prev_status.get("status") or "").strip().upper()
    if total_msgs <= 0:
        final_status = "NO_MESSAGES"
        final_rc = 3
    elif prev_status_name in {"APPROVAL_FAIL", "STREAM_ERROR", "RUN_FOREVER_EXCEPTION", "RECONNECT_EXHAUSTED"}:
        final_status = prev_status_name
        final_rc = 2
    else:
        final_status = "DONE"
        final_rc = 0

    final = {
        "ts": _now_ts(),
        "status": final_status,
        "ws_url": ws_url,
        "mock": bool(client.cfg.mock),
        "codes": codes,
        "code_count": len(codes),
        "max_sub_codes": MAX_SUB_CODES,
        "max_subscriptions": max_subscriptions,
        "requested_subscription_count": requested_subscription_count,
        "subscription_plan_count": len(subscription_plan),
        "tr_ids": effective_tr_ids,
        "reconnects": reconnects,
        "total_msgs": total_msgs,
        "session_max_sec": session_max_sec,
        "stale_frame_sec": stale_frame_sec,
        "stale_market_data_sec": stale_market_data_sec,
        "previous_status": prev_status_name,
        "out_jsonl": str(out_jsonl),
    }
    _write_latest(status_latest, final)
    if final_rc == 0:
        logger.info("[OK] done total_msgs=%s reconnects=%s out=%s", total_msgs, reconnects, out_jsonl)
    else:
        logger.error("[STOP] %s total_msgs=%s reconnects=%s out=%s", final_status, total_msgs, reconnects, out_jsonl)
    return final_rc


if __name__ == "__main__":
    raise SystemExit(main())
