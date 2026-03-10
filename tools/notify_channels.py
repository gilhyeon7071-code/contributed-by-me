from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import requests


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
ALERT_DIR = LOG_DIR / "alerts"


def _now_ts() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _as_bool(v: object, default: bool = False) -> bool:
    if v is None:
        return bool(default)
    s = str(v).strip().lower()
    if s in {"1", "true", "y", "yes", "on"}:
        return True
    if s in {"0", "false", "n", "no", "off"}:
        return False
    return bool(default)


def _parse_channels(raw: Optional[str]) -> List[str]:
    if raw is None:
        raw = os.getenv("ALERT_CHANNELS", "telegram,kakao,file")
    vals: List[str] = []
    for x in str(raw or "").split(","):
        t = str(x).strip().lower()
        if t:
            vals.append(t)
    if "file" not in vals:
        vals.append("file")
    return vals


def _log_alert(payload: Dict[str, object]) -> None:
    ALERT_DIR.mkdir(parents=True, exist_ok=True)
    ymd = dt.datetime.now().strftime("%Y%m%d")
    out = ALERT_DIR / f"alerts_{ymd}.jsonl"
    with out.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _send_telegram(text: str, timeout_sec: float = 10.0) -> Dict[str, object]:
    token = str(os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
    chat_id = str(os.getenv("TELEGRAM_CHAT_ID", "")).strip()
    if not token or not chat_id:
        return {"ok": False, "channel": "telegram", "error": "missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID"}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(
            url,
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
            timeout=float(timeout_sec),
        )
        body = r.json() if r.content else {}
        ok = (r.status_code < 400) and bool(body.get("ok", False))
        return {"ok": ok, "channel": "telegram", "status_code": r.status_code, "body": body if not ok else {"ok": True}}
    except Exception as e:
        return {"ok": False, "channel": "telegram", "error": str(e)}


def _send_kakao(text: str, timeout_sec: float = 10.0) -> Dict[str, object]:
    token = str(os.getenv("KAKAO_ACCESS_TOKEN", "")).strip()
    if not token:
        return {"ok": False, "channel": "kakao", "error": "missing KAKAO_ACCESS_TOKEN"}

    url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
    template = {
        "object_type": "text",
        "text": text[:1000],
        "link": {"web_url": "https://open.kakao.com", "mobile_web_url": "https://open.kakao.com"},
        "button_title": "열기",
    }
    headers = {"Authorization": f"Bearer {token}"}
    data = {"template_object": json.dumps(template, ensure_ascii=False)}
    try:
        r = requests.post(url, headers=headers, data=data, timeout=float(timeout_sec))
        body = r.json() if r.content else {}
        ok = (r.status_code < 400) and (int(body.get("result_code", -1)) == 0)
        return {"ok": ok, "channel": "kakao", "status_code": r.status_code, "body": body if not ok else {"result_code": 0}}
    except Exception as e:
        return {"ok": False, "channel": "kakao", "error": str(e)}


def send_alert(
    text: str,
    *,
    level: str = "info",
    channels: Optional[str] = None,
    fail_silent: bool = True,
    timeout_sec: float = 10.0,
    extra: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    msg = str(text or "").strip()
    lvl = str(level or "info").strip().lower()
    chs = _parse_channels(channels)

    payload: Dict[str, object] = {
        "ts": _now_ts(),
        "level": lvl,
        "text": msg,
        "channels": chs,
        "results": [],
        "ok": True,
    }
    if extra:
        payload["extra"] = extra

    for ch in chs:
        if ch == "telegram":
            r = _send_telegram(msg, timeout_sec=timeout_sec)
        elif ch == "kakao":
            r = _send_kakao(msg, timeout_sec=timeout_sec)
        elif ch == "file":
            r = {"ok": True, "channel": "file"}
        else:
            r = {"ok": False, "channel": ch, "error": "unsupported channel"}

        cast = dict(r)
        payload["results"].append(cast)
        if not bool(cast.get("ok", False)) and ch != "file":
            payload["ok"] = False

    _log_alert(payload)

    if (not bool(payload["ok"])) and (not _as_bool(fail_silent, True)):
        raise RuntimeError(f"alert send failed: {payload}")

    return payload
