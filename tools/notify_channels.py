from __future__ import annotations

import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
ALERT_DIR = LOG_DIR / "alerts"
ALERT_STATE_PATH = ALERT_DIR / "alert_policy_state_latest.json"


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


# [2026-08-24] 자격증명 출처 이원화 해소.
# 이 모듈은 환경변수만 보고, telegram_notifier.py 는 .secrets/*.txt 만 봤다.
# 그 결과 2026-05-06 이후 알림 45건이 전부 미전송됐고(토큰 파일은 멀쩡했다),
# 배치 실패 경보가 매일 울리는 동안 아무에게도 닿지 않았다.
# 환경변수를 우선하되, 없으면 telegram_notifier.py 와 같은 파일을 본다.
SECRETS_DIR = ROOT / ".secrets"


def _secret_or_env(env_key: str, secret_filename: str) -> str:
    v = str(os.getenv(env_key, "")).strip()
    if v:
        return v
    try:
        f = SECRETS_DIR / secret_filename
        if f.exists():
            return f.read_text(encoding="utf-8-sig").strip()
    except Exception:
        pass
    return ""


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


_DEFAULT_CHANNELS_BY_LEVEL = {
    "critical": "telegram,file",
    "error": "telegram,file",
    # [2026-08-24] maintenance 가 빠져 있어 .get(lvl,"file") 폴백으로 떨어졌다.
    #   게이트(notifyMaintenance)는 통과시키는데 채널이 없어 결국 차단되는 상태였다.
    #   사용자 선택은 "error 만 휴대폰" 이므로 file 이 맞다. 그것을 명시한다.
    "maintenance": "file",
    "warning": "file",
    "warn": "file",
    "info": "file",
}


def _parse_channels_for_level(level: str, raw: Optional[str]) -> List[str]:
    lvl = str(level or "info").strip().lower()
    env_key = {
        "critical": "ALERT_CHANNELS_CRITICAL",
        "error": "ALERT_CHANNELS_ERROR",
        "warning": "ALERT_CHANNELS_WARNING",
        "warn": "ALERT_CHANNELS_WARNING",
        "info": "ALERT_CHANNELS_INFO",
    }.get(lvl, "")
    if env_key and str(os.getenv(env_key, "")).strip():
        return _parse_channels(os.getenv(env_key, ""))
    if raw is None and not str(os.getenv("ALERT_CHANNELS", "")).strip():
        # [2026-08-24] 사용자 선택: error 만 휴대폰으로 받는다.
        # kakao 는 자격증명이 없어 항상 실패하므로 기본에서 뺀다.
        return _parse_channels(_DEFAULT_CHANNELS_BY_LEVEL.get(lvl, "file"))
    return _parse_channels(raw)


def _cooldown_sec(level: str) -> float:
    lvl = str(level or "info").strip().lower()
    defaults = {
        "critical": 30.0,
        "error": 120.0,
        "warning": 300.0,
        "warn": 300.0,
        "info": 60.0,
    }
    env_key = {
        "critical": "ALERT_COOLDOWN_CRITICAL_SEC",
        "error": "ALERT_COOLDOWN_ERROR_SEC",
        "warning": "ALERT_COOLDOWN_WARNING_SEC",
        "warn": "ALERT_COOLDOWN_WARNING_SEC",
        "info": "ALERT_COOLDOWN_INFO_SEC",
    }.get(lvl, "ALERT_COOLDOWN_INFO_SEC")
    try:
        return max(0.0, float(os.getenv(env_key, str(defaults.get(lvl, 60.0))) or defaults.get(lvl, 60.0)))
    except Exception:
        return float(defaults.get(lvl, 60.0))


def _alert_fingerprint(level: str, text: str, dedup_key: Optional[str] = None) -> str:
    """중복 억제 키.

    [2026-09-08] 기본값이 **본문 전체**라 반복 실패에 쿨다운이 한 번도 걸리지 않았다.
      실측: 2026-09-08 하루에 topn_dispatch rc=2 알림이 28건. cooldown_sec=3600 인데
      전부 발송됐고 결국 텔레그램이 429 Too Many Requests(retry after 342)로 막았다.
      원인은 task_fail_alert 가 본문에 **로그 마지막 25줄**을 넣는다는 것이다.
      거기엔 시각·주문번호·파일명이 들어가 매 회차 텍스트가 달라진다
      -> fingerprint 가 매번 새것 -> 억제 대상이 아니다.
      같은 실패가 채널을 포화시키면 **정작 새 경보가 못 나간다.**
      이 저장소가 2026-05-06~08-24 에 겪은 것이 바로 그 형태다.
      -> 호출자가 안정된 키를 줄 수 있게 한다. 안 주면 기존 동작 그대로다.
    """
    lvl = str(level or "").strip().lower()
    key = str(dedup_key or "").strip()
    if key:
        return f"{lvl}|key:{key}"
    return f"{lvl}|{str(text or '').strip()}"


def _load_alert_state() -> Dict[str, object]:
    try:
        if ALERT_STATE_PATH.exists():
            return json.loads(ALERT_STATE_PATH.read_text(encoding="utf-8-sig"))
    except Exception:
        pass
    return {}


def _save_alert_state(state: Dict[str, object]) -> None:
    """[2026-09-08] 저장 전에 오래된 지문을 버린다.

    지문이 본문 전체였던 탓에 반복 실패마다 새 항목이 쌓였다 - 2026-09-08 하루에
    topn_dispatch 하나로 28개, 파일 전체 178개. 쿨다운은 길어야 1시간이라
    하루 지난 항목은 판정에 쓰이지 않는데 파일만 무한히 커진다.
    보존 기간은 넉넉히 7일로 둔다(쿨다운 최대치보다 훨씬 길다).
    """
    try:
        keep_sec = _env_float("ALERT_STATE_KEEP_SEC", 7 * 24 * 3600.0)
        fps = state.get("fingerprints")
        if isinstance(fps, dict) and keep_sec > 0:
            now = dt.datetime.now().timestamp()
            state["fingerprints"] = {
                k: v for k, v in fps.items()
                if not isinstance(v, dict)
                or (now - float(v.get("last_sent_epoch", 0.0) or 0.0)) <= keep_sec
            }
    except Exception:
        pass  # 정리 실패가 알림 저장을 막으면 안 된다
    ALERT_DIR.mkdir(parents=True, exist_ok=True)
    ALERT_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _log_alert(payload: Dict[str, object]) -> None:
    ALERT_DIR.mkdir(parents=True, exist_ok=True)
    ymd = dt.datetime.now().strftime("%Y%m%d")
    out = ALERT_DIR / f"alerts_{ymd}.jsonl"
    with out.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")



# 2026-08-20: 이 모듈은 텔레그램 API 를 직접 호출해서
# 대시보드의 "수신 이벤트 설정"(config/notification_config.json)을 거치지 않았다.
# 그 결과 사용자가 화면에서 끌 수 없는 알림 경로가 존재했고,
# STOC_FullAuto 가 8일간 실패 알림을 보내는 동안 제어 수단이 없었다.
# telegram_notifier.py 와 같은 설정 파일을 보게 해서 두 경로의 동작을 맞춘다.
NOTIFY_CONFIG_PATH = Path(r"E:\1_Data\config\notification_config.json")

# level -> 수신 이벤트 키. 없는 level 은 게이트하지 않는다(기본 발송).
_LEVEL_TO_NOTIFY_KEY = {
    "error": "notifyError",
    "critical": "notifyError",
    "maintenance": "notifyMaintenance",
    "buy": "notifyBuy",
    "sell": "notifySell",
}


def _telegram_allowed(level: str) -> Tuple[bool, str]:
    """대시보드 수신 이벤트 설정에 따라 텔레그램 발송 허용 여부를 돌려준다.

    설정 파일이 없거나 읽기 실패하면 **막지 않는다**(fail-open).
    알림은 관측 수단이므로, 설정을 못 읽었다고 침묵하면 더 나쁘다.
    """
    key = _LEVEL_TO_NOTIFY_KEY.get(str(level or "").strip().lower())
    if not key:
        return True, "level_not_gated"
    try:
        cfg = json.loads(NOTIFY_CONFIG_PATH.read_text(encoding="utf-8-sig"))
    except Exception:
        return True, "config_unreadable_fail_open"
    if not isinstance(cfg, dict):
        return True, "config_not_dict_fail_open"
    # 기본값은 telegram_notifier.load_config() 과 맞춘다.
    default = True if key in ("notifyError", "notifyMaintenance") else False
    allowed = bool(cfg.get(key, default))
    return allowed, ("allowed" if allowed else f"{key}_off")


def _env_float(key: str, default: float) -> float:
    try:
        return max(0.0, float(str(os.getenv(key, "")).strip() or default))
    except Exception:
        return float(default)


def _send_telegram_once(url: str, chat_id: str, text: str, timeout_sec: float) -> Dict[str, object]:
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


def _telegram_retry_plan(res: Dict[str, object], max_retry_after: float) -> Tuple[bool, float, str]:
    """이 실패를 다시 시도할 것인가, 얼마나 기다릴 것인가.

    [2026-09-08] 재시도가 아예 없었다. timeout 10초 한 번 실패하면 그 경보는 유실됐다.
      실측 2026-09-08: Read timed out 3건. 전부 그대로 사라졌다(file 채널에만 남음).
      경보는 관측 수단이라 한 번의 네트워크 흔들림으로 잃으면 안 된다.
    """
    if res.get("ok"):
        return False, 0.0, "ok"
    if res.get("error"):
        return True, -1.0, "network_error"          # 예외(타임아웃 포함) - 백오프로 재시도
    sc = int(res.get("status_code") or 0)
    if sc == 429:
        body = res.get("body") or {}
        params = body.get("parameters") if isinstance(body, dict) else {}
        wait = float((params or {}).get("retry_after") or 0.0)
        if 0.0 < wait <= max_retry_after:
            return True, wait, "rate_limited_wait"
        # 342초처럼 긴 값은 여기서 기다리지 않는다. 배치가 그만큼 멈추면 안 된다.
        return False, 0.0, "rate_limited_too_long"
    if sc >= 500:
        return True, -1.0, "server_error"
    # 401/400 등은 재시도해도 같은 답이다 (토큰·chat_id 문제)
    return False, 0.0, "not_retryable"


def _send_telegram(text: str, timeout_sec: float = 10.0) -> Dict[str, object]:
    token = _secret_or_env("TELEGRAM_BOT_TOKEN", "telegram_bot_token.txt")
    chat_id = _secret_or_env("TELEGRAM_CHAT_ID", "telegram_chat_id.txt")
    if not token or not chat_id:
        return {"ok": False, "channel": "telegram", "error": "missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID"}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # [2026-09-08] 유한 재시도. **총 대기는 상한이 있어야 한다** -
    #   이 함수는 예약작업 스텝 안에서 불리므로 오래 붙들면 매매 경로가 늦어진다.
    #   기본 3회 시도 / 백오프 1s, 3s / 429 는 retry_after 가 30초 이하일 때만 기다린다.
    attempts_max = int(_env_float("ALERT_TELEGRAM_MAX_ATTEMPTS", 3.0)) or 3
    backoff = [_env_float("ALERT_TELEGRAM_BACKOFF1_SEC", 1.0),
               _env_float("ALERT_TELEGRAM_BACKOFF2_SEC", 3.0)]
    max_retry_after = _env_float("ALERT_TELEGRAM_MAX_RETRY_AFTER_SEC", 30.0)

    trail: List[Dict[str, object]] = []
    res: Dict[str, object] = {}
    for i in range(max(1, attempts_max)):
        res = _send_telegram_once(url, chat_id, text, timeout_sec)
        trail.append({"attempt": i + 1,
                      "ok": bool(res.get("ok")),
                      "status_code": res.get("status_code"),
                      "error": str(res.get("error"))[:120] if res.get("error") else None})
        if res.get("ok"):
            break
        retry, wait, why = _telegram_retry_plan(res, max_retry_after)
        trail[-1]["retry_reason"] = why
        if not retry or i >= max(1, attempts_max) - 1:
            break
        if wait < 0:
            wait = backoff[i] if i < len(backoff) else backoff[-1]
        time.sleep(float(wait))

    # 실패했으면 시도가 한 번이어도 사유를 남긴다. 왜 재시도를 안 했는지가
    # (rate_limited_too_long / not_retryable) 나중에 읽을 때 가장 중요한 정보다.
    if len(trail) > 1 or not res.get("ok"):
        res = dict(res)
        res["attempts"] = trail
    return res


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
    cooldown_sec: Optional[float] = None,
    dedup_key: Optional[str] = None,
) -> Dict[str, object]:
    msg = str(text or "").strip()
    lvl = str(level or "info").strip().lower()
    chs = _parse_channels_for_level(lvl, channels)
    # 반복 진입하는 호출자(예: 60초 루프)가 더 긴 간격을 지정할 수 있다.
    cooldown = _cooldown_sec(lvl) if cooldown_sec is None else max(0.0, float(cooldown_sec))
    fingerprint = _alert_fingerprint(lvl, msg, dedup_key)
    alert_state = _load_alert_state()
    fingerprints = alert_state.get("fingerprints", {}) if isinstance(alert_state.get("fingerprints"), dict) else {}
    prev = fingerprints.get(fingerprint, {}) if isinstance(fingerprints.get(fingerprint), dict) else {}
    now_epoch = dt.datetime.now().timestamp()
    last_epoch = float(prev.get("last_sent_epoch", 0.0) or 0.0)
    suppressed = cooldown > 0 and last_epoch > 0 and (now_epoch - last_epoch) < cooldown

    payload: Dict[str, object] = {
        "ts": _now_ts(),
        "level": lvl,
        "text": msg,
        "channels": chs,
        "cooldown_sec": cooldown,
        "suppressed": suppressed,
        "results": [],
        "ok": True,
    }
    if extra:
        payload["extra"] = extra

    for ch in chs:
        if suppressed and ch != "file":
            r = {"ok": True, "channel": ch, "suppressed": True}
        elif ch == "telegram":
            _allow, _why = _telegram_allowed(lvl)
            if not _allow:
                r = {"ok": True, "channel": "telegram", "skipped": True, "reason": _why}
            else:
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
    if not suppressed:
        fingerprints[fingerprint] = {
            "last_sent_epoch": now_epoch,
            "level": lvl,
            "text": msg,
            "channels": chs,
        }
        alert_state["fingerprints"] = fingerprints
        _save_alert_state(alert_state)

    if (not bool(payload["ok"])) and (not _as_bool(fail_silent, True)):
        raise RuntimeError(f"alert send failed: {payload}")

    return payload
