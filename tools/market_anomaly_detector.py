# -*- coding: utf-8 -*-
"""지수 이상(급등/급락) 탐지기.

2026-08-21 개편. 그 전에는 두 가지 결함이 있었다.

1. 지수 3개(0001 코스피 / 1001 코스닥 / 2001 코스피200)가 모두 같은 WS 구독으로 들어와
   `kis_ws_index_latest.json` **한 파일을 덮어썼다.** 탐지기는 그 파일 하나를 읽고
   거기 있던 지수를 "시장 상태"로 판정했다. 어느 지수를 보게 될지는 도착 순서에 달렸다.
   실측(2026-08-21 30MB 구간): 2001 200프레임 / 0001 18 / 1001 18 -> 코스닥을 볼 확률 약 8%.
   같은 날 코스피 +0.77%, 코스닥 -5.26% 로 서로 다른 이야기를 하고 있었다.

2. 한 번 기록된 이벤트가 **그날 내내 남았다.** 레벨이 NORMAL 로 돌아와도 이벤트는 빠지지
   않아서, 장중 한 틱이 -5% 를 스치면 그날 매수가 전부 막혔다. 관측은 확률적인데 기록은
   단방향이라, 하루가 길수록 언젠가 한 번은 걸리는 구조였다.

지금은
- 지수별로 **각각** 최신 틱을 모아 **각각** 판정한다(틱 로그 tail 스캔, WS 재시작 불필요)
- 이벤트를 매 실행마다 **현재값으로 재조정**한다. 조건이 풀리면 이벤트도 해제되고
  이력(`market_event_gate_history.jsonl`)에 `resolved_at` 과 함께 남는다

바뀌지 않은 것: 임계값(±5.0%), 이벤트 타입, `scope="MARKET"`.
scope 는 종목별 시장 정보(후보 파일 `market` 컬럼)가 대부분 UNKNOWN 이라 아직 나눌 수 없다.
따라서 **코스닥 급락이 코스피 종목까지 막는 동작은 그대로다.**
상세: .agent/PLANS.md 2026-08-21 (10)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
INDEX_WS_LOG = LOGS / "kis_ws_index_latest.json"
GATE_FILE = LOGS / "market_event_gate_latest.json"
GATE_HISTORY_FILE = LOGS / "market_event_gate_history.jsonl"
STATUS_FILE = LOGS / "market_anomaly_detector_status_latest.json"
# [2026-09-13] **관측 실패 빈도를 알아야 판단할 수 있다.**
#   상태 파일은 매 실행 덮이므로 이력이 없다. 2026-09-12 에 진입 게이트를
#   "관측 못 하면 막는다" 로 바꿨는데, 그게 얼마나 자주 걸리는지 잴 방법이 없었다.
#   날짜 없는 append-only 다 - 날짜 붙은 산출물은 30일 뒤 사라진다.
OBS_LEDGER = LOGS / "market_observation_ledger.jsonl"

KST = timezone(timedelta(hours=9))

# KIS 업종코드. 0001 코스피 / 1001 코스닥 / 2001 코스피200
# 표본 근거: 2_Logs/kis_ws_ticks_index_20260709.jsonl (0001=7458.43, 1001=794.36, 2001=1197.58)
INDEX_CODE_MARKET = {"0001": "kospi", "1001": "kosdaq", "2001": "kospi"}
INDEX_CODE_NAME = {"0001": "코스피", "1001": "코스닥", "2001": "코스피200"}

SURGE_PCT = 5.0
CRASH_PCT = -5.0
EVENT_BOOST = "MARKET_SURGE_BOOST"
EVENT_CRASH = "CIRCUIT_BREAKER"

# 지수 틱은 장중에만 갱신된다. 이보다 오래된 값은 직전 세션의 잔존값이므로
# 오늘의 시장 상태로 사용하지 않는다.
MAX_SOURCE_AGE_SEC = max(1.0, float(str(os.getenv("MARKET_ANOMALY_MAX_AGE_SEC", "300")).strip() or "300"))
# 틱 로그에서 지수 프레임은 희소하다(30MB 당 236개, 그중 85% 가 2001).
# 세 지수를 모두 잡으려면 tail 이 충분히 커야 한다.
TICK_TAIL_MB = max(0.5, float(str(os.getenv("MARKET_ANOMALY_TICK_TAIL_MB", "8")).strip() or "8"))
TICK_MAX_FILES = max(1, int(str(os.getenv("MARKET_ANOMALY_TICK_MAX_FILES", "3")).strip() or "3"))

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(message)s")
logger = logging.getLogger("market_anomaly_detector")


def _now_iso() -> str:
    return datetime.now(tz=KST).isoformat(timespec="seconds")


def _now_ymd() -> str:
    return datetime.now(tz=KST).strftime("%Y%m%d")


def _parse_time(fields: List[str]) -> str:
    tick_time_raw = fields[1] if len(fields) > 1 else ""
    formatted_time = _now_iso()
    if len(tick_time_raw) == 6:
        hh = int(tick_time_raw[:2])
        mm = int(tick_time_raw[2:4])
        ss = int(tick_time_raw[4:])
        ampm = "오후" if hh >= 12 else "오전"
        hh_12 = hh - 12 if hh > 12 else (12 if hh == 0 else hh)
        formatted_time = f"{ampm} {hh_12}시 {mm}분 {ss}초"
    return formatted_time


def _write_status(payload: Dict[str, Any]) -> None:
    try:
        doc = {"generated_at": _now_iso(), **payload}
        STATUS_FILE.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning(f"Failed to write status file: {e}")


def _age_sec(raw_ts: Any) -> Tuple[Optional[float], str]:
    text = str(raw_ts or "").strip()
    if not text:
        return None, "missing_ts"
    try:
        ts = datetime.fromisoformat(text)
    except ValueError:
        return None, f"unparsable_ts:{text}"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=KST)
    return (datetime.now(tz=KST) - ts).total_seconds(), ""


def _frame_from_doc(doc: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
    """WS 프레임 하나를 판정 가능한 형태로 정규화한다. 형식이 어긋나면 None."""
    if not isinstance(doc, dict):
        return None
    normalized = doc.get("normalized") if isinstance(doc.get("normalized"), dict) else doc
    tr_id = str(normalized.get("tr_id") or doc.get("tr_id") or "").strip().upper()
    if tr_id and not tr_id.startswith("H0UP"):
        return None
    event_type = str(normalized.get("event_type") or "").strip().lower()
    if event_type and event_type != "index_trade":
        return None
    raw_payload = str(normalized.get("raw_payload") or "")
    fields = raw_payload.split("^") if raw_payload else []
    if len(fields) < 10:
        return None
    code = str(fields[0]).strip()
    if code not in INDEX_CODE_MARKET:
        return None
    try:
        change_pct = float(fields[9])
    except (TypeError, ValueError):
        return None
    return {
        "code": code,
        "change_pct": change_pct,
        "index_value": fields[2] if len(fields) > 2 else "",
        "tick_time": fields[1] if len(fields) > 1 else "",
        "ts": str(doc.get("ts") or normalized.get("ts") or ""),
        "fields": fields,
        "source": source,
    }


def _candidate_tick_files() -> List[Path]:
    ymd = _now_ymd()
    files = [p for p in LOGS.glob(f"kis_ws_ticks_*_{ymd}.jsonl") if p.is_file()]
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[:TICK_MAX_FILES]


def _iter_tail_lines(path: Path, tail_bytes: int) -> List[str]:
    size = path.stat().st_size
    if tail_bytes <= 0 or size <= tail_bytes:
        text = path.read_text(encoding="utf-8", errors="replace")
        return text.splitlines()
    with path.open("rb") as f:
        f.seek(max(0, size - tail_bytes))
        chunk = f.read()
    lines = chunk.decode("utf-8", errors="replace").splitlines()
    return lines[1:] if lines else []


def _collect_index_frames() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """지수 코드별 **최신** 프레임을 모은다.

    틱 로그를 tail 스캔한다. WS 프로세스를 고치지 않아도 오늘 바로 동작하고,
    한 파일을 덮어쓰는 `kis_ws_index_latest.json` 과 달리 지수 3개를 모두 볼 수 있다.
    틱 로그에서 아무것도 못 찾으면 기존 단일 파일로 폴백한다(관측 1개라도 남긴다).
    """
    meta: Dict[str, Any] = {"mode": "", "files": [], "lines_scanned": 0, "frames": 0}
    frames: Dict[str, Dict[str, Any]] = {}
    tail_bytes = int(TICK_TAIL_MB * 1024 * 1024)
    scanned = 0
    files = _candidate_tick_files()
    for path in files:
        try:
            lines = _iter_tail_lines(path, tail_bytes)
        except Exception as e:
            logger.warning(f"tick tail read failed {path.name}: {e}")
            continue
        meta["files"].append(path.name)
        for line in lines:
            scanned += 1
            if "H0UP" not in line:
                continue
            try:
                doc = json.loads(line)
            except Exception:
                continue
            frame = _frame_from_doc(doc, source=path.name)
            if not frame:
                continue
            prev = frames.get(frame["code"])
            if prev is None or str(frame.get("ts") or "") >= str(prev.get("ts") or ""):
                frames[frame["code"]] = frame
    meta["lines_scanned"] = scanned
    meta["frames"] = len(frames)
    meta["mode"] = "tick_tail" if frames else "tick_tail_empty"

    if not frames and INDEX_WS_LOG.exists():
        try:
            doc = json.loads(INDEX_WS_LOG.read_text(encoding="utf-8-sig"))
            frame = _frame_from_doc(doc, source=INDEX_WS_LOG.name)
            if frame:
                frames[frame["code"]] = frame
                meta["mode"] = "single_latest_fallback"
        except Exception as e:
            logger.warning(f"index latest read failed: {e}")
    return frames, meta


def _append_obs_ledger(payload: Dict[str, Any]) -> None:
    """관측 가능 여부를 한 줄씩 남긴다. 실패해도 본 작업을 막지 않는다."""
    try:
        observed = payload.get("observed") or {}
        rec = {
            "ts": _now_iso(),
            "ymd": _now_ymd(),
            "valid": bool(payload.get("valid", bool(observed))),
            "action": str(payload.get("action") or ""),
            "n_observed": len(observed),
            "codes": sorted(observed.keys()),
            "rejected": payload.get("rejected") or {},
        }
        OBS_LEDGER.parent.mkdir(parents=True, exist_ok=True)
        with OBS_LEDGER.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + chr(10))
    except Exception as exc:
        logger.warning("[OBS_LEDGER] 기록 실패 %s: %s", type(exc).__name__, exc)


def _classify(change_pct: float) -> Tuple[str, str]:
    """(event_type, level). 정상이면 ('', 'NORMAL')."""
    if change_pct >= SURGE_PCT:
        return EVENT_BOOST, "BOOST"
    if change_pct <= CRASH_PCT:
        return EVENT_CRASH, "HIGH"
    return "", "NORMAL"


def _archive_events(rows: List[Dict[str, Any]], note: str, extra: Optional[Dict[str, Any]] = None) -> bool:
    """이벤트를 이력 파일로 옮긴다. 실패하면 False(호출부는 원본을 지우지 않는다)."""
    if not rows:
        return True
    try:
        with GATE_HISTORY_FILE.open("a", encoding="utf-8") as fh:
            for row in rows:
                doc = {"archived_at": _now_iso(), "archive_reason": note, **(row or {})}
                if extra:
                    doc.update(extra)
                fh.write(json.dumps(doc, ensure_ascii=False) + "\n")
        return True
    except Exception as e:
        logger.warning(f"Failed to archive events ({note}), keeping them: {e}")
        return False


def _load_gate_doc() -> Dict[str, Any]:
    if not GATE_FILE.exists():
        return {}
    try:
        doc = json.loads(GATE_FILE.read_text(encoding="utf-8-sig"))
        return doc if isinstance(doc, dict) else {}
    except Exception:
        return {}


def _update_market_status(states: Dict[str, Dict[str, Any]], frames: Dict[str, Dict[str, Any]]) -> List[str]:
    """지수 판정을 시장별 플래그로 반영한다. 관측된 시장만 건드린다."""
    changes: List[str] = []
    config_file = ROOT / "config" / "market_status.json"
    try:
        status_doc: Dict[str, Any] = {"kospi": {}, "kosdaq": {}}
        if config_file.exists():
            try:
                old_doc = json.loads(config_file.read_text(encoding="utf-8-sig"))
                if isinstance(old_doc, dict) and "kospi" in old_doc:
                    status_doc = old_doc
            except Exception:
                pass

        # 한 시장에 여러 지수가 매핑된다(0001, 2001 -> kospi). 이상이 하나라도 있으면 이상으로 본다.
        by_market: Dict[str, Dict[str, Any]] = {}
        for code, st in states.items():
            market_key = INDEX_CODE_MARKET.get(code, "")
            if not market_key:
                continue
            cur = by_market.setdefault(market_key, {"circuit": False, "sidecar": False, "code": code})
            if st["event_type"] == EVENT_CRASH:
                cur["circuit"] = True
                cur["code"] = code
            elif st["event_type"] == EVENT_BOOST:
                cur["sidecar"] = True
                if not cur["circuit"]:
                    cur["code"] = code

        for market_key, agg in by_market.items():
            target = status_doc.get(market_key, {}) if isinstance(status_doc.get(market_key), dict) else {}
            is_circuit = bool(agg["circuit"])
            is_sidecar = bool(agg["sidecar"] and not agg["circuit"])
            was_circuit = bool(target.get("circuit_breaker"))
            was_sidecar = bool(target.get("sidecar"))
            if was_circuit == is_circuit and was_sidecar == is_sidecar:
                continue
            tick_fields = (frames.get(agg["code"]) or {}).get("fields") or []
            target["circuit_breaker"] = is_circuit
            target["sidecar"] = is_sidecar
            if is_circuit or is_sidecar:
                target["trigger_time"] = _parse_time(tick_fields)
                target.pop("release_time", None)
                target.pop("last_alert_type", None)
            else:
                target["last_alert_type"] = "서킷브레이커" if was_circuit else "사이드카"
                target["release_time"] = _parse_time(tick_fields)
                target.pop("trigger_time", None)
            status_doc[market_key] = target
            changes.append(f"{market_key}:circuit={is_circuit},sidecar={is_sidecar}")

        if changes:
            config_file.write_text(json.dumps(status_doc, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning(f"Failed to update market_status.json: {e}")
    return changes


def main() -> int:
    today = _now_ymd()
    frames, collect_meta = _collect_index_frames()

    # 지수별로 신선도를 따로 본다. 하나가 늙었다고 나머지를 버리지 않는다.
    states: Dict[str, Dict[str, Any]] = {}
    rejected: Dict[str, str] = {}
    for code, frame in frames.items():
        age, ts_reason = _age_sec(frame.get("ts"))
        if ts_reason:
            rejected[code] = ts_reason
            continue
        if age is None:
            rejected[code] = "missing_ts"
            continue
        if age > MAX_SOURCE_AGE_SEC:
            rejected[code] = f"stale_source:age={int(age)}s>max={int(MAX_SOURCE_AGE_SEC)}s"
            continue
        if age < -60.0:
            rejected[code] = f"future_ts:age={int(age)}s"
            continue
        event_type, level = _classify(float(frame["change_pct"]))
        states[code] = {
            "code": code,
            "market_name": INDEX_CODE_NAME.get(code, code),
            "market_key": INDEX_CODE_MARKET.get(code, ""),
            "change_pct": float(frame["change_pct"]),
            "index_value": frame.get("index_value", ""),
            "tick_time": frame.get("tick_time", ""),
            "age_sec": round(float(age), 1),
            "event_type": event_type,
            "level": level,
            "source": frame.get("source", ""),
        }

    status_payload: Dict[str, Any] = {
        "as_of_ymd": today,
        "collect": collect_meta,
        "max_age_sec": MAX_SOURCE_AGE_SEC,
        "observed": {c: {k: v for k, v in s.items() if k != "code"} for c, s in states.items()},
        "rejected": rejected,
    }

    if not states:
        # 볼 수 있는 지수가 하나도 없다. 없는 이벤트를 만들지도, 있는 이벤트를 지우지도 않는다.
        logger.warning(f"No usable index frame ({rejected or collect_meta}); gate untouched")
        status_payload.update({"valid": False, "action": "no_observation"})
        _append_obs_ledger(status_payload)
        _write_status(status_payload)
        return 0

    gate_doc = _load_gate_doc()
    events = gate_doc.get("events")
    if not isinstance(events, list):
        events = []

    # 1) 과거 날짜 이벤트는 이력으로 뺀다. 게이트 파일은 당일치만 들고 간다.
    past_rows = [e for e in events if str((e or {}).get("date", "") or "").strip() != today]
    today_rows = [e for e in events if str((e or {}).get("date", "") or "").strip() == today]
    if past_rows and _archive_events(past_rows, "past_day"):
        events = today_rows
    else:
        events = today_rows if not past_rows else events

    # 2) 당일 이벤트를 현재값으로 재조정한다.
    #    관측된 지수에 대해서만 판단한다 - 못 본 지수의 이벤트는 건드리지 않는다.
    active: Dict[Tuple[str, str], Dict[str, Any]] = {}
    keep: List[Dict[str, Any]] = []
    resolved: List[Dict[str, Any]] = []
    for row in today_rows:
        code = str((row or {}).get("code", "") or "").strip()
        event_type = str((row or {}).get("event_type", "") or "").strip()
        state = states.get(code)
        if state is None:
            keep.append(row)  # 관측 못한 지수 -> 판단 보류
            continue
        if state["event_type"] and state["event_type"] == event_type:
            active[(code, event_type)] = row
            keep.append(row)
        else:
            resolved.append({**row, "resolved_change_pct": state["change_pct"], "resolved_at": _now_iso()})

    if resolved and not _archive_events(resolved, "condition_cleared"):
        keep.extend([{k: v for k, v in r.items() if k not in ("resolved_change_pct", "resolved_at")} for r in resolved])
        resolved = []

    # 3) 지금 조건이 걸린 지수는 이벤트를 세운다(있으면 값만 갱신).
    added: List[str] = []
    for code, state in states.items():
        if not state["event_type"]:
            continue
        key = (code, state["event_type"])
        row = active.get(key)
        if row is None:
            row = {
                "date": today,
                "event_type": state["event_type"],
                "scope": "MARKET",
                "code": code,
                "change_pct": state["change_pct"],
                "first_seen_at": _now_iso(),
                "last_seen_at": _now_iso(),
            }
            keep.append(row)
            added.append(f"{code}:{state['event_type']}:{state['change_pct']}")
        else:
            row["change_pct"] = state["change_pct"]
            row["last_seen_at"] = _now_iso()

    # 4) 레벨은 관측된 지수 전체의 합이다. 급락이 급등을 이긴다.
    if any(s["event_type"] == EVENT_CRASH for s in states.values()):
        level = "HIGH"
    elif any(s["event_type"] == EVENT_BOOST for s in states.values()):
        level = "BOOST"
    else:
        level = "NORMAL"

    gate_doc["generated_at"] = _now_iso()
    gate_doc["as_of_ymd"] = today
    gate_doc["market_event_level"] = level
    gate_doc["source"] = "market_anomaly_detector_index_tr"
    gate_doc["events"] = keep
    gate_doc["observed_indices"] = {
        c: {"change_pct": s["change_pct"], "level": s["level"], "age_sec": s["age_sec"]}
        for c, s in states.items()
    }
    GATE_FILE.write_text(json.dumps(gate_doc, ensure_ascii=False, indent=2), encoding="utf-8")

    market_changes = _update_market_status(states, frames)

    if added or resolved or market_changes:
        logger.info(
            f"level={level} added={added or '-'} resolved={len(resolved)} "
            f"market_status={market_changes or '-'} observed={sorted(states)}"
        )

    status_payload.update(
        {
            "valid": True,
            "action": "evaluated",
            "market_event_level": level,
            "events_active": len(keep),
            "events_added": added,
            "events_resolved": len(resolved),
            "past_day_archived": len(past_rows),
            "market_status_changes": market_changes,
        }
    )
    # 성공도 남긴다. 실패만 남기면 분모를 알 수 없어 빈도를 못 낸다
    _append_obs_ledger(status_payload)
    _write_status(status_payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
