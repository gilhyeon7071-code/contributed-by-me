"""intraday_paper_loop.py
Run the intraday paper-trading pipeline on a fixed interval during market hours.

Cycle flow:
  1. intraday_price_snapshot  -> intraday_prices_latest.csv
  2. paper_engine             -> inject PAPER_INTRADAY_PRICE_PATH
  3. kis_order_dispatch       -> dispatch simulated KIS orders
  4. kis_sync_fills           -> sync simulated fills; 5. vibe_make_live_fills_stub -> refresh live_fills.csv

Usage:
    python intraday_paper_loop.py [--mock auto|true|false] [--interval 5] [--dry-run]
"""
from __future__ import annotations

import argparse
import atexit
import csv
import ctypes
import datetime as dt
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent
TOOLS_DIR = ROOT / "tools"
LOG_DIR = ROOT / "2_Logs"
ROOTB_DIR = ROOT.parent / "vibe" / "buffett"

FILLS_CSV = ROOT / "paper" / "fills.csv"
INTRADAY_PRICES_CSV = LOG_DIR / "intraday_prices_latest.csv"
LOOP_STATUS_PATH = LOG_DIR / "intraday_loop_status.json"
LOOP_STATUS_LATEST_PATH = LOG_DIR / "intraday_loop_status_latest.json"
LOOP_LOG_PATH = LOG_DIR / "intraday_loop_last.txt"
LOOP_LOCK_PATH = LOG_DIR / "intraday_paper_loop.lock"
SURGE_REALTIME_LATEST_PATH = LOG_DIR / "surge_realtime_latest.json"

KRX_OPEN_HHMM = 900
KRX_CLOSE_HHMM = 1530

# [2026-09-12] KRX 애프터마켓. 2026-09-14 개설, 16:00~20:00 접속매매.
#   우리는 이 구간에 **발주하지 않는다** - 그래서 진입·청산 경계는 여전히 15:30 이다.
#   다만 '장이 열려 있는가' 를 묻는 자리는 이 구간을 장중으로 봐야 한다.
#   미결 대장 C21, PLANS (374)(377).
KRX_AFTERMARKET_START_YMD = 20260914
KRX_AFTERMARKET_OPEN_HHMM = 1600
KRX_AFTERMARKET_CLOSE_HHMM = 2000
KST = dt.timezone(dt.timedelta(hours=9))

# [2026-09-10] 이 프로세스가 언제 떴나. 런처 수정 시각과 대조하면
#   "옛 설정을 들고 도는 프로세스" 를 바로 가려낼 수 있다.
_PROC_STARTED_AT = dt.datetime.now(tz=KST).isoformat(timespec="seconds")

logger = logging.getLogger("intraday_paper_loop")
_JOB_HANDLE: Optional[int] = None
_LOCK_FD: Optional[int] = None


def _enable_job_kill_on_close() -> bool:
    """Attach current process to a Job Object with KILL_ON_JOB_CLOSE (Windows only)."""
    global _JOB_HANDLE
    if os.name != "nt":
        return False
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
        JobObjectExtendedLimitInformation = 9

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", ctypes.c_uint32),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", ctypes.c_uint32),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", ctypes.c_uint32),
                ("SchedulingClass", ctypes.c_uint32),
            ]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
        kernel32.CreateJobObjectW.restype = ctypes.c_void_p
        kernel32.SetInformationJobObject.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
        kernel32.SetInformationJobObject.restype = ctypes.c_int
        kernel32.AssignProcessToJobObject.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        kernel32.AssignProcessToJobObject.restype = ctypes.c_int
        kernel32.GetCurrentProcess.argtypes = []
        kernel32.GetCurrentProcess.restype = ctypes.c_void_p

        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            raise OSError(f"CreateJobObjectW failed: {ctypes.get_last_error()}")

        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        ok = kernel32.SetInformationJobObject(
            handle,
            JobObjectExtendedLimitInformation,
            ctypes.byref(info),
            ctypes.sizeof(info),
        )
        if not ok:
            raise OSError(f"SetInformationJobObject failed: {ctypes.get_last_error()}")

        ok = kernel32.AssignProcessToJobObject(handle, kernel32.GetCurrentProcess())
        if not ok:
            raise OSError(f"AssignProcessToJobObject failed: {ctypes.get_last_error()}")

        _JOB_HANDLE = int(handle)
        logger.info("[LOOP] job_object kill-on-close enabled")
    except Exception:
        logger.warning("[LOOP] job_object enable failed: %s", e)
        return False
    return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_surge_detector_status() -> Dict[str, Any]:
    try:
        obj = json.loads(SURGE_REALTIME_LATEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _is_surge_detector_soft_failure(label: str, returncode: int) -> tuple[bool, str, str]:
    if label != "surge_detector" or returncode == 0:
        return False, "", ""
    obj = _read_surge_detector_status()
    status = str(obj.get("status", "") or "")
    reason = str(obj.get("reason", "") or "")
    if status == "STALE_INTRADAY_DATE":
        return True, status, reason
    return False, status, reason

def _now_ts() -> str:
    return dt.datetime.now(tz=KST).isoformat(timespec="seconds")


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return int(exit_code.value) == STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    except Exception:
        return False


def _pid_matches_intraday_loop(pid: int) -> bool:
    if not _pid_alive(pid):
        return False
    if os.name != "nt":
        return True
    try:
        cmd = [
            "powershell",
            "-NoProfile",
            "-Command",
            f"(Get-CimInstance Win32_Process -Filter 'ProcessId={int(pid)}').CommandLine",
        ]
        cp = subprocess.run(cmd, capture_output=True, text=True, timeout=3.0)
        command_line = (cp.stdout or "").lower()
    except Exception:
        command_line = ""
    return "intraday_paper_loop.py" in command_line


def _read_loop_lock(path: Path = LOOP_LOCK_PATH) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_json_obj(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _norm_ymd_text(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _latest_paper_pnl_as_of() -> str:
    obj = _read_json_obj(LOG_DIR / "paper_pnl_summary_last.json")
    return _norm_ymd_text(obj.get("as_of") or obj.get("as_of_ymd"))


def _build_live_vs_bt_align_cmd(py: str) -> tuple[List[str] | None, str]:
    as_of = _latest_paper_pnl_as_of()
    if not as_of:
        return None, ""
    min_stable_score = str(os.environ.get("LVB_MIN_STABLE_SCORE", "-20")).strip() or "-20"
    return (
        [
            py,
            str(ROOT / "live_vs_bt_paper_daily.py"),
            "--date",
            as_of,
            "--align-window-trades",
            "30",
            "--min-shared-trades",
            "10",
            "--max-backtest-age-days",
            "7",
            "--min-oos-trades",
            "20",
            "--min-oos-pf",
            "0.75",
            "--min-stable-score",
            min_stable_score,
        ],
        as_of,
    )


def _release_singleton_lock() -> None:
    global _LOCK_FD
    fd = _LOCK_FD
    _LOCK_FD = None
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass
    try:
        payload = _read_loop_lock()
        if int(payload.get("pid", 0) or 0) == os.getpid():
            LOOP_LOCK_PATH.unlink(missing_ok=True)
    except OSError:
        pass


def _acquire_singleton_lock() -> tuple[bool, str]:
    global _LOCK_FD
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if LOOP_LOCK_PATH.exists():
        payload = _read_loop_lock()
        pid = int(payload.get("pid", 0) or 0)
        if pid and _pid_matches_intraday_loop(pid):
            return False, f"already_running:pid={pid}"
        try:
            LOOP_LOCK_PATH.unlink()
        except OSError as exc:
            return False, f"stale_lock_unlink_failed:{type(exc).__name__}:{exc}"
    payload = {
        "pid": os.getpid(),
        "created_at": _now_ts(),
        "script": str(Path(__file__).resolve()),
        "argv": sys.argv,
    }
    try:
        fd = os.open(str(LOOP_LOCK_PATH), os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        os.write(fd, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
        _LOCK_FD = fd
        atexit.register(_release_singleton_lock)
        return True, "acquired"
    except FileExistsError:
        payload = _read_loop_lock()
        pid = int(payload.get("pid", 0) or 0)
        return False, f"already_running:pid={pid or 'unknown'}"
    except OSError as exc:
        return False, f"lock_create_failed:{type(exc).__name__}:{exc}"


def _now_hhmm() -> int:
    n = dt.datetime.now(tz=KST)
    return n.hour * 100 + n.minute


def _offhours_news_session(now: dt.datetime) -> str:
    """뉴스 수집의 시간대 라벨. 라벨이 예산(심볼수·TTL·타임아웃)을 정한다.

    [2026-09-12] C21. 2026-09-14 부터 16:00~20:00 은 **장중**(애프터마켓)인데
    이 함수는 15:30~20:00 을 통째로 evening(장외)으로 불렀다. 장이 열려 있는 동안
    장외 예산으로 수집하면 그 시간의 뉴스가 다른 밀도로 들어온다.
    개설일 전에는 판정이 바뀌지 않도록 날짜로 막았다.
    """
    hhmm = now.hour * 100 + now.minute
    ymd = int(now.strftime("%Y%m%d"))
    if (ymd >= KRX_AFTERMARKET_START_YMD
            and KRX_AFTERMARKET_OPEN_HHMM <= hhmm < KRX_AFTERMARKET_CLOSE_HHMM
            and not _krx_closed_reason(now)):
        return "intraday"
    if 1530 <= hhmm < 2000:
        return "evening"
    if hhmm >= 2000 or hhmm < 400:
        return "night"
    if 400 <= hhmm < 900:
        return "dawn"
    return "intraday"


# [2026-08-21] 휴장일 판정.
#
# 아래 `_in_krx_session()` 은 `weekday() >= 5` 로 주말만 걸렀다. **평일 공휴일은 장중으로 봤다.**
# 그러면 추석·한글날 같은 날 09:00~15:30 동안 전체 사이클이 돌면서 진입 판정까지 간다.
# 그날 데이터는 시장이 없어 갱신되지 않으므로, 신선도 검사에 걸리든 안 걸리든
# **비거래일에 만들어진 판정 산출물**이 남는다. 그 자체가 나중에 잘못 읽힌다.
#
# `holidays.json` 은 이미 있다(exchange_calendars.XKRX 기반, 42일, 2025-01-01~2027-05-26).
# paper_engine/common.py 는 이 파일을 쓰는데 루프만 안 쓰고 있었다.
#
# 파일을 못 읽으면 **예전 동작(주말만 판정)으로 되돌아간다.** 달력을 못 읽는다고
# 매매를 통째로 멈추는 쪽이 더 위험하기 때문이다. 대신 로그로 드러낸다.
HOLIDAYS_PATH = ROOT / "holidays.json"
_HOLIDAYS_CACHE: Dict[str, Any] = {"mtime": None, "ymds": set()}


def _load_krx_holidays() -> set:
    try:
        if not HOLIDAYS_PATH.exists():
            return set()
        mtime = HOLIDAYS_PATH.stat().st_mtime
        if _HOLIDAYS_CACHE.get("mtime") == mtime:
            return _HOLIDAYS_CACHE.get("ymds") or set()
        doc = json.loads(HOLIDAYS_PATH.read_text(encoding="utf-8-sig"))
        raw = None
        if isinstance(doc, dict):
            for key in ("holidays", "krx_holidays", "dates", "holiday_dates"):
                if isinstance(doc.get(key), list):
                    raw = doc[key]
                    break
        elif isinstance(doc, list):
            raw = doc
        ymds = {
            "".join(ch for ch in str(x) if ch.isdigit())[:8]
            for x in (raw or [])
        }
        ymds = {y for y in ymds if len(y) == 8}
        _HOLIDAYS_CACHE["mtime"] = mtime
        _HOLIDAYS_CACHE["ymds"] = ymds
        return ymds
    except Exception as exc:
        logger.warning("[CALENDAR] holidays.json read failed (%s); weekend-only fallback", type(exc).__name__)
        return set()


def _krx_closed_reason(now: dt.datetime) -> str:
    """거래일이 아니면 사유를, 거래일이면 빈 문자열을 준다."""
    if now.weekday() >= 5:
        return "weekend"
    if now.strftime("%Y%m%d") in _load_krx_holidays():
        return "holiday"
    return ""


def _in_krx_session(allow_offhours: bool = False) -> bool:
    if allow_offhours:
        return True
    now = dt.datetime.now(tz=KST)
    if _krx_closed_reason(now):
        return False
    hhmm = _now_hhmm()
    return KRX_OPEN_HHMM <= hhmm < KRX_CLOSE_HHMM


# [2026-08-21] 하드 블록 임계값.
#
# 이 아래 _run() 은 원래 비-offhours 스텝이 rc!=0 이면 **1회로 즉시** 하드 블록 플래그를 썼다.
# 메시지는 "threshold exceeded" 였지만 카운터는 존재하지 않았다.
# 그리고 _try_auto_release_hard_block 은 `offhours_freshness_check` + 전일 날짜만 해제하므로,
# 장중 스텝이 한 번 죽으면 사람이 플래그를 치울 때까지 루프 전체가 정지한다.
# 2026-08-21 에 take_profit 예외 1건으로 09:30~10:04 (34분) 가 그렇게 멈췄다.
# 보유 포지션이 있는 상태에서 같은 일이 나면 손절·익절도 함께 멈춘다.
#
# 그래서 라벨별 **연속** 실패를 세고 임계값에 도달했을 때만 블록한다.
#   - 일회성 예외/일시 오류: 다음 사이클에 회복되면 카운터가 0으로 돌아간다
#   - 진짜 크래시 루프: 연속 실패가 쌓여 임계값에서 여전히 멈춘다 (fail-closed 유지)
# 카운터는 프로세스 메모리에 있다. 루프 재시작 시 0에서 시작한다.
_STEP_FAIL_STREAK: Dict[str, int] = {}


def _hard_block_threshold() -> int:
    raw = str(os.environ.get("PAPER_LOOP_HARD_BLOCK_THRESHOLD", "2")).strip()
    try:
        return max(1, int(raw or "2"))
    except ValueError:
        return 2


# [2026-08-22] 하드 블록 자동 해제.
#
# 2026-08-21 에 붙인 임계값은 "언제 막을까"만 정했고 "언제 풀까"는 그대로 두었다.
# 기존 _try_auto_release_hard_block 은 `offhours_freshness_check` + 전일 날짜에만
# 해당해서, 장중에 걸린 블록은 어떤 경우에도 자동으로 풀리지 않았다.
# 실측: 2_Logs/_archive 의 플래그 9건이 전부 사람이 옮긴 것이다(자동 해제 0건).
#
# 블록의 비용은 대칭이 아니다. 막는 동안 진입만 멈추는 게 아니라 **청산도 멈춘다**.
# 보유 포지션이 있으면 손절과 익절이 함께 정지한다. 실제로 2026-08-21 에
# take_profit 예외 1건으로 09:30~10:04 (34분) 가 그렇게 멈췄다.
# 그래서 블록을 "영구 정지"가 아니라 "냉각"으로 다룬다.
# 다만 같은 실패가 반복되면 결국 사람을 부른다 - fail-closed 는 유지한다.
#
# 해제 조건 (셋 다 충족해야 한다)
#   1) 자동 해제가 켜져 있다        PAPER_LOOP_HARD_BLOCK_AUTO_RELEASE      (기본 1)
#   2) 냉각 시간이 지났다            PAPER_LOOP_HARD_BLOCK_COOLDOWN_SEC      (기본 900초)
#   3) 오늘 같은 라벨의 자동 해제 횟수가 상한 미만
#                                   PAPER_LOOP_HARD_BLOCK_MAX_AUTO_RELEASE  (기본 2)
#
# 상한에 도달하면 더는 풀지 않는다. 최악의 경우에도 사람이 개입하기 전까지
# (상한+1) x 임계값 회의 연속 실패로 끝난다. 기본값이면 3 x 2 = 6 회다.
# 상한을 0 으로 두면 예전 동작(장중 블록은 자동 해제 없음)이 그대로 재현된다.
#
# 모든 블록과 해제는 paper_intraday_hard_block_history.jsonl 에 남는다.
# 해제 횟수도 그 파일에서 센다 - 별도 상태 파일을 만들지 않는다.
HARD_BLOCK_HISTORY_PATH = LOG_DIR / "paper_intraday_hard_block_history.jsonl"


def _hard_block_auto_release_enabled() -> bool:
    raw = str(os.environ.get("PAPER_LOOP_HARD_BLOCK_AUTO_RELEASE", "1")).strip().lower()
    return raw in ("1", "true", "yes", "on")


def _hard_block_cooldown_sec() -> float:
    raw = str(os.environ.get("PAPER_LOOP_HARD_BLOCK_COOLDOWN_SEC", "900")).strip()
    try:
        return max(0.0, float(raw or "900"))
    except ValueError:
        return 900.0


def _hard_block_max_auto_release() -> int:
    raw = str(os.environ.get("PAPER_LOOP_HARD_BLOCK_MAX_AUTO_RELEASE", "2")).strip()
    try:
        return max(0, int(raw or "2"))
    except ValueError:
        return 2


def _append_hard_block_history(event: Dict[str, Any]) -> None:
    """이력 한 줄 추가. 기록 실패가 매매를 막아서는 안 되므로 best-effort 다."""
    try:
        HARD_BLOCK_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
        with HARD_BLOCK_HISTORY_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning("[HARD-BLOCK] history append failed: %s", exc)


def _count_auto_releases(label: str, ymd: str) -> int:
    """오늘(ymd) 같은 label 로 자동 해제한 횟수.

    이력을 못 읽으면 상한값을 돌려준다 - 셀 수 없을 때는 풀지 않는 쪽이 안전하다.
    """
    if not HARD_BLOCK_HISTORY_PATH.exists():
        return 0
    count = 0
    try:
        with HARD_BLOCK_HISTORY_PATH.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if not isinstance(rec, dict):
                    continue
                if (
                    rec.get("event") == "auto_release"
                    and str(rec.get("label") or "") == str(label)
                    and str(rec.get("ymd") or "") == str(ymd)
                ):
                    count += 1
    except Exception as exc:
        logger.warning("[HARD-BLOCK] history read failed (해제 보류): %s", exc)
        return _hard_block_max_auto_release()
    return count


def _parse_flag_created_at(value: Any) -> Optional[dt.datetime]:
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        parsed = dt.datetime.fromisoformat(txt)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=KST)
    return parsed


def _release_hard_block_flag(
    flag_path: Path,
    reason: str,
    detail: Dict[str, Any],
    dry_run: bool,
) -> bool:
    """플래그를 _archive 로 옮기고 이력을 남긴다. dry_run 이면 아무것도 바꾸지 않는다."""
    archive_dir = LOG_DIR / "_archive"
    ts = dt.datetime.now(tz=KST).strftime("%Y%m%d_%H%M%S")
    archive_path = archive_dir / f"paper_intraday_hard_blocked_{ts}_{reason}.flag"
    if dry_run:
        logger.info(
            "[AUTO-RELEASE] dry-run PASS reason=%s flag=%s archive=%s",
            reason,
            flag_path,
            archive_path,
        )
        return True
    archive_dir.mkdir(parents=True, exist_ok=True)
    flag_path.replace(archive_path)

    label = str(detail.get("label") or "")
    if label:
        # 이걸 안 하면 해제가 무의미하다. 같은 프로세스라면 연속 실패 카운터가
        # 아직 임계값에 있어서, 다음 한 번의 실패로 즉시 다시 막힌다.
        _STEP_FAIL_STREAK.pop(label, None)

    event = {
        "event": "auto_release",
        "ts": _now_ts(),
        "ymd": dt.datetime.now(tz=KST).strftime("%Y%m%d"),
        "reason": reason,
        "archive": str(archive_path),
    }
    event.update(detail)
    _append_hard_block_history(event)
    logger.warning(
        "[AUTO-RELEASE] released hard block reason=%s label=%s detail=%s archive=%s",
        reason,
        label or "-",
        json.dumps(detail, ensure_ascii=False),
        archive_path,
    )
    return True


def _run(
    cmd: List[str],
    label: str,
    env: Optional[Dict[str, str]] = None,
    timeout: float = 120.0,
) -> Dict[str, Any]:
    merged_env = {**os.environ, **(env or {})}
    started = time.time()
    try:
        cp = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            env=merged_env,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        timeout_sec = max(1.0, timeout)
        try:
            stdout, stderr = cp.communicate(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            # Windows에서 하위 프로세스까지 포함해 정리
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(cp.pid)],
                text=True,
                capture_output=True,
                check=False,
            )
            try:
                stdout, stderr = cp.communicate(timeout=5)
            except Exception:
                stdout, stderr = "", "TIMEOUT"
            elapsed = round(time.time() - started, 2)
            logger.warning("[%s] TIMEOUT (%.1fs) pid=%s", label, elapsed, cp.pid)
            return {
                "label": label,
                "ok": False,
                "returncode": 124,
                "elapsed": elapsed,
                "stdout_tail": (stdout or "").strip().splitlines()[-10:],
                "stderr_tail": (stderr or "").strip().splitlines()[-5:] or ["TIMEOUT"],
            }
        elapsed = round(time.time() - started, 2)
        ok = cp.returncode == 0
        tail = (stdout or "").strip().splitlines()[-10:]
        err_tail = (stderr or "").strip().splitlines()[-5:]
        if ok:
            _STEP_FAIL_STREAK.pop(label, None)
            logger.info("[%s] OK (%.1fs)", label, elapsed)
        else:
            soft_failure, soft_status, soft_reason = _is_surge_detector_soft_failure(label, cp.returncode)
            if soft_failure:
                logger.warning(
                    "[%s] SOFT_FAILED rc=%d (%.1fs) status=%s reason=%s",
                    label,
                    cp.returncode,
                    elapsed,
                    soft_status,
                    soft_reason,
                )
                return {
                    "label": label,
                    "ok": True,
                    "returncode": cp.returncode,
                    "elapsed": elapsed,
                    "stdout_tail": tail,
                    "stderr_tail": err_tail,
                    "advisory_only": True,
                    "fallback_reason": "surge_detector_stale_intraday_date_non_blocking",
                    "surge_detector_status": soft_status,
                    "surge_detector_reason": soft_reason,
                }
            err_msg = "\n  ".join(err_tail)
            err_code = "ERR_UNKNOWN"
            if "KIS env/config failed" in err_msg or "KIS_APP_KEY" in err_msg:
                err_code = "ERR_KIS_AUTH_REQ"
            elif "timeout" in err_msg.lower() or "connection reset" in err_msg.lower():
                err_code = "ERR_KIS_ORDER_TIMEOUT"
            elif "PermissionError" in err_msg or "Permission denied" in err_msg:
                err_code = "ERR_OUTPUT_PERMISSION"
            logger.warning("[%s] FAILED rc=%d (%.1fs) err_code=%s\n  stderr: %s",
                           label, cp.returncode, elapsed, err_code, err_msg)
            # [2026-08-24] exit_cycle_* 는 **이미 블록 상태에서** 청산 관리를 위해 도는 단계다.
            # 이것이 실패했다고 다시 하드 블록을 쓰면 블록이 덧씌워지고 이력만 오염된다.
            # 실패해도 non-blocking 으로 둔다 - 어차피 진입은 PAPER_EXIT_ONLY 로 막혀 있다.
            if str(label).startswith("offhours_") or str(label).startswith("exit_cycle_"):
                return {
                    "label": label,
                    "ok": False,
                    "returncode": cp.returncode,
                    "elapsed": elapsed,
                    "stdout_tail": tail,
                    "stderr_tail": err_tail,
                    "advisory_only": True,
                    "fallback_reason": (
                        "exit_cycle_step_failed_non_blocking"
                        if str(label).startswith("exit_cycle_")
                        else "offhours_step_failed_non_blocking"
                    ),
                    "err_code": err_code,
                }
            fail_streak = int(_STEP_FAIL_STREAK.get(label, 0)) + 1
            _STEP_FAIL_STREAK[label] = fail_streak
            threshold = _hard_block_threshold()
            if fail_streak < threshold:
                logger.warning(
                    "[%s] failure %d/%d - not hard blocking yet (err_code=%s)",
                    label,
                    fail_streak,
                    threshold,
                    err_code,
                )
                return {
                    "label": label,
                    "ok": False,
                    "returncode": cp.returncode,
                    "elapsed": elapsed,
                    "stdout_tail": tail,
                    "stderr_tail": err_tail,
                    "err_code": err_code,
                    "fail_streak": fail_streak,
                    "hard_block_threshold": threshold,
                    "hard_block_armed": False,
                }
            hard_block_payload = {
                "created_at": _now_ts(),
                "err_code": err_code,
                "label": label,
                "returncode": cp.returncode,
                "elapsed": elapsed,
                "stderr_tail": err_tail,
                "stdout_tail": tail,
                "fail_streak": fail_streak,
                "hard_block_threshold": threshold,
                "message": f"{err_code} in {label} failed {fail_streak}/{threshold} consecutive cycles",
            }
            (LOG_DIR / "paper_intraday_hard_blocked.flag").write_text(
                json.dumps(hard_block_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            _append_hard_block_history(
                {
                    "event": "block",
                    "ts": hard_block_payload["created_at"],
                    "ymd": dt.datetime.now(tz=KST).strftime("%Y%m%d"),
                    "label": label,
                    "err_code": err_code,
                    "returncode": cp.returncode,
                    "fail_streak": fail_streak,
                    "hard_block_threshold": threshold,
                }
            )
            logger.error(
                "[%s] Hard blocking intraday loop. (PID=%s, Error=%s, streak=%d/%d)",
                label,
                os.getpid(),
                err_code,
                fail_streak,
                threshold,
            )
        return {
            "label": label,
            "ok": ok,
            "returncode": cp.returncode,
            "elapsed": elapsed,
            "stdout_tail": tail,
            "stderr_tail": err_tail,
        }
    except Exception as e:
        elapsed = round(time.time() - started, 2)
        logger.error("[%s] ERROR: %s", label, e)
        return {"label": label, "ok": False, "returncode": 1, "elapsed": elapsed,
                "stdout_tail": [], "stderr_tail": [str(e)]}


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            tmp.write_text(text, encoding="utf-8")
            os.replace(str(tmp), str(path))
            return
        except PermissionError as e:
            last_error = e
            time.sleep(0.2 * (attempt + 1))
    if tmp.exists():
        try:
            tmp.unlink()
        except OSError:
            pass
    if last_error is not None:
        raise last_error


def _sync_rootb_orders_exec_current(trading_ymd: str) -> Dict[str, Any]:
    src = ROOT / "paper" / f"orders_{trading_ymd}_exec.xlsx"
    dst = ROOTB_DIR / "data" / "orders" / f"orders_{trading_ymd}_exec.xlsx"
    result: Dict[str, Any] = {
        "label": "rootb_orders_exec_current_sync",
        "ok": False,
        "src": str(src),
        "dst": str(dst),
    }
    if not src.exists():
        result["reason"] = "source_orders_exec_missing"
        return result
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_name(f".{dst.name}.tmp.{os.getpid()}")
        shutil.copy2(src, tmp)
        os.replace(str(tmp), str(dst))
        result.update({"ok": True, "bytes": int(dst.stat().st_size)})
    except Exception as exc:
        result["reason"] = f"{type(exc).__name__}: {exc}"
        try:
            if "tmp" in locals() and tmp.exists():
                tmp.unlink()
        except Exception:
            pass
    return result


def _write_lock_heartbeat(payload: Dict[str, Any]) -> None:
    raw_path = str(os.environ.get("INTRADAY_LOCK_HEARTBEAT") or "").strip()
    if not raw_path:
        return
    try:
        heartbeat_path = Path(raw_path)
        heartbeat = {
            "ts": payload.get("ts"),
            "pid": os.getpid(),
            "cycle": payload.get("cycle"),
            "steps_ok": payload.get("steps_ok"),
            "steps_total": payload.get("steps_total"),
            "next_run_at": payload.get("next_run_at"),
            "status_latest": str(LOOP_STATUS_LATEST_PATH),
        }
        _write_text_atomic(heartbeat_path, json.dumps(heartbeat, ensure_ascii=False, indent=2))
    except Exception as e:
        logger.warning("lock heartbeat write failed: %s", e)


def _switch_state() -> Dict[str, Any]:
    """루프가 **지금 들고 있는** 매매 스위치. 원문과 해석을 나란히 남긴다.

    [2026-09-10 신설] 런처 파일을 고쳐도 **돌고 있는 프로세스**가 뭘 들고 있는지
    알 방법이 없었다. 재기동 여부를 산출물로 확인할 수 있어야 한다.
    해석 규칙은 강제 지점(paper_engine.py:440)과 같아야 한다.
    """
    on = {"1", "true", "yes", "on"}
    raw = {k: os.environ.get(k) for k in
           ("PAPER_EXIT_ONLY", "PAPER_NO_ENTRY", "KIS_MOCK",
            "BROKER_MODE", "LOOP_DISPATCH_APPLY")}
    exit_only = str(raw.get("PAPER_EXIT_ONLY") or raw.get("PAPER_NO_ENTRY") or "")
    return {
        "raw": raw,                                   # 원문 그대로 (None = 미설정)
        "exit_only_mode": exit_only.strip().lower() in on,
        "pid": os.getpid(),
        "started_at": _PROC_STARTED_AT,
    }


def _write_status(cycle: int, results: List[Dict[str, Any]], next_run_at: str) -> None:
    ok_count = sum(1 for r in results if r.get("ok"))
    payload = {
        "ts": _now_ts(),
        "cycle": cycle,
        "steps_ok": ok_count,
        "steps_total": len(results),
        "next_run_at": next_run_at,
        # [2026-09-10] 이 프로세스가 들고 있는 스위치. 재기동 확인용.
        "switches": _switch_state(),
        "steps": results,
    }
    try:
        payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
        _write_text_atomic(LOOP_STATUS_PATH, payload_json)
        _write_text_atomic(LOOP_STATUS_LATEST_PATH, payload_json)
        _write_lock_heartbeat(payload)
    except Exception as e:
        logger.warning("status write failed: %s", e)


def _append_step_status(cycle: int, results: List[Dict[str, Any]], step: Dict[str, Any]) -> None:
    """Append one step result and flush latest status immediately for watchdog visibility."""
    results.append(step)
    _write_status(cycle, results, "-")


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = str(os.environ.get(name, str(default))).strip()
    try:
        value = int(raw or default)
    except ValueError:
        value = int(default)
    return max(int(minimum), int(value))


def _env_bool(name: str, default: bool = False) -> bool:
    raw_default = "1" if default else "0"
    raw = str(os.environ.get(name, raw_default)).strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _run_on_cycle(cycle: int, every_n: int) -> bool:
    every_n = max(1, int(every_n))
    return (int(cycle) % every_n) == 0


DAILY_BATCH_LOCK_PATH = LOG_DIR / "run_paper_daily.lock"


def _lock_owner_pid(path: Path) -> int:
    """락 디렉터리의 run.info 에서 pid 를 읽는다. 못 읽으면 0."""
    try:
        info = path / "run.info"
        if not info.is_file():
            return 0
        for line in info.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if line.startswith("pid="):
                return int("".join(c for c in line[4:] if c.isdigit()) or 0)
    except Exception:
        return 0
    return 0


def _pid_alive(pid: int) -> bool:
    """pid 가 살아 있나. **판정 불가면 살아 있는 것으로 본다**(fail-closed).

    틀리는 방향이 중요하다 - 죽은 것을 살았다고 보면 사이클 몇 개를 건너뛰고 끝나지만,
    살아 있는 배치를 죽었다고 보면 루프가 배치와 같이 돌아 충돌한다.
    """
    if pid <= 0:
        return True
    try:
        r = subprocess.run(["tasklist", "/FI", "PID eq %d" % pid, "/NH"],
                           capture_output=True, text=True, timeout=10,
                           encoding="utf-8", errors="replace")
        out = (r.stdout or "")
        return str(pid) in out
    except Exception:
        return True


def _daily_batch_lock_state(path: Path = DAILY_BATCH_LOCK_PATH) -> Dict[str, Any]:
    """Report whether run_paper_daily.bat currently holds its lock.

    The batch creates 2_Logs/run_paper_daily.lock for its whole run
    (run_paper_daily.bat:44,59).  Reading it is advisory and never raises: a
    failure here must not stop the intraday loop.

    A stale lock (crashed batch) would otherwise freeze the loop forever, so
    anything older than DAILY_BATCH_LOCK_MAX_AGE_SEC is treated as not active.
    The 2026-08-20 batch ran 53 minutes, so the default leaves ample headroom.
    """
    out: Dict[str, Any] = {"active": False, "age_sec": 0.0, "reason": ""}
    try:
        if not path.exists():
            return out
        age = max(0.0, time.time() - path.stat().st_mtime)
        out["age_sec"] = age
        max_age = float(_env_int("DAILY_BATCH_LOCK_MAX_AGE_SEC", 7200))
        if age > max_age:
            out["reason"] = "stale_lock_ignored"
            return out
        # [2026-09-13] **죽은 pid 를 2시간이나 존중하고 있었다.**
        #   실측: 배치가 메모리 부족으로 죽어 락만 남았는데(pid 13708 종료, 나이 3분)
        #   이 함수는 active=True 를 냈다. 나이만 보고 주인을 안 봤기 때문이다.
        #   배치 자신은 pid 기반으로 판정한다(run_paper_daily.bat:2286) - 루프만 달랐다.
        #   **주인이 죽었으면 락은 없는 것이다.** 아침 창을 통째로 먹는 경로다.
        owner_pid = _lock_owner_pid(path)
        if owner_pid and not _pid_alive(owner_pid):
            out["reason"] = "stale_lock_dead_pid:%d" % owner_pid
            out["owner_pid"] = owner_pid
            return out
        out["active"] = True
        out["owner_pid"] = owner_pid
        out["reason"] = "run_paper_daily_lock_active"
        return out
    except Exception as exc:
        out["reason"] = "lock_check_failed:" + type(exc).__name__
        return out


def _cadence_skip(label: str, every_n: int) -> Dict[str, Any]:
    return {
        "label": label,
        "ok": True,
        "skipped": True,
        "skip_reason": f"cadence_every_{int(every_n)}_cycles",
    }


def _date8(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())[:8]


def _derive_d_from_fills(path: Path = FILLS_CSV) -> str:
    if not path.exists():
        return dt.datetime.now(tz=KST).strftime("%Y%m%d")
    buy_dates: List[str] = []
    all_dates: List[str] = []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                ymd = _date8(row.get("datetime") or row.get("date") or row.get("exec_date"))
                if len(ymd) != 8:
                    continue
                all_dates.append(ymd)
                if str(row.get("side") or "").strip().upper() == "BUY":
                    buy_dates.append(ymd)
    except Exception as exc:
        logger.warning("D derive from fills failed: %s", exc)
        return dt.datetime.now(tz=KST).strftime("%Y%m%d")
    return max(buy_dates or all_dates or [dt.datetime.now(tz=KST).strftime("%Y%m%d")])


def _check_freshness_pass(pointer_path: Path = LOG_DIR / "freshness_source_last.json") -> bool:
    try:
        if not pointer_path.exists():
            logger.warning("[AUTO-RELEASE] freshness pointer missing path=%s", pointer_path)
            return False
        ptr = json.loads(pointer_path.read_text(encoding="utf-8-sig"))
        last_path_raw = str(ptr.get("last") or "").strip() if isinstance(ptr, dict) else ""
        if not last_path_raw:
            logger.warning("[AUTO-RELEASE] freshness pointer has no last path=%s", pointer_path)
            return False
        last_path = Path(last_path_raw)
        if not last_path.is_absolute():
            last_path = ROOT / last_path
        if not last_path.exists():
            logger.warning("[AUTO-RELEASE] freshness result missing path=%s", last_path)
            return False
        obj = json.loads(last_path.read_text(encoding="utf-8-sig"))
        verdict = str((obj.get("verdict") or obj.get("status") or "") if isinstance(obj, dict) else "").strip().upper()
        ok = verdict == "PASS"
        logger.info("[AUTO-RELEASE] freshness verdict=%s path=%s", verdict or "-", last_path)
        return ok
    except Exception as exc:
        logger.warning("[AUTO-RELEASE] freshness check failed: %s", exc)
        return False


def _freshness_pointer_step(max_age_sec: float) -> Dict[str, Any]:
    pointer_path = LOG_DIR / "freshness_source_last.json"
    try:
        if not pointer_path.exists():
            return {"label": "surge_freshness_gate", "ok": False, "reason": "freshness_pointer_missing"}
        ptr = json.loads(pointer_path.read_text(encoding="utf-8-sig"))
        last_path_raw = str(ptr.get("last") or "").strip() if isinstance(ptr, dict) else ""
        if not last_path_raw:
            return {"label": "surge_freshness_gate", "ok": False, "reason": "freshness_pointer_last_missing"}
        last_path = Path(last_path_raw)
        if not last_path.is_absolute():
            last_path = ROOT / last_path
        if not last_path.exists():
            return {
                "label": "surge_freshness_gate",
                "ok": False,
                "reason": "freshness_pointer_target_missing",
                "path": str(last_path),
            }
        age_sec = max(0.0, time.time() - last_path.stat().st_mtime)
        obj = json.loads(last_path.read_text(encoding="utf-8-sig"))
        verdict = str((obj.get("verdict") or obj.get("status") or "") if isinstance(obj, dict) else "").strip().upper()
        ok = verdict == "PASS" and age_sec <= float(max_age_sec)
        return {
            "label": "surge_freshness_gate",
            "ok": bool(ok),
            "skipped": bool(ok),
            "source": "freshness_pointer",
            "verdict": verdict or "-",
            "age_sec": round(age_sec, 1),
            "max_age_sec": float(max_age_sec),
            "path": str(last_path),
            "reason": "freshness_pointer_pass" if ok else "freshness_pointer_not_usable",
        }
    except Exception as exc:
        return {
            "label": "surge_freshness_gate",
            "ok": False,
            "reason": f"freshness_pointer_exception:{type(exc).__name__}",
        }


def _check_current_date_consistency(d_ymd: str) -> bool:
    if len(str(d_ymd or "")) != 8:
        logger.warning("[AUTO-RELEASE] invalid D=%s", d_ymd)
        return False

    orders_path = ROOT / "paper" / f"orders_{d_ymd}_exec.xlsx"
    if not orders_path.exists():
        logger.warning("[AUTO-RELEASE] orders exec missing path=%s", orders_path)
        return False

    if not FILLS_CSV.exists():
        logger.warning("[AUTO-RELEASE] fills missing path=%s", FILLS_CSV)
        return False
    derived_d = _derive_d_from_fills(FILLS_CSV)
    if derived_d != d_ymd:
        logger.warning("[AUTO-RELEASE] fills D mismatch derived=%s expected=%s", derived_d, d_ymd)
        return False

    ledger_path = ROOT / "virtual_ledger.csv"
    if not ledger_path.exists():
        logger.warning("[AUTO-RELEASE] virtual ledger missing path=%s", ledger_path)
        return False
    try:
        with ledger_path.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception as exc:
        logger.warning("[AUTO-RELEASE] virtual ledger read failed: %s", exc)
        return False
    if not rows:
        logger.info("[AUTO-RELEASE] virtual ledger empty path=%s", ledger_path)
        return True

    last = rows[-1]
    last_ymd = _date8(
        last.get("purchase_date")
        or last.get("datetime")
        or last.get("date")
        or last.get("exec_date")
        or last.get("ymd")
    )
    if last_ymd != d_ymd:
        logger.warning("[AUTO-RELEASE] virtual ledger latest date mismatch latest=%s expected=%s", last_ymd or "-", d_ymd)
        return False
    logger.info("[AUTO-RELEASE] date consistency PASS D=%s orders=%s ledger_latest=%s", d_ymd, orders_path, last_ymd)
    return True


# [2026-08-24] 자동 해제 한도 소진 알림.
# 이 분기는 로그에 "사람이 확인해야 한다" 라고 적으면서 사람에게 알리는 경로가 없었다.
# 2026-08-24 에 하드 블록이 09:47 에 걸려 12:26 까지 아무도 몰랐다.
# 60초마다 재진입하므로 억제 간격을 1시간으로 준다(레벨 기본 120초로는 폭주한다).
_AUTO_RELEASE_ALERT_COOLDOWN_SEC = 3600.0


def _alert_auto_release_exhausted(label: str, used: int, cap: int, payload: dict, flag_path: Path) -> None:
    """알림 실패가 루프를 멈추게 해서는 안 된다. 모든 예외를 삼킨다."""
    try:
        import sys as _sys
        _t = str(TOOLS_DIR)
        if _t not in _sys.path:
            _sys.path.insert(0, _t)
        from notify_channels import send_alert  # type: ignore
        send_alert(
            "[HARD_BLOCK] 자동 해제 한도 소진 - 사람이 확인해야 한다 "
            "label=%s used=%d/%d err_code=%s" % (label, used, cap, payload.get("err_code")),
            level="error",
            cooldown_sec=_AUTO_RELEASE_ALERT_COOLDOWN_SEC,
            extra={
                "label": label,
                "auto_release_used": used,
                "auto_release_cap": cap,
                "err_code": payload.get("err_code"),
                "blocked_at": payload.get("created_at"),
                "flag": str(flag_path),
            },
        )
    except Exception as e:
        logger.warning("[AUTO-RELEASE] 소진 알림 실패(무시하고 계속): %s", e)


def _try_auto_release_hard_block(flag_path: Path, d_ymd: str, dry_run: bool = False) -> bool:
    """하드 블록 플래그를 자동 해제할 수 있으면 해제한다.

    경로가 둘이다.
      (A) offhours_freshness_check - 예전부터 있던 경로. 날짜/신선도/정합성을 본다.
          냉각 경로보다 강한 검사이므로 그대로 둔다.
      (B) 그 밖의 모든 라벨 - 냉각 시간 + 오늘 자동 해제 횟수 상한.
          상한에 도달하면 풀지 않는다(fail-closed).
    """
    try:
        if not flag_path.exists():
            return False
        content = flag_path.read_text(encoding="utf-8-sig", errors="replace")
        try:
            payload = json.loads(content)
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        label = str(payload.get("label") or "").strip()

        # ---- (A) 기존 경로: offhours_freshness_check -------------------------
        if "offhours_freshness_check" in content:
            flag_ymd = dt.datetime.fromtimestamp(flag_path.stat().st_mtime, tz=KST).strftime("%Y%m%d")
            if flag_ymd == d_ymd:
                logger.warning("[AUTO-RELEASE] skip: flag date matches D flag_ymd=%s D=%s", flag_ymd, d_ymd)
                return False
            if not _check_freshness_pass():
                logger.warning("[AUTO-RELEASE] skip: freshness not recovered")
                return False
            if not _check_current_date_consistency(d_ymd):
                logger.warning("[AUTO-RELEASE] skip: current date consistency failed D=%s", d_ymd)
                return False
            return _release_hard_block_flag(
                flag_path,
                "offhours_freshness_recovered",
                {"label": label or "offhours_freshness_check", "flag_ymd": flag_ymd, "d_ymd": d_ymd},
                dry_run,
            )

        # ---- (B) 신규 경로: 냉각 + 횟수 상한 ---------------------------------
        if not _hard_block_auto_release_enabled():
            logger.warning("[AUTO-RELEASE] skip: PAPER_LOOP_HARD_BLOCK_AUTO_RELEASE 로 꺼져 있다")
            return False
        if not label:
            logger.warning("[AUTO-RELEASE] skip: 플래그에 label 이 없다 path=%s", flag_path)
            return False

        created_at = _parse_flag_created_at(payload.get("created_at"))
        if created_at is None:
            logger.warning("[AUTO-RELEASE] skip: created_at 을 읽을 수 없다 label=%s", label)
            return False

        age_sec = (dt.datetime.now(tz=KST) - created_at).total_seconds()
        cooldown = _hard_block_cooldown_sec()
        if age_sec < cooldown:
            logger.info(
                "[AUTO-RELEASE] cooling down label=%s age=%.0fs / cooldown=%.0fs (남은 %.0fs)",
                label,
                age_sec,
                cooldown,
                cooldown - age_sec,
            )
            return False

        today_ymd = dt.datetime.now(tz=KST).strftime("%Y%m%d")
        used = _count_auto_releases(label, today_ymd)
        cap = _hard_block_max_auto_release()
        if used >= cap:
            logger.error(
                "[AUTO-RELEASE] REFUSED label=%s 오늘 자동해제 %d/%d 소진 - 사람이 확인해야 한다 "
                "(err_code=%s, flag=%s)",
                label,
                used,
                cap,
                payload.get("err_code"),
                flag_path,
            )
            _alert_auto_release_exhausted(label, used, cap, payload, flag_path)
            return False

        return _release_hard_block_flag(
            flag_path,
            "cooldown",
            {
                "label": label,
                "err_code": payload.get("err_code"),
                "returncode": payload.get("returncode"),
                "blocked_at": payload.get("created_at"),
                "age_sec": round(age_sec, 1),
                "cooldown_sec": cooldown,
                "auto_release_seq": used + 1,
                "auto_release_cap": cap,
                "d_ymd": d_ymd,
            },
            dry_run,
        )
    except Exception as exc:
        logger.warning("[AUTO-RELEASE] failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Cycle execution
# ---------------------------------------------------------------------------

def run_cycle(
    cycle: int,
    py: str,
    mock: str,
    dry_run: bool,
    max_orders: int,
    dispatch_apply: bool,
    once: bool = False,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    cycle_started_at = time.time()
    _price_timeout_raw = float(str(os.environ.get("INTRADAY_PRICE_TIMEOUT_SEC", "65")).strip() or "65")
    _price_timeout_cap = float(str(os.environ.get("INTRADAY_PRICE_TIMEOUT_CAP_SEC", "90")).strip() or "90")
    _price_timeout_sec = min(max(5.0, _price_timeout_raw), max(5.0, _price_timeout_cap))
    _price_retry = max(0, int(str(os.environ.get("INTRADAY_PRICE_RETRY", "0")).strip() or "0"))
    _price_retry_sleep = max(0.0, float(str(os.environ.get("INTRADAY_PRICE_RETRY_SLEEP", "0.2")).strip() or "0.2"))
    _price_rate_limit_retry_max = max(
        0,
        int(str(os.environ.get("INTRADAY_PRICE_RATE_LIMIT_RETRY_MAX", "0")).strip() or "0"),
    )
    _price_request_interval_raw = float(
        str(os.environ.get("INTRADAY_PRICE_REQUEST_INTERVAL_SEC", "1.05")).strip() or "1.05"
    )
    _price_request_interval_cap = float(
        str(os.environ.get("INTRADAY_PRICE_REQUEST_INTERVAL_CAP_SEC", "0.35")).strip() or "0.35"
    )
    _price_request_interval_sec = min(
        max(0.0, _price_request_interval_raw),
        max(0.0, _price_request_interval_cap),
    )
    _price_max_elapsed_sec = max(
        1.0,
        float(str(os.environ.get("INTRADAY_PRICE_MAX_ELAPSED_SEC", "40")).strip() or "40"),
    )
    _price_max_elapsed_sec = min(_price_max_elapsed_sec, max(1.0, _price_timeout_sec - 15.0))
    _snapshot_retry = max(0, int(str(os.environ.get("INTRADAY_SNAPSHOT_RETRY", "0")).strip() or "0"))
    _snapshot_retry_sleep = max(0.0, float(str(os.environ.get("INTRADAY_SNAPSHOT_RETRY_SLEEP", "0.1")).strip() or "0.1"))
    _surge_universe_max = max(1, int(str(os.environ.get("INTRADAY_SURGE_UNIVERSE_MAX", "10")).strip() or "10"))
    _price_max_total_codes = max(0, int(str(os.environ.get("INTRADAY_PRICE_MAX_TOTAL_CODES", "85")).strip() or "85"))
    _surge_universe_slice_max = max(
        1,
        min(
            _surge_universe_max,
            int(str(os.environ.get("INTRADAY_SURGE_UNIVERSE_SLICE_MAX", "50")).strip() or "50"),
        ),
    )
    _price_broad_every_n = max(1, int(str(os.environ.get("INTRADAY_PRICE_BROAD_EVERY_N", "3")).strip() or "3"))
    _price_mock = str(os.environ.get("INTRADAY_PRICE_MOCK", "auto")).strip().lower() or "auto"
    if _price_mock not in {"auto", "true", "false"}:
        _price_mock = "auto"

    # Step 0: check market anomaly (Sidecar / CB) from index WS
    anomaly_cmd = [
        py,
        str(TOOLS_DIR / "market_anomaly_detector.py")
    ]
    if not dry_run:
        anomaly_step = _run(anomaly_cmd, "market_anomaly_detector", timeout=10.0)
        if not anomaly_step.get("ok"):
            anomaly_step["ok"] = True
            anomaly_step["advisory_only"] = True
            anomaly_step["fallback_reason"] = "market_anomaly_detector_non_blocking"
        results.append(anomaly_step)
    else:
        logger.info("[DRY-RUN] Would run: %s", " ".join(anomaly_cmd))
        results.append({"label": "market_anomaly_detector", "ok": True, "elapsed": 0.0, "reason": "dry-run"})

    # Step 1: fetch realtime prices.
    import os as _os
    _surge_univ = str(_os.environ.get("SURGE_UNIVERSE_ENABLED", "1")).strip().lower()
    _surge_univ_enabled = _surge_univ not in {"0", "false", "no", "off"}
    _use_surge_univ = bool(_surge_univ_enabled and (int(cycle) % int(_price_broad_every_n) == 0))
    snap_cmd = [
        py,
        str(TOOLS_DIR / "intraday_price_snapshot.py"),
        "--from-candidates",
        "--mock", _price_mock,
        "--kis-timeout-sec", str(os.environ.get("INTRADAY_KIS_TIMEOUT_SEC", "2.5")).strip() or "2.5",
        "--retry", str(_price_retry),
        "--retry-sleep", str(_price_retry_sleep),
        "--workers", "1",
        "--request-interval-sec", str(_price_request_interval_sec),
        "--rate-limit-retry-max", str(_price_rate_limit_retry_max),
        "--max-elapsed-sec", str(_price_max_elapsed_sec),
        "--max-total-codes", str(_price_max_total_codes),
    ]
    if _use_surge_univ:
        snap_cmd.append("--with-surge-universe")
        _broad_seq = max(0, (int(cycle) // int(_price_broad_every_n)) - 1)
        _surge_universe_offset = (_broad_seq * _surge_universe_slice_max) % _surge_universe_max
        snap_cmd.extend(["--surge-universe-max", str(_surge_universe_slice_max)])
        snap_cmd.extend(["--surge-universe-offset", str(_surge_universe_offset)])
    else:
        _surge_universe_offset = 0
    # Intraday loop needs timely price refresh; hoga enrichment is optional.
    _hoga_mode = str(os.environ.get("INTRADAY_HOGA_FALLBACK_MODE", "off")).strip() or "off"
    snap_cmd.extend(["--hoga-fallback-mode", _hoga_mode])
    price_tier = "broad" if _use_surge_univ else "fast"
    logger.info(
        "[PRICE_TIER] cycle=%d tier=%s broad_enabled=%s broad_every_n=%d surge_universe_max=%d",
        int(cycle),
        price_tier,
        bool(_surge_univ_enabled),
        int(_price_broad_every_n),
        int(_surge_universe_max),
    )
    _price_rest_cutoff_hhmm = int(str(os.environ.get("INTRADAY_PRICE_REST_CUTOFF_HHMM", "1520")).strip() or "1520")
    _now_hhmm_for_price = int(dt.datetime.now(tz=KST).strftime("%H%M"))
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(snap_cmd))
        _append_step_status(
            cycle,
            results,
            {
                "label": "price_snapshot",
                "ok": True,
                "dry_run": True,
                "tier": price_tier,
                "broad_every_n": int(_price_broad_every_n),
                "with_surge_universe": bool(_use_surge_univ),
                "surge_universe_slice_max": int(_surge_universe_slice_max),
                "surge_universe_offset": int(_surge_universe_offset),
                "price_max_total_codes": int(_price_max_total_codes),
                "price_timeout_sec": float(_price_timeout_sec),
                "price_retry": int(_price_retry),
                "price_request_interval_sec": float(_price_request_interval_sec),
                "price_rate_limit_retry_max": int(_price_rate_limit_retry_max),
                "price_max_elapsed_sec": float(_price_max_elapsed_sec),
            },
        )
    elif _now_hhmm_for_price >= _price_rest_cutoff_hhmm:
        _append_step_status(cycle, results, {
            "label": "price_snapshot",
            "ok": True,
            "skipped": True,
            "advisory_only": True,
            "fallback_used": "parquet_prices",
            "fallback_reason": "late_market_rest_cutoff",
            "hhmm": int(_now_hhmm_for_price),
            "cutoff_hhmm": int(_price_rest_cutoff_hhmm),
            "tier": price_tier,
            "broad_every_n": int(_price_broad_every_n),
            "with_surge_universe": bool(_use_surge_univ),
            "price_max_total_codes": int(_price_max_total_codes),
            "price_timeout_sec": float(_price_timeout_sec),
            "price_max_elapsed_sec": float(_price_max_elapsed_sec),
        })
        _write_status(cycle, results, "-")
    else:
        price_step = _run(snap_cmd, "price_snapshot", timeout=_price_timeout_sec)
        price_step["tier"] = price_tier
        price_step["broad_every_n"] = int(_price_broad_every_n)
        price_step["with_surge_universe"] = bool(_use_surge_univ)
        price_step["surge_universe_slice_max"] = int(_surge_universe_slice_max)
        price_step["surge_universe_offset"] = int(_surge_universe_offset)
        price_step["price_max_total_codes"] = int(_price_max_total_codes)
        price_step["price_timeout_sec"] = float(_price_timeout_sec)
        price_step["price_retry"] = int(_price_retry)
        price_step["price_request_interval_sec"] = float(_price_request_interval_sec)
        price_step["price_rate_limit_retry_max"] = int(_price_rate_limit_retry_max)
        price_step["price_max_elapsed_sec"] = float(_price_max_elapsed_sec)
        _append_step_status(cycle, results, price_step)
        if not results[-1]["ok"]:
            logger.warning("[LOOP] price_snapshot failed; using parquet prices as fallback")
            results[-1]["ok"] = True
            results[-1]["fallback_used"] = "parquet_prices"
            results[-1]["fallback_reason"] = "price_snapshot_failed"
            _write_status(cycle, results, "-")

    arl_auto_cmd = [py, str(TOOLS_DIR / "arl_auto_validate_intraday_history.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(arl_auto_cmd))
        _append_step_status(cycle, results, {"label": "arl_auto_validate_intraday_history", "ok": True, "dry_run": True})
    else:
        arl_step = _run(arl_auto_cmd, "arl_auto_validate_intraday_history", timeout=90.0)
        if not arl_step.get("ok"):
            arl_step["ok"] = True
            arl_step["advisory_only"] = True
            arl_step["fallback_reason"] = "arl_auto_validation_non_blocking"
        _append_step_status(cycle, results, arl_step)

    _rising_top_n = max(1, int(str(os.environ.get("MARKET_RISING_TOP_N", "30")).strip() or "30"))
    _rising_bucket_max = max(0, int(str(os.environ.get("MARKET_RISING_BUCKET_MAX", "1")).strip() or "1"))
    _rising_timeout_raw = float(str(os.environ.get("MARKET_RISING_TIMEOUT_SEC", "90")).strip() or "90")
    _rising_timeout_cap = float(str(os.environ.get("MARKET_RISING_TIMEOUT_CAP_SEC", "15")).strip() or "15")
    _rising_timeout_sec = min(max(5.0, _rising_timeout_raw), max(5.0, _rising_timeout_cap))
    rising_cmd = [
        py,
        str(ROOTB_DIR / "tools" / "market_rising_snapshot.py"),
        "--top-n",
        str(_rising_top_n),
        "--bucket-max",
        str(_rising_bucket_max),
        "--kis-timeout-sec",
        str(os.environ.get("MARKET_RISING_KIS_TIMEOUT_SEC", "4.0")).strip() or "4.0",
        "--deadline-sec",
        str(max(5.0, _rising_timeout_sec - 5.0)),
        "--skip-bid-reinforcement",
    ]
    _rising_every_n = _env_int("MARKET_RISING_EVERY_N", 2)
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(rising_cmd))
        _append_step_status(cycle, results, {"label": "market_rising_snapshot", "ok": True, "dry_run": True})
    elif not (cycle == 1 or once or _run_on_cycle(cycle, _rising_every_n)):
        _append_step_status(cycle, results, _cadence_skip("market_rising_snapshot", _rising_every_n))
    else:
        rising_step = _run(rising_cmd, "market_rising_snapshot", timeout=_rising_timeout_sec)
        if not rising_step.get("ok"):
            rising_step["ok"] = True
            rising_step["advisory_only"] = True
            rising_step["fallback_reason"] = "market_rising_snapshot_stale_non_blocking"
        _append_step_status(cycle, results, rising_step)

    lob_cmd = [py, str(TOOLS_DIR / "surge_lob_ingest.py")]
    _lob_every_n = _env_int("SURGE_LOB_INGEST_EVERY_N", 2)
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(lob_cmd))
        _append_step_status(cycle, results, {"label": "surge_lob_ingest", "ok": True, "dry_run": True})
    elif not _run_on_cycle(cycle, _lob_every_n):
        _append_step_status(cycle, results, _cadence_skip("surge_lob_ingest", _lob_every_n))
    else:
        _lob_timeout_raw = float(str(os.environ.get("SURGE_LOB_INGEST_TIMEOUT_SEC", "30")).strip() or "30")
        _lob_timeout_cap = float(str(os.environ.get("SURGE_LOB_INGEST_TIMEOUT_CAP_SEC", "20")).strip() or "20")
        _lob_timeout_sec = min(max(10.0, _lob_timeout_raw), max(10.0, _lob_timeout_cap))
        _lob_env = {
            "SURGE_LOB_HOGA_MAX_FETCH": str(os.environ.get("SURGE_LOB_HOGA_MAX_FETCH", "5")),
        }
        _lob_step = _run(lob_cmd, "surge_lob_ingest", env=_lob_env, timeout=_lob_timeout_sec)
        if not _lob_step.get("ok"):
            _lob_step["ok"] = True
            _lob_step["advisory_only"] = True
            _lob_step["fallback_reason"] = "surge_lob_ingest_stale_lob_non_blocking"
        _append_step_status(cycle, results, _lob_step)

    orderflow_glr_cmd = [py, str(TOOLS_DIR / "orderflow_hawkes_glr.py")]
    _orderflow_glr_every_n = _env_int("ORDERFLOW_GLR_EVERY_N", 1)
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(orderflow_glr_cmd))
        _append_step_status(cycle, results, {"label": "orderflow_hawkes_glr", "ok": True, "dry_run": True})
    elif not (once or _run_on_cycle(cycle, _orderflow_glr_every_n)):
        _append_step_status(cycle, results, _cadence_skip("orderflow_hawkes_glr", _orderflow_glr_every_n))
    else:
        _orderflow_timeout_raw = float(str(os.environ.get("ORDERFLOW_GLR_TIMEOUT_SEC", "20")).strip() or "20")
        _orderflow_timeout_cap = float(str(os.environ.get("ORDERFLOW_GLR_TIMEOUT_CAP_SEC", "20")).strip() or "20")
        _orderflow_timeout_sec = min(max(5.0, _orderflow_timeout_raw), max(5.0, _orderflow_timeout_cap))
        orderflow_step = _run(orderflow_glr_cmd, "orderflow_hawkes_glr", timeout=_orderflow_timeout_sec)
        if not orderflow_step.get("ok"):
            orderflow_step["ok"] = True
            orderflow_step["advisory_only"] = True
            orderflow_step["fallback_reason"] = "orderflow_hawkes_glr_non_blocking"
        _append_step_status(cycle, results, orderflow_step)

    _news_collect_every_n = _env_int("INTRADAY_NEWS_COLLECT_EVERY_N", 5)
    _news_score_every_n = _env_int("INTRADAY_NEWS_SCORE_EVERY_N", _news_collect_every_n)
    news_collect_cmd = [
        py,
        str(TOOLS_DIR / "news_collect_naver_daily.py"),
        "--session-name",
        "intraday",
        "--window-start",
        "09:00",
        "--window-end",
        "15:30",
        "--max-symbols",
        str(os.environ.get("INTRADAY_NEWS_COLLECT_MAX_SYMBOLS", "8")).strip() or "8",
        "--cache-ttl-sec",
        str(os.environ.get("INTRADAY_NEWS_COLLECT_CACHE_TTL_SEC", "1800")).strip() or "1800",
    ]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(news_collect_cmd))
        _append_step_status(cycle, results, {"label": "news_collect_naver_daily", "ok": True, "dry_run": True})
    elif not _env_bool("INTRADAY_NEWS_COLLECT_ENABLED", False):
        _append_step_status(cycle, results, {
            "label": "news_collect_naver_daily",
            "ok": True,
            "skipped": True,
            "skip_reason": "intraday_news_collect_disabled",
        })
    elif not _run_on_cycle(cycle, _news_collect_every_n):
        _append_step_status(cycle, results, _cadence_skip("news_collect_naver_daily", _news_collect_every_n))
    else:
        _news_collect_timeout_sec = max(
            15.0,
            float(str(os.environ.get("INTRADAY_NEWS_COLLECT_TIMEOUT_SEC", "45")).strip() or "45"),
        )
        _news_mode_env = {"NEWS_COLLECT_MODE": str(os.environ.get("NEWS_COLLECT_MODE", "production"))}
        news_collect_step = _run(news_collect_cmd, "news_collect_naver_daily", env=_news_mode_env, timeout=_news_collect_timeout_sec)
        if not news_collect_step.get("ok"):
            news_collect_step["ok"] = True
            news_collect_step["advisory_only"] = True
            news_collect_step["fallback_reason"] = "news_collect_intraday_non_blocking"
        _append_step_status(cycle, results, news_collect_step)

    news_score_cmd = [py, str(TOOLS_DIR / "news_score_daily.py")]
    if dry_run:
        logger.info("[DRY] would run: %s (NEWS_SCORE_REFERENCE_YMD=today)", " ".join(news_score_cmd))
        _append_step_status(cycle, results, {"label": "news_score_daily", "ok": True, "dry_run": True})
    elif not _run_on_cycle(cycle, _news_score_every_n):
        _append_step_status(cycle, results, _cadence_skip("news_score_daily", _news_score_every_n))
    else:
        news_score_step = _run(
            news_score_cmd, "news_score_daily",
            env={
                "NEWS_SESSION_NAME": "intraday",
                "NEWS_SCORE_REFERENCE_YMD": "today",
                "NEWS_SCORE_T1_MODE": "",
                "NEWS_COLLECT_MODE": str(os.environ.get("NEWS_COLLECT_MODE", "production")),
            },
            timeout=60.0,
        )
        # [2026-08-22] 뉴스 점수 실패를 non-blocking 으로 (PLANS 59).
        #
        # 2026-08-21 에 이 스텝의 ERR_OUTPUT_PERMISSION 이 하드 블록을 만들어
        # 인트라데이 루프 전체가 멈췄다. 진입만이 아니라 청산도 함께 멈춘다.
        #
        # 그런데 뉴스 축은 2026-08-20 (80) 에서 격리돼 final_score 에 반영되지 않고,
        # 2026-08-22 (58) 검정에서 예측력도 없었다(IC ~0, t~0.3).
        # 진입층의 뉴스 게이트 18개는 전부 fail-open 이다 - 뉴스가 없어도 판단이 정상 작동한다.
        # **기여도 0 인 구성요소가 죽으면 매매를 멈추는 비대칭**을 없앤다.
        #
        # 바로 위 news_collect_naver_daily 가 이미 같은 처리를 한다(동일 관용).
        # 실패 사실은 advisory_only / fallback_reason 으로 상태에 남으므로 조용히 지나가지 않는다.
        if not news_score_step.get("ok"):
            news_score_step["ok"] = True
            news_score_step["advisory_only"] = True
            news_score_step["fallback_reason"] = "news_score_intraday_non_blocking"
            _STEP_FAIL_STREAK.pop("news_score_daily", None)
        _append_step_status(cycle, results, news_score_step)

    # Step 2: build realtime candidate inputs.
    surge_ml_cmd = [py, str(TOOLS_DIR / "surge_ml_score_realtime.py")]
    surge_ml_env = {
        "LOKY_MAX_CPU_COUNT": str(_env_int("LOKY_MAX_CPU_COUNT", max(1, os.cpu_count() or 1))),
    }
    _surge_ml_timeout_raw = float(str(os.environ.get("SURGE_ML_SCORE_TIMEOUT_SEC", "35")).strip() or "35")
    _surge_ml_timeout_cap = float(str(os.environ.get("SURGE_ML_SCORE_TIMEOUT_CAP_SEC", "90")).strip() or "90")
    _surge_ml_timeout_sec = min(max(5.0, _surge_ml_timeout_raw), max(5.0, _surge_ml_timeout_cap))
    _surge_sanity_timeout_sec = max(
        60.0,
        float(str(os.environ.get("SURGE_SANITY_TIMEOUT_SEC", "90")).strip() or "90"),
    )
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_ml_cmd))
        _append_step_status(cycle, results, {"label": "surge_ml_score", "ok": True, "dry_run": True})
    else:
        surge_ml_step = _run(surge_ml_cmd, "surge_ml_score", env=surge_ml_env, timeout=_surge_ml_timeout_sec)
        if not surge_ml_step.get("ok"):
            surge_ml_step["ok"] = True
            surge_ml_step["advisory_only"] = True
            surge_ml_step["fallback_reason"] = "surge_ml_score_stale_non_blocking"
        _append_step_status(cycle, results, surge_ml_step)

    freshness_gate_cmd = [py, str(TOOLS_DIR / "freshness_check_v1.py")]
    surge_cmd = [py, str(TOOLS_DIR / "surge_detector_realtime.py")]
    surge_sanity_cmd = [py, str(TOOLS_DIR / "fast_surge_sanity_labeler.py")]
    surge_attack_candidate_tracker_cmd = [py, str(TOOLS_DIR / "build_surge_attack_candidate_tracker.py")]
    surge_active_response_layer_cmd = [py, str(TOOLS_DIR / "build_surge_active_response_layer.py")]
    surge_active_response_queue_cmd = [py, str(TOOLS_DIR / "build_surge_active_response_queue.py")]
    surge_live_readiness_audit_cmd = [py, str(TOOLS_DIR / "build_surge_live_readiness_audit.py")]
    wait_lob_hoga_cmd = [py, str(TOOLS_DIR / "surge_wait_lob_hoga_probe.py")]
    entry_change_exception_cmd = [py, str(TOOLS_DIR / "build_surge_entry_change_exception_candidates.py")]
    entry_change_clean_watch_cmd = [py, str(TOOLS_DIR / "build_surge_entry_change_clean_continuation_watch.py")]
    high_rejection_gray_watch_cmd = [py, str(TOOLS_DIR / "build_surge_high_rejection_gray_zone_markout_watch.py")]
    entry_change_policy_fitness_cmd = [py, str(TOOLS_DIR / "build_surge_entry_change_policy_fitness.py")]
    surge_no_entry_hard_blocker_summary_cmd = [py, str(TOOLS_DIR / "build_surge_no_entry_hard_blocker_policy_summary.py")]
    surge_ev_shadow_cmd = [py, str(TOOLS_DIR / "build_surge_ev_probe_candidates.py")]
    surge_ev_source_price_cmd = [
        py,
        str(TOOLS_DIR / "intraday_price_snapshot.py"),
        "--ev-source-queues-only",
        "--mock", _price_mock,
        "--kis-timeout-sec", str(os.environ.get("INTRADAY_KIS_TIMEOUT_SEC", "2.5")).strip() or "2.5",
        "--hoga-fallback-mode", "off",
        "--retry", "0",
        "--retry-sleep", str(_snapshot_retry_sleep),
        "--workers", "1",
        "--max-elapsed-sec", str(os.environ.get("SURGE_EV_SOURCE_PRICE_MAX_ELAPSED_SEC", "30")).strip() or "30",
        "--out-csv", str(LOG_DIR / "intraday_prices_ev_source_observation_latest.csv"),
    ]
    _surge_ev_source_price_timeout_sec = max(
        35.0,
        float(str(os.environ.get("SURGE_EV_SOURCE_PRICE_TIMEOUT_SEC", "45")).strip() or "45"),
    )
    surge_ev_shadow_sim_cmd = [py, str(TOOLS_DIR / "simulate_surge_ev_micro_probe.py")]
    surge_ev_shadow_gradebook_cmd = [py, str(TOOLS_DIR / "build_surge_ev_shadow_gradebook.py")]
    surge_ev_bucket_decomp_cmd = [py, str(TOOLS_DIR / "build_surge_ev_bucket_decomposition.py")]
    surge_ev_conditional_verdict_cmd = [py, str(TOOLS_DIR / "build_surge_ev_conditional_verdict.py")]
    surge_ev_probe_readiness_cmd = [py, str(TOOLS_DIR / "build_surge_ev_probe_readiness.py")]
    surge_ev_paper_probe_staged_cmd = [py, str(TOOLS_DIR / "build_surge_ev_paper_probe_staged.py")]
    surge_ev_paper_probe_consumer_cmd = [py, str(TOOLS_DIR / "consume_surge_ev_paper_probe_staged.py")]
    surge_lob_quality_diag_cmd = [py, str(TOOLS_DIR / "build_surge_lob_quality_diagnostic.py")]
    surge_entry_cap_decomp_cmd = [py, str(TOOLS_DIR / "build_surge_entry_cap_decomposition.py")]
    surge_virtual_probe_audit_cmd = [py, str(TOOLS_DIR / "build_surge_virtual_probe_safety_audit.py")]
    surge_outcome_promotion_cmd = [py, str(TOOLS_DIR / "build_surge_outcome_promotion_report.py")]
    microstructure_observe_cmd = [py, str(TOOLS_DIR / "build_microstructure_observe_contract.py")]
    surge_freshness_audit_cmd = [py, str(TOOLS_DIR / "build_surge_freshness_audit.py")]
    surge_ev_reject_diag_cmd = [py, str(TOOLS_DIR / "build_surge_ev_shadow_reject_diagnostic.py")]
    surge_state_machine_shadow_cmd = [py, str(TOOLS_DIR / "build_surge_state_machine_shadow.py")]
    surge_state_transition_history_cmd = [py, str(TOOLS_DIR / "build_surge_state_machine_transition_history.py")]
    surge_state_transition_outcome_cmd = [py, str(TOOLS_DIR / "build_surge_state_transition_outcome.py")]
    surge_transition_probe_review_cmd = [py, str(TOOLS_DIR / "build_surge_transition_probe_review_candidates.py")]
    surge_probe_review_cohort_cmd = [py, str(TOOLS_DIR / "build_surge_probe_review_cohort_diagnostic.py")]
    capital_operation_decision_cmd = [py, str(TOOLS_DIR / "build_capital_operation_decision_report.py")]
    surge_probe_sizing_review_cmd = [py, str(TOOLS_DIR / "build_surge_probe_sizing_review.py")]
    surge_probe_sizing_virtual_ledger_cmd = [py, str(TOOLS_DIR / "consume_surge_probe_sizing_review_virtual_ledger.py")]
    surge_probe_sizing_markout_cmd = [py, str(TOOLS_DIR / "build_surge_probe_sizing_markout.py")]

    def _cycle_elapsed_sec() -> float:
        return max(0.0, time.time() - cycle_started_at)

    def _surge_research_budget_sec() -> float:
        return max(
            60.0,
            float(str(os.environ.get("SURGE_RESEARCH_CHAIN_MAX_CYCLE_ELAPSED_SEC", "180")).strip() or "180"),
        )

    def _append_surge_probe_review_chain() -> None:
        chain_every_n = _env_int("SURGE_PROBE_REVIEW_CHAIN_EVERY_N", 3)
        budget_sec = _surge_research_budget_sec()
        if (not dry_run) and (not once) and _cycle_elapsed_sec() > budget_sec:
            _append_step_status(cycle, results, {
                "label": "surge_probe_review_chain",
                "ok": True,
                "skipped": True,
                "advisory_only": True,
                "skip_reason": "cycle_elapsed_budget_exceeded",
                "cycle_elapsed_sec": round(_cycle_elapsed_sec(), 1),
                "budget_sec": budget_sec,
            })
            return
        if dry_run:
            for _label, _cmd in [
                ("surge_transition_probe_review_candidates", surge_transition_probe_review_cmd),
                ("surge_probe_review_cohort_diagnostic", surge_probe_review_cohort_cmd),
                ("surge_entry_change_clean_continuation_watch", entry_change_clean_watch_cmd),
                ("surge_high_rejection_gray_zone_markout_watch", high_rejection_gray_watch_cmd),
                ("surge_entry_change_policy_fitness", entry_change_policy_fitness_cmd),
                ("surge_no_entry_hard_blocker_policy_summary", surge_no_entry_hard_blocker_summary_cmd),
                ("surge_probe_review_lob_quality", [py, str(TOOLS_DIR / "build_surge_probe_review_lob_quality.py")]),
                ("capital_operation_decision_report", capital_operation_decision_cmd),
                ("surge_probe_sizing_review", surge_probe_sizing_review_cmd),
                ("surge_probe_sizing_virtual_ledger", surge_probe_sizing_virtual_ledger_cmd),
                ("surge_probe_sizing_markout", surge_probe_sizing_markout_cmd),
            ]:
                logger.info("[DRY] would run: %s", " ".join(_cmd))
                _append_step_status(cycle, results, {"label": _label, "ok": True, "dry_run": True})
            return
        if not (cycle == 1 or once or _run_on_cycle(cycle, chain_every_n)):
            _append_step_status(cycle, results, _cadence_skip("surge_probe_review_chain", chain_every_n))
            return
        for _label, _cmd, _timeout, _env in [
            ("surge_transition_probe_review_candidates", surge_transition_probe_review_cmd, 30.0, None),
            ("surge_probe_review_cohort_diagnostic", surge_probe_review_cohort_cmd, 30.0, None),
            ("surge_entry_change_clean_continuation_watch", entry_change_clean_watch_cmd, 30.0, None),
            ("surge_high_rejection_gray_zone_markout_watch", high_rejection_gray_watch_cmd, 30.0, None),
            ("surge_entry_change_policy_fitness", entry_change_policy_fitness_cmd, 30.0, None),
            ("surge_no_entry_hard_blocker_policy_summary", surge_no_entry_hard_blocker_summary_cmd, 30.0, None),
            (
                "surge_probe_review_lob_quality",
                [
                    py,
                    str(TOOLS_DIR / "build_surge_probe_review_lob_quality.py"),
                    "--max-fetch",
                    str(_env_int("SURGE_PROBE_REVIEW_LOB_MAX_FETCH", 7)),
                ],
                90.0,
                {
                    "SURGE_LOB_KIS_SLEEP_SEC": str(
                        os.environ.get("SURGE_PROBE_REVIEW_LOB_SLEEP_SEC", "1.25")
                    ),
                },
            ),
            ("capital_operation_decision_report", capital_operation_decision_cmd, 30.0, None),
            ("surge_probe_sizing_review", surge_probe_sizing_review_cmd, 30.0, None),
            ("surge_probe_sizing_virtual_ledger", surge_probe_sizing_virtual_ledger_cmd, 30.0, None),
            ("surge_probe_sizing_markout", surge_probe_sizing_markout_cmd, 30.0, None),
        ]:
            _step = _run(_cmd, _label, env=_env, timeout=_timeout)
            if not _step.get("ok"):
                _step["ok"] = True
                _step["advisory_only"] = True
                _step["fallback_reason"] = f"{_label}_non_blocking"
            _append_step_status(cycle, results, _step)

    dashboard_state_built = False
    surge_ran = False
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(freshness_gate_cmd))
        _append_step_status(cycle, results, {"label": "surge_freshness_gate", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_cmd))
        _append_step_status(cycle, results, {"label": "surge_detector", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_sanity_cmd))
        _append_step_status(cycle, results, {"label": "surge_sanity_labeler", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_attack_candidate_tracker_cmd))
        _append_step_status(cycle, results, {"label": "surge_attack_candidate_tracker", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_active_response_layer_cmd))
        _append_step_status(cycle, results, {"label": "surge_active_response_layer", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_active_response_queue_cmd))
        _append_step_status(cycle, results, {"label": "surge_active_response_queue", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_live_readiness_audit_cmd))
        _append_step_status(cycle, results, {"label": "surge_live_readiness_audit", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(wait_lob_hoga_cmd))
        _append_step_status(cycle, results, {"label": "surge_wait_lob_hoga_observe", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(entry_change_exception_cmd))
        _append_step_status(cycle, results, {"label": "surge_entry_change_exception_candidates", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_ev_shadow_cmd))
        _append_step_status(cycle, results, {"label": "surge_ev_shadow_candidates", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_ev_source_price_cmd))
        _append_step_status(cycle, results, {"label": "surge_ev_source_price_observation", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_ev_shadow_sim_cmd))
        _append_step_status(cycle, results, {"label": "surge_ev_shadow_simulation", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_ev_shadow_gradebook_cmd))
        _append_step_status(cycle, results, {"label": "surge_ev_shadow_gradebook", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_ev_bucket_decomp_cmd))
        _append_step_status(cycle, results, {"label": "surge_ev_bucket_decomposition", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_ev_conditional_verdict_cmd))
        _append_step_status(cycle, results, {"label": "surge_ev_conditional_verdict", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_ev_probe_readiness_cmd))
        _append_step_status(cycle, results, {"label": "surge_ev_probe_readiness", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_ev_paper_probe_staged_cmd))
        _append_step_status(cycle, results, {"label": "surge_ev_paper_probe_staged", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_ev_paper_probe_consumer_cmd))
        _append_step_status(cycle, results, {"label": "surge_ev_paper_probe_consumer", "ok": True, "dry_run": True})
        for _label, _cmd in [
            ("surge_lob_quality_diagnostic", surge_lob_quality_diag_cmd),
            ("surge_entry_cap_decomposition", surge_entry_cap_decomp_cmd),
            ("surge_virtual_probe_safety_audit", surge_virtual_probe_audit_cmd),
            ("surge_outcome_promotion_report", surge_outcome_promotion_cmd),
            ("microstructure_observe_contract", microstructure_observe_cmd),
            ("surge_freshness_audit", surge_freshness_audit_cmd),
        ]:
            logger.info("[DRY] would run: %s", " ".join(_cmd))
            _append_step_status(cycle, results, {"label": _label, "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_ev_reject_diag_cmd))
        _append_step_status(cycle, results, {"label": "surge_ev_shadow_reject_diagnostic", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_state_machine_shadow_cmd))
        _append_step_status(cycle, results, {"label": "surge_state_machine_shadow", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_state_transition_history_cmd))
        _append_step_status(cycle, results, {"label": "surge_state_transition_history", "ok": True, "dry_run": True})
        logger.info("[DRY] would run: %s", " ".join(surge_state_transition_outcome_cmd))
        _append_step_status(cycle, results, {"label": "surge_state_transition_outcome", "ok": True, "dry_run": True})
        _append_surge_probe_review_chain()
        surge_ran = True
    else:
        freshness_pointer_max_age_sec = float(
            str(os.environ.get("SURGE_FRESHNESS_POINTER_MAX_AGE_SEC", "3600")).strip() or "3600"
        )
        freshness_gate_step = _freshness_pointer_step(freshness_pointer_max_age_sec)
        if not freshness_gate_step.get("ok"):
            freshness_timeout_raw = float(str(os.environ.get("SURGE_FRESHNESS_GATE_TIMEOUT_SEC", "30")).strip() or "30")
            freshness_timeout_cap = float(str(os.environ.get("SURGE_FRESHNESS_GATE_TIMEOUT_CAP_SEC", "30")).strip() or "30")
            freshness_timeout_sec = min(max(5.0, freshness_timeout_raw), max(5.0, freshness_timeout_cap))
            freshness_gate_step = _run(freshness_gate_cmd, "surge_freshness_gate", timeout=freshness_timeout_sec)
        _append_step_status(cycle, results, freshness_gate_step)
        if freshness_gate_step.get("ok"):
            surge_step = _run(surge_cmd, "surge_detector", timeout=60.0)
            _append_step_status(cycle, results, surge_step)
            surge_ran = (
                bool(surge_step.get("ok"))
                and not bool(surge_step.get("advisory_only"))
                and not bool(surge_step.get("skipped"))
            )
            if surge_ran:
                _append_step_status(
                    cycle,
                    results,
                    _run(surge_sanity_cmd, "surge_sanity_labeler", timeout=_surge_sanity_timeout_sec),
                )
                _attack_candidate_tracker_step = _run(
                    surge_attack_candidate_tracker_cmd,
                    "surge_attack_candidate_tracker",
                    timeout=30.0,
                )
                if not _attack_candidate_tracker_step.get("ok"):
                    _attack_candidate_tracker_step["ok"] = True
                    _attack_candidate_tracker_step["advisory_only"] = True
                    _attack_candidate_tracker_step["fallback_reason"] = "surge_attack_candidate_tracker_non_blocking"
                _append_step_status(cycle, results, _attack_candidate_tracker_step)
                _active_response_layer_step = _run(
                    surge_active_response_layer_cmd,
                    "surge_active_response_layer",
                    timeout=30.0,
                )
                _active_response_layer_ok = bool(_active_response_layer_step.get("ok"))
                if not _active_response_layer_ok:
                    _active_response_layer_step["ok"] = True
                    _active_response_layer_step["advisory_only"] = True
                    _active_response_layer_step["fallback_reason"] = "surge_active_response_layer_non_blocking"
                _append_step_status(cycle, results, _active_response_layer_step)
                if _active_response_layer_ok:
                    _active_response_queue_step = _run(
                        surge_active_response_queue_cmd,
                        "surge_active_response_queue",
                        timeout=30.0,
                    )
                    if not _active_response_queue_step.get("ok"):
                        _active_response_queue_step["ok"] = True
                        _active_response_queue_step["advisory_only"] = True
                        _active_response_queue_step["fallback_reason"] = "surge_active_response_queue_non_blocking"
                    _append_step_status(cycle, results, _active_response_queue_step)
                    _live_readiness_audit_step = _run(
                        surge_live_readiness_audit_cmd,
                        "surge_live_readiness_audit",
                        timeout=30.0,
                    )
                    if not _live_readiness_audit_step.get("ok"):
                        _live_readiness_audit_step["ok"] = True
                        _live_readiness_audit_step["advisory_only"] = True
                        _live_readiness_audit_step["fallback_reason"] = "surge_live_readiness_audit_non_blocking"
                    _append_step_status(cycle, results, _live_readiness_audit_step)
                else:
                    _append_step_status(cycle, results, {
                        "label": "surge_active_response_queue",
                        "ok": True,
                        "skipped": True,
                        "advisory_only": True,
                        "skip_reason": "active_response_layer_failed",
                    })
                    _append_step_status(cycle, results, {
                        "label": "surge_live_readiness_audit",
                        "ok": True,
                        "skipped": True,
                        "advisory_only": True,
                        "skip_reason": "active_response_layer_failed",
                    })
                _wait_lob_every_n = _env_int("WAIT_LOB_HOGA_OBSERVE_EVERY_N", 1)
                if not (cycle == 1 or once or _run_on_cycle(cycle, _wait_lob_every_n)):
                    _append_step_status(cycle, results, _cadence_skip("surge_wait_lob_hoga_observe", _wait_lob_every_n))
                    _entry_change_exception_step = _run(
                        entry_change_exception_cmd,
                        "surge_entry_change_exception_candidates",
                        timeout=30.0,
                    )
                    if not _entry_change_exception_step.get("ok"):
                        _entry_change_exception_step["ok"] = True
                        _entry_change_exception_step["advisory_only"] = True
                        _entry_change_exception_step["fallback_reason"] = "surge_entry_change_exception_candidates_non_blocking"
                    _append_step_status(cycle, results, _entry_change_exception_step)
                    _ev_probe_step = _run(surge_ev_shadow_cmd, "surge_ev_shadow_candidates", timeout=30.0)
                    if not _ev_probe_step.get("ok"):
                        _ev_probe_step["ok"] = True
                        _ev_probe_step["advisory_only"] = True
                        _ev_probe_step["fallback_reason"] = "surge_ev_shadow_candidates_non_blocking"
                    _append_step_status(cycle, results, _ev_probe_step)
                    _ev_source_price_step = _run(
                        surge_ev_source_price_cmd,
                        "surge_ev_source_price_observation",
                        timeout=_surge_ev_source_price_timeout_sec,
                    )
                    if not _ev_source_price_step.get("ok"):
                        _ev_source_price_step["ok"] = True
                        _ev_source_price_step["advisory_only"] = True
                        _ev_source_price_step["fallback_reason"] = "surge_ev_source_price_observation_non_blocking"
                    _append_step_status(cycle, results, _ev_source_price_step)
                    _ev_sim_step = _run(surge_ev_shadow_sim_cmd, "surge_ev_shadow_simulation", timeout=30.0)
                    if not _ev_sim_step.get("ok"):
                        _ev_sim_step["ok"] = True
                        _ev_sim_step["advisory_only"] = True
                        _ev_sim_step["fallback_reason"] = "surge_ev_shadow_simulation_non_blocking"
                    _append_step_status(cycle, results, _ev_sim_step)
                    _ev_gradebook_step = _run(surge_ev_shadow_gradebook_cmd, "surge_ev_shadow_gradebook", timeout=30.0)
                    if not _ev_gradebook_step.get("ok"):
                        _ev_gradebook_step["ok"] = True
                        _ev_gradebook_step["advisory_only"] = True
                        _ev_gradebook_step["fallback_reason"] = "surge_ev_shadow_gradebook_non_blocking"
                    _append_step_status(cycle, results, _ev_gradebook_step)
                    _ev_bucket_decomp_step = _run(surge_ev_bucket_decomp_cmd, "surge_ev_bucket_decomposition", timeout=30.0)
                    if not _ev_bucket_decomp_step.get("ok"):
                        _ev_bucket_decomp_step["ok"] = True
                        _ev_bucket_decomp_step["advisory_only"] = True
                        _ev_bucket_decomp_step["fallback_reason"] = "surge_ev_bucket_decomposition_non_blocking"
                    _append_step_status(cycle, results, _ev_bucket_decomp_step)
                    _ev_conditional_verdict_step = _run(surge_ev_conditional_verdict_cmd, "surge_ev_conditional_verdict", timeout=30.0)
                    if not _ev_conditional_verdict_step.get("ok"):
                        _ev_conditional_verdict_step["ok"] = True
                        _ev_conditional_verdict_step["advisory_only"] = True
                        _ev_conditional_verdict_step["fallback_reason"] = "surge_ev_conditional_verdict_non_blocking"
                    _append_step_status(cycle, results, _ev_conditional_verdict_step)
                    _ev_probe_readiness_step = _run(surge_ev_probe_readiness_cmd, "surge_ev_probe_readiness", timeout=30.0)
                    if not _ev_probe_readiness_step.get("ok"):
                        _ev_probe_readiness_step["ok"] = True
                        _ev_probe_readiness_step["advisory_only"] = True
                        _ev_probe_readiness_step["fallback_reason"] = "surge_ev_probe_readiness_non_blocking"
                    _append_step_status(cycle, results, _ev_probe_readiness_step)
                    _ev_paper_probe_staged_step = _run(surge_ev_paper_probe_staged_cmd, "surge_ev_paper_probe_staged", timeout=30.0)
                    if not _ev_paper_probe_staged_step.get("ok"):
                        _ev_paper_probe_staged_step["ok"] = True
                        _ev_paper_probe_staged_step["advisory_only"] = True
                        _ev_paper_probe_staged_step["fallback_reason"] = "surge_ev_paper_probe_staged_non_blocking"
                    _append_step_status(cycle, results, _ev_paper_probe_staged_step)
                    _ev_paper_probe_consumer_step = _run(surge_ev_paper_probe_consumer_cmd, "surge_ev_paper_probe_consumer", timeout=30.0)
                    if not _ev_paper_probe_consumer_step.get("ok"):
                        _ev_paper_probe_consumer_step["ok"] = True
                        _ev_paper_probe_consumer_step["advisory_only"] = True
                        _ev_paper_probe_consumer_step["fallback_reason"] = "surge_ev_paper_probe_consumer_non_blocking"
                    _append_step_status(cycle, results, _ev_paper_probe_consumer_step)
                    for _label, _cmd in [
                        ("surge_lob_quality_diagnostic", surge_lob_quality_diag_cmd),
                        ("surge_entry_cap_decomposition", surge_entry_cap_decomp_cmd),
                        ("surge_virtual_probe_safety_audit", surge_virtual_probe_audit_cmd),
                        ("surge_outcome_promotion_report", surge_outcome_promotion_cmd),
                        ("microstructure_observe_contract", microstructure_observe_cmd),
                        ("surge_freshness_audit", surge_freshness_audit_cmd),
                    ]:
                        _diag_step = _run(_cmd, _label, timeout=30.0)
                        if not _diag_step.get("ok"):
                            _diag_step["ok"] = True
                            _diag_step["advisory_only"] = True
                            _diag_step["fallback_reason"] = f"{_label}_non_blocking"
                        _append_step_status(cycle, results, _diag_step)
                    _ev_reject_diag_step = _run(surge_ev_reject_diag_cmd, "surge_ev_shadow_reject_diagnostic", timeout=30.0)
                    if not _ev_reject_diag_step.get("ok"):
                        _ev_reject_diag_step["ok"] = True
                        _ev_reject_diag_step["advisory_only"] = True
                        _ev_reject_diag_step["fallback_reason"] = "surge_ev_shadow_reject_diagnostic_non_blocking"
                    _append_step_status(cycle, results, _ev_reject_diag_step)
                    _state_chain_every_n = _env_int("SURGE_STATE_RESEARCH_CHAIN_EVERY_N", 3)
                    _state_budget_sec = _surge_research_budget_sec()
                    if not (cycle == 1 or once or _run_on_cycle(cycle, _state_chain_every_n)):
                        for _label in [
                            "surge_state_machine_shadow",
                            "surge_state_transition_history",
                            "surge_state_transition_outcome",
                        ]:
                            _append_step_status(cycle, results, _cadence_skip(_label, _state_chain_every_n))
                        _append_surge_probe_review_chain()
                    elif (not once) and _cycle_elapsed_sec() > _state_budget_sec:
                        _append_step_status(cycle, results, {
                            "label": "surge_state_research_chain",
                            "ok": True,
                            "skipped": True,
                            "advisory_only": True,
                            "skip_reason": "cycle_elapsed_budget_exceeded",
                            "cycle_elapsed_sec": round(_cycle_elapsed_sec(), 1),
                            "budget_sec": _state_budget_sec,
                        })
                        _append_surge_probe_review_chain()
                    else:
                        _state_machine_step = _run(surge_state_machine_shadow_cmd, "surge_state_machine_shadow", timeout=30.0)
                        if not _state_machine_step.get("ok"):
                            _state_machine_step["ok"] = True
                            _state_machine_step["advisory_only"] = True
                            _state_machine_step["fallback_reason"] = "surge_state_machine_shadow_non_blocking"
                        _append_step_status(cycle, results, _state_machine_step)
                        if _state_machine_step.get("ok"):
                            _transition_history_step = _run(surge_state_transition_history_cmd, "surge_state_transition_history", timeout=30.0)
                            if not _transition_history_step.get("ok"):
                                _transition_history_step["ok"] = True
                                _transition_history_step["advisory_only"] = True
                                _transition_history_step["fallback_reason"] = "surge_state_transition_history_non_blocking"
                            _append_step_status(cycle, results, _transition_history_step)
                            _transition_outcome_step = _run(surge_state_transition_outcome_cmd, "surge_state_transition_outcome", timeout=30.0)
                            if not _transition_outcome_step.get("ok"):
                                _transition_outcome_step["ok"] = True
                                _transition_outcome_step["advisory_only"] = True
                                _transition_outcome_step["fallback_reason"] = "surge_state_transition_outcome_non_blocking"
                            _append_step_status(cycle, results, _transition_outcome_step)
                            _append_surge_probe_review_chain()
                else:
                    _wait_lob_env = {
                        "KIS_MOCK": str(os.environ.get("WAIT_LOB_HOGA_KIS_MOCK", os.environ.get("KIS_MOCK", "1"))),
                        "SURGE_LOB_HOGA_MAX_FETCH": str(os.environ.get("WAIT_LOB_HOGA_MAX_FETCH", "30")),
                        "SURGE_LOB_HOGA_SOFT_TIMEOUT_SEC": str(os.environ.get("WAIT_LOB_HOGA_SOFT_TIMEOUT_SEC", "90")),
                        "SURGE_LOB_KIS_SLEEP_SEC": str(os.environ.get("WAIT_LOB_HOGA_SLEEP_SEC", "0.6")),
                        "SURGE_LOB_KIS_RATE_LIMIT_RETRIES": str(os.environ.get("WAIT_LOB_HOGA_RATE_LIMIT_RETRIES", "2")),
                    }
                    _wait_lob_timeout_sec = max(
                        5.0,
                        float(str(os.environ.get("WAIT_LOB_HOGA_OBSERVE_TIMEOUT_SEC", "150")).strip() or "150"),
                    )
                    _wait_lob_step = _run(
                        wait_lob_hoga_cmd,
                        "surge_wait_lob_hoga_observe",
                        env=_wait_lob_env,
                        timeout=_wait_lob_timeout_sec,
                    )
                    if not _wait_lob_step.get("ok"):
                        _wait_lob_step["ok"] = True
                        _wait_lob_step["advisory_only"] = True
                        _wait_lob_step["fallback_reason"] = "wait_lob_hoga_observe_non_blocking"
                    _append_step_status(cycle, results, _wait_lob_step)
                    _entry_change_exception_step = _run(
                        entry_change_exception_cmd,
                        "surge_entry_change_exception_candidates",
                        timeout=30.0,
                    )
                    if not _entry_change_exception_step.get("ok"):
                        _entry_change_exception_step["ok"] = True
                        _entry_change_exception_step["advisory_only"] = True
                        _entry_change_exception_step["fallback_reason"] = "surge_entry_change_exception_candidates_non_blocking"
                    _append_step_status(cycle, results, _entry_change_exception_step)
                    _ev_probe_step = _run(surge_ev_shadow_cmd, "surge_ev_shadow_candidates", timeout=30.0)
                    if not _ev_probe_step.get("ok"):
                        _ev_probe_step["ok"] = True
                        _ev_probe_step["advisory_only"] = True
                        _ev_probe_step["fallback_reason"] = "surge_ev_shadow_candidates_non_blocking"
                    _append_step_status(cycle, results, _ev_probe_step)
                    _ev_source_price_step = _run(
                        surge_ev_source_price_cmd,
                        "surge_ev_source_price_observation",
                        timeout=_surge_ev_source_price_timeout_sec,
                    )
                    if not _ev_source_price_step.get("ok"):
                        _ev_source_price_step["ok"] = True
                        _ev_source_price_step["advisory_only"] = True
                        _ev_source_price_step["fallback_reason"] = "surge_ev_source_price_observation_non_blocking"
                    _append_step_status(cycle, results, _ev_source_price_step)
                    _ev_sim_step = _run(surge_ev_shadow_sim_cmd, "surge_ev_shadow_simulation", timeout=30.0)
                    if not _ev_sim_step.get("ok"):
                        _ev_sim_step["ok"] = True
                        _ev_sim_step["advisory_only"] = True
                        _ev_sim_step["fallback_reason"] = "surge_ev_shadow_simulation_non_blocking"
                    _append_step_status(cycle, results, _ev_sim_step)
                    _ev_gradebook_step = _run(surge_ev_shadow_gradebook_cmd, "surge_ev_shadow_gradebook", timeout=30.0)
                    if not _ev_gradebook_step.get("ok"):
                        _ev_gradebook_step["ok"] = True
                        _ev_gradebook_step["advisory_only"] = True
                        _ev_gradebook_step["fallback_reason"] = "surge_ev_shadow_gradebook_non_blocking"
                    _append_step_status(cycle, results, _ev_gradebook_step)
                    _ev_bucket_decomp_step = _run(surge_ev_bucket_decomp_cmd, "surge_ev_bucket_decomposition", timeout=30.0)
                    if not _ev_bucket_decomp_step.get("ok"):
                        _ev_bucket_decomp_step["ok"] = True
                        _ev_bucket_decomp_step["advisory_only"] = True
                        _ev_bucket_decomp_step["fallback_reason"] = "surge_ev_bucket_decomposition_non_blocking"
                    _append_step_status(cycle, results, _ev_bucket_decomp_step)
                    _ev_conditional_verdict_step = _run(surge_ev_conditional_verdict_cmd, "surge_ev_conditional_verdict", timeout=30.0)
                    if not _ev_conditional_verdict_step.get("ok"):
                        _ev_conditional_verdict_step["ok"] = True
                        _ev_conditional_verdict_step["advisory_only"] = True
                        _ev_conditional_verdict_step["fallback_reason"] = "surge_ev_conditional_verdict_non_blocking"
                    _append_step_status(cycle, results, _ev_conditional_verdict_step)
                    _ev_probe_readiness_step = _run(surge_ev_probe_readiness_cmd, "surge_ev_probe_readiness", timeout=30.0)
                    if not _ev_probe_readiness_step.get("ok"):
                        _ev_probe_readiness_step["ok"] = True
                        _ev_probe_readiness_step["advisory_only"] = True
                        _ev_probe_readiness_step["fallback_reason"] = "surge_ev_probe_readiness_non_blocking"
                    _append_step_status(cycle, results, _ev_probe_readiness_step)
                    _ev_paper_probe_staged_step = _run(surge_ev_paper_probe_staged_cmd, "surge_ev_paper_probe_staged", timeout=30.0)
                    if not _ev_paper_probe_staged_step.get("ok"):
                        _ev_paper_probe_staged_step["ok"] = True
                        _ev_paper_probe_staged_step["advisory_only"] = True
                        _ev_paper_probe_staged_step["fallback_reason"] = "surge_ev_paper_probe_staged_non_blocking"
                    _append_step_status(cycle, results, _ev_paper_probe_staged_step)
                    _ev_paper_probe_consumer_step = _run(surge_ev_paper_probe_consumer_cmd, "surge_ev_paper_probe_consumer", timeout=30.0)
                    if not _ev_paper_probe_consumer_step.get("ok"):
                        _ev_paper_probe_consumer_step["ok"] = True
                        _ev_paper_probe_consumer_step["advisory_only"] = True
                        _ev_paper_probe_consumer_step["fallback_reason"] = "surge_ev_paper_probe_consumer_non_blocking"
                    _append_step_status(cycle, results, _ev_paper_probe_consumer_step)
                    for _label, _cmd in [
                        ("surge_lob_quality_diagnostic", surge_lob_quality_diag_cmd),
                        ("surge_entry_cap_decomposition", surge_entry_cap_decomp_cmd),
                        ("surge_virtual_probe_safety_audit", surge_virtual_probe_audit_cmd),
                        ("surge_outcome_promotion_report", surge_outcome_promotion_cmd),
                        ("microstructure_observe_contract", microstructure_observe_cmd),
                        ("surge_freshness_audit", surge_freshness_audit_cmd),
                    ]:
                        _diag_step = _run(_cmd, _label, timeout=30.0)
                        if not _diag_step.get("ok"):
                            _diag_step["ok"] = True
                            _diag_step["advisory_only"] = True
                            _diag_step["fallback_reason"] = f"{_label}_non_blocking"
                        _append_step_status(cycle, results, _diag_step)
                    _ev_reject_diag_step = _run(surge_ev_reject_diag_cmd, "surge_ev_shadow_reject_diagnostic", timeout=30.0)
                    if not _ev_reject_diag_step.get("ok"):
                        _ev_reject_diag_step["ok"] = True
                        _ev_reject_diag_step["advisory_only"] = True
                        _ev_reject_diag_step["fallback_reason"] = "surge_ev_shadow_reject_diagnostic_non_blocking"
                    _append_step_status(cycle, results, _ev_reject_diag_step)
                    _state_chain_every_n = _env_int("SURGE_STATE_RESEARCH_CHAIN_EVERY_N", 3)
                    _state_budget_sec = _surge_research_budget_sec()
                    if not (cycle == 1 or once or _run_on_cycle(cycle, _state_chain_every_n)):
                        for _label in [
                            "surge_state_machine_shadow",
                            "surge_state_transition_history",
                            "surge_state_transition_outcome",
                        ]:
                            _append_step_status(cycle, results, _cadence_skip(_label, _state_chain_every_n))
                        _append_surge_probe_review_chain()
                    elif (not once) and _cycle_elapsed_sec() > _state_budget_sec:
                        _append_step_status(cycle, results, {
                            "label": "surge_state_research_chain",
                            "ok": True,
                            "skipped": True,
                            "advisory_only": True,
                            "skip_reason": "cycle_elapsed_budget_exceeded",
                            "cycle_elapsed_sec": round(_cycle_elapsed_sec(), 1),
                            "budget_sec": _state_budget_sec,
                        })
                        _append_surge_probe_review_chain()
                    else:
                        _state_machine_step = _run(surge_state_machine_shadow_cmd, "surge_state_machine_shadow", timeout=30.0)
                        if not _state_machine_step.get("ok"):
                            _state_machine_step["ok"] = True
                            _state_machine_step["advisory_only"] = True
                            _state_machine_step["fallback_reason"] = "surge_state_machine_shadow_non_blocking"
                        _append_step_status(cycle, results, _state_machine_step)
                        if _state_machine_step.get("ok"):
                            _transition_history_step = _run(surge_state_transition_history_cmd, "surge_state_transition_history", timeout=30.0)
                            if not _transition_history_step.get("ok"):
                                _transition_history_step["ok"] = True
                                _transition_history_step["advisory_only"] = True
                                _transition_history_step["fallback_reason"] = "surge_state_transition_history_non_blocking"
                            _append_step_status(cycle, results, _transition_history_step)
                            _transition_outcome_step = _run(surge_state_transition_outcome_cmd, "surge_state_transition_outcome", timeout=30.0)
                            if not _transition_outcome_step.get("ok"):
                                _transition_outcome_step["ok"] = True
                                _transition_outcome_step["advisory_only"] = True
                                _transition_outcome_step["fallback_reason"] = "surge_state_transition_outcome_non_blocking"
                            _append_step_status(cycle, results, _transition_outcome_step)
                            _append_surge_probe_review_chain()
                dashboard_state_cmd = [py, str(ROOTB_DIR / "tools" / "build_dashboard_state_v2.py")]
                _dashboard_realtime_every_n = _env_int("INTRADAY_DASHBOARD_REALTIME_EVERY_N", 1)
                if not (cycle == 1 or once or _run_on_cycle(cycle, _dashboard_realtime_every_n)):
                    _append_step_status(cycle, results, _cadence_skip("build_dashboard_state_v2_realtime", _dashboard_realtime_every_n))
                else:
                    dashboard_step = _run(dashboard_state_cmd, "build_dashboard_state_v2_realtime", timeout=120.0)
                    _append_step_status(cycle, results, dashboard_step)
                    dashboard_state_built = bool(dashboard_step.get("ok"))
            else:
                for _label in [
                    "surge_sanity_labeler",
                    "surge_attack_candidate_tracker",
                    "surge_active_response_layer",
                    "surge_active_response_queue",
                    "surge_live_readiness_audit",
                    "surge_wait_lob_hoga_observe",
                ]:
                    _append_step_status(cycle, results, {
                        "label": _label,
                        "ok": True,
                        "skipped": True,
                        "fail_closed": True,
                        "blocked_by": "surge_detector",
                        "upstream_returncode": surge_step.get("returncode"),
                        "upstream_status": surge_step.get("surge_detector_status"),
                        "upstream_reason": surge_step.get("surge_detector_reason") or surge_step.get("fallback_reason"),
                    })
        else:
            _append_step_status(cycle, results, {
                "label": "surge_detector",
                "ok": True,
                "skipped": True,
                "fail_closed": True,
                "blocked_by": "surge_freshness_gate",
                "upstream_returncode": freshness_gate_step.get("returncode"),
            })
            _append_step_status(cycle, results, {
                "label": "surge_sanity_labeler",
                "ok": True,
                "skipped": True,
                "fail_closed": True,
                "blocked_by": "surge_freshness_gate",
                "upstream_returncode": freshness_gate_step.get("returncode"),
            })
            _append_step_status(cycle, results, {
                "label": "surge_attack_candidate_tracker",
                "ok": True,
                "skipped": True,
                "fail_closed": True,
                "blocked_by": "surge_freshness_gate",
                "upstream_returncode": freshness_gate_step.get("returncode"),
            })
            _append_step_status(cycle, results, {
                "label": "surge_wait_lob_hoga_observe",
                "ok": True,
                "skipped": True,
                "fail_closed": True,
                "blocked_by": "surge_freshness_gate",
                "upstream_returncode": freshness_gate_step.get("returncode"),
            })

    surge_followthrough_cmd = [
        py,
        str(TOOLS_DIR / "validate_surge_followthrough.py"),
        "--date",
        dt.datetime.now(tz=KST).strftime("%Y%m%d"),
        "--min-age-minutes",
        "30",
    ]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_followthrough_cmd))
        _append_step_status(cycle, results, {"label": "surge_followthrough_validation", "ok": True, "dry_run": True})
    elif str(os.environ.get("SURGE_FOLLOWTHROUGH_VALIDATION_SKIP", "0")).strip().lower() in {"1", "true", "yes", "on"}:
        _append_step_status(cycle, results, {
            "label": "surge_followthrough_validation",
            "ok": True,
            "skipped": True,
            "skip_reason": "SURGE_FOLLOWTHROUGH_VALIDATION_SKIP",
        })
    elif not surge_ran:
        _append_step_status(cycle, results, {
            "label": "surge_followthrough_validation",
            "ok": True,
            "skipped": True,
            "blocked_by": "surge_detector",
        })
    elif not (once or _run_on_cycle(cycle, _env_int("SURGE_FOLLOWTHROUGH_VALIDATION_EVERY_N", 1))):
        _append_step_status(
            cycle,
            results,
            _cadence_skip("surge_followthrough_validation", _env_int("SURGE_FOLLOWTHROUGH_VALIDATION_EVERY_N", 1)),
        )
    else:
        surge_followthrough_step = _run(surge_followthrough_cmd, "surge_followthrough_validation", timeout=90.0)
        if not surge_followthrough_step.get("ok"):
            surge_followthrough_step["ok"] = True
            surge_followthrough_step["advisory_only"] = True
            surge_followthrough_step["fallback_reason"] = "surge_followthrough_validation_non_blocking"
        _append_step_status(cycle, results, surge_followthrough_step)

    _news_mode = str(os.environ.get("NEWS_COLLECT_MODE", "production"))
    _news_mode_env = {"NEWS_COLLECT_MODE": _news_mode}

    news_candidates_cmd = [py, str(TOOLS_DIR / "news_candidates_daily.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(news_candidates_cmd))
        _append_step_status(cycle, results, {"label": "news_candidates_daily", "ok": True, "dry_run": True})
    elif not _run_on_cycle(cycle, _env_int("NEWS_CANDIDATES_EVERY_N", 5)):
        _append_step_status(cycle, results, _cadence_skip("news_candidates_daily", _env_int("NEWS_CANDIDATES_EVERY_N", 5)))
    else:
        _append_step_status(
            cycle,
            results,
            _run(news_candidates_cmd, "news_candidates_daily", env=_news_mode_env, timeout=60.0),
        )

    final_merge_cmd = [py, str(TOOLS_DIR / "final_score_merge_daily.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(final_merge_cmd))
        _append_step_status(cycle, results, {"label": "final_score_merge_daily", "ok": True, "dry_run": True})
    elif not _run_on_cycle(cycle, _env_int("FINAL_SCORE_MERGE_EVERY_N", 5)):
        _append_step_status(cycle, results, _cadence_skip("final_score_merge_daily", _env_int("FINAL_SCORE_MERGE_EVERY_N", 5)))
    else:
        _append_step_status(
            cycle,
            results,
            _run(final_merge_cmd, "final_score_merge_daily", env=_news_mode_env, timeout=60.0),
        )

    signal_integration_cmd = [py, str(TOOLS_DIR / "signal_integration_daily.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(signal_integration_cmd))
        _append_step_status(cycle, results, {"label": "signal_integration_daily", "ok": True, "dry_run": True})
    elif not _run_on_cycle(cycle, _env_int("SIGNAL_INTEGRATION_EVERY_N", 5)):
        _append_step_status(cycle, results, _cadence_skip("signal_integration_daily", _env_int("SIGNAL_INTEGRATION_EVERY_N", 5)))
    else:
        _append_step_status(cycle, results, _run(signal_integration_cmd, "signal_integration_daily", timeout=60.0))

    integrated_ops_cmd = [py, str(TOOLS_DIR / "build_integrated_ops_snapshot.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(integrated_ops_cmd))
        _append_step_status(cycle, results, {"label": "build_integrated_ops_snapshot", "ok": True, "dry_run": True})
    elif not _run_on_cycle(cycle, _env_int("INTEGRATED_OPS_SNAPSHOT_EVERY_N", 5)):
        _append_step_status(cycle, results, _cadence_skip("build_integrated_ops_snapshot", _env_int("INTEGRATED_OPS_SNAPSHOT_EVERY_N", 5)))
    else:
        _append_step_status(cycle, results, _run(integrated_ops_cmd, "build_integrated_ops_snapshot", timeout=60.0))

    # Validation-only: collect realtime follow-through observation samples.
    # This step never creates orders and is fail-soft so it cannot block the intraday loop.
    followthrough_cmd = [py, str(TOOLS_DIR / "followthrough_realtime.py")]
    if str(os.environ.get("FOLLOWTHROUGH_RT_SKIP", "0")).strip().lower() in {"1", "true", "yes", "on"}:
        _append_step_status(cycle, results, {"label": "followthrough_realtime_validation", "ok": True, "skipped": True})
    elif dry_run:
        logger.info("[DRY] would run: %s", " ".join(followthrough_cmd))
        _append_step_status(cycle, results, {"label": "followthrough_realtime_validation", "ok": True, "dry_run": True})
    else:
        followthrough_step = _run(followthrough_cmd, "followthrough_realtime_validation", timeout=60.0)
        if not followthrough_step.get("ok"):
            followthrough_step["ok"] = True
            followthrough_step["advisory_only"] = True
            followthrough_step["fallback_reason"] = "followthrough_validation_non_blocking"
        _append_step_status(cycle, results, followthrough_step)

    promoted_recheck_cmd = [py, str(TOOLS_DIR / "build_promoted_recheck_candidates.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(promoted_recheck_cmd))
        _append_step_status(cycle, results, {"label": "promoted_recheck_candidates", "ok": True, "dry_run": True})
    else:
        promoted_recheck_step = _run(promoted_recheck_cmd, "promoted_recheck_candidates", timeout=30.0)
        if not promoted_recheck_step.get("ok"):
            promoted_recheck_step["ok"] = True
            promoted_recheck_step["advisory_only"] = True
            promoted_recheck_step["fallback_reason"] = "promoted_recheck_candidates_non_blocking"
        _append_step_status(cycle, results, promoted_recheck_step)

    # [2026-08-21] 진입 판정 직전 LOB 갱신 (순서 보장).
    #
    # 진입 판정(`paper_engine/entry.py:_normal_entry_execution_quality_decision`)은
    # `2_Logs/surge_lob_latest.csv` 를 읽으면서 그 파일의 나이를 검사하지 않는다.
    # 위쪽 `surge_lob_ingest` 스텝은 `SURGE_LOB_INGEST_EVERY_N` 주기로만 돌기 때문에
    # "LOB 갱신 -> 진입 판정" 순서가 보장되지 않는다.
    # 2026-08-21 09:09 판정은 전날 22:04(장 마감 후) 스냅샷을 읽었고, 그 파일은 전 행이
    # NO_LOB 이라 NORMAL 후보 3건 중 2건이 NORMAL_LOB_UNAVAILABLE 로 막혔다.
    # 같은 사이클이 09:22 에 갱신하자 같은 종목들이 전부 lob_status=OK 였다.
    # 상세: .agent/PLANS.md 2026-08-21 (4)
    #
    # 실패해도 여기서 사이클을 멈추지 않는다. 진입 게이트가 NO_LOB 를 fail-closed 로
    # 막으므로, 갱신 실패는 "묵은 파일로 판정 -> 차단"이라는 기존 동작으로 되돌아갈 뿐이다.
    # 다만 결과를 ok=True 로 덮지 않는다. 실패가 드러나야 한다.
    pre_entry_lob_cmd = [py, str(TOOLS_DIR / "surge_lob_ingest.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(pre_entry_lob_cmd))
        _append_step_status(cycle, results, {"label": "pre_entry_lob_refresh", "ok": True, "dry_run": True})
    elif not _env_bool("PRE_ENTRY_LOB_REFRESH", True):
        _append_step_status(
            cycle,
            results,
            {"label": "pre_entry_lob_refresh", "ok": True, "skipped": True, "skip_reason": "disabled_by_env"},
        )
    else:
        _pre_lob_timeout_sec = max(
            10.0,
            float(str(os.environ.get("PRE_ENTRY_LOB_REFRESH_TIMEOUT_SEC", "30")).strip() or "30"),
        )
        _pre_lob_env = {"SURGE_LOB_HOGA_MAX_FETCH": str(os.environ.get("SURGE_LOB_HOGA_MAX_FETCH", "5"))}
        _pre_lob_step = _run(pre_entry_lob_cmd, "pre_entry_lob_refresh", env=_pre_lob_env, timeout=_pre_lob_timeout_sec)
        try:
            _pre_lob_csv = LOG_DIR / "surge_lob_latest.csv"
            if _pre_lob_csv.exists():
                _pre_lob_step["lob_age_sec"] = round(time.time() - _pre_lob_csv.stat().st_mtime, 1)
        except Exception:
            pass
        _append_step_status(cycle, results, _pre_lob_step)

    # Step 3: run paper_engine with intraday price input.
    engine_env = {}
    if not dry_run and INTRADAY_PRICES_CSV.exists():
        engine_env["PAPER_INTRADAY_PRICE_PATH"] = str(INTRADAY_PRICES_CSV)
        engine_env["PAPER_INTRADAY_REALTIME_MODE"] = "1"

    engine_cmd = [py, str(ROOT / "paper_engine.py")]
    if dry_run:
        logger.info("[DRY] would run: %s (env PAPER_INTRADAY_PRICE_PATH=%s)",
                    " ".join(engine_cmd), engine_env.get("PAPER_INTRADAY_PRICE_PATH", ""))
        _append_step_status(cycle, results, {"label": "paper_engine", "ok": True, "dry_run": True})
    else:
        paper_engine_timeout_sec = float(str(os.environ.get("PAPER_ENGINE_TIMEOUT_SEC", "240")).strip() or "240")
        _append_step_status(cycle, results, _run(engine_cmd, "paper_engine", env=engine_env, timeout=paper_engine_timeout_sec))

        # Keep post-fill artifacts aligned during the live session, not only after close.
        sync_steps = [
            ([py, str(ROOT / "paper_pnl_report.py")], "intraday_pnl_report", 180.0),
            ([py, str(TOOLS_DIR / "repair_rootb_ledger_missing_live_fills.py"), "--start-ymd", str(os.environ.get("PAPER_OPER_START_YMD", "20260301")), "--apply"], "intraday_rootb_ledger_sync", 120.0),
        ]
        for sync_cmd, sync_label, sync_timeout in sync_steps:
            sync_step = _run(sync_cmd, sync_label, timeout=sync_timeout)
            if not sync_step.get("ok"):
                sync_step["ok"] = True
                sync_step["advisory_only"] = True
                sync_step["fallback_reason"] = f"{sync_label}_non_blocking"
            _append_step_status(cycle, results, sync_step)
        live_vs_bt_cmd, live_vs_bt_as_of = _build_live_vs_bt_align_cmd(py)
        if live_vs_bt_cmd is not None:
            live_vs_bt_step = _run(live_vs_bt_cmd, "intraday_live_vs_bt_align", timeout=90.0)
            live_vs_bt_step["as_of"] = live_vs_bt_as_of
            if not live_vs_bt_step.get("ok"):
                live_vs_bt_step["ok"] = True
                live_vs_bt_step["advisory_only"] = True
                live_vs_bt_step["fallback_reason"] = "intraday_live_vs_bt_align_non_blocking"
            _append_step_status(cycle, results, live_vs_bt_step)
    normal_bottleneck_cmd = [py, str(TOOLS_DIR / "build_normal_entry_path_bottleneck_audit.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(normal_bottleneck_cmd))
        _append_step_status(cycle, results, {"label": "normal_entry_path_bottleneck_audit", "ok": True, "dry_run": True})
    else:
        normal_bottleneck_step = _run(normal_bottleneck_cmd, "normal_entry_path_bottleneck_audit", timeout=30.0)
        if not normal_bottleneck_step.get("ok"):
            normal_bottleneck_step["ok"] = True
            normal_bottleneck_step["advisory_only"] = True
            normal_bottleneck_step["fallback_reason"] = "normal_entry_path_bottleneck_audit_non_blocking"
        _append_step_status(cycle, results, normal_bottleneck_step)

    surge_probe_intent_split_cmd = [py, str(TOOLS_DIR / "build_surge_probe_intent_split.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_probe_intent_split_cmd))
        _append_step_status(cycle, results, {"label": "surge_probe_intent_split", "ok": True, "dry_run": True})
    else:
        surge_probe_intent_split_step = _run(surge_probe_intent_split_cmd, "surge_probe_intent_split", timeout=30.0)
        if not surge_probe_intent_split_step.get("ok"):
            surge_probe_intent_split_step["ok"] = True
            surge_probe_intent_split_step["advisory_only"] = True
            surge_probe_intent_split_step["fallback_reason"] = "surge_probe_intent_split_non_blocking"
        _append_step_status(cycle, results, surge_probe_intent_split_step)

    candidate_queue_cmd = [py, str(TOOLS_DIR / "build_candidate_action_queue.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(candidate_queue_cmd))
        _append_step_status(cycle, results, {"label": "candidate_action_queue", "ok": True, "dry_run": True})
    else:
        candidate_queue_step = _run(candidate_queue_cmd, "candidate_action_queue", timeout=30.0)
        if not candidate_queue_step.get("ok"):
            candidate_queue_step["ok"] = True
            candidate_queue_step["advisory_only"] = True
            candidate_queue_step["fallback_reason"] = "candidate_action_queue_non_blocking"
        _append_step_status(cycle, results, candidate_queue_step)

    candidate_decision_ledger_cmd = [py, str(TOOLS_DIR / "build_candidate_decision_outcome_ledger.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(candidate_decision_ledger_cmd))
        _append_step_status(cycle, results, {"label": "candidate_decision_outcome_ledger", "ok": True, "dry_run": True})
    else:
        candidate_decision_ledger_step = _run(
            candidate_decision_ledger_cmd,
            "candidate_decision_outcome_ledger",
            timeout=180.0,
        )
        if not candidate_decision_ledger_step.get("ok"):
            candidate_decision_ledger_step["ok"] = True
            candidate_decision_ledger_step["advisory_only"] = True
            candidate_decision_ledger_step["fallback_reason"] = "candidate_decision_outcome_ledger_non_blocking"
        _append_step_status(cycle, results, candidate_decision_ledger_step)

    surge_path_validation_outcome_cmd = [py, str(TOOLS_DIR / "build_surge_path_validation_outcome.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_path_validation_outcome_cmd))
        _append_step_status(cycle, results, {"label": "surge_path_validation_outcome", "ok": True, "dry_run": True})
    else:
        surge_path_validation_outcome_step = _run(surge_path_validation_outcome_cmd, "surge_path_validation_outcome", timeout=30.0)
        if not surge_path_validation_outcome_step.get("ok"):
            surge_path_validation_outcome_step["ok"] = True
            surge_path_validation_outcome_step["advisory_only"] = True
            surge_path_validation_outcome_step["fallback_reason"] = "surge_path_validation_outcome_non_blocking"
        _append_step_status(cycle, results, surge_path_validation_outcome_step)

    surge_blocked_path_review_cmd = [py, str(TOOLS_DIR / "build_surge_blocked_path_observation_review.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_blocked_path_review_cmd))
        _append_step_status(cycle, results, {"label": "surge_blocked_path_observation_review", "ok": True, "dry_run": True})
    else:
        _blocked_path_review_every_n = _env_int("SURGE_BLOCKED_PATH_REVIEW_EVERY_N", 5)
        _blocked_path_budget_sec = _surge_research_budget_sec()
        if not (cycle == 1 or once or _run_on_cycle(cycle, _blocked_path_review_every_n)):
            _append_step_status(
                cycle,
                results,
                _cadence_skip("surge_blocked_path_observation_review", _blocked_path_review_every_n),
            )
        elif (not once) and _cycle_elapsed_sec() > _blocked_path_budget_sec:
            _append_step_status(cycle, results, {
                "label": "surge_blocked_path_observation_review",
                "ok": True,
                "skipped": True,
                "advisory_only": True,
                "skip_reason": "cycle_elapsed_budget_exceeded",
                "cycle_elapsed_sec": round(_cycle_elapsed_sec(), 1),
                "budget_sec": _blocked_path_budget_sec,
            })
        else:
            surge_blocked_path_review_step = _run(
                surge_blocked_path_review_cmd, "surge_blocked_path_observation_review", timeout=30.0
            )
            if not surge_blocked_path_review_step.get("ok"):
                surge_blocked_path_review_step["ok"] = True
                surge_blocked_path_review_step["advisory_only"] = True
                surge_blocked_path_review_step["fallback_reason"] = "surge_blocked_path_observation_review_non_blocking"
            _append_step_status(cycle, results, surge_blocked_path_review_step)

    surge_recovery_reentry_cmd = [py, str(TOOLS_DIR / "build_surge_recovery_reentry_candidates.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_recovery_reentry_cmd))
        _append_step_status(cycle, results, {"label": "surge_recovery_reentry_candidates", "ok": True, "dry_run": True})
    else:
        surge_recovery_reentry_step = _run(
            surge_recovery_reentry_cmd, "surge_recovery_reentry_candidates", timeout=30.0
        )
        if not surge_recovery_reentry_step.get("ok"):
            surge_recovery_reentry_step["ok"] = True
            surge_recovery_reentry_step["advisory_only"] = True
            surge_recovery_reentry_step["fallback_reason"] = "surge_recovery_reentry_candidates_non_blocking"
        _append_step_status(cycle, results, surge_recovery_reentry_step)

    surge_expectancy_evidence_gap_cmd = [py, str(TOOLS_DIR / "build_surge_expectancy_evidence_gap.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_expectancy_evidence_gap_cmd))
        _append_step_status(cycle, results, {"label": "surge_expectancy_evidence_gap", "ok": True, "dry_run": True})
    else:
        surge_expectancy_evidence_gap_step = _run(surge_expectancy_evidence_gap_cmd, "surge_expectancy_evidence_gap", timeout=30.0)
        if not surge_expectancy_evidence_gap_step.get("ok"):
            surge_expectancy_evidence_gap_step["ok"] = True
            surge_expectancy_evidence_gap_step["advisory_only"] = True
            surge_expectancy_evidence_gap_step["fallback_reason"] = "surge_expectancy_evidence_gap_non_blocking"
        _append_step_status(cycle, results, surge_expectancy_evidence_gap_step)

    candidate_followup_cmd = [py, str(TOOLS_DIR / "build_candidate_action_followup.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(candidate_followup_cmd))
        _append_step_status(cycle, results, {"label": "candidate_action_followup", "ok": True, "dry_run": True})
    else:
        candidate_followup_step = _run(candidate_followup_cmd, "candidate_action_followup", timeout=30.0)
        if not candidate_followup_step.get("ok"):
            candidate_followup_step["ok"] = True
            candidate_followup_step["advisory_only"] = True
            candidate_followup_step["fallback_reason"] = "candidate_action_followup_non_blocking"
        _append_step_status(cycle, results, candidate_followup_step)

    candidate_review_cmd = [py, str(TOOLS_DIR / "build_candidate_action_review.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(candidate_review_cmd))
        _append_step_status(cycle, results, {"label": "candidate_action_review", "ok": True, "dry_run": True})
    else:
        candidate_review_step = _run(candidate_review_cmd, "candidate_action_review", timeout=30.0)
        if not candidate_review_step.get("ok"):
            candidate_review_step["ok"] = True
            candidate_review_step["advisory_only"] = True
            candidate_review_step["fallback_reason"] = "candidate_action_review_non_blocking"
        _append_step_status(cycle, results, candidate_review_step)

    candidate_plan_cmd = [py, str(TOOLS_DIR / "build_candidate_action_plan.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(candidate_plan_cmd))
        _append_step_status(cycle, results, {"label": "candidate_action_plan", "ok": True, "dry_run": True})
    else:
        candidate_plan_step = _run(candidate_plan_cmd, "candidate_action_plan", timeout=30.0)
        if not candidate_plan_step.get("ok"):
            candidate_plan_step["ok"] = True
            candidate_plan_step["advisory_only"] = True
            candidate_plan_step["fallback_reason"] = "candidate_action_plan_non_blocking"
        _append_step_status(cycle, results, candidate_plan_step)

    no_lob_review_cmd = [py, str(TOOLS_DIR / "build_no_lob_recheck_review_report.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(no_lob_review_cmd))
        _append_step_status(cycle, results, {"label": "no_lob_recheck_review", "ok": True, "dry_run": True})
    else:
        no_lob_review_step = _run(no_lob_review_cmd, "no_lob_recheck_review", timeout=30.0)
        if not no_lob_review_step.get("ok"):
            no_lob_review_step["ok"] = True
            no_lob_review_step["advisory_only"] = True
            no_lob_review_step["fallback_reason"] = "no_lob_recheck_review_non_blocking"
        _append_step_status(cycle, results, no_lob_review_step)

    surge_shadow_probe_cmd = [py, str(TOOLS_DIR / "build_surge_shadow_probe_candidate_report.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_shadow_probe_cmd))
        _append_step_status(cycle, results, {"label": "surge_shadow_probe_candidate", "ok": True, "dry_run": True})
    else:
        surge_shadow_probe_step = _run(surge_shadow_probe_cmd, "surge_shadow_probe_candidate", timeout=30.0)
        if not surge_shadow_probe_step.get("ok"):
            surge_shadow_probe_step["ok"] = True
            surge_shadow_probe_step["advisory_only"] = True
            surge_shadow_probe_step["fallback_reason"] = "surge_shadow_probe_candidate_non_blocking"
        _append_step_status(cycle, results, surge_shadow_probe_step)

    surge_probe_policy_design_cmd = [py, str(TOOLS_DIR / "build_surge_probe_policy_design.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_probe_policy_design_cmd))
        _append_step_status(cycle, results, {"label": "surge_probe_policy_design", "ok": True, "dry_run": True})
    else:
        surge_probe_policy_design_step = _run(surge_probe_policy_design_cmd, "surge_probe_policy_design", timeout=30.0)
        if not surge_probe_policy_design_step.get("ok"):
            surge_probe_policy_design_step["ok"] = True
            surge_probe_policy_design_step["advisory_only"] = True
            surge_probe_policy_design_step["fallback_reason"] = "surge_probe_policy_design_non_blocking"
        _append_step_status(cycle, results, surge_probe_policy_design_step)

    surge_shadow_probe_markout_cmd = [py, str(TOOLS_DIR / "build_surge_shadow_probe_markout_tracker.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_shadow_probe_markout_cmd))
        _append_step_status(cycle, results, {"label": "surge_shadow_probe_markout", "ok": True, "dry_run": True})
    else:
        surge_shadow_probe_markout_step = _run(surge_shadow_probe_markout_cmd, "surge_shadow_probe_markout", timeout=30.0)
        if not surge_shadow_probe_markout_step.get("ok"):
            surge_shadow_probe_markout_step["ok"] = True
            surge_shadow_probe_markout_step["advisory_only"] = True
            surge_shadow_probe_markout_step["fallback_reason"] = "surge_shadow_probe_markout_non_blocking"
        _append_step_status(cycle, results, surge_shadow_probe_markout_step)

    surge_shadow_probe_summary_cmd = [py, str(TOOLS_DIR / "build_surge_shadow_probe_markout_summary.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_shadow_probe_summary_cmd))
        _append_step_status(cycle, results, {"label": "surge_shadow_probe_markout_summary", "ok": True, "dry_run": True})
    else:
        surge_shadow_probe_summary_step = _run(surge_shadow_probe_summary_cmd, "surge_shadow_probe_markout_summary", timeout=30.0)
        if not surge_shadow_probe_summary_step.get("ok"):
            surge_shadow_probe_summary_step["ok"] = True
            surge_shadow_probe_summary_step["advisory_only"] = True
            surge_shadow_probe_summary_step["fallback_reason"] = "surge_shadow_probe_markout_summary_non_blocking"
        _append_step_status(cycle, results, surge_shadow_probe_summary_step)

    surge_shadow_probe_timepoint_cmd = [py, str(TOOLS_DIR / "build_surge_shadow_probe_timepoint_markout.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_shadow_probe_timepoint_cmd))
        _append_step_status(cycle, results, {"label": "surge_shadow_probe_timepoint_markout", "ok": True, "dry_run": True})
    else:
        surge_shadow_probe_timepoint_step = _run(surge_shadow_probe_timepoint_cmd, "surge_shadow_probe_timepoint_markout", timeout=30.0)
        if not surge_shadow_probe_timepoint_step.get("ok"):
            surge_shadow_probe_timepoint_step["ok"] = True
            surge_shadow_probe_timepoint_step["advisory_only"] = True
            surge_shadow_probe_timepoint_step["fallback_reason"] = "surge_shadow_probe_timepoint_markout_non_blocking"
        _append_step_status(cycle, results, surge_shadow_probe_timepoint_step)

    surge_followthrough_kill_diag_cmd = [py, str(TOOLS_DIR / "build_surge_followthrough_kill_diagnostic.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_followthrough_kill_diag_cmd))
        _append_step_status(cycle, results, {"label": "surge_followthrough_kill_diagnostic", "ok": True, "dry_run": True})
    else:
        surge_followthrough_kill_diag_step = _run(surge_followthrough_kill_diag_cmd, "surge_followthrough_kill_diagnostic", timeout=30.0)
        if not surge_followthrough_kill_diag_step.get("ok"):
            surge_followthrough_kill_diag_step["ok"] = True
            surge_followthrough_kill_diag_step["advisory_only"] = True
            surge_followthrough_kill_diag_step["fallback_reason"] = "surge_followthrough_kill_diagnostic_non_blocking"
        _append_step_status(cycle, results, surge_followthrough_kill_diag_step)

    surge_probe_latency_source_diag_cmd = [py, str(TOOLS_DIR / "build_surge_probe_latency_source_diagnostic.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_probe_latency_source_diag_cmd))
        _append_step_status(cycle, results, {"label": "surge_probe_latency_source_diagnostic", "ok": True, "dry_run": True})
    else:
        surge_probe_latency_source_diag_step = _run(surge_probe_latency_source_diag_cmd, "surge_probe_latency_source_diagnostic", timeout=30.0)
        if not surge_probe_latency_source_diag_step.get("ok"):
            surge_probe_latency_source_diag_step["ok"] = True
            surge_probe_latency_source_diag_step["advisory_only"] = True
            surge_probe_latency_source_diag_step["fallback_reason"] = "surge_probe_latency_source_diagnostic_non_blocking"
        _append_step_status(cycle, results, surge_probe_latency_source_diag_step)

    lob_recheck_source_consistency_cmd = [py, str(TOOLS_DIR / "build_lob_recheck_source_consistency_audit.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(lob_recheck_source_consistency_cmd))
        _append_step_status(cycle, results, {"label": "lob_recheck_source_consistency_audit", "ok": True, "dry_run": True})
    else:
        lob_recheck_source_consistency_step = _run(lob_recheck_source_consistency_cmd, "lob_recheck_source_consistency_audit", timeout=30.0)
        if not lob_recheck_source_consistency_step.get("ok"):
            lob_recheck_source_consistency_step["ok"] = True
            lob_recheck_source_consistency_step["advisory_only"] = True
            lob_recheck_source_consistency_step["fallback_reason"] = "lob_recheck_source_consistency_audit_non_blocking"
        _append_step_status(cycle, results, lob_recheck_source_consistency_step)

    lob_ingest_priority_diag_cmd = [py, str(TOOLS_DIR / "build_lob_ingest_priority_diagnostic.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(lob_ingest_priority_diag_cmd))
        _append_step_status(cycle, results, {"label": "lob_ingest_priority_diagnostic", "ok": True, "dry_run": True})
    else:
        lob_ingest_priority_diag_step = _run(lob_ingest_priority_diag_cmd, "lob_ingest_priority_diagnostic", timeout=30.0)
        if not lob_ingest_priority_diag_step.get("ok"):
            lob_ingest_priority_diag_step["ok"] = True
            lob_ingest_priority_diag_step["advisory_only"] = True
            lob_ingest_priority_diag_step["fallback_reason"] = "lob_ingest_priority_diagnostic_non_blocking"
        _append_step_status(cycle, results, lob_ingest_priority_diag_step)

    shadow_promotion_cmd = [py, str(TOOLS_DIR / "build_shadow_promotion_report.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(shadow_promotion_cmd))
        _append_step_status(cycle, results, {"label": "shadow_promotion_report", "ok": True, "dry_run": True})
    else:
        shadow_step = _run(shadow_promotion_cmd, "shadow_promotion_report", timeout=30.0)
        if not shadow_step.get("ok"):
            shadow_step["ok"] = True
            shadow_step["advisory_only"] = True
            shadow_step["fallback_reason"] = "shadow_promotion_non_blocking"
        _append_step_status(cycle, results, shadow_step)

    candidate_recheck_cmd = [py, str(TOOLS_DIR / "build_candidate_recheck_queue.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(candidate_recheck_cmd))
        _append_step_status(cycle, results, {"label": "candidate_recheck_queue", "ok": True, "dry_run": True})
    else:
        candidate_recheck_step = _run(candidate_recheck_cmd, "candidate_recheck_queue", timeout=30.0)
        if not candidate_recheck_step.get("ok"):
            candidate_recheck_step["ok"] = True
            candidate_recheck_step["advisory_only"] = True
            candidate_recheck_step["fallback_reason"] = "candidate_recheck_queue_non_blocking"
        _append_step_status(cycle, results, candidate_recheck_step)

    post_entry_learning_cmd = [py, str(TOOLS_DIR / "build_post_entry_learning.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(post_entry_learning_cmd))
        _append_step_status(cycle, results, {"label": "post_entry_learning", "ok": True, "dry_run": True})
    else:
        post_entry_learning_step = _run(post_entry_learning_cmd, "post_entry_learning", timeout=30.0)
        if not post_entry_learning_step.get("ok"):
            post_entry_learning_step["ok"] = True
            post_entry_learning_step["advisory_only"] = True
            post_entry_learning_step["fallback_reason"] = "post_entry_learning_non_blocking"
        _append_step_status(cycle, results, post_entry_learning_step)

    surge_exec_validation_cmd = [py, str(TOOLS_DIR / "validate_surge_intraday_execution.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(surge_exec_validation_cmd))
        _append_step_status(cycle, results, {"label": "surge_intraday_execution_validation", "ok": True, "dry_run": True})
    else:
        surge_exec_step = _run(surge_exec_validation_cmd, "surge_intraday_execution_validation", timeout=60.0)
        if not surge_exec_step.get("ok"):
            surge_exec_step["ok"] = True
            surge_exec_step["advisory_only"] = True
            surge_exec_step["fallback_reason"] = "surge_intraday_execution_validation_non_blocking"
        _append_step_status(cycle, results, surge_exec_step)

    current_trading_ymd = dt.datetime.now(tz=KST).strftime("%Y%m%d")
    intraday_orders_exec_cmd = [
        py,
        str(TOOLS_DIR / "build_intraday_orders_exec_from_fills.py"),
        current_trading_ymd,
    ]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(intraday_orders_exec_cmd))
        _append_step_status(cycle, results, {"label": "intraday_orders_exec_from_fills", "ok": True, "dry_run": True})
    else:
        _append_step_status(
            cycle,
            results,
            _run(intraday_orders_exec_cmd, "intraday_orders_exec_from_fills", timeout=30.0),
        )
        _append_step_status(cycle, results, _sync_rootb_orders_exec_current(current_trading_ymd))

    # Step 4: KIS 紐⑥쓽?ъ옄 二쇰Ц 諛쒖넚
    dispatch_cmd = [
        py,
        str(TOOLS_DIR / "kis_order_dispatch_from_exec.py"),
        "--date", dt.datetime.now(tz=KST).strftime("%Y%m%d"),
        "--mock", mock,
        "--max-orders", str(max_orders),
    ]
    if dispatch_apply:
        dispatch_cmd.append("--apply")
    mock_risk_guarded_buy = str(os.environ.get("LOOP_MOCK_ALLOW_RISK_GUARDED_BUY", "0")).strip().lower() in {"1", "true", "y", "yes"}
    if dispatch_apply and mock_risk_guarded_buy and str(mock).strip().lower() != "false":
        dispatch_cmd.append("--mock-allow-risk-guarded-buy")
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(dispatch_cmd))
        _append_step_status(cycle, results, {"label": "order_dispatch", "ok": True, "dry_run": True})
    else:
        _append_step_status(cycle, results, _run(dispatch_cmd, "order_dispatch", timeout=90.0))

    # Step 5: 모의 체결 동기화
    sync_cmd = [
        py,
        str(TOOLS_DIR / "kis_sync_fills_from_api.py"),
        "--mock", mock,
    ]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(sync_cmd))
        _append_step_status(cycle, results, {"label": "sync_fills", "ok": True, "dry_run": True})
    else:
        _append_step_status(cycle, results, _run(sync_cmd, "sync_fills", timeout=60.0))

    # Step 6: refresh live_fills.csv for dashboard bridge.
    stub_script = ROOTB_DIR / "vibe_make_live_fills_stub.py"
    if stub_script.exists():
        stub_cmd = [py, str(stub_script)]
        if dry_run:
            logger.info("[DRY] would run: %s", " ".join(stub_cmd))
            _append_step_status(cycle, results, {"label": "live_fills_stub", "ok": True, "dry_run": True})
        else:
            _append_step_status(cycle, results, _run(stub_cmd, "live_fills_stub", timeout=30.0))
    else:
        logger.debug("[LOOP] vibe_make_live_fills_stub.py not found; skip")

    # Step 6b: regenerate orders_exec from latest fills so intraday fills are reflected.
    # Non-blocking: advisory only. Runs every cycle so orders_exec stays in sync with fills.
    _p0_onepass_every_n = _env_int("P0_ONEPASS_EXEC_EVERY_N", 1)
    _p0_script = TOOLS_DIR / "p0_onepass_from_fills.py"
    if not _p0_script.exists():
        logger.debug("[LOOP] p0_onepass_from_fills.py not found; skip orders_exec refresh")
    elif dry_run:
        logger.info("[DRY] would run p0_onepass_from_fills.py for orders_exec refresh")
        _append_step_status(cycle, results, {"label": "p0_orders_exec_refresh", "ok": True, "dry_run": True})
    elif not _run_on_cycle(cycle, _p0_onepass_every_n):
        _append_step_status(cycle, results, _cadence_skip("p0_orders_exec_refresh", _p0_onepass_every_n))
    else:
        _p0_d = _derive_d_from_fills()
        _p0_step = _run([py, str(_p0_script), _p0_d], "p0_orders_exec_refresh", timeout=60.0)
        _p0_step["as_of_ymd"] = _p0_d
        if not _p0_step.get("ok"):
            _p0_step["ok"] = True
            _p0_step["advisory_only"] = True
            _p0_step["fallback_reason"] = "p0_orders_exec_refresh_non_blocking"
        _append_step_status(cycle, results, _p0_step)

    # Sync RootA paper fills into RootB live/ledger before ledger coverage repair.
    fills_bridge_cmd = [
        py,
        str(TOOLS_DIR / "repair_rootb_fills_from_roota_fills.py"),
        "--apply",
    ]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(fills_bridge_cmd))
        _append_step_status(cycle, results, {"label": "rootb_fills_from_roota", "ok": True, "dry_run": True})
    else:
        _append_step_status(cycle, results, _run(fills_bridge_cmd, "rootb_fills_from_roota", timeout=90.0))

    # Refresh ledger coverage evidence before dashboard_state so ledger freshness
    # does not depend only on the daily batch.
    repair_cmd = [
        py,
        str(TOOLS_DIR / "repair_rootb_ledger_missing_live_fills.py"),
        "--start-ymd",
        str(os.environ.get("PAPER_OPER_START_YMD", "20260301")),
        "--apply",
    ]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(repair_cmd))
        _append_step_status(cycle, results, {"label": "ledger_live_fills_repair", "ok": True, "dry_run": True})
    else:
        _append_step_status(cycle, results, _run(repair_cmd, "ledger_live_fills_repair", timeout=90.0))

    if dry_run:
        logger.info("[DRY] would run post-ledger: %s", " ".join(surge_exec_validation_cmd))
        _append_step_status(cycle, results, {"label": "surge_intraday_execution_validation_post_ledger", "ok": True, "dry_run": True})
    else:
        surge_exec_post_step = _run(surge_exec_validation_cmd, "surge_intraday_execution_validation_post_ledger", timeout=60.0)
        if not surge_exec_post_step.get("ok"):
            surge_exec_post_step["ok"] = True
            surge_exec_post_step["advisory_only"] = True
            surge_exec_post_step["fallback_reason"] = "surge_intraday_execution_validation_post_ledger_non_blocking"
        _append_step_status(cycle, results, surge_exec_post_step)

    ledger_dry_run_cmd = [py, str(TOOLS_DIR / "ledger_live_fills_dry_run_report.py")]
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(ledger_dry_run_cmd))
        _append_step_status(cycle, results, {"label": "ledger_live_fills_dry_run", "ok": True, "dry_run": True})
    else:
        _append_step_status(cycle, results, _run(ledger_dry_run_cmd, "ledger_live_fills_dry_run", timeout=60.0))

    dashboard_state_cmd = [py, str(ROOTB_DIR / "tools" / "build_dashboard_state_v2.py")]
    _dashboard_state_every_n = _env_int("INTRADAY_DASHBOARD_STATE_EVERY_N", 3)
    if dry_run:
        logger.info("[DRY] would run: %s", " ".join(dashboard_state_cmd))
        _append_step_status(cycle, results, {"label": "build_dashboard_state_v2", "ok": True, "dry_run": True})
    elif dashboard_state_built:
        _append_step_status(cycle, results, {
            "label": "build_dashboard_state_v2",
            "ok": True,
            "skipped": True,
            "skip_reason": "already_built_after_realtime_surge",
        })
    elif not _run_on_cycle(cycle, _dashboard_state_every_n):
        _append_step_status(cycle, results, _cadence_skip("build_dashboard_state_v2", _dashboard_state_every_n))
    else:
        _append_step_status(cycle, results, _run(dashboard_state_cmd, "build_dashboard_state_v2", timeout=120.0))

    return results


# ---------------------------------------------------------------------------
# [2026-08-24] 하드 블록 중 청산 관리 (축소 사이클)
#
# 문제: 블록 시 `continue` 가 사이클 전체를 건너뛰어 **청산·손절·트레일까지 정지**했다.
#       2026-08-24 09:47~12:32, 포지션(005690 1주)을 든 채 2시간 45분 무방비였다.
#       손절선 미접촉으로 손실은 없었으나 보호가 없었다.
#
# 근거: 게이트의 근거("데이터가 낡았으니 새 판단을 하지 마라")는 진입 차단은 정당화하지만
#       청산 차단은 정당화하지 않는다. 손절선은 진입 시점에 확정된 값이고 후보 신선도와 무관하다.
#       => "살 수 없다"는 안전하지만 "팔 수 없다"는 위험하다.
#
# 설계: 진입 억제는 **이미 있는** PAPER_EXIT_ONLY 를 쓴다(paper_engine.py:440/448/1255/1388).
#       새 스위치를 만들지 않는다.
# 상세: docs/exec-plans/active/20260824_hard_block_exit_management.md
# ---------------------------------------------------------------------------

# 블록 사유가 가격 계열이면 축소 사이클을 돌지 않는다 - 가격을 못 믿으면 청산 판단도 못 한다.
PRICE_RELATED_BLOCK_LABELS = ("price", "quote", "hoga", "lob")


def _open_position_count() -> Optional[int]:
    """paper_state.json 의 open_positions 개수. 읽기 실패하면 None.

    주의: `paper/positions.csv` 가 아니다. 2026-08-24 에 그것을 보고 "포지션 0" 이라 오판했다.
    None 이면 호출측은 **현행 동작(전체 스킵)으로 폴백**한다. 열지 않는다.
    """
    try:
        state_path = ROOT / "paper" / "paper_state.json"
        if not state_path.exists():
            return None
        obj = json.loads(state_path.read_text(encoding="utf-8"))
        pos = obj.get("open_positions")
        if pos is None:
            return None
        return int(len(pos))
    except Exception as exc:
        logger.warning("[EXIT_CYCLE] paper_state 읽기 실패: %s", exc)
        return None


def _block_label_is_price_related(flag_path: Path) -> bool:
    try:
        obj = json.loads(flag_path.read_text(encoding="utf-8"))
        label = str(obj.get("label") or "").lower()
        return any(k in label for k in PRICE_RELATED_BLOCK_LABELS)
    except Exception:
        # 사유를 못 읽으면 보수적으로 가격 계열로 간주해 축소 사이클을 돌지 않는다
        return True


def _run_exit_only_cycle(py: str, flag_path: Path) -> bool:
    """블록 중 축소 사이클. 실행했으면 True, 건너뛰었으면 False."""
    # [2026-08-24] 배치가 도는 동안에는 축소 사이클을 돌리지 않는다.
    #   정상 사이클에는 이 검사가 있는데(아래 run_cycle 진입부) 블록 분기에는 없었다.
    #   paper_engine 에는 싱글턴/락이 없어서, 배치의 [7/9] paper_engine 과
    #   축소 사이클의 paper_engine 이 동시에 뜨면 둘 다 paper_state.json 과
    #   원장을 쓴다. 평일 21:30 VIBE_Paper_Daily 와 08:30 auto_daily_sync 가
    #   run_paper_daily.bat 을 돌리므로 실제로 겹칠 수 있는 구간이다.
    _bl = _daily_batch_lock_state()
    if _bl.get("active"):
        logger.warning(
            "[EXIT_CYCLE] run_paper_daily.lock active age=%.0fs - 축소 사이클 생략",
            float(_bl.get("age_sec") or 0.0),
        )
        return False
    if _block_label_is_price_related(flag_path):
        logger.info("[EXIT_CYCLE] 블록 사유가 가격 계열 - 축소 사이클 생략")
        return False
    n = _open_position_count()
    if n is None:
        logger.info("[EXIT_CYCLE] 포지션 확인 불가 - 현행 동작으로 폴백(전체 스킵)")
        return False
    if n <= 0:
        return False

    logger.warning("[EXIT_CYCLE] 블록 중이나 보유 %d건 - 청산 관리만 실행한다", n)
    snap_cmd = [
        py,
        str(TOOLS_DIR / "intraday_price_snapshot.py"),
        "--from-candidates",
        "--mock", "false",
        "--workers", "1",
    ]
    _run(snap_cmd, "exit_cycle_price_snapshot", timeout=90.0)

    engine_env = {"PAPER_EXIT_ONLY": "1"}
    if INTRADAY_PRICES_CSV.exists():
        engine_env["PAPER_INTRADAY_PRICE_PATH"] = str(INTRADAY_PRICES_CSV)
        engine_env["PAPER_INTRADAY_REALTIME_MODE"] = "1"
    _run([py, str(ROOT / "paper_engine.py")], "exit_cycle_paper_engine",
         env=engine_env, timeout=240.0)
    return True


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="[%(levelname)s] %(asctime)s %(name)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
            ],
        )
    _enable_job_kill_on_close()

    ap = argparse.ArgumentParser(description="Intraday paper trading loop")
    ap.add_argument("--mock", default="true", choices=["auto", "true", "false"],
                    help="KIS mock mode (default: true)")
    ap.add_argument("--interval", type=float, default=5.0,
                    help="Cycle interval in minutes (default: 5)")
    ap.add_argument("--max-orders", type=int, default=5,
                    help="Max orders per cycle (default: 5)")
    ap.add_argument("--dispatch-apply", action="store_true",
                    help="Actually send dispatch orders (default: false)")
    ap.add_argument("--allow-offhours", action="store_true",
                    help="Run outside KRX session hours (testing)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print commands without executing")
    ap.add_argument("--once", action="store_true",
                    help="Run one cycle then exit (no loop)")
    args = ap.parse_args()

    interval_sec = max(60.0, float(args.interval) * 60.0)
    py = sys.executable
    cycle = 0

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    lock_ok, lock_reason = _acquire_singleton_lock()
    if not lock_ok:
        logger.error("[LOOP] singleton lock blocked start: %s", lock_reason)
        return 2

    # state/news_collect_mode.txt가 있으면 환경변수로 적용 (run_news_pipeline_once.bat과 동일 방식)
    _mode_file = ROOT / "state" / "news_collect_mode.txt"
    if not os.environ.get("NEWS_COLLECT_MODE") and _mode_file.exists():
        try:
            _mode_from_file = _mode_file.read_text(encoding="utf-8").strip()
            if _mode_from_file in {"accumulate", "production"}:
                os.environ["NEWS_COLLECT_MODE"] = _mode_from_file
        except Exception:
            pass

    logger.info("=" * 60)
    logger.info("[LOOP] start  mock=%s interval=%.0fm dry_run=%s once=%s",
                args.mock, args.interval, args.dry_run, args.once)
    logger.info("[LOOP] dispatch_apply=%s", bool(args.dispatch_apply))
    logger.info("[LOOP] news_collect_mode=%s", os.environ.get("NEWS_COLLECT_MODE", "production"))
    logger.info("[LOOP] singleton_lock=%s path=%s", lock_reason, LOOP_LOCK_PATH)
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("[LOOP] DRY-RUN mode: commands will be printed, not executed")

    while True:
        flag_path = LOG_DIR / "paper_intraday_hard_blocked.flag"
        if flag_path.exists():
            d_str = _derive_d_from_fills()
            can_release = _try_auto_release_hard_block(flag_path, d_str, dry_run=True)
            if can_release and _try_auto_release_hard_block(flag_path, d_str, dry_run=False):
                logger.info("[LOOP] Auto-released outdated hard block flag. Resuming loop.")
            else:
                logger.error("[LOOP] paper_intraday_hard_blocked.flag exists. Sleeping to prevent crash loop.")
                # [2026-08-24] 블록이라도 보유 포지션이 있으면 청산 관리는 돌린다.
                # 진입 억제는 PAPER_EXIT_ONLY 가 담당한다. 상세는 위 축소 사이클 주석.
                try:
                    _run_exit_only_cycle(py, flag_path)
                except Exception as exc:
                    logger.warning("[EXIT_CYCLE] 실패: %s (현행 동작 유지)", exc)
                time.sleep(60)
                continue
        now = dt.datetime.now(tz=KST)

        if not _in_krx_session(allow_offhours=args.allow_offhours):
            hhmm = _now_hhmm()
            _closed = _krx_closed_reason(now)
            if _closed == "weekend":
                wait_reason = "weekend"
            elif _closed == "holiday":
                # [2026-08-21] 공휴일을 "pre-market" 으로 적으면 로그를 읽는 쪽이 오해한다.
                wait_reason = f"holiday ({now.strftime('%Y%m%d')})"
            elif hhmm < KRX_OPEN_HHMM:
                wait_reason = f"pre-market (now {hhmm:04d}, open {KRX_OPEN_HHMM:04d})"
            else:
                wait_reason = f"after-market (now {hhmm:04d}, close {KRX_CLOSE_HHMM:04d})"
            logger.info("[LOOP] outside KRX session (%s); sleeping 60s", wait_reason)
            offhours_results: List[Dict[str, Any]] = [
                {"label": "offhours_wait", "ok": True, "reason": wait_reason, "elapsed": 0.0}
            ]
            if not args.dry_run:
                # 장외에도 상태/손익/신선도 산출물을 갱신해 stale 경고를 방지한다.
                offhours_results.append(
                    _run([py, str(TOOLS_DIR / "reconcile_paper_state_from_fills.py")], "offhours_reconcile_state", timeout=60.0)
                )
                offhours_results.append(
                    _run([py, str(ROOT / "paper_pnl_report.py")], "offhours_pnl_report", timeout=180.0)
                )
                live_vs_bt_cmd, live_vs_bt_as_of = _build_live_vs_bt_align_cmd(py)
                if live_vs_bt_cmd is None:
                    offhours_results.append(
                        {
                            "label": "offhours_live_vs_bt_align",
                            "ok": False,
                            "returncode": 2,
                            "elapsed": 0.0,
                            "stdout_tail": [],
                            "stderr_tail": ["paper_pnl_summary_last missing as_of/as_of_ymd"],
                        }
                    )
                else:
                    live_vs_bt_step = _run(live_vs_bt_cmd, "offhours_live_vs_bt_align", timeout=90.0)
                    live_vs_bt_step["as_of"] = live_vs_bt_as_of
                    offhours_results.append(live_vs_bt_step)
                offhours_results.append(
                    _run([py, str(TOOLS_DIR / "freshness_check_v1.py")], "offhours_freshness_check", timeout=120.0)
                )
                offhours_results.append(
                    _run(
                        [
                            py,
                            str(TOOLS_DIR / "repair_rootb_ledger_missing_live_fills.py"),
                            "--start-ymd",
                            str(os.environ.get("PAPER_OPER_START_YMD", "20260301")),
                            "--apply",
                        ],
                        "offhours_ledger_live_fills_repair",
                        timeout=90.0,
                    )
                )
                offhours_results.append(
                    _run(
                        [py, str(TOOLS_DIR / "ledger_live_fills_dry_run_report.py")],
                        "offhours_ledger_live_fills_dry_run",
                        timeout=60.0,
                    )
                )
                # [2026-08-22] 브로커 실청구 비용 동기화 (PLANS 56).
                # 비용 상수는 실매매 이력이 없어 추측으로 박혀 있었고, 2026-07-29 실거래
                # 조회로 fee 0 / tax 0.20% 임이 드러나 상수를 고쳤다. 수수료 0 은
                # 증권사 무료 혜택일 수 있으므로 끝나면 드러나야 한다 - 그 눈이 이 스텝이다.
                # 도구가 하루 1회 + 16:00 이후 가드를 스스로 갖고 있어 매 사이클 불러도
                # API 를 두드리지 않는다. offhours_ 접두사라 실패해도 루프를 막지 않는다.
                # [2026-08-22] final_score 사이드카 스냅샷 (PLANS 57).
                # (80) 이 6축을 "제거가 아니라 격리" 하며 나중에 검정할 데이터를 남긴다고 했는데,
                # 그 컬럼이 담긴 with_final_score.csv 는 최신 1개뿐이라 이력이 안 쌓이고 있었다.
                # 해시 비교로 내용이 바뀔 때만 뜬다 - 매 사이클 불러도 파일이 안 불어난다.
                offhours_results.append(
                    _run(
                        [py, str(TOOLS_DIR / "archive_final_score_snapshot.py")],
                        "offhours_final_score_snapshot",
                        timeout=60.0,
                    )
                )
                offhours_results.append(
                    _run(
                        [
                            py,
                            str(TOOLS_DIR / "kis_sync_broker_costs.py"),
                            "--once-per-day",
                            "--after-hhmm",
                            str(os.environ.get("BROKER_COST_SYNC_AFTER_HHMM", "1600")),
                            "--days",
                            str(os.environ.get("BROKER_COST_SYNC_DAYS", "7")),
                        ],
                        "offhours_broker_cost_sync",
                        timeout=120.0,
                    )
                )
                offhours_results.append(
                    _run(
                        [py, str(ROOTB_DIR / "tools" / "build_dashboard_state_v2.py")],
                        "offhours_build_dashboard_state_v2",
                        env={
                            "RUNTIME_CHAIN_STAGE_MAX_AGE_SECONDS": os.environ.get(
                                "RUNTIME_CHAIN_STAGE_MAX_AGE_SECONDS_OFFHOURS",
                                "43200",
                            ),
                        },
                        timeout=600.0,
                    )
                )
                if now.minute % 15 == 0:
                    offhours_session = _offhours_news_session(now)
                    offhours_results.append(
                        _run(
                            [
                                "cmd",
                                "/c",
                                str(ROOT / "run_news_pipeline_once.bat"),
                                offhours_session,
                                "",
                                "",
                                "--force-outside-window",
                            ],
                            f"offhours_news_pipeline_{offhours_session}",
                            env={
                                "NEWS_PIPELINE_SKIP_FINAL": "1",
                                "NEWS_PIPELINE_SKIP_INTEGRATION": "1",
                            },
                            timeout=240.0,
                        )
                    )
                if now.minute % 15 == 0:
                    offhours_results.append(
                        _run(
                            [
                                py,
                                str(TOOLS_DIR / "build_ssot_health_card.py"),
                                "--config",
                                str(ROOT / "config" / "ssot_health_card.json"),
                                "--out-dir",
                                str(LOG_DIR),
                            ],
                            "offhours_ssot_health",
                            timeout=120.0,
                        )
                    )
            next_run_at = (dt.datetime.now() + dt.timedelta(seconds=60)).strftime("%H:%M:%S")
            _write_status(
                cycle,
                offhours_results,
                next_run_at,
            )
            time.sleep(60)
            continue

        cycle += 1
        cycle_start = time.time()
        logger.info("[LOOP] cycle %d start %s", cycle, _now_ts())
        try:
            _write_status(cycle, [{"label": "cycle_start", "ok": True, "elapsed": 0.0}], "-")
        except Exception:
            pass

        # 2026-08-20: run_paper_daily.bat holds 2_Logs/run_paper_daily.lock while it
        # runs (run_paper_daily.bat:44,59).  batch<->batch and watchdog<->batch already
        # honour it, but this loop did not, so a mid-session batch (53 min on
        # 2026-08-20) wrote candidates/status/ledger at the same time as the loop.
        # Skip the cycle rather than wait: the next one re-checks in DAILY_BATCH_SKIP_SLEEP_SEC.
        # Stale-lock cap prevents a crashed batch from freezing the loop forever.
        _batch_skip = _daily_batch_lock_state()
        if _batch_skip.get("active"):
            logger.warning(
                "[LOOP] cycle %d skipped: run_paper_daily.lock active age=%.0fs",
                cycle, float(_batch_skip.get("age_sec") or 0.0),
            )
            try:
                _write_status(cycle, [{
                    "label": "daily_batch_skip",
                    "ok": True,
                    "skipped": True,
                    "skip_reason": "run_paper_daily_lock_active",
                    "lock_age_sec": round(float(_batch_skip.get("age_sec") or 0.0), 1),
                    "elapsed": 0.0,
                }], "-")
            except Exception:
                pass
            time.sleep(_env_int("DAILY_BATCH_SKIP_SLEEP_SEC", 60))
            continue

        try:
            results = run_cycle(
                cycle=cycle,
                py=py,
                mock=args.mock,
                dry_run=args.dry_run,
                max_orders=args.max_orders,
                dispatch_apply=bool(args.dispatch_apply),
                once=bool(args.once),
            )
        except Exception as e:
            logger.exception("[LOOP] run_cycle failed: %s", e)
            try:
                import subprocess
                msg_body = f"[LOOP CRASH] {type(e).__name__}: {e}"
                subprocess.Popen([
                    sys.executable,
                    str(TOOLS_DIR / "telegram_notifier.py"),
                    "--msg", msg_body,
                    "--event", "error"
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass
            results = [{"label": "cycle_fatal", "ok": False, "elapsed": 0.0, "stderr_tail": [f"{type(e).__name__}: {e}"]}]
            next_run_at_dt = dt.datetime.now() + dt.timedelta(seconds=interval_sec)
            next_run_at = next_run_at_dt.strftime("%H:%M:%S")
            _write_status(cycle, results, next_run_at)
            if args.once:
                return 1
            time.sleep(min(30.0, interval_sec))
            continue

        elapsed = round(time.time() - cycle_start, 1)
        ok_count = sum(1 for r in results if r.get("ok"))
        logger.info("[LOOP] cycle %d done: %d/%d ok (%.1fs)",
                    cycle, ok_count, len(results), elapsed)

        next_run_at_dt = dt.datetime.now() + dt.timedelta(seconds=interval_sec)
        next_run_at = next_run_at_dt.strftime("%H:%M:%S")
        _write_status(cycle, results, next_run_at)

        if args.once:
            logger.info("[LOOP] --once: exiting after cycle %d", cycle)
            break

        sleep_remaining = max(0.0, interval_sec - elapsed)
        if sleep_remaining > 0:
            logger.info("[LOOP] next cycle at %s (sleep %.0fs)", next_run_at, sleep_remaining)
            time.sleep(sleep_remaining)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
