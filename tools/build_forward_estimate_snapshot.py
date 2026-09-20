# -*- coding: utf-8 -*-
"""
build_forward_estimate_snapshot.py
====================================
Collect forward valuation inputs and write _cache/forward_estimate_latest.csv.

Collected fields:
  1. Forward PER          -> scrape WiseReport (navercomp.wisereport.co.kr).
  2. TTM fields           -> aggregate latest four quarters from DART filings.
     - ttm_revenue          : TTM revenue     - ttm_operating_profit : TTM operating profit
     - ttm_net_income       : TTM net income  - ttm_opm              : TTM operating margin
  3. Derived fields        -> forward_eps and peg_ratio.

Run:
  python tools/build_forward_estimate_snapshot.py [--max-codes 100] [--sleep 0.2]

References:
  - E:/1_Data/_cache/dart_api_key.txt or DART_API_KEY environment variable
  - E:/1_Data/_cache/dart_corp_code_map.csv
  - E:/1_Data/_cache/dart_fundamental_latest.csv  (revenue_growth 李몄“)
  - E:/1_Data/2_Logs/candidates_latest_data.csv   (target code list)
"""
from __future__ import annotations

import argparse
import atexit
import hashlib
import json
import os
import re
import shutil
import traceback
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
import logging

BASE_DIR   = Path(os.environ.get("STOC_BASE_DIR", str(Path(__file__).resolve().parents[1])))
CACHE_DIR  = BASE_DIR / "_cache"
LOG_DIR    = BASE_DIR / "2_Logs"

OUTPUT_CSV  = CACHE_DIR / "forward_estimate_latest.csv"
META_JSON   = CACHE_DIR / "forward_estimate_meta.json"
RUN_MARKER_JSON = CACHE_DIR / "forward_estimate_run_marker.json"
RUN_LINEAGE_JSON = CACHE_DIR / "forward_estimate_lineage_latest.json"
CORP_MAP    = CACHE_DIR / "dart_corp_code_map.csv"
KEY_FILE    = CACHE_DIR / "dart_api_key.txt"
FNLTT_URL   = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
PIPELINE_VERSION = os.environ.get("FWD_PIPELINE_VERSION", "2026-04-06-rerun-safety-v1")
NO_DATA_STATUSES = {"013"}

# WiseReport reprt_code 紐⑸줉: (肄붾뱶, ?ㅻ챸, 遺꾧린踰덊샇)
QUARTERLY_REPORTS = [
    ("11013", "Q1"),  # 1遺꾧린
    ("11012", "H1"),  # 諛섍린(Q2 ?꾩쟻)
    ("11014", "Q3"),  # 3遺꾧린
    ("11011", "FY"),  # ?곌컙(Q4 ?ы븿)
]

WISEREPORT_URL = "https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
WISEREPORT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
}
_NUM_PAIR = re.compile(r">([^<>]{1,30})\s*<b class=\"num\">([\d,.-]+)</b>")

FSM_INIT = "INIT"
FSM_INPUT_VALIDATED = "INPUT_VALIDATED"
FSM_SOURCES_READY = "SOURCES_READY"
FSM_COLLECTED = "COLLECTED"
FSM_QUALITY_PASSED = "QUALITY_PASSED"
FSM_SAVED = "SAVED"
FSM_FAILED = "FAILED"

FSM_ALLOWED: dict[str, dict[str, str]] = {
    FSM_INIT: {"validate_input": FSM_INPUT_VALIDATED, "fail": FSM_FAILED},
    FSM_INPUT_VALIDATED: {"prepare_sources": FSM_SOURCES_READY, "fail": FSM_FAILED},
    FSM_SOURCES_READY: {"collect_done": FSM_COLLECTED, "fail": FSM_FAILED},
    FSM_COLLECTED: {"quality_pass": FSM_QUALITY_PASSED, "fail": FSM_FAILED},
    FSM_QUALITY_PASSED: {"persist_done": FSM_SAVED, "fail": FSM_FAILED},
    FSM_SAVED: {},
    FSM_FAILED: {},
}




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _fsm_transition(state: str, event: str) -> str:
    nxt = FSM_ALLOWED.get(state, {}).get(event)
    if not nxt:
        raise SystemExit(f"[ERR] invalid state transition: {state} --{event}--> ?")
    return nxt


def _require_state(state: str, required: str, action: str) -> None:
    if state != required:
        raise SystemExit(f"[ERR] action_requires_{required}: {action}, current={state}")


def _dev_assert(cond: bool, msg: str) -> None:
    # Development invariant checks; disabled with -O.
    if __debug__:
        assert cond, msg


def _ensure(condition: bool, message: str) -> None:
    # Operational fail-fast checks; always active.
    if not condition:
        raise SystemExit(message)


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def _log(level: str, event: str, **fields) -> None:
    rec = {"ts": _now_iso(), "level": str(level).upper(), "event": str(event)}
    rec.update({k: v for k, v in fields.items() if v is not None})
    try:
        _log_print(json.dumps(rec, ensure_ascii=False))
    except Exception:
        _log_print(f"[{rec['level']}] {event} {fields}")


def _stat_inc(stats: Optional[dict], key: str, n: int = 1) -> None:
    if stats is None:
        return
    stats[key] = int(stats.get(key, 0)) + int(n)


def _stat_inc_nested(stats: Optional[dict], key: str, subkey: str, n: int = 1) -> None:
    if stats is None:
        return
    cur = stats.get(key)
    if not isinstance(cur, dict):
        cur = {}
    cur[str(subkey)] = int(cur.get(str(subkey), 0)) + int(n)
    stats[key] = cur


def _sleep_backoff(attempt: int, base: float, cap: float) -> None:
    delay = min(cap, base * (2 ** max(0, attempt)))
    if delay > 0:
        time.sleep(delay)


def _http_get_with_retry(
    session: requests.Session,
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: float = 15.0,
    expect_json: bool = False,
    retry_max: int = 0,
    backoff_base: float = 0.5,
    backoff_cap: float = 4.0,
    stats: Optional[dict] = None,
    stats_prefix: str = "",
):
    for attempt in range(max(0, retry_max) + 1):
        t0 = time.time()
        try:
            r = session.get(url, params=params, headers=headers, timeout=timeout)
            sc = int(getattr(r, "status_code", 0) or 0)
            if sc == 429:
                _stat_inc(stats, f"{stats_prefix}http_429")
                _log("WARNING", "http_429", url=url, status=sc, attempt=attempt, retry_max=retry_max)
                if attempt < retry_max:
                    ra = str(r.headers.get("Retry-After", "")).strip()
                    try:
                        wait_s = max(0.0, float(ra))
                    except Exception:
                        wait_s = min(backoff_cap, backoff_base * (2 ** attempt))
                    time.sleep(wait_s)
                    continue
                return None
            if sc >= 500:
                _stat_inc(stats, f"{stats_prefix}http_5xx")
                _log("WARNING", "http_5xx", url=url, status=sc, attempt=attempt, retry_max=retry_max)
                if attempt < retry_max:
                    _sleep_backoff(attempt, backoff_base, backoff_cap)
                    continue
                return None
            if sc >= 400:
                _stat_inc(stats, f"{stats_prefix}http_4xx")
                _log("ERROR", "http_4xx", url=url, status=sc, attempt=attempt)
                return None
            if expect_json:
                try:
                    _log("DEBUG", "http_ok", url=url, status=sc, attempt=attempt, latency_ms=int((time.time() - t0) * 1000))
                    return r.json()
                except Exception:
                    _stat_inc(stats, f"{stats_prefix}schema_errors")
                    _log("ERROR", "http_json_schema_error", url=url, status=sc, attempt=attempt)
                    return None
            _log("DEBUG", "http_ok", url=url, status=sc, attempt=attempt, latency_ms=int((time.time() - t0) * 1000))
            return r
        except requests.Timeout:
            _stat_inc(stats, f"{stats_prefix}timeout_errors")
            _log("WARNING", "http_timeout", url=url, attempt=attempt, retry_max=retry_max)
            if attempt < retry_max:
                _sleep_backoff(attempt, backoff_base, backoff_cap)
                continue
            return None
        except requests.RequestException:
            _stat_inc(stats, f"{stats_prefix}network_errors")
            _log("WARNING", "http_network_error", url=url, attempt=attempt, retry_max=retry_max)
            if attempt < retry_max:
                _sleep_backoff(attempt, backoff_base, backoff_cap)
                continue
            return None
        except Exception:
            _stat_inc(stats, f"{stats_prefix}unknown_errors")
            _log("ERROR", "http_unknown_error", url=url, attempt=attempt)
            return None
    return None


def _compute_idempotency_key(
    as_of_ymd: str,
    codes: list[str],
    *,
    skip_ttm: bool,
    skip_wisereport: bool,
    strict: bool,
    max_external_fail_rate: float,
    min_source_coverage: float,
    pipeline_version: str,
    override: str = "",
) -> str:
    if override:
        return str(override).strip()
    payload = {
        "as_of_ymd": as_of_ymd,
        "codes": sorted(set(codes)),
        "skip_ttm": bool(skip_ttm),
        "skip_wisereport": bool(skip_wisereport),
        "strict": bool(strict),
        "max_external_fail_rate": float(max_external_fail_rate),
        "min_source_coverage": float(min_source_coverage),
        "pipeline_version": str(pipeline_version or ""),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _codes_digest(codes: list[str]) -> str:
    raw = "|".join(sorted(set(codes)))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _stable_hash_obj(obj: object) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _file_fingerprint(path_like: str) -> dict:
    p = Path(str(path_like or "")).expanduser()
    out = {
        "path": str(path_like or ""),
        "exists": False,
        "size": 0,
        "mtime": "",
        "sha256_16": "",
    }
    try:
        if not str(path_like or "").strip() or not p.exists() or not p.is_file():
            return out
        st = p.stat()
        out["exists"] = True
        out["size"] = int(st.st_size)
        out["mtime"] = datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                if not chunk:
                    break
                h.update(chunk)
        out["sha256_16"] = h.hexdigest()[:16]
        return out
    except Exception:
        return out


def _audit_path_for_output(out_path: Path) -> Path:
    return out_path.with_suffix(out_path.suffix + ".decision_audit.jsonl")


def _load_json_if_exists(path: Path, *, strict: bool = False, label: str = "") -> dict:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        if strict:
            raise SystemExit(f"[ERR] invalid {label or 'json'}: {path}")
        return {}


def _atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding=encoding)
    os.replace(tmp, path)


def _atomic_write_csv(path: Path, df: pd.DataFrame) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    os.replace(tmp, path)


def _write_run_marker(path: Path, payload: dict) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _clear_run_marker(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def _is_reusable_output(path: Path, expected_count: int, as_of_ymd: str) -> bool:
    if not path.exists():
        return False
    try:
        df = pd.read_csv(path, dtype={"code": str})
    except Exception:
        return False
    if len(df) != expected_count:
        return False
    if "code" not in df.columns or "as_of_ymd" not in df.columns:
        return False
    if df["as_of_ymd"].astype(str).nunique() != 1 or str(df["as_of_ymd"].iloc[0]) != as_of_ymd:
        return False
    if int(df.duplicated(["code", "as_of_ymd"]).sum()) > 0:
        return False
    if (~df["code"].astype(str).map(_valid_code6)).any():
        return False
    return True


def _build_lineage_snapshot(meta: dict) -> dict:
    return {
        "updated_at": str(meta.get("updated_at") or ""),
        "as_of_ymd": str(meta.get("as_of_ymd") or ""),
        "run_id": str(meta.get("run_id") or ""),
        "idempotency_key": str(meta.get("idempotency_key") or ""),
        "code_set_sha": str(meta.get("code_set_sha") or ""),
        "source_paths": meta.get("source_paths") or {},
        "lineage": meta.get("lineage") or {},
        "output": str(meta.get("output") or ""),
    }


def _is_committed_state_consistent(
    *,
    meta_obj: dict,
    lineage_obj: dict,
    marker_obj: dict,
    as_of_ymd: str,
    out_path: Path,
    idempotency_key: str,
    code_set_sha: str,
    expected_count: int,
) -> bool:
    run_id = str(meta_obj.get("run_id") or "")
    if not run_id:
        return False
    if str(meta_obj.get("as_of_ymd") or "") != as_of_ymd:
        return False
    if str(meta_obj.get("idempotency_key") or "") != idempotency_key:
        return False
    if str(meta_obj.get("code_set_sha") or "") != code_set_sha:
        return False
    if int(meta_obj.get("total_codes", -1) or -1) != int(expected_count):
        return False
    if str(lineage_obj.get("run_id") or "") != run_id:
        return False
    if str(lineage_obj.get("as_of_ymd") or "") != as_of_ymd:
        return False
    if str(lineage_obj.get("idempotency_key") or "") != idempotency_key:
        return False
    if str(marker_obj.get("run_id") or "") != run_id:
        return False
    if str(marker_obj.get("as_of_ymd") or "") != as_of_ymd:
        return False
    if str(marker_obj.get("idempotency_key") or "") != idempotency_key:
        return False
    if str(marker_obj.get("output") or "") != str(out_path):
        return False
    return True


def _state_files_for_output(out_path: Path) -> tuple[Path, Path, Path]:
    parent = out_path.parent
    stem = out_path.stem
    meta = parent / f"{stem}.meta.json"
    marker = parent / f"{stem}.run_marker.json"
    lineage = parent / f"{stem}.lineage.json"
    return meta, marker, lineage


def _cleanup_stale_tmp(paths: list[Path]) -> None:
    for p in paths:
        tmp = p.with_suffix(p.suffix + ".tmp")
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def _backup_if_exists(path: Path) -> None:
    if path.exists():
        bak = path.with_suffix(path.suffix + ".bak_prev")
        shutil.copy2(path, bak)


def _restore_backup_if_exists(path: Path) -> None:
    bak = path.with_suffix(path.suffix + ".bak_prev")
    if bak.exists():
        shutil.copy2(bak, path)


def _acquire_run_lock(lock_path: Path) -> bool:
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.close(fd)
        return True
    except FileExistsError:
        return False


def _release_run_lock(lock_path: Path) -> None:
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass


def _run_fsm_transition_tests() -> int:
    invalid_cases = [
        (FSM_INIT, "collect_done"),
        (FSM_SOURCES_READY, "persist_done"),
        (FSM_COLLECTED, "prepare_sources"),
    ]
    for st, ev in invalid_cases:
        try:
            _fsm_transition(st, ev)
            _log_print(f"[FSM_TEST][FAIL] invalid transition allowed: {st} --{ev}-->")
            return 1
        except SystemExit:
            pass

    st = FSM_INIT
    st = _fsm_transition(st, "validate_input")
    st = _fsm_transition(st, "prepare_sources")
    st = _fsm_transition(st, "collect_done")
    st = _fsm_transition(st, "quality_pass")
    st = _fsm_transition(st, "persist_done")
    if st != FSM_SAVED:
        _log_print(f"[FSM_TEST][FAIL] final state mismatch: {st}")
        return 1

    st2 = FSM_INPUT_VALIDATED
    st2 = _fsm_transition(st2, "fail")
    if st2 != FSM_FAILED:
        _log_print(f"[FSM_TEST][FAIL] fail transition mismatch: {st2}")
        return 1

    _log_print("[FSM_TEST][OK] transition tests passed")
    return 0


def _run_contract_tests() -> int:
    # 1) 정상 입력 테스트
    try:
        code = _norm_code6("1234")
        if code != "001234":
            _log_print(f"[CONTRACT_TEST][FAIL] normal normalize failed: {code}")
            return 1
        row = {"forward_per": 10.0, "ttm_revenue": 100.0, "ttm_operating_profit": 10.0, "ttm_net_income": 5.0}
        derived = _compute_derived(row, current_price=50000.0, revenue_growth=20.0)
        if set(derived.keys()) != {"forward_eps", "ttm_opm", "peg_ratio"}:
            _log_print("[CONTRACT_TEST][FAIL] normal derived keys mismatch")
            return 1
    except Exception as e:
        _log_print(f"[CONTRACT_TEST][FAIL] normal input test crashed: {e}")
        return 1

    # 2) 비정상 입력 테스트
    try:
        _compute_derived(None, current_price=1.0, revenue_growth=1.0)  # type: ignore[arg-type]
        _log_print("[CONTRACT_TEST][FAIL] invalid input did not fail")
        return 1
    except SystemExit:
        pass

    # 3) 사후 조건 위반 테스트
    try:
        bad = pd.DataFrame(
            [
                {
                    "code": "005930",
                    "as_of_ymd": "20260406",
                    "updated_at": "2026-04-06 00:00:00",
                    "forward_per": np.nan,
                    "ttm_revenue": np.nan,
                    "peg_ratio": np.nan,
                }
            ]
        )
        _validate_before_save(
            bad,
            "20260406",
            expected_count=1,
            strict=True,
            require_wisereport=True,
            require_ttm=False,
            min_source_coverage=0.3,
        )
        _log_print("[CONTRACT_TEST][FAIL] postcondition violation did not fail")
        return 1
    except SystemExit:
        pass

    _log_print("[CONTRACT_TEST][OK] contract tests passed")
    return 0


def _run_validation_layer_tests() -> int:
    # invalid input sample: bad as_of format should fail
    try:
        _normalize_asof_ymd("2026-04-06")
        _log_print("[VAL_TEST][FAIL] invalid as_of format did not fail")
        return 1
    except Exception:
        pass

    # missing/outlier block test: outlier forward_per must fail in strict mode
    bad_row = {
        "code": "005930",
        "as_of_ymd": "20260406",
        "run_id": "R1",
        "updated_at": "2026-04-06 00:00:00",
        "forward_per": 100000.0,
        "current_per_wr": 10.0,
        "eps_wr": 1000.0,
        "ttm_revenue": 1_000_000.0,
        "ttm_operating_profit": 100_000.0,
        "ttm_net_income": 90_000.0,
        "forward_eps": 500.0,
        "ttm_opm": 10.0,
        "peg_ratio": 2.0,
    }
    try:
        _validate_transform_row(bad_row, strict=True)
        _log_print("[VAL_TEST][FAIL] outlier did not fail")
        return 1
    except SystemExit:
        pass

    # pre-save check: duplicate key rows should fail
    df_dup = pd.DataFrame(
        [
            {"code": "005930", "as_of_ymd": "20260406", "run_id": "R1", "updated_at": "2026-04-06 00:00:00", "forward_per": 10.0, "ttm_revenue": 100.0, "peg_ratio": 1.0},
            {"code": "005930", "as_of_ymd": "20260406", "run_id": "R1", "updated_at": "2026-04-06 00:00:00", "forward_per": 11.0, "ttm_revenue": 101.0, "peg_ratio": 1.1},
        ]
    )
    try:
        _validate_before_save(
            df_dup,
            "20260406",
            expected_count=2,
            strict=True,
            require_wisereport=False,
            require_ttm=False,
            min_source_coverage=0.0,
        )
        _log_print("[VAL_TEST][FAIL] duplicate rows did not fail")
        return 1
    except SystemExit:
        pass

    # lineage snapshot must include run/as_of/source fields
    snap = _build_lineage_snapshot(
        {
            "updated_at": "2026-04-06 00:00:00",
            "as_of_ymd": "20260406",
            "run_id": "RID",
            "idempotency_key": "KEY",
            "code_set_sha": "SHA",
            "source_paths": {"codes_source": "x.csv"},
            "lineage": {"rows_out": 1},
            "output": "out.csv",
        }
    )
    need = ["updated_at", "as_of_ymd", "run_id", "idempotency_key", "source_paths", "lineage", "output"]
    if any(str(snap.get(k, "")).strip() == "" for k in ["as_of_ymd", "run_id", "idempotency_key", "output"]):
        _log_print("[VAL_TEST][FAIL] lineage snapshot required identity fields missing")
        return 1
    if not all(k in snap for k in need):
        _log_print("[VAL_TEST][FAIL] lineage snapshot keys missing")
        return 1

    _log_print("[VAL_TEST][OK] validation layer tests passed")
    return 0


# ---------------------------------------------------------------------------
def _norm_code6(c: str) -> str:
    _ensure(c is None or isinstance(c, (str, int, float)), "[ERR] code must be str/int/float or None")
    raw = str(c or "").strip()
    if not raw:
        return ""
    if not raw.isdigit():
        return ""
    if len(raw) > 6:
        return ""
    out = raw.zfill(6)
    _dev_assert((out == "") or bool(re.fullmatch(r"\d{6}", out)), "postcondition failed: normalized code format")
    return out


def _valid_code6(c: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", str(c or ""))) and str(c) != "000000"


def _normalize_asof_ymd(raw: str) -> str:
    _ensure(isinstance(raw, str), "[ERR] as_of must be string YYYYMMDD")
    ymd = str(raw or "").strip()
    _ensure(bool(ymd), "[ERR] as_of is empty")
    datetime.strptime(ymd, "%Y%m%d")
    _dev_assert(bool(re.fullmatch(r"\d{8}", ymd)), "postcondition failed: as_of must be 8 digits")
    return ymd

def _to_float(v) -> float:
    try:
        return float(str(v or "").replace(",", "").strip())
    except Exception:
        return np.nan

def _safe_div(a: float, b: float) -> float:
    if np.isfinite(a) and np.isfinite(b) and abs(b) > 1e-9:
        return float(a / b)
    return np.nan

def _resolve_api_key() -> str:
    k = str(os.environ.get("DART_API_KEY", "")).strip()
    if k:
        return k
    if KEY_FILE.exists():
        return KEY_FILE.read_text(encoding="utf-8").strip()
    return ""


def _dedupe_codes(values: list[str], max_codes: int) -> list[str]:
    _ensure(isinstance(values, list), "[ERR] values must be list")
    _ensure(isinstance(max_codes, int) and max_codes > 0, "[ERR] max_codes must be positive int")
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        code = _norm_code6(v)
        if not _valid_code6(code):
            continue
        if code in seen:
            continue
        seen.add(code)
        out.append(code)
        if len(out) >= max_codes:
            break
    _dev_assert(len(out) <= max_codes, "postcondition failed: dedupe length exceeds max_codes")
    _dev_assert(len(out) == len(set(out)), "postcondition failed: dedupe result not unique")
    _dev_assert(all(_valid_code6(x) for x in out), "postcondition failed: dedupe contains invalid code")
    return out


def _sanitize_numeric_field(
    row: dict,
    key: str,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
    strict: bool = True,
) -> None:
    v = row.get(key, np.nan)
    fv = _to_float(v)
    if not np.isfinite(fv):
        row[key] = np.nan
        return
    if min_value is not None and fv < min_value:
        if strict:
            raise SystemExit(f"[ERR] invalid {key}: {fv} < {min_value}")
        row[key] = np.nan
        return
    if max_value is not None and fv > max_value:
        if strict:
            raise SystemExit(f"[ERR] invalid {key}: {fv} > {max_value}")
        row[key] = np.nan
        return
    row[key] = fv


def _validate_transform_row(row: dict, strict: bool) -> None:
    _ensure(isinstance(row, dict), "[ERR] row must be dict")
    required = ["code", "as_of_ymd", "run_id", "updated_at"]
    for k in required:
        _ensure(k in row, f"[ERR] row missing required field: {k}")
        _ensure(str(row.get(k) or "").strip() != "", f"[ERR] row empty required field: {k}")
    _ensure(_valid_code6(str(row.get("code"))), f"[ERR] invalid row code: {row.get('code')}")
    _normalize_asof_ymd(str(row.get("as_of_ymd")))

    _sanitize_numeric_field(row, "forward_per", min_value=0.0, max_value=10000.0, strict=strict)
    _sanitize_numeric_field(row, "current_per_wr", min_value=0.0, max_value=10000.0, strict=strict)
    _sanitize_numeric_field(row, "eps_wr", min_value=-1_000_000_000.0, max_value=1_000_000_000.0, strict=strict)
    _sanitize_numeric_field(row, "ttm_revenue", min_value=-1_000_000_000_000_000.0, max_value=1_000_000_000_000_000.0, strict=strict)
    _sanitize_numeric_field(row, "ttm_operating_profit", min_value=-1_000_000_000_000_000.0, max_value=1_000_000_000_000_000.0, strict=strict)
    _sanitize_numeric_field(row, "ttm_net_income", min_value=-1_000_000_000_000_000.0, max_value=1_000_000_000_000_000.0, strict=strict)
    _sanitize_numeric_field(row, "forward_eps", min_value=-1_000_000_000.0, max_value=1_000_000_000.0, strict=strict)
    _sanitize_numeric_field(row, "ttm_opm", min_value=-1000.0, max_value=1000.0, strict=strict)
    _sanitize_numeric_field(row, "peg_ratio", min_value=0.0, max_value=10000.0, strict=strict)


# ---------------------------------------------------------------------------
def _scrape_wisereport(
    code: str,
    session: requests.Session,
    quality_stats: Optional[dict] = None,
    *,
    retry_max: int = 0,
    backoff_base: float = 0.5,
    backoff_cap: float = 4.0,
) -> dict:
    """Scrape Forward PER and related fields from WiseReport."""
    url = WISEREPORT_URL.format(code=code)
    result: dict = {"code": code, "forward_per": np.nan, "current_per_wr": np.nan, "eps_wr": np.nan}
    if quality_stats is not None:
        quality_stats["wr_attempts"] = int(quality_stats.get("wr_attempts", 0)) + 1
    try:
        r = _http_get_with_retry(
            session,
            url,
            headers=WISEREPORT_HEADERS,
            timeout=15.0,
            expect_json=False,
            retry_max=retry_max,
            backoff_base=backoff_base,
            backoff_cap=backoff_cap,
            stats=quality_stats,
            stats_prefix="wr_",
        )
        if r is None:
            _stat_inc(quality_stats, "wr_errors")
            return result
        try:
            text = r.content.decode("utf-8")
        except UnicodeDecodeError:
            text = r.content.decode("euc-kr", errors="replace")

        label_map: dict = {}
        for label, val in _NUM_PAIR.findall(text):
            label_clean = label.strip()
            label_map[label_clean] = val.replace(",", "")
        if not label_map:
            _stat_inc(quality_stats, "wr_schema_errors")

        # Prefer label keys ending with PER but not the plain aggregate PER label.
        # Some pages include encoded prefixes, so match by suffix and non-empty prefix.
        for key, val in label_map.items():
            is_forward_per = (
                key.endswith("PER")
                and key != "PER"
                and len(key) > 3  # prefix exists
            )
            if is_forward_per:
                result["forward_per"] = _to_float(val)
                break
        # Explicit fallback labels.
        if not np.isfinite(result["forward_per"]):
            for key in ["異붿젙PER", "PER(異붿젙)", "Forward PER"]:
                if key in label_map:
                    result["forward_per"] = _to_float(label_map[key])
                    break

        # ?꾩옱 PER (WR 湲곗?)
        result["current_per_wr"] = _to_float(label_map.get("PER", np.nan))
        # EPS (WR 湲곗?)
        result["eps_wr"] = _to_float(label_map.get("EPS", np.nan))

    except Exception as e:
        if quality_stats is not None:
            quality_stats["wr_errors"] = int(quality_stats.get("wr_errors", 0)) + 1
        _log_print(f"  [WR] {code}: {e}")
    return result


DART_REVENUE_IDS = {"ifrs-full_Revenue", "ifrs_Revenue"}
DART_REVENUE_NAMES = ["revenue", "sales", "매출", "매출액"]
DART_OP_IDS = {"dart_OperatingIncomeLoss"}
DART_OP_NAMES = ["operating income", "operating profit", "영업이익"]
DART_NP_IDS = {"ifrs-full_ProfitLoss", "ifrs_ProfitLoss"}
DART_NP_NAMES = ["net income", "profit for the period", "당기순이익", "지배기업 소유주지분 순이익"]

EXCLUDE_REVENUE_NAMES = [
    "operating income",
    "operating profit",
    "영업이익",
    "당기순이익",
    "순이익",
    "자산",
    "부채",
    "자본",
]
EXCLUDE_OP_NAMES = [
    "매출",
    "매출액",
    "자산",
    "부채",
    "자본",
]
EXCLUDE_NET_NAMES = [
    "매출",
    "매출액",
    "영업수익",
    "영업이익",
    "operating income",
    "operating profit",
    "자산",
    "부채",
    "자본",
]


def _is_income_statement_row(row: dict) -> bool:
    sj_div = str(row.get("sj_div") or "").strip().upper()
    if sj_div and sj_div not in {"IS", "CIS"}:
        return False
    sj_nm = str(row.get("sj_nm") or "").strip()
    if sj_nm and any(x in sj_nm for x in ["재무상태표", "현금흐름표", "자본변동표"]):
        return False
    return True


def _pick_dart_value(rows: list, ids: set, names: list, excludes: list[str] | None = None) -> float:
    excludes = [str(x).lower() for x in (excludes or [])]

    for row in rows:
        if not _is_income_statement_row(row):
            continue
        aid = str(row.get("account_id") or "").strip()
        if aid in ids:
            v = _to_float(row.get("thstrm_amount"))
            if np.isfinite(v):
                return v

    for row in rows:
        if not _is_income_statement_row(row):
            continue
        anm = str(row.get("account_nm") or "").strip().lower()
        if any(ex in anm for ex in excludes):
            continue
        if any(str(n).lower() in anm for n in names):
            v = _to_float(row.get("thstrm_amount"))
            if np.isfinite(v):
                return v
    return np.nan


def _fetch_quarter(
    session: requests.Session,
    api_key: str,
    corp_code: str,
    bsns_year: int,
    reprt_code: str,
    fs_div: str,
    sleep_sec: float,
    quality_stats: Optional[dict] = None,
    retry_max: int = 0,
    backoff_base: float = 0.5,
    backoff_cap: float = 4.0,
    circuit_fail_threshold: int = 8,
) -> Optional[tuple]:
    """Fetch one quarterly DART report and return revenue, operating profit, and net income."""
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": str(bsns_year),
        "reprt_code": reprt_code,
        "fs_div": fs_div,
    }
    try:
        if bool(quality_stats and quality_stats.get("dart_circuit_open", False)):
            _stat_inc(quality_stats, "dart_circuit_skip")
            return None
        if quality_stats is not None:
            quality_stats["dart_attempts"] = int(quality_stats.get("dart_attempts", 0)) + 1
        d = _http_get_with_retry(
            session,
            FNLTT_URL,
            params=params,
            timeout=30.0,
            expect_json=True,
            retry_max=retry_max,
            backoff_base=backoff_base,
            backoff_cap=backoff_cap,
            stats=quality_stats,
            stats_prefix="dart_",
        )
        if not isinstance(d, dict):
            _stat_inc(quality_stats, "dart_errors")
            _stat_inc(quality_stats, "dart_consecutive_failures")
            if quality_stats is not None and int(quality_stats.get("dart_consecutive_failures", 0)) >= int(circuit_fail_threshold):
                quality_stats["dart_circuit_open"] = True
            return None
        status = str(d.get("status") or "").strip()
        if status != "000":
            _stat_inc_nested(quality_stats, "dart_status_counts", status or "UNKNOWN")
            if status in NO_DATA_STATUSES:
                _stat_inc(quality_stats, "dart_no_data")
                _log("WARNING", "dart_no_data", corp_code=corp_code, bsns_year=bsns_year, reprt_code=reprt_code, fs_div=fs_div, api_status=status)
                return None
            _stat_inc(quality_stats, "dart_errors")
            _stat_inc(quality_stats, "dart_api_status_errors")
            _log("ERROR", "dart_api_status_error", corp_code=corp_code, bsns_year=bsns_year, reprt_code=reprt_code, fs_div=fs_div, api_status=status)
            if quality_stats is not None:
                quality_stats["dart_consecutive_failures"] = int(quality_stats.get("dart_consecutive_failures", 0)) + 1
                if int(quality_stats.get("dart_consecutive_failures", 0)) >= int(circuit_fail_threshold):
                    quality_stats["dart_circuit_open"] = True
                    _log("ERROR", "dart_circuit_open", threshold=circuit_fail_threshold)
            return None
        if not isinstance(d.get("list"), list):
            _stat_inc(quality_stats, "dart_errors")
            _stat_inc(quality_stats, "dart_schema_errors")
            _log("ERROR", "dart_schema_error", corp_code=corp_code, bsns_year=bsns_year, reprt_code=reprt_code, fs_div=fs_div)
            if quality_stats is not None:
                quality_stats["dart_consecutive_failures"] = int(quality_stats.get("dart_consecutive_failures", 0)) + 1
                if int(quality_stats.get("dart_consecutive_failures", 0)) >= int(circuit_fail_threshold):
                    quality_stats["dart_circuit_open"] = True
                    _log("ERROR", "dart_circuit_open", threshold=circuit_fail_threshold)
            return None
        if not d.get("list"):
            _stat_inc(quality_stats, "dart_no_data")
            return None
        rows = d["list"]
        rev = _pick_dart_value(rows, DART_REVENUE_IDS, DART_REVENUE_NAMES, EXCLUDE_REVENUE_NAMES)
        op  = _pick_dart_value(rows, DART_OP_IDS,      DART_OP_NAMES,      EXCLUDE_OP_NAMES)
        net = _pick_dart_value(rows, DART_NP_IDS,      DART_NP_NAMES,      EXCLUDE_NET_NAMES)
        if np.isfinite(rev) and np.isfinite(op) and rev > 0 and abs(op) > (abs(rev) * 1.1):
            op = np.nan
        if np.isfinite(rev):
            if quality_stats is not None:
                quality_stats["dart_consecutive_failures"] = 0
            return rev, op, net
    except Exception:
        if quality_stats is not None:
            quality_stats["dart_errors"] = int(quality_stats.get("dart_errors", 0)) + 1
            quality_stats["dart_consecutive_failures"] = int(quality_stats.get("dart_consecutive_failures", 0)) + 1
            if int(quality_stats.get("dart_consecutive_failures", 0)) >= int(circuit_fail_threshold):
                quality_stats["dart_circuit_open"] = True
        pass
    finally:
        if sleep_sec > 0:
            time.sleep(sleep_sec)
    return None


def _fetch_ttm(
    session: requests.Session,
    api_key: str,
    corp_code: str,
    as_of_year: int,
    sleep_sec: float,
    quality_stats: Optional[dict] = None,
    retry_max: int = 0,
    backoff_base: float = 0.5,
    backoff_cap: float = 4.0,
    circuit_fail_threshold: int = 8,
) -> dict:
    """
    Calculate TTM (Trailing Twelve Months).
    Summary: use prior FY plus the latest available current-year quarter delta.
      TTM = FY(n-1) + [Q3(n) - Q3(n-1)] or [H1(n) - H1(n-1)] or [Q1(n) - Q1(n-1)]
    """
    result = {"ttm_revenue": np.nan, "ttm_operating_profit": np.nan, "ttm_net_income": np.nan}
    cur_year = as_of_year
    prev_year = as_of_year - 1

    # Fetch prior fiscal-year report.
    for fs_div in ("CFS", "OFS"):
        fy_prev = _fetch_quarter(
            session,
            api_key,
            corp_code,
            prev_year,
            "11011",
            fs_div,
            sleep_sec,
            quality_stats=quality_stats,
            retry_max=retry_max,
            backoff_base=backoff_base,
            backoff_cap=backoff_cap,
            circuit_fail_threshold=circuit_fail_threshold,
        )
        if fy_prev:
            break

    if not fy_prev:
        return result

    # Fetch latest available current-year quarter pair: Q3 > H1 > Q1.
    latest_cur = None
    latest_prev = None
    for reprt_code, _ in [("11014", "Q3"), ("11012", "H1"), ("11013", "Q1")]:
        for fs_div in ("CFS", "OFS"):
            cur = _fetch_quarter(
                session,
                api_key,
                corp_code,
                cur_year,
                reprt_code,
                fs_div,
                sleep_sec,
                quality_stats=quality_stats,
                retry_max=retry_max,
                backoff_base=backoff_base,
                backoff_cap=backoff_cap,
                circuit_fail_threshold=circuit_fail_threshold,
            )
            prev = _fetch_quarter(
                session,
                api_key,
                corp_code,
                prev_year,
                reprt_code,
                fs_div,
                sleep_sec,
                quality_stats=quality_stats,
                retry_max=retry_max,
                backoff_base=backoff_base,
                backoff_cap=backoff_cap,
                circuit_fail_threshold=circuit_fail_threshold,
            )
            if cur and prev:
                latest_cur  = cur
                latest_prev = prev
                break
        if latest_cur:
            break

    if latest_cur and latest_prev:
        # TTM = FY(n-1) + [latest quarter(n) - latest quarter(n-1)].
        result["ttm_revenue"]           = fy_prev[0] + (latest_cur[0] - latest_prev[0])
        result["ttm_operating_profit"]  = fy_prev[1] + (latest_cur[1] - latest_prev[1])
        result["ttm_net_income"]        = fy_prev[2] + (latest_cur[2] - latest_prev[2])
    else:
        # 분기 데이터가 없으면 FY(n-1) 값을 그대로 사용
        result["ttm_revenue"] = fy_prev[0]
        result["ttm_operating_profit"]  = fy_prev[1]
        result["ttm_net_income"]        = fy_prev[2]

    # 최종 일관성 가드: 영업이익이 매출보다 과도하게 큰 값은 폐기
    if (
        np.isfinite(result["ttm_revenue"])
        and np.isfinite(result["ttm_operating_profit"])
        and result["ttm_revenue"] > 0
        and abs(result["ttm_operating_profit"]) > (abs(result["ttm_revenue"]) * 1.1)
    ):
        result["ttm_operating_profit"] = np.nan
    if (
        np.isfinite(result["ttm_revenue"])
        and np.isfinite(result["ttm_net_income"])
        and result["ttm_revenue"] > 0
        and abs(result["ttm_net_income"]) > (abs(result["ttm_revenue"]) * 1.1)
    ):
        result["ttm_net_income"] = np.nan

    return result


# ---------------------------------------------------------------------------
def _compute_derived(row: dict, current_price: float, revenue_growth: float) -> dict:
    _ensure(isinstance(row, dict), "[ERR] row must be dict")
    _ensure(isinstance(current_price, (int, float, np.floating)), "[ERR] current_price must be numeric")
    _ensure(isinstance(revenue_growth, (int, float, np.floating)), "[ERR] revenue_growth must be numeric")
    fwd_per = row.get("forward_per", np.nan)
    ttm_net = row.get("ttm_net_income", np.nan)
    ttm_rev = row.get("ttm_revenue", np.nan)
    ttm_op  = row.get("ttm_operating_profit", np.nan)

    # Forward EPS: current_price / forward_per.
    forward_eps = np.nan
    if np.isfinite(fwd_per) and fwd_per > 0 and np.isfinite(current_price) and current_price > 0:
        forward_eps = current_price / fwd_per

    # TTM OPM
    ttm_opm = _safe_div(ttm_op, ttm_rev) * 100 if np.isfinite(ttm_op) and np.isfinite(ttm_rev) else np.nan

    # PEG ratio: forward_per / YoY revenue growth, only when growth is positive.
    peg_ratio = np.nan
    if np.isfinite(fwd_per) and fwd_per > 0 and np.isfinite(revenue_growth) and revenue_growth > 1.0:
        peg_ratio = fwd_per / revenue_growth

    out = {
        "forward_eps": round(forward_eps, 2) if np.isfinite(forward_eps) else np.nan,
        "ttm_opm":     round(ttm_opm, 4)    if np.isfinite(ttm_opm)    else np.nan,
        "peg_ratio":   round(peg_ratio, 4)   if np.isfinite(peg_ratio)  else np.nan,
    }
    _dev_assert(set(out.keys()) == {"forward_eps", "ttm_opm", "peg_ratio"}, "postcondition failed: derived keys")
    _dev_assert(all(np.isfinite(v) or pd.isna(v) for v in out.values()), "postcondition failed: derived contains non-finite")
    return out


# ---------------------------------------------------------------------------
def _load_target_codes(codes_file: Optional[str], max_codes: int) -> tuple[list[str], str]:
    if codes_file:
        p = Path(codes_file)
        if not p.exists():
            raise SystemExit(f"[ERR] codes file not found: {codes_file}")
        df = pd.read_csv(p, dtype=str)
        col = next((c for c in df.columns if c.lower() in ("code", "ticker", "醫낅ぉ肄붾뱶")), None)
        if not col:
            raise SystemExit(f"[ERR] invalid codes file schema (code/ticker required): {codes_file}")
        return _dedupe_codes(df[col].dropna().tolist(), max_codes), str(p)

    # This snapshot is built before final_score is regenerated in the official
    # batch. Prefer current candidate-chain inputs over a possibly stale
    # with_final_score file from a previous run.
    for p in [
        LOG_DIR / "candidates_latest_data.with_news_score.csv",
        LOG_DIR / "candidates_latest_data.with_policy_score.csv",
        LOG_DIR / "candidates_latest_data.filtered.csv",
        LOG_DIR / "candidates_latest_data.csv",
        LOG_DIR / "candidates_latest_data.with_final_score.csv",
    ]:
        if p.exists():
            df = pd.read_csv(p, dtype={"code": str})
            if "code" in df.columns:
                return _dedupe_codes(df["code"].dropna().tolist(), max_codes), str(p)
    return [], ""


def _default_asof_from_candidates() -> str:
    # This snapshot is built before final_score is regenerated in the official
    # batch. Prefer current candidate-chain inputs over a possibly stale
    # with_final_score file from a previous run.
    for p in [
        LOG_DIR / "candidates_latest_data.with_news_score.csv",
        LOG_DIR / "candidates_latest_data.with_policy_score.csv",
        LOG_DIR / "candidates_latest_data.filtered.csv",
        LOG_DIR / "candidates_latest_data.csv",
        LOG_DIR / "candidates_latest_data.with_final_score.csv",
    ]:
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p, dtype=str)
        except Exception:
            continue
        for col in ["date_yyyymmdd", "date", "signal_date", "as_of_ymd"]:
            if col in df.columns:
                s = df[col].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
                s = s[s.str.fullmatch(r"\d{8}", na=False)]
                if len(s):
                    return str(s.max())
    return datetime.now().strftime("%Y%m%d")


def _load_price_map() -> tuple[dict, dict]:
    """Load current close prices from candidate CSV files."""
    for p in [
        LOG_DIR / "candidates_latest_data.with_news_score.csv",
        LOG_DIR / "candidates_latest_data.with_policy_score.csv",
        LOG_DIR / "candidates_latest_data.filtered.csv",
        LOG_DIR / "candidates_latest_data.csv",
        LOG_DIR / "candidates_latest_data.with_final_score.csv",
    ]:
        if p.exists():
            df = pd.read_csv(p, dtype={"code": str})
            if "code" in df.columns and "close" in df.columns:
                work = pd.DataFrame(
                    {
                        "code": df["code"].map(_norm_code6),
                        "close": pd.to_numeric(df["close"], errors="coerce"),
                    }
                )
                work = work[work["code"].map(_valid_code6)]
                dup_n = int(work.duplicated(["code"]).sum())
                if dup_n > 0:
                    _log_print(f"[WARN] price_map duplicate code={dup_n} (last value wins): {p}")
                work = work.drop_duplicates(subset=["code"], keep="last")
                return dict(zip(work["code"], work["close"])), {
                    "source_path": str(p),
                    "source_rows": int(len(df)),
                    "dedup_rows": int(len(work)),
                    "dup_code_count": dup_n,
                }
    return {}, {"source_path": "", "source_rows": 0, "dedup_rows": 0, "dup_code_count": 0}


def _load_growth_map() -> tuple[dict, dict]:
    """dart_fundamental_latest.csv?먯꽌 醫낅ぉ蹂?revenue_growth 濡쒕뱶."""
    p = CACHE_DIR / "dart_fundamental_latest.csv"
    if p.exists():
        df = pd.read_csv(p, dtype={"code": str})
        if "code" in df.columns and "revenue_growth" in df.columns:
            work = pd.DataFrame(
                {
                    "code": df["code"].map(_norm_code6),
                    "revenue_growth": pd.to_numeric(df["revenue_growth"], errors="coerce"),
                }
            )
            work = work[work["code"].map(_valid_code6)]
            dup_n = int(work.duplicated(["code"]).sum())
            if dup_n > 0:
                _log_print(f"[WARN] growth_map duplicate code={dup_n} (last value wins): {p}")
            work = work.drop_duplicates(subset=["code"], keep="last")
            return dict(zip(work["code"], work["revenue_growth"])), {
                "source_path": str(p),
                "source_rows": int(len(df)),
                "dedup_rows": int(len(work)),
                "dup_code_count": dup_n,
            }
    return {}, {"source_path": "", "source_rows": 0, "dedup_rows": 0, "dup_code_count": 0}


def _validate_before_save(
    out_df: pd.DataFrame,
    as_of_ymd: str,
    expected_count: int,
    *,
    strict: bool,
    require_wisereport: bool,
    require_ttm: bool,
    min_source_coverage: float,
) -> dict:
    _ensure(isinstance(out_df, pd.DataFrame), "[ERR] out_df must be DataFrame")
    _ensure(isinstance(expected_count, int) and expected_count >= 0, "[ERR] expected_count must be non-negative int")
    _ensure(isinstance(strict, bool), "[ERR] strict must be bool")
    _ensure(isinstance(require_wisereport, bool), "[ERR] require_wisereport must be bool")
    _ensure(isinstance(require_ttm, bool), "[ERR] require_ttm must be bool")
    _ensure(0.0 <= float(min_source_coverage) <= 1.0, "[ERR] min_source_coverage must be 0~1")
    required = ["code", "as_of_ymd", "run_id", "updated_at", "forward_per", "ttm_revenue", "peg_ratio"]
    missing = [c for c in required if c not in out_df.columns]
    if missing:
        raise SystemExit(f"[ERR] missing required columns: {missing}")

    _normalize_asof_ymd(as_of_ymd)
    if out_df["as_of_ymd"].astype(str).nunique() != 1 or str(out_df["as_of_ymd"].iloc[0]) != as_of_ymd:
        raise SystemExit("[ERR] mixed or mismatched as_of_ymd in output")
    if out_df["run_id"].astype(str).nunique() != 1 or str(out_df["run_id"].iloc[0]).strip() == "":
        raise SystemExit("[ERR] missing or mixed run_id in output")

    bad_code = ~out_df["code"].astype(str).map(_valid_code6)
    bad_code_n = int(bad_code.sum())
    if bad_code_n > 0:
        samples = out_df.loc[bad_code, "code"].astype(str).head(5).tolist()
        raise SystemExit(f"[ERR] invalid code rows={bad_code_n}, sample={samples}")

    dup_n = int(out_df.duplicated(["code", "as_of_ymd"]).sum())
    if dup_n > 0:
        raise SystemExit(f"[ERR] duplicate rows on code+as_of_ymd: {dup_n}")

    if len(out_df) != expected_count:
        raise SystemExit(f"[ERR] output row count mismatch: out={len(out_df)} expected={expected_count}")

    cov_fwd = float(out_df["forward_per"].notna().mean()) if len(out_df) else 0.0
    cov_ttm = float(out_df["ttm_revenue"].notna().mean()) if len(out_df) else 0.0
    cov_any = float((out_df["forward_per"].notna() | out_df["ttm_revenue"].notna()).mean()) if len(out_df) else 0.0
    if strict:
        if require_wisereport and cov_fwd < min_source_coverage:
            raise SystemExit(f"[ERR] forward_per coverage too low: {cov_fwd:.2%} < {min_source_coverage:.2%}")
        if require_ttm and cov_ttm < min_source_coverage:
            raise SystemExit(f"[ERR] ttm_revenue coverage too low: {cov_ttm:.2%} < {min_source_coverage:.2%}")
        if (require_wisereport or require_ttm) and cov_any < min_source_coverage:
            raise SystemExit(f"[ERR] overall coverage too low: {cov_any:.2%} < {min_source_coverage:.2%}")
    out = {
        "dup_code_asof": dup_n,
        "invalid_code_rows": bad_code_n,
        "coverage_forward_per": cov_fwd,
        "coverage_ttm_revenue": cov_ttm,
        "coverage_any": cov_any,
    }
    _dev_assert(set(out.keys()) == {"dup_code_asof", "invalid_code_rows", "coverage_forward_per", "coverage_ttm_revenue", "coverage_any"}, "postcondition failed: validation keys")
    _dev_assert(0.0 <= out["coverage_forward_per"] <= 1.0, "postcondition failed: coverage_forward_per range")
    _dev_assert(0.0 <= out["coverage_ttm_revenue"] <= 1.0, "postcondition failed: coverage_ttm_revenue range")
    _dev_assert(0.0 <= out["coverage_any"] <= 1.0, "postcondition failed: coverage_any range")
    return out


# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build Forward Estimate Snapshot")
    ap.add_argument("--codes-file",  default="", help="CSV file with code column")
    ap.add_argument("--max-codes",   type=int, default=150, help="Max codes to process")
    ap.add_argument("--sleep",       type=float, default=0.2, help="Sleep between API calls")
    ap.add_argument("--as-of",       default=_default_asof_from_candidates())
    ap.add_argument("--skip-ttm",    action="store_true", help="Skip DART TTM (WiseReport only)")
    ap.add_argument("--skip-wisereport", action="store_true", help="Skip WiseReport scraping")
    ap.add_argument("--strict", action=argparse.BooleanOptionalAction, default=True, help="Fail closed on input/quality gate violations")
    ap.add_argument("--max-external-fail-rate", type=float, default=0.5, help="Max tolerated external call error ratio [0,1]")
    ap.add_argument("--min-source-coverage", type=float, default=0.3, help="Minimum non-null coverage per enabled source [0,1]")
    ap.add_argument("--self-test", action="store_true", help="Run FSM transition tests and exit")
    ap.add_argument("--contract-test", action="store_true", help="Run pre/post-condition tests and exit")
    ap.add_argument("--validation-test", action="store_true", help="Run validation-layer tests and exit")
    ap.add_argument("--idempotency-key-override", default="", help="Override idempotency key (testing only)")
    ap.add_argument("--fail-after-rows", type=int, default=-1, help="Force failure after N rows for retry test")
    ap.add_argument("--in-progress-ttl-min", type=int, default=120, help="TTL minutes for stale in-progress marker")
    ap.add_argument("--http-retry-max", type=int, default=2, help="Max retries for external HTTP transient failures")
    ap.add_argument("--http-backoff-base", type=float, default=0.5, help="Base seconds for exponential backoff")
    ap.add_argument("--http-backoff-cap", type=float, default=4.0, help="Max seconds per retry backoff")
    ap.add_argument("--dart-circuit-threshold", type=int, default=8, help="Consecutive DART failures to open circuit")
    ap.add_argument("--output",      default=str(OUTPUT_CSV))
    return ap.parse_args()


def main() -> int:
    run_t0 = time.time()
    run_id = ""
    args = _parse_args()
    _log(
        "INFO",
        "run_start",
        as_of=str(args.as_of),
        output=str(args.output),
        strict=bool(args.strict),
        skip_ttm=bool(args.skip_ttm),
        skip_wisereport=bool(args.skip_wisereport),
    )
    config_snapshot = {
        "as_of": str(args.as_of),
        "output": str(args.output),
        "strict": bool(args.strict),
        "skip_ttm": bool(args.skip_ttm),
        "skip_wisereport": bool(args.skip_wisereport),
        "max_codes": int(args.max_codes),
        "max_external_fail_rate": float(args.max_external_fail_rate),
        "min_source_coverage": float(args.min_source_coverage),
        "http_retry_max": int(args.http_retry_max),
        "http_backoff_base": float(args.http_backoff_base),
        "http_backoff_cap": float(args.http_backoff_cap),
        "dart_circuit_threshold": int(args.dart_circuit_threshold),
        "pipeline_version": str(PIPELINE_VERSION),
    }
    config_hash = _stable_hash_obj(config_snapshot)
    if args.self_test:
        return _run_fsm_transition_tests()
    if args.contract_test:
        return _run_contract_tests()
    if args.validation_test:
        return _run_validation_layer_tests()

    state = FSM_INIT
    def _run_end_log(status: str, code: int, reason: str, **fields) -> None:
        lvl = "ERROR" if str(status).lower() == "fail" else "INFO"
        _log(
            lvl,
            "run_end",
            run_id=(run_id or None),
            status=status,
            code=int(code),
            reason=reason,
            state=state,
            duration_ms=int((time.time() - run_t0) * 1000),
            **fields,
        )

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if args.max_external_fail_rate < 0.0 or args.max_external_fail_rate > 1.0:
        state = _fsm_transition(state, "fail")
        _log_print(f"[ERR] invalid --max-external-fail-rate: {args.max_external_fail_rate} (must be 0~1)")
        _run_end_log("fail", 2, "invalid_max_external_fail_rate", value=float(args.max_external_fail_rate))
        return 2
    if args.min_source_coverage < 0.0 or args.min_source_coverage > 1.0:
        state = _fsm_transition(state, "fail")
        _log_print(f"[ERR] invalid --min-source-coverage: {args.min_source_coverage} (must be 0~1)")
        _run_end_log("fail", 2, "invalid_min_source_coverage", value=float(args.min_source_coverage))
        return 2
    if args.fail_after_rows < -1:
        state = _fsm_transition(state, "fail")
        _log_print(f"[ERR] invalid --fail-after-rows: {args.fail_after_rows} (must be -1 or >=0)")
        _run_end_log("fail", 2, "invalid_fail_after_rows", value=int(args.fail_after_rows))
        return 2
    if args.in_progress_ttl_min <= 0:
        state = _fsm_transition(state, "fail")
        _log_print(f"[ERR] invalid --in-progress-ttl-min: {args.in_progress_ttl_min} (must be >0)")
        _run_end_log("fail", 2, "invalid_in_progress_ttl_min", value=int(args.in_progress_ttl_min))
        return 2
    if args.http_retry_max < 0:
        state = _fsm_transition(state, "fail")
        _log_print(f"[ERR] invalid --http-retry-max: {args.http_retry_max} (must be >=0)")
        _run_end_log("fail", 2, "invalid_http_retry_max", value=int(args.http_retry_max))
        return 2
    if args.http_backoff_base < 0 or args.http_backoff_cap < 0 or args.http_backoff_base > args.http_backoff_cap:
        state = _fsm_transition(state, "fail")
        _log_print(f"[ERR] invalid backoff params: base={args.http_backoff_base}, cap={args.http_backoff_cap}")
        _run_end_log("fail", 2, "invalid_http_backoff", base=float(args.http_backoff_base), cap=float(args.http_backoff_cap))
        return 2
    if args.dart_circuit_threshold <= 0:
        state = _fsm_transition(state, "fail")
        _log_print(f"[ERR] invalid --dart-circuit-threshold: {args.dart_circuit_threshold} (must be >0)")
        _run_end_log("fail", 2, "invalid_dart_circuit_threshold", value=int(args.dart_circuit_threshold))
        return 2
    if args.strict and str(args.idempotency_key_override or "").strip():
        state = _fsm_transition(state, "fail")
        _log_print("[ERR] idempotency-key-override is not allowed in strict mode")
        _run_end_log("fail", 2, "invalid_idempotency_override_in_strict")
        return 2
    try:
        as_of_ymd = _normalize_asof_ymd(args.as_of)
    except SystemExit as e:
        state = _fsm_transition(state, "fail")
        _log_print(str(e))
        _run_end_log("fail", 2, "invalid_as_of_systemexit", as_of=str(args.as_of))
        return 2
    except Exception:
        state = _fsm_transition(state, "fail")
        _log_print(f"[ERR] invalid --as-of (YYYYMMDD required): {args.as_of}")
        _run_end_log("fail", 2, "invalid_as_of_exception", as_of=str(args.as_of))
        return 2
    state = _fsm_transition(state, "validate_input")

    api_key = _resolve_api_key()
    if not api_key and not args.skip_ttm:
        if args.strict:
            state = _fsm_transition(state, "fail")
            _log_print("[ERR] DART API key not found while TTM is enabled. Set key or use --skip-ttm.")
            _run_end_log("fail", 2, "missing_dart_api_key_strict")
            return 2
        _log_print("[WARN] DART API key not found. TTM calculation will be skipped.")
        args.skip_ttm = True

    try:
        codes, codes_source_path = _load_target_codes(args.codes_file or None, args.max_codes)
    except SystemExit:
        state = _fsm_transition(state, "fail")
        _run_end_log("fail", 2, "load_target_codes_failed", codes_file=str(args.codes_file or ""))
        raise
    if not codes:
        state = _fsm_transition(state, "fail")
        _log_print("[ERR] No target codes found.")
        _run_end_log("fail", 2, "no_target_codes")
        return 2
    _log_print(f"[FWD] target codes: {len(codes)}")

    out_path = Path(args.output)
    decision_audit_path = _audit_path_for_output(out_path)
    state_meta_json, state_marker_json, state_lineage_json = _state_files_for_output(out_path)
    lock_path = out_path.with_suffix(out_path.suffix + ".lock")
    if not _acquire_run_lock(lock_path):
        state = _fsm_transition(state, "fail")
        _log("ERROR", "lock_acquire_failed", lock_path=str(lock_path), output=str(out_path))
        _log_print(f"[ERR] another process is writing this output: {lock_path}")
        _run_end_log("fail", 6, "lock_acquire_failed", lock_path=str(lock_path), output=str(out_path))
        return 6
    _log("INFO", "lock_acquired", lock_path=str(lock_path), output=str(out_path))
    atexit.register(_release_run_lock, lock_path)
    _cleanup_stale_tmp([out_path, decision_audit_path, state_meta_json, state_lineage_json, state_marker_json, META_JSON, RUN_LINEAGE_JSON, RUN_MARKER_JSON])
    codes_sha = _codes_digest(codes)
    idempotency_key = _compute_idempotency_key(
        as_of_ymd,
        codes,
        skip_ttm=bool(args.skip_ttm),
        skip_wisereport=bool(args.skip_wisereport),
        strict=bool(args.strict),
        max_external_fail_rate=float(args.max_external_fail_rate),
        min_source_coverage=float(args.min_source_coverage),
        pipeline_version=PIPELINE_VERSION,
        override=str(args.idempotency_key_override or ""),
    )
    run_marker = _load_json_if_exists(state_marker_json, strict=bool(args.strict), label="run marker")
    if str(run_marker.get("status") or "") == "in_progress":
        marker_age_min = 0.0
        try:
            started_raw = str(run_marker.get("started_at") or run_marker.get("updated_at") or "")
            started_at = datetime.strptime(started_raw, "%Y-%m-%d %H:%M:%S")
            marker_age_min = max(0.0, (datetime.now() - started_at).total_seconds() / 60.0)
        except Exception:
            marker_age_min = float(args.in_progress_ttl_min + 1)
        if marker_age_min > float(args.in_progress_ttl_min):
            _log("WARNING", "stale_in_progress_marker_cleared", marker_age_min=round(marker_age_min, 3), ttl_min=int(args.in_progress_ttl_min))
            _log_print(f"[WARN] stale in-progress marker cleared: age_min={marker_age_min:.1f}")
            _clear_run_marker(state_marker_json)
            run_marker = {}
    if str(run_marker.get("status") or "") == "in_progress":
        marker_key = str(run_marker.get("idempotency_key") or "")
        marker_out = str(run_marker.get("output") or "")
        if marker_key == idempotency_key and marker_out == str(out_path):
            _log("WARNING", "in_progress_same_key_resume", idempotency_key=idempotency_key, output=str(out_path))
            _log_print(f"[WARN] resume same in-progress run key={idempotency_key}")
        elif args.strict:
            state = _fsm_transition(state, "fail")
            _log("ERROR", "in_progress_conflict_strict", existing_key=marker_key, idempotency_key=idempotency_key, output=str(out_path))
            _log_print(f"[ERR] another in-progress run exists (key={marker_key}). clear marker or finish previous run.")
            _run_end_log("fail", 5, "in_progress_conflict_strict", existing_key=marker_key, idempotency_key=idempotency_key, output=str(out_path))
            return 5
        else:
            _log("WARNING", "in_progress_conflict_non_strict", existing_key=marker_key, idempotency_key=idempotency_key, output=str(out_path))
            _log_print(f"[WARN] in-progress marker exists but strict=false. continue overwrite: key={marker_key}")
    old_meta = _load_json_if_exists(state_meta_json, strict=bool(args.strict), label="state meta")
    try:
        old_total_codes = int(old_meta.get("total_codes", -1))
    except Exception:
        old_total_codes = -1
    old_lineage = _load_json_if_exists(state_lineage_json, strict=bool(args.strict), label="state lineage")
    if (
        str(old_meta.get("idempotency_key") or "") == idempotency_key
        and str(old_meta.get("as_of_ymd") or "") == as_of_ymd
        and str(old_meta.get("code_set_sha") or "") == codes_sha
        and old_total_codes == len(codes)
        and str(run_marker.get("status") or "") == "committed"
        and _is_reusable_output(out_path, expected_count=len(codes), as_of_ymd=as_of_ymd)
        and _is_committed_state_consistent(
            meta_obj=old_meta,
            lineage_obj=old_lineage,
            marker_obj=run_marker,
            as_of_ymd=as_of_ymd,
            out_path=out_path,
            idempotency_key=idempotency_key,
            code_set_sha=codes_sha,
            expected_count=len(codes),
        )
    ):
        if old_meta:
            lineage_snapshot = _build_lineage_snapshot(old_meta)
            _atomic_write_text(state_lineage_json, json.dumps(lineage_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            _atomic_write_text(RUN_LINEAGE_JSON, json.dumps(lineage_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        _log("INFO", "idempotent_skip", idempotency_key=idempotency_key, as_of_ymd=as_of_ymd, output=str(out_path), total_codes=len(codes))
        _log_print(f"[IDEMPOTENT] skip persist (same input key={idempotency_key})")
        _release_run_lock(lock_path)
        _run_end_log("skip", 0, "idempotent_skip", idempotency_key=idempotency_key, output=str(out_path))
        return 0

    price_map, price_map_stats = _load_price_map()
    growth_map, growth_map_stats = _load_growth_map()

    corp_map_df = pd.DataFrame(columns=["code", "corp_code"])
    if CORP_MAP.exists():
        corp_map_df = pd.read_csv(CORP_MAP, dtype=str)
        if "code" not in corp_map_df.columns or "corp_code" not in corp_map_df.columns:
            state = _fsm_transition(state, "fail")
            _log_print(f"[ERR] invalid corp map schema: {CORP_MAP} (requires code, corp_code)")
            _run_end_log("fail", 2, "invalid_corp_map_schema", corp_map=str(CORP_MAP))
            return 2
        corp_map_df["code"] = corp_map_df["code"].map(_norm_code6)
        corp_map_df["corp_code"] = corp_map_df["corp_code"].astype(str).str.strip()
        corp_map_df = corp_map_df[corp_map_df["code"].map(_valid_code6)]
        corp_map_df = corp_map_df[corp_map_df["corp_code"].str.fullmatch(r"\d{8}", na=False)]
        corp_map_df = corp_map_df.drop_duplicates(subset=["code"], keep="last")

    code_to_corp = dict(zip(corp_map_df["code"], corp_map_df["corp_code"])) if "corp_code" in corp_map_df.columns else {}
    state = _fsm_transition(state, "prepare_sources")
    _require_state(state, FSM_SOURCES_READY, "collect_rows")

    as_of_year = int(as_of_ymd[:4])
    session    = requests.Session()
    rows_out: list[dict] = []
    quality_stats: dict = {
        "wr_attempts": 0,
        "wr_errors": 0,
        "dart_attempts": 0,
        "dart_errors": 0,
        "dart_api_status_errors": 0,
        "dart_no_data": 0,
        "dart_status_counts": {},
        "dart_consecutive_failures": 0,
        "dart_circuit_open": False,
    }
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_id = f"FWD_{as_of_ymd}_{idempotency_key}"
    _log("INFO", "run_context", run_id=run_id, as_of_ymd=as_of_ymd, total_codes=len(codes), idempotency_key=idempotency_key, config_hash=config_hash[:16])
    decision_audit_rows: list[dict] = []

    for i, code in enumerate(codes, 1):
        _log_print(f"[{i:3d}/{len(codes)}] {code}", end=" ")
        row: dict = {
            "code": code,
            "as_of_ymd": as_of_ymd,
            "run_id": run_id,
            "forward_per": np.nan,
            "current_per_wr": np.nan,
            "eps_wr": np.nan,
            "ttm_revenue": np.nan,
            "ttm_operating_profit": np.nan,
            "ttm_net_income": np.nan,
            "forward_eps": np.nan,
            "ttm_opm": np.nan,
            "peg_ratio": np.nan,
            "updated_at": ts,
        }

        # 1. WiseReport 異붿젙PER
        if not args.skip_wisereport:
            wr = _scrape_wisereport(
                code,
                session,
                quality_stats=quality_stats,
                retry_max=int(args.http_retry_max),
                backoff_base=float(args.http_backoff_base),
                backoff_cap=float(args.http_backoff_cap),
            )
            row.update({k: v for k, v in wr.items() if k != "code"})
            time.sleep(args.sleep * 0.5)

        # 2. DART TTM
        if not args.skip_ttm and code in code_to_corp:
            corp_code = str(code_to_corp[code]).zfill(8)
            ttm = _fetch_ttm(
                session,
                api_key,
                corp_code,
                as_of_year,
                args.sleep,
                quality_stats=quality_stats,
                retry_max=int(args.http_retry_max),
                backoff_base=float(args.http_backoff_base),
                backoff_cap=float(args.http_backoff_cap),
                circuit_fail_threshold=int(args.dart_circuit_threshold),
            )
            row.update(ttm)

        # 3. 파생 지표
        price = float(price_map.get(code, np.nan) or np.nan)
        growth = float(growth_map.get(code, np.nan) or np.nan)
        derived = _compute_derived(row, price, growth)
        row.update(derived)
        _validate_transform_row(row, strict=bool(args.strict))
        wr_applicable = not bool(args.skip_wisereport)
        wr_ok = bool(np.isfinite(row.get("forward_per", np.nan))) if wr_applicable else True
        ttm_applicable = (not bool(args.skip_ttm)) and (code in code_to_corp)
        ttm_ok = bool(np.isfinite(row.get("ttm_revenue", np.nan))) if ttm_applicable else True
        if (not wr_applicable) and (not ttm_applicable):
            decision_status = "NOT_EVALUABLE"
            reason_code = "NO_APPLICABLE_SOURCE"
        elif wr_ok and ttm_ok:
            decision_status = "PASS"
            reason_code = "PASS_ALL_APPLICABLE_SOURCES"
        elif (not wr_ok) and (not ttm_ok):
            decision_status = "PARTIAL"
            reason_code = "MISSING_WISEREPORT_AND_TTM"
        elif not wr_ok:
            decision_status = "PARTIAL"
            reason_code = "MISSING_WISEREPORT"
        else:
            decision_status = "PARTIAL"
            reason_code = "MISSING_TTM"
        decision_audit_rows.append(
            {
                "run_id": run_id,
                "as_of_ymd": as_of_ymd,
                "code": code,
                "decision_status": decision_status,
                "reason_code": reason_code,
                "source_success_map": {
                    "wisereport_applicable": bool(wr_applicable),
                    "wisereport_forward_per": wr_ok,
                    "ttm_revenue": ttm_ok,
                    "ttm_applicable": bool(ttm_applicable),
                    "skip_ttm": bool(args.skip_ttm),
                    "skip_wisereport": bool(args.skip_wisereport),
                },
                "updated_at": ts,
            }
        )

        rows_out.append(row)
        if args.fail_after_rows >= 0 and len(rows_out) >= args.fail_after_rows:
            state = _fsm_transition(state, "fail")
            _log_print(f"[ERR] forced failure for retry test: fail_after_rows={args.fail_after_rows}")
            _run_end_log("fail", 9, "forced_failure_for_retry_test", fail_after_rows=int(args.fail_after_rows), rows_out=int(len(rows_out)))
            return 9
        status_parts = []
        if np.isfinite(row.get("forward_per", np.nan)):
            status_parts.append(f"fwd_per={row['forward_per']:.1f}")
        if np.isfinite(row.get("ttm_revenue", np.nan)):
            status_parts.append("ttm=ok")
        if np.isfinite(row.get("peg_ratio", np.nan)):
            status_parts.append(f"peg={row['peg_ratio']:.2f}")
        _log_print(" | ".join(status_parts) if status_parts else "no_data")

    if not rows_out:
        state = _fsm_transition(state, "fail")
        _log_print("[ERR] No data collected.")
        _run_end_log("fail", 3, "no_data_collected")
        return 3

    out_df = pd.DataFrame(rows_out)
    state = _fsm_transition(state, "collect_done")
    _require_state(state, FSM_COLLECTED, "quality_gate")
    wr_attempts = int(quality_stats.get("wr_attempts", 0))
    wr_errors = int(quality_stats.get("wr_errors", 0))
    dart_attempts = int(quality_stats.get("dart_attempts", 0))
    dart_errors = int(quality_stats.get("dart_errors", 0))
    wr_fail_rate = (wr_errors / wr_attempts) if wr_attempts > 0 else 0.0
    dart_fail_rate = (dart_errors / dart_attempts) if dart_attempts > 0 else 0.0
    if args.strict:
        if wr_attempts > 0 and wr_fail_rate > args.max_external_fail_rate:
            state = _fsm_transition(state, "fail")
            _log("ERROR", "quality_gate_fail", run_id=run_id, source="wisereport", fail_rate=wr_fail_rate, threshold=float(args.max_external_fail_rate))
            _log_print(f"[ERR] WiseReport fail rate too high: {wr_fail_rate:.2%} > {args.max_external_fail_rate:.2%}")
            _run_end_log("fail", 4, "quality_gate_fail_wisereport", fail_rate=float(wr_fail_rate), threshold=float(args.max_external_fail_rate))
            return 4
        if dart_attempts > 0 and dart_fail_rate > args.max_external_fail_rate:
            state = _fsm_transition(state, "fail")
            _log("ERROR", "quality_gate_fail", run_id=run_id, source="dart", fail_rate=dart_fail_rate, threshold=float(args.max_external_fail_rate))
            _log_print(f"[ERR] DART fail rate too high: {dart_fail_rate:.2%} > {args.max_external_fail_rate:.2%}")
            _run_end_log("fail", 4, "quality_gate_fail_dart", fail_rate=float(dart_fail_rate), threshold=float(args.max_external_fail_rate))
            return 4
    try:
        qcheck = _validate_before_save(
            out_df,
            as_of_ymd,
            expected_count=len(codes),
            strict=bool(args.strict),
            require_wisereport=not bool(args.skip_wisereport),
            require_ttm=not bool(args.skip_ttm),
            min_source_coverage=float(args.min_source_coverage),
        )
    except SystemExit:
        state = _fsm_transition(state, "fail")
        _run_end_log("fail", 4, "validate_before_save_failed", as_of_ymd=as_of_ymd, output=str(out_path))
        raise
    state = _fsm_transition(state, "quality_pass")
    _require_state(state, FSM_QUALITY_PASSED, "persist_output")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _backup_if_exists(out_path)
    _backup_if_exists(decision_audit_path)
    _backup_if_exists(state_meta_json)
    _backup_if_exists(state_lineage_json)
    _backup_if_exists(state_marker_json)
    _log("INFO", "persist_start", run_id=run_id, rows=int(len(out_df)), output=str(out_path))
    marker_payload = {
        "status": "in_progress",
        "started_at": ts,
        "updated_at": ts,
        "as_of_ymd": as_of_ymd,
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "code_set_sha": codes_sha,
        "output": str(out_path),
    }
    _write_run_marker(state_marker_json, marker_payload)
    _write_run_marker(RUN_MARKER_JSON, marker_payload)
    try:
        _atomic_write_csv(out_path, out_df)
        if decision_audit_rows:
            audit_lines = "\n".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in decision_audit_rows) + "\n"
        else:
            audit_lines = ""
        _atomic_write_text(decision_audit_path, audit_lines, encoding="utf-8")
        _log_print(f"[OK] saved {len(out_df)} rows -> {out_path}")

        # 硫뷀? 湲곕줉
        meta = {
            "updated_at": ts,
            "as_of_ymd": as_of_ymd,
            "run_id": run_id,
            "pipeline_version": PIPELINE_VERSION,
            "total_codes": len(rows_out),
            "idempotency_key": idempotency_key,
            "code_set_sha": codes_sha,
            "forward_per_count": int(out_df["forward_per"].notna().sum()),
            "ttm_count": int(out_df["ttm_revenue"].notna().sum()),
            "peg_count": int(out_df["peg_ratio"].notna().sum()),
            "validation": qcheck,
            "quality_stats": {
                "wr_attempts": wr_attempts,
                "wr_errors": wr_errors,
                "wr_fail_rate": wr_fail_rate,
                "dart_attempts": dart_attempts,
                "dart_errors": dart_errors,
                "dart_fail_rate": dart_fail_rate,
                "wr_http_429": int(quality_stats.get("wr_http_429", 0)),
                "wr_http_5xx": int(quality_stats.get("wr_http_5xx", 0)),
                "wr_timeout_errors": int(quality_stats.get("wr_timeout_errors", 0)),
                "wr_schema_errors": int(quality_stats.get("wr_schema_errors", 0)),
                "dart_http_429": int(quality_stats.get("dart_http_429", 0)),
                "dart_http_5xx": int(quality_stats.get("dart_http_5xx", 0)),
                "dart_timeout_errors": int(quality_stats.get("dart_timeout_errors", 0)),
                "dart_schema_errors": int(quality_stats.get("dart_schema_errors", 0)),
                "dart_api_status_errors": int(quality_stats.get("dart_api_status_errors", 0)),
                "dart_no_data": int(quality_stats.get("dart_no_data", 0)),
                "dart_status_counts": quality_stats.get("dart_status_counts", {}),
                "dart_circuit_open": bool(quality_stats.get("dart_circuit_open", False)),
            },
            "source_paths": {
                "codes_source": codes_source_path,
                "price_source": str(price_map_stats.get("source_path", "")),
                "growth_source": str(growth_map_stats.get("source_path", "")),
                "corp_map": str(CORP_MAP),
            },
            "source_fingerprints": {
                "codes_source": _file_fingerprint(codes_source_path),
                "price_source": _file_fingerprint(str(price_map_stats.get("source_path", ""))),
                "growth_source": _file_fingerprint(str(growth_map_stats.get("source_path", ""))),
                "corp_map": _file_fingerprint(str(CORP_MAP)),
            },
            "config_snapshot": config_snapshot,
            "config_hash": config_hash,
            "lineage": {
                "input_codes": len(codes),
                "price_map_rows": int(price_map_stats.get("dedup_rows", 0)),
                "growth_map_rows": int(growth_map_stats.get("dedup_rows", 0)),
                "price_dup_code_count": int(price_map_stats.get("dup_code_count", 0)),
                "growth_dup_code_count": int(growth_map_stats.get("dup_code_count", 0)),
                "rows_out": len(rows_out),
                "decision_audit_rows": len(decision_audit_rows),
            },
            "state_files": {
                "meta": str(state_meta_json),
                "marker": str(state_marker_json),
                "lineage": str(state_lineage_json),
            },
            "output": str(out_path),
            "audit_trail": {
                "decision_audit_path": str(decision_audit_path),
            },
        }
        _atomic_write_text(state_meta_json, json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        lineage_snapshot = _build_lineage_snapshot(meta)
        _atomic_write_text(state_lineage_json, json.dumps(lineage_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        if not _is_committed_state_consistent(
            meta_obj=meta,
            lineage_obj=lineage_snapshot,
            marker_obj=marker_payload,
            as_of_ymd=as_of_ymd,
            out_path=out_path,
            idempotency_key=idempotency_key,
            code_set_sha=codes_sha,
            expected_count=len(codes),
        ):
            raise SystemExit("[ERR] committed consistency check failed before marker commit")

        # commit marker after all artifacts are durable
        marker_payload["status"] = "committed"
        marker_payload["committed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _write_run_marker(state_marker_json, marker_payload)
        _write_run_marker(RUN_MARKER_JSON, marker_payload)
        _atomic_write_text(META_JSON, json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        _atomic_write_text(RUN_LINEAGE_JSON, json.dumps(lineage_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        _log(
            "INFO",
            "persist_committed",
            run_id=run_id,
            output=str(out_path),
            rows=int(len(out_df)),
            forward_per_count=int(meta["forward_per_count"]),
            ttm_count=int(meta["ttm_count"]),
            peg_count=int(meta["peg_count"]),
        )
        _log_print(f"[OK] meta -> {state_meta_json}")
        _log_print(f"[SUMMARY] forward_per={meta['forward_per_count']}, ttm={meta['ttm_count']}, peg={meta['peg_count']}")
        state = _fsm_transition(state, "persist_done")
        _require_state(state, FSM_SAVED, "finalize")
        _release_run_lock(lock_path)
        _log("INFO", "run_end", run_id=run_id, status="success", duration_ms=int((time.time() - run_t0) * 1000))
        return 0
    except Exception:
        _restore_backup_if_exists(out_path)
        _restore_backup_if_exists(decision_audit_path)
        _restore_backup_if_exists(state_meta_json)
        _restore_backup_if_exists(state_lineage_json)
        _restore_backup_if_exists(state_marker_json)
        marker_payload["status"] = "failed_rollback"
        marker_payload["failed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _write_run_marker(state_marker_json, marker_payload)
        _write_run_marker(RUN_MARKER_JSON, marker_payload)
        _log(
            "ERROR",
            "persist_rollback",
            run_id=run_id,
            output=str(out_path),
            duration_ms=int((time.time() - run_t0) * 1000),
            traceback=traceback.format_exc(),
        )
        _run_end_log("fail", 1, "persist_rollback", output=str(out_path))
        _release_run_lock(lock_path)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
