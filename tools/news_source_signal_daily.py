from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
CACHE = ROOT / "_cache"
DB = ROOT / "news_trading" / "data" / "trading.db"
DART_KEY_FILE = CACHE / "dart_api_key.txt"
CORP_MAP = CACHE / "dart_corp_code_map.csv"
DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
KST = timezone(timedelta(hours=9))

TARGET_FILES = [
    LOGS / "candidates_latest_data.with_sector_score.csv",
    LOGS / "candidates_latest_data.with_news_score.csv",
    LOGS / "candidates_latest_data.filtered.csv",
    LOGS / "candidates_latest_data.csv",
    LOGS / "market_rising_latest.csv",
    LOGS / "surge_realtime_latest.csv",
]


def _now_kst() -> datetime:
    return datetime.now(KST)


def _norm_code6(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6) if s else ""


def _norm_date8(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype={"code": str, "ticker": str, "종목코드": str})
        except Exception:
            continue
    return pd.read_csv(path, dtype={"code": str, "ticker": str, "종목코드": str})


def _resolve_api_key(api_key: str = "") -> str:
    key = str(api_key or "").strip()
    if key:
        return key
    key = str(os.getenv("DART_API_KEY", "")).strip()
    if key:
        return key
    if DART_KEY_FILE.exists():
        return DART_KEY_FILE.read_text(encoding="utf-8").replace("\ufeff", "").strip()
    return ""


def _load_target_codes(max_codes: int) -> Tuple[List[str], Dict[str, str], List[str]]:
    codes: List[str] = []
    names: Dict[str, str] = {}
    sources: List[str] = []
    for path in TARGET_FILES:
        if not path.exists():
            continue
        try:
            df = _read_csv(path)
        except Exception:
            continue
        code_col = next((c for c in ["code", "ticker", "종목코드"] if c in df.columns), None)
        if not code_col:
            continue
        name_col = next((c for c in ["name", "종목명"] if c in df.columns), None)
        for _, row in df.iterrows():
            code = _norm_code6(row.get(code_col))
            if not code:
                continue
            codes.append(code)
            if name_col and code not in names:
                names[code] = str(row.get(name_col) or "").strip()
        sources.append(str(path))

    out = sorted(set(codes))
    if max_codes > 0:
        out = out[: int(max_codes)]
    return out, names, sources


def _load_corp_map() -> pd.DataFrame:
    if not CORP_MAP.exists():
        return pd.DataFrame(columns=["code", "corp_code", "corp_name"])
    try:
        df = pd.read_csv(CORP_MAP, encoding="utf-8-sig", dtype={"code": str, "corp_code": str})
    except Exception:
        df = pd.read_csv(CORP_MAP, dtype={"code": str, "corp_code": str})
    if not {"code", "corp_code"}.issubset(df.columns):
        return pd.DataFrame(columns=["code", "corp_code", "corp_name"])
    df["code"] = df["code"].map(_norm_code6)
    df["corp_code"] = df["corp_code"].astype(str).str.strip()
    if "corp_name" not in df.columns:
        df["corp_name"] = ""
    return df[(df["code"] != "") & (df["corp_code"] != "")].drop_duplicates("code")


def _load_fin_signals() -> Dict[str, str]:
    path = LOGS / "disclosure_risk_latest.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {
            str(item.get("code", "")).zfill(6): str(item.get("disclosure_signal", ""))
            for item in (data.get("items") or [])
            if isinstance(item, dict) and item.get("code")
        }
    except Exception:
        return {}


def _classify_event(report_nm: object, fin_signal: str = "") -> Tuple[float, str, str]:
    title = str(report_nm or "").strip()
    text = title.replace(" ", "")
    # 유상증자: 배정 방식 우선 분류 (제목에 방식이 포함되는 경우가 많음)
    if "유상증자" in text:
        if any(key in text for key in ("제3자배정", "제삼자배정", "3자배정")):
            return 0.6, "direct_positive", "DART:third_party_paid_in_capital"
        if any(key in text for key in ("주주배정", "주주우선공모")):
            return -0.20, "dilution_or_financing", "DART:rights_offering"
        if "일반공모" in text:
            return -0.25, "dilution_or_financing", "DART:general_public_offering"
        return -0.35, "dilution_or_financing", "DART:유상증자"
    # 무상증자: 재무 건전성 기반 함정 탐지
    if "무상증자" in text:
        if fin_signal == "NEGATIVE":
            # 영업이익 적자 등 재무 부실 기업의 무상증자 → 주가 부양 꼼수 가능성
            return 0.05, "indirect_positive", "DART:무상증자_fin_trap_risk"
        return 0.35, "direct_positive", "DART:무상증자"
    negative = [
        ("횡령", -1.0, "direct_negative"),
        ("배임", -1.0, "direct_negative"),
        ("상장폐지", -1.0, "direct_negative"),
        ("거래정지", -0.9, "direct_negative"),
        ("관리종목", -0.8, "direct_negative"),
        ("불성실공시", -0.7, "direct_negative"),
        ("전환사채", -0.25, "dilution_or_financing"),
        ("신주인수권", -0.25, "dilution_or_financing"),
        ("소송", -0.25, "indirect_negative"),
    ]
    positive = [
        ("단일판매", 0.8, "direct_positive"),
        ("공급계약", 0.8, "direct_positive"),
        ("수주", 0.7, "direct_positive"),
        ("자기주식취득", 0.45, "direct_positive"),
        ("신규시설투자", 0.35, "indirect_positive"),
        ("영업실적", 0.25, "indirect_positive"),
    ]
    for key, score, impact in negative:
        if key in text:
            return float(score), impact, f"DART:{key}"
    for key, score, impact in positive:
        if key in text:
            return float(score), impact, f"DART:{key}"
    return 0.0, "neutral", "DART:unclassified"


def _fetch_dart_list(api_key: str, corp_code: str, bgn_de: str, end_de: str, page_count: int) -> Dict[str, Any]:
    qs = urllib.parse.urlencode(
        {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": 1,
            "page_count": max(1, min(int(page_count), 100)),
            "sort": "date",
            "sort_mth": "desc",
        }
    )
    req = urllib.request.Request(f"{DART_LIST_URL}?{qs}")
    with urllib.request.urlopen(req, timeout=25) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _ensure_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS source_signals_daily (
            raw_id TEXT PRIMARY KEY,
            date8 TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            source TEXT NOT NULL,
            event_type TEXT,
            event_title TEXT,
            event_url TEXT,
            source_score REAL NOT NULL DEFAULT 0,
            impact TEXT,
            reason TEXT,
            fetched_at TEXT NOT NULL,
            run_id TEXT NOT NULL
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_source_signals_daily_code_date ON source_signals_daily(code, date8)")


def _write_rows(rows: List[Dict[str, Any]]) -> int:
    DB.parent.mkdir(parents=True, exist_ok=True)
    attempts = max(1, int(os.getenv("NEWS_SOURCE_SIGNAL_DB_RETRY", "8") or "8"))
    backoff = max(0.1, float(os.getenv("NEWS_SOURCE_SIGNAL_DB_BACKOFF_SEC", "1.0") or "1.0"))
    last_exc: Optional[Exception] = None
    for attempt in range(attempts):
        con = sqlite3.connect(str(DB), timeout=max(30.0, backoff * attempts * 2.0))
        try:
            con.execute(f"PRAGMA busy_timeout={int(max(30000.0, backoff * attempts * 2000.0))}")
            _ensure_table(con)
            for r in rows:
                con.execute(
                    """
                    INSERT INTO source_signals_daily(
                        raw_id, date8, code, name, source, event_type, event_title, event_url,
                        source_score, impact, reason, fetched_at, run_id
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(raw_id) DO UPDATE SET
                        date8=excluded.date8,
                        code=excluded.code,
                        name=excluded.name,
                        source=excluded.source,
                        event_type=excluded.event_type,
                        event_title=excluded.event_title,
                        event_url=excluded.event_url,
                        source_score=excluded.source_score,
                        impact=excluded.impact,
                        reason=excluded.reason,
                        fetched_at=excluded.fetched_at,
                        run_id=excluded.run_id
                    """,
                    (
                        r["raw_id"],
                        r["date8"],
                        r["code"],
                        r.get("name", ""),
                        r["source"],
                        r.get("event_type", ""),
                        r.get("event_title", ""),
                        r.get("event_url", ""),
                        float(r.get("source_score", 0.0) or 0.0),
                        r.get("impact", ""),
                        r.get("reason", ""),
                        r["fetched_at"],
                        r["run_id"],
                    ),
                )
            con.commit()
            return int(len(rows))
        except sqlite3.OperationalError as e:
            last_exc = e
            try:
                con.rollback()
            except Exception:
                pass
            if "locked" not in str(e).lower() or attempt >= attempts - 1:
                raise
            time.sleep(backoff * float(attempt + 1))
        finally:
            con.close()
    if last_exc:
        raise last_exc
    return 0


def _write_status(payload: Dict[str, Any], as_of: str) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    dated = LOGS / f"news_source_signal_status_{as_of}.json"
    latest = LOGS / "news_source_signal_status_latest.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    dated.write_text(text, encoding="utf-8")
    latest.write_text(text, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Collect original-source disclosure signals for the news score layer")
    ap.add_argument("--as-of", default=_now_kst().strftime("%Y%m%d"))
    ap.add_argument("--lookback-days", type=int, default=int(os.getenv("NEWS_SOURCE_SIGNAL_LOOKBACK_DAYS", "3")))
    ap.add_argument("--max-codes", type=int, default=int(os.getenv("NEWS_SOURCE_SIGNAL_MAX_CODES", "120")))
    ap.add_argument("--page-count", type=int, default=int(os.getenv("NEWS_SOURCE_SIGNAL_PAGE_COUNT", "20")))
    ap.add_argument("--sleep", type=float, default=float(os.getenv("NEWS_SOURCE_SIGNAL_SLEEP", "0.05")))
    ap.add_argument("--api-key", default="")
    args = ap.parse_args()

    as_of = _norm_date8(args.as_of) or _now_kst().strftime("%Y%m%d")
    run_id = f"source_signal_{_now_kst().strftime('%Y%m%d_%H%M%S')}"
    status: Dict[str, Any] = {
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "run_id": run_id,
        "asof_ymd": as_of,
        "db": str(DB),
        "table": "source_signals_daily",
        "source": "DART_OPENAPI",
        "quality": "SKIP",
        "reason": "",
        "requested_codes": 0,
        "mapped_codes": 0,
        "fetched_codes": 0,
        "rows_saved": 0,
        "positive_rows": 0,
        "negative_rows": 0,
        "neutral_rows": 0,
        "errors": [],
    }

    api_key = _resolve_api_key(args.api_key)
    if not api_key:
        status["reason"] = "dart_api_key_missing"
        _write_status(status, as_of)
        print("[SOURCE_SIGNAL] skip: DART API key missing")
        return 0

    codes, names, sources = _load_target_codes(max_codes=int(args.max_codes))
    status["target_sources"] = sources
    status["requested_codes"] = int(len(codes))
    if not codes:
        status["reason"] = "target_codes_empty"
        _write_status(status, as_of)
        print("[SOURCE_SIGNAL] skip: target codes empty")
        return 0

    corp_map = _load_corp_map()
    if corp_map.empty:
        status["reason"] = "corp_map_missing"
        _write_status(status, as_of)
        print("[SOURCE_SIGNAL] skip: corp map missing")
        return 0

    code_to_corp = dict(zip(corp_map["code"], corp_map["corp_code"]))
    code_to_corp_name = dict(zip(corp_map["code"], corp_map["corp_name"]))
    fin_signals = _load_fin_signals()
    mapped = [c for c in codes if c in code_to_corp]
    status["mapped_codes"] = int(len(mapped))

    end_dt = datetime.strptime(as_of, "%Y%m%d").date()
    bgn_dt = end_dt - timedelta(days=max(0, int(args.lookback_days)))
    bgn_de = bgn_dt.strftime("%Y%m%d")
    end_de = end_dt.strftime("%Y%m%d")
    fetched_at = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
    rows: List[Dict[str, Any]] = []
    errors: List[str] = []

    for idx, code in enumerate(mapped, start=1):
        corp_code = code_to_corp.get(code, "")
        try:
            payload = _fetch_dart_list(api_key, corp_code, bgn_de, end_de, int(args.page_count))
            st = str(payload.get("status") or "")
            if st not in {"000", "013"}:
                errors.append(f"{code}:{st}:{payload.get('message','')}")
                continue
            for item in payload.get("list") or []:
                if not isinstance(item, dict):
                    continue
                rcept_no = str(item.get("rcept_no") or "").strip()
                rcept_dt = _norm_date8(item.get("rcept_dt"))
                report_nm = str(item.get("report_nm") or "").strip()
                if not rcept_no or not rcept_dt:
                    continue
                score, impact, reason = _classify_event(report_nm, fin_signals.get(code, ""))
                rows.append(
                    {
                        "raw_id": f"DART:{rcept_no}:{code}",
                        "date8": rcept_dt,
                        "code": code,
                        "name": names.get(code) or code_to_corp_name.get(code) or "",
                        "source": "DART_OPENAPI",
                        "event_type": str(item.get("pblntf_detail_ty") or item.get("pblntf_ty") or ""),
                        "event_title": report_nm,
                        "event_url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={urllib.parse.quote(rcept_no)}",
                        "source_score": score,
                        "impact": impact,
                        "reason": reason,
                        "fetched_at": fetched_at,
                        "run_id": run_id,
                    }
                )
        except Exception as e:
            errors.append(f"{code}:{type(e).__name__}:{e}")
        if float(args.sleep) > 0 and idx < len(mapped):
            time.sleep(float(args.sleep))

    saved = _write_rows(rows) if rows else 0
    status["fetched_codes"] = int(len(mapped) - len(errors))
    status["rows_saved"] = int(saved)
    status["positive_rows"] = int(sum(1 for r in rows if float(r.get("source_score", 0.0)) > 0))
    status["negative_rows"] = int(sum(1 for r in rows if float(r.get("source_score", 0.0)) < 0))
    status["neutral_rows"] = int(sum(1 for r in rows if abs(float(r.get("source_score", 0.0))) <= 1e-12))
    status["errors"] = errors[:20]
    status["error_count"] = int(len(errors))
    if errors and not rows:
        status["quality"] = "WARN"
        status["reason"] = "all_fetches_failed"
    else:
        status["quality"] = "PASS"
        status["reason"] = "ok"
    _write_status(status, as_of)
    print(f"[SOURCE_SIGNAL] quality={status['quality']} rows_saved={saved} errors={len(errors)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
