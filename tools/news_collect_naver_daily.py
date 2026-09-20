from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import socket
import sqlite3
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, time as dt_time, timedelta, timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import logging

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
DB = ROOT / "news_trading" / "data" / "trading.db"
KEY_FILE = ROOT / "paper" / "news_api_keys.json"
CACHE_DIR = ROOT / "_cache" / "news_collect_naver_daily"
LOCK_PATH = LOGS / "news_collect_naver_daily.lock.json"
QUOTA_STATE_PATH = LOGS / "news_collect_naver_quota_state.json"
LAST_SUCCESS_PATH = LOGS / "news_collect_last_success.json"
DEFAULT_HOLIDAYS_PATH = ROOT / "holidays.json"
PAPER_STATE_FILES = [
    ROOT / "paper" / "paper_state.json",
    ROOT / "paper" / "paper_state_shadow.json",
]
SECTOR_SSOT = ROOT / "_cache" / "sector_ssot.csv"
DART_FUNDAMENTAL_LATEST = ROOT / "_cache" / "dart_fundamental_latest.csv"
DART_FUNDAMENTAL_META = ROOT / "_cache" / "dart_fundamental_meta.json"
MACRO_SIGNAL_LATEST = LOGS / "macro_signal_latest.json"
MACRO_FEATURE_LATEST = LOGS / "macro_feature_external_latest.json"


def _sqlite_timeout_sec() -> float:
    try:
        return max(1.0, float(str(os.getenv("NEWS_COLLECT_SQLITE_TIMEOUT_SEC", "60")).strip() or "60"))
    except Exception:
        return 60.0


def _connect_news_db() -> sqlite3.Connection:
    timeout_sec = _sqlite_timeout_sec()
    con = sqlite3.connect(str(DB), timeout=timeout_sec)
    con.execute(f"PRAGMA busy_timeout={int(timeout_sec * 1000)}")
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.OperationalError:
        pass
    return con


def _db_writer_preflight(timeout_sec: float = 3.0) -> Dict[str, object]:
    started = time.monotonic()
    out: Dict[str, object] = {
        "ok": False,
        "elapsed_sec": 0.0,
        "journal_mode": "",
        "reason": "",
    }
    con: Optional[sqlite3.Connection] = None
    try:
        con = sqlite3.connect(str(DB), timeout=max(0.1, float(timeout_sec)))
        con.execute(f"PRAGMA busy_timeout={int(max(0.1, float(timeout_sec)) * 1000)}")
        out["journal_mode"] = str(con.execute("PRAGMA journal_mode=WAL").fetchone()[0])
        con.execute("BEGIN IMMEDIATE")
        con.rollback()
        out["ok"] = True
        out["reason"] = "ok"
    except Exception as e:
        out["reason"] = f"{type(e).__name__}:{str(e)[:160]}"
        try:
            if con is not None:
                con.rollback()
        except Exception:
            pass
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
        out["elapsed_sec"] = round(float(time.monotonic() - started), 3)
    return out


SESSION_DEFAULT_WINDOWS: Dict[str, Tuple[str, str]] = {
    "premarket": ("08:30", "09:00"),
    "intraday": ("09:00", "15:30"),
    "afterhours": ("15:30", "16:00"),
    "evening": ("15:30", "20:00"),
    "night": ("20:00", "04:00"),
    "dawn": ("04:00", "09:00"),
    "overnight": ("00:00", "07:00"),
    "daily": ("08:30", "16:00"),
}
SESSION_DAILY_BUDGET_RATIO: Dict[str, float] = {
    "premarket": 0.15,
    "intraday": 0.45,
    "afterhours": 0.20,
    "evening": 0.15,
    "night": 0.15,
    "dawn": 0.10,
    "overnight": 0.05,
    "daily": 1.00,
}

IN_FILES = [
    LOGS / "candidates_latest_data.with_final_score.csv",
    LOGS / "candidates_latest_data.with_news_score.csv",
    LOGS / "candidates_latest_data.with_sector_score.csv",
    LOGS / "candidates_latest_data.filtered.csv",
    LOGS / "candidates_latest_data.csv",
]
MARKET_RISING_LATEST = LOGS / "market_rising_latest.csv"
SURGE_REALTIME_LATEST = LOGS / "surge_realtime_latest.csv"

API_URL = "https://openapi.naver.com/v1/search/news.json"
NAVER_FINANCE_SOURCE_URLS: Dict[str, List[str]] = {
    "NAVER_FINANCE_NEWS": [
        "https://finance.naver.com/news/",
        "https://finance.naver.com/news/mainnews.naver",
    ],
    "NAVER_FINANCE_RESEARCH": [
        "https://finance.naver.com/research/",
        "https://finance.naver.com/research/company_list.naver",
        "https://finance.naver.com/research/industry_list.naver",
        "https://finance.naver.com/research/market_info_list.naver",
        "https://finance.naver.com/research/invest_list.naver",
        "https://finance.naver.com/research/economy_list.naver",
        "https://finance.naver.com/research/debenture_list.naver",
    ],
}
KST = timezone(timedelta(hours=9))
PRESS_SOURCES: Dict[str, List[str]] = {
    "tier1": [
        "news.einfomax.co.kr",
        "yna.co.kr",
        "yonhapnewstv.co.kr",
        "edaily.co.kr",
        "mt.co.kr",
        "heraldcorp.com",
    ],
    "tier2": [
        "hankyung.com",
        "mk.co.kr",
        "sedaily.com",
    ],
    "tier3": [
        "chosunbiz.com",
        "hani.co.kr",
        "news2day.co.kr",
    ],
    "tier4": [
        "paxnet.co.kr",
        "paxnetnews.com",
        "dealsite.co.kr",
        "itooza.com",
        "mtn.co.kr",
        "newstomato.com",
        "newspim.com",
        "etoday.co.kr",
        "fnnews.com",
        "etnews.com",
        "ajunews.com",
        "pinpointnews.co.kr",
        "choicenews.co.kr",
        "businesspost.co.kr",
        "thebell.co.kr",
        "investchosun.com",
        "infostockdaily.co.kr",
        "bloter.net",
        "finance.naver.com",
    ],
}

POS_KW = [
    # 기존 — 기업 실적/모멘텀
    "상승", "급등", "호재", "실적개선", "수주",
    "신고가", "매수", "성장", "흑자", "강세",
    "돌파", "확대", "회복", "반등", "증가",
    # 추가 — 실적/계약 확신도
    "공급계약", "수출계약", "사상최대", "분기최고",
    "목표가상향", "기관순매수", "외인순매수",
    "흑자전환", "수주잔고",
    # 추가 — 중동전쟁 수혜 (방산·에너지·해운)
    "방산수주", "K방산", "방위산업", "무기수출", "탄약",
    "정유수혜", "에너지주", "원유수혜",
    "해운운임", "운임급등", "대체항로",
]
NEG_KW = [
    # 기존 — 기업 실적/모멘텀
    "하락", "급락", "악재", "실적악화", "적자",
    "신저가", "매도", "약세", "부진", "우려",
    "축소", "감소", "리스크", "폭락", "악화",
    # 추가 — 지정학·공급망 충격
    "중동전쟁", "이란전쟁", "확전", "공습",
    "호르무즈", "호르무즈봉쇄", "후티", "봉쇄",
    "유가급등", "원유공급", "공급충격",
    "외국인순매도", "원화약세", "환율급등",
    "공급망붕괴", "해운차질", "수에즈봉쇄",
]




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
def _now_kst() -> datetime:
    return datetime.now(KST)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if raw == "":
        return default
    return raw in {"1", "true", "t", "y", "yes", "on"}


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _norm_code6(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6) if s else ""


def _norm_date8(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _input_asof_ymd(path: Path) -> str:
    try:
        return _max_date8(_read_csv(path))
    except Exception:
        return ""


def _dated_input_rank(path: Path) -> tuple[str, float]:
    if not path.exists():
        return ("", -1.0)
    try:
        df = _read_csv(path)
    except Exception:
        return ("", float(path.stat().st_mtime))
    if not isinstance(df, pd.DataFrame) or df.empty:
        return ("", float(path.stat().st_mtime))
    return (_max_date8(df), float(path.stat().st_mtime))


def _pick_input() -> Optional[Path]:
    best_path: Optional[Path] = None
    best_rank = ("", -1.0)
    for p in IN_FILES:
        rank = _dated_input_rank(p)
        if (best_path is None) or (rank > best_rank):
            best_path = p
            best_rank = rank
    return best_path


def _load_paper_holdings() -> pd.DataFrame:
    rows: List[Dict[str, str]] = []
    for path in PAPER_STATE_FILES:
        if not path.exists():
            continue
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        open_positions = obj.get("open_positions")
        if not isinstance(open_positions, list):
            continue
        for raw in open_positions:
            if not isinstance(raw, dict):
                continue
            code = _norm_code6(raw.get("code"))
            if not code:
                continue
            rows.append(
                {
                    "code": code,
                    "name": str(raw.get("name", "")).strip(),
                    "source_scope": "holding",
                }
            )
    if not rows:
        return pd.DataFrame(columns=["code", "name", "source_scope"])
    out = pd.DataFrame(rows)
    out = out.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out


def _load_market_rising_symbols(limit: int = 30) -> pd.DataFrame:
    if not MARKET_RISING_LATEST.exists():
        return pd.DataFrame(columns=["code", "name", "source_scope", "krx_sector"])
    try:
        raw = _read_csv(MARKET_RISING_LATEST)
    except Exception:
        return pd.DataFrame(columns=["code", "name", "source_scope", "krx_sector"])
    if "code" not in raw.columns:
        return pd.DataFrame(columns=["code", "name", "source_scope", "krx_sector"])

    work = raw.copy()
    if "rank" in work.columns:
        work["_rank"] = pd.to_numeric(work["rank"], errors="coerce")
        work = work.sort_values(["_rank", "code"], ascending=[True, True], kind="mergesort")

    out = pd.DataFrame()
    out["code"] = work["code"].map(_norm_code6)
    out["name"] = work["name"].astype(str).str.strip() if "name" in work.columns else ""
    out["krx_sector"] = ""
    out["source_scope"] = "market_rising"
    out = out[out["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    if limit > 0 and len(out) > limit:
        out = out.head(limit).reset_index(drop=True)
    return out


def _load_surge_symbols() -> pd.DataFrame:
    if not SURGE_REALTIME_LATEST.exists():
        return pd.DataFrame(columns=["code", "name", "source_scope", "krx_sector"])
    try:
        raw = _read_csv(SURGE_REALTIME_LATEST)
    except Exception:
        return pd.DataFrame(columns=["code", "name", "source_scope", "krx_sector"])
    if "code" not in raw.columns:
        return pd.DataFrame(columns=["code", "name", "source_scope", "krx_sector"])

    work = raw.copy()

    out = pd.DataFrame()
    out["code"] = work["code"].map(_norm_code6)
    out["name"] = work["name"].astype(str).str.strip() if "name" in work.columns else ""
    out["krx_sector"] = ""
    out["source_scope"] = "surge"
    out = out[out["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out


def _compute_signal_coverage(reference_ymd: str, surge_symbols: pd.DataFrame) -> Dict[str, object]:
    universe = sorted(set(surge_symbols["code"].map(_norm_code6).tolist())) if "code" in surge_symbols.columns else []
    universe = [c for c in universe if c]
    out: Dict[str, object] = {
        "reference_ymd": str(reference_ymd or ""),
        "surge_universe_symbols": int(len(universe)),
        "signals_covered_symbols": 0,
        "signals_missing_symbols": int(len(universe)),
        "missing_codes_sample": universe[:30],
        "db_path": str(DB),
        "reason": "no_surge_universe" if not universe else "",
    }
    if not universe:
        return out
    if not DB.exists():
        out["reason"] = "db_missing"
        return out
    try:
        con = _connect_news_db()
        rows = con.execute(
            "SELECT DISTINCT code FROM signals_naver_daily WHERE date8 = ?",
            (str(reference_ymd),),
        ).fetchall()
        con.close()
    except Exception as e:
        out["reason"] = f"db_read_fail:{type(e).__name__}"
        return out

    covered = set()
    for row in rows:
        if not row:
            continue
        code = _norm_code6(row[0])
        if code:
            covered.add(code)
    universe_set = set(universe)
    covered_in_universe = sorted(universe_set.intersection(covered))
    missing = sorted(universe_set.difference(covered))
    out["signals_covered_symbols"] = int(len(covered_in_universe))
    out["signals_missing_symbols"] = int(len(missing))
    out["missing_codes_sample"] = missing[:30]
    out["reason"] = "ok"
    return out


def _load_sector_ssot() -> pd.DataFrame:
    if not SECTOR_SSOT.exists():
        return pd.DataFrame(columns=["code", "name", "krx_sector"])
    try:
        raw = _read_csv(SECTOR_SSOT)
    except Exception:
        return pd.DataFrame(columns=["code", "name", "krx_sector"])
    if "code" not in raw.columns:
        return pd.DataFrame(columns=["code", "name", "krx_sector"])
    out = pd.DataFrame()
    out["code"] = raw["code"].map(_norm_code6)
    out["name"] = raw["name"].astype(str).str.strip() if "name" in raw.columns else ""
    out["krx_sector"] = raw["krx_sector"].astype(str).str.strip() if "krx_sector" in raw.columns else ""
    out = out[(out["code"] != "") & (out["krx_sector"] != "")]
    out = out.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out


def _resolve_session_window(session_name: str, window_start: str, window_end: str) -> Tuple[str, str]:
    start = str(window_start or "").strip()
    end = str(window_end or "").strip()
    if start and end:
        return start, end
    default_pair = SESSION_DEFAULT_WINDOWS.get(str(session_name or "").strip().lower())
    if not default_pair:
        return start, end
    return start or default_pair[0], end or default_pair[1]


def _pipeline_source_binding() -> Dict[str, Any]:
    return {
        "binding_layer": "signal_integration",
        "dart_bound": bool(DART_FUNDAMENTAL_LATEST.exists() and DART_FUNDAMENTAL_META.exists()),
        "macro_bound": bool(MACRO_SIGNAL_LATEST.exists() and MACRO_FEATURE_LATEST.exists()),
    }


def _build_symbol_scope(
    input_df: pd.DataFrame,
    include_sector_peers: bool = False,
    max_symbols: int = 0,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    base = input_df.copy()
    if "code" not in base.columns:
        return pd.DataFrame(columns=["code", "name", "source_scope"]), {
            "candidate_symbols": 0,
            "holding_symbols": 0,
            "market_rising_symbols": 0,
            "surge_symbols": 0,
            "sector_peer_symbols": 0,
            "index_symbols": 0,
            "total_before_cap": 0,
            "total_symbols": 0,
            "total_after_cap": 0,
            "cap_limit": int(max(0, int(max_symbols))),
            "cap_applied": 0,
            "include_sector_peers": 1 if include_sector_peers else 0,
        }
    if "name" not in base.columns:
        base["name"] = ""
    if "krx_sector" not in base.columns:
        base["krx_sector"] = ""
    candidate = (
        base[["code", "name", "krx_sector"]]
        .assign(
            code=lambda x: x["code"].map(_norm_code6),
            name=lambda x: x["name"].astype(str).str.strip(),
            krx_sector=lambda x: x["krx_sector"].astype(str).str.strip(),
            source_scope="candidate",
        )
        .drop_duplicates(subset=["code"], keep="first")
    )
    candidate = candidate[candidate["code"] != ""].reset_index(drop=True)

    holdings = _load_paper_holdings()
    market_rising = _load_market_rising_symbols(limit=int(str(os.getenv("NEWS_MARKET_RISING_LIMIT", "30")).strip() or "30"))
    surge_symbols = _load_surge_symbols()
    if len(holdings):
        sector_ssot = _load_sector_ssot()
        if len(sector_ssot):
            holdings = holdings.merge(sector_ssot[["code", "krx_sector"]], on="code", how="left")
        else:
            holdings["krx_sector"] = ""
    else:
        holdings = pd.DataFrame(columns=["code", "name", "source_scope", "krx_sector"])

    MAX_SECTOR_PEERS = int(str(os.getenv("NEWS_MAX_SECTOR_PEERS", "30")).strip() or "30")
    if include_sector_peers:
        sector_ssot = _load_sector_ssot()
        sector_values = set(
            s for s in pd.concat([candidate.get("krx_sector", pd.Series(dtype=str)), holdings.get("krx_sector", pd.Series(dtype=str))], ignore_index=True).astype(str).str.strip().tolist() if s
        )
        if len(sector_ssot) and sector_values:
            sector_peer = sector_ssot[sector_ssot["krx_sector"].isin(sorted(sector_values))].copy()
            sector_peer["source_scope"] = "sector_peer"
            sector_peer = sector_peer[["code", "name", "krx_sector", "source_scope"]]
            if MAX_SECTOR_PEERS > 0 and len(sector_peer) > MAX_SECTOR_PEERS:
                sector_peer = sector_peer.head(MAX_SECTOR_PEERS)
        else:
            sector_peer = pd.DataFrame(columns=["code", "name", "krx_sector", "source_scope"])
    else:
        sector_peer = pd.DataFrame(columns=["code", "name", "krx_sector", "source_scope"])

    merged = pd.concat(
        [
            candidate[["code", "name", "krx_sector", "source_scope"]],
            holdings[["code", "name", "krx_sector", "source_scope"]],
            market_rising[["code", "name", "krx_sector", "source_scope"]],
            surge_symbols[["code", "name", "krx_sector", "source_scope"]],
            sector_peer[["code", "name", "krx_sector", "source_scope"]],
        ],
        ignore_index=True,
    )
    merged["scope_rank"] = merged["source_scope"].map(
        {"candidate": 0, "holding": 1, "market_rising": 2, "surge": 3, "sector_peer": 4}
    ).fillna(9)
    merged = merged.sort_values(["code", "scope_rank"]).drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    merged = merged[merged["code"] != ""].copy()
    total_before_cap = int(len(merged))
    cap_limit = int(max(0, int(max_symbols)))
    if cap_limit > 0 and len(merged) > cap_limit:
        merged = merged.sort_values(["scope_rank", "code"]).head(cap_limit).reset_index(drop=True)

    counts = {
        "candidate_symbols": int(len(candidate)),
        "holding_symbols": int(len(holdings)),
        "market_rising_symbols": int(len(market_rising)),
        "surge_symbols": int(len(surge_symbols)),
        "sector_peer_symbols": int(len(sector_peer[~sector_peer["code"].isin(set(candidate["code"]).union(set(holdings["code"]))) ])) if len(sector_peer) else 0,
        "index_symbols": 0,
        "total_before_cap": total_before_cap,
        "total_symbols": int(len(merged)),
        "total_after_cap": int(len(merged)),
        "cap_limit": cap_limit,
        "cap_applied": 1 if cap_limit > 0 and total_before_cap > cap_limit else 0,
        "include_sector_peers": 1 if include_sector_peers else 0,
    }
    return merged[["code", "name", "source_scope"]], counts


def _max_date8(df: pd.DataFrame) -> str:
    if "date_yyyymmdd" in df.columns:
        s = df["date_yyyymmdd"].astype(str)
    elif "date" in df.columns:
        s = df["date"].astype(str)
    elif "signal_date" in df.columns:
        s = df["signal_date"].astype(str)
    else:
        return _now_kst().strftime("%Y%m%d")
    d8 = s.map(_norm_date8)
    d8 = d8[d8.str.len() == 8]
    return str(d8.max()) if len(d8) else _now_kst().strftime("%Y%m%d")


def _strip_html(s: str) -> str:
    return (
        re.sub(r"<[^>]+>", "", str(s or ""))
        .replace("&quot;", '"')
        .replace("&apos;", "'")
        .replace("&amp;", "&")
    )


def _parse_hhmm(v: str) -> Optional[dt_time]:
    raw = str(v or "").strip()
    if not raw:
        return None
    m = re.fullmatch(r"(\d{1,2}):?(\d{2})", raw)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None
    return dt_time(hour=hh, minute=mm)


def _time_in_window(now_tm: dt_time, start_tm: Optional[dt_time], end_tm: Optional[dt_time]) -> bool:
    if not start_tm or not end_tm:
        return True
    if start_tm <= end_tm:
        return start_tm <= now_tm < end_tm
    return now_tm >= start_tm or now_tm < end_tm


def _session_label_for_now(now_tm: dt_time) -> str:
    hhmm = now_tm.hour * 100 + now_tm.minute
    if 1530 <= hhmm < 2000:
        return "EVENING"
    if hhmm >= 2000 or hhmm < 400:
        return "NIGHT"
    if 400 <= hhmm < 900:
        return "DAWN"
    return "INTRADAY"


def _load_last_success_dt(path: Path) -> Optional[datetime]:
    obj = _safe_json(path)
    if not isinstance(obj, dict):
        return None
    ts = _parse_dt(obj.get("last_success_at"))
    return ts


def _write_last_success(path: Path, now: datetime, session_label: str, run_id: str) -> None:
    payload = {
        "last_success_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "session_label": session_label,
        "run_id": run_id,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _should_record_last_success(status: Dict[str, object]) -> bool:
    reason = str(status.get("reason") or "").strip().lower()
    return reason in {"ok", "no_rows_saved"}


def _charge_quota(
    quota_state: Dict[str, Any],
    session_daily_used_map: Dict[str, Any],
    session_daily_key: str,
) -> int:
    quota_state["daily_used"] = int(quota_state.get("daily_used") or 0) + 1
    quota_state["hourly_used"] = int(quota_state.get("hourly_used") or 0) + 1
    session_daily_used = int(session_daily_used_map.get(session_daily_key, 0) or 0) + 1
    session_daily_used_map[session_daily_key] = int(session_daily_used)
    return int(session_daily_used)


def _is_cache_hit(query: str, reference_ymd: str, display: int, cache_ttl_sec: int) -> bool:
    cache_path = _cache_path(query, reference_ymd, display)
    return _read_cache(cache_path, cache_ttl_sec) is not None


def _parse_dt(v: object) -> Optional[datetime]:
    raw = str(v or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            return dt.astimezone(KST)
        except Exception:
            continue
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        return None


def _safe_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _pid_alive(pid: object) -> bool:
    try:
        raw = int(pid or 0)
    except Exception:
        return False
    if raw <= 0:
        return False
    try:
        os.kill(raw, 0)
        return True
    except OSError:
        return False


def _load_holidays_ymd(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()

    vals: List[str] = []
    if isinstance(obj, dict):
        for k in ("holidays", "krx_holidays", "dates", "holiday_dates"):
            v = obj.get(k)
            if isinstance(v, list):
                vals.extend([str(x) for x in v])
    elif isinstance(obj, list):
        vals.extend([str(x) for x in obj])

    out = set()
    for x in vals:
        y = _norm_date8(x)
        if len(y) == 8:
            out.add(y)
    return out


def _acquire_lock(lock_path: Path, ttl_sec: int, run_id: str) -> Tuple[bool, Dict[str, Any]]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    now = _now_kst()
    if lock_path.exists():
        cur = _safe_json(lock_path) or {}
        created = _parse_dt(cur.get("created_at"))
        pid_alive = _pid_alive(cur.get("pid"))
        age_sec = None
        if created is not None:
            age_sec = int(max(0.0, (now - created).total_seconds()))
        if pid_alive and (age_sec is None or age_sec <= max(1, int(ttl_sec))):
            cur["age_sec"] = age_sec
            cur["pid_alive"] = True
            return False, cur
        stale = lock_path.with_name(f"{lock_path.stem}_stale_{now.strftime('%Y%m%d_%H%M%S')}{lock_path.suffix}")
        try:
            lock_path.replace(stale)
        except Exception:
            return False, {"reason": "lock_replace_fail", "age_sec": age_sec, "pid_alive": pid_alive}

    payload = {
        "run_id": run_id,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "pid": os.getpid(),
    }
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        return True, payload
    except FileExistsError:
        return False, _safe_json(lock_path) or {"reason": "lock_exists"}


def _release_lock(lock_path: Path, run_id: str) -> None:
    cur = _safe_json(lock_path) or {}
    if cur and str(cur.get("run_id") or "") not in {"", run_id}:
        return
    try:
        lock_path.unlink()
    except Exception:
        return


def _load_quota_state(path: Path) -> Dict[str, Any]:
    raw = _safe_json(path) or {}
    session_daily_raw = raw.get("session_daily_used")
    session_daily_used: Dict[str, int] = {}
    if isinstance(session_daily_raw, dict):
        for k, v in session_daily_raw.items():
            try:
                session_daily_used[str(k)] = int(v or 0)
            except Exception:
                continue
    return {
        "ymd": str(raw.get("ymd") or ""),
        "hour_key": str(raw.get("hour_key") or ""),
        "daily_used": int(raw.get("daily_used") or 0),
        "hourly_used": int(raw.get("hourly_used") or 0),
        "session_daily_used": session_daily_used,
        "updated_at": str(raw.get("updated_at") or ""),
    }


def _save_quota_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ymd": str(state.get("ymd") or ""),
        "hour_key": str(state.get("hour_key") or ""),
        "daily_used": int(state.get("daily_used") or 0),
        "hourly_used": int(state.get("hourly_used") or 0),
        "session_daily_used": state.get("session_daily_used") if isinstance(state.get("session_daily_used"), dict) else {},
        "updated_at": _now_kst().strftime("%Y-%m-%d %H:%M:%S"),
    }
    last_err: Optional[Exception] = None
    for attempt in range(10):
        tmp = path.with_name(f"{path.name}.{os.getpid()}.{int(time.time() * 1000)}.{attempt}.tmp")
        try:
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
            return
        except PermissionError as e:
            last_err = e
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            time.sleep(min(0.25 * (attempt + 1), 2.0))
        except Exception:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            raise
    if last_err is not None:
        raise last_err


def _normalize_url(url: object) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parts = urllib.parse.urlsplit(raw)
        q = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
        q = [(k, v) for k, v in q if not k.lower().startswith("utm_")]
        query = urllib.parse.urlencode(q, doseq=True)
        return urllib.parse.urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, query, ""))
    except Exception:
        return raw


def _source_host(url: object) -> str:
    raw = _normalize_url(url)
    if not raw:
        return ""
    try:
        return str(urllib.parse.urlsplit(raw).netloc or "").strip().lower()
    except Exception:
        return ""


def _classify_press_tier(url: object) -> str:
    host = _source_host(url)
    if not host:
        return "excluded"
    for tier, domains in PRESS_SOURCES.items():
        for dom in domains:
            dom_l = str(dom).strip().lower()
            if host == dom_l or host.endswith("." + dom_l):
                return tier
    return "excluded"


def _cache_path(query: str, reference_ymd: str, display: int) -> Path:
    key = hashlib.sha256(f"{reference_ymd}|{display}|{query}".encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{key}.json"


def _read_cache(cache_path: Path, ttl_sec: int) -> Optional[Dict[str, Any]]:
    if ttl_sec <= 0 or (not cache_path.exists()):
        return None
    try:
        obj = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    fetched_at = _parse_dt(obj.get("fetched_at"))
    if fetched_at is None:
        return None
    age_sec = (_now_kst() - fetched_at).total_seconds()
    if age_sec > float(ttl_sec):
        return None
    payload = obj.get("payload")
    return payload if isinstance(payload, dict) else None


def _write_cache(cache_path: Path, payload: Dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    obj = {
        "fetched_at": _now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "payload": payload,
    }
    cache_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _fetch_naver_news(
    query: str,
    client_id: str,
    client_secret: str,
    display: int,
    retry_attempts: int,
    retry_backoff_sec: float,
    cache_ttl_sec: int,
    reference_ymd: str,
) -> Tuple[Dict[str, object], bool, int]:
    cache_path = _cache_path(query, reference_ymd, display)
    cached = _read_cache(cache_path, cache_ttl_sec)
    if cached is not None:
        return cached, True, 0

    qs = urllib.parse.urlencode(
        {
            "query": query,
            "display": max(1, min(int(display), 100)),
            "start": 1,
            "sort": "date",
        }
    )
    req = urllib.request.Request(f"{API_URL}?{qs}")
    req.add_header("X-Naver-Client-Id", client_id)
    req.add_header("X-Naver-Client-Secret", client_secret)

    max_attempts = max(1, int(retry_attempts) + 1)
    retries_used = 0
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read().decode("utf-8")
            payload = json.loads(raw)
            if isinstance(payload, dict):
                _write_cache(cache_path, payload)
            return payload, False, retries_used
        except Exception as e:
            last_err = e
            if attempt >= max_attempts:
                break
            retries_used += 1
            sleep_sec = max(0.0, float(retry_backoff_sec)) * float(attempt)
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    if last_err is None:
        raise RuntimeError("fetch_failed_without_exception")
    raise last_err



def _fetch_url_text(url: str, timeout_sec: int = 10) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 news_collect_naver_daily",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=max(1, int(timeout_sec))) as resp:
        raw = resp.read()
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", "ignore")


def _finance_pubdate(reference_ymd: str) -> str:
    try:
        dt = datetime.strptime(reference_ymd, "%Y%m%d").replace(tzinfo=KST)
    except Exception:
        dt = _now_kst()
    return format_datetime(dt)


def _extract_naver_finance_links(html_text: str, base_url: str, source: str, reference_ymd: str) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    seen: set[str] = set()
    source_l = str(source or "").upper()

    tr_re = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
    td_re = re.compile(r"<t[dh]\b[^>]*>(.*?)</t[dh]>", re.IGNORECASE | re.DOTALL)
    a_re = re.compile(r"<a\b[^>]*href=[\"'](?P<href>[^\"']+)[\"'][^>]*>(?P<text>.*?)</a>", re.IGNORECASE | re.DOTALL)

    def _is_valid_path(path: str) -> bool:
        leaf = path.rsplit("/", 1)[-1]
        if source_l == "NAVER_FINANCE_NEWS":
            return "/news/" in path and leaf.endswith("_read.naver")
        if source_l == "NAVER_FINANCE_RESEARCH":
            return "/research/" in path and leaf.endswith("_read.naver")
        return False

    for tr_m in tr_re.finditer(str(html_text or "")):
        tr_content = tr_m.group(1)
        link_href: Optional[str] = None
        link_title: Optional[str] = None

        for a_m in a_re.finditer(tr_content):
            href = html.unescape(str(a_m.group("href") or "")).strip()
            title = _strip_html(html.unescape(str(a_m.group("text") or ""))).strip()
            title = re.sub(r"\s+", " ", title)
            if not href or not title or len(title) < 2:
                continue
            full_url = urllib.parse.urljoin(base_url, href)
            parsed = urllib.parse.urlparse(full_url)
            if str(parsed.netloc or "").lower() != "finance.naver.com":
                continue
            if not _is_valid_path(str(parsed.path or "").lower()):
                continue
            link_href = full_url.split("#", 1)[0]
            link_title = title
            break

        if not link_href or not link_title:
            continue
        if link_href in seen:
            continue
        seen.add(link_href)

        # 같은 행의 셀에서 증권사명·날짜 등 부가 정보를 description으로 추출
        cell_texts: List[str] = []
        for td_m in td_re.finditer(tr_content):
            cell = _strip_html(html.unescape(td_m.group(1))).strip()
            cell = re.sub(r"\s+", " ", cell)
            if not cell or cell == link_title or len(cell) > 60:
                continue
            cell_texts.append(cell)
        description = " | ".join(cell_texts[:2])

        rows.append(
            {
                "title": link_title,
                "description": description,
                "originallink": link_href,
                "link": link_href,
                "pubDate": _finance_pubdate(reference_ymd),
                "source": source_l,
            }
        )
    return rows


def _fetch_naver_finance_items(reference_ymd: str) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    started = time.monotonic()
    timeout_sec = max(1, int(str(os.getenv("NEWS_NAVER_FINANCE_TIMEOUT_SEC", "5")).strip() or "5"))
    max_runtime_sec = max(1.0, float(str(os.getenv("NEWS_NAVER_FINANCE_MAX_RUNTIME_SEC", "20")).strip() or "20"))
    status: Dict[str, object] = {
        "enabled": bool(_env_bool("NEWS_NAVER_FINANCE_SOURCES_ENABLED", True)),
        "news_enabled": bool(_env_bool("NEWS_NAVER_FINANCE_NEWS_ENABLED", True)),
        "research_enabled": bool(_env_bool("NEWS_NAVER_FINANCE_RESEARCH_ENABLED", True)),
        "timeout_sec": int(timeout_sec),
        "max_runtime_sec": float(max_runtime_sec),
        "urls": [],
        "items": 0,
        "matched_items": 0,
        "errors": [],
        "runtime_guard_stop": False,
    }
    if not bool(status["enabled"]):
        return [], status
    all_items: List[Dict[str, object]] = []
    for source, urls in NAVER_FINANCE_SOURCE_URLS.items():
        if source == "NAVER_FINANCE_NEWS" and not bool(status["news_enabled"]):
            continue
        if source == "NAVER_FINANCE_RESEARCH" and not bool(status["research_enabled"]):
            continue
        for url in urls:
            if (time.monotonic() - started) >= float(max_runtime_sec):
                status["runtime_guard_stop"] = True
                break
            status["urls"].append(url)
            try:
                html_text = _fetch_url_text(url, timeout_sec=timeout_sec)
                all_items.extend(_extract_naver_finance_links(html_text, url, source, reference_ymd))
            except Exception as e:
                status["errors"].append(f"{source}:{type(e).__name__}:{url}")
        if bool(status["runtime_guard_stop"]):
            break
    deduped: Dict[str, Dict[str, object]] = {}
    for item in all_items:
        key = str(item.get("originallink") or item.get("link") or item.get("title") or "")
        if key and key not in deduped:
            deduped[key] = item
    items = list(deduped.values())
    status["items"] = int(len(items))
    status["elapsed_sec"] = round(float(time.monotonic() - started), 3)
    return items, status


def _filter_naver_finance_items_for_symbol(items: List[Dict[str, object]], code: str, name: str) -> List[Dict[str, object]]:
    code_s = str(code or "").strip()
    name_s = str(name or "").strip()
    if name_s.lower() in {"nan", "none", "null"}:
        name_s = ""
    out: List[Dict[str, object]] = []
    for item in items:
        hay = " ".join(
            str(item.get(k) or "") for k in ("title", "description", "originallink", "link")
        )
        if (name_s and name_s in hay) or (code_s and code_s in hay):
            out.append(item)
    return out

def _pubdate_to_datetime(v: object) -> Optional[datetime]:
    try:
        dt = parsedate_to_datetime(str(v or ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        return None


def _keyword_hits(text: str) -> Tuple[int, int]:
    pos = 0
    neg = 0
    for kw in POS_KW:
        if kw in text:
            pos += 1
    for kw in NEG_KW:
        if kw in text:
            neg += 1
    return pos, neg


def _extract_entity_tags(text: str) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    t = raw.upper()
    rules: List[Tuple[str, Tuple[str, ...]]] = [
        ("금리", ("금리", "기준금리", "국채", "채권금리", "FOMC", "FED", "BOND YIELD")),
        ("환율", ("환율", "원달러", "USD/KRW", "DOLLAR", "달러", "YEN", "외환", "엔화")),
        ("인플레이션", ("물가", "인플레이션", "CPI", "PPI")),
        ("반도체", ("반도체", "SEMICON", "메모리", "HBM")),
        ("배터리", ("2차전지", "배터리", "BATTERY", "양극재", "음극재", "전해질")),
        ("조선", ("조선", "SHIP", "LNG", "해운")),
        ("자동차", ("자동차", "AUTO", "전기차", "EV")),
        ("바이오", ("바이오", "BIO", "신약", "제약", "임상")),
        ("AI", ("AI", "인공지능", "GPU", "데이터센터")),
        ("정책", ("정책", "규제", "법안", "정부", "국회")),
    ]
    tags: List[str] = []
    for label, kws in rules:
        if any(kw.upper() in t for kw in kws):
            tags.append(label)
    return tags[:6]


def _article_key(code: str, item: Dict[str, object]) -> str:
    primary = _normalize_url(item.get("originallink")) or _normalize_url(item.get("link"))
    if not primary:
        primary = f"{_strip_html(item.get('title'))}|{item.get('pubDate')}"
    return hashlib.sha256(f"{code}|{primary}".encode("utf-8")).hexdigest()


def _candidate_priority_query(name: str, code: str) -> str:
    n = str(name or "").strip()
    c = _norm_code6(code)
    if n:
        return f"{n} 공시"
    return f"{c} 공시"


def _candidate_priority_queries(name: str, code: str, session_name: str) -> List[str]:
    session = str(session_name or "").strip().lower()
    queries: List[str] = [_candidate_priority_query(name, code)]
    if session in {"intraday", "afterhours", "evening", "daily"}:
        queries.extend(_intraday_issue_queries(name, code))

    out: List[str] = []
    seen: set[str] = set()
    for q in queries:
        qn = str(q or "").strip()
        if not qn or qn in seen:
            continue
        seen.add(qn)
        out.append(qn)
    return out


def _intraday_issue_queries(name: str, code: str) -> List[str]:
    n = str(name or "").strip()
    c = _norm_code6(code)
    queries: List[str] = []
    if n:
        queries.append(f"{n} 개별종목 이슈")
        queries.append(f"{n} 특징주")
    elif c:
        queries.append(f"{c} 개별종목 이슈")
        queries.append(f"{c} 특징주")
    # 중복 제거 + 순서 유지
    out: List[str] = []
    seen: set[str] = set()
    for q in queries:
        qn = str(q or "").strip()
        if not qn or qn in seen:
            continue
        seen.add(qn)
        out.append(qn)
    return out


def _extract_articles(
    code: str,
    name: str,
    query: str,
    items: List[Dict[str, object]],
    reference_ymd: str,
    lookback_days: int,
    fetched_at: datetime,
    session_name: str,
    run_id: str,
    backfill_start_dt: Optional[datetime] = None,
    backfill_end_dt: Optional[datetime] = None,
) -> Tuple[List[Dict[str, object]], float, int, int, Dict[str, int]]:
    ref_date = datetime.strptime(reference_ymd, "%Y%m%d").date()
    dmin = ref_date - timedelta(days=max(0, int(lookback_days)))
    rows: List[Dict[str, object]] = []
    seen: set[str] = set()
    pos_total = 0
    neg_total = 0
    tier_counts: Dict[str, int] = {"tier1": 0, "tier2": 0, "tier3": 0, "tier4": 0, "excluded": 0}

    for raw in items:
        if not isinstance(raw, dict):
            continue
        article_id = _article_key(code, raw)
        if article_id in seen:
            continue

        published_dt = _pubdate_to_datetime(raw.get("pubDate"))
        published_date = published_dt.date() if published_dt is not None else None
        if published_date is not None and (published_date < dmin or published_date > ref_date):
            continue
        if published_dt is not None and backfill_start_dt is not None and published_dt < backfill_start_dt:
            continue
        if published_dt is not None and backfill_end_dt is not None and published_dt > backfill_end_dt:
            continue

        title = _strip_html(raw.get("title")).strip()
        desc = _strip_html(raw.get("description")).strip()
        text = (title + " " + desc).strip()
        if not text:
            continue
        origin_link = _normalize_url(raw.get("originallink"))
        link = _normalize_url(raw.get("link"))
        press_tier = _classify_press_tier(origin_link or link)
        tier_counts[press_tier] = int(tier_counts.get(press_tier, 0)) + 1
        if press_tier == "excluded":
            continue

        pos_hits, neg_hits = _keyword_hits(text)
        entity_tags = _extract_entity_tags(text)
        denom = float(pos_hits + neg_hits)
        article_score = 0.0 if denom <= 0 else max(-1.0, min(1.0, (pos_hits - neg_hits) / denom))

        rows.append(
            {
                "article_key": article_id,
                "code": code,
                "name": name,
                "query": query,
                "date8": published_dt.strftime("%Y%m%d") if published_dt is not None else reference_ymd,
                "published_at": published_dt.strftime("%Y-%m-%d %H:%M:%S") if published_dt is not None else "",
                "fetched_at": fetched_at.strftime("%Y-%m-%d %H:%M:%S"),
                "title": title,
                "description": desc,
                "originallink": origin_link,
                "link": link,
                "article_score": round(float(article_score), 6),
                "pos_hits": int(pos_hits),
                "neg_hits": int(neg_hits),
                "entity_tags": "|".join(entity_tags),
                "entity_count": int(len(entity_tags)),
                "session_name": session_name,
                "run_id": run_id,
                "source": str(raw.get("source") or "NAVER_OPENAPI").strip() or "NAVER_OPENAPI",
                "press_tier": press_tier,
            }
        )
        seen.add(article_id)
        pos_total += int(pos_hits)
        neg_total += int(neg_hits)

    denom = float(pos_total + neg_total)
    score = 0.0 if denom <= 0 else max(-1.0, min(1.0, (pos_total - neg_total) / denom))
    return rows, round(float(score), 6), int(pos_total), int(neg_total), tier_counts


def _table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    rows = con.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [str(r[1]) for r in rows]


def _ensure_columns(con: sqlite3.Connection, table: str, cols: Dict[str, str]) -> None:
    existing = set(_table_columns(con, table))
    for col, spec in cols.items():
        if col in existing:
            continue
        con.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {spec}')
    con.commit()


def _pick_col(cols: List[str], keys: List[str]) -> Optional[str]:
    low = {c.lower(): c for c in cols}
    for k in keys:
        if k.lower() in low:
            return low[k.lower()]
    return None


def _ensure_signals_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date8 TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            news_score REAL NOT NULL,
            source TEXT NOT NULL DEFAULT 'NAVER_OPENAPI',
            headline_count INTEGER NOT NULL DEFAULT 0,
            pos_hits INTEGER NOT NULL DEFAULT 0,
            neg_hits INTEGER NOT NULL DEFAULT 0,
            query TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            collected_at TEXT,
            session_name TEXT,
            run_id TEXT,
            window_start TEXT,
            window_end TEXT,
            cache_hit INTEGER NOT NULL DEFAULT 0,
            article_count INTEGER NOT NULL DEFAULT 0,
            UNIQUE(date8, code, source)
        )
        """
    )
    con.commit()


def _ensure_articles_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS news_articles_naver (
            article_key TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            query TEXT,
            date8 TEXT NOT NULL,
            published_at TEXT,
            fetched_at TEXT NOT NULL,
            title TEXT,
            description TEXT,
            originallink TEXT,
            link TEXT,
            article_score REAL NOT NULL DEFAULT 0,
            pos_hits INTEGER NOT NULL DEFAULT 0,
            neg_hits INTEGER NOT NULL DEFAULT 0,
            session_name TEXT,
            run_id TEXT,
            source TEXT NOT NULL DEFAULT 'NAVER_OPENAPI',
            PRIMARY KEY(article_key, code)
        )
        """
    )
    _ensure_columns(
        con,
        "news_articles_naver",
        {
            "entity_tags": "TEXT",
            "entity_count": "INTEGER NOT NULL DEFAULT 0",
        },
    )
    con.commit()


def _cleanup_old_articles(con: sqlite3.Connection, keep_days: int = 90) -> int:
    """keep_days 이전 기사 삭제. 기본 90일 보관으로 발행사 신뢰도 표본을 유지한다."""
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=max(1, int(keep_days)))).strftime("%Y%m%d")
    cur = con.execute("DELETE FROM news_articles_naver WHERE date8 < ?", (cutoff,))
    deleted = cur.rowcount
    return deleted


def _upsert_articles(con: sqlite3.Connection, rows: List[Dict[str, object]]) -> int:
    _ensure_articles_table(con)
    if not rows:
        return 0
    for row in rows:
        con.execute(
            """
            INSERT INTO news_articles_naver(
                article_key, code, name, query, date8, published_at, fetched_at,
                title, description, originallink, link, article_score, pos_hits,
                neg_hits, entity_tags, entity_count, session_name, run_id, source
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(article_key, code) DO UPDATE SET
                name=excluded.name,
                query=excluded.query,
                date8=excluded.date8,
                published_at=excluded.published_at,
                fetched_at=excluded.fetched_at,
                title=excluded.title,
                description=excluded.description,
                originallink=excluded.originallink,
                link=excluded.link,
                article_score=excluded.article_score,
                pos_hits=excluded.pos_hits,
                neg_hits=excluded.neg_hits,
                entity_tags=excluded.entity_tags,
                entity_count=excluded.entity_count,
                session_name=excluded.session_name,
                run_id=excluded.run_id,
                source=excluded.source
            """,
            (
                row["article_key"],
                row["code"],
                row.get("name", ""),
                row.get("query", ""),
                row["date8"],
                row.get("published_at", ""),
                row["fetched_at"],
                row.get("title", ""),
                row.get("description", ""),
                row.get("originallink", ""),
                row.get("link", ""),
                float(row.get("article_score", 0.0)),
                int(row.get("pos_hits", 0)),
                int(row.get("neg_hits", 0)),
                row.get("entity_tags", ""),
                int(row.get("entity_count", 0)),
                row.get("session_name", ""),
                row.get("run_id", ""),
                row.get("source", "NAVER_OPENAPI"),
            ),
        )
    con.commit()
    return int(len(rows))


def _upsert_signals(con: sqlite3.Connection, date8: str, rows: List[Dict[str, object]]) -> Tuple[str, int]:
    tables = [str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    target = "signals" if "signals" in tables else None
    if target is None:
        _ensure_signals_table(con)
        target = "signals"

    cols = _table_columns(con, target)
    code_col = _pick_col(cols, ["code", "ticker", "symbol"])
    date_col = _pick_col(cols, ["date8", "date", "ymd", "trade_date", "signal_date", "created_at", "ts"])
    score_col = _pick_col(cols, ["news_score", "score", "sentiment", "signal_strength", "strength"])

    if not (code_col and date_col and score_col):
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS signals_naver_daily (
                date8 TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                news_score REAL NOT NULL,
                source TEXT NOT NULL DEFAULT 'NAVER_OPENAPI',
                headline_count INTEGER NOT NULL DEFAULT 0,
                pos_hits INTEGER NOT NULL DEFAULT 0,
                neg_hits INTEGER NOT NULL DEFAULT 0,
                query TEXT,
                updated_at TEXT NOT NULL,
                collected_at TEXT,
                session_name TEXT,
                run_id TEXT,
                window_start TEXT,
                window_end TEXT,
                cache_hit INTEGER NOT NULL DEFAULT 0,
                article_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(date8, code)
            )
            """
        )
        target = "signals_naver_daily"
        _ensure_columns(
            con,
            target,
            {
                "collected_at": "TEXT",
                "session_name": "TEXT",
                "run_id": "TEXT",
                "window_start": "TEXT",
                "window_end": "TEXT",
                "cache_hit": "INTEGER NOT NULL DEFAULT 0",
                "article_count": "INTEGER NOT NULL DEFAULT 0",
            },
        )
        for r in rows:
            con.execute(
                """
                INSERT INTO signals_naver_daily(
                    date8, code, name, news_score, source, headline_count,
                    pos_hits, neg_hits, query, updated_at, collected_at,
                    session_name, run_id, window_start, window_end, cache_hit,
                    article_count
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(date8, code) DO UPDATE SET
                    name=excluded.name,
                    news_score=excluded.news_score,
                    source=excluded.source,
                    headline_count=excluded.headline_count,
                    pos_hits=excluded.pos_hits,
                    neg_hits=excluded.neg_hits,
                    query=excluded.query,
                    updated_at=excluded.updated_at,
                    collected_at=excluded.collected_at,
                    session_name=excluded.session_name,
                    run_id=excluded.run_id,
                    window_start=excluded.window_start,
                    window_end=excluded.window_end,
                    cache_hit=excluded.cache_hit,
                    article_count=excluded.article_count
                """,
                (
                    date8,
                    r["code"],
                    r.get("name", ""),
                    float(r.get("news_score", 0.0)),
                    "NAVER_OPENAPI",
                    int(r.get("headline_count", 0)),
                    int(r.get("pos_hits", 0)),
                    int(r.get("neg_hits", 0)),
                    str(r.get("query", "")),
                    str(r.get("updated_at", "")),
                    str(r.get("collected_at", "")),
                    str(r.get("session_name", "")),
                    str(r.get("run_id", "")),
                    str(r.get("window_start", "")),
                    str(r.get("window_end", "")),
                    int(r.get("cache_hit", 0)),
                    int(r.get("article_count", 0)),
                ),
            )
        con.commit()
        return target, int(len(rows))

    _ensure_columns(
        con,
        target,
        {
            "collected_at": "TEXT",
            "session_name": "TEXT",
            "run_id": "TEXT",
            "window_start": "TEXT",
            "window_end": "TEXT",
            "cache_hit": "INTEGER NOT NULL DEFAULT 0",
            "article_count": "INTEGER NOT NULL DEFAULT 0",
        },
    )

    cols = _table_columns(con, target)
    name_col = _pick_col(cols, ["name", "stock_name", "corp_name"])
    source_col = _pick_col(cols, ["source", "provider"])
    cnt_col = _pick_col(cols, ["headline_count", "news_count", "count"])
    pos_col = _pick_col(cols, ["pos_hits", "pos_count", "positive_hits"])
    neg_col = _pick_col(cols, ["neg_hits", "neg_count", "negative_hits"])
    query_col = _pick_col(cols, ["query", "keyword"])
    up_col = _pick_col(cols, ["updated_at", "updated", "ts"])
    created_col = _pick_col(cols, ["created_at"])
    collected_col = _pick_col(cols, ["collected_at", "fetched_at"])
    session_col = _pick_col(cols, ["session_name", "session"])
    run_col = _pick_col(cols, ["run_id", "collector_run_id"])
    wstart_col = _pick_col(cols, ["window_start", "window_start_hhmm"])
    wend_col = _pick_col(cols, ["window_end", "window_end_hhmm"])
    cache_col = _pick_col(cols, ["cache_hit"])
    article_col = _pick_col(cols, ["article_count"])

    for r in rows:
        vals: Dict[str, object] = {
            date_col: date8,
            code_col: r["code"],
            score_col: float(r.get("news_score", 0.0)),
        }
        if name_col:
            vals[name_col] = str(r.get("name", ""))
        if source_col:
            vals[source_col] = "NAVER_OPENAPI"
        if cnt_col:
            vals[cnt_col] = int(r.get("headline_count", 0))
        if pos_col:
            vals[pos_col] = int(r.get("pos_hits", 0))
        if neg_col:
            vals[neg_col] = int(r.get("neg_hits", 0))
        if query_col:
            vals[query_col] = str(r.get("query", ""))
        if up_col:
            vals[up_col] = str(r.get("updated_at", ""))
        if created_col:
            vals[created_col] = str(r.get("collected_at", ""))
        if collected_col:
            vals[collected_col] = str(r.get("collected_at", ""))
        if session_col:
            vals[session_col] = str(r.get("session_name", ""))
        if run_col:
            vals[run_col] = str(r.get("run_id", ""))
        if wstart_col:
            vals[wstart_col] = str(r.get("window_start", ""))
        if wend_col:
            vals[wend_col] = str(r.get("window_end", ""))
        if cache_col:
            vals[cache_col] = int(r.get("cache_hit", 0))
        if article_col:
            vals[article_col] = int(r.get("article_count", 0))

        where = f'"{date_col}"=? AND "{code_col}"=?'
        cur = con.execute(f'SELECT 1 FROM "{target}" WHERE {where} LIMIT 1', (date8, r["code"]))
        exists = cur.fetchone() is not None

        if exists:
            set_cols = [k for k in vals.keys() if k not in (date_col, code_col)]
            if set_cols:
                set_sql = ", ".join([f'"{k}"=?' for k in set_cols])
                params = [vals[k] for k in set_cols] + [date8, r["code"]]
                con.execute(f'UPDATE "{target}" SET {set_sql} WHERE {where}', params)
        else:
            csql = ", ".join([f'"{k}"' for k in vals.keys()])
            psql = ", ".join(["?"] * len(vals))
            con.execute(f'INSERT INTO "{target}" ({csql}) VALUES ({psql})', list(vals.values()))

    con.commit()
    return target, int(len(rows))


def _load_credentials() -> Tuple[str, str, str]:
    cid = str(os.getenv("NAVER_CLIENT_ID", "")).strip()
    csec = str(os.getenv("NAVER_CLIENT_SECRET", "")).strip()
    if cid and csec:
        return cid, csec, "env"

    if KEY_FILE.exists():
        try:
            obj = json.loads(KEY_FILE.read_text(encoding="utf-8"))
            cid2 = str(obj.get("naver_client_id", "")).strip()
            csec2 = str(obj.get("naver_client_secret", "")).strip()
            if cid2 and csec2:
                return cid2, csec2, "key_file"
        except Exception:
            pass

    return "", "", "missing"


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Collect Naver news into the local news DB.")
    ap.add_argument("--session-name", default=str(os.getenv("NEWS_SESSION_NAME", "daily")).strip() or "daily")
    ap.add_argument("--window-start", default=str(os.getenv("NEWS_WINDOW_START", "")).strip())
    ap.add_argument("--window-end", default=str(os.getenv("NEWS_WINDOW_END", "")).strip())
    ap.add_argument("--display", type=int, default=max(1, min(int(str(os.getenv("NEWS_DISPLAY", "20")).strip() or "20"), 100)))
    ap.add_argument("--lookback-days", type=int, default=max(0, int(str(os.getenv("NEWS_LOOKBACK_DAYS", "3")).strip() or "3")))
    ap.add_argument("--retry-attempts", type=int, default=max(0, int(str(os.getenv("NEWS_FETCH_RETRIES", "2")).strip() or "2")))
    ap.add_argument("--retry-backoff-sec", type=float, default=max(0.0, float(str(os.getenv("NEWS_FETCH_BACKOFF_SEC", "1.0")).strip() or "1.0")))
    ap.add_argument("--cache-ttl-sec", type=int, default=max(0, int(str(os.getenv("NEWS_CACHE_TTL_SEC", "300")).strip() or "300")))
    ap.add_argument("--lock-ttl-sec", type=int, default=max(60, int(str(os.getenv("NEWS_COLLECT_LOCK_TTL_SEC", "3600")).strip() or "3600")))
    ap.add_argument("--include-sector-peers", action="store_true", default=_env_bool("NEWS_INCLUDE_SECTOR_PEERS", False))
    ap.add_argument("--max-symbols", type=int, default=max(0, int(str(os.getenv("NEWS_MAX_SYMBOLS", "120")).strip() or "120")))
    ap.add_argument("--daily-query-budget", type=int, default=max(1, int(str(os.getenv("NEWS_DAILY_QUERY_BUDGET", "20000")).strip() or "20000")))
    ap.add_argument("--hourly-query-budget", type=int, default=max(1, int(str(os.getenv("NEWS_HOURLY_QUERY_BUDGET", "600")).strip() or "600")))
    ap.add_argument("--allow-weekend", action="store_true", default=_env_bool("NEWS_ALLOW_WEEKEND", False))
    ap.add_argument("--holidays-file", default=str(os.getenv("NEWS_HOLIDAYS_FILE", str(DEFAULT_HOLIDAYS_PATH))).strip())
    ap.add_argument("--allow-holiday", action="store_true", default=_env_bool("NEWS_ALLOW_HOLIDAY", False))
    ap.add_argument("--force-outside-window", action="store_true", default=_env_bool("NEWS_FORCE_OUTSIDE_WINDOW", False))
    ap.add_argument("--max-runtime-sec", type=float, default=max(0.0, float(str(os.getenv("NEWS_COLLECT_MAX_RUNTIME_SEC", "0")).strip() or "0")))
    return ap.parse_args()


def _finalize_status(status: Dict[str, object]) -> Dict[str, object]:
    symbols = int(status.get("symbols") or 0)
    fetched = int(status.get("fetched") or 0)
    saved = int(status.get("saved") or 0)
    article_rows_saved = int(status.get("article_rows_saved") or 0)
    errors = status.get("errors") if isinstance(status.get("errors"), list) else []
    err_cnt = int(len(errors))

    if symbols > 0:
        fetch_rate = float(fetched) / float(symbols)
        save_rate = float(saved) / float(symbols)
    else:
        fetch_rate = 0.0
        save_rate = 0.0

    reason = str(status.get("reason") or "")
    if reason == "ok" and save_rate >= 0.80 and err_cnt == 0:
        quality = "PASS"
    elif reason == "naver_quota_exceeded":
        # Quota exhaustion is an external limit; keep it visible as operational warning.
        quality = "WARN"
    elif reason == "quota_budget_guard":
        # Internal pacing guard to distribute API calls over the day.
        quality = "WARN"
    elif reason == "session_budget_guard":
        # Session-level pacing guard to spread calls across day-part sessions.
        quality = "WARN"
    elif reason == "max_runtime_guard":
        quality = "WARN" if fetched > 0 else "FAIL"
    elif reason in {"ok", "no_rows_saved"} and save_rate >= 0.50:
        quality = "WARN"
    elif reason == "outside_time_window":
        quality = "REFERENCE"
    elif reason == "intraday_external_collect_guard_skip":
        quality = "REFERENCE"
    else:
        quality = "FAIL"

    status["fetch_rate"] = round(fetch_rate, 6)
    status["save_rate"] = round(save_rate, 6)
    status["error_count"] = err_cnt
    status["errors_preview"] = [str(x)[:120] for x in errors[:10]]
    status["article_rows_saved"] = article_rows_saved
    status["quality"] = quality
    return status


def _write_status(status: Dict[str, object]) -> None:
    asof = str(status.get("asof_ymd") or _now_kst().strftime("%Y%m%d"))
    p1 = LOGS / f"news_collect_status_{asof}.json"
    p2 = LOGS / "news_collect_status_latest.json"
    text = json.dumps(status, ensure_ascii=False, indent=2)
    p1.write_text(text, encoding="utf-8")
    p2.write_text(text, encoding="utf-8")


def _apply_collect_mode_defaults() -> str:
    """NEWS_COLLECT_MODE 기반 파라미터 기본값 적용.
    이미 설정된 개별 env var는 덮어쓰지 않음 — 개별 값이 항상 우선."""
    mode = str(os.getenv("NEWS_COLLECT_MODE", "production")).strip().lower()
    if mode not in {"accumulate", "production"}:
        mode = "production"
    presets: Dict[str, Dict[str, str]] = {
        "accumulate": {
            "NEWS_INCLUDE_SECTOR_PEERS": "true",
            "NEWS_LOOKBACK_DAYS": "7",
            "NEWS_DISPLAY": "50",
            "NEWS_MAX_SYMBOLS": "200",
            "NEWS_MAX_LAG_DAYS": "4",
            "NEWS_SCORE_PASS_MAPPED_RATE": "0.30",
            "NEWS_SCORE_WARN_MAPPED_RATE": "0.20",
            "NEWS_GATE_MIN_MAPPED_RATE": "0.20",
        },
        "production": {
            "NEWS_INCLUDE_SECTOR_PEERS": "false",
            "NEWS_LOOKBACK_DAYS": "3",
            "NEWS_DISPLAY": "20",
            "NEWS_MAX_SYMBOLS": "120",
            "NEWS_MAX_LAG_DAYS": "2",
            "NEWS_SCORE_PASS_MAPPED_RATE": "0.60",
            "NEWS_SCORE_WARN_MAPPED_RATE": "0.40",
            "NEWS_GATE_MIN_MAPPED_RATE": "0.50",
        },
    }
    for k, v in presets[mode].items():
        if not os.getenv(k):
            os.environ[k] = v
    return mode


def main() -> int:
    collect_mode = _apply_collect_mode_defaults()
    try:
        socket.setdefaulttimeout(max(1.0, float(str(os.getenv("NEWS_COLLECT_SOCKET_TIMEOUT_SEC", "10")).strip() or "10")))
    except Exception:
        pass
    args = _parse_args()
    started_mono = time.monotonic()
    now = _now_kst()
    today_ymd = now.strftime("%Y%m%d")
    run_id = now.strftime("newscollect_%Y%m%d_%H%M%S")
    requested_session_name = str(args.session_name or "").strip().lower() or "daily"
    resolved_window_start, resolved_window_end = _resolve_session_window(
        requested_session_name,
        str(args.window_start),
        str(args.window_end),
    )
    start_tm = _parse_hhmm(resolved_window_start)
    end_tm = _parse_hhmm(resolved_window_end)
    outside_window = not _time_in_window(now.time(), start_tm, end_tm)
    auto_extended_session = outside_window and requested_session_name in {"daily", "intraday"}
    effective_force_outside = bool(args.force_outside_window or auto_extended_session)
    session_label = _session_label_for_now(now.time())
    effective_session_name = session_label.lower() if auto_extended_session else requested_session_name

    input_path = _pick_input()
    input_asof_ymd = _input_asof_ymd(input_path) if input_path else ""
    reference_ymd = max(input_asof_ymd or "", today_ymd)
    backfill_end_dt = now
    backfill_start_dt = _load_last_success_dt(LAST_SUCCESS_PATH)
    cid, csec, cred_src = _load_credentials()

    status: Dict[str, object] = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "input": str(input_path) if input_path else None,
        "input_asof_ymd": input_asof_ymd or None,
        "reference_ymd": reference_ymd,
        "db": str(DB),
        "enabled": True,
        "reason": "",
        "asof_ymd": reference_ymd,
        "collect_mode": collect_mode,
        "symbols": 0,
        "fetched": 0,
        "saved": 0,
        "article_rows_saved": 0,
        "table": None,
        "article_table": "news_articles_naver",
        "credential_source": cred_src,
        "session_name": effective_session_name,
        "requested_session_name": requested_session_name,
        "session_label": session_label,
        "window_start": resolved_window_start,
        "window_end": resolved_window_end,
        "lookback_days": int(args.lookback_days),
        "display": int(args.display),
        "retry_attempts": int(args.retry_attempts),
        "retry_backoff_sec": float(args.retry_backoff_sec),
        "cache_ttl_sec": int(args.cache_ttl_sec),
        "lock_ttl_sec": int(args.lock_ttl_sec),
        "max_runtime_sec": float(args.max_runtime_sec),
        "elapsed_sec": 0.0,
        "include_sector_peers": bool(args.include_sector_peers),
        "max_symbols": int(args.max_symbols),
        "daily_query_budget": int(args.daily_query_budget),
        "hourly_query_budget": int(args.hourly_query_budget),
        "holidays_file": str(args.holidays_file or ""),
        "run_id": run_id,
        "cache_hits": 0,
        "cache_misses": 0,
        "retry_count": 0,
        "weekend_skip": False,
        "holiday_skip": False,
        "holiday_reason": None,
        "outside_window_skip": False,
        "outside_window_reference": bool(outside_window),
        "auto_extended_session": bool(auto_extended_session),
        "lock_wait": None,
        "errors": [],
        "quota_exhausted_external": False,
        "fallback_mode": "",
        "scope": {
            "candidate_symbols": 0,
            "holding_symbols": 0,
            "sector_peer_symbols": 0,
            "index_symbols": 0,
            "total_before_cap": 0,
            "total_symbols": 0,
            "total_after_cap": 0,
            "cap_limit": int(args.max_symbols),
            "cap_applied": 0,
            "include_sector_peers": 1 if bool(args.include_sector_peers) else 0,
            "index_source_ready": False,
        },
        "quota_budget": {
            "daily_budget": int(args.daily_query_budget),
            "hourly_budget": int(args.hourly_query_budget),
            "daily_used": 0,
            "hourly_used": 0,
            "session_name": str(args.session_name),
            "session_daily_budget": 0,
            "session_daily_used": 0,
            "session_guard_stop": False,
            "quota_state_path": str(QUOTA_STATE_PATH),
            "guard_stop": False,
        },
        "source_policy": {
            "press_source": "naver_openapi+naver_finance_direct",
            "press_allowed_tiers": ["tier1", "tier2", "tier3", "tier4"],
            "direct_sources": ["https://finance.naver.com/news/", "https://finance.naver.com/research/"],
            **_pipeline_source_binding(),
        },
        "source_counts": {
            "tier1": 0,
            "tier2": 0,
            "tier3": 0,
            "tier4": 0,
            "excluded": 0,
        },
        "candidate_priority_retry_count": 0,
        "candidate_priority_recovered_count": 0,
        "backfill_start_at": backfill_start_dt.strftime("%Y-%m-%d %H:%M:%S") if backfill_start_dt else None,
        "backfill_end_at": backfill_end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }

    if not input_path or (not input_path.exists()):
        status["reason"] = "input_missing"
        _finalize_status(status)
        _write_status(status)
        _log_print("[NEWS_COLLECT] input missing -> skip")
        return 0

    if now.weekday() >= 5 and not args.allow_weekend:
        status["reason"] = "weekend_guard_skip"
        status["weekend_skip"] = True
        _finalize_status(status)
        _write_status(status)
        _log_print("[NEWS_COLLECT] weekend guard -> skip")
        return 0

    holidays_ymd = _load_holidays_ymd(Path(str(args.holidays_file or "")))
    if today_ymd in holidays_ymd and not args.allow_holiday:
        status["reason"] = f"holiday_guard_skip:{today_ymd}"
        status["holiday_skip"] = True
        status["holiday_reason"] = f"holiday ymd={today_ymd}"
        _finalize_status(status)
        _write_status(status)
        _log_print(f"[NEWS_COLLECT] holiday guard -> skip ymd={today_ymd}")
        return 0

    if not effective_force_outside and outside_window:
        status["reason"] = "outside_time_window"
        status["outside_window_skip"] = True
        _finalize_status(status)
        _write_status(status)
        _log_print("[NEWS_COLLECT] outside configured window -> skip")
        return 0

    intraday_external_enabled = _env_bool("NEWS_INTRADAY_EXTERNAL_COLLECT_ENABLED", False)
    if effective_session_name == "intraday" and not intraday_external_enabled:
        status["reason"] = "intraday_external_collect_guard_skip"
        status["enabled"] = False
        status["fallback_mode"] = "intraday_external_collect_disabled_fail_soft"
        _finalize_status(status)
        _write_status(status)
        _log_print("[NEWS_COLLECT] intraday external collect guard -> skip")
        return 0

    locked, lock_info = _acquire_lock(LOCK_PATH, int(args.lock_ttl_sec), run_id)
    if not locked:
        status["reason"] = "lock_busy"
        status["lock_wait"] = lock_info
        _finalize_status(status)
        _write_status(status)
        _log_print("[NEWS_COLLECT] existing lock detected -> skip")
        return 0

    try:
        df = _read_csv(input_path)
        if "code" not in df.columns:
            status["reason"] = "input_no_code"
            _finalize_status(status)
            _write_status(status)
            _log_print("[NEWS_COLLECT] input has no code column -> skip")
            return 0

        if "name" not in df.columns:
            df["name"] = ""

        df["code"] = df["code"].map(_norm_code6)
        symbols, scope_counts = _build_symbol_scope(
            df,
            include_sector_peers=bool(args.include_sector_peers),
            max_symbols=int(args.max_symbols),
        )
        surge_symbols_for_coverage = _load_surge_symbols()
        status["symbols"] = int(len(symbols))
        status["scope"] = {
            **scope_counts,
            "index_source_ready": False,
        }
        status["coverage"] = {
            "surge_signal_coverage": _compute_signal_coverage(reference_ymd, surge_symbols_for_coverage),
        }

        if not cid or not csec:
            status["reason"] = "missing_naver_api_keys"
            _finalize_status(status)
            _write_status(status)
            _log_print("[NEWS_COLLECT] missing NAVER_CLIENT_ID/SECRET -> skip")
            return 0

        signal_rows: List[Dict[str, object]] = []
        article_rows_all: List[Dict[str, object]] = []
        fetched_at = _now_kst()
        quota_exceeded = False
        quota_error_text = ""
        quota_guard_stop = False
        session_guard_stop = False
        max_runtime_stop = False
        naver_finance_items, naver_finance_status = _fetch_naver_finance_items(reference_ymd)
        status["naver_finance_sources"] = naver_finance_status

        quota_state = _load_quota_state(QUOTA_STATE_PATH)
        quota_now = _now_kst()
        quota_ymd = quota_now.strftime("%Y%m%d")
        quota_hour_key = quota_now.strftime("%Y%m%d%H")
        quota_daily_budget = int(max(1, int(args.daily_query_budget)))
        quota_hourly_budget = int(max(1, int(args.hourly_query_budget)))
        session_name = str(effective_session_name or "").strip().lower()
        session_ratio = float(SESSION_DAILY_BUDGET_RATIO.get(session_name, SESSION_DAILY_BUDGET_RATIO.get("daily", 1.0)))
        session_daily_budget = max(1, int(quota_daily_budget * max(0.0, min(1.0, session_ratio))))
        session_daily_key = f"{quota_ymd}:{session_name or 'unknown'}"
        session_daily_used_map = quota_state.get("session_daily_used")
        if not isinstance(session_daily_used_map, dict):
            session_daily_used_map = {}
        try:
            session_daily_used = int(session_daily_used_map.get(session_daily_key, 0) or 0)
        except Exception:
            session_daily_used = 0
        quota_state["session_daily_used"] = session_daily_used_map
        if str(quota_state.get("ymd") or "") != quota_ymd:
            quota_state["ymd"] = quota_ymd
            quota_state["daily_used"] = 0
            quota_state["session_daily_used"] = {}
            session_daily_used_map = quota_state["session_daily_used"]
            session_daily_used = 0
        if str(quota_state.get("hour_key") or "") != quota_hour_key:
            quota_state["hour_key"] = quota_hour_key
            quota_state["hourly_used"] = 0
        quota_state["ymd"] = quota_ymd
        quota_state["hour_key"] = quota_hour_key
        status["quota_budget"] = {
            "daily_budget": quota_daily_budget,
            "hourly_budget": quota_hourly_budget,
            "daily_used": int(quota_state.get("daily_used") or 0),
            "hourly_used": int(quota_state.get("hourly_used") or 0),
            "session_name": session_name,
            "session_daily_budget": int(session_daily_budget),
            "session_daily_used": int(session_daily_used),
            "session_guard_stop": False,
            "quota_state_path": str(QUOTA_STATE_PATH),
            "guard_stop": False,
        }
        _save_quota_state(QUOTA_STATE_PATH, quota_state)

        for _, r in symbols.iterrows():
            status["elapsed_sec"] = round(float(time.monotonic() - started_mono), 3)
            if float(args.max_runtime_sec) > 0 and float(status["elapsed_sec"]) >= float(args.max_runtime_sec):
                max_runtime_stop = True
                status["reason"] = "max_runtime_guard"
                break
            code = str(r.get("code", ""))
            name = str(r.get("name", "")).strip()
            source_scope = str(r.get("source_scope", "")).strip().lower()
            if not code:
                continue
            if quota_exceeded:
                break
            if int(quota_state.get("daily_used") or 0) >= quota_daily_budget:
                quota_guard_stop = True
                status["reason"] = "quota_budget_guard"
                break
            if int(quota_state.get("hourly_used") or 0) >= quota_hourly_budget:
                quota_guard_stop = True
                status["reason"] = "quota_budget_guard"
                break
            if int(session_daily_used) >= int(session_daily_budget):
                session_guard_stop = True
                status["reason"] = "session_budget_guard"
                break

            query = (f"{name} {code} 주식") if name else (code + " 주식")
            used_query = query
            cache_hit_pre = _is_cache_hit(query, reference_ymd, int(args.display), int(args.cache_ttl_sec))
            if not cache_hit_pre:
                session_daily_used = _charge_quota(quota_state, session_daily_used_map, session_daily_key)
                _save_quota_state(QUOTA_STATE_PATH, quota_state)
            try:
                payload, cache_hit, retries_used = _fetch_naver_news(
                    query=query,
                    client_id=cid,
                    client_secret=csec,
                    display=int(args.display),
                    retry_attempts=int(args.retry_attempts),
                    retry_backoff_sec=float(args.retry_backoff_sec),
                    cache_ttl_sec=int(args.cache_ttl_sec),
                    reference_ymd=reference_ymd,
                )
                items = payload.get("items", []) if isinstance(payload, dict) else []
                finance_items_for_symbol = _filter_naver_finance_items_for_symbol(naver_finance_items, code, name)
                if finance_items_for_symbol:
                    items = list(items if isinstance(items, list) else []) + finance_items_for_symbol
                    naver_finance_status["matched_items"] = int(naver_finance_status.get("matched_items", 0) or 0) + int(len(finance_items_for_symbol))
                if cache_hit:
                    status["cache_hits"] = int(status.get("cache_hits", 0)) + 1
                else:
                    status["cache_misses"] = int(status.get("cache_misses", 0)) + 1
                status["retry_count"] = int(status.get("retry_count", 0)) + int(retries_used)

                # Article pool should represent the recent lookback window for downstream
                # scoring/candidate use, not only the incremental window since last success.
                article_rows, score, pos, neg, tier_counts = _extract_articles(
                    code=code,
                    name=name,
                    query=query,
                    items=items if isinstance(items, list) else [],
                    reference_ymd=reference_ymd,
                    lookback_days=int(args.lookback_days),
                    fetched_at=fetched_at,
                    session_name=session_label,
                    run_id=run_id,
                    backfill_start_dt=None,
                    backfill_end_dt=backfill_end_dt,
                )
                for tier_name, tier_n in tier_counts.items():
                    status["source_counts"][tier_name] = int(status["source_counts"].get(tier_name, 0)) + int(tier_n)

                # 후보 종목 우선 수집 강화: 기본 질의가 빈 결과면 추가 질의 1회 재시도
                if source_scope == "candidate" and len(article_rows) == 0:
                    for retry_query in _candidate_priority_queries(name, code, effective_session_name):
                        status["elapsed_sec"] = round(float(time.monotonic() - started_mono), 3)
                        if float(args.max_runtime_sec) > 0 and float(status["elapsed_sec"]) >= float(args.max_runtime_sec):
                            max_runtime_stop = True
                            status["reason"] = "max_runtime_guard"
                            break
                        if (
                            int(quota_state.get("daily_used") or 0) >= quota_daily_budget
                            or int(quota_state.get("hourly_used") or 0) >= quota_hourly_budget
                            or int(session_daily_used) >= int(session_daily_budget)
                        ):
                            break
                        status["candidate_priority_retry_count"] = int(status.get("candidate_priority_retry_count", 0)) + 1
                        retry_cache_hit_pre = _is_cache_hit(retry_query, reference_ymd, int(args.display), int(args.cache_ttl_sec))
                        if not retry_cache_hit_pre:
                            session_daily_used = _charge_quota(quota_state, session_daily_used_map, session_daily_key)
                            _save_quota_state(QUOTA_STATE_PATH, quota_state)
                        retry_payload, retry_cache_hit, retry_retries_used = _fetch_naver_news(
                            query=retry_query,
                            client_id=cid,
                            client_secret=csec,
                            display=int(args.display),
                            retry_attempts=int(args.retry_attempts),
                            retry_backoff_sec=float(args.retry_backoff_sec),
                            cache_ttl_sec=int(args.cache_ttl_sec),
                            reference_ymd=reference_ymd,
                        )
                        retry_items = retry_payload.get("items", []) if isinstance(retry_payload, dict) else []
                        if retry_cache_hit:
                            status["cache_hits"] = int(status.get("cache_hits", 0)) + 1
                        else:
                            status["cache_misses"] = int(status.get("cache_misses", 0)) + 1
                        status["retry_count"] = int(status.get("retry_count", 0)) + int(retry_retries_used)

                        retry_rows, retry_score, retry_pos, retry_neg, retry_tier_counts = _extract_articles(
                            code=code,
                            name=name,
                            query=retry_query,
                            items=retry_items if isinstance(retry_items, list) else [],
                            reference_ymd=reference_ymd,
                            lookback_days=int(args.lookback_days),
                            fetched_at=fetched_at,
                            session_name=session_label,
                            run_id=run_id,
                            # 후보 우선 재질의는 초기 부트스트랩 목적이라 최근 lookback 범위까지 허용
                            backfill_start_dt=None,
                            backfill_end_dt=backfill_end_dt,
                        )
                        for tier_name, tier_n in retry_tier_counts.items():
                            status["source_counts"][tier_name] = int(status["source_counts"].get(tier_name, 0)) + int(tier_n)
                        if len(retry_rows) > 0:
                            article_rows = retry_rows
                            score = retry_score
                            pos = retry_pos
                            neg = retry_neg
                            used_query = retry_query
                            status["candidate_priority_recovered_count"] = int(status.get("candidate_priority_recovered_count", 0)) + 1
                            break
                # 장내 수집 강화: 후보/보유 종목에 대해 개별종목 이슈 + 특징주 질의를 추가 반영
                intraday_extra_rows: List[Dict[str, object]] = []
                intraday_extra_queries: List[str] = []
                if str(effective_session_name or "").strip().lower() == "intraday" and source_scope in {"candidate", "holding"}:
                    for extra_query in _intraday_issue_queries(name, code):
                        status["elapsed_sec"] = round(float(time.monotonic() - started_mono), 3)
                        if float(args.max_runtime_sec) > 0 and float(status["elapsed_sec"]) >= float(args.max_runtime_sec):
                            max_runtime_stop = True
                            status["reason"] = "max_runtime_guard"
                            break
                        if int(quota_state.get("daily_used") or 0) >= quota_daily_budget:
                            quota_guard_stop = True
                            status["reason"] = "quota_budget_guard"
                            break
                        if int(quota_state.get("hourly_used") or 0) >= quota_hourly_budget:
                            quota_guard_stop = True
                            status["reason"] = "quota_budget_guard"
                            break
                        if int(session_daily_used) >= int(session_daily_budget):
                            session_guard_stop = True
                            status["reason"] = "session_budget_guard"
                            break

                        extra_cache_hit_pre = _is_cache_hit(extra_query, reference_ymd, int(args.display), int(args.cache_ttl_sec))
                        if not extra_cache_hit_pre:
                            session_daily_used = _charge_quota(quota_state, session_daily_used_map, session_daily_key)
                            _save_quota_state(QUOTA_STATE_PATH, quota_state)
                        status["intraday_issue_query_count"] = int(status.get("intraday_issue_query_count", 0)) + 1

                        extra_payload, extra_cache_hit, extra_retries_used = _fetch_naver_news(
                            query=extra_query,
                            client_id=cid,
                            client_secret=csec,
                            display=int(args.display),
                            retry_attempts=int(args.retry_attempts),
                            retry_backoff_sec=float(args.retry_backoff_sec),
                            cache_ttl_sec=int(args.cache_ttl_sec),
                            reference_ymd=reference_ymd,
                        )
                        extra_items = extra_payload.get("items", []) if isinstance(extra_payload, dict) else []
                        if extra_cache_hit:
                            status["cache_hits"] = int(status.get("cache_hits", 0)) + 1
                        else:
                            status["cache_misses"] = int(status.get("cache_misses", 0)) + 1
                        status["retry_count"] = int(status.get("retry_count", 0)) + int(extra_retries_used)

                        extra_rows, _, _, _, extra_tier_counts = _extract_articles(
                            code=code,
                            name=name,
                            query=extra_query,
                            items=extra_items if isinstance(extra_items, list) else [],
                            reference_ymd=reference_ymd,
                            lookback_days=int(args.lookback_days),
                            fetched_at=fetched_at,
                            session_name=session_label,
                            run_id=run_id,
                            backfill_start_dt=None,
                            backfill_end_dt=backfill_end_dt,
                        )
                        for tier_name, tier_n in extra_tier_counts.items():
                            status["source_counts"][tier_name] = int(status["source_counts"].get(tier_name, 0)) + int(tier_n)
                        if len(extra_rows) > 0:
                            intraday_extra_rows.extend(extra_rows)
                            intraday_extra_queries.append(extra_query)

                if len(intraday_extra_rows) > 0:
                    merged_rows: Dict[str, Dict[str, object]] = {}
                    for ar in article_rows + intraday_extra_rows:
                        k = str(ar.get("article_key", ""))
                        if k and k not in merged_rows:
                            merged_rows[k] = ar
                    article_rows = list(merged_rows.values()) if len(merged_rows) > 0 else article_rows
                    pos = int(sum(int(x.get("pos_hits", 0) or 0) for x in article_rows))
                    neg = int(sum(int(x.get("neg_hits", 0) or 0) for x in article_rows))
                    denom = float(pos + neg)
                    score = 0.0 if denom <= 0 else max(-1.0, min(1.0, (pos - neg) / denom))
                    used_query = " | ".join([q for q in [used_query] + intraday_extra_queries if str(q).strip()])
                    status["intraday_issue_hit_symbols"] = int(status.get("intraday_issue_hit_symbols", 0)) + 1
                article_rows_all.extend(article_rows)
                signal_rows.append(
                    {
                        "code": code,
                        "name": name,
                        "query": used_query,
                        "news_score": float(score),
                        "headline_count": int(len(article_rows)),
                        "pos_hits": int(pos),
                        "neg_hits": int(neg),
                        "updated_at": fetched_at.strftime("%Y-%m-%d %H:%M:%S"),
                        "collected_at": fetched_at.strftime("%Y-%m-%d %H:%M:%S"),
                        "session_name": session_label,
                        "run_id": run_id,
                        "window_start": str(resolved_window_start),
                        "window_end": str(resolved_window_end),
                        "cache_hit": 1 if cache_hit else 0,
                        "article_count": int(len(article_rows)),
                    }
                )
                status["fetched"] = int(status.get("fetched", 0)) + 1
            except Exception as e:
                if isinstance(e, urllib.error.HTTPError) and int(getattr(e, "code", 0) or 0) == 429:
                    quota_exceeded = True
                    status["quota_exhausted_external"] = True
                    try:
                        quota_error_text = e.read().decode("utf-8", "ignore")[:300]
                    except Exception:
                        quota_error_text = ""
                    status["errors"].append(f"{code}:HTTPError:429")
                    break
                status["errors"].append(f"{code}:{type(e).__name__}")

        status["elapsed_sec"] = round(float(time.monotonic() - started_mono), 3)
        status["quota_budget"] = {
            "daily_budget": quota_daily_budget,
            "hourly_budget": quota_hourly_budget,
            "daily_used": int(quota_state.get("daily_used") or 0),
            "hourly_used": int(quota_state.get("hourly_used") or 0),
            "session_name": session_name,
            "session_daily_budget": int(session_daily_budget),
            "session_daily_used": int(session_daily_used),
            "session_guard_stop": bool(session_guard_stop),
            "quota_state_path": str(QUOTA_STATE_PATH),
            "guard_stop": bool(quota_guard_stop),
        }
        _save_quota_state(QUOTA_STATE_PATH, quota_state)

        DB.parent.mkdir(parents=True, exist_ok=True)
        status["db_write_preflight"] = _db_writer_preflight(
            timeout_sec=max(1.0, min(10.0, _sqlite_timeout_sec()))
        )
        try:
            attempts = max(1, int(str(os.getenv("NEWS_COLLECT_SQLITE_RETRY", "3")).strip() or "3"))
            for attempt in range(1, attempts + 1):
                con = _connect_news_db()
                try:
                    article_saved = _upsert_articles(con, article_rows_all)
                    table, saved = _upsert_signals(con, reference_ymd, signal_rows)
                    keep_days = max(1, int(str(os.getenv("NEWS_DB_KEEP_DAYS", "90")).strip() or "90"))
                    article_deleted = _cleanup_old_articles(con, keep_days=keep_days)
                    con.commit()
                    if article_deleted > 0:
                        try:
                            con.execute("VACUUM")
                        except Exception:
                            pass
                    break
                except sqlite3.OperationalError as e:
                    try:
                        con.rollback()
                    except Exception:
                        pass
                    if "locked" not in str(e).lower() or attempt >= attempts:
                        raise
                    time.sleep(min(5.0, 0.5 * attempt))
                finally:
                    con.close()
            status["table"] = table
            status["saved"] = int(saved)
            status["article_rows_saved"] = int(article_saved)
            status["article_rows_deleted"] = int(article_deleted)
            status["coverage"] = {
                "surge_signal_coverage": _compute_signal_coverage(reference_ymd, surge_symbols_for_coverage),
            }
            if quota_exceeded:
                status["reason"] = "naver_quota_exceeded"
                status["fallback_mode"] = "external_quota_soft_skip"
                if quota_error_text:
                    status["quota_error"] = quota_error_text
            elif quota_guard_stop:
                status["reason"] = "quota_budget_guard"
                status["fallback_mode"] = "budget_guard_soft_skip"
            elif session_guard_stop:
                status["reason"] = "session_budget_guard"
                status["fallback_mode"] = "session_budget_soft_skip"
            elif max_runtime_stop:
                status["reason"] = "max_runtime_guard"
                status["fallback_mode"] = "runtime_guard_partial_collect"
            else:
                status["reason"] = "ok" if saved > 0 else "no_rows_saved"
        except Exception as e:
            status["reason"] = f"db_write_fail:{type(e).__name__}"
            status["errors"].append(str(e)[:200])

        _finalize_status(status)
        _write_status(status)
        status["last_success_recorded"] = bool(_should_record_last_success(status))
        if bool(status.get("last_success_recorded")):
            _write_last_success(LAST_SUCCESS_PATH, now, session_label, run_id)
        _log_print(
            f"[NEWS_COLLECT] asof={reference_ymd} symbols={status['symbols']} "
            f"fetched={status['fetched']} saved={status['saved']} "
            f"articles={status['article_rows_saved']} cache_hits={status['cache_hits']} "
            f"reason={status['reason']} cred={status['credential_source']}"
        )
        return 0
    finally:
        _release_lock(LOCK_PATH, run_id)


if __name__ == "__main__":
    raise SystemExit(main())
