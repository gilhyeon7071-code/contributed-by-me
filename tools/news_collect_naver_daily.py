from __future__ import annotations

import json
import os
import re
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
DB = ROOT / "news_trading" / "data" / "trading.db"
KEY_FILE = ROOT / "paper" / "news_api_keys.json"

IN_FILES = [
    LOGS / "candidates_latest_data.with_final_score.csv",
    LOGS / "candidates_latest_data.with_news_score.csv",
    LOGS / "candidates_latest_data.with_sector_score.csv",
    LOGS / "candidates_latest_data.filtered.csv",
    LOGS / "candidates_latest_data.csv",
]

API_URL = "https://openapi.naver.com/v1/search/news.json"
KST = timezone(timedelta(hours=9))

# Korean keywords encoded with \u escapes to keep source ASCII-safe.
POS_KW = [
    "\uc0c1\uc2b9", "\uae09\ub4f1", "\ud638\uc7ac", "\uc2e4\uc801\uac1c\uc120", "\uc218\uc8fc",
    "\uc2e0\uace0\uac00", "\ub9e4\uc218", "\uc131\uc7a5", "\ud751\uc790", "\uac15\uc138",
    "\ub3cc\ud30c", "\ud655\ub300", "\ud68c\ubcf5", "\ubc18\ub4f1", "\uc99d\uac00",
]
NEG_KW = [
    "\ud558\ub77d", "\uae09\ub77d", "\uc545\uc7ac", "\uc2e4\uc801\uc545\ud654", "\uc801\uc790",
    "\uc2e0\uc800\uac00", "\ub9e4\ub3c4", "\uc57d\uc138", "\ubd80\uc9c4", "\uc6b0\ub824",
    "\ucd95\uc18c", "\uac10\uc18c", "\ub9ac\uc2a4\ud06c", "\ud3ed\ub77d", "\uc545\ud654",
]


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


def _pick_input() -> Optional[Path]:
    # Prefer the newest available artifact by as-of date, not the first existing file.
    best_path: Optional[Path] = None
    best_asof = ""
    for p in IN_FILES:
        if not p.exists():
            continue
        asof = _input_asof_ymd(p)
        if (best_path is None) or (asof > best_asof):
            best_path = p
            best_asof = asof
    return best_path


def _max_date8(df: pd.DataFrame) -> str:
    if "date_yyyymmdd" in df.columns:
        s = df["date_yyyymmdd"].astype(str)
    elif "date" in df.columns:
        s = df["date"].astype(str)
    elif "signal_date" in df.columns:
        s = df["signal_date"].astype(str)
    else:
        return datetime.now(KST).strftime("%Y%m%d")
    d8 = s.map(_norm_date8)
    d8 = d8[d8.str.len() == 8]
    return str(d8.max()) if len(d8) else datetime.now(KST).strftime("%Y%m%d")


def _strip_html(s: str) -> str:
    return (
        re.sub(r"<[^>]+>", "", str(s or ""))
        .replace("&quot;", '"')
        .replace("&apos;", "'")
        .replace("&amp;", "&")
    )


def _fetch_naver_news(query: str, client_id: str, client_secret: str, display: int = 20) -> Dict[str, object]:
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
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def _pubdate_to_date8(v: object) -> str:
    try:
        dt = parsedate_to_datetime(str(v or ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST).strftime("%Y%m%d")
    except Exception:
        return ""


def _score_items(items: List[Dict[str, object]], asof_ymd: str, lookback_days: int = 3) -> Tuple[float, int, int, int]:
    asof = datetime.strptime(asof_ymd, "%Y%m%d").date()
    dmin = asof - timedelta(days=max(0, int(lookback_days)))

    pos = 0
    neg = 0
    used = 0
    for it in items:
        d8 = _pubdate_to_date8(it.get("pubDate"))
        if d8:
            try:
                d = datetime.strptime(d8, "%Y%m%d").date()
                if d < dmin or d > asof:
                    continue
            except Exception:
                pass

        txt = (_strip_html(it.get("title")) + " " + _strip_html(it.get("description"))).strip()
        if not txt:
            continue

        used += 1
        for kw in POS_KW:
            if kw in txt:
                pos += 1
        for kw in NEG_KW:
            if kw in txt:
                neg += 1

    denom = float(pos + neg)
    if denom <= 0:
        return 0.0, used, pos, neg

    score = (float(pos) - float(neg)) / denom
    score = max(-1.0, min(1.0, score))
    return float(round(score, 6)), used, pos, neg


def _table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    rows = con.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [str(r[1]) for r in rows]


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
            UNIQUE(date8, code, source)
        )
        """
    )
    con.commit()


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
                PRIMARY KEY(date8, code)
            )
            """
        )
        for r in rows:
            con.execute(
                """
                INSERT INTO signals_naver_daily(date8, code, name, news_score, source, headline_count, pos_hits, neg_hits, query, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(date8, code) DO UPDATE SET
                    name=excluded.name,
                    news_score=excluded.news_score,
                    source=excluded.source,
                    headline_count=excluded.headline_count,
                    pos_hits=excluded.pos_hits,
                    neg_hits=excluded.neg_hits,
                    query=excluded.query,
                    updated_at=excluded.updated_at
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
                    datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        con.commit()
        return "signals_naver_daily", int(len(rows))

    name_col = _pick_col(cols, ["name", "stock_name", "corp_name"])
    source_col = _pick_col(cols, ["source", "provider"])
    cnt_col = _pick_col(cols, ["headline_count", "news_count", "count"])
    pos_col = _pick_col(cols, ["pos_hits", "pos_count", "positive_hits"])
    neg_col = _pick_col(cols, ["neg_hits", "neg_count", "negative_hits"])
    query_col = _pick_col(cols, ["query", "keyword"])
    up_col = _pick_col(cols, ["updated_at", "updated", "ts", "created_at"])

    now_s = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
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
            vals[up_col] = now_s

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



def _finalize_status(status: Dict[str, object]) -> Dict[str, object]:
    symbols = int(status.get("symbols") or 0)
    fetched = int(status.get("fetched") or 0)
    saved = int(status.get("saved") or 0)
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
    elif reason in {"ok", "no_rows_saved"} and save_rate >= 0.50:
        quality = "WARN"
    else:
        quality = "FAIL"

    status["fetch_rate"] = round(fetch_rate, 6)
    status["save_rate"] = round(save_rate, 6)
    status["error_count"] = err_cnt
    status["errors_preview"] = [str(x)[:120] for x in errors[:10]]
    status["quality"] = quality
    return status

def _write_status(status: Dict[str, object]) -> None:
    asof = str(status.get("asof_ymd") or datetime.now(KST).strftime("%Y%m%d"))
    p1 = LOGS / f"news_collect_status_{asof}.json"
    p2 = LOGS / "news_collect_status_latest.json"
    text = json.dumps(status, ensure_ascii=False, indent=2)
    p1.write_text(text, encoding="utf-8")
    p2.write_text(text, encoding="utf-8")


def main() -> int:
    cid, csec, cred_src = _load_credentials()

    input_path = _pick_input()
    input_asof_ymd = _input_asof_ymd(input_path) if input_path else ""
    status: Dict[str, object] = {
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "input": str(input_path) if input_path else None,
        "input_asof_ymd": input_asof_ymd or None,
        "db": str(DB),
        "enabled": True,
        "reason": "",
        "asof_ymd": None,
        "symbols": 0,
        "fetched": 0,
        "saved": 0,
        "table": None,
        "credential_source": cred_src,
        "errors": [],
    }

    if not input_path or (not input_path.exists()):
        status["reason"] = "input_missing"
        _finalize_status(status)
        _write_status(status)
        print("[NEWS_COLLECT] input missing -> skip")
        return 0

    print(f"[NEWS_COLLECT] input={input_path.name} input_asof={input_asof_ymd or 'NA'}")

    if not cid or not csec:
        status["reason"] = "missing_naver_api_keys"
        _finalize_status(status)
        _write_status(status)
        print("[NEWS_COLLECT] missing NAVER_CLIENT_ID/SECRET -> skip")
        return 0

    df = _read_csv(input_path)
    if "code" not in df.columns:
        status["reason"] = "input_no_code"
        _finalize_status(status)
        _write_status(status)
        print("[NEWS_COLLECT] input has no code column -> skip")
        return 0

    asof_ymd = _max_date8(df)
    status["asof_ymd"] = asof_ymd

    if "name" not in df.columns:
        df["name"] = ""

    df["code"] = df["code"].map(_norm_code6)
    symbols = (
        df[["code", "name"]]
        .drop_duplicates(subset=["code"])
        .assign(name=lambda x: x["name"].astype(str).str.strip())
    )
    symbols = symbols[symbols["code"] != ""]
    status["symbols"] = int(len(symbols))

    rows: List[Dict[str, object]] = []
    for _, r in symbols.iterrows():
        code = str(r.get("code", ""))
        name = str(r.get("name", "")).strip()
        if not code:
            continue

        query = (name + " \uc8fc\uc2dd") if name else (code + " \uc8fc\uc2dd")
        try:
            payload = _fetch_naver_news(query, cid, csec, display=20)
            items = payload.get("items", []) if isinstance(payload, dict) else []
            score, used, pos, neg = _score_items(items if isinstance(items, list) else [], asof_ymd)
            rows.append(
                {
                    "code": code,
                    "name": name,
                    "query": query,
                    "news_score": float(score),
                    "headline_count": int(used),
                    "pos_hits": int(pos),
                    "neg_hits": int(neg),
                }
            )
            status["fetched"] = int(status.get("fetched", 0)) + 1
        except Exception as e:
            status["errors"].append(f"{code}:{type(e).__name__}")

    DB.parent.mkdir(parents=True, exist_ok=True)
    try:
        con = sqlite3.connect(str(DB))
        table, saved = _upsert_signals(con, asof_ymd, rows)
        con.close()
        status["table"] = table
        status["saved"] = int(saved)
        status["reason"] = "ok" if saved > 0 else "no_rows_saved"
    except Exception as e:
        status["reason"] = f"db_write_fail:{type(e).__name__}"
        status["errors"].append(str(e)[:200])

    _finalize_status(status)
    _write_status(status)
    print(
        f"[NEWS_COLLECT] asof={asof_ymd} symbols={status['symbols']} "
        f"fetched={status['fetched']} saved={status['saved']} "
        f"reason={status['reason']} cred={status['credential_source']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())