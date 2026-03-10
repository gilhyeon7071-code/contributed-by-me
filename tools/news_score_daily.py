from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
NEWS_DB = ROOT / "news_trading" / "data" / "trading.db"

IN_SECTOR = LOGS / "candidates_latest_data.with_sector_score.csv"
IN_FILTERED = LOGS / "candidates_latest_data.filtered.csv"
IN_BASE = LOGS / "candidates_latest_data.csv"
OUT = LOGS / "candidates_latest_data.with_news_score.csv"


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _norm_code6(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6) if s else ""


def _norm_date8(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""



def _ymd_lag_days(base_ymd: str, target_ymd: str) -> Optional[int]:
    b = _norm_date8(base_ymd)
    t = _norm_date8(target_ymd)
    if len(b) != 8 or len(t) != 8:
        return None
    try:
        bd = datetime.strptime(b, "%Y%m%d").date()
        td = datetime.strptime(t, "%Y%m%d").date()
        return int((bd - td).days)
    except Exception:
        return None

def _max_date8(df: pd.DataFrame) -> str:
    if "date_yyyymmdd" in df.columns:
        s = df["date_yyyymmdd"].astype(str)
    elif "date" in df.columns:
        s = df["date"].astype(str)
    elif "signal_date" in df.columns:
        s = df["signal_date"].astype(str)
    else:
        return datetime.now().strftime("%Y%m%d")
    d8 = s.map(_norm_date8)
    d8 = d8[d8.str.len() == 8]
    return str(d8.max()) if len(d8) else datetime.now().strftime("%Y%m%d")


def _table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    q = f'PRAGMA table_info("{table}")'
    rows = con.execute(q).fetchall()
    return [str(r[1]) for r in rows]


def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    low = {c.lower(): c for c in cols}
    for k in candidates:
        if k.lower() in low:
            return low[k.lower()]
    return None


def _load_news_map(asof_ymd: str, max_lag_days: int = 2) -> Tuple[Dict[str, float], Dict[str, object]]:
    meta: Dict[str, object] = {
        "db_path": str(NEWS_DB),
        "table": None,
        "reason": "",
        "rows_raw": 0,
        "rows_used": 0,
        "used_date8": None,
        "used_lag_days": None,
        "max_lag_days": int(max_lag_days),
    }
    if not NEWS_DB.exists():
        meta["reason"] = "db_missing"
        return {}, meta

    try:
        con = sqlite3.connect(str(NEWS_DB))
    except Exception as e:
        meta["reason"] = f"db_open_fail:{type(e).__name__}"
        return {}, meta

    try:
        tbl_rows = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        tables = [str(r[0]) for r in tbl_rows]

        table = None
        code_col = None
        date_col = None
        score_col = None
        checked: Dict[str, List[str]] = {}
        for cand_table in ["signals", "signals_naver_daily"]:
            if cand_table not in tables:
                continue
            cols = _table_columns(con, cand_table)
            checked[cand_table] = cols
            c_code = _pick_col(cols, ["code", "ticker", "symbol"])
            c_date = _pick_col(cols, ["date8", "date", "ymd", "trade_date", "signal_date", "created_at", "ts"])
            c_score = _pick_col(cols, ["news_score", "score", "sentiment", "signal_strength", "strength"])
            if c_code and c_date and c_score:
                table = cand_table
                code_col = c_code
                date_col = c_date
                score_col = c_score
                break

        if not table:
            meta["reason"] = "signals_table_missing_or_unsupported"
            meta["tables_checked"] = checked
            return {}, meta
        meta["table"] = table

        q = f'SELECT "{code_col}" AS code, "{date_col}" AS d, "{score_col}" AS score FROM "{table}"'
        sdf = pd.read_sql_query(q, con)
        meta["rows_raw"] = int(len(sdf))
        if sdf.empty:
            meta["reason"] = "signals_empty"
            return {}, meta

        sdf["code"] = sdf["code"].map(_norm_code6)
        sdf["date8"] = sdf["d"].map(_norm_date8)
        sdf["score"] = pd.to_numeric(sdf["score"], errors="coerce")

        sdf = sdf[(sdf["code"] != "") & (sdf["date8"] != "")]
        sdf = sdf.dropna(subset=["score"])
        if asof_ymd:
            sdf = sdf[sdf["date8"] <= str(asof_ymd)]
        if sdf.empty:
            meta["reason"] = "signals_no_rows_upto_asof"
            return {}, meta

        used_date8 = str(sdf["date8"].max() or "")
        meta["used_date8"] = used_date8
        lag_days = _ymd_lag_days(asof_ymd, used_date8)
        meta["used_lag_days"] = lag_days
        if lag_days is not None and lag_days > int(max_lag_days):
            meta["reason"] = f"signals_stale_lag_{lag_days}d"
            return {}, meta

        sdf = sdf.sort_values(["code", "date8"]).drop_duplicates(["code"], keep="last")

        mx = float(sdf["score"].abs().max()) if len(sdf) else 0.0
        if mx > 1.0:
            sdf["score"] = sdf["score"] / mx
        sdf["score"] = sdf["score"].clip(lower=-1.0, upper=1.0)

        out = {str(r["code"]): float(r["score"]) for _, r in sdf.iterrows()}
        meta["rows_used"] = int(len(out))
        meta["reason"] = "ok" if out else "signals_zero_after_filter"
        return out, meta
    except Exception as e:
        meta["reason"] = f"signals_query_fail:{type(e).__name__}"
        return {}, meta
    finally:
        try:
            con.close()
        except Exception:
            pass


def _pick_input() -> Path:
    if IN_SECTOR.exists():
        return IN_SECTOR
    if IN_FILTERED.exists():
        return IN_FILTERED
    return IN_BASE


def main() -> int:
    in_path = _pick_input()
    if not in_path.exists():
        raise SystemExit(f"[FATAL] missing input candidates file: {in_path}")

    df = _read_csv(in_path)
    if "code" not in df.columns:
        raise SystemExit(f"[FATAL] missing code column: {in_path}")

    df["code"] = df["code"].map(_norm_code6)
    asof_ymd = _max_date8(df)

    max_lag_days = int(str(os.getenv("NEWS_MAX_LAG_DAYS", "2")).strip() or "2")
    max_lag_days = max(0, min(max_lag_days, 30))
    news_map, meta = _load_news_map(asof_ymd, max_lag_days=max_lag_days)

    df["news_score"] = df["code"].map(lambda c: float(news_map.get(str(c), 0.0)))
    df["news_sentiment"] = df["news_score"]
    df["news_source"] = df["code"].map(lambda c: "DB_ASOF" if str(c) in news_map else "FAIL_SOFT")

    _write_csv(OUT, df)

    mapped = int((df["news_source"] == "DB_ASOF").sum())
    rows_n = max(1, int(len(df)))
    nonzero = int((pd.to_numeric(df["news_score"], errors="coerce").fillna(0.0) != 0).sum())
    mapped_rate = float(mapped) / float(rows_n)
    nonzero_rate = float(nonzero) / float(rows_n)
    reason = str(meta.get("reason") or "")
    if reason == "ok" and mapped_rate >= 0.80 and nonzero_rate >= 0.20:
        quality = "PASS"
    elif reason in {"ok", "signals_zero_after_filter"} and mapped_rate >= 0.50:
        quality = "WARN"
    else:
        quality = "FAIL"

    status = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input": str(in_path),
        "output": str(OUT),
        "asof_ymd": asof_ymd,
        "rows": int(len(df)),
        "mapped_rows": mapped,
        "mapped_rate": round(mapped_rate, 6),
        "nonzero_rows": nonzero,
        "nonzero_rate": round(nonzero_rate, 6),
        "quality": quality,
        "max_lag_days": max_lag_days,
        "meta": meta,
    }

    st = LOGS / f"news_score_status_{asof_ymd}.json"
    st_latest = LOGS / "news_score_status_latest.json"
    st.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    st_latest.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        f"[NEWS_SCORE] input={in_path.name} rows={len(df)} mapped={mapped} "
        f"mapped_rate={mapped_rate:.2%} nonzero_rate={nonzero_rate:.2%} "
        f"quality={quality} reason={meta.get('reason')}"
    )
    print(f"[NEWS_SCORE] wrote {OUT}")
    print(f"[NEWS_SCORE] status={st}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())