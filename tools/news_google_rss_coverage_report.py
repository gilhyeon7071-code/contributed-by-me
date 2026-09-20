from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
NEWS_DB = ROOT / "news_trading" / "data" / "trading.db"

IN_FILES = [
    LOGS / "candidates_latest_data.with_final_score.csv",
    LOGS / "candidates_latest_data.with_news_score.csv",
    LOGS / "candidates_latest_data.with_sector_score.csv",
    LOGS / "candidates_latest_data.filtered.csv",
    LOGS / "candidates_latest_data.csv",
]

KST = timezone(timedelta(hours=9))


def _now_kst() -> datetime:
    return datetime.now(KST)


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


def _pick_input() -> Optional[Path]:
    for path in IN_FILES:
        if path.exists():
            return path
    return None


def _max_date8(df: pd.DataFrame) -> str:
    vals: List[str] = []
    for col in ("date_yyyymmdd", "date", "signal_date", "ymd"):
        if col in df.columns:
            vals.extend([_norm_date8(v) for v in df[col].tolist()])
    vals = [v for v in vals if len(v) == 8]
    return max(vals) if vals else _now_kst().strftime("%Y%m%d")


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _load_counts(con: sqlite3.Connection, table: str, reference_ymd: str) -> Dict[str, int]:
    if not _table_exists(con, table):
        return {}
    cols = [str(r[1]) for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    if "code" not in cols:
        return {}
    date_col = "date8" if "date8" in cols else ("data_dt" if "data_dt" in cols else "")
    if not date_col:
        return {}
    df = pd.read_sql_query(
        f'SELECT code, {date_col} AS date8, COUNT(*) AS article_count FROM "{table}" WHERE {date_col} <= ? GROUP BY code, {date_col}',
        con,
        params=[reference_ymd],
    )
    if df.empty:
        return {}
    df["code"] = df["code"].map(_norm_code6)
    df["date8"] = df["date8"].map(_norm_date8)
    df["article_count"] = pd.to_numeric(df["article_count"], errors="coerce").fillna(0).astype(int)
    df = df[(df["code"] != "") & (df["date8"] != "")]
    if df.empty:
        return {}
    latest = df.sort_values(["code", "date8"]).drop_duplicates("code", keep="last")
    return {str(r["code"]): int(r["article_count"]) for _, r in latest.iterrows()}


def main() -> int:
    now = _now_kst()
    in_path = _pick_input()
    status: Dict[str, object] = {
        "generated_at": now.isoformat(timespec="seconds"),
        "mode": "read_only_coverage_compare",
        "db_write": False,
        "input": str(in_path) if in_path else None,
        "db": str(NEWS_DB),
        "quality": "FAIL",
        "reason": "",
        "rows": 0,
        "reference_ymd": None,
    }

    if not in_path or not in_path.exists():
        status["reason"] = "input_missing"
    elif not NEWS_DB.exists():
        status["reason"] = "db_missing"
    else:
        candidates = _read_csv(in_path)
        if "code" not in candidates.columns:
            status["reason"] = "input_no_code"
        else:
            if "name" not in candidates.columns:
                candidates["name"] = ""
            candidates["code"] = candidates["code"].map(_norm_code6)
            candidates["name"] = candidates["name"].astype(str)
            candidates = candidates[candidates["code"] != ""].drop_duplicates("code", keep="first").reset_index(drop=True)
            reference_ymd = _max_date8(candidates)
            status["reference_ymd"] = reference_ymd

            con: Optional[sqlite3.Connection] = None
            try:
                con = sqlite3.connect(f"file:{NEWS_DB.as_posix()}?mode=ro", uri=True, timeout=10.0)
                con.execute("PRAGMA busy_timeout=10000")
                naver_counts = _load_counts(con, "news_articles_naver", reference_ymd)
                google_counts = _load_counts(con, "news_articles_google_rss", reference_ymd)
                kis_counts = _load_counts(con, "news_articles_kis_title", reference_ymd)
                table_counts = {}
                for table in ("news_articles_naver", "news_articles_google_rss", "news_articles_kis_title"):
                    table_counts[table] = (
                        int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                        if _table_exists(con, table)
                        else 0
                    )
            except sqlite3.OperationalError as exc:
                msg = str(exc)
                status["reason"] = "db_locked" if "locked" in msg.lower() else "db_operational_error"
                status["db_error"] = msg
                status["db_timeout_ms"] = 10000
            else:
                rows = []
                for _, row in candidates.iterrows():
                    code = str(row.get("code") or "")
                    naver_n = int(naver_counts.get(code, 0))
                    google_n = int(google_counts.get(code, 0))
                    kis_n = int(kis_counts.get(code, 0))
                    rows.append(
                        {
                            "code": code,
                            "name": str(row.get("name") or ""),
                            "naver_article_count": naver_n,
                            "google_rss_article_count": google_n,
                            "kis_title_count": kis_n,
                            "naver_covered": naver_n > 0,
                            "google_rss_covered": google_n > 0,
                            "kis_title_covered": kis_n > 0,
                            "google_only": google_n > 0 and naver_n <= 0,
                            "kis_only": kis_n > 0 and naver_n <= 0 and google_n <= 0,
                            "all_three_covered": naver_n > 0 and google_n > 0 and kis_n > 0,
                            "any_covered": naver_n > 0 or google_n > 0 or kis_n > 0,
                        }
                    )

                out_df = pd.DataFrame(rows)
                row_count = max(1, int(len(out_df)))
                naver_covered = int(out_df["naver_covered"].sum()) if len(out_df) else 0
                google_covered = int(out_df["google_rss_covered"].sum()) if len(out_df) else 0
                kis_covered = int(out_df["kis_title_covered"].sum()) if len(out_df) else 0
                google_only = int(out_df["google_only"].sum()) if len(out_df) else 0
                kis_only = int(out_df["kis_only"].sum()) if len(out_df) else 0
                all_three_covered = int(out_df["all_three_covered"].sum()) if len(out_df) else 0
                any_covered = int(out_df["any_covered"].sum()) if len(out_df) else 0

                LOGS.mkdir(parents=True, exist_ok=True)
                csv_path = LOGS / "google_news_rss_coverage_report_latest.csv"
                out_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

                status.update(
                    {
                        "reason": "ok",
                        "quality": "PASS",
                        "rows": int(len(out_df)),
                        "reference_ymd": reference_ymd,
                        "output_csv": str(csv_path),
                        "table_counts": table_counts,
                        "coverage": {
                            "naver_covered": naver_covered,
                            "naver_coverage_rate": round(float(naver_covered) / float(row_count), 6),
                            "google_rss_covered": google_covered,
                            "google_rss_coverage_rate": round(float(google_covered) / float(row_count), 6),
                            "kis_title_covered": kis_covered,
                            "kis_title_coverage_rate": round(float(kis_covered) / float(row_count), 6),
                            "google_only": google_only,
                            "kis_only": kis_only,
                            "all_three_covered": all_three_covered,
                            "any_covered": any_covered,
                            "any_coverage_rate": round(float(any_covered) / float(row_count), 6),
                        },
                    }
                )
            finally:
                if con is not None:
                    con.close()

    latest = LOGS / "google_news_rss_coverage_report_latest.json"
    dated = LOGS / f"google_news_rss_coverage_report_{now.strftime('%Y%m%d')}.json"
    text = json.dumps(status, ensure_ascii=False, indent=2)
    latest.write_text(text, encoding="utf-8")
    dated.write_text(text, encoding="utf-8")
    print(
        f"[GOOGLE_NEWS_RSS_COVERAGE] quality={status['quality']} reason={status['reason']} "
        f"rows={status.get('rows')} latest={latest}"
    )
    return 0 if status["quality"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
