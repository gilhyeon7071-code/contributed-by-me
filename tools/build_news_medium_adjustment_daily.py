"""
뉴스 medium horizon 판단을 final_score 소폭 조정값으로 변환.

DB(news_articles_naver)에서 최근 lookback일치 medium horizon 기사를 읽어
코드별 medium_news_adjustment 계산 후 CSV 출력.

  penalize (confidence >= 0.60) -> -0.06
  penalize (confidence >= 0.75) -> -0.08
  boost    (confidence >= 0.60) -> +0.03
  boost    (confidence >= 0.75) -> +0.05
  avoid_chase                   -> -0.03
  충돌(boost + penalize)        -> penalize 우선

출력:
  2_Logs/news_medium_adjustment_latest.csv
  2_Logs/news_medium_adjustment_status_latest.json
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

KST = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
DB = ROOT / "news_trading" / "data" / "trading.db"
OUT_CSV = LOGS / "news_medium_adjustment_latest.csv"
OUT_STATUS = LOGS / "news_medium_adjustment_status_latest.json"

DEFAULT_LOOKBACK_DAYS = int(os.getenv("NEWS_MEDIUM_ADJ_LOOKBACK_DAYS", "14") or "14")
DEFAULT_MIN_CONF = float(os.getenv("NEWS_MEDIUM_ADJ_MIN_CONF", "0.60") or "0.60")
HIGH_CONF = float(os.getenv("NEWS_MEDIUM_ADJ_HIGH_CONF", "0.75") or "0.75")

ADJ_PENALIZE_NORMAL = -0.06
ADJ_PENALIZE_HIGH   = -0.08
ADJ_BOOST_NORMAL    = +0.03
ADJ_BOOST_HIGH      = +0.05
ADJ_AVOID_CHASE     = -0.03


def _now_kst() -> datetime:
    return datetime.now(KST)


def _norm_code6(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s.zfill(6) if s else ""


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text(
            "code,medium_news_adjustment,action,max_confidence,n_articles,asof_ymd\n",
            encoding="utf-8-sig",
        )
        return
    header = list(rows[0].keys())
    lines = [",".join(header)]
    for r in rows:
        lines.append(",".join(str(r.get(k, "")) for k in header))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def main() -> int:
    now = _now_kst()
    asof_ymd = now.strftime("%Y%m%d")
    cutoff_date = (now - timedelta(days=DEFAULT_LOOKBACK_DAYS)).strftime("%Y%m%d")

    status: Dict[str, Any] = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "asof_ymd": asof_ymd,
        "cutoff_date": cutoff_date,
        "lookback_days": DEFAULT_LOOKBACK_DAYS,
        "min_confidence": DEFAULT_MIN_CONF,
        "high_confidence": HIGH_CONF,
        "db": str(DB),
        "output_csv": str(OUT_CSV),
        "reason": "",
        "db_rows_read": 0,
        "codes_with_adjustment": 0,
        "penalize_codes": 0,
        "boost_codes": 0,
        "avoid_chase_codes": 0,
        "adj_range": [0.0, 0.0],
    }

    if not DB.exists():
        status["reason"] = "db_missing"
        _write_json(OUT_STATUS, status)
        _write_csv(OUT_CSV, [])
        print(f"[MEDIUM_ADJ] db missing -> wrote empty csv")
        return 0

    try:
        con = sqlite3.connect(str(DB), timeout=30)
        con.execute("PRAGMA journal_mode=WAL")
        db_rows = con.execute(
            """
            SELECT code, implication_action, implication_confidence
            FROM news_articles_naver
            WHERE implication_horizon = 'medium'
              AND date8 >= ?
              AND implication_action IN ('boost', 'penalize', 'avoid_chase')
              AND implication_confidence IS NOT NULL
              AND CAST(implication_confidence AS REAL) >= ?
            """,
            (cutoff_date, DEFAULT_MIN_CONF),
        ).fetchall()
        con.close()
    except Exception as e:
        status["reason"] = f"db_read_failed:{type(e).__name__}:{str(e)[:80]}"
        _write_json(OUT_STATUS, status)
        _write_csv(OUT_CSV, [])
        print(f"[MEDIUM_ADJ] db read failed: {e}")
        return 0

    status["db_rows_read"] = len(db_rows)

    # 코드별 집계: 최대 confidence, action 집합
    from collections import defaultdict
    code_data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "actions": set(), "max_conf": 0.0, "n": 0
    })
    for raw_code, action, raw_conf in db_rows:
        code = _norm_code6(raw_code)
        if not code:
            continue
        try:
            conf = float(raw_conf or 0.0)
        except Exception:
            conf = 0.0
        code_data[code]["actions"].add(str(action or "").strip())
        code_data[code]["max_conf"] = max(code_data[code]["max_conf"], conf)
        code_data[code]["n"] += 1

    out_rows: List[Dict[str, Any]] = []
    penalize_cnt = boost_cnt = avoid_cnt = 0

    for code, d in code_data.items():
        actions = d["actions"]
        conf = d["max_conf"]
        n = d["n"]
        high = conf >= HIGH_CONF

        # penalize 우선 (리스크 관리 원칙)
        if "penalize" in actions:
            adj = ADJ_PENALIZE_HIGH if high else ADJ_PENALIZE_NORMAL
            dominant = "penalize"
            penalize_cnt += 1
        elif "avoid_chase" in actions:
            adj = ADJ_AVOID_CHASE
            dominant = "avoid_chase"
            avoid_cnt += 1
        elif "boost" in actions:
            adj = ADJ_BOOST_HIGH if high else ADJ_BOOST_NORMAL
            dominant = "boost"
            boost_cnt += 1
        else:
            continue

        out_rows.append({
            "code": code,
            "medium_news_adjustment": round(adj, 4),
            "action": dominant,
            "max_confidence": round(conf, 4),
            "n_articles": n,
            "asof_ymd": asof_ymd,
        })

    adj_vals = [r["medium_news_adjustment"] for r in out_rows]
    status.update({
        "reason": "ok",
        "codes_with_adjustment": len(out_rows),
        "penalize_codes": penalize_cnt,
        "boost_codes": boost_cnt,
        "avoid_chase_codes": avoid_cnt,
        "adj_range": [min(adj_vals, default=0.0), max(adj_vals, default=0.0)],
    })

    _write_csv(OUT_CSV, out_rows)
    _write_json(OUT_STATUS, status)
    print(
        f"[MEDIUM_ADJ] ok codes={len(out_rows)} "
        f"penalize={penalize_cnt} boost={boost_cnt} avoid={avoid_cnt} "
        f"lookback={DEFAULT_LOOKBACK_DAYS}d cutoff={cutoff_date}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
