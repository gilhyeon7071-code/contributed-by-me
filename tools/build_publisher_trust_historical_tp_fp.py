"""Build read-only publisher historical TP/FP evidence for news trust.

This report evaluates non-zero article/news implication scores against next
available daily close direction. It is observe-only and has no trading effect.
"""

from __future__ import annotations

import csv
import json
import sqlite3
from bisect import bisect_left
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
NEWS_DB = ROOT / "news_trading" / "data" / "trading.db"
PRICE_DIR = ROOT / "krx_daily_archive"
OUT_CSV = LOGS / "publisher_trust_historical_tp_fp_latest.csv"
OUT_JSON = LOGS / "publisher_trust_historical_tp_fp_latest.json"
KST = timezone(timedelta(hours=9))


def _now_kst() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _norm_code(v: object) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s.zfill(6) if s else ""


def _norm_date(v: object) -> str:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    return s[:8] if len(s) >= 8 else ""


def _to_float(v: object, default: float = 0.0) -> float:
    try:
        if v is None or str(v).strip() == "":
            return default
        return float(v)
    except Exception:
        return default


def _load_news_rows() -> pd.DataFrame:
    con = sqlite3.connect(str(NEWS_DB))
    try:
        q = """
            SELECT code, date8, source, article_score, implication_score, title
            FROM news_articles_naver
            WHERE code IS NOT NULL AND date8 IS NOT NULL
        """
        df = pd.read_sql_query(q, con)
    finally:
        con.close()
    if df.empty:
        return df
    df["code"] = df["code"].map(_norm_code)
    df["date8"] = df["date8"].map(_norm_date)
    df["article_score"] = pd.to_numeric(df["article_score"], errors="coerce")
    df["implication_score"] = pd.to_numeric(df["implication_score"], errors="coerce")
    df["score"] = df["implication_score"].where(df["implication_score"].notna(), df["article_score"])
    df["source"] = df["source"].astype(str).replace({"": "UNKNOWN"})
    return df[(df["code"] != "") & (df["date8"] != "")].dropna(subset=["score"])


def _load_price_rows() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in sorted(PRICE_DIR.glob("krx_daily_*_clean.parquet")):
        if ".bak_" in path.name:
            continue
        try:
            frames.append(pd.read_parquet(path))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        return df
    df["code"] = df["code"].map(_norm_code)
    df["date8"] = df["date"].map(_norm_date)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df[(df["code"] != "") & (df["date8"] != "") & (df["close"] > 0)].copy()
    return df.sort_values(["code", "date8"])


def _price_lookup(prices: pd.DataFrame) -> Dict[str, Tuple[List[str], List[float]]]:
    lookup: Dict[str, Tuple[List[str], List[float]]] = {}
    for code, g in prices.groupby("code", sort=False):
        ordered = g.sort_values("date8")
        lookup[str(code)] = (
            ordered["date8"].astype(str).tolist(),
            pd.to_numeric(ordered["close"], errors="coerce").fillna(0.0).astype(float).tolist(),
        )
    return lookup


def _forward_return(code_prices: Tuple[List[str], List[float]] | None, date8: str) -> Dict[str, Any]:
    if not code_prices:
        return {"status": "NO_PRICE_ROWS"}
    dates, closes = code_prices
    base_idx = bisect_left(dates, date8)
    if base_idx >= len(dates) or base_idx + 1 >= len(dates):
        return {"status": "NO_FORWARD_PRICE"}
    base_close = _to_float(closes[base_idx], 0.0)
    target_close = _to_float(closes[base_idx + 1], 0.0)
    if base_close <= 0 or target_close <= 0:
        return {"status": "BAD_CLOSE"}
    return {
        "status": "EVALUATED",
        "base_date8": str(dates[base_idx]),
        "target_date8": str(dates[base_idx + 1]),
        "base_close": base_close,
        "target_close": target_close,
        "forward_return": (target_close / base_close) - 1.0,
    }


def build_report() -> Dict[str, Any]:
    LOGS.mkdir(parents=True, exist_ok=True)
    if not NEWS_DB.exists():
        return {"status": "FAIL", "reason": "news_db_missing", "db": str(NEWS_DB)}
    if not PRICE_DIR.exists():
        return {"status": "FAIL", "reason": "price_dir_missing", "price_dir": str(PRICE_DIR)}

    news = _load_news_rows()
    prices = _load_price_rows()
    by_code = _price_lookup(prices)
    rows: List[Dict[str, Any]] = []
    for _, r in news.iterrows():
        score = _to_float(r.get("score"), 0.0)
        if abs(score) < 0.10:
            continue
        code = str(r.get("code") or "")
        ret = _forward_return(by_code.get(code), str(r.get("date8") or ""))
        out = {
            "code": code,
            "date8": str(r.get("date8") or ""),
            "publisher_source": str(r.get("source") or "UNKNOWN"),
            "score": round(score, 6),
            "signal_direction": "positive" if score > 0 else "negative",
            "title": str(r.get("title") or "")[:200],
            **ret,
        }
        if ret.get("status") == "EVALUATED":
            fwd = _to_float(ret.get("forward_return"), 0.0)
            actual = "positive" if fwd > 0 else "negative_or_flat"
            out["actual_direction"] = actual
            out["tp_fp"] = "TP" if ((score > 0 and fwd > 0) or (score < 0 and fwd <= 0)) else "FP"
        else:
            out["actual_direction"] = ""
            out["tp_fp"] = "UNEVALUATED"
        rows.append(out)

    fieldnames = [
        "code",
        "date8",
        "publisher_source",
        "score",
        "signal_direction",
        "status",
        "base_date8",
        "target_date8",
        "base_close",
        "target_close",
        "forward_return",
        "actual_direction",
        "tp_fp",
        "title",
    ]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    evaluated = [r for r in rows if r.get("tp_fp") in {"TP", "FP"}]
    by_source: Dict[str, Dict[str, Any]] = {}
    for source in sorted({str(r.get("publisher_source") or "UNKNOWN") for r in evaluated}):
        g = [r for r in evaluated if str(r.get("publisher_source") or "UNKNOWN") == source]
        tp = sum(1 for r in g if r.get("tp_fp") == "TP")
        fp = sum(1 for r in g if r.get("tp_fp") == "FP")
        total = tp + fp
        by_source[source] = {
            "evaluated_rows": int(total),
            "tp": int(tp),
            "fp": int(fp),
            "historical_tp_fp_score": round(float(tp) / float(total), 6) if total else 0.0,
        }

    status = {
        "generated_at": _now_kst(),
        "version": "publisher_trust_historical_tp_fp_v1",
        "status": "PASS" if rows else "WARN",
        "quality": "PASS" if evaluated else "WARN",
        "reason": "ok" if evaluated else "no_evaluated_rows",
        "db": str(NEWS_DB),
        "price_dir": str(PRICE_DIR),
        "output_csv": str(OUT_CSV),
        "rows": int(len(rows)),
        "evaluated_rows": int(len(evaluated)),
        "unevaluated_rows": int(len(rows) - len(evaluated)),
        "by_source": by_source,
        "score_effect": False,
        "trading_effect": False,
        "policy_effect": False,
        "auto_emit_enabled": False,
    }
    OUT_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    return status


def main() -> int:
    status = build_report()
    print(json.dumps({"status": status.get("status"), "evaluated_rows": status.get("evaluated_rows"), "by_source": status.get("by_source")}, ensure_ascii=False))
    return 0 if status.get("status") in {"PASS", "WARN"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
