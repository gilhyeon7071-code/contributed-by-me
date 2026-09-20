from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
DB = ROOT / "news_trading" / "data" / "trading.db"
KST = timezone(timedelta(hours=9))
OPENAI_KEY_FILES = [
    ROOT / ".secrets" / "openai_api_key.txt",
    ROOT / "_secrets" / "openai_api_key.txt",
    ROOT / "_cache" / "openai_api_key.txt",
]
ANTHROPIC_KEY_FILES = [
    ROOT / ".secrets" / "anthropic_api_key.txt",
    ROOT / "_secrets" / "anthropic_api_key.txt",
    ROOT / "_cache" / "anthropic_api_key.txt",
]


def _now_kst() -> datetime:
    return datetime.now(KST)


def _norm_date8(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _api_key(provider: str) -> str:
    p = provider.lower().strip()
    if p == "anthropic":
        key = str(os.getenv("ANTHROPIC_API_KEY", "")).strip()
        files = ANTHROPIC_KEY_FILES
    else:
        key = str(os.getenv("OPENAI_API_KEY", "")).strip()
        files = OPENAI_KEY_FILES
    if key:
        return key
    for path in files:
        try:
            if path.exists():
                key = path.read_text(encoding="utf-8").replace("\ufeff", "").strip()
                if key:
                    return key
        except Exception:
            continue
    return ""


def _sqlite_timeout_sec() -> float:
    try:
        return max(5.0, float(os.getenv("NEWS_LLM_SQLITE_TIMEOUT_SEC", "60") or "60"))
    except Exception:
        return 60.0


def _sqlite_retry_count() -> int:
    try:
        return max(1, int(os.getenv("NEWS_LLM_SQLITE_RETRY", "4") or "4"))
    except Exception:
        return 4


def _is_locked_error(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).lower()


def _connect_db() -> sqlite3.Connection:
    timeout = _sqlite_timeout_sec()
    con = sqlite3.connect(str(DB), timeout=timeout)
    con.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    return con


def _with_db_retry(fn):
    retries = _sqlite_retry_count()
    last_exc: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            if not _is_locked_error(exc) or attempt >= retries:
                raise
            last_exc = exc
            time.sleep(min(5.0, 0.75 * attempt))
    if last_exc is not None:
        raise last_exc
    return None


def _ensure_columns(con: sqlite3.Connection) -> None:
    cols = {str(r[1]) for r in con.execute("PRAGMA table_info(news_articles_naver)").fetchall()}
    additions = {
        "llm_score": "REAL",
        "llm_reasoning": "TEXT",
        "llm_impact": "TEXT",
        "llm_scored_at": "TEXT",
        "llm_provider": "TEXT",
        "llm_model": "TEXT",
        "llm_error": "TEXT",
    }
    for col, typ in additions.items():
        if col not in cols:
            con.execute(f"ALTER TABLE news_articles_naver ADD COLUMN {col} {typ}")
    con.commit()


def _load_rows(con: sqlite3.Connection, as_of: str, lookback_days: int, limit: int, retry_errors: bool) -> List[Dict[str, Any]]:
    end_d = datetime.strptime(as_of, "%Y%m%d").date()
    start_d = end_d - timedelta(days=max(0, int(lookback_days)))
    params: List[Any] = [start_d.strftime("%Y%m%d"), as_of]
    where = "date8 >= ? AND date8 <= ?"
    if not retry_errors:
        where += " AND (llm_error IS NULL OR llm_error = '')"
    where += " AND llm_score IS NULL"
    q = (
        "SELECT article_key, code, name, date8, title, description, article_score "
        "FROM news_articles_naver "
        f"WHERE {where} "
        "ORDER BY date8 DESC, fetched_at DESC "
        "LIMIT ?"
    )
    params.append(max(1, int(limit)))
    cur = con.execute(q, params)
    return [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]


def _prompt(row: Dict[str, Any]) -> str:
    title = str(row.get("title") or "").strip()
    desc = str(row.get("description") or "").strip()
    name = str(row.get("name") or "").strip()
    code = str(row.get("code") or "").strip()
    return (
        "한국 주식 뉴스 제목과 스니펫만 보고 해당 종목 주가에 대한 단기 영향을 평가하라.\n"
        "반드시 JSON만 반환한다. 형식: "
        "{\"score\": -1.0~1.0, \"impact\": \"direct|indirect|neutral\", \"reasoning\": \"40자 이내 한국어\"}\n"
        "score 기준: 명확한 악재 -1, 약한 악재 -0.3, 중립 0, 약한 호재 0.3, 명확한 호재 1.\n"
        "주의: 이미 급등했다는 사실만 있으면 호재로 보지 말고, 투자주의/경고/희석/소송은 악재로 본다.\n\n"
        f"종목: {name}({code})\n"
        f"제목: {title}\n"
        f"스니펫: {desc}"
    )


def _parse_json_text(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if m:
        raw = m.group(0)
    obj = json.loads(raw)
    score = max(-1.0, min(1.0, float(obj.get("score", 0.0) or 0.0)))
    impact = str(obj.get("impact") or "neutral").strip().lower()
    if impact not in {"direct", "indirect", "neutral"}:
        impact = "neutral"
    reasoning = str(obj.get("reasoning") or "").strip()
    return {"score": score, "impact": impact, "reasoning": reasoning[:160]}


def _call_openai(api_key: str, model: str, prompt: str) -> Dict[str, Any]:
    max_tokens = max(300, int(os.getenv("NEWS_LLM_MAX_TOKENS", "500") or "500"))
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You classify Korean stock news sentiment. Return valid JSON only."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=40) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    text = payload["choices"][0]["message"]["content"]
    return _parse_json_text(text)


def _call_anthropic(api_key: str, model: str, prompt: str) -> Dict[str, Any]:
    max_tokens = max(300, int(os.getenv("NEWS_LLM_MAX_TOKENS", "500") or "500"))
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": 0,
        "system": "You classify Korean stock news sentiment. Return valid JSON only.",
        "messages": [{"role": "user", "content": prompt}],
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=40) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    parts = payload.get("content") or []
    text = "\n".join(str(p.get("text") or "") for p in parts if isinstance(p, dict))
    return _parse_json_text(text)


def _write_status(payload: Dict[str, Any], as_of: str, *, latest: bool = True, prefix: str = "news_llm_score_status") -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    (LOGS / f"{prefix}_{as_of}.json").write_text(text, encoding="utf-8")
    if latest:
        (LOGS / f"{prefix}_latest.json").write_text(text, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Optional LLM scoring for Naver news snippets")
    ap.add_argument("--as-of", default=_now_kst().strftime("%Y%m%d"))
    ap.add_argument("--provider", default=os.getenv("NEWS_LLM_PROVIDER", "openai"))
    ap.add_argument("--model", default=os.getenv("NEWS_LLM_MODEL", ""))
    ap.add_argument("--max-rows", type=int, default=int(os.getenv("NEWS_LLM_MAX_ROWS", "80")))
    ap.add_argument("--max-daily-rows", type=int, default=int(os.getenv("NEWS_LLM_MAX_DAILY_ROWS", "150")))
    ap.add_argument("--lookback-days", type=int, default=int(os.getenv("NEWS_LLM_LOOKBACK_DAYS", "3")))
    ap.add_argument("--sleep", type=float, default=float(os.getenv("NEWS_LLM_SLEEP", "0.05")))
    ap.add_argument("--retry-errors", action="store_true")
    args = ap.parse_args()

    as_of = _norm_date8(args.as_of) or _now_kst().strftime("%Y%m%d")
    provider = str(args.provider or "openai").strip().lower()
    model = str(args.model or "").strip()
    if provider == "anthropic" and not model:
        model = "claude-haiku-4-5"
    if provider != "anthropic" and not model:
        model = "gpt-4o-mini"

    status: Dict[str, Any] = {
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "asof_ymd": as_of,
        "db": str(DB),
        "table": "news_articles_naver",
        "provider": provider,
        "model": model,
        "enabled": str(os.getenv("NEWS_LLM_ENABLE", "")).strip().lower() in {"1", "true", "yes", "on"},
        "quality": "SKIP",
        "reason": "",
        "rows_targeted": 0,
        "rows_scored": 0,
        "error_count": 0,
        "errors": [],
    }

    con = _connect_db()
    try:
        _with_db_retry(lambda: _ensure_columns(con))
        if not status["enabled"]:
            status["reason"] = "NEWS_LLM_ENABLE_not_set"
            _write_status(status, as_of, latest=True, prefix="news_llm_score_status_disabled")
            print("[NEWS_LLM] skip: NEWS_LLM_ENABLE not set")
            return 0

        key = _api_key(provider)
        if not key:
            status["reason"] = f"{provider}_api_key_missing"
            _write_status(status, as_of)
            print(f"[NEWS_LLM] skip: {provider} API key missing")
            return 0

        already_scored_today: int = _with_db_retry(lambda: con.execute(
            "SELECT COUNT(*) FROM news_articles_naver WHERE date8=? AND llm_score IS NOT NULL",
            (as_of,),
        ).fetchone()[0])
        max_daily = max(0, int(args.max_daily_rows))
        remaining_quota = max(0, max_daily - already_scored_today)
        status["already_scored_today"] = int(already_scored_today)
        status["max_daily_rows"] = int(max_daily)
        status["remaining_quota"] = int(remaining_quota)
        if remaining_quota <= 0:
            status["quality"] = "SKIP"
            status["reason"] = f"daily_quota_exhausted:already_scored={already_scored_today}>={max_daily}"
            _write_status(status, as_of)
            print(f"[NEWS_LLM] skip: daily quota exhausted ({already_scored_today}/{max_daily})")
            return 0
        capped_max_rows = min(int(args.max_rows), remaining_quota)
        rows = _with_db_retry(
            lambda: _load_rows(con, as_of, int(args.lookback_days), capped_max_rows, bool(args.retry_errors))
        )
        status["rows_targeted"] = int(len(rows))
        scored = 0
        errors: List[str] = []
        abort_remaining = False
        for idx, row in enumerate(rows, start=1):
            article_key = str(row.get("article_key") or "")
            try:
                prompt = _prompt(row)
                if provider == "anthropic":
                    result = _call_anthropic(key, model, prompt)
                else:
                    result = _call_openai(key, model, prompt)
                con.execute(
                    """
                    UPDATE news_articles_naver
                    SET llm_score=?, llm_reasoning=?, llm_impact=?, llm_scored_at=?,
                        llm_provider=?, llm_model=?, llm_error=''
                    WHERE article_key=?
                    """,
                    (
                        float(result["score"]),
                        str(result["reasoning"]),
                        str(result["impact"]),
                        _now_kst().strftime("%Y-%m-%d %H:%M:%S"),
                        provider,
                        model,
                        article_key,
                    ),
                )
                scored += 1
            except urllib.error.HTTPError as e:
                try:
                    body = e.read().decode("utf-8", errors="replace").replace("\n", " ")
                except Exception:
                    body = ""
                err = f"{article_key}:HTTPError:{int(e.code)}:{body[:400]}"
                errors.append(err)
                con.execute(
                    "UPDATE news_articles_naver SET llm_error=? WHERE article_key=?",
                    (err[:500], article_key),
                )
                if int(e.code) in {401, 429}:
                    abort_remaining = True
            except Exception as e:
                err = f"{article_key}:{type(e).__name__}:{e}"
                errors.append(err)
                con.execute(
                    "UPDATE news_articles_naver SET llm_error=? WHERE article_key=?",
                    (err[:500], article_key),
                )
            con.commit()
            if abort_remaining:
                break
            if float(args.sleep) > 0 and idx < len(rows):
                time.sleep(float(args.sleep))

        status["rows_scored"] = int(scored)
        status["error_count"] = int(len(errors))
        status["errors"] = errors[:20]
        status["quality"] = "PASS" if scored == len(rows) else ("WARN" if scored > 0 else "SKIP")
        status["reason"] = "ok" if not errors else "partial_errors"
        _write_status(status, as_of)
        print(f"[NEWS_LLM] quality={status['quality']} rows_scored={scored}/{len(rows)} errors={len(errors)}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
