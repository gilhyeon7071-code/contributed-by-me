from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
DB = ROOT / "news_trading" / "data" / "trading.db"
KST = timezone(timedelta(hours=9))

OUT_JSON = LOGS / "global_macro_risk_latest.json"
OUT_CSV = LOGS / "global_macro_risk_latest.csv"
OPENAI_KEY_FILES = [
    ROOT / ".secrets" / "openai_api_key.txt",
    ROOT / "_secrets" / "openai_api_key.txt",
    ROOT / "_cache" / "openai_api_key.txt",
]

RISK_TERMS = {
    "risk_off": ["risk off", "selloff", "slump", "tumble", "safe haven", "volatility", "outflows"],
    "ai_profit_taking": ["ai bubble", "profit taking", "crowded trade", "overvalued", "mega-cap tech", "technology shares"],
    "foreign_outflow": ["foreign investors sold", "foreign outflows", "trimmed exposure", "capital outflow"],
    "asia_pressure": ["asia stocks slip", "asia markets fall", "emerging asia", "south korea stocks"],
}
ROTATION_TERMS = ["rotation", "defense shares outperform", "value shares", "laggards outperform", "sector rotation"]
SECTOR_TERMS = {
    "semiconductor": ["semiconductor", "chip", "chips", "hbm", "memory", "nvidia", "ai-linked"],
    "defense": ["defense", "defence", "aerospace", "weapons"],
    "battery": ["battery", "ev", "lithium"],
}

TEST_ROWS = [
    {
        "article_key": "test_global_macro_1",
        "date8": "20260702",
        "source_id": "test",
        "title": "Asia stocks slip as investors take profits in AI-linked technology shares",
        "description": "Foreign investors trimmed exposure to crowded semiconductor and AI trades.",
        "link": "https://example.invalid/1",
        "published_at": "2026-07-02T08:00:00+09:00",
    },
    {
        "article_key": "test_global_macro_2",
        "date8": "20260702",
        "source_id": "test",
        "title": "Defense shares outperform as funds rotate away from mega-cap technology",
        "description": "The move was described as sector rotation rather than broad liquidation.",
        "link": "https://example.invalid/2",
        "published_at": "2026-07-02T08:05:00+09:00",
    },
]


def _now_kst() -> datetime:
    return datetime.now(KST)


def _read_key() -> str:
    key = str(os.getenv("OPENAI_API_KEY", "")).strip()
    if key:
        return key
    for path in OPENAI_KEY_FILES:
        try:
            if path.exists():
                key = path.read_text(encoding="utf-8").replace("\ufeff", "").strip()
                if key:
                    return key
        except Exception:
            continue
    return ""


def _norm_date8(value: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else _now_kst().strftime("%Y%m%d")


def _load_rows(as_of: str, lookback_days: int, limit: int, db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    con = sqlite3.connect(str(db_path), timeout=max(1.0, float(os.getenv("NEWS_GLOBAL_MACRO_SQLITE_TIMEOUT_SEC", "30") or "30")))
    try:
        con.row_factory = sqlite3.Row
        start_dt = datetime.strptime(as_of, "%Y%m%d") - timedelta(days=max(0, int(lookback_days)))
        start = start_dt.strftime("%Y%m%d")
        cur = con.execute(
            """
            SELECT article_key, date8, source_id, source_name, title, description, link, published_at
            FROM news_articles_global_macro
            WHERE date8 >= ? AND date8 <= ?
            ORDER BY date8 DESC, published_at DESC
            LIMIT ?
            """,
            (start, as_of, max(1, int(limit))),
        )
        return [dict(row) for row in cur.fetchall()]
    except sqlite3.OperationalError:
        return []
    finally:
        con.close()


def _keyword_assessment(rows: list[dict[str, Any]]) -> dict[str, Any]:
    hits: dict[str, int] = {k: 0 for k in RISK_TERMS}
    sectors: dict[str, int] = {k: 0 for k in SECTOR_TERMS}
    rotation_hits = 0
    evidence: list[str] = []
    for row in rows:
        text = f"{row.get('title', '')} {row.get('description', '')}".lower()
        for bucket, terms in RISK_TERMS.items():
            if any(term in text for term in terms):
                hits[bucket] += 1
        for sector, terms in SECTOR_TERMS.items():
            if any(term in text for term in terms):
                sectors[sector] += 1
        if any(term in text for term in ROTATION_TERMS):
            rotation_hits += 1
        if len(evidence) < 5 and any(any(term in text for term in terms) for terms in RISK_TERMS.values()):
            evidence.append(str(row.get("title") or "")[:220])

    risk_score = 0.0
    risk_score += min(35.0, hits["risk_off"] * 18.0)
    risk_score += min(25.0, hits["ai_profit_taking"] * 14.0)
    risk_score += min(20.0, hits["foreign_outflow"] * 12.0)
    risk_score += min(15.0, hits["asia_pressure"] * 8.0)
    if rotation_hits:
        risk_score = max(0.0, risk_score - min(20.0, rotation_hits * 8.0))
    risk_score = round(min(100.0, risk_score), 3)

    if risk_score >= 70:
        state = "RISK_OFF_WATCH"
    elif risk_score >= 40:
        state = "SECTOR_RISK_WATCH"
    elif risk_score > 0:
        state = "MACRO_MONITOR"
    else:
        state = "NO_GLOBAL_MACRO_RISK"

    active_sectors = [k for k, v in sectors.items() if v > 0]
    if not active_sectors and risk_score >= 40:
        scope = "global_macro"
    elif active_sectors:
        scope = "sector"
    else:
        scope = "none"

    return {
        "risk_score": risk_score,
        "risk_state": state,
        "scope": scope,
        "sector_scope": active_sectors,
        "risk_term_counts": hits,
        "sector_term_counts": sectors,
        "rotation_hits": rotation_hits,
        "evidence_titles": evidence,
        "reason": "keyword_shadow_assessment",
    }


def _prompt(rows: list[dict[str, Any]]) -> str:
    items = []
    for i, row in enumerate(rows[:10], start=1):
        items.append(
            f"{i}. title={row.get('title','')}\n"
            f"   source={row.get('source_id','')} published={row.get('published_at','')}\n"
            f"   summary={row.get('description','')}"
        )
    return (
        "Classify the following English global market headlines for Korean equity trading risk.\n"
        "Return JSON only with keys: risk_score(0-100), risk_state(one of NO_GLOBAL_MACRO_RISK, MACRO_MONITOR, "
        "SECTOR_RISK_WATCH, RISK_OFF_WATCH), scope(one of none, sector, global_macro), sector_scope(array), "
        "evidence_titles(array), reasoning(short English).\n"
        "Important: do not recommend live trading, do not change gates, distinguish broad risk-off from sector rotation.\n\n"
        + "\n".join(items)
    )


def _parse_json_text(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()
    m = re.search(r"\{.*\}", raw, flags=re.S)
    obj = json.loads(m.group(0) if m else raw)
    risk_score = max(0.0, min(100.0, float(obj.get("risk_score", 0.0) or 0.0)))
    risk_state = str(obj.get("risk_state") or "MACRO_MONITOR").strip().upper()
    if risk_state not in {"NO_GLOBAL_MACRO_RISK", "MACRO_MONITOR", "SECTOR_RISK_WATCH", "RISK_OFF_WATCH"}:
        risk_state = "MACRO_MONITOR"
    scope = str(obj.get("scope") or "none").strip().lower()
    if scope not in {"none", "sector", "global_macro"}:
        scope = "none"
    sectors = obj.get("sector_scope") if isinstance(obj.get("sector_scope"), list) else []
    evidence = obj.get("evidence_titles") if isinstance(obj.get("evidence_titles"), list) else []
    return {
        "risk_score": round(risk_score, 3),
        "risk_state": risk_state,
        "scope": scope,
        "sector_scope": [str(x)[:80] for x in sectors[:8]],
        "evidence_titles": [str(x)[:220] for x in evidence[:5]],
        "reasoning": str(obj.get("reasoning") or "")[:500],
    }


def _call_openai(rows: list[dict[str, Any]], model: str, timeout_sec: float) -> dict[str, Any]:
    key = _read_key()
    if not key:
        raise RuntimeError("openai_api_key_missing")
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a conservative market-risk classifier. Return valid JSON only."},
            {"role": "user", "content": _prompt(rows)},
        ],
        "temperature": 0,
        "max_tokens": 700,
        "response_format": {"type": "json_object"},
    }
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=max(1.0, float(timeout_sec))) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return _parse_json_text(payload["choices"][0]["message"]["content"])


def _write_csv(payload: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "as_of_ymd",
        "risk_state",
        "risk_score",
        "scope",
        "sector_scope",
        "llm_used",
        "quality",
        "reason",
        "evidence_title",
    ]
    evidence = payload.get("evidence_titles") if isinstance(payload.get("evidence_titles"), list) else []
    if not evidence:
        evidence = [str(row.get("title") or "") for row in rows[:5]]
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for title in evidence[:10]:
            writer.writerow(
                {
                    "as_of_ymd": payload.get("as_of_ymd", ""),
                    "risk_state": payload.get("risk_state", ""),
                    "risk_score": payload.get("risk_score", 0),
                    "scope": payload.get("scope", ""),
                    "sector_scope": "|".join(payload.get("sector_scope") or []),
                    "llm_used": str(bool(payload.get("llm_used"))),
                    "quality": payload.get("quality", ""),
                    "reason": payload.get("reason", ""),
                    "evidence_title": str(title)[:240],
                }
            )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build a shadow-only global macro risk judgment from English headlines.")
    ap.add_argument("--as-of", default=_now_kst().strftime("%Y%m%d"))
    ap.add_argument("--lookback-days", type=int, default=int(os.getenv("NEWS_GLOBAL_RISK_LOOKBACK_DAYS", "2") or "2"))
    ap.add_argument("--max-articles", type=int, default=int(os.getenv("NEWS_GLOBAL_RISK_MAX_ARTICLES", "12") or "12"))
    ap.add_argument("--test-prompt", action="store_true", help="Use deterministic English sample headlines.")
    ap.add_argument("--no-api", action="store_true", help="Force keyword-only classification.")
    ap.add_argument("--timeout-sec", type=float, default=float(os.getenv("NEWS_GLOBAL_RISK_LLM_TIMEOUT_SEC", "25") or "25"))
    ap.add_argument("--model", default=os.getenv("NEWS_GLOBAL_RISK_LLM_MODEL", "gpt-4o-mini"))
    ap.add_argument("--db-path", default=str(DB), help="SQLite DB path. Defaults to the operational trading.db.")
    args = ap.parse_args(argv)

    LOGS.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    as_of = _norm_date8(args.as_of)
    db_path = Path(str(args.db_path))
    rows = list(TEST_ROWS) if args.test_prompt else _load_rows(as_of, args.lookback_days, args.max_articles, db_path=db_path)
    llm_requested = (str(os.getenv("NEWS_GLOBAL_RISK_LLM_ENABLE", "")).strip() == "1") or (
        str(os.getenv("NEWS_LLM_ENABLE", "")).strip() == "1" and not args.no_api
    )

    keyword_payload = _keyword_assessment(rows)
    llm_payload: dict[str, Any] = {}
    llm_error = ""
    llm_used = False
    if rows and llm_requested and not args.no_api:
        try:
            llm_payload = _call_openai(rows, model=str(args.model), timeout_sec=float(args.timeout_sec))
            llm_used = True
        except Exception as exc:
            llm_error = f"{type(exc).__name__}: {str(exc)[:180]}"

    chosen = dict(keyword_payload)
    if llm_payload:
        chosen.update(llm_payload)
        chosen["reason"] = "llm_shadow_assessment"
    quality = "PASS" if rows else "FAIL_SOFT"
    if rows and llm_requested and not llm_used:
        quality = "WARN"

    payload = {
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "as_of_ymd": as_of,
        "quality": quality,
        "reason": chosen.get("reason", "no_rows"),
        "row_count": len(rows),
        "risk_score": float(chosen.get("risk_score", 0.0) or 0.0),
        "risk_state": str(chosen.get("risk_state", "NO_GLOBAL_MACRO_RISK")),
        "scope": str(chosen.get("scope", "none")),
        "sector_scope": chosen.get("sector_scope", []),
        "risk_term_counts": chosen.get("risk_term_counts", {}),
        "sector_term_counts": chosen.get("sector_term_counts", {}),
        "rotation_hits": int(chosen.get("rotation_hits", 0) or 0),
        "evidence_titles": chosen.get("evidence_titles", []),
        "llm_requested": bool(llm_requested and not args.no_api),
        "llm_used": bool(llm_used),
        "llm_error": llm_error,
        "llm_model": str(args.model) if llm_used else "",
        "output_csv": str(OUT_CSV),
        "source_table": "news_articles_global_macro",
        "db_path": str(db_path),
        "elapsed_sec": round(float(time.monotonic() - started), 3),
        "trading_effect": False,
        "gate_effect": False,
        "score_effect": False,
        "risk_lock_effect": False,
        "order_route_effect": False,
        "broker_route_effect": False,
        "policy_change": False,
        "execution_allowed": False,
        "top_rows": rows[:10],
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(payload, rows)
    print(json.dumps({"quality": quality, "risk_state": payload["risk_state"], "risk_score": payload["risk_score"], "llm_used": llm_used, "output": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
