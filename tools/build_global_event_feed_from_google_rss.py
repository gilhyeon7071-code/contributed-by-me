from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

GOOGLE_RSS_JSON = LOG_DIR / "google_news_rss_probe_macro_latest.json"
OUT_JSON = LOG_DIR / "global_event_feed_latest.json"
OUT_CSV = LOG_DIR / "global_event_feed_latest.csv"

THEME_KEYWORDS = {
    "TECH_INNOVATION": ["AI", "GPT", "로봇", "반도체", "데이터센터", "양자", "스테이블코인", "비전프로", "피지컬"],
    "GLOBAL_CONFLICT": ["전쟁", "분쟁", "이스라엘", "하마스", "러시아", "우크라이나", "이란", "중동", "방산"],
    "POLICY_REGULATION": ["정책", "규제", "IRA", "관세", "원전", "재건", "대마", "정부", "법안"],
    "COMMODITY_SUPPLY": ["리튬", "희토류", "구리", "곡물", "팜유", "천연가스", "원자재", "공급망"],
    "BIGTECH_LINK": ["삼성", "애플", "테슬라", "스페이스X", "엔비디아", "구글", "마이크로소프트"],
    "CONTENT_CULTURE": ["K푸드", "K화장품", "오징어게임", "한강", "콘텐츠", "화장품", "미디어"],
    "POLITICAL_PERSON": ["대선", "후보", "정치", "트럼프", "한동훈", "계엄"],
    "ENERGY_SPACE": ["우주", "태양광", "전력", "에너지", "원전", "전선", "유리기판"],
}

GLOBAL_THEMES = {"GLOBAL_CONFLICT", "POLICY_REGULATION", "COMMODITY_SUPPLY", "BIGTECH_LINK", "ENERGY_SPACE"}


KST = timezone(timedelta(hours=9))

def _now_kst() -> datetime:
    return datetime.now(KST)

def _now_ts() -> str:
    return _now_kst().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except UnicodeDecodeError:
            continue
    return {}


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _code(value: Any) -> str:
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def _theme_hits(text: str) -> tuple[list[str], list[str]]:
    upper_text = text.upper()
    themes: list[str] = []
    terms: list[str] = []
    for theme, keywords in THEME_KEYWORDS.items():
        hit_terms = [keyword for keyword in keywords if keyword.upper() in upper_text]
        if hit_terms:
            themes.append(theme)
            terms.extend(hit_terms[:3])
    return themes, terms


def _score(themes: list[str], item_count: int, fresh_items: int) -> tuple[str, float, str]:
    score = 0.0
    evidence: list[str] = []
    if any(theme in GLOBAL_THEMES for theme in themes):
        score += 45.0
        evidence.append("global_theme_hit")
    elif themes:
        score += 25.0
        evidence.append("theme_hit")
    if item_count >= 5:
        score += 20.0
        evidence.append(f"rss_items={item_count}")
    elif item_count >= 1:
        score += 8.0
        evidence.append(f"rss_items={item_count}")
    if fresh_items >= 2:
        score += 20.0
        evidence.append(f"fresh_items={fresh_items}")
    elif fresh_items >= 1:
        score += 10.0
        evidence.append(f"fresh_items={fresh_items}")

    score = min(score, 100.0)
    if score >= 60.0:
        return "GLOBAL_EVENT_FEED_MATCH", round(score, 3), "|".join(evidence)
    if score >= 35.0:
        return "THEME_NEWS_FEED_MATCH", round(score, 3), "|".join(evidence)
    if score > 0.0:
        return "WEAK_FEED_MATCH", round(score, 3), "|".join(evidence)
    return "NO_EVENT_FEED_MATCH", 0.0, "no_keyword_event_match"


def build(dry_run: bool = False) -> dict[str, Any]:
    src = _read_json(GOOGLE_RSS_JSON)
    results = src.get("results") if isinstance(src.get("results"), list) else []
    rows: list[dict[str, Any]] = []
    
    now = _now_kst()
    cutoff = now - timedelta(days=2)

    for result in results:
        code = _code(result.get("code"))
        items = result.get("items_sample") if isinstance(result.get("items_sample"), list) else []
        text = " ".join(
            " ".join([str(item.get("title") or ""), str(item.get("description") or ""), str(item.get("source") or "")])
            for item in items
            if isinstance(item, dict)
        )
        themes, terms = _theme_hits(text)
        
        fresh_items = 0
        for item in items:
            pub_str = str((item or {}).get("published_at") or "")
            try:
                pub_dt = datetime.fromisoformat(pub_str)
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=KST)
                if pub_dt >= cutoff:
                    fresh_items += 1
            except Exception:
                pass

        label, score, evidence = _score(themes, int(result.get("item_count") or 0), fresh_items)
        rows.append(
            {
                "code": code,
                "name": str(result.get("name") or ""),
                "global_event_feed_label": label,
                "global_event_feed_score": score,
                "global_event_feed_themes": "|".join(themes),
                "global_event_feed_terms": "|".join(sorted(set(terms))),
                "global_event_feed_evidence": evidence,
                "global_event_feed_item_count": int(result.get("item_count") or 0),
                "global_event_feed_fresh_items": fresh_items,
                "global_event_feed_source": "google_news_rss_probe_latest",
                "research_only": True,
                "policy_change": False,
                "entry_approval_changed": False,
                "paper_order_route": False,
                "broker_order_route": False,
                "trading_route": False,
            }
        )
    rows.sort(key=lambda r: (-float(r["global_event_feed_score"]), str(r["code"])))
    fields = list(rows[0].keys()) if rows else ["code", "global_event_feed_label"]
    
    if not dry_run:
        _write_csv(OUT_CSV, rows, fields)
        
    summary = {
        "generated_at": _now_ts(),
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "scope": "read_only_global_event_feed_from_google_rss",
        "source_file": str(GOOGLE_RSS_JSON),
        "source_generated_at": src.get("generated_at", ""),
        "source_quality": src.get("quality", ""),
        "source_item_count_total": src.get("item_count_total", 0),
        "row_count": len(rows),
        "label_counts": dict(Counter(row["global_event_feed_label"] for row in rows)),
        "theme_counts": dict(Counter(theme for row in rows for theme in str(row["global_event_feed_themes"]).split("|") if theme)),
        "top_rows": rows[:20],
        "access_issues": [
            "feed is based on latest available Google RSS probe artifact, not a fresh network fetch in this tool",
            "keyword theme matching is a read-only event proxy and does not prove direct beneficiary status",
        ],
        "dry_run": dry_run,
    }
    
    if not dry_run:
        with OUT_JSON.open("w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
            
    return summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    summary = build(dry_run=args.dry_run)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
