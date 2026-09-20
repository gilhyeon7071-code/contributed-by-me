from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def ymd_from_timestamp(value: str, fallback: str) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return fallback


def build_articles(probe: dict, max_items_per_code: int) -> list[dict]:
    generated_at = str(probe.get("generated_at") or "").strip()
    fallback_date = ymd_from_timestamp(generated_at, datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d"))
    articles: list[dict] = []

    for result in probe.get("results", []):
        if not isinstance(result, dict):
            continue
        code = str(result.get("code") or "").strip()
        name = str(result.get("name") or "").strip()
        if not code:
            continue
        items = result.get("items_sample") or []
        if not isinstance(items, list):
            continue
        for idx, item in enumerate(items[: max(1, int(max_items_per_code))], start=1):
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            url = str(item.get("link") or "").strip()
            description = str(item.get("description") or "").strip()
            published_at = str(item.get("published_at") or "").strip()
            source = str(item.get("source") or "").strip()
            article_date = ymd_from_timestamp(published_at, fallback_date)
            key_material = "|".join([code, title, url, published_at])
            archive_id = sha256_text(key_material)[:16]
            articles.append(
                {
                    "archive_id": archive_id,
                    "code": code,
                    "name": name,
                    "title": title,
                    "source": source,
                    "published_at": published_at,
                    "url": url,
                    "description": description,
                    "article_date": article_date,
                    "evidence_path": "",
                    "body": "",
                    "body_sha256": "",
                    "body_status": "missing_original_text",
                    "original_text_available": False,
                    "verification_status": "unknown",
                    "verified": False,
                    "trading_approved": False,
                    "direct_candidate_allowed": False,
                    "execution_allowed": False,
                    "source_probe_run_id": str(probe.get("run_id") or "").strip(),
                    "source_probe_generated_at": generated_at,
                    "source_probe_quality": str(probe.get("quality") or "").strip(),
                    "item_rank": idx,
                }
            )
    return articles


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a local article archive seed from Google RSS probe metadata.")
    parser.add_argument("--probe-json", required=True, type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--max-items-per-code", type=int, default=1)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    probe = read_json(args.probe_json)
    articles = build_articles(probe, args.max_items_per_code)
    generated_at = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    out = args.out
    if out is None:
        ymd = generated_at[:10]
        out = ROOT / "01_Sources" / "article_archive" / f"google_rss_article_archive_seed_{ymd}.json"

    payload = {
        "schema_version": 1,
        "kind": "google_rss_article_archive_seed",
        "generated_at": generated_at,
        "source_probe_json": str(args.probe_json),
        "source_probe_run_id": str(probe.get("run_id") or "").strip(),
        "source_probe_quality": str(probe.get("quality") or "").strip(),
        "body_contract": {
            "body_empty_means": "original article text has not been archived",
            "safe_to_mark_original_text_available": False,
            "generator_behavior": "entries with empty body are ignored by generate_coverage_notes.py",
        },
        "articles": articles,
        "summary": {
            "article_count": len(articles),
            "code_count": len({item["code"] for item in articles}),
            "all_original_text_available": False,
            "verification_status": "unknown",
            "trading_approved": False,
        },
    }

    planned = {"apply": bool(args.apply), "out": str(out), "article_count": len(articles)}
    if args.apply:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8-sig")
        planned["written"] = True
    else:
        planned["written"] = False
    print(json.dumps(planned, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
