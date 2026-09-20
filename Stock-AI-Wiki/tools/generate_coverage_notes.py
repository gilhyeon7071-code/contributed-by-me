from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class CoverageRow:
    code: str
    name: str
    naver_article_count: str
    google_rss_article_count: str
    kis_title_count: str
    naver_covered: str
    google_rss_covered: str
    kis_title_covered: str
    any_covered: str


@dataclass(frozen=True)
class ProbeItem:
    title: str
    link: str
    published_at: str
    source: str


@dataclass(frozen=True)
class ArticleArchiveEntry:
    code: str
    title: str
    source: str
    published_at: str
    url: str
    evidence_path: str
    body: str


def slug(value: str) -> str:
    text = re.sub(r"[^0-9A-Za-z가-힣_-]+", "-", value.strip())
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


def load_rows(path: Path, limit: int | None) -> list[CoverageRow]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows: list[CoverageRow] = []
        for raw in reader:
            rows.append(
                CoverageRow(
                    code=(raw.get("code") or "unknown").strip(),
                    name=(raw.get("name") or "unknown").strip(),
                    naver_article_count=(raw.get("naver_article_count") or "unknown").strip(),
                    google_rss_article_count=(raw.get("google_rss_article_count") or "unknown").strip(),
                    kis_title_count=(raw.get("kis_title_count") or "unknown").strip(),
                    naver_covered=(raw.get("naver_covered") or "unknown").strip(),
                    google_rss_covered=(raw.get("google_rss_covered") or "unknown").strip(),
                    kis_title_covered=(raw.get("kis_title_covered") or "unknown").strip(),
                    any_covered=(raw.get("any_covered") or "unknown").strip(),
                )
            )
            if limit is not None and len(rows) >= limit:
                break
        return rows


def load_probe_items(path: Path | None) -> dict[str, ProbeItem]:
    if path is None or not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    out: dict[str, ProbeItem] = {}
    for result in data.get("results", []):
        code = str(result.get("code") or "").strip()
        items = result.get("items_sample") or []
        if not code or not items:
            continue
        first = items[0]
        out[code] = ProbeItem(
            title=str(first.get("title") or "").strip(),
            link=str(first.get("link") or "").strip(),
            published_at=str(first.get("published_at") or "").strip(),
            source=str(first.get("source") or "").strip(),
        )
    return out


def load_article_archive(path: Path | None) -> dict[str, ArticleArchiveEntry]:
    if path is None or not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    raw_items = data.get("articles") if isinstance(data, dict) else data
    if isinstance(raw_items, dict):
        raw_items = [dict({"code": code}, **item) for code, item in raw_items.items()]
    if not isinstance(raw_items, list):
        raise ValueError("article archive JSON must be a list, an articles list, or a code-keyed object")

    out: dict[str, ArticleArchiveEntry] = {}
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        code = str(raw.get("code") or "").strip()
        body = str(raw.get("body") or raw.get("text") or "").strip()
        if not code or not body:
            continue
        out[code] = ArticleArchiveEntry(
            code=code,
            title=str(raw.get("title") or "").strip(),
            source=str(raw.get("source") or "").strip(),
            published_at=str(raw.get("published_at") or "").strip(),
            url=str(raw.get("url") or raw.get("link") or "").strip(),
            evidence_path=str(raw.get("evidence_path") or "").strip(),
            body=body,
        )
    return out


def md_frontmatter(body: str) -> str:
    return body.strip() + "\n"


def body_excerpt(body: str, max_chars: int = 900) -> str:
    compact = re.sub(r"\s+", " ", body).strip()
    if len(compact) <= max_chars:
        return compact
    return compact[: max_chars - 3].rstrip() + "..."


def build_notes(
    row: CoverageRow,
    date: str,
    source_path: Path,
    probe_item: ProbeItem | None = None,
    article_entry: ArticleArchiveEntry | None = None,
) -> dict[Path, str]:
    code = slug(row.code)
    name_slug = slug(row.name)
    stem = f"{date}_KRX_{code}_google-rss-coverage"
    source_ref = str(source_path)

    common_facts = [
        f"code={row.code}",
        f"name={row.name}",
        f"naver_article_count={row.naver_article_count}",
        f"google_rss_article_count={row.google_rss_article_count}",
        f"kis_title_count={row.kis_title_count}",
        f"google_rss_covered={row.google_rss_covered}",
        f"kis_title_covered={row.kis_title_covered}",
        f"any_covered={row.any_covered}",
    ]
    facts_yaml = "\n".join(f"    - {fact}" for fact in common_facts)
    facts_md = "\n".join(f"- `{fact}`" for fact in common_facts)
    if probe_item is None:
        probe_md = "- not_available"
        source_uncertainty = "    - Original article text is not stored in this coverage CSV row."
        news_uncertainty = "    - Coverage exists, but original article text is not verified."
        verification_uncertainty = "    - Original article text unavailable."
    else:
        probe_md = "\n".join(
            [
                f"- Title: `{probe_item.title}`",
                f"- Source: `{probe_item.source}`",
                f"- Published at: `{probe_item.published_at}`",
                f"- Link: `{probe_item.link}`",
            ]
        )
        source_uncertainty = "    - RSS item metadata is available, but full original article body is not stored locally."
        news_uncertainty = "    - RSS item metadata is available, but full original article body is not verified."
        verification_uncertainty = "    - RSS item metadata is available, but full original article body is unavailable."

    if article_entry is None:
        article_md = "- not_available"
        original_text_available = "false"
        article_question = "Which original article should be attached before source verification can pass?"
    else:
        article_md = "\n".join(
            [
                f"- Title: `{article_entry.title or 'unknown'}`",
                f"- Source: `{article_entry.source or 'unknown'}`",
                f"- Published at: `{article_entry.published_at or 'unknown'}`",
                f"- URL: `{article_entry.url or 'unknown'}`",
                f"- Evidence path: `{article_entry.evidence_path or 'unknown'}`",
                f"- Body excerpt: {body_excerpt(article_entry.body)}",
            ]
        )
        original_text_available = "true"
        article_question = "Does the archived article body actually refer to this listed company and event?"
        source_uncertainty = "    - Article body archive is available locally, but source verification has not passed."
        news_uncertainty = "    - Article body archive is available locally, but implication and entity verification have not passed."
        verification_uncertainty = "    - Article body archive is available locally, but entity/event verification is still unknown."

    source_note = md_frontmatter(
        f"""
---
id: source-{date}-KRX-{code}-google-rss-coverage
type: source
title: KRX {code} Google RSS Coverage Source
created: {date}
updated: {date}
status: raw
stage: 0

market: KRX
ticker: "{row.code}"
company: {row.name}
theme: []

source:
  type: local_coverage_csv
  name: {source_path.name}
  url: {source_ref}
  published_at:
  collected_at: {date}

analysis:
  summary: Local coverage report row shows news coverage for KRX {code}.
  key_facts:
{facts_yaml}
  related_entities:
    - KRX {code}
    - {row.name}
  possible_impact: unknown
  uncertainty:
{source_uncertainty}

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
  policy_link:

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: generated coverage source note
---

# KRX {code} Google RSS Coverage Source

## Original Material
- Evidence path: `{source_ref}`
{facts_md}

## Facts
- The local coverage report contains a coverage row for KRX {code}.

## RSS Item Metadata
{probe_md}

## Article Body Archive
{article_md}

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- {article_question}
"""
    )

    news_note = md_frontmatter(
        f"""
---
id: news-{date}-KRX-{code}-google-rss-coverage
type: news
title: KRX {code} Google RSS Coverage
created: {date}
updated: {date}
status: interpreted
stage: 0

market: KRX
ticker: "{row.code}"
company: {row.name}
theme: []

source:
  type: local_coverage_csv
  name: {source_path.name}
  url: {source_ref}
  published_at:
  collected_at: {date}

analysis:
  summary: Local coverage report indicates news coverage for KRX {code}.
  key_facts:
{facts_yaml}
  related_entities:
    - KRX {code}
    - {row.name}
  possible_impact: unknown
  uncertainty:
{news_uncertainty}

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
  policy_link:

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: generated coverage news note
---

# KRX {code} Google RSS Coverage

## Source
- [[{stem}-source]]

## Facts
{facts_md}

## RSS Item Metadata
{probe_md}

## Article Body Archive
{article_md}

## Interpretation
- The local pipeline observed news coverage for KRX {code}.
- This note does not infer sentiment or trading implication.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Is the coverage item actually about the listed company?
"""
    )

    verification_note = md_frontmatter(
        f"""
---
id: verification-{date}-KRX-{code}-google-rss-coverage
type: verification
title: KRX {code} Google RSS Coverage Verification
created: {date}
updated: {date}
status: verification
stage: 1

market: KRX
ticker: "{row.code}"
company: {row.name}
theme: []

source:
  type: local_coverage_csv
  name: {source_path.name}
  url: {source_ref}
  published_at:
  collected_at: {date}

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
{facts_yaml}
  related_entities:
    - KRX {code}
    - {row.name}
  possible_impact: unknown
  uncertainty:
{verification_uncertainty}

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: {original_text_available}
  numeric_values_checked: true
  date_values_checked: true
  entity_names_checked: false
  conflict_exists: false
  conflict_summary:
  confidence: unknown

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
  policy_link:

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: generated coverage verification note
---

# KRX {code} Google RSS Coverage Verification

## Source Being Checked
- [[{stem}-source]]

## Facts Checked
{facts_md}

## RSS Item Metadata Checked
{probe_md}

## Article Body Archive Checked
{article_md}

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
"""
    )

    thesis_note = md_frontmatter(
        f"""
---
id: thesis-{date}-KRX-{code}-google-rss-blocked
type: thesis
title: KRX {code} Google RSS Thesis Blocked
created: {date}
updated: {date}
status: thesis
stage: 2

market: KRX
ticker: "{row.code}"
company: {row.name}
theme: []

verification:
  verified: false
  verification_status: unknown
  source_count: 1
  confidence: unknown

thesis:
  thesis_status: blocked_unverified_source
  source_verified_required: true
  source_verified: false
  claim: unknown
  supporting_notes:
    - [[{stem}-source]]
    - [[{stem}-verification]]
  opposing_notes:
    - Source verification is unknown.
  invalidation_conditions:
    - Original article body remains unavailable.
    - Source verification remains unknown.
  time_horizon: unknown
  impact_direction: unknown
  impact_strength: unknown

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
  policy_link:

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: generated blocked thesis note
---

# KRX {code} Google RSS Thesis Blocked

## Claim
- unknown

## Supporting Evidence
- [[{stem}-source]]
- [[{stem}-verification]]

## Remaining Uncertainty
- Coverage exists, but no verified event claim exists.

## Trading Boundary
- This blocked thesis does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
"""
    )

    return {
        ROOT / "01_Sources" / f"{stem}-source.md": source_note,
        ROOT / "30_News" / f"{stem}.md": news_note,
        ROOT / "01_Sources" / f"{stem}-verification.md": verification_note,
        ROOT / "20_Themes" / f"{date}_KRX_{code}_google-rss-thesis-blocked.md": thesis_note,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Stage 0-2 coverage wiki notes.")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--date", required=True)
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--probe-json", type=Path)
    parser.add_argument("--article-archive-json", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    rows = load_rows(args.input, args.limit)
    probe_items = load_probe_items(args.probe_json)
    article_entries = load_article_archive(args.article_archive_json)
    planned: list[dict[str, str]] = []

    for row in rows:
        for path, content in build_notes(
            row,
            args.date,
            args.input,
            probe_items.get(row.code),
            article_entries.get(row.code),
        ).items():
            exists = path.exists()
            action = "skip_exists" if exists else ("write" if args.apply else "would_write")
            planned.append({"path": str(path), "action": action})
            if args.apply and not exists:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")

    print(json.dumps({"apply": args.apply, "rows": len(rows), "planned": planned}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
