from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
BEGIN = "<!-- STOCK_AI_AUTO_LINKS_BEGIN -->"
END = "<!-- STOCK_AI_AUTO_LINKS_END -->"

CONCEPT_RULES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("earnings", "실적", ("실적", "영업이익", "매출", "1분기", "2분기", "3분기", "4분기", "1Q", "2Q", "3Q", "4Q")),
    ("bio", "바이오", ("바이오", "신약", "의약", "헬스케어")),
    ("eco-packaging", "친환경 패키징", ("친환경", "패키징", "PHA", "코팅제")),
    ("exports", "수출", ("수출", "해외", "글로벌")),
    ("robotics", "로봇", ("로봇", "로보틱스", "자동화")),
    ("gas-energy", "가스 에너지", ("가스", "LPG", "에너지", "수소")),
    ("medical-cooperation", "의료 협진", ("협진", "응급", "의료", "병원")),
    ("holding-company", "지주회사", ("지주", "홀딩스")),
)


@dataclass(frozen=True)
class Article:
    code: str
    name: str
    title: str
    source: str
    published_at: str
    url: str
    article_date: str
    body_status: str
    original_text_available: bool
    description: str = ""
    body: str = ""


def slug(value: str) -> str:
    text = re.sub(r"[^0-9A-Za-z가-힣A-Z_a-z-]+", "-", value.strip())
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


def wiki_name(path: Path) -> str:
    return path.stem


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def load_articles(path: Path) -> list[Article]:
    data = read_json(path)
    articles: list[Article] = []
    for raw in data.get("articles", []):
        if not isinstance(raw, dict):
            continue
        code = str(raw.get("code") or "").strip()
        if not code:
            continue
        articles.append(
            Article(
                code=code,
                name=str(raw.get("name") or "").strip(),
                title=str(raw.get("title") or "").strip(),
                source=str(raw.get("source") or "").strip(),
                published_at=str(raw.get("published_at") or "").strip(),
                url=str(raw.get("url") or "").strip(),
                article_date=str(raw.get("article_date") or "").strip(),
                body_status=str(raw.get("body_status") or "missing_original_text").strip(),
                original_text_available=bool(raw.get("original_text_available") is True),
                description=str(raw.get("description") or "").strip(),
                body=str(raw.get("body") or "").strip(),
            )
        )
    return articles


# 기업 기사 본문에 습관적으로 등장해 변별력이 없는 축.
# 이들만 제목/종목명/출처에서 판정하고, 나머지는 본문까지 본다.
# 근거: 2026-08-21 실측(n=10). 본문을 전 규칙에 넣으면 미분류 6->1 로 줄지만
# earnings 가 8/10 에 붙어 태그가 필터 구실을 못 했다. 흔한 축을 제목 한정으로 바꾸면
# 미분류 3/10, 기사당 개념 1.1, earnings 1 로 변별력이 생긴다.
# **표본이 10건뿐이므로 며칠 쌓인 뒤 재검토할 것.** (PLANS 2026-08-21 (42))
GENERIC_CONCEPT_KEYS: frozenset[str] = frozenset({"earnings", "exports", "holding-company"})


def detect_concepts(article: Article) -> list[tuple[str, str]]:
    head = " ".join([article.title, article.name, article.source]).lower()
    full = " ".join([head, article.description, article.body]).lower()
    found: list[tuple[str, str]] = []
    for key, label, terms in CONCEPT_RULES:
        haystack = head if key in GENERIC_CONCEPT_KEYS else full
        if any(term.lower() in haystack for term in terms):
            found.append((key, label))
    if not found:
        found.append(("unclassified-news", "미분류 뉴스"))
    return found


def company_path(article: Article) -> Path:
    return ROOT / "10_Companies" / f"KRX_{article.code}_{slug(article.name)}.md"


def concept_path(key: str, label: str) -> Path:
    return ROOT / "20_Themes" / "concepts" / f"concept_{key}_{slug(label)}.md"


def managed_block(article: Article, archive_path: Path, company: Path, concepts: list[Path], now: str) -> str:
    concept_links = "\n".join(f"- Concept: [[{wiki_name(path)}]]" for path in concepts)
    if not concept_links:
        concept_links = "- Concept: not_available"
    return f"""{BEGIN}
## Auto Links
- Updated at: `{now}`
- Company: [[{wiki_name(company)}]]
- Article archive seed: `{archive_path}`
- Latest observation title: `{article.title or 'unknown'}`
- Latest observation source: `{article.source or 'unknown'}`
- Latest observation published_at: `{article.published_at or 'unknown'}`
- Latest observation url: `{article.url or 'unknown'}`
- Body status: `{article.body_status}`
- Original text available: `{str(article.original_text_available).lower()}`
{concept_links}

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
{END}
"""


def replace_or_append_managed_block(text: str, block: str) -> str:
    pattern = re.compile(re.escape(BEGIN) + r".*?" + re.escape(END), re.DOTALL)
    if pattern.search(text):
        return pattern.sub(lambda _match: block.strip(), text).rstrip() + "\n"
    return text.rstrip() + "\n\n" + block.strip() + "\n"


def refresh_company_header(text: str, article: Article, concepts: list[Path]) -> str:
    """회사 노트의 기계 생성 머리말 두 섹션을 현재 값으로 갱신한다 (PLANS 2026-08-21 (43)).

    기존에는 `## Linked Concepts` / `## Latest Observation` 이 노트 **생성 시점**에만 쓰였다.
    그 뒤 분류가 좋아지거나 새 기사가 와도 머리말은 옛 값을 유지해서,
    같은 노트 안에서 머리말(옛 값)과 `## Auto Links`(최신)가 어긋나 보였다.
    회사 노트 293개가 전부 `human_reviewed: false` 이므로 기계 섹션 교체는 안전하다.
    """
    concept_links = "\n".join(f"    - [[{wiki_name(path)}]]" for path in concepts) or "    - not_available"
    text = re.sub(
        r"(## Linked Concepts\n).*?(?=\n## )",
        lambda m: m.group(1) + concept_links + "\n",
        text,
        count=1,
        flags=re.DOTALL,
    )
    observation = (
        f"- Title: `{article.title or 'unknown'}`\n"
        f"- Source: `{article.source or 'unknown'}`\n"
        f"- Published at: `{article.published_at or 'unknown'}`\n"
        f"- Original text available: `{str(article.original_text_available).lower()}`\n"
    )
    text = re.sub(
        r"(## Latest Observation\n).*?(?=\n## )",
        lambda m: m.group(1) + observation,
        text,
        count=1,
        flags=re.DOTALL,
    )
    return text


def note_paths_for(article: Article, note_date: str) -> list[Path]:
    stem = f"{note_date}_KRX_{article.code}_google-rss-coverage"
    return [
        ROOT / "01_Sources" / f"{stem}-source.md",
        ROOT / "01_Sources" / f"{stem}-verification.md",
        ROOT / "30_News" / f"{stem}.md",
        ROOT / "20_Themes" / f"{note_date}_KRX_{article.code}_google-rss-thesis-blocked.md",
    ]


def company_doc(article: Article, concepts: list[Path], now: str) -> str:
    concept_links = "\n".join(f"    - [[{wiki_name(path)}]]" for path in concepts)
    return f"""---
id: company-KRX-{article.code}
type: company
title: KRX {article.code} {article.name}
created: {now[:10]}
updated: {now[:10]}
status: active
stage: 0

market: KRX
ticker: "{article.code}"
company: {article.name}

verification:
  verified: false
  verification_status: unknown

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: generated company link hub
---

# KRX {article.code} {article.name}

## Linked Concepts
{concept_links or "    - not_available"}

## Latest Observation
- Title: `{article.title or 'unknown'}`
- Source: `{article.source or 'unknown'}`
- Published at: `{article.published_at or 'unknown'}`
- Original text available: `false`

## Safety Boundary
- This company note is a research link hub only.
- It does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
"""


def concept_doc(key: str, label: str, articles: list[Article], now: str) -> str:
    company_links = "\n".join(
        f"- [[{wiki_name(company_path(article))}]]" for article in sorted(articles, key=lambda item: item.code)
    )
    observations = "\n".join(
        f"- `{article.title or 'unknown'}` ({article.source or 'unknown'}, {article.published_at or 'unknown'})"
        for article in sorted(articles, key=lambda item: item.code)
    )
    return f"""---
id: concept-{key}
type: theme
title: {label}
created: {now[:10]}
updated: {now[:10]}
status: concept_candidate
stage: 0

verification:
  verified: false
  verification_status: unknown

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: generated concept candidate from RSS metadata
---

# {label}

## Linked Companies
{company_links or "- not_available"}

## Source Observations
{observations or "- not_available"}

## Verification Boundary
- This is a concept candidate from RSS metadata only.
- Source verification remains `unknown` until article body text is archived and checked.

## Safety Boundary
- This concept note does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Update Stock-AI-Wiki links from an article archive seed.")
    parser.add_argument("--article-archive-json", required=True, type=Path)
    parser.add_argument("--note-date", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    now = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    articles = load_articles(args.article_archive_json)
    planned: list[dict[str, str]] = []
    planned_new: set[Path] = set()
    concept_articles: dict[tuple[str, str], list[Article]] = {}

    for article in articles:
        for key, label in detect_concepts(article):
            concept_articles.setdefault((key, label), []).append(article)

    for article in articles:
        concepts = [concept_path(key, label) for key, label in detect_concepts(article)]
        cpath = company_path(article)
        if not cpath.exists() and cpath not in planned_new:
            planned.append({"path": str(cpath), "action": "write_company"})
            planned_new.add(cpath)
            if args.apply:
                cpath.parent.mkdir(parents=True, exist_ok=True)
                cpath.write_text(company_doc(article, concepts, now), encoding="utf-8")

        block = managed_block(article, args.article_archive_json, cpath, concepts, now)
        if cpath.exists():
            company_text = cpath.read_text(encoding="utf-8-sig")
            company_updated = refresh_company_header(company_text, article, concepts)
            company_updated = replace_or_append_managed_block(company_updated, block)
            if company_updated != company_text:
                company_updated = re.sub(
                    r"^updated: .*$", f"updated: {now[:10]}", company_updated, count=1, flags=re.M
                )
                planned.append({"path": str(cpath), "action": "update_company_links"})
                if args.apply:
                    cpath.write_text(company_updated, encoding="utf-8")

        for path in note_paths_for(article, args.note_date):
            if not path.exists():
                planned.append({"path": str(path), "action": "missing_note"})
                continue
            text = path.read_text(encoding="utf-8-sig")
            updated = replace_or_append_managed_block(text, block)
            if updated == text:
                planned.append({"path": str(path), "action": "unchanged"})
                continue
            planned.append({"path": str(path), "action": "update_links"})
            if args.apply:
                path.write_text(updated, encoding="utf-8")

    for (key, label), linked_articles in sorted(concept_articles.items()):
        path = concept_path(key, label)
        content = concept_doc(key, label, linked_articles, now)
        if path.exists():
            current = path.read_text(encoding="utf-8-sig")
            if current == content:
                planned.append({"path": str(path), "action": "unchanged_concept"})
                continue
            planned.append({"path": str(path), "action": "update_concept"})
        else:
            planned.append({"path": str(path), "action": "write_concept"})
        if args.apply:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    print(json.dumps({"apply": args.apply, "articles": len(articles), "planned": planned}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
