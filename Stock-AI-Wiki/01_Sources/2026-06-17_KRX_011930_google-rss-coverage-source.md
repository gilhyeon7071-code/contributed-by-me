---
id: source-2026-06-17-KRX-011930-google-rss-coverage
type: source
title: KRX 011930 Google RSS Coverage Source
created: 2026-06-17
updated: 2026-06-17
status: raw
stage: 0

market: KRX
ticker: "011930"
company: 신성이엔지
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-17

analysis:
  summary: Local coverage report row shows news coverage for KRX 011930.
  key_facts:
    - code=011930
    - name=신성이엔지
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=11
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 011930
    - 신성이엔지
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

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

# KRX 011930 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=011930`
- `name=신성이엔지`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 011930.

## RSS Item Metadata
- Title: `신성이엔지 이승환 임원 장내서 500주 매입... 책임경영 의지 반영 - 데이터투자`
- Source: `데이터투자`
- Published at: `2026-06-16T14:59:36+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE8zb2JWcXI4dnd1am5sa20yY2dHQkt4N3BwdU5DV3FnVXpzNnk5ZXVvTkZoRXlLSXNIdnpTWm1IWXlPdzZVUDVxb3huYzBwQTU5d3dDelhfWFpPbV9ocXpSaS05OXZHdFdmeThURTZvWElnU0E?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-17T17:05:11+09:00`
- Company: [[KRX_011930_신성이엔지]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-17.json`
- Latest observation title: `신성이엔지-스털링앤윌슨 MOU, 인도·중동 시장 공략 협력 - 디일렉`
- Latest observation source: `디일렉`
- Latest observation published_at: `2026-06-15T21:47:08+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTFBuMzc2RkxscmlDalRYQU9wMFlmOWhVemYxM3dRVUpzbk9MTm9KaTI2WFB2UUctbzNtNjhwWlAyajIxLUg4QXFNU3RmYkNEejVjZ0QzWEg4ZS1VeU1LWHc0VWhJaUstdw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
