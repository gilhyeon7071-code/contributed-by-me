---
id: source-2026-07-15-KRX-093370-google-rss-coverage
type: source
title: KRX 093370 Google RSS Coverage Source
created: 2026-07-15
updated: 2026-07-15
status: raw
stage: 0

market: KRX
ticker: "093370"
company: 후성
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-15

analysis:
  summary: Local coverage report row shows news coverage for KRX 093370.
  key_facts:
    - code=093370
    - name=후성
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 093370
    - 후성
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

# KRX 093370 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=093370`
- `name=후성`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 093370.

## RSS Item Metadata
- Title: `경상국립대, 한·미 반려견 후성유전학적 노화 차이 규명 - 쿠키뉴스`
- Source: `쿠키뉴스`
- Published at: `2026-07-14T17:49:53+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE1rY2Y5aDdqN28taU1QdVlaaEZXSURiYm1LQVgydHFJZmdDOWtVc0FDQVRSV3NtVjJVLTBCWGljM1p2dlJVQkhCNG8yRTlLOUtOMVhnM2k0RDdSdWRWb3ZRb1o1OA?oc=5`

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
- Updated at: `2026-07-15T08:05:13+09:00`
- Company: [[KRX_093370_후성]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `경상국립대, 한·미 반려견 후성유전학적 노화 차이 규명 - 쿠키뉴스`
- Latest observation source: `쿠키뉴스`
- Latest observation published_at: `2026-07-14T17:49:53+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE1rY2Y5aDdqN28taU1QdVlaaEZXSURiYm1LQVgydHFJZmdDOWtVc0FDQVRSV3NtVjJVLTBCWGljM1p2dlJVQkhCNG8yRTlLOUtOMVhnM2k0RDdSdWRWb3ZRb1o1OA?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
