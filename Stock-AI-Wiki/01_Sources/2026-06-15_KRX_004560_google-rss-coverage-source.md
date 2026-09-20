---
id: source-2026-06-15-KRX-004560-google-rss-coverage
type: source
title: KRX 004560 Google RSS Coverage Source
created: 2026-06-15
updated: 2026-06-15
status: raw
stage: 0

market: KRX
ticker: "004560"
company: 현대비앤지스틸
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-15

analysis:
  summary: Local coverage report row shows news coverage for KRX 004560.
  key_facts:
    - code=004560
    - name=현대비앤지스틸
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 004560
    - 현대비앤지스틸
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

# KRX 004560 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=004560`
- `name=현대비앤지스틸`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 004560.

## RSS Item Metadata
- Title: `현대비앤지스틸 투자분석 2026. 06. 14 - 주달`
- Source: `주달`
- Published at: `2026-06-15T01:45:25+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9pQmZiaW9GYl92Mm1TRW1VSXpNTjdReGR6alUxZzE1Qlp0eGEwMU1TbzJSeFNGR29rWDBUUXFlNHpYLW1qeF9JY2ZIdGR6WEg2TXpGS002T0Q5S1Jqa3hIVnJQYWJTazJUZjRmWURKVkdrNEk?oc=5`

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
- Updated at: `2026-08-21T19:21:08+09:00`
- Company: [[KRX_004560_현대비앤지스틸]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-15.json`
- Latest observation title: `현대비앤지스틸 투자분석 2026. 06. 14 - 주달`
- Latest observation source: `주달`
- Latest observation published_at: `2026-06-15T01:45:25+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9pQmZiaW9GYl92Mm1TRW1VSXpNTjdReGR6alUxZzE1Qlp0eGEwMU1TbzJSeFNGR29rWDBUUXFlNHpYLW1qeF9JY2ZIdGR6WEg2TXpGS002T0Q5S1Jqa3hIVnJQYWJTazJUZjRmWURKVkdrNEk?oc=5`
- Body status: `fetch_failed_no_body`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
