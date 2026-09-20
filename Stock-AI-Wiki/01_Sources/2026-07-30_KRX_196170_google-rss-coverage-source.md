---
id: source-2026-07-30-KRX-196170-google-rss-coverage
type: source
title: KRX 196170 Google RSS Coverage Source
created: 2026-07-30
updated: 2026-07-30
status: raw
stage: 0

market: KRX
ticker: "196170"
company: 알테오젠
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-30

analysis:
  summary: Local coverage report row shows news coverage for KRX 196170.
  key_facts:
    - code=196170
    - name=알테오젠
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=30
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 196170
    - 알테오젠
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

# KRX 196170 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=196170`
- `name=알테오젠`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=30`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 196170.

## RSS Item Metadata
- Title: `폭락장에도 끄떡없었다…역대급 실적에 주가 들썩이는 종목 [분석+] - 한국경제`
- Source: `한국경제`
- Published at: `2026-07-29T06:30:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE1kMnh5bDBnLU11Yy1PMjF3a0FXVFNEczc5bmFPaE0wZ2VsSlZ2WHdVcEVxQnFDSlh2X2FTN1BQdHNyZVB2VEJxdjBNb1NEMkV6b3FEZFhHTVcwZw?oc=5`

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
- Updated at: `2026-08-21T19:33:12+09:00`
- Company: [[KRX_196170_알테오젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-30.json`
- Latest observation title: `폭락장에도 끄떡없었다…역대급 실적에 주가 들썩이는 종목 [분석+] - hankyung.com`
- Latest observation source: `hankyung.com`
- Latest observation published_at: `2026-07-29T06:30:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE1kMnh5bDBnLU11Yy1PMjF3a0FXVFNEczc5bmFPaE0wZ2VsSlZ2WHdVcEVxQnFDSlh2X2FTN1BQdHNyZVB2VEJxdjBNb1NEMkV6b3FEZFhHTVcwZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
