---
id: source-2026-07-20-KRX-004310-google-rss-coverage
type: source
title: KRX 004310 Google RSS Coverage Source
created: 2026-07-20
updated: 2026-07-20
status: raw
stage: 0

market: KRX
ticker: "004310"
company: 현대약품
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-20

analysis:
  summary: Local coverage report row shows news coverage for KRX 004310.
  key_facts:
    - code=004310
    - name=현대약품
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 004310
    - 현대약품
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

# KRX 004310 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=004310`
- `name=현대약품`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 004310.

## RSS Item Metadata
- Title: `미프진 도입 기대감…현대약품 주가 ‘62.6% 급등’ - 데일리메디`
- Source: `데일리메디`
- Published at: `2026-07-18T06:39:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMidEFVX3lxTE1KOG00ckJiSmtlNlR2NXhkNDlzMGw3UFZtbU5vMllqRVZObHBMZER5OVUtRTl0OHdHcjJYZ08xTWwyTEpwVVBqeVNoSnhjVFdUMU5nLTAybWZQUWtfM1RlQTRMN0RYWGd4b1V0QThyTVpJUnRT?oc=5`

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
- Updated at: `2026-07-20T08:33:21+09:00`
- Company: [[KRX_004310_현대약품]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-20.json`
- Latest observation title: `미프진 도입 기대감…현대약품 주가 ‘62.6% 급등’ - 데일리메디`
- Latest observation source: `데일리메디`
- Latest observation published_at: `2026-07-18T06:39:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMidEFVX3lxTE1KOG00ckJiSmtlNlR2NXhkNDlzMGw3UFZtbU5vMllqRVZObHBMZER5OVUtRTl0OHdHcjJYZ08xTWwyTEpwVVBqeVNoSnhjVFdUMU5nLTAybWZQUWtfM1RlQTRMN0RYWGd4b1V0QThyTVpJUnRT?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
