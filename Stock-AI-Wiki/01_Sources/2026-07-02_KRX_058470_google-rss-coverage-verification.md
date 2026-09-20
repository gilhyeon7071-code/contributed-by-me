---
id: verification-2026-07-02-KRX-058470-google-rss-coverage
type: verification
title: KRX 058470 Google RSS Coverage Verification
created: 2026-07-02
updated: 2026-07-02
status: verification
stage: 1

market: KRX
ticker: "058470"
company: 리노공업
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-02

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=058470
    - name=리노공업
    - naver_article_count=5
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 058470
    - 리노공업
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is unavailable.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: false
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

# KRX 058470 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-02_KRX_058470_google-rss-coverage-source]]

## Facts Checked
- `code=058470`
- `name=리노공업`
- `naver_article_count=5`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `리노공업 이채윤 창업주, 6300억 블록딜 - 디일렉`
- Source: `디일렉`
- Published at: `2026-06-15T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTFBFSTJnUU9uTzB0Z0lRUm1yWWxzMUw0cnpqdktPR0tBR0dqMVpUR0hiMVlXbXpWVF8zUDljTkNWeWd1eEk1VHB4T1FqTE5CejkyN3FOT1l4b1NlbFJyLV9QempIYW55dw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-02T16:10:30+09:00`
- Company: [[KRX_058470_리노공업]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-02.json`
- Latest observation title: `리노공업 이채윤 창업주, 6300억 블록딜 - 디일렉`
- Latest observation source: `디일렉`
- Latest observation published_at: `2026-06-15T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTFBFSTJnUU9uTzB0Z0lRUm1yWWxzMUw0cnpqdktPR0tBR0dqMVpUR0hiMVlXbXpWVF8zUDljTkNWeWd1eEk1VHB4T1FqTE5CejkyN3FOT1l4b1NlbFJyLV9QempIYW55dw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
