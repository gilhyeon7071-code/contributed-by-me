---
id: verification-2026-07-15-KRX-196170-google-rss-coverage
type: verification
title: KRX 196170 Google RSS Coverage Verification
created: 2026-07-15
updated: 2026-07-15
status: verification
stage: 1

market: KRX
ticker: "196170"
company: 알테오젠
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=196170
    - name=알테오젠
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 196170
    - 알테오젠
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

# KRX 196170 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-15_KRX_196170_google-rss-coverage-source]]

## Facts Checked
- `code=196170`
- `name=알테오젠`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `전태연 알테오젠 대표 “삼성에피스 특허, 물질 아닌 공정 기술…ALT-B4 영향 없다” - 팜이데일리`
- Source: `팜이데일리`
- Published at: `2026-07-14T14:30:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE50Q1dXS1kxMXlPQ3RleHVkVEpEQnlMbk1jTVJOUWI0YkxyWV8yajJvREhtNHg5RVNJNU8tUHFoWUxQSW9iSW1mQlM1bHl6WkQwY2R5Qjd4SUczdTctdmZNTTlqMDVKRG9yTjFQRW9n?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-15T21:05:34+09:00`
- Company: [[KRX_196170_알테오젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `전태연 알테오젠 대표 “삼성에피스 특허, 물질 아닌 공정 기술…ALT-B4 영향 없다” - 팜이데일리`
- Latest observation source: `팜이데일리`
- Latest observation published_at: `2026-07-14T14:30:04+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMihAFBVV95cUxQZ05uaXlJOE1XeFctTXF4UHlMOGljSk5RWVJRWHZNZnBBT2Y4REhCbnV0aVVmcFJxakE2bHdTME91U25lTmFreWl6TUpBV1V2RW5xbzFEcmMyOF8xdk91TkNWc1hUVjAyejBUOHhQWW9LSzdmSDdUY3R4enJudE5sb0hnWVI?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
