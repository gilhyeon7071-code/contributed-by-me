---
id: verification-2026-05-27-KRX-466100-google-rss-coverage
type: verification
title: KRX 466100 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "466100"
company: 클로봇
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=466100
    - name=클로봇
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 466100
    - 클로봇
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

# KRX 466100 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_466100_google-rss-coverage-source]]

## Facts Checked
- `code=466100`
- `name=클로봇`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `두산로지스틱스 삼킨다는 클로봇, 승자의 저주 피할 수 있을까 - 디일렉`
- Source: `디일렉`
- Published at: `2026-05-08T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE93SXNSLVpQOW5tVXE1ZDQ0WVpiQlZKN1VFRDJHODdfRGloNE9FeVZkY2hmWXRIWkV1V2JnRTlnM01ibVl3NTdVSDhGeG9LUkNZSktRdnYzYmMzTDZPVU9NcGtWLUZIQQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:44+09:00`
- Company: [[KRX_466100_클로봇]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `두산로지스틱스 삼킨다는 클로봇, 승자의 저주 피할 수 있을까 - 디일렉`
- Latest observation source: `디일렉`
- Latest observation published_at: `2026-05-08T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE93SXNSLVpQOW5tVXE1ZDQ0WVpiQlZKN1VFRDJHODdfRGloNE9FeVZkY2hmWXRIWkV1V2JnRTlnM01ibVl3NTdVSDhGeG9LUkNZSktRdnYzYmMzTDZPVU9NcGtWLUZIQQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
