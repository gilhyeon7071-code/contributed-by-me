---
id: verification-2026-07-09-KRX-101730-google-rss-coverage
type: verification
title: KRX 101730 Google RSS Coverage Verification
created: 2026-07-09
updated: 2026-07-09
status: verification
stage: 1

market: KRX
ticker: "101730"
company: 위메이드맥스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-09

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=101730
    - name=위메이드맥스
    - naver_article_count=3
    - google_rss_article_count=36
    - kis_title_count=21
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 101730
    - 위메이드맥스
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

# KRX 101730 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-09_KRX_101730_google-rss-coverage-source]]

## Facts Checked
- `code=101730`
- `name=위메이드맥스`
- `naver_article_count=3`
- `google_rss_article_count=36`
- `kis_title_count=21`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[업앤다운]게임주 하락…카카오게임즈↑·위메이드맥스↓ - NSP통신`
- Source: `NSP통신`
- Published at: `2026-07-08T17:53:56+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9rU2J1SFdzTFhqWThVbHY1Y1BLd0I5cWJCRjdZQ2lKd3M0OTFvNEFqaG5fYW5WWEItT1laLVRwb29oVmlzX25MQ09uOXNJMDVFOFNYOHVFYW9HZE93WlhMWQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:28:05+09:00`
- Company: [[KRX_101730_위메이드맥스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-09.json`
- Latest observation title: `[업앤다운]게임주 하락…카카오게임즈↑·위메이드맥스↓ - NSP통신`
- Latest observation source: `NSP통신`
- Latest observation published_at: `2026-07-08T17:53:56+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9rU2J1SFdzTFhqWThVbHY1Y1BLd0I5cWJCRjdZQ2lKd3M0OTFvNEFqaG5fYW5WWEItT1laLVRwb29oVmlzX25MQ09uOXNJMDVFOFNYOHVFYW9HZE93WlhMWQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
