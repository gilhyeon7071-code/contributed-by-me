---
id: verification-2026-08-21-KRX-226340-google-rss-coverage
type: verification
title: KRX 226340 Google RSS Coverage Verification
created: 2026-08-21
updated: 2026-08-21
status: verification
stage: 1

market: KRX
ticker: "226340"
company: 본느
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=226340
    - name=본느
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 226340
    - 본느
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

# KRX 226340 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-21_KRX_226340_google-rss-coverage-source]]

## Facts Checked
- `code=226340`
- `name=본느`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `주가 7배 뛴 본느의 '역설'…구원투수인 줄 알았던 구미현 M&A의 ‘자충수’① - 녹색경제신문`
- Source: `녹색경제신문`
- Published at: `2026-08-18T17:35:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE93eFVvWG5Yam40ekNKb3dXRC02b2Z5ZWtvLW1vcVUtaWp1RThXQ0VwTHgxUHdHeWJNekw1UUFrTWtpX0FQNmE4RksxSTV2OVFjUUZCdmpxbTFtSnJHcXR4OTRVNmQ3V2ZS?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T23:05:26+09:00`
- Company: [[KRX_226340_본느]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-21.json`
- Latest observation title: `'미리 점상한가' 본느 M&A…개점휴업 법인 통한 '우회 투자' - 서울경제TV`
- Latest observation source: `서울경제TV`
- Latest observation published_at: `2026-08-19T06:00:04+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTFBaNkhRYUZNdklsVHhHWTFfMlBkVjI5em9wU1pLUmZDZWpKNnp4eExpR2g2cFNjUUVSR3ZHOXJlWTZxVzdNYUk5S1lxeTV0Y2p6MjlsQVk1ZVJteWlvLWN2enRtdU0?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
