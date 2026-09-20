---
id: verification-2026-05-20-KRX-453340-google-rss-coverage
type: verification
title: KRX 453340 Google RSS Coverage Verification
created: 2026-05-20
updated: 2026-05-20
status: verification
stage: 1

market: KRX
ticker: "453340"
company: 현대그린푸드
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=453340
    - name=현대그린푸드
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 453340
    - 현대그린푸드
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

# KRX 453340 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-20_KRX_453340_google-rss-coverage-source]]

## Facts Checked
- `code=453340`
- `name=현대그린푸드`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `현대그린푸드, 성남시 독거노인들에 맞춤형 영상상담 제공 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-19T09:36:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1DSXB5MzhPRzM0cUxLUWVKN0NsMmtJR3RPaWt4cVI5TXVXSkZtUF8wY1dDLW1mN1YxQnFpM2dVU1JDandDWU44Ukh3bE1IV1U?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-20T18:05:04+09:00`
- Company: [[KRX_453340_현대그린푸드]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `현대그린푸드, 성남시 독거노인들에 맞춤형 영상상담 제공 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-19T09:36:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1DSXB5MzhPRzM0cUxLUWVKN0NsMmtJR3RPaWt4cVI5TXVXSkZtUF8wY1dDLW1mN1YxQnFpM2dVU1JDandDWU44Ukh3bE1IV1U?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
