---
id: verification-2026-06-01-KRX-011070-google-rss-coverage
type: verification
title: KRX 011070 Google RSS Coverage Verification
created: 2026-06-01
updated: 2026-06-01
status: verification
stage: 1

market: KRX
ticker: "011070"
company: LG이노텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-01

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=011070
    - name=LG이노텍
    - naver_article_count=22
    - google_rss_article_count=48
    - kis_title_count=47
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 011070
    - LG이노텍
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

# KRX 011070 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-01_KRX_011070_google-rss-coverage-source]]

## Facts Checked
- `code=011070`
- `name=LG이노텍`
- `naver_article_count=22`
- `google_rss_article_count=48`
- `kis_title_count=47`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `귀한 몸값된 '패키지 기판'…삼성전기·LG이노텍, 높아진 눈높이 - MTN 머니투데이방송`
- Source: `MTN 머니투데이방송`
- Published at: `2026-05-31T12:04:01+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE9HOE5oZVRoRXV4TTctWDBaMlJ0Tkhxb2ItVEl4NzJNd3ZCUXE3UWl5c1lnTkJmSmU2aDJRWXVpWHZSS2cyNDB5a2pxM0NFQ24zX1ZqYnUtSjY3OHZQSGxnVDA3a3k?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-01T08:42:44+09:00`
- Company: [[KRX_011070_LG이노텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-01.json`
- Latest observation title: `귀한 몸값된 '패키지 기판'…삼성전기·LG이노텍, 높아진 눈높이 - MTN 머니투데이방송`
- Latest observation source: `MTN 머니투데이방송`
- Latest observation published_at: `2026-05-31T12:04:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE9HOE5oZVRoRXV4TTctWDBaMlJ0Tkhxb2ItVEl4NzJNd3ZCUXE3UWl5c1lnTkJmSmU2aDJRWXVpWHZSS2cyNDB5a2pxM0NFQ24zX1ZqYnUtSjY3OHZQSGxnVDA3a3k?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
