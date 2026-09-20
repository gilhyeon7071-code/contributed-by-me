---
id: verification-2026-07-23-KRX-011070-google-rss-coverage
type: verification
title: KRX 011070 Google RSS Coverage Verification
created: 2026-07-23
updated: 2026-07-23
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
  collected_at: 2026-07-23

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=011070
    - name=LG이노텍
    - naver_article_count=1
    - google_rss_article_count=11
    - kis_title_count=8
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
- [[2026-07-23_KRX_011070_google-rss-coverage-source]]

## Facts Checked
- `code=011070`
- `name=LG이노텍`
- `naver_article_count=1`
- `google_rss_article_count=11`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `외인, 반도체 소부장 쓸어담았다...순매수 1위 'LG이노텍' 전망은 - 오피니언뉴스`
- Source: `오피니언뉴스`
- Published at: `2026-07-22T13:37:29+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTE50cHZuaVc0clN1OG1HRzlfZVB6aE1Nem0wN2c3OFo5eGU4VnRJVWZHQ1VpZy04OHl1TjVUYTNlVEV1d1JwV3NSVjE5ME5hTW9mME9XZ2xoekpWeUtDQ1BScjdCU09ORUtEeXlMMnRxNnE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:30:57+09:00`
- Company: [[KRX_011070_LG이노텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-23.json`
- Latest observation title: `특징주, LG이노텍-반도체 기판(FC-BGA/PCB/MLB 등) 테마 상승세에 6.34% ↑ - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-07-23T10:16:25+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE1jQlQtWUpBVUxWbjBzVU9QS256SEhxOHhLdHJCMTdmZ0cyRjNXSjdBTDBtUzBUWFJyaF9NMERDeVBTeFRDN1FVc1dub25OYUlMUFE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
