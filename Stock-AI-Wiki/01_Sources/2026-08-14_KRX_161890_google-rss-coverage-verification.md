---
id: verification-2026-08-14-KRX-161890-google-rss-coverage
type: verification
title: KRX 161890 Google RSS Coverage Verification
created: 2026-08-14
updated: 2026-08-14
status: verification
stage: 1

market: KRX
ticker: "161890"
company: 한국콜마
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-14

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=161890
    - name=한국콜마
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 161890
    - 한국콜마
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

# KRX 161890 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-14_KRX_161890_google-rss-coverage-source]]

## Facts Checked
- `code=161890`
- `name=한국콜마`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `증권가, '분기 영업익 첫 1천억' 한국콜마 목표가 잇단 상향 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-08-13T09:03:49+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTFB5SlRPU1BZU0VjS2RRRVVpbmYzTGp0MXIwS2U0SE1aZE50N3oxOEdrenQydjJ4enRYdDVFMXhzZWJXSFZNdFgtTWlKUWxZTlVJNnk0b3ppR0xYYkXSAWBBVV95cUxQX0xaemxZQnhGSUlaMDIySlY3YlZXckJYOVRHQzdyTmphY3pVREVLT3BUQTNDbjdia2p5M1N5LXBKbWVMUEtWbTY2cGhDQllSeE8yakctRVBlSU9LYjdaNUE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:10+09:00`
- Company: [[KRX_161890_한국콜마]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-14.json`
- Latest observation title: `K뷰티 타고 영업익 첫 1000억…한국콜마 오너父子 보수 71억 ‘두 배’ - 천지일보`
- Latest observation source: `천지일보`
- Latest observation published_at: `2026-08-14T20:49:57+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTFBIV1NfeXluMTJld1QzYVZLQzd6U0hCOTF4OU44bUJPc2RaR1JzMFd3TEh4WXROcTE3Z2lubFNUMW8zN1VIZGh1czlaaVFiakhfSlEyTXZrcVZhZU83OUlvTlZqLVFyakdmY2c?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
