---
id: verification-2026-07-07-KRX-011070-google-rss-coverage
type: verification
title: KRX 011070 Google RSS Coverage Verification
created: 2026-07-07
updated: 2026-07-07
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
  collected_at: 2026-07-07

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=011070
    - name=LG이노텍
    - naver_article_count=1
    - google_rss_article_count=32
    - kis_title_count=25
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
- [[2026-07-07_KRX_011070_google-rss-coverage-source]]

## Facts Checked
- `code=011070`
- `name=LG이노텍`
- `naver_article_count=1`
- `google_rss_article_count=32`
- `kis_title_count=25`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `iM證 “LG이노텍, 아이폰 가격 인상 우려에도 펀더멘털 견고…목표가 110만원" - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-07-06T07:48:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiiAFBVV95cUxQMGh1a1BlakRCNHkwY2w2WHJHOEptQ2o2dEJwUjc4NUduTTU5TXY2RXBIdlhJNHVtNFRHRFVaMHZCbkItVGM5d1ByV094ajRyYlNiTEtLdndEQUJmZEx6NzFfX3c0Tlg0SGp6SW9OcjFTdTFvSWpNaDEtekwxREdXSTBOOTB1WDVF0gGcAUFVX3lxTE4zOUFZdTQtdEh3OHl6MVBackpPbExrQVVxUDhHTnZVY3c5UzBCVWZqakRvY0ZUb01xcXZCeC1neERlYUhVSDh5YzVKeE5tZWRZYTA3LXdac09xdFIxQzAzSC03T1dfNzdRUEVzdlpTRENkTE1FVmhQSUtTc01UZWFnN1F3NzRvVm1IN3cySjlYd1RaS2haOHctbGFINQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:27:28+09:00`
- Company: [[KRX_011070_LG이노텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-07.json`
- Latest observation title: `애플 쇼크 우려 과했다…LG이노텍 목표가 110만원 ‘쑥’ [오늘, 이 종목] - 매일경제`
- Latest observation source: `매일경제`
- Latest observation published_at: `2026-07-06T11:55:14+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE05WDU4cThza3k0X2RkWDRLQUN1WTNOMDBCaFV6Z2lWc0RsX1hQTWJkTUh6M0Jld3pNNk5MWVoyUjlVTFJkQVdnYy1Fb1hzZXViRHpuX2hR?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
