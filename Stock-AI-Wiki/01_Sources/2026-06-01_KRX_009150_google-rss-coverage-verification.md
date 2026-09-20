---
id: verification-2026-06-01-KRX-009150-google-rss-coverage
type: verification
title: KRX 009150 Google RSS Coverage Verification
created: 2026-06-01
updated: 2026-06-01
status: verification
stage: 1

market: KRX
ticker: "009150"
company: 삼성전기
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
    - code=009150
    - name=삼성전기
    - naver_article_count=81
    - google_rss_article_count=161
    - kis_title_count=96
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 009150
    - 삼성전기
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

# KRX 009150 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-01_KRX_009150_google-rss-coverage-source]]

## Facts Checked
- `code=009150`
- `name=삼성전기`
- `naver_article_count=81`
- `google_rss_article_count=161`
- `kis_title_count=96`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼성전기 담은 ETF 강세…반도체주 '고공행진' 계속 - 한국경제`
- Source: `한국경제`
- Published at: `2026-05-31T18:48:32+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE1DNzFfQ3VzTXpZUnpGS2xNVVNCYlpvQ2xPOWRCdk5QUEdNMS1VN2FyS09PNDRrTU9qY3c3YzlaTTFfWFlQQU8xSWNRU192ZFJNS1c2bXctZjhrZ9IBVEFVX3lxTFBGOFZrakZNaWhSS2ZnZ0J2Vklyd3JDUEF4c2loTjY0ZEtLa0FYNkpqVHJaNDRaU3JkWlk1WC1JYmcxeHVodG91OGJGM3VtWjJKU2ZLTQ?oc=5`

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
- Company: [[KRX_009150_삼성전기]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-01.json`
- Latest observation title: `삼성전기 담은 ETF 강세…반도체주 '고공행진' 계속 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-05-31T18:48:32+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE1DNzFfQ3VzTXpZUnpGS2xNVVNCYlpvQ2xPOWRCdk5QUEdNMS1VN2FyS09PNDRrTU9qY3c3YzlaTTFfWFlQQU8xSWNRU192ZFJNS1c2bXctZjhrZ9IBVEFVX3lxTFBGOFZrakZNaWhSS2ZnZ0J2Vklyd3JDUEF4c2loTjY0ZEtLa0FYNkpqVHJaNDRaU3JkWlk1WC1JYmcxeHVodG91OGJGM3VtWjJKU2ZLTQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
