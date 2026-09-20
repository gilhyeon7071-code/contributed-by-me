---
id: verification-2026-07-20-KRX-196170-google-rss-coverage
type: verification
title: KRX 196170 Google RSS Coverage Verification
created: 2026-07-20
updated: 2026-07-20
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
  collected_at: 2026-07-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=196170
    - name=알테오젠
    - naver_article_count=7
    - google_rss_article_count=18
    - kis_title_count=15
    - google_rss_covered=True
    - kis_title_covered=True
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
- [[2026-07-20_KRX_196170_google-rss-coverage-source]]

## Facts Checked
- `code=196170`
- `name=알테오젠`
- `naver_article_count=7`
- `google_rss_article_count=18`
- `kis_title_count=15`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `알테오젠, 30% 무상증자 결정...신주 상장일 8월 26일로 변경 - Investing.com 한국어`
- Source: `Investing.com 한국어`
- Published at: `2026-07-20T15:32:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFB2dnA0Zmg2ZFNRNUF0RFdPUTVzTElhLWJxN0pkaG0zb0NqWEVMOW1zWnJfTEJzaFlQejJBS1RsYlN2Nml2ZzF5V0g2RGhGa25WMlpMUHVOb3RGNmpDT0c3SnpRQUJvRzhMd3VkX21XRkxja0k?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:29:58+09:00`
- Company: [[KRX_196170_알테오젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-20.json`
- Latest observation title: `알테오젠, 30% 무상증자 결정...신주 상장일 8월 26일로 변경 - Investing.com 한국어`
- Latest observation source: `Investing.com 한국어`
- Latest observation published_at: `2026-07-20T15:32:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFB2dnA0Zmg2ZFNRNUF0RFdPUTVzTElhLWJxN0pkaG0zb0NqWEVMOW1zWnJfTEJzaFlQejJBS1RsYlN2Nml2ZzF5V0g2RGhGa25WMlpMUHVOb3RGNmpDT0c3SnpRQUJvRzhMd3VkX21XRkxja0k?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
