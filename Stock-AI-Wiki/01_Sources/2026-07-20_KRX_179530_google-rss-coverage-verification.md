---
id: verification-2026-07-20-KRX-179530-google-rss-coverage
type: verification
title: KRX 179530 Google RSS Coverage Verification
created: 2026-07-20
updated: 2026-07-20
status: verification
stage: 1

market: KRX
ticker: "179530"
company: 애드바이오텍
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
    - code=179530
    - name=애드바이오텍
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 179530
    - 애드바이오텍
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

# KRX 179530 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-20_KRX_179530_google-rss-coverage-source]]

## Facts Checked
- `code=179530`
- `name=애드바이오텍`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[머니게임] 원전 테마세력에 상폐 세력까지…대주주 바뀔 때마다 곳간 비어가는 애드바이오텍 - 아주경제`
- Source: `아주경제`
- Published at: `2026-07-16T16:19:07+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE1QN2V5R25KeTgySzhJay0tdjB1N1lQMldxMV9EdzNyaGlyeU1aY1dEdWJOZzNFWEc0RzNXTnBWZ0NmZDI3M3ZHOVBoZTdXZWFQc0VCazJvRWU5UdIBWEFVX3lxTE8wMzFxQ1hhRTZCa3pQTXhKY3F6VnZWTG52OTRPdHJhbDZLS2ZiMlh5cXFocENCSlo1NDhadHJOcXJrbHEybVBoYXNWUTBucy1ndHJybTdTSHk?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-20T08:33:21+09:00`
- Company: [[KRX_179530_애드바이오텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-20.json`
- Latest observation title: `[머니게임] 원전 테마세력에 상폐 세력까지…대주주 바뀔 때마다 곳간 비어가는 애드바이오텍 - 아주경제`
- Latest observation source: `아주경제`
- Latest observation published_at: `2026-07-16T16:19:07+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE1QN2V5R25KeTgySzhJay0tdjB1N1lQMldxMV9EdzNyaGlyeU1aY1dEdWJOZzNFWEc0RzNXTnBWZ0NmZDI3M3ZHOVBoZTdXZWFQc0VCazJvRWU5UdIBWEFVX3lxTE8wMzFxQ1hhRTZCa3pQTXhKY3F6VnZWTG52OTRPdHJhbDZLS2ZiMlh5cXFocENCSlo1NDhadHJOcXJrbHEybVBoYXNWUTBucy1ndHJybTdTSHk?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
