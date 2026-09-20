---
id: verification-2026-06-16-KRX-004560-google-rss-coverage
type: verification
title: KRX 004560 Google RSS Coverage Verification
created: 2026-06-16
updated: 2026-06-16
status: verification
stage: 1

market: KRX
ticker: "004560"
company: 현대비앤지스틸
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-16

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=004560
    - name=현대비앤지스틸
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 004560
    - 현대비앤지스틸
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

# KRX 004560 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-16_KRX_004560_google-rss-coverage-source]]

## Facts Checked
- `code=004560`
- `name=현대비앤지스틸`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `현대비앤지스틸 투자분석 2026. 06. 14 - 주달`
- Source: `주달`
- Published at: `2026-06-15T01:45:25+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9pQmZiaW9GYl92Mm1TRW1VSXpNTjdReGR6alUxZzE1Qlp0eGEwMU1TbzJSeFNGR29rWDBUUXFlNHpYLW1qeF9JY2ZIdGR6WEg2TXpGS002T0Q5S1Jqa3hIVnJQYWJTazJUZjRmWURKVkdrNEk?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-16T09:05:36+09:00`
- Company: [[KRX_004560_현대비앤지스틸]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-16.json`
- Latest observation title: `[사업보고서] 현대비앤지스틸, ‘60년 신뢰’ 위기 뚫고 빛났다 - 스틸데일리`
- Latest observation source: `스틸데일리`
- Latest observation published_at: `2026-03-25T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE5mRFFKSXJwRTV2UFRqNWVNUldHSWtNMndMREVONUxJNVJ2ZFVVRnk0RGNYTnNVWnYwR2xJd2R4VTlfZzNiZmVqci1sRWs3dllnU1dhNGlzNWt6dGJFVEVZcXNlR095dTBoZzF2TFVzTXDSAXRBVV95cUxNSkZhVlhtOVRvNUVTd3Y5dDFfeUg5aXhTWkRka0t3dEkxejhoUWpaOW50bUx2aGV6NlpsU3RmSEx0WWVuNWpqM3FYX1c2OTcxSTFjUXBKRGJuMkN4WlI4SFJzclJoVTZIMTg5SHpLREdxYnE0dw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
