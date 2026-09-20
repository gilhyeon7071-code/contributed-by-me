---
id: verification-2026-08-06-KRX-240810-google-rss-coverage
type: verification
title: KRX 240810 Google RSS Coverage Verification
created: 2026-08-06
updated: 2026-08-06
status: verification
stage: 1

market: KRX
ticker: "240810"
company: 원익IPS
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=240810
    - name=원익IPS
    - naver_article_count=1
    - google_rss_article_count=6
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 240810
    - 원익IPS
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

# KRX 240810 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-06_KRX_240810_google-rss-coverage-source]]

## Facts Checked
- `code=240810`
- `name=원익IPS`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `원익IPS 주가, 8월 5일 97,700원 1.24% 상승 마감 - 톱스타뉴스`
- Source: `톱스타뉴스`
- Published at: `2026-08-05T15:40:37+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTE5HVG5LM19tbjFEUWExSHdweGZScWJoc2RMazVpa2J2OFh0Wlc3WTVqWFR4UEFoVEhIT3JRalpsVU9lVmJMS2k4SjdpMjdnNGpuekF4dEFRLXlJTUVCVE1qb2RJbUJzQWJ1cjdZVzE2MUhJdw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-06T16:05:15+09:00`
- Company: [[KRX_240810_원익IPS]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `원익IPS 2분기 영업이익 184억원…작년 동기 대비 49.6%↓ - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-08-06T15:41:10+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE9qaXJyZS1GR2lBcThoV1dLX3hUSnFWYnZTSHJ5WngwMnJSV3U0VFVUWHM1ZGFRTl9mdzNrSF96UDY2aHVJQUNrN0Z1S2c1TWxqbGNQYkhPVTVPdlXSAWBBVV95cUxNSXJySGZGU2FBeEZDbGZPU0h1cHEzWjE1cFk5TlhVUnBXb0NvVU9NZUtLNm9VRGZvbEd1ZkstMlBRR1VNQ2tfWE11NDdwelZ1c0ZjYm1NUjRPVUlOc1BfRFc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
