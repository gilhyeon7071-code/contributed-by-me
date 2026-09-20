---
id: source-2026-08-06-KRX-240810-google-rss-coverage
type: source
title: KRX 240810 Google RSS Coverage Source
created: 2026-08-06
updated: 2026-08-06
status: raw
stage: 0

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
  summary: Local coverage report row shows news coverage for KRX 240810.
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
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 240810 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=240810`
- `name=원익IPS`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 240810.

## RSS Item Metadata
- Title: `원익IPS 주가, 8월 5일 97,700원 1.24% 상승 마감 - 톱스타뉴스`
- Source: `톱스타뉴스`
- Published at: `2026-08-05T15:40:37+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTE5HVG5LM19tbjFEUWExSHdweGZScWJoc2RMazVpa2J2OFh0Wlc3WTVqWFR4UEFoVEhIT3JRalpsVU9lVmJMS2k4SjdpMjdnNGpuekF4dEFRLXlJTUVCVE1qb2RJbUJzQWJ1cjdZVzE2MUhJdw?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

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
