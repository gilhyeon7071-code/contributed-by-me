---
id: source-2026-07-01-KRX-009830-google-rss-coverage
type: source
title: KRX 009830 Google RSS Coverage Source
created: 2026-07-01
updated: 2026-07-01
status: raw
stage: 0

market: KRX
ticker: "009830"
company: 한화솔루션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-01

analysis:
  summary: Local coverage report row shows news coverage for KRX 009830.
  key_facts:
    - code=009830
    - name=한화솔루션
    - naver_article_count=2
    - google_rss_article_count=17
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 009830
    - 한화솔루션
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

# KRX 009830 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=009830`
- `name=한화솔루션`
- `naver_article_count=2`
- `google_rss_article_count=17`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 009830.

## RSS Item Metadata
- Title: `한화생명, 한화솔루션처럼 유증할까…기본자본비율 '빠듯' - 톱데일리`
- Source: `톱데일리`
- Published at: `2026-07-01T08:10:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUEFVX3lxTE4xNUhlOXVmRjMydFRXMjVLa0xDUFBUZEVna1ZlcGxjMXZacGxpUGJoNWxOLURRSzBnLUhtZWszWkNLXy1QMm1GLXV5ZVh2b1h3?oc=5`

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
- Updated at: `2026-07-01T11:05:15+09:00`
- Company: [[KRX_009830_한화솔루션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-01.json`
- Latest observation title: `중국산 견제 수혜…한화솔루션, 美 대체 불가능한 ‘소수 업체’로 우뚝 - 에너지경제신문`
- Latest observation source: `에너지경제신문`
- Latest observation published_at: `2026-07-01T09:59:55+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE9lcnplVEtVQVlqZ0VyczFrT3RWaHJoakFLR1J5TXU4UlR5SFY5RkR3ekV5ejd5LWdTNUpzZlZOZkdkd1VtbnpSUWVfN1hsMGVZTDRmZ3gxbURscFhfM05NbjRDQQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
