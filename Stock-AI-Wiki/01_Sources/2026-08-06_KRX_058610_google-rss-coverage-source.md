---
id: source-2026-08-06-KRX-058610-google-rss-coverage
type: source
title: KRX 058610 Google RSS Coverage Source
created: 2026-08-06
updated: 2026-08-06
status: raw
stage: 0

market: KRX
ticker: "058610"
company: 에스피지
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Local coverage report row shows news coverage for KRX 058610.
  key_facts:
    - code=058610
    - name=에스피지
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 058610
    - 에스피지
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

# KRX 058610 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=058610`
- `name=에스피지`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 058610.

## RSS Item Metadata
- Title: `에스피지, 로봇 액추에이터 'SDD' 9월 양산 돌입 - 지디넷코리아`
- Source: `지디넷코리아`
- Published at: `2026-07-23T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFBkWXlpZ0JVX1BxSkVWZHVXUmUxU1hvZlBEMHFsYnBULU44d3RDQ1R0c1lLWE9zOFFLell0X1pyOXVpYjFzU3BDeEFtalBYMEMwMm90cUpB?oc=5`

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
- Updated at: `2026-08-21T19:33:31+09:00`
- Company: [[KRX_058610_에스피지]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `에스피지, 로봇 액추에이터 'SDD' 9월 양산 돌입 - 지디넷코리아`
- Latest observation source: `지디넷코리아`
- Latest observation published_at: `2026-07-23T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFBkWXlpZ0JVX1BxSkVWZHVXUmUxU1hvZlBEMHFsYnBULU44d3RDQ1R0c1lLWE9zOFFLell0X1pyOXVpYjFzU3BDeEFtalBYMEMwMm90cUpB?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
