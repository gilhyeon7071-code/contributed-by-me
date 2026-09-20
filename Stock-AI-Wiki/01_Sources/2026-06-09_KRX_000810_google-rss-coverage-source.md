---
id: source-2026-06-09-KRX-000810-google-rss-coverage
type: source
title: KRX 000810 Google RSS Coverage Source
created: 2026-06-09
updated: 2026-06-09
status: raw
stage: 0

market: KRX
ticker: "000810"
company: 삼성화재
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-09

analysis:
  summary: Local coverage report row shows news coverage for KRX 000810.
  key_facts:
    - code=000810
    - name=삼성화재
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=2
    - google_rss_covered=False
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000810
    - 삼성화재
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

# KRX 000810 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=000810`
- `name=삼성화재`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=2`
- `google_rss_covered=False`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 000810.

## RSS Item Metadata
- Title: `늦은 만큼 "치료·보장범위" 더 넓혔다… 삼성화재, 6월 출시 ‘순.통.치’ 차별화 승부수 - 보험저널`
- Source: `보험저널`
- Published at: `2026-06-09T06:02:26+09:00`
- Link: `https://news.google.com/rss/articles/CBMib0FVX3lxTE1LdS12ZDhLaVYwcV9sdmVDMjgtQVJIdF94RlNPcnppX0JxRmxEdC0yNURyb3JVdjJlaEY5YUV5MEJwQWdwVlE2RTdlZnZyTzdpSVB6SUZDUGxaNkx3RDVfZlhaYWtIaS1iN0VTTUdadw?oc=5`

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
- Updated at: `2026-08-21T19:19:32+09:00`
- Company: [[KRX_000810_nan]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-09.json`
- Latest observation title: `2026 북중미 월드컵 실시간 경기 결과 및 일정 - BBC`
- Latest observation source: `BBC`
- Latest observation published_at: `2026-06-01T15:57:28+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTFA3T1hvYWpZUnU5Y080azVfVDR2YWllM3F6b0VjMm5WMUpYRGVSbDcwMWJ3ZzV6eGh1dHdNcVZKSVp5cmJLYkQxLW9pWUljYl9fVWVzZThobHZ4OVpn0gFiQVVfeXFMUDF2LVlWVk9xRWRkQ1hlVWZ6blFraGJqOHlpZFJSWXZfT2FaOU1XWlNzVjJkd0J5cUE2SkdMXzQxV2RSaHJWVlFBMVlJS2g2NWV6S0tsaXNRcTY3dHJSa0lQY0E?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_medical-cooperation_의료-협진]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
