---
id: source-2026-06-11-KRX-032830-google-rss-coverage
type: source
title: KRX 032830 Google RSS Coverage Source
created: 2026-06-11
updated: 2026-06-11
status: raw
stage: 0

market: KRX
ticker: "032830"
company: 삼성생명
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-11

analysis:
  summary: Local coverage report row shows news coverage for KRX 032830.
  key_facts:
    - code=032830
    - name=삼성생명
    - naver_article_count=2
    - google_rss_article_count=7
    - kis_title_count=19
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 032830
    - 삼성생명
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

# KRX 032830 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=032830`
- `name=삼성생명`
- `naver_article_count=2`
- `google_rss_article_count=7`
- `kis_title_count=19`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 032830.

## RSS Item Metadata
- Title: `보유 삼전지분 가치보다 시총 작은 삼성생명…리레이팅 찾아올까 - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-06-10T10:07:54+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFA5UHJIV182WUJjaTFTYmtvUS1ka0FLTVRGTkpudmltS2RMblI5c1JaOGVMcE1NWjRHblBJTTZKSUVhclpxVy1JNVNVZVNCUFhLNUE?oc=5`

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
- Updated at: `2026-08-21T19:20:10+09:00`
- Company: [[KRX_032830_nan]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-11.json`
- Latest observation title: `2026 북중미 월드컵 실시간 경기 결과 및 일정 - BBC`
- Latest observation source: `BBC`
- Latest observation published_at: `2026-06-01T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTFA3T1hvYWpZUnU5Y080azVfVDR2YWllM3F6b0VjMm5WMUpYRGVSbDcwMWJ3ZzV6eGh1dHdNcVZKSVp5cmJLYkQxLW9pWUljYl9fVWVzZThobHZ4OVpn0gFiQVVfeXFMUDF2LVlWVk9xRWRkQ1hlVWZ6blFraGJqOHlpZFJSWXZfT2FaOU1XWlNzVjJkd0J5cUE2SkdMXzQxV2RSaHJWVlFBMVlJS2g2NWV6S0tsaXNRcTY3dHJSa0lQY0E?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
