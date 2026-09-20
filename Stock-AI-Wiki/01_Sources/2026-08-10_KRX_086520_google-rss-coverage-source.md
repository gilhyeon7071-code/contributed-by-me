---
id: source-2026-08-10-KRX-086520-google-rss-coverage
type: source
title: KRX 086520 Google RSS Coverage Source
created: 2026-08-10
updated: 2026-08-10
status: raw
stage: 0

market: KRX
ticker: "086520"
company: 에코프로
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-10

analysis:
  summary: Local coverage report row shows news coverage for KRX 086520.
  key_facts:
    - code=086520
    - name=에코프로
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=11
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 086520
    - 에코프로
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

# KRX 086520 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=086520`
- `name=에코프로`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 086520.

## RSS Item Metadata
- Title: `산업부, 한화오션·에코프로비엠 등 5개사 '슈퍼 을(乙)' 선정 - 전자신문`
- Source: `전자신문`
- Published at: `2026-08-06T12:17:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMiTkFVX3lxTE5seTNPMVdZNDIxQV9raTl3MFNSM0FMeDBiUE9NX2hlYk40TTJaSFJqMUJjdThjWFptd0drWVNzYTNMc2Vqa0syS0J1T2JrZw?oc=5`

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
- Updated at: `2026-08-21T19:34:49+09:00`
- Company: [[KRX_086520_에코프로]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-10.json`
- Latest observation title: `에코프로비엠, 유상증자 정정 신고서 제출…1.2조 규모는 유지 - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-08-09T19:09:37+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE9WR0dYSW5JYldWNmdkWXR3RUUyVjhNWDdIcjRwcGJIMnc0RmhER0w5eE9oS1RUbm9sV0lRdW54WjIxUXNyQTBHdmh0cWJsQThIc2I0VEs2VmFEc0HSAWBBVV95cUxNZkdOeVc5V1dhdEFuOTBDbmZjaFpHWTBnTF9URGRpX0NvLWZDSjNlb2FfTDVKT0E2ejZWYUpsTUZNWk9ZYlQ5OFBoaDlHdkJ2QjY3WV9MWnlXQVFJN2VjQ08?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
