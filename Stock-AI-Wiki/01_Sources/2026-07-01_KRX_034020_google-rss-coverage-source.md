---
id: source-2026-07-01-KRX-034020-google-rss-coverage
type: source
title: KRX 034020 Google RSS Coverage Source
created: 2026-07-01
updated: 2026-07-01
status: raw
stage: 0

market: KRX
ticker: "034020"
company: 두산에너빌리티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-01

analysis:
  summary: Local coverage report row shows news coverage for KRX 034020.
  key_facts:
    - code=034020
    - name=두산에너빌리티
    - naver_article_count=2
    - google_rss_article_count=10
    - kis_title_count=24
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034020
    - 두산에너빌리티
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

# KRX 034020 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=034020`
- `name=두산에너빌리티`
- `naver_article_count=2`
- `google_rss_article_count=10`
- `kis_title_count=24`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 034020.

## RSS Item Metadata
- Title: `두산에너빌리티 교섭 결렬…20년 만에 파업 위기 - KBS 뉴스`
- Source: `KBS 뉴스`
- Published at: `2026-06-27T21:46:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE8wT3lkU25pcEFYTUIyM2FmNk4yZ0VRa3V1OVgzS0RsbFl5NWdCSWZtNm9kZDM3SkstcU0zalZHWmVZVm1aSnhOSEdhWXpRbVJPLTdROXQyNnJTdmM?oc=5`

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
- Updated at: `2026-08-21T19:25:55+09:00`
- Company: [[KRX_034020_두산에너빌리티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-01.json`
- Latest observation title: `두산에너빌리티, 경제부총리 표창 수상 - 뉴스1`
- Latest observation source: `뉴스1`
- Latest observation published_at: `2026-07-01T14:52:35+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE9RZ2pIQkZkUjdCZTdhWVlreWM4VDIzQnRxLUYtYWNNaGMyLVFQYVZ1cWlJN0hTdDFxSnF3YTNhYjdOLUU1eGRUWW5MR1JIUkhJaGVLNWtwQmdkcHZXckpfRWZMRnE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
