---
id: source-2026-08-10-KRX-017900-google-rss-coverage
type: source
title: KRX 017900 Google RSS Coverage Source
created: 2026-08-10
updated: 2026-08-10
status: raw
stage: 0

market: KRX
ticker: "017900"
company: 광전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-10

analysis:
  summary: Local coverage report row shows news coverage for KRX 017900.
  key_facts:
    - code=017900
    - name=광전자
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 017900
    - 광전자
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

# KRX 017900 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=017900`
- `name=광전자`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 017900.

## RSS Item Metadata
- Title: `[Biz-inside,China] ‘세계 광밸리’로 도약하는 中 우한...광전자정보 산업 경쟁력 ‘쑥쑥’ - 중앙일보`
- Source: `중앙일보`
- Published at: `2026-08-06T16:46:33+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE40Ql9FWk93Uzdrcml0eVRRMVNwSkwxUnF1RDVMNlRqakZTaDZyMWJpQ0JZRlRXY1UycXd2NHZqaG9tS0U4YVZtaFBhOHJjaWxGbXJDZzl3?oc=5`

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
- Company: [[KRX_017900_광전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-10.json`
- Latest observation title: `특징주, 광전자-마이크로 LED 테마 상승세에 15.24% ↑ - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-08-10T09:44:16+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE9KbGtIWHQtbHh6VHUzUTlscW84WWZOVWJHT2ZIaXRrLVZoYTk4Sm1PdVNESFdvbE1PdUFTbEdyWTBBeVFtUWNEd1V1emtZMVNJaXc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
