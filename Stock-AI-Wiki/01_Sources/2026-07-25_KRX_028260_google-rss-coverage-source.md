---
id: source-2026-07-25-KRX-028260-google-rss-coverage
type: source
title: KRX 028260 Google RSS Coverage Source
created: 2026-07-25
updated: 2026-07-25
status: raw
stage: 0

market: KRX
ticker: "028260"
company: 삼성물산
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 028260.
  key_facts:
    - code=028260
    - name=삼성물산
    - naver_article_count=2
    - google_rss_article_count=4
    - kis_title_count=13
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 028260
    - 삼성물산
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

# KRX 028260 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=028260`
- `name=삼성물산`
- `naver_article_count=2`
- `google_rss_article_count=4`
- `kis_title_count=13`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 028260.

## RSS Item Metadata
- Title: `HJ중공업 ‘해모로’ 아파트, 삼성물산 ‘홈닉’ 품는다 - 한국경제`
- Source: `한국경제`
- Published at: `2026-07-15T15:09:17+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE14M3FnNVBIUnRoU2tldkNPRjhzcEFUREk4dnJHWUZoWDhSbXkzMDJaNmswTXlqTTBqNnVOdjdhWlN2Zlp4UnFnMzNiTmM4cDY4V3RodzFlM3BCdw?oc=5`

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
- Updated at: `2026-08-21T19:31:34+09:00`
- Company: [[KRX_028260_삼성물산]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-25.json`
- Latest observation title: `뉴로메카, 삼성물산과 손잡고 '피지컬 AI 빌딩' 시대 연다 - 로봇신문`
- Latest observation source: `로봇신문`
- Latest observation published_at: `2026-07-22T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTFBpWHN4cnFUUDkxVzZrMmRWckNJd2JRMWNpWUdCRlJRNDNzTThJZnJHQzE2cm81TWJkTnBjVGd4eFMxVFRPY19RaWZ5a2hMWWZ6Vm5IODAtOHhHVUtKZEpFUGRwX3VTZW12NGxHYg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
