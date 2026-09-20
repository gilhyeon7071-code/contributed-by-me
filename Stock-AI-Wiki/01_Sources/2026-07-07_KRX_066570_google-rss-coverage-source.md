---
id: source-2026-07-07-KRX-066570-google-rss-coverage
type: source
title: KRX 066570 Google RSS Coverage Source
created: 2026-07-07
updated: 2026-07-07
status: raw
stage: 0

market: KRX
ticker: "066570"
company: LG전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-07

analysis:
  summary: Local coverage report row shows news coverage for KRX 066570.
  key_facts:
    - code=066570
    - name=LG전자
    - naver_article_count=1
    - google_rss_article_count=89
    - kis_title_count=14
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 066570
    - LG전자
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

# KRX 066570 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=066570`
- `name=LG전자`
- `naver_article_count=1`
- `google_rss_article_count=89`
- `kis_title_count=14`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 066570.

## RSS Item Metadata
- Title: `LG전자·두산로보틱스 주가 '반토막' - 네이트`
- Source: `네이트`
- Published at: `2026-07-06T18:07:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiU0FVX3lxTE51UjNnWVBRaWo0YWRHMlpnWmpSMjFIaHoxaFVpaVFVckJ4NTJKaXpSTTI0MHNKM19pY0pfbFZuRXZBS2VKMUo0US1aTFJjOG9Ha1o4?oc=5`

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
- Updated at: `2026-08-21T19:27:28+09:00`
- Company: [[KRX_066570_LG전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-07.json`
- Latest observation title: `LG전자, 매출·영업익 모두 역대 2분기 최대...로봇·CDU 신산업 가시화 - 지디넷코리아`
- Latest observation source: `지디넷코리아`
- Latest observation published_at: `2026-07-07T12:11:26+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE1ZX3RVVkx3ckpiLU92U1dZY3dlVW1tUjFxZFl2OTlsajRNUDlTWVJwNEY1Wm56eE1MQmpMclcwSWlBd1c3a3Y1UUpWWXl3SlIxdk0tam9B?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_medical-cooperation_의료-협진]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
