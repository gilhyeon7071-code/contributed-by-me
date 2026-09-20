---
id: source-2026-07-06-KRX-028260-google-rss-coverage
type: source
title: KRX 028260 Google RSS Coverage Source
created: 2026-07-06
updated: 2026-07-06
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
  collected_at: 2026-07-06

analysis:
  summary: Local coverage report row shows news coverage for KRX 028260.
  key_facts:
    - code=028260
    - name=삼성물산
    - naver_article_count=1
    - google_rss_article_count=11
    - kis_title_count=0
    - google_rss_covered=True
    - kis_title_covered=False
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
- `naver_article_count=1`
- `google_rss_article_count=11`
- `kis_title_count=0`
- `google_rss_covered=True`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 028260.

## RSS Item Metadata
- Title: `[단독] 삼성물산, 72조 규모 英 SMR 14기 건설 프로젝트 참여 신청 - 더구루`
- Source: `더구루`
- Published at: `2026-07-02T14:31:13+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE5iV2VtV3pTbFZyVmJIb1dOTDRIWjV1clF1RFR4S3hjLUJOVG5yYmxWRnBWN0wzRnhiQlZqWk1xM2VtVk45UzhHRVhrZllCU2h3VU1VOElWdWJhSld1NEE?oc=5`

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
- Updated at: `2026-08-21T19:27:09+09:00`
- Company: [[KRX_028260_삼성물산]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-06.json`
- Latest observation title: `래미안 원페를라 국제무대서 호평... 삼성물산 런던디자인 어워즈 금상 - 파이낸셜뉴스`
- Latest observation source: `파이낸셜뉴스`
- Latest observation published_at: `2026-07-06T18:20:27+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9mZWVwREtYZDJ0R3Bscnc2WFQyTFNYNGNNNE9TbWxDWnVybXpQbjNqMkJRTEJKVmNmcTR1eER1ZXRyNjJWUmJIbF9aSHF2S0syZzg2cy1yalhpQQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
