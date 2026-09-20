---
id: source-2026-08-07-KRX-010060-google-rss-coverage
type: source
title: KRX 010060 Google RSS Coverage Source
created: 2026-08-07
updated: 2026-08-07
status: raw
stage: 0

market: KRX
ticker: "010060"
company: OCI홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-07

analysis:
  summary: Local coverage report row shows news coverage for KRX 010060.
  key_facts:
    - code=010060
    - name=OCI홀딩스
    - naver_article_count=2
    - google_rss_article_count=7
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 010060
    - OCI홀딩스
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

# KRX 010060 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=010060`
- `name=OCI홀딩스`
- `naver_article_count=2`
- `google_rss_article_count=7`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 010060.

## RSS Item Metadata
- Title: `美 태양광 관세 임박…한화솔루션·OCI, 非중국산 '반사이익' 기대 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-08-06T13:48:32+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBEZnczVkt2RmVsM09wSXdLREI3a250eVM0aXdWZ3QxS2NKYnRKUkFlZTBQTVhLeHRqcGtBdnk0bHFBeXFraks1YUY5R0dyMEtvc1ZuZkx2SmliTng3cGt0b9IBeEFVX3lxTE1tUm5qXzNSMFpLcVBvRFZpWDR0REZ0dmNQbDUzU0kzdUFGeld1VUxsNjIxZTlhd3UtVzJCbmlHMVpZSnNSMXUwMW5ubDZOZkoyaDE5YWlLcWhsamlMM05XV1p2aVlVNnJmdG1lZkNvMzQ3ZEtrTnpNUg?oc=5`

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
- Updated at: `2026-08-21T19:33:51+09:00`
- Company: [[KRX_010060_OCI홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-07.json`
- Latest observation title: `美, 수입 폴리실리콘에 15% 관세…OCI홀딩스·한화솔루션 수혜 전망 - 기후에너지데이터뱅크`
- Latest observation source: `기후에너지데이터뱅크`
- Latest observation published_at: `2026-08-07T12:10:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE5RclU1RU01ZzFlRE0wNEhZUnYyZnNMZmRLODBxSDBVbW1aS01Vc294aTF5cGFTZkI4djZGX2kxanhDV0dvdEU3aXVvSndVZ2NGUzc4ZkpKTXlCNU9Cdmc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
