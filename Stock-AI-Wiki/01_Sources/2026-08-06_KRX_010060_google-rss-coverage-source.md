---
id: source-2026-08-06-KRX-010060-google-rss-coverage
type: source
title: KRX 010060 Google RSS Coverage Source
created: 2026-08-06
updated: 2026-08-06
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
  collected_at: 2026-08-06

analysis:
  summary: Local coverage report row shows news coverage for KRX 010060.
  key_facts:
    - code=010060
    - name=OCI홀딩스
    - naver_article_count=2
    - google_rss_article_count=6
    - kis_title_count=7
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
- `google_rss_article_count=6`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 010060.

## RSS Item Metadata
- Title: `[클릭 e종목] OCI홀딩스, '우주향 공급망' 구체화…목표가 40만원 - 아시아경제`
- Source: `아시아경제`
- Published at: `2026-08-06T07:20:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE5WaHctNzZNQWUzLWNxVERwN2tZRFdKdWdfYndjNVZOMk9PeWI2OXNwVk8wcWJtUHNwZGJwR2FjUUFwSWxIdXUzYnNILXN3Q1VXZzI1cW9FV2cwZ1I0U2llTA?oc=5`

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
- Company: [[KRX_010060_OCI홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `트럼프, 폴리실리콘 '15% 관세' 검토…한화큐셀·OCI 영향은? - 비즈워치`
- Latest observation source: `비즈워치`
- Latest observation published_at: `2026-08-06T14:10:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE5iQWxiNFdsSm1rSVVhMnBCakgwNHhQTjMzTGJkUldORWZ6SUZKQW9WQ3hxcGpWR0lwd2s5dmFrc2VLTGNueE85c3poU3l2MncyRUNCQi1PMFlGNGJUMFo2Zzh4eS11TGZVVmkwSg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
