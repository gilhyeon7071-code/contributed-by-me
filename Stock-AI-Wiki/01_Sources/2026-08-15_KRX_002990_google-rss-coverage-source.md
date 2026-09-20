---
id: source-2026-08-15-KRX-002990-google-rss-coverage
type: source
title: KRX 002990 Google RSS Coverage Source
created: 2026-08-15
updated: 2026-08-15
status: raw
stage: 0

market: KRX
ticker: "002990"
company: 금호건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-15

analysis:
  summary: Local coverage report row shows news coverage for KRX 002990.
  key_facts:
    - code=002990
    - name=금호건설
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 002990
    - 금호건설
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

# KRX 002990 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=002990`
- `name=금호건설`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 002990.

## RSS Item Metadata
- Title: `[특징주] 호남 반도체 클러스터 속도…관련株 '금호전기·금호건설' 강세 - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-08-12T10:10:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5tLUJmLWQtbDRmQjV2YkhaWC1iVGRGTUczcWpTaHlnMTNobTZKNkdBaWhDd2tMZGJwQ2hYWU5COVN4eEk1aEcxV3Iydm5senZhWWFQSEt2dHp2dTMw?oc=5`

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
- Updated at: `2026-08-21T19:36:30+09:00`
- Company: [[KRX_002990_금호건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-15.json`
- Latest observation title: `8월 2주차 금호건설 주간 상승분 일부 반납 변동성 - 톱스타뉴스`
- Latest observation source: `톱스타뉴스`
- Latest observation published_at: `2026-08-15T12:15:23+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTFB3NGZCUXV2aDZUZU1sSWo2eEw4aHk1QXZMWno1aEI1clNmeldnV3pPR2hKaXVlQzBOSWVJMExKLXkxNTVlWkNTSGUwRW9VNU1ZVDVlbks3SUxwOUF1dFNRQ1NBdG9VOGJJaEtqeklqTDQtdw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
