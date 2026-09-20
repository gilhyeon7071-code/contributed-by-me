---
id: source-2026-06-06-KRX-071050-google-rss-coverage
type: source
title: KRX 071050 Google RSS Coverage Source
created: 2026-06-06
updated: 2026-06-06
status: raw
stage: 0

market: KRX
ticker: "071050"
company: 한국금융지주
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-06

analysis:
  summary: Local coverage report row shows news coverage for KRX 071050.
  key_facts:
    - code=071050
    - name=한국금융지주
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 071050
    - 한국금융지주
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

# KRX 071050 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=071050`
- `name=한국금융지주`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 071050.

## RSS Item Metadata
- Title: `특징주, 한국금융지주-증권 테마 상승세에 8.18% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-06-05T12:23:34+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE1nUXB3T0tZVF9UTDRaaFR0S0JyS2I2R0RxWlV4alMyU0R5ejM4czN5T3RMUTRMRUdsX1l1RVlUdnpCdjRhSDNiUEZuanVSX1dYdmc?oc=5`

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
- Updated at: `2026-08-21T19:18:37+09:00`
- Company: [[KRX_071050_한국금융지주]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-06.json`
- Latest observation title: `기업은행, 한국투자금융 지분 매각 재개…정부 승인 추진 - 연합인포맥스`
- Latest observation source: `연합인포맥스`
- Latest observation published_at: `2026-06-04T08:54:53+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTFBnWVBVMU1sWi00ZEtsTjdEdjJVVEt6eHJMU0cwdVV2cDVaZmlnUHVNLWRTWEs5cHA4SVhSNnJFamlHV1pKWFQ0V0VKbUMtZmRnanlTNXlqODdsamtaT0pORWw0WUlnZnlXd2l0TmdNMWw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
