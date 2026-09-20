---
id: source-2026-08-11-KRX-214450-google-rss-coverage
type: source
title: KRX 214450 Google RSS Coverage Source
created: 2026-08-11
updated: 2026-08-11
status: raw
stage: 0

market: KRX
ticker: "214450"
company: 파마리서치
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-11

analysis:
  summary: Local coverage report row shows news coverage for KRX 214450.
  key_facts:
    - code=214450
    - name=파마리서치
    - naver_article_count=1
    - google_rss_article_count=77
    - kis_title_count=13
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 214450
    - 파마리서치
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

# KRX 214450 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=214450`
- `name=파마리서치`
- `naver_article_count=1`
- `google_rss_article_count=77`
- `kis_title_count=13`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 214450.

## RSS Item Metadata
- Title: `의료기기 이어 화장품도…파마리서치 ‘쌍끌이 성장’ [Why 바이오] - 서울경제`
- Source: `서울경제`
- Published at: `2026-08-10T08:40:32+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE5PcDY0X1hUdmxTd09vUkZydmxuZTBGU0o3MDdLaFdiMjcxUVhySTIxVFV4b0I5cEdfZU8taWU2NnpEZUR3UXpWRVpwUVlYd0UwT2fSAVNBVV95cUxNR2Y5SzE4NWlxVC0xV2pORFRJZDhsVHlWWlRGVkJRa0xka21CVENXYmVvaXJ2UHItc2NrWU5aYWhEb3VwTGQ4UUNKbFEzUEhiby1uSQ?oc=5`

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
- Updated at: `2026-08-11T09:10:54+09:00`
- Company: [[KRX_214450_파마리서치]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-11.json`
- Latest observation title: `증권가 파마리서치 '수출' 주목…2분기 넘어 하반기도 성장 지속 - 약사공론`
- Latest observation source: `약사공론`
- Latest observation published_at: `2026-08-10T12:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTFAwR3lMT0daZlpzWTR4TmdNRGdxVFB4YmJoZW1zVUNYTjFPR1k2c2FIci0wTjVqSFFEcHFnRml0RnItazV6ckt5akFYaWNvYzZUN1k3X3hXYTVDNkJvYklWOVRkU0RvWTdNeGwtdQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
