---
id: source-2026-05-26-KRX-001740-google-rss-coverage
type: source
title: KRX 001740 Google RSS Coverage Source
created: 2026-05-26
updated: 2026-05-26
status: raw
stage: 0

market: KRX
ticker: "001740"
company: SK네트웍스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-26

analysis:
  summary: Local coverage report row shows news coverage for KRX 001740.
  key_facts:
    - code=001740
    - name=SK네트웍스
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=9
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 001740
    - SK네트웍스
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

# KRX 001740 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=001740`
- `name=SK네트웍스`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=9`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 001740.

## RSS Item Metadata
- Title: `SK네트웍스, 성장 기대·수익 불확실성 교차하는 업스테이지 투자·나무엑스 - 한국정경신문`
- Source: `한국정경신문`
- Published at: `2026-05-10T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFBMMklRR3VKbWxhaFpUcGRhaE1CbjctNVZsQzhvbk1ST3Vqd2hZc1JwLV9CcGJtWjBRYU5ialB3aWhNMjdaNmltSmJEaHJHUzV5X2c?oc=5`

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
- Updated at: `2026-08-21T19:15:28+09:00`
- Company: [[KRX_001740_SK네트웍스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-26.json`
- Latest observation title: `[특징주] SK네트웍스, 업스테이지 투자 확대에 27% 급등…장중 52주 신고가 - 아주경제`
- Latest observation source: `아주경제`
- Latest observation published_at: `2026-05-26T14:39:39+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFBwSjN2eUhQUkp5M2lTS0lETWgzdmhkSGU0RnJxVWVEVEY3Z0JZZUhiSENNcnM0VXRLMkhjOS1ReXdhMmRENTVUYU1ZaDNrRDJ1ZGRfNzZGTlRrd9IBWEFVX3lxTE9zekdyR0R4a0FsMnZpZGVNbWJhNGZsTEZkaVI1WUVSRjhaLU5HcVRtX002QWFiUkNoX1VobjNYRHN1SXRjNnk1cndPMDIxWHpJN2pfWFVkT0o?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
