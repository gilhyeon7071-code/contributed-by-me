---
id: source-2026-07-28-KRX-003230-google-rss-coverage
type: source
title: KRX 003230 Google RSS Coverage Source
created: 2026-07-28
updated: 2026-07-28
status: raw
stage: 0

market: KRX
ticker: "003230"
company: 삼양식품
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 003230.
  key_facts:
    - code=003230
    - name=삼양식품
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 003230
    - 삼양식품
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

# KRX 003230 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=003230`
- `name=삼양식품`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 003230.

## RSS Item Metadata
- Title: `육개장·햇반 다 가격 오르는데…삼양식품은 동결하는 이유 [김연하의 킬링이슈] - 서울경제`
- Source: `서울경제`
- Published at: `2026-07-27T15:47:49+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE9sTXltVTZWT01ZSi02RkhOVDZIZXF2ZDJpNHR5R3pvcmtlWDF3djhsanZHOFdLWXBtUmx6OHdtWE0tZ3NXbFdLeEc3Z3hYdGE3WVHSAVNBVV95cUxQbUNYay1CRTEtMzJaclN0eVlXcDlWZmJtR0hoNlEycTIxQ25mbzVnRUl6WkE0dGpoaDlDWUJtclBjYmVqZFhIdkFnVTFRb1QxZVhTYw?oc=5`

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
- Updated at: `2026-07-28T13:06:41+09:00`
- Company: [[KRX_003230_삼양식품]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `육개장·햇반 다 가격 오르는데…삼양식품은 동결하는 이유 [김연하의 킬링이슈] - 서울경제`
- Latest observation source: `서울경제`
- Latest observation published_at: `2026-07-27T15:47:49+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE9sTXltVTZWT01ZSi02RkhOVDZIZXF2ZDJpNHR5R3pvcmtlWDF3djhsanZHOFdLWXBtUmx6OHdtWE0tZ3NXbFdLeEc3Z3hYdGE3WVHSAVNBVV95cUxQbUNYay1CRTEtMzJaclN0eVlXcDlWZmJtR0hoNlEycTIxQ25mbzVnRUl6WkE0dGpoaDlDWUJtclBjYmVqZFhIdkFnVTFRb1QxZVhTYw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
