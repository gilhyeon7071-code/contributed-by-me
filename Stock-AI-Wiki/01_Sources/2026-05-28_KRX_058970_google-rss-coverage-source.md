---
id: source-2026-05-28-KRX-058970-google-rss-coverage
type: source
title: KRX 058970 Google RSS Coverage Source
created: 2026-05-28
updated: 2026-05-28
status: raw
stage: 0

market: KRX
ticker: "058970"
company: 엠로
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 058970.
  key_facts:
    - code=058970
    - name=엠로
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 058970
    - 엠로
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

# KRX 058970 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=058970`
- `name=엠로`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 058970.

## RSS Item Metadata
- Title: `엠로, 공급망 관리 SW 글로벌 공략 박차... 가트너 주관 행사 참여 - 디일렉`
- Source: `디일렉`
- Published at: `2026-05-22T09:02:41+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTFBqdHNvSzN4ZXdaN1haenJDX1NPTHhiNFRyZVd4Q21rU2pUUDR3enZTSXdsM2hPZkFza21VNHlXUUVhOTVWTElKeUhFWS1CVEtqODVRMnFxUjk3SEgyclRSbDBNTFkwQQ?oc=5`

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
- Updated at: `2026-05-28T21:05:04+09:00`
- Company: [[KRX_058970_엠로]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `엠로, +3.25% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-27T10:41:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxOcU45d21jWmhlREhhNEczRUNyZHNIM2dUWHBvZHdOZVd3Y19MWGc5clRrNzdsbnVSLU1BQmI3NHRyVElUS08tblplSHZ6S1RnVVp0YjhCRXd1U2VFZGF4dXZVQmJrNkRYTTM4Wm5NUS1adUdkeHpCd002SEhSRkxnNngta9IBlwFBVV95cUxOc3pSQWpIYmFsRm1CaWhhMFAxSGVweXZSS1I1RFBYc3plNEQzMjFYRlc3LS1lNlY0c3dyem1ZTVBHQkNaX0l5TzdPMXR3TDc5clNpOG9hZVJxT2JDcmJndUpJbTdtTTgyRUVtRklfS1lNRGFibVNKX0hLVlFYdHpvVGo3NEhDVTNRTVZsaEZoNnhiSWNYNVhF?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
