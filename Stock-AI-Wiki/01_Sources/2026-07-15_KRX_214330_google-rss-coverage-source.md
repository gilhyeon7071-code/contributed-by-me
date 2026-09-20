---
id: source-2026-07-15-KRX-214330-google-rss-coverage
type: source
title: KRX 214330 Google RSS Coverage Source
created: 2026-07-15
updated: 2026-07-15
status: raw
stage: 0

market: KRX
ticker: "214330"
company: 금호에이치티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-15

analysis:
  summary: Local coverage report row shows news coverage for KRX 214330.
  key_facts:
    - code=214330
    - name=금호에이치티
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 214330
    - 금호에이치티
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

# KRX 214330 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=214330`
- `name=금호에이치티`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 214330.

## RSS Item Metadata
- Title: `[EBN 데이터센터] 8일 상승 종목 30選…다스코·금호전기·금호에이치티 上 - ebn.co.kr`
- Source: `ebn.co.kr`
- Published at: `2026-07-08T15:55:17+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9kTFFRdlV3SUozbFVfa0MtZHAwUWFIOXhDOFduRkRKRjRrQnN3VTRTWnhEcjctMzAtN2w0MHoyNVVqdFdRVXNnWmVDNXF1S182a1J6c1NVNkFSYUswQ21nRW1IcUlNRDZv?oc=5`

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
- Updated at: `2026-07-15T21:05:34+09:00`
- Company: [[KRX_214330_금호에이치티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `전기차 관련주, '웃음꽃' 금호에이치티·신흥에스이씨·삼성SDI·LG에너지솔루션... '시들시들' 한온시스템·모베이스전자 - 현대경제신문`
- Latest observation source: `현대경제신문`
- Latest observation published_at: `2026-04-07T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTE45elBKSlF2UHFNSlNYbjktSjkwaWRQOFQxQi15c293NHlnV190Q0VNYktKZk8tSEFNbXV4N2ZWUlBGT2I2N3FMbWpFXzZXMFRncURBZmk1Nnl0akFkd0NOMkQ2Z1UwOFHSAWtBVV95cUxOUk9wa2F2a0xxY3NMc1lyRUVfTTNWNXJUZG1oa1BiTklKNFA2RHhkUmswWG1CVDNORm82YXBBSE1OR1dFeEhzdHhWSkxDcGVoUms2TGxpTGplRHh6cTBlRjVzQ19iejdRaFhrNA?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
