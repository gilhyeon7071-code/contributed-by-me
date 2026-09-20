---
id: source-2026-06-09-KRX-099430-google-rss-coverage
type: source
title: KRX 099430 Google RSS Coverage Source
created: 2026-06-09
updated: 2026-06-09
status: raw
stage: 0

market: KRX
ticker: "099430"
company: 바이오플러스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-09

analysis:
  summary: Local coverage report row shows news coverage for KRX 099430.
  key_facts:
    - code=099430
    - name=바이오플러스
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 099430
    - 바이오플러스
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

# KRX 099430 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=099430`
- `name=바이오플러스`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 099430.

## RSS Item Metadata
- Title: `바이오플러스, 명상운 부사장 영입으로 바이오의약품 경쟁력 강화 - K뉴스통신`
- Source: `K뉴스통신`
- Published at: `2026-06-05T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE9qallndzhjNXhxblhSb0U4RlFibFRkbTc4MlR1MVpaVFlwdnhTYjdXa2dRT3JycmN2V2MyUTZkSFVOM1JmWUxXUTEyUC1zX2k4YWZnYjNsNUEwWmtvckt3Ri1NRlhLZ9IBakFVX3lxTFBnXy10UEp2aEsxOTdVQ1ZMYzhkaUFtVFJNU2wwM01LT25SZE5pQlRxbHFjWjEycG5UNi0wSVFxNXplY2pIZ0RuVXhta1VMaF84Y2k3blQ2Wi16R3lIV3k1M01uVlhycXhrRXc?oc=5`

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
- Updated at: `2026-06-09T08:05:10+09:00`
- Company: [[KRX_099430_바이오플러스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-09.json`
- Latest observation title: `바이오플러스, 필러·화장품 넘어 바이오 소재로 사업 확대 - 팜이데일리`
- Latest observation source: `팜이데일리`
- Latest observation published_at: `2026-05-22T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTE9qQ2JuakdUMkdKLWZ1QnMyN2dZdjdfdXhaN2oyemctVGdiT19ibkNRS0Z5UDE1YW9rNWFCcHdYd1FrR1NzQklTeFdpVi1JbWlhWUZfZXNjcXFBYUlsLUNoWHRJRHdvMlc4OHV5RWln?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
