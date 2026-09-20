---
id: verification-2026-06-08-KRX-099430-google-rss-coverage
type: verification
title: KRX 099430 Google RSS Coverage Verification
created: 2026-06-08
updated: 2026-06-08
status: verification
stage: 1

market: KRX
ticker: "099430"
company: 바이오플러스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
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
    - RSS item metadata is available, but full original article body is unavailable.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: false
  numeric_values_checked: true
  date_values_checked: true
  entity_names_checked: false
  conflict_exists: false
  conflict_summary:
  confidence: unknown

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
  change_reason: generated coverage verification note
---

# KRX 099430 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-08_KRX_099430_google-rss-coverage-source]]

## Facts Checked
- `code=099430`
- `name=바이오플러스`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `바이오플러스, 명상운 부사장 영입으로 바이오의약품 경쟁력 강화 - K뉴스통신`
- Source: `K뉴스통신`
- Published at: `2026-06-05T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE9qallndzhjNXhxblhSb0U4RlFibFRkbTc4MlR1MVpaVFlwdnhTYjdXa2dRT3JycmN2V2MyUTZkSFVOM1JmWUxXUTEyUC1zX2k4YWZnYjNsNUEwWmtvckt3Ri1NRlhLZ9IBakFVX3lxTFBnXy10UEp2aEsxOTdVQ1ZMYzhkaUFtVFJNU2wwM01LT25SZE5pQlRxbHFjWjEycG5UNi0wSVFxNXplY2pIZ0RuVXhta1VMaF84Y2k3blQ2Wi16R3lIV3k1M01uVlhycXhrRXc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:19:12+09:00`
- Company: [[KRX_099430_바이오플러스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-08.json`
- Latest observation title: `바이오플러스, 명상운 부사장 영입으로 바이오의약품 경쟁력 강화 - K뉴스통신`
- Latest observation source: `K뉴스통신`
- Latest observation published_at: `2026-06-05T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE9qallndzhjNXhxblhSb0U4RlFibFRkbTc4MlR1MVpaVFlwdnhTYjdXa2dRT3JycmN2V2MyUTZkSFVOM1JmWUxXUTEyUC1zX2k4YWZnYjNsNUEwWmtvckt3Ri1NRlhLZ9IBakFVX3lxTFBnXy10UEp2aEsxOTdVQ1ZMYzhkaUFtVFJNU2wwM01LT25SZE5pQlRxbHFjWjEycG5UNi0wSVFxNXplY2pIZ0RuVXhta1VMaF84Y2k3blQ2Wi16R3lIV3k1M01uVlhycXhrRXc?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
