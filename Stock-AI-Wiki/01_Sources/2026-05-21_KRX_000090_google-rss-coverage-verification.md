---
id: verification-2026-05-21-KRX-000090-google-rss-coverage
type: verification
title: KRX 000090 Google RSS Coverage Verification
created: 2026-05-21
updated: 2026-05-21
status: verification
stage: 1

market: KRX
ticker: "000090"
company: 에임드바이오
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000090
    - name=에임드바이오
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=0
    - google_rss_covered=True
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 000090
    - 에임드바이오
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

# KRX 000090 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-21_KRX_000090_google-rss-coverage-source]]

## Facts Checked
- `code=000090`
- `name=에임드바이오`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=0`
- `google_rss_covered=True`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[서치 e종목] 에임드바이오, 파이프라인 3종 조기 L/O 가능성↑…주가 상향각? - 데일리인베스트`
- Source: `데일리인베스트`
- Published at: `2026-03-18T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE5uSmU4eDRNN2ZjUlRHN3hhaUd0a1FaZ3Y2d21GcjlfTUxDN2Y2R2l0Q01mcS1oVzV0MW5XN1BLeTdsNmNHNUZKOF9hNURIVEhaSFdfRkpPa2JaQWNYbEk1dDBHd1Jxa1B2QnhF0gFvQVVfeXFMT0pYckhzcEo0a0h5NDl6VVk3ZHItZVB6aTByUngyS09iUkE2QlRLTmlsM2RQaFlvY2VNTS1QbW9HNE9qRDVEQUVUcGRVQkJ3QXBZcEprM3ctMzRlbFdoUS1CYXJ3U3Z4MGpLcUJRajBV?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:51+09:00`
- Company: [[KRX_000090_에임드바이오]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `[서치 e종목] 에임드바이오, 파이프라인 3종 조기 L/O 가능성↑…주가 상향각? - 데일리인베스트`
- Latest observation source: `데일리인베스트`
- Latest observation published_at: `2026-03-18T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE5uSmU4eDRNN2ZjUlRHN3hhaUd0a1FaZ3Y2d21GcjlfTUxDN2Y2R2l0Q01mcS1oVzV0MW5XN1BLeTdsNmNHNUZKOF9hNURIVEhaSFdfRkpPa2JaQWNYbEk1dDBHd1Jxa1B2QnhF0gFvQVVfeXFMT0pYckhzcEo0a0h5NDl6VVk3ZHItZVB6aTByUngyS09iUkE2QlRLTmlsM2RQaFlvY2VNTS1QbW9HNE9qRDVEQUVUcGRVQkJ3QXBZcEprM3ctMzRlbFdoUS1CYXJ3U3Z4MGpLcUJRajBV?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
