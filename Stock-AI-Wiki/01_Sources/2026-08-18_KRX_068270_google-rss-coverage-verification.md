---
id: verification-2026-08-18-KRX-068270-google-rss-coverage
type: verification
title: KRX 068270 Google RSS Coverage Verification
created: 2026-08-18
updated: 2026-08-18
status: verification
stage: 1

market: KRX
ticker: "068270"
company: 셀트리온
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=068270
    - name=셀트리온
    - naver_article_count=1
    - google_rss_article_count=82
    - kis_title_count=25
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 068270
    - 셀트리온
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

# KRX 068270 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-18_KRX_068270_google-rss-coverage-source]]

## Facts Checked
- `code=068270`
- `name=셀트리온`
- `naver_article_count=1`
- `google_rss_article_count=82`
- `kis_title_count=25`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `셀트리온, '지프레' 인수 후 정비 완료…"약국 네트워크로 유럽 확장" - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-08-18T08:26:01+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE0tWHJjMjFwRklrMEIyWDZhSEVlTDNhM25FdnQyRzlLQnpJQjZ3VERqOVJVaGZ5NlhjQ1BLXzg3SUNHdW9meWdmdFZPYUo3czlaRERGTEhyeXZJUlFRR1RuOFNJc1ZLS1dHVHfSAW9BVV95cUxOS0pJcXJkZVRQSjhUMWtQR3BucG9mRnZvWE9aWHBiUklnVGlfNUZ2aUFwTktGb3hWYnVSRkZuU1FES1NYZjd6SEFnRzlzTDc2YzRFd051Nk9PdHZ4aHhDbUMxaFFJbENmUS1HVHRfYzg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:51+09:00`
- Company: [[KRX_068270_셀트리온]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-18.json`
- Latest observation title: `셀트리온, 프랑스 '지프레' 인수 후 정비 완료 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-08-18T14:35:07+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFBBd1pnUk1Wamw2UzlaeDl6V19xUG1jLVFLdGR1REVaMWdCeElId3ZQT3RkNU1ENk1CZkNWMkl6N2hpVXFHWGFXRU13Qm5pMzQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
