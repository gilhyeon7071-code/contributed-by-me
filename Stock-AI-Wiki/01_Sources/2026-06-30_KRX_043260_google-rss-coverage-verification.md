---
id: verification-2026-06-30-KRX-043260-google-rss-coverage
type: verification
title: KRX 043260 Google RSS Coverage Verification
created: 2026-06-30
updated: 2026-06-30
status: verification
stage: 1

market: KRX
ticker: "043260"
company: 성호전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-30

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=043260
    - name=성호전자
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 043260
    - 성호전자
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

# KRX 043260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-30_KRX_043260_google-rss-coverage-source]]

## Facts Checked
- `code=043260`
- `name=성호전자`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `성호전자 "ADS테크, 엔비디아·마벨서 100억원 수주…젠슨 황이 찍은 차세대 기술" - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-06-17T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE03SGg1VDl0eklKQXRrb2xhT0ozSG1rRDg0WFJsYXJoNnhvYzgzbHpkYm5BOUduTDQtbUF1RFd5THBKQmdmZy1pYkRXSlhSSDBvY2d2ZXhwRnF0dW5iLWtVeXZ5WjVnLUYt0gFuQVVfeXFMTjY0aDVBazhnN3cyclppbEkzLWYxQWJVamNFa0RERjRVSFRMRjJ4QWJha3E3MlQycHVLa2xTRHFkaTlkdmNPcldxUm1IMXJSMnpwckE2NnJqSmx4YzRSVXRocGlLdHZFY0ZSeUFhdFE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-30T03:05:26+09:00`
- Company: [[KRX_043260_성호전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-30.json`
- Latest observation title: `성호전자 "ADS테크, 엔비디아·마벨서 100억원 수주…젠슨 황이 찍은 차세대 기술" - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-06-17T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE03SGg1VDl0eklKQXRrb2xhT0ozSG1rRDg0WFJsYXJoNnhvYzgzbHpkYm5BOUduTDQtbUF1RFd5THBKQmdmZy1pYkRXSlhSSDBvY2d2ZXhwRnF0dW5iLWtVeXZ5WjVnLUYt0gFuQVVfeXFMTjY0aDVBazhnN3cyclppbEkzLWYxQWJVamNFa0RERjRVSFRMRjJ4QWJha3E3MlQycHVLa2xTRHFkaTlkdmNPcldxUm1IMXJSMnpwckE2NnJqSmx4YzRSVXRocGlLdHZFY0ZSeUFhdFE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
