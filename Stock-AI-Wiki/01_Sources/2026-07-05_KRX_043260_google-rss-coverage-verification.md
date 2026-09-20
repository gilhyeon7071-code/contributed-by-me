---
id: verification-2026-07-05-KRX-043260-google-rss-coverage
type: verification
title: KRX 043260 Google RSS Coverage Verification
created: 2026-07-05
updated: 2026-07-05
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
  collected_at: 2026-07-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=043260
    - name=성호전자
    - naver_article_count=1
    - google_rss_article_count=14
    - kis_title_count=29
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
- [[2026-07-05_KRX_043260_google-rss-coverage-source]]

## Facts Checked
- `code=043260`
- `name=성호전자`
- `naver_article_count=1`
- `google_rss_article_count=14`
- `kis_title_count=29`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `성호전자 "최대주주 주담대 추가 담보 여력 충분…선제적 담보 10% 추가 제공" - 아시아경제`
- Source: `아시아경제`
- Published at: `2026-07-02T14:29:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE12RVEwa19GekFQVUZ0d0h6Y2stZU1LdTNYR04zSy1nWGVGOTd1Q2tDU3l6cDVsYktiNzR6Z25YUjRrcUk0R3p3djU3eEZ0Z0VNVlNvVVpkNDFjQzFHWGRxVQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:26:51+09:00`
- Company: [[KRX_043260_성호전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-05.json`
- Latest observation title: `인베스팅프로의 적정가치, 성호전자 52% 하락 예측 적중 - Investing.com 한국어`
- Latest observation source: `Investing.com 한국어`
- Latest observation published_at: `2026-07-05T20:44:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMidkFVX3lxTE1LVlE4a2pPVGMxbllDS2w2UnBVRno1VjZlRHhucl9WNFdSQ3oxQkpLMzV2RV9ONUk5V0h2ODg4aE1CbTg4aGNudHZfOW5vcEhIRlEyUDd5b3NQTUhHdVUzS0tQRVdrYzNKM1F1bG92NGJmdS1KMlE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
