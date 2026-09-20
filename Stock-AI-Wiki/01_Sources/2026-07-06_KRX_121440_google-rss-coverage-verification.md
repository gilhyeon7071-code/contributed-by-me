---
id: verification-2026-07-06-KRX-121440-google-rss-coverage
type: verification
title: KRX 121440 Google RSS Coverage Verification
created: 2026-07-06
updated: 2026-07-06
status: verification
stage: 1

market: KRX
ticker: "121440"
company: 골프존홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=121440
    - name=골프존홀딩스
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 121440
    - 골프존홀딩스
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

# KRX 121440 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-06_KRX_121440_google-rss-coverage-source]]

## Facts Checked
- `code=121440`
- `name=골프존홀딩스`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `골프존홀딩스 공개매수에 거래 집중…기관 매수세 유입 - cctoday.co.kr`
- Source: `cctoday.co.kr`
- Published at: `2026-07-06T15:24:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTFBsOEJkMFd1M2dUYjlwM1pzel9ZOGc4THd1MFdKLUV3WEdsVjBPMlNJTm5ZS3NZeWQwdjFDNlp4OC1FemVaOUpqRXFVcWdFSGt1OF9mLTFBYlVKS3ZheDhJTFRiTDd4aG9KNE8wOXRR?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:27:09+09:00`
- Company: [[KRX_121440_골프존홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-06.json`
- Latest observation title: `골프존홀딩스 공개매수에 거래 집중…기관 매수세 유입 - cctoday.co.kr`
- Latest observation source: `cctoday.co.kr`
- Latest observation published_at: `2026-07-06T15:24:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTFBsOEJkMFd1M2dUYjlwM1pzel9ZOGc4THd1MFdKLUV3WEdsVjBPMlNJTm5ZS3NZeWQwdjFDNlp4OC1FemVaOUpqRXFVcWdFSGt1OF9mLTFBYlVKS3ZheDhJTFRiTDd4aG9KNE8wOXRR?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
