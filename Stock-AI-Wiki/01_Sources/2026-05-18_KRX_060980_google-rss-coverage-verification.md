---
id: verification-2026-05-18-KRX-060980-google-rss-coverage
type: verification
title: KRX 060980 Google RSS Coverage Verification
created: 2026-05-18
updated: 2026-05-18
status: verification
stage: 1

market: KRX
ticker: "060980"
company: HL홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=060980
    - name=HL홀딩스
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 060980
    - HL홀딩스
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

# KRX 060980 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-18_KRX_060980_google-rss-coverage-source]]

## Facts Checked
- `code=060980`
- `name=HL홀딩스`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `HL홀딩스 최대주주 정몽원, HL홀딩스 주식등의 수 1만5600주 증가…총 지분율 37.25% - 디지털투데이`
- Source: `디지털투데이`
- Published at: `2026-05-14T17:20:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1tRXAwdFBhWmtQUjBmODFMd3hPT2w2dnU1akl4QUE3M2NxbTZxQUlnSUwtd01nUnpJckM3NkdybGFTVXVFbEpNYWRXdUh4VTBjOEEyZDB4d0ZHVHdVWnJLZ1NCUlZaVlREYzNtWldzMm9TdTg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:22+09:00`
- Company: [[KRX_060980_HL홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-18.json`
- Latest observation title: `HL홀딩스 최대주주 정몽원, HL홀딩스 주식등의 수 1만5600주 증가…총 지분율 37.25% - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-05-14T17:20:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1tRXAwdFBhWmtQUjBmODFMd3hPT2w2dnU1akl4QUE3M2NxbTZxQUlnSUwtd01nUnpJckM3NkdybGFTVXVFbEpNYWRXdUh4VTBjOEEyZDB4d0ZHVHdVWnJLZ1NCUlZaVlREYzNtWldzMm9TdTg?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
