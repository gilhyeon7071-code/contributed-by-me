---
id: verification-2026-08-21-KRX-124500-google-rss-coverage
type: verification
title: KRX 124500 Google RSS Coverage Verification
created: 2026-08-21
updated: 2026-08-21
status: verification
stage: 1

market: KRX
ticker: "124500"
company: 아이티센글로벌
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=124500
    - name=아이티센글로벌
    - naver_article_count=2
    - google_rss_article_count=4
    - kis_title_count=10
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 124500
    - 아이티센글로벌
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

# KRX 124500 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-21_KRX_124500_google-rss-coverage-source]]

## Facts Checked
- `code=124500`
- `name=아이티센글로벌`
- `naver_article_count=2`
- `google_rss_article_count=4`
- `kis_title_count=10`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `오후 이슈 [스테이블코인] : 더즌, 아이티센글로벌, 헥토파이낸셜, 핑거, 우리기술투자 - 파이낸셜뉴스`
- Source: `파이낸셜뉴스`
- Published at: `2026-08-20T16:27:23+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE8zcEstS2ExNEp2QVBCd1VpS2NDSWFvSnAyS1A1UmM4X2haRENTSmRvUWE4Zm9aU3FiSzRZbENrQkVONDRzZEtwb003R2xmV3lQdVY2a0twUmRxUQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T23:05:26+09:00`
- Company: [[KRX_124500_아이티센글로벌]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-21.json`
- Latest observation title: `아이티센글로벌, 디지털 금 ‘KGLD’… 아시아 대표 프로젝트 성장 목표 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-08-21T15:19:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigAFBVV95cUxNeFVnMHo4V3daQ3BCdDgzTy04ZzlIU3RKbnFTMEg3R1ZLZzJyUTdUZXpzQVFqLXRjQ0txTm9tRmlYdF9zQUFzbHAyU1lQZGRFem1vblNseDVBMC0tN0FpeTVqa09oekFqVUtWVWtfNW5KTW50RC1CRjZfZjBCcU1lWNIBlAFBVV95cUxQZUkxSUpJdHdlaDNENnJmY3ROdjc1YVd2Tkh4WWk4VGFsYWtFNVJHNjdITWFzbjlDYTVfWjNLWXNFbTFvWXdvRWVSR2Z0M2taU2dUQ2N6VXMzcFFrTEJZVnpyNjdqMzVmcm9hNHRDTnI1ckRzTlB5bjRKNG1CN1dnLVA0c2ZGWTN3bVBkVjFXRXZHUHlQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
