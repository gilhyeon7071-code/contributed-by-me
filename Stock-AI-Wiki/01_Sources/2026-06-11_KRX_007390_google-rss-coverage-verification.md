---
id: verification-2026-06-11-KRX-007390-google-rss-coverage
type: verification
title: KRX 007390 Google RSS Coverage Verification
created: 2026-06-11
updated: 2026-06-11
status: verification
stage: 1

market: KRX
ticker: "007390"
company: 네이처셀
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-11

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=007390
    - name=네이처셀
    - naver_article_count=1
    - google_rss_article_count=3
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 007390
    - 네이처셀
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

# KRX 007390 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-11_KRX_007390_google-rss-coverage-source]]

## Facts Checked
- `code=007390`
- `name=네이처셀`
- `naver_article_count=1`
- `google_rss_article_count=3`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `네이처셀 조인트스템, 한국 3상 기반 美 BLA 경로 검토 - 네이트`
- Source: `네이트`
- Published at: `2026-06-10T09:22:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiU0FVX3lxTE96cjlXTENoVVhNRGwwZ3EyM1pHUC14VTdSc095M2NidWxUY1l5N3ZZeVhucDEyLVFYXzRGd190SDJuWGZVaVVMNXpWRXJXZEM2Z0tB?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-11T09:05:15+09:00`
- Company: [[KRX_007390_네이처셀]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-11.json`
- Latest observation title: `네이처셀 '조인트스템' 한국 3상 3년 추적 결과, 글로벌 학술지 게재 - 히트뉴스`
- Latest observation source: `히트뉴스`
- Latest observation published_at: `2026-05-27T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE93eVJJSGE5TjdGUEJWRGF4VXI2Rnp2dGlJRC1FbGQta0VrNHNRSmItd1BnRW5tSkV6YzBLY2g1c2x3YVRJWGZ1ZmlHdGV4dU8tSUNsdDJxTFFGUlNGS1ZJc0NjYUZEMVc1a1ZB?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
