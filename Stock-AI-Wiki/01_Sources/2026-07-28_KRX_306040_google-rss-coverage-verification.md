---
id: verification-2026-07-28-KRX-306040-google-rss-coverage
type: verification
title: KRX 306040 Google RSS Coverage Verification
created: 2026-07-28
updated: 2026-07-28
status: verification
stage: 1

market: KRX
ticker: "306040"
company: 에스제이그룹
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=306040
    - name=에스제이그룹
    - naver_article_count=2
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 306040
    - 에스제이그룹
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

# KRX 306040 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-28_KRX_306040_google-rss-coverage-source]]

## Facts Checked
- `code=306040`
- `name=에스제이그룹`
- `naver_article_count=2`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `에스제이그룹, ‘LCDC’ 브랜드IP 상품화 - 패션포스트`
- Source: `패션포스트`
- Published at: `2026-07-25T07:16:50+09:00`
- Link: `https://news.google.com/rss/articles/CBMiekFVX3lxTFBaV2hVdUdwTTFjNF9rNVhHYmM4UTdvSzB6STZ5ZnhGWVF4aC1FbTZueVN1VmQxTTF1d0wxMlM3dm1hNlhMTHBfdEZfbWlzN3dRamZxWkRYRFotbEJZMTJMejFMSWNaSUlwWnc0ZEVmbG9BZmFNek45LVVn?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-28T14:06:17+09:00`
- Company: [[KRX_306040_에스제이그룹]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `에스제이그룹, ‘LCDC’ 브랜드IP 상품화 - 패션포스트`
- Latest observation source: `패션포스트`
- Latest observation published_at: `2026-07-25T07:16:50+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiekFVX3lxTFBaV2hVdUdwTTFjNF9rNVhHYmM4UTdvSzB6STZ5ZnhGWVF4aC1FbTZueVN1VmQxTTF1d0wxMlM3dm1hNlhMTHBfdEZfbWlzN3dRamZxWkRYRFotbEJZMTJMejFMSWNaSUlwWnc0ZEVmbG9BZmFNek45LVVn?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
