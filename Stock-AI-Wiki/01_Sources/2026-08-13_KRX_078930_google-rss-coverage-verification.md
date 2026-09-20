---
id: verification-2026-08-13-KRX-078930-google-rss-coverage
type: verification
title: KRX 078930 Google RSS Coverage Verification
created: 2026-08-13
updated: 2026-08-13
status: verification
stage: 1

market: KRX
ticker: "078930"
company: GS
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-13

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=078930
    - name=GS
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 078930
    - GS
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

# KRX 078930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-13_KRX_078930_google-rss-coverage-source]]

## Facts Checked
- `code=078930`
- `name=GS`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `GS 그룹 AI 데이터센터 조성 속도…12일 동해 현장 점검 - 강원도민일보`
- Source: `강원도민일보`
- Published at: `2026-08-10T00:02:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTE9IcHRqcU9Wc1lrS2I5djZBVHl0U2Q3VG1reHJoZFFYT0habUwyREpaMFRiclBSX0xjSjNKeTFLV0l6alFZNzhybV9rVlNvazdRNnp3dG0wa3hQZ0FPLUZtYXNlenQ1U2s?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:35:49+09:00`
- Company: [[KRX_078930_GS]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `GS글로벌, 김 수출 본격화…K푸드 사업 영토 넓힌다 - 이투데이`
- Latest observation source: `이투데이`
- Latest observation published_at: `2026-08-13T08:40:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVEFVX3lxTE9Dc2RIZ1BHZU5tVWRTaFZ4dklmNXBnYkh5UUVSOFpweGFjMjlFMWRreVZGSnh6Y3ZPMXpKRVByQnRyb2toeVZtRlZoVkYtT0VtVWt3VA?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
