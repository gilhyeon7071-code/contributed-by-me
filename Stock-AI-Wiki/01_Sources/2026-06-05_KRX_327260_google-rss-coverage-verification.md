---
id: verification-2026-06-05-KRX-327260-google-rss-coverage
type: verification
title: KRX 327260 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "327260"
company: RF머트리얼즈
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=327260
    - name=RF머트리얼즈
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 327260
    - RF머트리얼즈
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

# KRX 327260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_327260_google-rss-coverage-source]]

## Facts Checked
- `code=327260`
- `name=RF머트리얼즈`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[여의도 클라쓰] 'RF머트리얼즈, 두산로보틱스, LS ELECTRIC' 클라쓰 올릴 종목은? - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-06-05T06:48:22+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE03S3ptd1BWNkZmTE9PZUtGZ0lOdFl4YnNMbDk2WnE2S0R1NEV3QWlGYjduUkFCUDRaTGpsVVVvbms5b2huV0xZUGRQMVBCaGt6Q2VBbXhwMzctSWZsVktlSjhUb29McjdY0gFuQVVfeXFMT0Nwa2xQaldSOFJreDNPNThuRXQ1OUs4ZFlFWjVMRC0yeXNVVHZrYWh4U2d4cGhpS2xRS25acVo2WUVuVzFlOTAwOTMxVENKZVBBNTJhaUFpMGxab0tUbWZUYWE3a3NDUW55aGhhZ3c?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-05T21:10:20+09:00`
- Company: [[KRX_327260_RF머트리얼즈]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `[여의도 클라쓰] 'RF머트리얼즈, 두산로보틱스, LS ELECTRIC' 클라쓰 올릴 종목은? - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-06-05T06:48:22+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE03S3ptd1BWNkZmTE9PZUtGZ0lOdFl4YnNMbDk2WnE2S0R1NEV3QWlGYjduUkFCUDRaTGpsVVVvbms5b2huV0xZUGRQMVBCaGt6Q2VBbXhwMzctSWZsVktlSjhUb29McjdY0gFuQVVfeXFMT0Nwa2xQaldSOFJreDNPNThuRXQ1OUs4ZFlFWjVMRC0yeXNVVHZrYWh4U2d4cGhpS2xRS25acVo2WUVuVzFlOTAwOTMxVENKZVBBNTJhaUFpMGxab0tUbWZUYWE3a3NDUW55aGhhZ3c?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
