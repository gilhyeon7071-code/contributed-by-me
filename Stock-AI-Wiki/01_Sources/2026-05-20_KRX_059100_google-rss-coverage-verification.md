---
id: verification-2026-05-20-KRX-059100-google-rss-coverage
type: verification
title: KRX 059100 Google RSS Coverage Verification
created: 2026-05-20
updated: 2026-05-20
status: verification
stage: 1

market: KRX
ticker: "059100"
company: 아이컴포넌트
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=059100
    - name=아이컴포넌트
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 059100
    - 아이컴포넌트
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

# KRX 059100 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-20_KRX_059100_google-rss-coverage-source]]

## Facts Checked
- `code=059100`
- `name=아이컴포넌트`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `아이컴포넌트, +9.04% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-19T10:44:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxOMUtGQ0xraWU4UVJCV1ppWkFBMVI1QjQ2WGIzMnRyb3JpaUs2eFU5SXN1eG5LLU5lWW9PUC1seHBGNVNMMXpoSjJvNTNtaDRINVNQeHh0OHd4YU9Sa1Z0S1hFS0hveVNGMl9ITWp1RmZMVm0yV0VqU1FSZGxtZGhsdWhDY084dHRpcFNuZU50dGVObEJIc0Rn0gGXAUFVX3lxTE4xS0ZDTGtpZThRUkJXWmlaQUExUjVCNDZYYjMydHJvcmlpSzZ4VTlJc3V4bkstTmVZb09QLWx4cEY1U0wxemhKMm81M21oNEg1U1B4eHQ4d3hhT1JrVnRLWEVLSG95U0YyX0hNanVGZkxWbTJXRWpTUVJkbG1kaGx1aENjTzh0dGlwU25lTnR0ZU5sQkhzRGc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-20T16:05:04+09:00`
- Company: [[KRX_059100_아이컴포넌트]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `아이컴포넌트, +9.04% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-19T10:44:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxOMUtGQ0xraWU4UVJCV1ppWkFBMVI1QjQ2WGIzMnRyb3JpaUs2eFU5SXN1eG5LLU5lWW9PUC1seHBGNVNMMXpoSjJvNTNtaDRINVNQeHh0OHd4YU9Sa1Z0S1hFS0hveVNGMl9ITWp1RmZMVm0yV0VqU1FSZGxtZGhsdWhDY084dHRpcFNuZU50dGVObEJIc0Rn0gGXAUFVX3lxTE4xS0ZDTGtpZThRUkJXWmlaQUExUjVCNDZYYjMydHJvcmlpSzZ4VTlJc3V4bkstTmVZb09QLWx4cEY1U0wxemhKMm81M21oNEg1U1B4eHQ4d3hhT1JrVnRLWEVLSG95U0YyX0hNanVGZkxWbTJXRWpTUVJkbG1kaGx1aENjTzh0dGlwU25lTnR0ZU5sQkhzRGc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
