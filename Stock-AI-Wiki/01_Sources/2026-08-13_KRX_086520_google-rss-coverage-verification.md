---
id: verification-2026-08-13-KRX-086520-google-rss-coverage
type: verification
title: KRX 086520 Google RSS Coverage Verification
created: 2026-08-13
updated: 2026-08-13
status: verification
stage: 1

market: KRX
ticker: "086520"
company: 에코프로
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
    - code=086520
    - name=에코프로
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=24
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 086520
    - 에코프로
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

# KRX 086520 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-13_KRX_086520_google-rss-coverage-source]]

## Facts Checked
- `code=086520`
- `name=에코프로`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=24`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `에코프로비엠, 북미 매출 3년 만에 95% 증발 전망 - busan.com`
- Source: `busan.com`
- Published at: `2026-08-10T14:50:52+09:00`
- Link: `https://news.google.com/rss/articles/CBMidEFVX3lxTE1rTFFDbGtVT1dKM20wZ1JZX1F3NEdoT252RGRlMlFWUnJLR2ZRUmJwbGRvanV1bWZUZUZhNG5MYXRWaGEwX1RtUDR4MXcxdnc0RXpWSmJScTIxLW8zR2UyUmhKLWluc0x0aXJPRVFaUU1pRVNH?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-13T10:05:56+09:00`
- Company: [[KRX_086520_에코프로]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `에코프로비엠, 북미 매출 3년 만에 95% 증발 전망 - 부산일보`
- Latest observation source: `부산일보`
- Latest observation published_at: `2026-08-10T14:50:52+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMidEFVX3lxTE1rTFFDbGtVT1dKM20wZ1JZX1F3NEdoT252RGRlMlFWUnJLR2ZRUmJwbGRvanV1bWZUZUZhNG5MYXRWaGEwX1RtUDR4MXcxdnc0RXpWSmJScTIxLW8zR2UyUmhKLWluc0x0aXJPRVFaUU1pRVNH?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
