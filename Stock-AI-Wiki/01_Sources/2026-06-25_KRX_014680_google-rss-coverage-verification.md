---
id: verification-2026-06-25-KRX-014680-google-rss-coverage
type: verification
title: KRX 014680 Google RSS Coverage Verification
created: 2026-06-25
updated: 2026-06-25
status: verification
stage: 1

market: KRX
ticker: "014680"
company: 한솔케미칼
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=014680
    - name=한솔케미칼
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 014680
    - 한솔케미칼
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

# KRX 014680 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-25_KRX_014680_google-rss-coverage-source]]

## Facts Checked
- `code=014680`
- `name=한솔케미칼`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `한솔케미칼, 600억 규모 자사주 소각..."주주가치 제고" - thecommoditiesnews.com`
- Source: `thecommoditiesnews.com`
- Published at: `2026-06-19T15:54:09+09:00`
- Link: `https://news.google.com/rss/articles/CBMid0FVX3lxTE9ySUczTTZfMVB2d0JxLW03bGRfbGVGTXVXTDYtZlZyVTZKeVBUd2ZSd19FMjNYRUZPSm56SWplSDk5QmFhcXp6U3d6blpoalRhZUFGRVFMZlEwS0pHNUJoMnBKa0p5RWVLZXJfcGJCcXNHbkZ2THQ4?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:24:18+09:00`
- Company: [[KRX_014680_한솔케미칼]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-25.json`
- Latest observation title: `“반도체 호황에 성장력 올라탄다”..한솔케미칼, AI 반도체 수혜주로 우뚝 - 한국정경신문`
- Latest observation source: `한국정경신문`
- Latest observation published_at: `2026-06-09T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFA4M1VSTjZQNGx3R2NmLXQ2Q3NRQjA2RlIyUjlUNmVmdlJvdk8yNU1PSEt0aGQ3RzhOeGJXWEdCTDZRaUNONzZBUVhfQ2htLWtpT3c?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
