---
id: verification-2026-05-29-KRX-064400-google-rss-coverage
type: verification
title: KRX 064400 Google RSS Coverage Verification
created: 2026-05-29
updated: 2026-05-29
status: verification
stage: 1

market: KRX
ticker: "064400"
company: LG씨엔에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=064400
    - name=LG씨엔에스
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=0
    - google_rss_covered=True
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 064400
    - LG씨엔에스
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

# KRX 064400 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-29_KRX_064400_google-rss-coverage-source]]

## Facts Checked
- `code=064400`
- `name=LG씨엔에스`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=0`
- `google_rss_covered=True`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LG씨엔에스 주가 급등세... 왜? - 금강일보`
- Source: `금강일보`
- Published at: `2026-05-29T09:08:21+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE1Hd3djeDJqLWtVZHpFQmxoUV8ydDBtc3l2UHd4dk9OdG9KZGUtMk80OVVPYTZlOEtoX3hpSVlYdkhLRThxeWhvQUlnLUVIN1VyZ3JUcG0yNThsLTJQQW9LcmU2MlAwNTE3LVE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:16:19+09:00`
- Company: [[KRX_064400_LG씨엔에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `[특징주] LG씨엔에스, AI 에이전트 솔루션 공급.. 24%대 '급등' - 글로벌이코노믹`
- Latest observation source: `글로벌이코노믹`
- Latest observation published_at: `2026-05-29T11:32:39+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiiAFBVV95cUxOR1gxbXdXV3JtMF9yOHJUT1BaWDdoVGNBTTNhLWFqYkhmZTF0TUdLbDVIcDlIY2JVbC13VllKU0dfMENFOF9ENVR6aFFJRFI2dzdVVWpPVFc5OUd4NlVoUEd5R0ZYOGM5X21QU0hyV0JtZVowYjlvN2xlempHbkNJRTJ6d3FUQ043?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
