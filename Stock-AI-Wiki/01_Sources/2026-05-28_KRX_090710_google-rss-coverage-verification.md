---
id: verification-2026-05-28-KRX-090710-google-rss-coverage
type: verification
title: KRX 090710 Google RSS Coverage Verification
created: 2026-05-28
updated: 2026-05-28
status: verification
stage: 1

market: KRX
ticker: "090710"
company: 휴림로봇
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=090710
    - name=휴림로봇
    - naver_article_count=4
    - google_rss_article_count=7
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 090710
    - 휴림로봇
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

# KRX 090710 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-28_KRX_090710_google-rss-coverage-source]]

## Facts Checked
- `code=090710`
- `name=휴림로봇`
- `naver_article_count=4`
- `google_rss_article_count=7`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `시총 1.4조인데 매출은 37억…'로봇' 없는 휴림로봇 ‘경고등’ - 아시아타임즈`
- Source: `아시아타임즈`
- Published at: `2026-05-26T16:18:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXkFVX3lxTFAyaXFiZUwtYkVGeGdNUUVaaEVtWHJlVXl6X0RGSUVTNDNad2lyTEpCenEtdE5hUGhJSGRmTS1yakp2Uy1BTjY1b2NLa2VtR1gyWFoxYkY1eHhQZFFHRmc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-28T09:05:09+09:00`
- Company: [[KRX_090710_휴림로봇]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `시총 1.4조인데 매출은 37억…'로봇' 없는 휴림로봇 ‘경고등’ - 아시아타임즈`
- Latest observation source: `아시아타임즈`
- Latest observation published_at: `2026-05-26T16:18:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXkFVX3lxTFAyaXFiZUwtYkVGeGdNUUVaaEVtWHJlVXl6X0RGSUVTNDNad2lyTEpCenEtdE5hUGhJSGRmTS1yakp2Uy1BTjY1b2NLa2VtR1gyWFoxYkY1eHhQZFFHRmc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
