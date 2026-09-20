---
id: verification-2026-07-23-KRX-277810-google-rss-coverage
type: verification
title: KRX 277810 Google RSS Coverage Verification
created: 2026-07-23
updated: 2026-07-23
status: verification
stage: 1

market: KRX
ticker: "277810"
company: 레인보우로보틱스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-23

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=277810
    - name=레인보우로보틱스
    - naver_article_count=2
    - google_rss_article_count=41
    - kis_title_count=40
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 277810
    - 레인보우로보틱스
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

# KRX 277810 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-23_KRX_277810_google-rss-coverage-source]]

## Facts Checked
- `code=277810`
- `name=레인보우로보틱스`
- `naver_article_count=2`
- `google_rss_article_count=41`
- `kis_title_count=40`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `RX사업추진실 본격 가동한 삼성전자, 레인보우로보틱스와 '로봇 개발' 협업 나선다 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-07-21T10:26:13+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9fQ1BFOGY0cTNIdm0xVDB5bE5uUm5BLWFTMzFsaWtVaXk0VWczc09XNlMxRUk1TThvdlVHbHhWb3lSR1E0VnV2OWt3d1FPbVAtUjA0YzdLelVQNWdnUnlJSdIBeEFVX3lxTE81T3hXNkl6aW5KeGtFRXZzWG43bDhobXh0ZUZ4Wk9FZ011bzBhX2IzRVhuVDRQbk9naVFITXh5MDY1Z0pVSXVpQVozMXBYeEJBZFZYSjZqOHFVcVZycVdHaFFSNlZEdklIMFJHMzdvN0hnQWR0TkZMdg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-23T21:05:07+09:00`
- Company: [[KRX_277810_레인보우로보틱스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-23.json`
- Latest observation title: `삼성이 움직이자 레인보우로보틱스 28% 급등...이번엔 다를까 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-07-23T17:04:10+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE51NjJsNkUxcTJJaVZJT0RSVDRpZW4tUHNKNGU4cEk5V3ZtQ2tYc3VHSl9MbXRZZVVNOFFVR1NMRi1IUjUzaUxxY0VjbFAwLUhRVFFYWlEza3hNdw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
