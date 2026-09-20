---
id: verification-2026-08-19-KRX-034020-google-rss-coverage
type: verification
title: KRX 034020 Google RSS Coverage Verification
created: 2026-08-19
updated: 2026-08-19
status: verification
stage: 1

market: KRX
ticker: "034020"
company: 두산에너빌리티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-19

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=034020
    - name=두산에너빌리티
    - naver_article_count=3
    - google_rss_article_count=1
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034020
    - 두산에너빌리티
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

# KRX 034020 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-19_KRX_034020_google-rss-coverage-source]]

## Facts Checked
- `code=034020`
- `name=두산에너빌리티`
- `naver_article_count=3`
- `google_rss_article_count=1`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `두산에너빌리티, 테라파워 ‘나트륨’ 핵심 기자재 수주…美 케머러 공급 - 인사이트N파워`
- Source: `인사이트N파워`
- Published at: `2026-08-18T16:35:46+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE0xbm9HS3UxUFZFanVGckNnR3JQeG1CZjl6Ml9SYmMzMks2UkNRQzZXYzd3T19xMVRiLVJnTFlXUzZja3BHMUtLczNOalc2cTJUNExuYk42WVN2clBSRWNxeEY5UjhFdw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:37:11+09:00`
- Company: [[KRX_034020_두산에너빌리티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-19.json`
- Latest observation title: `두산, 세계 3강 아성에 균열…美 대형 가스터빈 수주 1위 배경은 - CEO스코어데일리`
- Latest observation source: `CEO스코어데일리`
- Latest observation published_at: `2026-08-19T07:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE43b29lWDBiYlFOUDVxLU9KaV94SEVkZEVQaThtWkFJTzEzZFhxTGFZZm5SN1BCWlJWQWMxSXFRcm5XNW5iMGlrenMyTmNDUkVhZTlwV0VOUWdYRW0waWpSRnpXZlR0THBVR3pN?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
