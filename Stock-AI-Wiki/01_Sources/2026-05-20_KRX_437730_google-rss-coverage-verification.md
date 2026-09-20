---
id: verification-2026-05-20-KRX-437730-google-rss-coverage
type: verification
title: KRX 437730 Google RSS Coverage Verification
created: 2026-05-20
updated: 2026-05-20
status: verification
stage: 1

market: KRX
ticker: "437730"
company: 삼현
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
    - code=437730
    - name=삼현
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 437730
    - 삼현
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

# KRX 437730 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-20_KRX_437730_google-rss-coverage-source]]

## Facts Checked
- `code=437730`
- `name=삼현`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼현철강 투자분석 2026. 05. 19 - 주달`
- Source: `주달`
- Published at: `2026-05-19T17:24:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1tMml1TGltNlZ5RHFPRGlFNk03MUZ3MFRkVGcwM25DdWZtWThqVktqY3lJRWllZ2dyNXozSFpBNzNfdEMwOHhNRjQwMlh5OThXdVp2cnpic0R4bWpCTVpqeW5oazhIb1hOY0xLa2xzbjVCWnM?oc=5`

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
- Company: [[KRX_437730_삼현]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `[고래사냥] '두산로보틱스·삼현·나노팀·뉴프렉스! 내일장 고래 종목은?! - MTN 머니투데이방송`
- Latest observation source: `MTN 머니투데이방송`
- Latest observation published_at: `2026-05-19T06:14:04+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE5ISnhTeUlVVy1lWTJiLTd1V0R4eTMxb3RvT1dHVDBLb0QwRlA5MTQ2bGRpMDllZTF1YnlFLTVrNnQxVnNyUU82Vlk2LW95SnN6TUMyZ2RfdnJCc2dVMnFGZHlGQW4?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
