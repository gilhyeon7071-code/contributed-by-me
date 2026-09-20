---
id: verification-2026-05-26-KRX-010770-google-rss-coverage
type: verification
title: KRX 010770 Google RSS Coverage Verification
created: 2026-05-26
updated: 2026-05-26
status: verification
stage: 1

market: KRX
ticker: "010770"
company: 평화홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=010770
    - name=평화홀딩스
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 010770
    - 평화홀딩스
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

# KRX 010770 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-26_KRX_010770_google-rss-coverage-source]]

## Facts Checked
- `code=010770`
- `name=평화홀딩스`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `평화홀딩스 투자분석 2026. 05. 22 - 주달`
- Source: `주달`
- Published at: `2026-05-22T22:11:09+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE0wUlBUTUFhdlgzcU9IWF9kS1ZQS3lLOXEwWmpIdmpib29yb3VsTGV2a1o0NUc2cWNOU0pTdENVN2Zvb0pqSmU4SEJ0UjhqdnZ6MEJFaG41cmY2STJtMGVfdDR4V0ZUaWtsOHNWWjUwZWNkeWc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-26T08:05:05+09:00`
- Company: [[KRX_010770_평화홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-26.json`
- Latest observation title: `평화홀딩스 투자분석 2026. 05. 22 - 주달`
- Latest observation source: `주달`
- Latest observation published_at: `2026-05-22T22:11:09+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE0wUlBUTUFhdlgzcU9IWF9kS1ZQS3lLOXEwWmpIdmpib29yb3VsTGV2a1o0NUc2cWNOU0pTdENVN2Zvb0pqSmU4SEJ0UjhqdnZ6MEJFaG41cmY2STJtMGVfdDR4V0ZUaWtsOHNWWjUwZWNkeWc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
