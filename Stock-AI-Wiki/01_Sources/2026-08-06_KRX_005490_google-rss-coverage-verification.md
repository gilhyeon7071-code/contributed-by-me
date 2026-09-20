---
id: verification-2026-08-06-KRX-005490-google-rss-coverage
type: verification
title: KRX 005490 Google RSS Coverage Verification
created: 2026-08-06
updated: 2026-08-06
status: verification
stage: 1

market: KRX
ticker: "005490"
company: POSCO홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005490
    - name=POSCO홀딩스
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005490
    - POSCO홀딩스
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

# KRX 005490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-06_KRX_005490_google-rss-coverage-source]]

## Facts Checked
- `code=005490`
- `name=POSCO홀딩스`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `POSCO홀딩스, 판가 인상 속 자회사 호조 '호실적'…"리튬 2단계 준공에 풀가동 주목" - 프라임경제`
- Source: `프라임경제`
- Published at: `2026-08-03T06:37:40+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTE4yTnIxaVJyRnEtTDdSbXBfN1BaazROeThHZDZvazFMSlBxZ1dCX1dLUndkbUNJMzREVTZtNW9CZThpMjNzNzctUFFleVhXQloteDVkOEFPenVQeGNoSWx0VXhqWlJ0R3RHUUhpYzNQR05KQQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-06T16:05:15+09:00`
- Company: [[KRX_005490_POSCO홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `POSCO홀딩스, 판가 인상 속 자회사 호조 '호실적'…"리튬 2단계 준공에 풀가동 주목" - 프라임경제`
- Latest observation source: `프라임경제`
- Latest observation published_at: `2026-08-03T06:37:40+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE4yTnIxaVJyRnEtTDdSbXBfN1BaazROeThHZDZvazFMSlBxZ1dCX1dLUndkbUNJMzREVTZtNW9CZThpMjNzNzctUFFleVhXQloteDVkOEFPenVQeGNoSWx0VXhqWlJ0R3RHUUhpYzNQR05KQQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
