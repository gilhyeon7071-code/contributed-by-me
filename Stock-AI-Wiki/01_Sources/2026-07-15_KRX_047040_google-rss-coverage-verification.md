---
id: verification-2026-07-15-KRX-047040-google-rss-coverage
type: verification
title: KRX 047040 Google RSS Coverage Verification
created: 2026-07-15
updated: 2026-07-15
status: verification
stage: 1

market: KRX
ticker: "047040"
company: 대우건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=047040
    - name=대우건설
    - naver_article_count=1
    - google_rss_article_count=23
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 047040
    - 대우건설
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

# KRX 047040 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-15_KRX_047040_google-rss-coverage-source]]

## Facts Checked
- `code=047040`
- `name=대우건설`
- `naver_article_count=1`
- `google_rss_article_count=23`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[이번주 재개발ㆍ재건축] 대우건설, 1.4조 상도15구역 수주 ‘초읽기’ - 대한경제`
- Source: `대한경제`
- Published at: `2026-07-13T05:00:11+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1XZUxmTlczNkZrSk1jQThlemhaLW9SVHlHYjI3b0JhT0o4VlQ4S1BiaWJYajZHR1BKaU9kSzVPSkhLYnBqT2FRcjlOb1llQ1FyazhSal9DS3BkTElYWnVOZWZIME1aZ3BXX1JtTlF0bDZ2Vzg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-15T08:05:13+09:00`
- Company: [[KRX_047040_대우건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `대우건설, 주택 마진 개선세…해외 수주도 기대-IBK - 마켓인`
- Latest observation source: `마켓인`
- Latest observation published_at: `2026-07-15T07:39:09+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1Vei1MQ1hvSUxpdk15UGt3Qk9MaXI3cm1yN2hSZHZxQ1ptRzZzbnY4Sk81WDY2Q1VKUXptYXJvZjlWQXhYYy1zTjk4UVdGdXFfYzhBc083aG1Lb3ZFUm1JY1puNlpEZERNOFhMWThrMHBDam8?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
