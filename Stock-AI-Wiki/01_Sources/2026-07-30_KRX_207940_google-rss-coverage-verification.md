---
id: verification-2026-07-30-KRX-207940-google-rss-coverage
type: verification
title: KRX 207940 Google RSS Coverage Verification
created: 2026-07-30
updated: 2026-07-30
status: verification
stage: 1

market: KRX
ticker: "207940"
company: 삼성바이오로직스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-30

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=207940
    - name=삼성바이오로직스
    - naver_article_count=1
    - google_rss_article_count=18
    - kis_title_count=22
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 207940
    - 삼성바이오로직스
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

# KRX 207940 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-30_KRX_207940_google-rss-coverage-source]]

## Facts Checked
- `code=207940`
- `name=삼성바이오로직스`
- `naver_article_count=1`
- `google_rss_article_count=18`
- `kis_title_count=22`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `셀트리온 2분기 실적, 삼성바이오로직스 제쳤다 - 대한경제`
- Source: `대한경제`
- Published at: `2026-07-28T06:00:33+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE56UGxjNnQyZDNWUXBqVThvamY0NHE0c1lYLXZqamZsanNjNGNrLVhldXZBRjc5bUVIQnpwc1hFdFdYRVNlcmxrcjV6SjlKNVJybEVLY3pBVzdLVEFRYzF4ME02d3pieHpCbHpiNmxERUxLWFU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:33:12+09:00`
- Company: [[KRX_207940_삼성바이오로직스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-30.json`
- Latest observation title: `삼성바이오·한미약품 등, 공급망 탄소배출 산정 논의 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-07-30T16:01:27+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1lUHpadWpnaDUzeUZQQmFYU2J5SDJsNUtSdzViaDdGQWc4NTBEV0dWcm9EY2d2X21idTNFVVFLSUxwMl8tdzRSWnR3YzBnYWM?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
