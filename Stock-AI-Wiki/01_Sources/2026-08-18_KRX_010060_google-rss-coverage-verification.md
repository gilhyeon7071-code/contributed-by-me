---
id: verification-2026-08-18-KRX-010060-google-rss-coverage
type: verification
title: KRX 010060 Google RSS Coverage Verification
created: 2026-08-18
updated: 2026-08-18
status: verification
stage: 1

market: KRX
ticker: "010060"
company: OCI홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=010060
    - name=OCI홀딩스
    - naver_article_count=1
    - google_rss_article_count=24
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 010060
    - OCI홀딩스
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

# KRX 010060 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-18_KRX_010060_google-rss-coverage-source]]

## Facts Checked
- `code=010060`
- `name=OCI홀딩스`
- `naver_article_count=1`
- `google_rss_article_count=24`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `OCI홀딩스, 판로 확보, 1.9조 증설 시동 - DealSite경제TV`
- Source: `DealSite경제TV`
- Published at: `2026-07-28T08:00:27+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE1XeUxiVkNrMjBjYmV5QzgtVzNoRnJKNHZjLUlkcHFLZXVsTXhBMnJKLUZGendiN3AyZFUwSk9TRzJrY2RFN1NpMm9rSWJ3MERWMU53SXFB?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:51+09:00`
- Company: [[KRX_010060_OCI홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-18.json`
- Latest observation title: `OCI홀딩스 투자분석 2026. 08. 18 - 주달`
- Latest observation source: `주달`
- Latest observation published_at: `2026-08-18T21:34:21+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE03Szd6ckF5YU5PYVd5QWRDYzhFdU4wc2xBQjV1X1gwOE9QMTlNSlVDOVZrNXcwajBVbVd0YkJ3SG9SWV9nMEhZaVdRVngzZVJ0LUdheDAyTUJncWFTMHQ3TFprRlZ1bmM4Q3R3SnVKbTBNaGc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
