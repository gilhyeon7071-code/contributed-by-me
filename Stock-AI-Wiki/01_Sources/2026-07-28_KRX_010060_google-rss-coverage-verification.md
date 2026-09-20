---
id: verification-2026-07-28-KRX-010060-google-rss-coverage
type: verification
title: KRX 010060 Google RSS Coverage Verification
created: 2026-07-28
updated: 2026-07-28
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
  collected_at: 2026-07-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=010060
    - name=OCI홀딩스
    - naver_article_count=4
    - google_rss_article_count=6
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
- [[2026-07-28_KRX_010060_google-rss-coverage-source]]

## Facts Checked
- `code=010060`
- `name=OCI홀딩스`
- `naver_article_count=4`
- `google_rss_article_count=6`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `국내는 좁고 중국산 팽배… K-태양광, 미국서 ‘돌파구’ 찾았다 - 에너지경제신문`
- Source: `에너지경제신문`
- Published at: `2026-07-28T10:06:36+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTFBwWGJINEJzSHBPVGlHcV9nblNmRHVTQkhIQjZJd3N3Q3RUZGUycXM3N1lldmIxLXBRNzZrXzQ1TDdhV3VzTTZGWWZPT2RhUTExZ21pa2tZRWtaN0ExazZwRVg0NA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-28T13:06:41+09:00`
- Company: [[KRX_010060_OCI홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `OCI홀딩스, 판로 확보 끝낸, 1.9조 증설 시동 - DealSite경제TV`
- Latest observation source: `DealSite경제TV`
- Latest observation published_at: `2026-07-28T08:00:27+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE1XeUxiVkNrMjBjYmV5QzgtVzNoRnJKNHZjLUlkcHFLZXVsTXhBMnJKLUZGendiN3AyZFUwSk9TRzJrY2RFN1NpMm9rSWJ3MERWMU53SXFB?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
