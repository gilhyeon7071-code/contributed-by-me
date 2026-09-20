---
id: verification-2026-07-02-KRX-010120-google-rss-coverage
type: verification
title: KRX 010120 Google RSS Coverage Verification
created: 2026-07-02
updated: 2026-07-02
status: verification
stage: 1

market: KRX
ticker: "010120"
company: 엘에스일렉트릭
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-02

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=010120
    - name=엘에스일렉트릭
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=0
    - google_rss_covered=True
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 010120
    - 엘에스일렉트릭
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

# KRX 010120 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-02_KRX_010120_google-rss-coverage-source]]

## Facts Checked
- `code=010120`
- `name=엘에스일렉트릭`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=0`
- `google_rss_covered=True`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LS일렉트릭의 '두 얼굴'…전력은 '질주' 자동화는 '적자' - 딜사이트`
- Source: `딜사이트`
- Published at: `2026-06-22T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1nWWNic3hQbnhPRWZsZG1hRlBZWEY1d0JBQlVtZVNSczViQS1VbWprRkxLOWFUYmpJdmlCS3JIbVZEczk2aHRpMHBUX2hkSGc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-02T09:06:42+09:00`
- Company: [[KRX_010120_엘에스일렉트릭]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-02.json`
- Latest observation title: `LS일렉트릭의 '두 얼굴'…전력은 '질주' 자동화는 '적자' - 딜사이트`
- Latest observation source: `딜사이트`
- Latest observation published_at: `2026-06-22T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1nWWNic3hQbnhPRWZsZG1hRlBZWEY1d0JBQlVtZVNSczViQS1VbWprRkxLOWFUYmpJdmlCS3JIbVZEczk2aHRpMHBUX2hkSGc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
