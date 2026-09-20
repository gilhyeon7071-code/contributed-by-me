---
id: verification-2026-05-27-KRX-336370-google-rss-coverage
type: verification
title: KRX 336370 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "336370"
company: 솔루스첨단소재
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=336370
    - name=솔루스첨단소재
    - naver_article_count=0
    - google_rss_article_count=5
    - kis_title_count=20
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 336370
    - 솔루스첨단소재
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

# KRX 336370 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_336370_google-rss-coverage-source]]

## Facts Checked
- `code=336370`
- `name=솔루스첨단소재`
- `naver_article_count=0`
- `google_rss_article_count=5`
- `kis_title_count=20`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `SK넥실리스, 美 동박 특허 소송 승소…솔루스첨단소재 침해 인정 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-26T16:59:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1CZEFhNmVhaWI4aDNXZzBfU1Z5RW1qdUZuSERpNFdVTVM3R3lyVVRRZmJ4cEN0eEJTTnZqOW1ndm1LYTA5aDFkOG05bVBWMUk?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-27T17:05:04+09:00`
- Company: [[KRX_336370_솔루스첨단소재]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `SK넥실리스, 美 동박 특허 소송 승소…솔루스첨단소재 침해 인정 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-26T16:59:04+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1CZEFhNmVhaWI4aDNXZzBfU1Z5RW1qdUZuSERpNFdVTVM3R3lyVVRRZmJ4cEN0eEJTTnZqOW1ndm1LYTA5aDFkOG05bVBWMUk?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
