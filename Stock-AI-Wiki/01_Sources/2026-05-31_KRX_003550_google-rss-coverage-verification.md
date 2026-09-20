---
id: verification-2026-05-31-KRX-003550-google-rss-coverage
type: verification
title: KRX 003550 Google RSS Coverage Verification
created: 2026-05-31
updated: 2026-05-31
status: verification
stage: 1

market: KRX
ticker: "003550"
company: LG
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-31

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=003550
    - name=LG
    - naver_article_count=48
    - google_rss_article_count=264
    - kis_title_count=50
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003550
    - LG
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

# KRX 003550 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-31_KRX_003550_google-rss-coverage-source]]

## Facts Checked
- `code=003550`
- `name=LG`
- `naver_article_count=48`
- `google_rss_article_count=264`
- `kis_title_count=50`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `'장기 저평가' 한방에 깬 LG그룹주…핵심 동력과 리스크 따져보니 - 한국경제`
- Source: `한국경제`
- Published at: `2026-05-30T08:30:08+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE5yTjdkbGZoWm84UUh5YjdKTDMxU3h2UkZCRWw0QkhGX3dXZUEwUWtSYnJZTk03Z1RIT1VUWHU4M0JKZHhIWWZlRkhvaVlISlBCeGs1ZmZJeTd0UdIBVEFVX3lxTFB3Nl80THlTVnQ2bFRIbWFfTFh6NndhNEtmTjc2WGktNEluYW1TRU9zaEFIVGkyUEVRU0RPZDBzMkZsU0JhWVlXYXNMMjhSdlpqNExzMw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:16:54+09:00`
- Company: [[KRX_003550_LG]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-31.json`
- Latest observation title: `'장기 저평가' 한방에 깬 LG그룹주…핵심 동력과 리스크 따져보니 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-05-30T08:30:08+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE5yTjdkbGZoWm84UUh5YjdKTDMxU3h2UkZCRWw0QkhGX3dXZUEwUWtSYnJZTk03Z1RIT1VUWHU4M0JKZHhIWWZlRkhvaVlISlBCeGs1ZmZJeTd0UdIBVEFVX3lxTFB3Nl80THlTVnQ2bFRIbWFfTFh6NndhNEtmTjc2WGktNEluYW1TRU9zaEFIVGkyUEVRU0RPZDBzMkZsU0JhWVlXYXNMMjhSdlpqNExzMw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
