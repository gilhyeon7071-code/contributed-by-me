---
id: verification-2026-05-27-KRX-452190-google-rss-coverage
type: verification
title: KRX 452190 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "452190"
company: 한빛레이저
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
    - code=452190
    - name=한빛레이저
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 452190
    - 한빛레이저
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

# KRX 452190 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_452190_google-rss-coverage-source]]

## Facts Checked
- `code=452190`
- `name=한빛레이저`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"다음 무대는 우주항공" 한빛레이저···'2단계 점프' 시작 - 헬로디디`
- Source: `헬로디디`
- Published at: `2026-04-29T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE9DMzRRX3JZQ0NOQV9nNUdDMjR4WnROcEtub1F3M3lOZE9FNjlObzBoa2tiU1JUOS1rNERmVVVXcXlMX3FubGtGY1NSazk4bjdtR2pNVUFfSnlObnFNNXRDdVRrR0t5N3k2U1E?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-27T16:05:05+09:00`
- Company: [[KRX_452190_한빛레이저]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `한빛레이저, -8.05% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-27T11:26:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxQNHIwby1Mck9odnR6dERwRTB4M2Eyc0dJN0xGc2xhOVppWm5jVDJzYVVrN1otSExNakNFZi1ZVTdQazg1QWdPQjRWM2NIY0JGV003Z1JhVW1FUW9wcExqMGMyNjJwRUtaTjdiNVYxd1l1ZmtIdjZnb1FsSVZkNkg4OEQtUdIBlwFBVV95cUxQeUltZ1dReldlemxDZHNaSGc3ajhGR1JqelI0RFFJVEtBd010a3BtcmFyZC12Nmt0Tkh3ajgwb1ZKQUZnNHl0Nl9SRV93NEpGTVlOVUdNMzg3MlNGYmpEd2Mzc3luN2kwU3R5elRteUJlWmF2ZGxRSzEwTFoxRy1FSHNueTJEOHJtWVJ5RGRQakJEeGQ4QXBF?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
