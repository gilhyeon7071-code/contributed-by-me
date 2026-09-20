---
id: verification-2026-07-15-KRX-093370-google-rss-coverage
type: verification
title: KRX 093370 Google RSS Coverage Verification
created: 2026-07-15
updated: 2026-07-15
status: verification
stage: 1

market: KRX
ticker: "093370"
company: 후성
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
    - code=093370
    - name=후성
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 093370
    - 후성
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

# KRX 093370 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-15_KRX_093370_google-rss-coverage-source]]

## Facts Checked
- `code=093370`
- `name=후성`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `경상국립대, 한·미 반려견 후성유전학적 노화 차이 규명 - 쿠키뉴스`
- Source: `쿠키뉴스`
- Published at: `2026-07-14T17:49:53+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE1rY2Y5aDdqN28taU1QdVlaaEZXSURiYm1LQVgydHFJZmdDOWtVc0FDQVRSV3NtVjJVLTBCWGljM1p2dlJVQkhCNG8yRTlLOUtOMVhnM2k0RDdSdWRWb3ZRb1o1OA?oc=5`

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
- Company: [[KRX_093370_후성]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `경상국립대, 한·미 반려견 후성유전학적 노화 차이 규명 - 쿠키뉴스`
- Latest observation source: `쿠키뉴스`
- Latest observation published_at: `2026-07-14T17:49:53+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE1rY2Y5aDdqN28taU1QdVlaaEZXSURiYm1LQVgydHFJZmdDOWtVc0FDQVRSV3NtVjJVLTBCWGljM1p2dlJVQkhCNG8yRTlLOUtOMVhnM2k0RDdSdWRWb3ZRb1o1OA?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
