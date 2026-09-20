---
id: verification-2026-05-25-KRX-187790-google-rss-coverage
type: verification
title: KRX 187790 Google RSS Coverage Verification
created: 2026-05-25
updated: 2026-05-25
status: verification
stage: 1

market: KRX
ticker: "187790"
company: 나노
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=187790
    - name=나노
    - naver_article_count=2
    - google_rss_article_count=6
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 187790
    - 나노
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

# KRX 187790 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-25_KRX_187790_google-rss-coverage-source]]

## Facts Checked
- `code=187790`
- `name=나노`
- `naver_article_count=2`
- `google_rss_article_count=6`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `이주옥·안병구, 밀양시장 토론서 나노산단·선샤인파크 공방 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-05-25T12:57:23+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTFA0NXA2cVpVcEpoX2NpSTNvOGVJRHJtT1BkWHlJX0NPWTExWmd0b19jSTRXbHZBRUFlUXMyQ18zVTB4Sld5Z2s2ODJad3RjYmduQ0xVekFCbWYtUUnSAWBBVV95cUxNVVJ2ZmJ5UUcxQU5BUkp6dDZlak0yUUZ2eE1Xb3p5UGhBNEVGOEl4RVBfOUcySXM4NWFVYzlIRk9YRGZuXzBXb2lPSDhQMEtMYzBvZS1nM0FxOWpyYlNLSzU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:10+09:00`
- Company: [[KRX_187790_나노]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `이주옥·안병구, 밀양시장 토론서 나노산단·선샤인파크 공방 - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-05-25T12:57:23+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTFA0NXA2cVpVcEpoX2NpSTNvOGVJRHJtT1BkWHlJX0NPWTExWmd0b19jSTRXbHZBRUFlUXMyQ18zVTB4Sld5Z2s2ODJad3RjYmduQ0xVekFCbWYtUUnSAWBBVV95cUxNVVJ2ZmJ5UUcxQU5BUkp6dDZlak0yUUZ2eE1Xb3p5UGhBNEVGOEl4RVBfOUcySXM4NWFVYzlIRk9YRGZuXzBXb2lPSDhQMEtMYzBvZS1nM0FxOWpyYlNLSzU?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
