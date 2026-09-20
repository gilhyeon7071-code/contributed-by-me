---
id: verification-2026-07-28-KRX-108860-google-rss-coverage
type: verification
title: KRX 108860 Google RSS Coverage Verification
created: 2026-07-28
updated: 2026-07-28
status: verification
stage: 1

market: KRX
ticker: "108860"
company: 셀바스AI
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
    - code=108860
    - name=셀바스AI
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 108860
    - 셀바스AI
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

# KRX 108860 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-28_KRX_108860_google-rss-coverage-source]]

## Facts Checked
- `code=108860`
- `name=셀바스AI`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `듣고 말하는 AI의 독주… 셀바스AI, 전 방위 산업 영토 확장 속 퀀텀점프 채비 - 핀포인트뉴스`
- Source: `핀포인트뉴스`
- Published at: `2026-07-28T09:34:06+09:00`
- Link: `https://news.google.com/rss/articles/CBMid0FVX3lxTE1QRk9rSzVEdG81OWdHenpaWkptOWw0VkNJMmJWeDM2X3FhRGk0MUo0c256djR5R205MHBUYThYQ0MyS0ZNUlRuMXRMb3h3QjRCZU5MZVlfRk9kLWhIbkVLcExVWlhXV3g0WmYwV1pWdHdOdFFMRENv0gF3QVVfeXFMTVBGT2tLNUR0bzU5Z0d6elpaSm05bDRWQ0kyYlZ4MzZfcWFEaTQxSjRzbnp2NHlHbTkwcFRhOFhDQzJLRk1SVG4xdExveHdCNEJlTkxlWV9GT2QtaEhuRUtwTFVaWFdXeDRaZjBXWlZ0d050UUxEQ28?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-28T21:05:05+09:00`
- Company: [[KRX_108860_셀바스AI]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `듣고 말하는 AI의 독주… 셀바스AI, 전 방위 산업 영토 확장 속 퀀텀점프 채비 - 핀포인트뉴스`
- Latest observation source: `핀포인트뉴스`
- Latest observation published_at: `2026-07-28T09:24:29+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE82bjhGcGVHTFJ1WFhHR2djQ19tczJpRGVsSVZRQ2tJMUdWbHJEUHg2Rnc5LWNubnh0Q0NWQ2pYR0JfZHpuaWliTUdnMzgwRFExMTQzd096ODQ1cGRnckxVU29MZzdJa0Y3OE9vOFdHVG91XzjSAXdBVV95cUxNUEZPa0s1RHRvNTlnR3p6WlpKbTlsNFZDSTJiVngzNl9xYURpNDFKNHNuenY0eUdtOTBwVGE4WENDMktGTVJUbjF0TG94d0I0QmVOTGVZX0ZPZC1oSG5FS3BMVVpYV1d4NFpmMFdaVnR3TnRRTERDbw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
