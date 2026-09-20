---
id: verification-2026-07-28-KRX-011070-google-rss-coverage
type: verification
title: KRX 011070 Google RSS Coverage Verification
created: 2026-07-28
updated: 2026-07-28
status: verification
stage: 1

market: KRX
ticker: "011070"
company: LG이노텍
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
    - code=011070
    - name=LG이노텍
    - naver_article_count=1
    - google_rss_article_count=19
    - kis_title_count=11
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 011070
    - LG이노텍
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

# KRX 011070 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-28_KRX_011070_google-rss-coverage-source]]

## Facts Checked
- `code=011070`
- `name=LG이노텍`
- `naver_article_count=1`
- `google_rss_article_count=19`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LG이노텍 2분기 영업익 2천458억원…상반기 매출 첫 10조 돌파(종합) - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-07-27T14:41:54+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE1uWGRuU1N6ZldONmx5dVIwVzhVaU1hWEpJLU41anI3SjZYbGhVM3U0WE84eTdEclFXSHYtNWpsTVNSYW1xN25JNDlLQlBqd2lhT1E?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-28T08:05:50+09:00`
- Company: [[KRX_011070_LG이노텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `LG이노텍, 2분기 매출 역대 최대…영업익 2458억원·전년比 2057.3%↑ - 스트레이트뉴스`
- Latest observation source: `스트레이트뉴스`
- Latest observation published_at: `2026-07-27T16:45:42+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE0yRUF1dy05SjV5Wm1lYXhfeWlzSklyN3Voa2JmWmNZZmUxLUVxbW9nYktzX0JPM0k1Ylg1QmFhaDJrTmh4QjVyc1U4YWdmclZvZklPa2RIeWZqVXhlREJJSUIyd1dCYWJ0UnNjVndvd1Q1NzDSAXdBVV95cUxQSy0yNENWSmk4OEFUWHUxN3Rvb1c5WGIwUGo2d3NXdVNWRXdtY0lOTUhUdmtDaDM2d05XbDN3VHBWUWxidWhxMmhIOHdORmVfWk9ObFA3bHRidDVLM0hKQTJvWTN6SzhHdlBiV2ZTVTkxUENZOXEwYw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
