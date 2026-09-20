---
id: verification-2026-06-22-KRX-000720-google-rss-coverage
type: verification
title: KRX 000720 Google RSS Coverage Verification
created: 2026-06-22
updated: 2026-06-22
status: verification
stage: 1

market: KRX
ticker: "000720"
company: 현대건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-22

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000720
    - name=현대건설
    - naver_article_count=2
    - google_rss_article_count=97
    - kis_title_count=9
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000720
    - 현대건설
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

# KRX 000720 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-22_KRX_000720_google-rss-coverage-source]]

## Facts Checked
- `code=000720`
- `name=현대건설`
- `naver_article_count=2`
- `google_rss_article_count=97`
- `kis_title_count=9`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `양효진 떠난 수원 현대건설, 더 빨라진다…강성형 감독의 ‘새 우승 공식’ - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-21T13:04:45+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE8ya21WdVhTRVlJeTRRTmhaLVJnclVndVhZb0dUWkpTVG5XNE52V2xDSktuaVoxNzd4TVJWX0daT2pxa0x0enRUd3d5VktBSGs?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:23:21+09:00`
- Company: [[KRX_000720_현대건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-22.json`
- Latest observation title: `현대건설 美 원전 파트너 페르미 경영 분란 계속…前 CEO, 現 경영진 압박 - 더구루`
- Latest observation source: `더구루`
- Latest observation published_at: `2026-06-22T08:36:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE9zZEZxbEhsRlplandmUk9rak1VQl9YVTA0dm4xdjEycFRXcEhGVDItQVowRVNzcGlpN2ZoS25CZkQ4eGdUMk5rcV9iRl9pYWZKdHVHU0ZwTW5uQVlQNEE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
