---
id: source-2026-07-16-KRX-034020-google-rss-coverage
type: source
title: KRX 034020 Google RSS Coverage Source
created: 2026-07-16
updated: 2026-07-16
status: raw
stage: 0

market: KRX
ticker: "034020"
company: 두산에너빌리티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-16

analysis:
  summary: Local coverage report row shows news coverage for KRX 034020.
  key_facts:
    - code=034020
    - name=두산에너빌리티
    - naver_article_count=1
    - google_rss_article_count=10
    - kis_title_count=13
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034020
    - 두산에너빌리티
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 034020 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=034020`
- `name=두산에너빌리티`
- `naver_article_count=1`
- `google_rss_article_count=10`
- `kis_title_count=13`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 034020.

## RSS Item Metadata
- Title: `두산, 청정전기 위한 가스터빈·SMR 등 발전기 공급 확대 - 네이트`
- Source: `네이트`
- Published at: `2026-07-16T05:03:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiU0FVX3lxTE13clVFbDZRaE9GMFRfSy1KRG5DTmw1M1prZGdWaU9wWFRva1dVRjVnMnp1bmhYb0RVNlE2UDhydmVPVVQzUGFuT0FnR3dyRlZza0pn?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:29:40+09:00`
- Company: [[KRX_034020_두산에너빌리티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-16.json`
- Latest observation title: `두산에너빌리티, SMR 매출 2031년 6조...내년부터 매출 본격화 - 한국금융신문`
- Latest observation source: `한국금융신문`
- Latest observation published_at: `2026-07-13T14:39:51+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMifEFVX3lxTE1PRFJVbF9HamlRMGNIaWJZbHNFLVpWbDAzcWFCZ3NXekxpR3JxR3hLM3BNcU9xdm9Jc3U5WjNMWU1VZExlLTg0ZjItdFVpM3BwXzUyQ3pJUlNSbzVScUl5SWdOZ28wTloza2tQemtBVktCX0J4V21Lb3NqUUY?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
