---
id: source-2026-05-28-KRX-067160-google-rss-coverage
type: source
title: KRX 067160 Google RSS Coverage Source
created: 2026-05-28
updated: 2026-05-28
status: raw
stage: 0

market: KRX
ticker: "067160"
company: SOOP
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 067160.
  key_facts:
    - code=067160
    - name=SOOP
    - naver_article_count=0
    - google_rss_article_count=18
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 067160
    - SOOP
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

# KRX 067160 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=067160`
- `name=SOOP`
- `naver_article_count=0`
- `google_rss_article_count=18`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 067160.

## RSS Item Metadata
- Title: `SOOP, '감스트' 앞세워 월드컵 입중계 콘텐츠 강화 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-05-27T17:16:39+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9iS2JHc2YwS0ZWRWlJWmd3Wlp6eVRwaG5RZWRzRzd1SFIwYl9uNG1jVVROd3ZGT09WUDg3VHZJNE9NaWdvNk1rQjk1NUJYQmpBX0pLTE1QbWZmUUdHaWNHSA?oc=5`

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
- Updated at: `2026-05-28T09:05:09+09:00`
- Company: [[KRX_067160_SOOP]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `SOOP, '감스트' 앞세워 월드컵 입중계 콘텐츠 강화 - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-05-27T17:16:39+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9iS2JHc2YwS0ZWRWlJWmd3Wlp6eVRwaG5RZWRzRzd1SFIwYl9uNG1jVVROd3ZGT09WUDg3VHZJNE9NaWdvNk1rQjk1NUJYQmpBX0pLTE1QbWZmUUdHaWNHSA?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
