---
id: verification-2026-08-10-KRX-096770-google-rss-coverage
type: verification
title: KRX 096770 Google RSS Coverage Verification
created: 2026-08-10
updated: 2026-08-10
status: verification
stage: 1

market: KRX
ticker: "096770"
company: SK이노베이션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-10

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=096770
    - name=SK이노베이션
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 096770
    - SK이노베이션
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

# KRX 096770 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-10_KRX_096770_google-rss-coverage-source]]

## Facts Checked
- `code=096770`
- `name=SK이노베이션`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `SK이노베이션, ESS 건설·관리 회사 자회사로 편입 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-07-22T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE0xNjdYYzFycVk0NlVNTVZXMk1SZXo1MG1CemdkUldNcF92eWp2NHl2VWtKZ3Fxd1ZRUVpCSEJGanh1OXZqdmxCYU9fTHdfeG82UjB2dlNJc19UOXlESXF4NdIBeEFVX3lxTE9uZmlGd1dyS19mSkxfN3FKR1g0RGh3Y0dLdFVSbU5paVdiZEpMZ1J0dUJseWVJeUJzTDdoZjdSSDRoenVXWFNZT2tZOC1wYmVycU0xaW5GeFhFS09NczRrU1JpdGV0ZzdKVEFzVFZhdWVlZ2VwakNUTQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:34:49+09:00`
- Company: [[KRX_096770_SK이노베이션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-10.json`
- Latest observation title: `SK이노베이션 울산CLX, 폭염 속 협력사 노동자에 간식 지원 - 2news.co.kr`
- Latest observation source: `2news.co.kr`
- Latest observation published_at: `2026-08-10T11:16:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9hVkxhb041TnEyTTNFT0p4ZDdHOFJxNE5uWDQzUHdTc0ZtTEpKOUN2ZFRvOXM2bFhpbEJnZ2Vha1FwTUxZaTBwbnZwWVVYNFNaNHhWYV84b0xoT2Q2a3VWekxiWUIySTk2?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
