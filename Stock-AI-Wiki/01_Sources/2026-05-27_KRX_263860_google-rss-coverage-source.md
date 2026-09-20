---
id: source-2026-05-27-KRX-263860-google-rss-coverage
type: source
title: KRX 263860 Google RSS Coverage Source
created: 2026-05-27
updated: 2026-05-27
status: raw
stage: 0

market: KRX
ticker: "263860"
company: 지니언스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Local coverage report row shows news coverage for KRX 263860.
  key_facts:
    - code=263860
    - name=지니언스
    - naver_article_count=1
    - google_rss_article_count=33
    - kis_title_count=9
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 263860
    - 지니언스
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

# KRX 263860 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=263860`
- `name=지니언스`
- `naver_article_count=1`
- `google_rss_article_count=33`
- `kis_title_count=9`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 263860.

## RSS Item Metadata
- Title: `지니언스, 양자 보안 시장 진출…기존 암호 체계 무력화 위협 차단 - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-05-26T09:03:51+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTE11WVRkdWwtdUFDcUg1NlpHNXNscWdjWXkzS2dWNml4clJwM19fdnFHWUhhUWlxaXAxNXNwU054eEstSmNDUlYwVTdZbnBZTTcwTUZ2aGtKbGZHYUlvNTl3R1hraUtYcGvSAWxBVV95cUxNdXA1MkpYUGJjZnp5eFJBamVHZHU4dVpTa2RFeU9Tam9Bamc5MThmdkJENDE3Wmd2Z0RQNEpIYlowN1JDRlFJSVI0RDNKMGZ1OXNPanRLeEpLak12U0tDMmtDVjFQSndpdEQyOHk?oc=5`

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
- Updated at: `2026-05-27T16:05:05+09:00`
- Company: [[KRX_263860_지니언스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `[클릭 e종목]"지니언스, NAC·EDR 성장에 양자보안까지" - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-27T08:20:53+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiRkFVX3lxTE91c3Nidl85aVdNSTJnX20tbmhjRk5FN051bEtBMElBbVlEbVJQOXlUNVFwRkxJcFVoTDBrcXpnQ3l4MnNKSWc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
