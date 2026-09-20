---
id: source-2026-05-28-KRX-105840-google-rss-coverage
type: source
title: KRX 105840 Google RSS Coverage Source
created: 2026-05-28
updated: 2026-05-28
status: raw
stage: 0

market: KRX
ticker: "105840"
company: 우진
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 105840.
  key_facts:
    - code=105840
    - name=우진
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 105840
    - 우진
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

# KRX 105840 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=105840`
- `name=우진`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 105840.

## RSS Item Metadata
- Title: `문우진, ‘허수아비’서 차시영 아역 맡아 긴장감 더한 존재감 - 톱스타뉴스`
- Source: `톱스타뉴스`
- Published at: `2026-05-28T08:35:40+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTE9GZjhDdmdSX0RjOVNSSVJiVG8wcG93VmlweW9MeDZWOUo3TThfZnRLZV9lNXhtcTI2RDVYNnprR2JTQ0Q2QktxY3Z2RWlEa0xzNGk0UGw5bmdXMHhUVWVrVFREV1h1d1gzd0c5eEhJNkhSUQ?oc=5`

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
- Updated at: `2026-05-28T21:05:04+09:00`
- Company: [[KRX_105840_우진]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `문우진, 박보검 이어 이희준 아역…화제작에 다 있다 - JTBC`
- Latest observation source: `JTBC`
- Latest observation published_at: `2026-05-28T09:15:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVEFVX3lxTE8xbUlUZ19uc0lBY0VYYnlxcFZ3cGp5eFVXS0NXMzN3UF9hRFBJbzY0NkcyRktXOUNHWlFSOGxvV1g3OWwzaFY3dWMxQ21wblJnNnljMA?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
