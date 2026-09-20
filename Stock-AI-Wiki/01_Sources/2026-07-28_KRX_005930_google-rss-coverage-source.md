---
id: source-2026-07-28-KRX-005930-google-rss-coverage
type: source
title: KRX 005930 Google RSS Coverage Source
created: 2026-07-28
updated: 2026-07-28
status: raw
stage: 0

market: KRX
ticker: "005930"
company: 삼성전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 005930.
  key_facts:
    - code=005930
    - name=삼성전자
    - naver_article_count=11
    - google_rss_article_count=121
    - kis_title_count=55
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005930
    - 삼성전자
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

# KRX 005930 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=005930`
- `name=삼성전자`
- `naver_article_count=11`
- `google_rss_article_count=121`
- `kis_title_count=55`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 005930.

## RSS Item Metadata
- Title: `삼성전자, ‘갤럭시 Z 폴드8 울트라·폴드8·플립8’, ‘갤럭시 워치 울트라2·워치9’ 사전 판매 시작 - Samsung Global Newsroom`
- Source: `Samsung Global Newsroom`
- Published at: `2026-07-27T08:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiwgJBVV95cUxNN1ZYNWNDNzlaWk16dHBYdUJrVjEzN0JZMmtKOWsxUnpqblYwbEdTbGQ4S1oxeEJmcmdFZVAyaTNPT2FpdGVKVmJuUFlyVTdWbFJqUUJqOW5aNGtFRnZ3NDc5SlBrQTJHSjlTOFlLcFh0OFN3TGg5WTNtNm5keWZxTFRXaGpuZEpnMkxZZHBrN0NUdjBqaG1Xd1JwT05FT1lFbGhCYnp0X2o5RHduTEJpa010em52c3AwTXNuVVY1NDlnVlNET0FoRVZkU1gyTjlIQzNTa2RlTEdZQlNCYVNNY01oRmxtMWdhU0FkRWY2ZVNva3c3MjhBUkhsby1YYkd6cnZ0ZkUySWduU3czcjdwQ3dBOUE4SllFVXpnQ3VnazdVb3Fmb0U5N21makZzZFhqOS11Z1JNdWc1enRSajlFWkhB?oc=5`

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
- Updated at: `2026-07-28T08:05:50+09:00`
- Company: [[KRX_005930_삼성전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `삼성전자, ‘갤럭시 Z 폴드8 울트라·폴드8·플립8’, ‘갤럭시 워치 울트라2·워치9’ 사전 판매 시작 - Samsung Global Newsroom`
- Latest observation source: `Samsung Global Newsroom`
- Latest observation published_at: `2026-07-27T08:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiwgJBVV95cUxNN1ZYNWNDNzlaWk16dHBYdUJrVjEzN0JZMmtKOWsxUnpqblYwbEdTbGQ4S1oxeEJmcmdFZVAyaTNPT2FpdGVKVmJuUFlyVTdWbFJqUUJqOW5aNGtFRnZ3NDc5SlBrQTJHSjlTOFlLcFh0OFN3TGg5WTNtNm5keWZxTFRXaGpuZEpnMkxZZHBrN0NUdjBqaG1Xd1JwT05FT1lFbGhCYnp0X2o5RHduTEJpa010em52c3AwTXNuVVY1NDlnVlNET0FoRVZkU1gyTjlIQzNTa2RlTEdZQlNCYVNNY01oRmxtMWdhU0FkRWY2ZVNva3c3MjhBUkhsby1YYkd6cnZ0ZkUySWduU3czcjdwQ3dBOUE4SllFVXpnQ3VnazdVb3Fmb0U5N21makZzZFhqOS11Z1JNdWc1enRSajlFWkhB?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
