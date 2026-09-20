---
id: verification-2026-07-27-KRX-005930-google-rss-coverage
type: verification
title: KRX 005930 Google RSS Coverage Verification
created: 2026-07-27
updated: 2026-07-27
status: verification
stage: 1

market: KRX
ticker: "005930"
company: 삼성전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005930
    - name=삼성전자
    - naver_article_count=5
    - google_rss_article_count=150
    - kis_title_count=81
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005930
    - 삼성전자
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

# KRX 005930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-27_KRX_005930_google-rss-coverage-source]]

## Facts Checked
- `code=005930`
- `name=삼성전자`
- `naver_article_count=5`
- `google_rss_article_count=150`
- `kis_title_count=81`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `Z플립8 2년 쓰면 반값 보장…삼성전자, ‘New 갤럭시 AI 구독클럽’ 공개 - 경기일보`
- Source: `경기일보`
- Published at: `2026-07-26T15:02:32+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE9FdGZTcHJ6VWZDN3J4b1RLVTNLMHdyTHdqQWcxUGJ0a29rVldpMlh4ckQ4R29CcHV5bXFRM0lldTB4dWx1TkZoOUNubGVDUVp1R2dWR1dDR1dKYUk?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:32:13+09:00`
- Company: [[KRX_005930_삼성전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-27.json`
- Latest observation title: `삼성전자, ‘갤럭시 Z 폴드8 울트라·폴드8·플립8’, ‘갤럭시 워치 울트라2·워치9’ 사전 판매 시작 - Samsung Global Newsroom`
- Latest observation source: `Samsung Global Newsroom`
- Latest observation published_at: `2026-07-27T08:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiwgJBVV95cUxNN1ZYNWNDNzlaWk16dHBYdUJrVjEzN0JZMmtKOWsxUnpqblYwbEdTbGQ4S1oxeEJmcmdFZVAyaTNPT2FpdGVKVmJuUFlyVTdWbFJqUUJqOW5aNGtFRnZ3NDc5SlBrQTJHSjlTOFlLcFh0OFN3TGg5WTNtNm5keWZxTFRXaGpuZEpnMkxZZHBrN0NUdjBqaG1Xd1JwT05FT1lFbGhCYnp0X2o5RHduTEJpa010em52c3AwTXNuVVY1NDlnVlNET0FoRVZkU1gyTjlIQzNTa2RlTEdZQlNCYVNNY01oRmxtMWdhU0FkRWY2ZVNva3c3MjhBUkhsby1YYkd6cnZ0ZkUySWduU3czcjdwQ3dBOUE4SllFVXpnQ3VnazdVb3Fmb0U5N21makZzZFhqOS11Z1JNdWc1enRSajlFWkhB?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
