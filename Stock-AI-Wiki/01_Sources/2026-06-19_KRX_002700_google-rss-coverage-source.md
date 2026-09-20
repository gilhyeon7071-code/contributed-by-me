---
id: source-2026-06-19-KRX-002700-google-rss-coverage
type: source
title: KRX 002700 Google RSS Coverage Source
created: 2026-06-19
updated: 2026-06-19
status: raw
stage: 0

market: KRX
ticker: "002700"
company: 신일전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-19

analysis:
  summary: Local coverage report row shows news coverage for KRX 002700.
  key_facts:
    - code=002700
    - name=신일전자
    - naver_article_count=0
    - google_rss_article_count=11
    - kis_title_count=10
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 002700
    - 신일전자
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

# KRX 002700 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=002700`
- `name=신일전자`
- `naver_article_count=0`
- `google_rss_article_count=11`
- `kis_title_count=10`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 002700.

## RSS Item Metadata
- Title: `[자사주 매입]신일전자, 20억원 자사주 취득 결정 - 데일리인베스트`
- Source: `데일리인베스트`
- Published at: `2026-06-17T14:30:51+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE84MVpGUGpDWEswV3NkWEstWEhkUnFKalBNRmFWWnBYZWtCejhpdmdEZnBFTU5mWWxkOGFBVTNaSFJZb2Nzc1NJVlVaN0lqOElaMldYTUt6VF93STZTUDRHbGR0SzZLRUFxdW45StIBb0FVX3lxTE5KQ2RaTGNmV0VqelBvUlBpUmpLQkE5ZkdCS0QtNWdFdHdyQW9GdTdSZm1hVDFfWnFWNW9SeXd6N0hEeW14LWttTVQxMWlVQ1BHN21xTHRJdjNWMHJyYkNta0tXdGFmSXNkUS1rcnBvaw?oc=5`

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
- Updated at: `2026-06-19T09:07:46+09:00`
- Company: [[KRX_002700_신일전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-19.json`
- Latest observation title: `[자사주 매입]신일전자, 20억원 자사주 취득 결정 - 데일리인베스트`
- Latest observation source: `데일리인베스트`
- Latest observation published_at: `2026-06-17T14:30:51+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE84MVpGUGpDWEswV3NkWEstWEhkUnFKalBNRmFWWnBYZWtCejhpdmdEZnBFTU5mWWxkOGFBVTNaSFJZb2Nzc1NJVlVaN0lqOElaMldYTUt6VF93STZTUDRHbGR0SzZLRUFxdW45StIBb0FVX3lxTE5KQ2RaTGNmV0VqelBvUlBpUmpLQkE5ZkdCS0QtNWdFdHdyQW9GdTdSZm1hVDFfWnFWNW9SeXd6N0hEeW14LWttTVQxMWlVQ1BHN21xTHRJdjNWMHJyYkNta0tXdGFmSXNkUS1rcnBvaw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
