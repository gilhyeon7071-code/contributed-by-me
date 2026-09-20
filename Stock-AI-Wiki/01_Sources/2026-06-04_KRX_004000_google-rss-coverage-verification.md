---
id: verification-2026-06-04-KRX-004000-google-rss-coverage
type: verification
title: KRX 004000 Google RSS Coverage Verification
created: 2026-06-04
updated: 2026-06-04
status: verification
stage: 1

market: KRX
ticker: "004000"
company: 롯데정밀화학
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-04

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=004000
    - name=롯데정밀화학
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 004000
    - 롯데정밀화학
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

# KRX 004000 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-04_KRX_004000_google-rss-coverage-source]]

## Facts Checked
- `code=004000`
- `name=롯데정밀화학`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `롯데화학군 ‘2026 Leadership Summit’ 개최 - 플라스틱코리아`
- Source: `플라스틱코리아`
- Published at: `2026-06-04T02:34:39+09:00`
- Link: `https://news.google.com/rss/articles/CBMidEFVX3lxTE5ISEpjdFpEQzhfZWphT2JYNndXRDNhcGNNdWRwTlF5eF85NkFJbF9VbXNvZEhZR1NsUVM3ZUszUm1mRDI2dGVaOUl2dk5IV0VLQXhuRmNaMHJCT2cyQXIwUUF0OU9Pc1pCbzl6djdMRnVGYTZZ0gF0QVVfeXFMTkhISmN0WkRDOF9lamFPYlg2d1dEM2FwY011ZHBOUXl4Xzk2QUlsX1Vtc29kSFlHU2xRUzdlSzNSbWZEMjZ0ZVo5SXZ2TkhXRUtBeG5GY1owckJPZzJBcjBRQXQ5T09zWkJvOXp2N0xGdUZhNlk?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:18:01+09:00`
- Company: [[KRX_004000_롯데정밀화학]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-04.json`
- Latest observation title: `롯데화학군 ‘2026 Leadership Summit’ 개최 - 플라스틱코리아`
- Latest observation source: `플라스틱코리아`
- Latest observation published_at: `2026-06-04T02:34:39+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMidEFVX3lxTE5ISEpjdFpEQzhfZWphT2JYNndXRDNhcGNNdWRwTlF5eF85NkFJbF9VbXNvZEhZR1NsUVM3ZUszUm1mRDI2dGVaOUl2dk5IV0VLQXhuRmNaMHJCT2cyQXIwUUF0OU9Pc1pCbzl6djdMRnVGYTZZ0gF0QVVfeXFMTkhISmN0WkRDOF9lamFPYlg2d1dEM2FwY011ZHBOUXl4Xzk2QUlsX1Vtc29kSFlHU2xRUzdlSzNSbWZEMjZ0ZVo5SXZ2TkhXRUtBeG5GY1owckJPZzJBcjBRQXQ5T09zWkJvOXp2N0xGdUZhNlk?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
