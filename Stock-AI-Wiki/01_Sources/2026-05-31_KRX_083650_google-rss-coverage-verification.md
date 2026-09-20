---
id: verification-2026-05-31-KRX-083650-google-rss-coverage
type: verification
title: KRX 083650 Google RSS Coverage Verification
created: 2026-05-31
updated: 2026-05-31
status: verification
stage: 1

market: KRX
ticker: "083650"
company: 비에이치아이
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-31

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=083650
    - name=비에이치아이
    - naver_article_count=0
    - google_rss_article_count=5
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 083650
    - 비에이치아이
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

# KRX 083650 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-31_KRX_083650_google-rss-coverage-source]]

## Facts Checked
- `code=083650`
- `name=비에이치아이`
- `naver_article_count=0`
- `google_rss_article_count=5`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `이스라엘·중동 수주 확대…비에이치아이 글로벌 확장 가속 - 핀포인트뉴스`
- Source: `핀포인트뉴스`
- Published at: `2026-05-30T15:29:59+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9uQ0VFN0J0OVZGRl9vbmFsTVhSSWU1UWFOTkxtZjU0dW0wV2sxeU5VeHY4bkRXUHFEY1dHMEM5Yloxelp2eXlNTzhBWkZYX040TWtiM0tuX3FjZHJBeFJFQjVxcXRSNTY5eF9HVS1nWWxlV2vSAXdBVV95cUxNSU9aRzc4OUhhUTZKdURXemJjeHdfQXV0cHZmUW1vRWtYUTZSMFRnUUs4WXZmQk9kRkFiaTdfcFk2T3NuSldLcU5EWUhBdHo0Y3RkTkdmaUY5RHJoSVoyeExHQi1NRGg0VFZCN3JPUnVIY1NoSlJ2bw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:16:54+09:00`
- Company: [[KRX_083650_비에이치아이]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-31.json`
- Latest observation title: `이스라엘·중동 수주 확대…비에이치아이 글로벌 확장 가속 - 핀포인트뉴스`
- Latest observation source: `핀포인트뉴스`
- Latest observation published_at: `2026-05-30T15:29:59+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9uQ0VFN0J0OVZGRl9vbmFsTVhSSWU1UWFOTkxtZjU0dW0wV2sxeU5VeHY4bkRXUHFEY1dHMEM5Yloxelp2eXlNTzhBWkZYX040TWtiM0tuX3FjZHJBeFJFQjVxcXRSNTY5eF9HVS1nWWxlV2vSAXdBVV95cUxNSU9aRzc4OUhhUTZKdURXemJjeHdfQXV0cHZmUW1vRWtYUTZSMFRnUUs4WXZmQk9kRkFiaTdfcFk2T3NuSldLcU5EWUhBdHo0Y3RkTkdmaUY5RHJoSVoyeExHQi1NRGg0VFZCN3JPUnVIY1NoSlJ2bw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
