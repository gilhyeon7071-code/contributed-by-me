---
id: source-2026-05-29-KRX-066570-google-rss-coverage
type: source
title: KRX 066570 Google RSS Coverage Source
created: 2026-05-29
updated: 2026-05-29
status: raw
stage: 0

market: KRX
ticker: "066570"
company: LG전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Local coverage report row shows news coverage for KRX 066570.
  key_facts:
    - code=066570
    - name=LG전자
    - naver_article_count=1
    - google_rss_article_count=122
    - kis_title_count=53
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 066570
    - LG전자
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

# KRX 066570 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=066570`
- `name=LG전자`
- `naver_article_count=1`
- `google_rss_article_count=122`
- `kis_title_count=53`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 066570.

## RSS Item Metadata
- Title: `LG전자 사무실서 임직원 2명 흉기로 찌른 협력업체 직원 체포 [지금뉴스] - KBS 뉴스`
- Source: `KBS 뉴스`
- Published at: `2026-05-27T14:03:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE5QdzhlUjhxZWJWOXowbXRMQWdldmlnNEhLTDFla3cyM0t3TUFLQTFlLS1YY3RadEk5UlBOZjlabDB0ZWlmbFprWTBRdllqZlJIVkFfakgxSUhIMjA?oc=5`

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
- Updated at: `2026-08-21T19:16:19+09:00`
- Company: [[KRX_066570_LG전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `LG전자도 뛰고 현대차도 뛰고 네이버도 뛴다…젠슨황 방한에 들썩 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-05-29T09:32:42+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE82R1NCZ3pIbHlvQVhTamZLbVJLajQ5cF8tajJhTUYtUWpsS3hfcGoyY1RwdkNKOVc1RnI4cE5LUGFiY2VOc3RzLXNmdHA3RE1Pc3hNSEZOX1pibXhsUVlvWVZ6aXRJQlYz0gFuQVVfeXFMTXhkQ3puYWJrR08yRTF6LUdkUVZTaU1ibE9HSXdSN21oSTh1eDE1WFROM0NsNTA3b2JxLVRhcWtjOFZSZzdnMmo1Q05WcWoxaHFvSnJXQUhBcFRaelppbEo5cWxwRHZNNWxvZUFpUXc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
