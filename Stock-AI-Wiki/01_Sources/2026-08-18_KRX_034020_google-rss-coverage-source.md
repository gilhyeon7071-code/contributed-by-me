---
id: source-2026-08-18-KRX-034020-google-rss-coverage
type: source
title: KRX 034020 Google RSS Coverage Source
created: 2026-08-18
updated: 2026-08-18
status: raw
stage: 0

market: KRX
ticker: "034020"
company: 두산에너빌리티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-18

analysis:
  summary: Local coverage report row shows news coverage for KRX 034020.
  key_facts:
    - code=034020
    - name=두산에너빌리티
    - naver_article_count=3
    - google_rss_article_count=7
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034020
    - 두산에너빌리티
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

# KRX 034020 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=034020`
- `name=두산에너빌리티`
- `naver_article_count=3`
- `google_rss_article_count=7`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 034020.

## RSS Item Metadata
- Title: `[시선강탈] 두산에너빌리티 vs 가온칩스 vs 두산테스나, 공략법은? - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-08-13T06:38:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE0xMHNaMXZsQzRVYWoxUUNsLVNRa3NOeks1UEU5T1lwSVgxVXJSMTVPamsyd0NYS0gyeDhBREswb0xoT0h5NENidzI1eC1ibEZ0c3oxNnRhTlR0T0w1M0N0M1hwQ3Y2TFVP?oc=5`

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
- Updated at: `2026-08-21T19:36:51+09:00`
- Company: [[KRX_034020_두산에너빌리티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-18.json`
- Latest observation title: `두산에너빌리티, 테라파워 ‘나트륨’ 핵심 기자재 수주…美 케머러 공급 - 인사이트N파워`
- Latest observation source: `인사이트N파워`
- Latest observation published_at: `2026-08-18T16:35:46+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE0xbm9HS3UxUFZFanVGckNnR3JQeG1CZjl6Ml9SYmMzMks2UkNRQzZXYzd3T19xMVRiLVJnTFlXUzZja3BHMUtLczNOalc2cTJUNExuYk42WVN2clBSRWNxeEY5UjhFdw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
