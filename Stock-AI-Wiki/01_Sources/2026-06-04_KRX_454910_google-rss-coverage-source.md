---
id: source-2026-06-04-KRX-454910-google-rss-coverage
type: source
title: KRX 454910 Google RSS Coverage Source
created: 2026-06-04
updated: 2026-06-04
status: raw
stage: 0

market: KRX
ticker: "454910"
company: 두산로보틱스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-04

analysis:
  summary: Local coverage report row shows news coverage for KRX 454910.
  key_facts:
    - code=454910
    - name=두산로보틱스
    - naver_article_count=3
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 454910
    - 두산로보틱스
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

# KRX 454910 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=454910`
- `name=두산로보틱스`
- `naver_article_count=3`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 454910.

## RSS Item Metadata
- Title: `두산로보틱스 美거점 9월 증설…“생산능력 2배” - 서울경제`
- Source: `서울경제`
- Published at: `2026-06-03T17:35:27+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE5iMVQ0enVtUGFuRnRobFNjSkZvcVRtQlgyV3F5VGVlRzlsQzZDNldKeThKUXlvMWF4YmRHWGFGamdaZk9FaXFvbERWdEstcWlkZ0HSAVNBVV95cUxPX3UzNFlaM3VXd1lHQlRNdUtGaXJYbkE0QkVYR3VzZ1J6UzFKWFk0dUJGdGk3V192eDJHTGo4dHA4eXJOVF9rTy04SGNHWVdTWGw2RQ?oc=5`

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
- Updated at: `2026-06-04T09:01:25+09:00`
- Company: [[KRX_454910_두산로보틱스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-04.json`
- Latest observation title: `두산로보틱스 美거점 9월 증설…“생산능력 2배” - 서울경제`
- Latest observation source: `서울경제`
- Latest observation published_at: `2026-06-03T17:35:27+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE5iMVQ0enVtUGFuRnRobFNjSkZvcVRtQlgyV3F5VGVlRzlsQzZDNldKeThKUXlvMWF4YmRHWGFGamdaZk9FaXFvbERWdEstcWlkZ0HSAVNBVV95cUxPX3UzNFlaM3VXd1lHQlRNdUtGaXJYbkE0QkVYR3VzZ1J6UzFKWFk0dUJGdGk3V192eDJHTGo4dHA4eXJOVF9rTy04SGNHWVdTWGw2RQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
