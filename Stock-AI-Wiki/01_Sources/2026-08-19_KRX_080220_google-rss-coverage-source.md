---
id: source-2026-08-19-KRX-080220-google-rss-coverage
type: source
title: KRX 080220 Google RSS Coverage Source
created: 2026-08-19
updated: 2026-08-19
status: raw
stage: 0

market: KRX
ticker: "080220"
company: 제주반도체
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-19

analysis:
  summary: Local coverage report row shows news coverage for KRX 080220.
  key_facts:
    - code=080220
    - name=제주반도체
    - naver_article_count=2
    - google_rss_article_count=8
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 080220
    - 제주반도체
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

# KRX 080220 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=080220`
- `name=제주반도체`
- `naver_article_count=2`
- `google_rss_article_count=8`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 080220.

## RSS Item Metadata
- Title: `[P전송금지]"범용 D램도 품귀 지속" 반등하는 제주반도체 - 한국경제`
- Source: `한국경제`
- Published at: `2026-08-18T17:36:52+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9VOE8tbHdZanJRV0gxTWtCN0dXZ1lwUHZMSEdPbHNJS2JGTWZMUXltN0FhWG0xekppSXdzQkxFM3JiZG45bzB3bGdIWFFCZDRpSklvQzJSdWVuZw?oc=5`

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
- Updated at: `2026-08-21T19:37:11+09:00`
- Company: [[KRX_080220_제주반도체]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-19.json`
- Latest observation title: `제주반도체, 2분기 영업익 1200억원…전년비 2700%↑ - 디일렉`
- Latest observation source: `디일렉`
- Latest observation published_at: `2026-08-14T16:38:51+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTFAyTHVVWlA1QTZWcVMtZ3lyQzAzSkY2NnZGR1RtYS05UjRKMzBRRkI1OEI1clAtU0paNGZxSkw0U1Y4RzB0cmc5a0d3LTVXWUVfVHNNRmwtdUNPZklxTG1meFVVX3FyQQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
