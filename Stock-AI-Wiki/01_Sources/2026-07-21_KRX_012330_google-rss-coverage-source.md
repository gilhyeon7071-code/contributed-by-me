---
id: source-2026-07-21-KRX-012330-google-rss-coverage
type: source
title: KRX 012330 Google RSS Coverage Source
created: 2026-07-21
updated: 2026-07-21
status: raw
stage: 0

market: KRX
ticker: "012330"
company: 현대모비스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 012330.
  key_facts:
    - code=012330
    - name=현대모비스
    - naver_article_count=1
    - google_rss_article_count=55
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 012330
    - 현대모비스
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

# KRX 012330 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=012330`
- `name=현대모비스`
- `naver_article_count=1`
- `google_rss_article_count=55`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 012330.

## RSS Item Metadata
- Title: `현대모비스, 유럽 부품 공장 본격 가동…"올해 수주 13조 넘긴다" - 뉴스1`
- Source: `뉴스1`
- Published at: `2026-07-21T09:30:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBNQ1haZ3Zoa2F0RWtDUXRabVR0Vk8xWWtsZlkxZG9GZFhCbWsyelN1d3FjVnpQSTFRMWVpSTRBZ3hsY2pGQjhuYmlEMWlUak9INWRSdXBHS1ZrSjlqZ1g1ZQ?oc=5`

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
- Updated at: `2026-08-21T19:30:18+09:00`
- Company: [[KRX_012330_현대모비스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-21.json`
- Latest observation title: `[단독]현대모비스 램프사업부 매각 완료…24일 본계약 체결 - 매일경제`
- Latest observation source: `매일경제`
- Latest observation published_at: `2026-07-21T14:46:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE16bnVmTzJwZEpqU0NYZ3p0M2dXcVRSX2MyN3FUUVFvOVZ6ZERxNFF4WXl2VFVmcGpqX0JVV0ZGWWxNclN1MlZxSmYyVEZ4elYtYWN0NDBB?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
