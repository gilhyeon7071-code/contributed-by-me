---
id: source-2026-08-06-KRX-010130-google-rss-coverage
type: source
title: KRX 010130 Google RSS Coverage Source
created: 2026-08-06
updated: 2026-08-06
status: raw
stage: 0

market: KRX
ticker: "010130"
company: 고려아연
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Local coverage report row shows news coverage for KRX 010130.
  key_facts:
    - code=010130
    - name=고려아연
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 010130
    - 고려아연
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

# KRX 010130 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=010130`
- `name=고려아연`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 010130.

## RSS Item Metadata
- Title: `11조 美 제련소 두고 폭발한 고려아연 vs 영풍·MBK, 형사고발 맞불 - magazine.hankyung.com`
- Source: `magazine.hankyung.com`
- Published at: `2026-08-05T14:06:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTFBqMHhRSEhJbkhMOUxwYUk1TnNrOHI1Ym1EaTA4VDk2MVgyYU8yd2d3RUc4NGNURXdrb3FHOHN2a1htc3FWeTZtTmRNNXBZREtzQUxSVjU0UXpHZ1hHMjF5cm12aVg2aHVMSFRSeg?oc=5`

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
- Updated at: `2026-08-21T19:33:31+09:00`
- Company: [[KRX_010130_고려아연]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `고려아연 상반기 매출 12조 돌파...창사 이후 최대 반기 실적 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-08-05T18:19:39+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1jN012Zm40N2xmbXpyTGd6dlNkTHhBQnRrQ2ZzN0tXcHMzdHVIV3lHWG5DMFFGUEV2dTJwcUxJNC0tOGZQSkNPMUdGVGxEMnM?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
