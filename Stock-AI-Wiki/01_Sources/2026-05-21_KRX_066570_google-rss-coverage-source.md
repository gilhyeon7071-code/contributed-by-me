---
id: source-2026-05-21-KRX-066570-google-rss-coverage
type: source
title: KRX 066570 Google RSS Coverage Source
created: 2026-05-21
updated: 2026-05-21
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
  collected_at: 2026-05-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 066570.
  key_facts:
    - code=066570
    - name=LG전자
    - naver_article_count=1
    - google_rss_article_count=38
    - kis_title_count=16
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
- `google_rss_article_count=38`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 066570.

## RSS Item Metadata
- Title: `“삼전닉스는 이제 그만”…LG전자·우리로 담는 상위 1% [주식 초고수는 지금] - 서울경제`
- Source: `서울경제`
- Published at: `2026-05-20T13:12:01+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE9QUWpYMGVJNjVCRjEzalFQRGd3X19ody1BeDVtN2d4WU9XamZTYmFEN2FMdHAtcFd5bTAyNmF1bWxBVXo3X3FVaTM0OE43dVZQN1HSAVNBVV95cUxPMHp5STluVWs2V1Ftc2tzOTlXTlBkRVBxWlJxTGZZWGJpdGpLY0REMWJNVUg0VkFibktkckMxS0l0aGx0UlhJdjRTVGdjZTVhd2dnZw?oc=5`

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
- Updated at: `2026-08-21T19:14:51+09:00`
- Company: [[KRX_066570_LG전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `“삼전닉스는 이제 그만”…LG전자·우리로 담는 상위 1% [주식 초고수는 지금] - 서울경제`
- Latest observation source: `서울경제`
- Latest observation published_at: `2026-05-20T13:12:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE9QUWpYMGVJNjVCRjEzalFQRGd3X19ody1BeDVtN2d4WU9XamZTYmFEN2FMdHAtcFd5bTAyNmF1bWxBVXo3X3FVaTM0OE43dVZQN1HSAVNBVV95cUxPMHp5STluVWs2V1Ftc2tzOTlXTlBkRVBxWlJxTGZZWGJpdGpLY0REMWJNVUg0VkFibktkckMxS0l0aGx0UlhJdjRTVGdjZTVhd2dnZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
