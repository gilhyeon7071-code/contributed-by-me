---
id: source-2026-06-15-KRX-005690-google-rss-coverage
type: source
title: KRX 005690 Google RSS Coverage Source
created: 2026-06-15
updated: 2026-06-15
status: raw
stage: 0

market: KRX
ticker: "005690"
company: 파미셀
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-15

analysis:
  summary: Local coverage report row shows news coverage for KRX 005690.
  key_facts:
    - code=005690
    - name=파미셀
    - naver_article_count=12
    - google_rss_article_count=5
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005690
    - 파미셀
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

# KRX 005690 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=005690`
- `name=파미셀`
- `naver_article_count=12`
- `google_rss_article_count=5`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 005690.

## RSS Item Metadata
- Title: `파미셀, 엔비디아 AI 서버용 CCL 핵심 소재 '두산에 독점 공급’ - 디일렉`
- Source: `디일렉`
- Published at: `2026-06-14T22:43:50+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE5BTVgxNkM4X251c0xWVnZCdmxXTldpeHRqV0lCb2lzVTk5SXM3M2JJVC15YnVrcTdCTTE3M1NaVVJybXpBWjQ0TWJJeEczYTlscDFDbHd0T293dWtUa2M0clJzREYtUQ?oc=5`

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
- Updated at: `2026-08-21T19:21:08+09:00`
- Company: [[KRX_005690_파미셀]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-15.json`
- Latest observation title: `파미셀, 엔비디아 AI 서버용 CCL 핵심 소재 '두산에 독점 공급’ - 디일렉`
- Latest observation source: `디일렉`
- Latest observation published_at: `2026-06-14T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE5BTVgxNkM4X251c0xWVnZCdmxXTldpeHRqV0lCb2lzVTk5SXM3M2JJVC15YnVrcTdCTTE3M1NaVVJybXpBWjQ0TWJJeEczYTlscDFDbHd0T293dWtUa2M0clJzREYtUQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
