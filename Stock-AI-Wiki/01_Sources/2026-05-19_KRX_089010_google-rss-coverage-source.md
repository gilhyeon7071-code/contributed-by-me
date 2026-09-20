---
id: source-2026-05-19-KRX-089010-google-rss-coverage
type: source
title: KRX 089010 Google RSS Coverage Source
created: 2026-05-19
updated: 2026-05-19
status: raw
stage: 0

market: KRX
ticker: "089010"
company: 켐트로닉스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-19

analysis:
  summary: Local coverage report row shows news coverage for KRX 089010.
  key_facts:
    - code=089010
    - name=켐트로닉스
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 089010
    - 켐트로닉스
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

# KRX 089010 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=089010`
- `name=켐트로닉스`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 089010.

## RSS Item Metadata
- Title: `"켐트로닉스, 2부기부터 반도체 소재 매출 본격화…목표가↑"-신한 - 네이트`
- Source: `네이트`
- Published at: `2026-05-19T08:12:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiU0FVX3lxTE1BdkVPcEo5eFJfVlpCd0N3X2ZDNFNnWF9ZUEREd2RHTEFBSFpJdHZQUF96amFuNVBJOVFZemNYVW5GTzlmdVdRQjdNQzZQbVZ5N1RN?oc=5`

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
- Updated at: `2026-08-21T19:14:34+09:00`
- Company: [[KRX_089010_켐트로닉스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-19.json`
- Latest observation title: `"켐트로닉스, 2부기부터 반도체 소재 매출 본격화…목표가↑"-신한 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-05-19T08:11:53+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVEFVX3lxTFBUYXY1Z3R1Z01uWnVTdkhvaTJZMER6bG1TbFYzLWQxUHdpanI2VzRoaWtjemdOMFVOaDNQNnZvem9QMGotZ1M5WlpHQUtsM1VuUWk2SNIBVEFVX3lxTFBUYXY1Z3R1Z01uWnVTdkhvaTJZMER6bG1TbFYzLWQxUHdpanI2VzRoaWtjemdOMFVOaDNQNnZvem9QMGotZ1M5WlpHQUtsM1VuUWk2SA?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
