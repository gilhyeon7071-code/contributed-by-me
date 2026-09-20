---
id: source-2026-07-01-KRX-086520-google-rss-coverage
type: source
title: KRX 086520 Google RSS Coverage Source
created: 2026-07-01
updated: 2026-07-01
status: raw
stage: 0

market: KRX
ticker: "086520"
company: 에코프로
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-01

analysis:
  summary: Local coverage report row shows news coverage for KRX 086520.
  key_facts:
    - code=086520
    - name=에코프로
    - naver_article_count=3
    - google_rss_article_count=26
    - kis_title_count=36
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 086520
    - 에코프로
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

# KRX 086520 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=086520`
- `name=에코프로`
- `naver_article_count=3`
- `google_rss_article_count=26`
- `kis_title_count=36`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 086520.

## RSS Item Metadata
- Title: `에코프로, 1.2조 유상증자…인니 니켈 제련소 투자 '가속화' - MTN 머니투데이방송`
- Source: `MTN 머니투데이방송`
- Published at: `2026-06-30T18:02:27+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE5nOUhvRTlabHY1amtka2ZVRTd1TG5kbERMbVpyVVowWnZMdER2NVRQWUxhak1uLUZ3a3A3WXFfNThua3Y2YmF2bkt3TmppRk5yVlBPUnlNazMwVnJ5T0I4QXpHOVk?oc=5`

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
- Updated at: `2026-08-21T19:25:55+09:00`
- Company: [[KRX_086520_에코프로]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-01.json`
- Latest observation title: `[특징주] 에코프로비엠, 1.2조 유상증자 소식에 6.9% 하락(종합) - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-07-01T15:55:20+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTFBoMWJMbkxnc2l2dHNVZzd0ckVkZU5WYVJLQndiY2VWMVlwY2FOenN1ZEYxOTUwMDBOX2hZR0wxaU9DMmZqM1FBN1ViR200OFJYeURoRTVhVkVidVXSAWBBVV95cUxNY250MkJwVExSd2NaamR2YlR1cEs0djFqLXlDVUxNajlVSWZsVGlYUmxIWDhwZ0xhSTRzUDI0RHZ5cno2S3VGeXh5QXg5ZURkM2s0QmNXX2FVN1ZUTmlMTjY?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
