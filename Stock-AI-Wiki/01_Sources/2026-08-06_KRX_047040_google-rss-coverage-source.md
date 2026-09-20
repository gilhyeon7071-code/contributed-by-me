---
id: source-2026-08-06-KRX-047040-google-rss-coverage
type: source
title: KRX 047040 Google RSS Coverage Source
created: 2026-08-06
updated: 2026-08-06
status: raw
stage: 0

market: KRX
ticker: "047040"
company: 대우건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Local coverage report row shows news coverage for KRX 047040.
  key_facts:
    - code=047040
    - name=대우건설
    - naver_article_count=1
    - google_rss_article_count=63
    - kis_title_count=9
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 047040
    - 대우건설
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

# KRX 047040 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=047040`
- `name=대우건설`
- `naver_article_count=1`
- `google_rss_article_count=63`
- `kis_title_count=9`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 047040.

## RSS Item Metadata
- Title: `[단독] 김보현 대우건설 대표 후임에 이강석 상무 내정 - MTN 머니투데이방송`
- Source: `MTN 머니투데이방송`
- Published at: `2026-08-06T15:00:44+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE96VGsyU2l3X3FXQUhKLVlyd3FuMDhxTFZTTFZBTEhxMkNieWtRM2tNV2c2Q0RsaElJN1lZdDFxcG9ibUJwN2N5cFJSS2twZDVmUkpOX19NVkhDZk5oQ2FyVGRUZlk?oc=5`

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
- Company: [[KRX_047040_대우건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `대우건설 김보현 대표이사 사임, 후임으로 이강석 부사장 추천 예정 - 비즈니스포스트`
- Latest observation source: `비즈니스포스트`
- Latest observation published_at: `2026-08-06T17:05:21+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFA1Ymw4OUl1QklOZzdaMDZMUDEzTjNoemlSeFQ5cDdRNi0xX1VNUW43Z3p2RlJwWHpMa3ZuMXJESjBuMlNmaXVUTVp4Wnp1dFlUeWVWTWdZQl81VDQ5d1FvMmVCQnV5OWl6YjVlMi00QjFBNnM?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
