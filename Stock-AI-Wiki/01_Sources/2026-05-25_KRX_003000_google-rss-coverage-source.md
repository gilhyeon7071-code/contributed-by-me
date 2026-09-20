---
id: source-2026-05-25-KRX-003000-google-rss-coverage
type: source
title: KRX 003000 Google RSS Coverage Source
created: 2026-05-25
updated: 2026-05-25
status: raw
stage: 0

market: KRX
ticker: "003000"
company: 부광약품
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 003000.
  key_facts:
    - code=003000
    - name=부광약품
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003000
    - 부광약품
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

# KRX 003000 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=003000`
- `name=부광약품`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 003000.

## RSS Item Metadata
- Title: `부광약품 한국유니온제약 품었는데 수익성은 물음표, 이제영 중추신경계 의약품이 믿는 구석 - 비즈니스포스트`
- Source: `비즈니스포스트`
- Published at: `2026-05-25T06:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBGZk9xcm9oOWtMUk15Mm9DY0d5djZDdk93emhWZ1hJak9BX25idHZFTlgxa0lMNGpDb3cyMjU5YU15eWRPTG00dV9MVkFBMktqaVV6OHl5LUpWZ1Q2WXFxdVl3RDQ0WWJ4MFpwRF9UUmlOb2c?oc=5`

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
- Updated at: `2026-05-25T16:05:04+09:00`
- Company: [[KRX_003000_부광약품]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `부광약품 '2026 대한민국 브랜드대상 선정' [포토] - bntnews.co.kr`
- Latest observation source: `bntnews.co.kr`
- Latest observation published_at: `2026-05-25T15:48:24+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE9BdWxjUG9hV0t4Rk9pOFhadEc5a3ZYZDZUdl9QdG5UalM3TVFLbjBEcUsxZzU1UVR3MjY3cGMtNFJLVjB4TmRjSlVRZnZPbDdxbDRaWVUzbVd2akZyQ0ljUlV2MTY?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
