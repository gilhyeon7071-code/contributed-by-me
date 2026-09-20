---
id: source-2026-05-27-KRX-115500-google-rss-coverage
type: source
title: KRX 115500 Google RSS Coverage Source
created: 2026-05-27
updated: 2026-05-27
status: raw
stage: 0

market: KRX
ticker: "115500"
company: 케이씨에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Local coverage report row shows news coverage for KRX 115500.
  key_facts:
    - code=115500
    - name=케이씨에스
    - naver_article_count=8
    - google_rss_article_count=7
    - kis_title_count=18
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 115500
    - 케이씨에스
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

# KRX 115500 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=115500`
- `name=케이씨에스`
- `naver_article_count=8`
- `google_rss_article_count=7`
- `kis_title_count=18`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 115500.

## RSS Item Metadata
- Title: `[이격도과열 종목] SK네트웍스 삼화콘덴서 네이처셀 한켐 케이씨에스 엑스게이트 '펄펄끓네' - 핀포인트뉴스`
- Source: `핀포인트뉴스`
- Published at: `2026-05-26T16:36:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMid0FVX3lxTE84OTZ2cWplM1BHekZiUEV2R0F4QU9OWUh3MnpiZGNTT0k2UHhVRGFKbjVQVzd5a05ObmhTSXVlQ25sNk4wbkdKT0RHeHp4eWI1TDF2R09sSTV3Rm9oX2trWWpscFptbXIwRnJhZmdNRjF2cDdTWTNR0gF3QVVfeXFMTzg5NnZxamUzUEd6RmJQRXZHQXhBT05ZSHcyemJkY1NPSTZQeFVEYUpuNVBXN3lrTk5uaFNJdWVDbmw2TjBuR0pPREd4enh5YjVMMXZHT2xJNXdGb2hfa2tZamxwWm1tcjBGcmFmZ01GMXZwN1NZM1E?oc=5`

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
- Updated at: `2026-05-27T16:05:05+09:00`
- Company: [[KRX_115500_케이씨에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `케이씨에스, +29.97% 상한가 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-26T11:25:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxOME9wRUd2TWNBZlhGM2tvMWkwZzRHal9NWGFIRXZJSXc3LWwtbG5xejFTWEpxSFdESVlZZmlJQnNqZHVpTVczSTZSMVpqQ25yWlVtNEpxZWd3TDRGUXJCRHhDNFJHLXFBc01tOHBNTHZkOENMZURBYzZzcFRWYlhXVjVTYXZvSEZiMm5JSWplOEpXZFV1cE1F0gGXAUFVX3lxTE4wT3BFR3ZNY0FmWEYza28xaTBnNEdqX01YYUhFdklJdzctbC1sbnF6MVNYSnFIV0RJWVlmaUlCc2pkdWlNVzNJNlIxWmpDbnJaVW00SnFlZ3dMNEZRckJEeEM0UkctcUFzTW04cE1MdmQ4Q0xlREFjNnNwVFZiWFdWNVNhdm9IRmIybklJamU4SldkVXVwTUU?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
