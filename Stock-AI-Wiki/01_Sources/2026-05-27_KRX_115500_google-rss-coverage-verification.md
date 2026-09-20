---
id: verification-2026-05-27-KRX-115500-google-rss-coverage
type: verification
title: KRX 115500 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

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
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
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
    - RSS item metadata is available, but full original article body is unavailable.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: false
  numeric_values_checked: true
  date_values_checked: true
  entity_names_checked: false
  conflict_exists: false
  conflict_summary:
  confidence: unknown

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
  change_reason: generated coverage verification note
---

# KRX 115500 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_115500_google-rss-coverage-source]]

## Facts Checked
- `code=115500`
- `name=케이씨에스`
- `naver_article_count=8`
- `google_rss_article_count=7`
- `kis_title_count=18`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[이격도과열 종목] SK네트웍스 삼화콘덴서 네이처셀 한켐 케이씨에스 엑스게이트 '펄펄끓네' - 핀포인트뉴스`
- Source: `핀포인트뉴스`
- Published at: `2026-05-26T16:36:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMid0FVX3lxTE84OTZ2cWplM1BHekZiUEV2R0F4QU9OWUh3MnpiZGNTT0k2UHhVRGFKbjVQVzd5a05ObmhTSXVlQ25sNk4wbkdKT0RHeHp4eWI1TDF2R09sSTV3Rm9oX2trWWpscFptbXIwRnJhZmdNRjF2cDdTWTNR0gF3QVVfeXFMTzg5NnZxamUzUEd6RmJQRXZHQXhBT05ZSHcyemJkY1NPSTZQeFVEYUpuNVBXN3lrTk5uaFNJdWVDbmw2TjBuR0pPREd4enh5YjVMMXZHT2xJNXdGb2hfa2tZamxwWm1tcjBGcmFmZ01GMXZwN1NZM1E?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

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
