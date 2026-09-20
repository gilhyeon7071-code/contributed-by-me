---
id: verification-2026-07-06-KRX-000990-google-rss-coverage
type: verification
title: KRX 000990 Google RSS Coverage Verification
created: 2026-07-06
updated: 2026-07-06
status: verification
stage: 1

market: KRX
ticker: "000990"
company: 미래에셋비전스팩11호
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000990
    - name=미래에셋비전스팩11호
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000990
    - 미래에셋비전스팩11호
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

# KRX 000990 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-06_KRX_000990_google-rss-coverage-source]]

## Facts Checked
- `code=000990`
- `name=미래에셋비전스팩11호`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[특징주] 미래에셋비전스팩11호·하나36호스팩 상장 첫날 급등 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2025-12-22T17:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiiAFBVV95cUxQRUp4Yy1lYUFoU1VEQUVidDYxZkpqRGFhWWRwZTdIWGprWDU1Tm9hbHZYOTNfZDRGOG5Qb21jRWVGSWRrMGZ2ZjVyRWpoZ3NWXzBTbS11Q1hfVjUyTHRWSTdvT2pueTNWMzB1b2lVajM4SHk1NXg4MzBWSXpUcF9pVWdfUXpZcHln0gGcAUFVX3lxTE1BSE5fb0JQRHFKeXgzVVBFTklEZU1ack5nMUkyWlRtZzF3NGdWRG9WY1lXRXN6OVFvYUdOeWdJb3cwQkpCSjNPM3JMaTFvQ3Bndm8wTVZQb3NSYnBHMHhBdXZHajJndTM0Y0VVWnJLZkk2TDJ2dUZwY0ppSGtQaWE4aTVJelFBaTRVRDdyRHNwSEpncFlaTVJBbU1VeQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-06T08:05:14+09:00`
- Company: [[KRX_000990_미래에셋비전스팩11호]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-06.json`
- Latest observation title: `[특징주] 미래에셋비전스팩11호·하나36호스팩 상장 첫날 급등 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2025-12-22T17:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiiAFBVV95cUxQRUp4Yy1lYUFoU1VEQUVidDYxZkpqRGFhWWRwZTdIWGprWDU1Tm9hbHZYOTNfZDRGOG5Qb21jRWVGSWRrMGZ2ZjVyRWpoZ3NWXzBTbS11Q1hfVjUyTHRWSTdvT2pueTNWMzB1b2lVajM4SHk1NXg4MzBWSXpUcF9pVWdfUXpZcHln0gGcAUFVX3lxTE1BSE5fb0JQRHFKeXgzVVBFTklEZU1ack5nMUkyWlRtZzF3NGdWRG9WY1lXRXN6OVFvYUdOeWdJb3cwQkpCSjNPM3JMaTFvQ3Bndm8wTVZQb3NSYnBHMHhBdXZHajJndTM0Y0VVWnJLZkk2TDJ2dUZwY0ppSGtQaWE4aTVJelFBaTRVRDdyRHNwSEpncFlaTVJBbU1VeQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
