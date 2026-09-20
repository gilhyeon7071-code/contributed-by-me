---
id: verification-2026-07-26-KRX-010120-google-rss-coverage
type: verification
title: KRX 010120 Google RSS Coverage Verification
created: 2026-07-26
updated: 2026-07-26
status: verification
stage: 1

market: KRX
ticker: "010120"
company: 엘에스일렉트릭
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=010120
    - name=엘에스일렉트릭
    - naver_article_count=1
    - google_rss_article_count=8
    - kis_title_count=27
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 010120
    - 엘에스일렉트릭
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

# KRX 010120 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-26_KRX_010120_google-rss-coverage-source]]

## Facts Checked
- `code=010120`
- `name=엘에스일렉트릭`
- `naver_article_count=1`
- `google_rss_article_count=8`
- `kis_title_count=27`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LS ELECTRIC, 매출 1조6000억원, 영업이익 1800억원 /연결잠정실적 - 데일리인베스트`
- Source: `데일리인베스트`
- Published at: `2026-07-23T13:08:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMib0FVX3lxTE55LThJUk85UV91MjctQ3lMZkR6STJOVXVPNUlyZWlCOHlaamhoenZwVl85WDF0TmQ0ODFLU0tFRmlubURnT3ZwTUNIdVF3NmRhYUtqOWRIMFdlRWZJVU55bG8yYVZwM2UwUlc0dnRzb9IBb0FVX3lxTE55LThJUk85UV91MjctQ3lMZkR6STJOVXVPNUlyZWlCOHlaamhoenZwVl85WDF0TmQ0ODFLU0tFRmlubURnT3ZwTUNIdVF3NmRhYUtqOWRIMFdlRWZJVU55bG8yYVZwM2UwUlc0dnRzbw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:31:54+09:00`
- Company: [[KRX_010120_엘에스일렉트릭]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-26.json`
- Latest observation title: `LS ELECTRIC (010120) - ThinkPool`
- Latest observation source: `ThinkPool`
- Latest observation published_at: `2026-07-23T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiS0FVX3lxTE40eVhTUk9qWFpoRlNtbm41dkFSRDVXOXhXcUt2WVZpWldKenNGU2lFNm1nTWVwcHphTThyZnM0NndpT0V2czVhWGUtcw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
