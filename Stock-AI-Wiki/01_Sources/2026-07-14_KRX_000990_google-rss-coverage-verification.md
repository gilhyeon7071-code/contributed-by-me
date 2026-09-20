---
id: verification-2026-07-14-KRX-000990-google-rss-coverage
type: verification
title: KRX 000990 Google RSS Coverage Verification
created: 2026-07-14
updated: 2026-07-14
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
  collected_at: 2026-07-14

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000990
    - name=미래에셋비전스팩11호
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=3
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
- [[2026-07-14_KRX_000990_google-rss-coverage-source]]

## Facts Checked
- `code=000990`
- `name=미래에셋비전스팩11호`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `미래에셋비전스팩11호 주주 더블유자산운용, 미래에셋비전스팩11호 주식등의 수 15만9998주 증가…총 지분율 7.57% - 디지털투데이`
- Source: `디지털투데이`
- Published at: `2026-07-08T16:38:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE5GV2hJM083Y1ctdEU0QWthRWlPb3JpY1N1d2poSHhGMDhuczNQT0ZBZkMyUm5EQWkwS0hxbkQ2WmgtMng1NnBjRmo3c0tDelhCcm01djk3SUNodjBScS1lcldfczNiWE12RHplV1A2RERHTnc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:29:21+09:00`
- Company: [[KRX_000990_미래에셋비전스팩11호]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-14.json`
- Latest observation title: `미래에셋비전스팩11호 주주 더블유자산운용, 미래에셋비전스팩11호 주식등의 수 15만9998주 증가…총 지분율 7.57% - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-07-08T16:38:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE5GV2hJM083Y1ctdEU0QWthRWlPb3JpY1N1d2poSHhGMDhuczNQT0ZBZkMyUm5EQWkwS0hxbkQ2WmgtMng1NnBjRmo3c0tDelhCcm01djk3SUNodjBScS1lcldfczNiWE12RHplV1A2RERHTnc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
