---
id: verification-2026-06-08-KRX-005690-google-rss-coverage
type: verification
title: KRX 005690 Google RSS Coverage Verification
created: 2026-06-08
updated: 2026-06-08
status: verification
stage: 1

market: KRX
ticker: "005690"
company: 파미셀
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005690
    - name=파미셀
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 005690
    - 파미셀
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

# KRX 005690 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-08_KRX_005690_google-rss-coverage-source]]

## Facts Checked
- `code=005690`
- `name=파미셀`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `파미셀, 숨겨진 젠슨황 수혜주?…두산 통해 엔비디아 AI소재 공급망 편입 - 팜이데일리`
- Source: `팜이데일리`
- Published at: `2026-06-04T08:20:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTFBRTDJXNFFWeXI3dkR3SGV3RlN6WW1RWGJPSkNzeTFrc0JXNUx0VmNBcm5yb05nRGw1NTJIbkg4emk5TGNrZVEtZjVUNkFUTGZkaG8yb0lVVVhDUmJFQU1Nb3RZNXgwNlJjbU00V01B?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:19:12+09:00`
- Company: [[KRX_005690_파미셀]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-08.json`
- Latest observation title: `파미셀, 숨겨진 젠슨황 수혜주?…두산 통해 엔비디아 AI소재 공급망 편입 - 팜이데일리`
- Latest observation source: `팜이데일리`
- Latest observation published_at: `2026-06-04T08:20:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTFBRTDJXNFFWeXI3dkR3SGV3RlN6WW1RWGJPSkNzeTFrc0JXNUx0VmNBcm5yb05nRGw1NTJIbkg4emk5TGNrZVEtZjVUNkFUTGZkaG8yb0lVVVhDUmJFQU1Nb3RZNXgwNlJjbU00V01B?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
