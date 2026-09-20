---
id: verification-2026-07-26-KRX-034730-google-rss-coverage
type: verification
title: KRX 034730 Google RSS Coverage Verification
created: 2026-07-26
updated: 2026-07-26
status: verification
stage: 1

market: KRX
ticker: "034730"
company: SK
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
    - code=034730
    - name=SK
    - naver_article_count=22
    - google_rss_article_count=214
    - kis_title_count=53
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034730
    - SK
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

# KRX 034730 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-26_KRX_034730_google-rss-coverage-source]]

## Facts Checked
- `code=034730`
- `name=SK`
- `naver_article_count=22`
- `google_rss_article_count=214`
- `kis_title_count=53`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `SK, 美 빅테크와 1100조원 'AI 동맹'…글로벌 생태계 주도권 잡는다 - 한국경제`
- Source: `한국경제`
- Published at: `2026-07-25T15:17:51+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9KVkM5SEotUTdzOHZpN3dXS0FZdmN2bnhIR3FpejdRLS1ib1EwcDNwbFBVMHhiLS01Y2NDUW9BV2haSmRQcnQwWXk1c1doQVV1eEhLWGx3M0dZdw?oc=5`

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
- Company: [[KRX_034730_SK]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-26.json`
- Latest observation title: `29일 실적 발표하는 SK하이닉스…2분기 영업익 64조원 전망 솔솔 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-07-26T10:39:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiiAFBVV95cUxNcU45ZzRSczVOMGVnUFZUWndqVlRDLTBJeXIwT0ZpZF9NMlZFYlJkZXI0RHhhd3hwamJIcXpkc01rdHFvQ3RSRGZkYTliaXlXOUkzQjU4QnloUUJ4YkpVMXB0MWFQN3FTSkRnUXQ2TmFLMGZhVDhzYVNSX3pzT05IdVhLd2hSODhS0gGcAUFVX3lxTFAycTVHdkV6eXVScFdlZDRMVFZUS1ZDX0tlN0dhb3A3eEdhQnRfR3R3SnRkNnRnX3ZvbHpRTk1EdFJRRmwyck9JWDdicVRldWdDUG91V0I0RFV0NTd6dS1kbDVWcTg3T3RfVXREdW0yTHJkXzFLVkJlV2MzMGZnSWJvQ2x2ZGNGV09XNmpSQ1lzN0xUa1l4dUR6ZlJNeA?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
