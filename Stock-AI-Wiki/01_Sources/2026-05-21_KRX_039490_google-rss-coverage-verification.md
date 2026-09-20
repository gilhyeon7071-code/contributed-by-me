---
id: verification-2026-05-21-KRX-039490-google-rss-coverage
type: verification
title: KRX 039490 Google RSS Coverage Verification
created: 2026-05-21
updated: 2026-05-21
status: verification
stage: 1

market: KRX
ticker: "039490"
company: 키움증권
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=039490
    - name=키움증권
    - naver_article_count=2
    - google_rss_article_count=19
    - kis_title_count=13
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 039490
    - 키움증권
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

# KRX 039490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-21_KRX_039490_google-rss-coverage-source]]

## Facts Checked
- `code=039490`
- `name=키움증권`
- `naver_article_count=2`
- `google_rss_article_count=19`
- `kis_title_count=13`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `키움증권 로보어드바이저 '1년 수익률 220%'…테스트베드 전체 1위 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-05-21T09:28:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBsODliQVFYbnFZVklPNUVIbXM0dU9NNUJZbE5VTHV5RmhiR1Nkc1gzTkxTalRRb2lFV1A4VUFybnZwY2FiVEd3QzdFN0QzY25rUUdKSHlKQ1pPQzlUbURhV9IBeEFVX3lxTE5URzRwcUVGa3l0RFlOMnRacE5qSndGRno2VG9JT0VfRDVjb0NtWVI5Q3ctSTJqaWl1U1k2QkE4LWpPaGxEc1k5NG9ESU02WEE0MVdqWFNOMDNiN3R5NTB1TC1odHVWVG5CNVNGV2N0LUtabTFwWmdicw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:51+09:00`
- Company: [[KRX_039490_키움증권]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `코스피 달리자 증권주도 '화색'…키움증권 12%대↑[핫종목] - 뉴스1`
- Latest observation source: `뉴스1`
- Latest observation published_at: `2026-05-21T16:38:12+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiX0FVX3lxTE05TmpndFdlN2lrcEdtNzRLbHlUV3JjX1ZqSk8yUE5pRTZ5eVpWcXFiVG9qZ19Mek5zaVBhSVdtVi12amdIUGFtTFVUZWIwQ004LWlVSndqbVJOVUptR1c4?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
