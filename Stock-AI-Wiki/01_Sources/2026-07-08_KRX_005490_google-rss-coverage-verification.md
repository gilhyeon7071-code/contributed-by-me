---
id: verification-2026-07-08-KRX-005490-google-rss-coverage
type: verification
title: KRX 005490 Google RSS Coverage Verification
created: 2026-07-08
updated: 2026-07-08
status: verification
stage: 1

market: KRX
ticker: "005490"
company: POSCO홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005490
    - name=POSCO홀딩스
    - naver_article_count=2
    - google_rss_article_count=4
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005490
    - POSCO홀딩스
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

# KRX 005490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-08_KRX_005490_google-rss-coverage-source]]

## Facts Checked
- `code=005490`
- `name=POSCO홀딩스`
- `naver_article_count=2`
- `google_rss_article_count=4`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `POSCO홀딩스(005490) 목표주가 55만원 유지…2분기 영업이익 6879억원 전망, 철강·리튬 회복이 관건 - 한국투데이`
- Source: `한국투데이`
- Published at: `2026-07-07T14:04:28+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE41M0l4SW5uYWd2MDBiajZDM2JmWlF2SkwxRzZDdFN4cjRiby10bUVZMHhfWFl5bkdvZl8yeE54Ym1BanoxWHFKa2tqS3oxTzk3MzQxTDMyV3NpRDNicC1xS3JHdmtEUktXcWZQY2FR0gFuQVVfeXFMTjUzSXhJbm5hZ3YwMGJqNkMzYmZaUXZKTDFHNkN0U3hyNGJvLXRtRVkweF9YWXluR29mXzJ4TnhibUFqejFYcUpra2pLejFPOTczNDFMMzJXc2lEM2JwLXFLckd2a0RSS1dxZlBjYVE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:27:46+09:00`
- Company: [[KRX_005490_POSCO홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-08.json`
- Latest observation title: `[리포트 브리핑]POSCO홀딩스, '하반기로 한 템포만 느리게' 목표가 460,000원 - 다올투자증권 - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-07-08T11:44:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE16MWpIelpELUd2UU5FV1UxVWxXb0JiTGI0MUR5OXBON3ppVGpURWd0NW9aM3pRN2hvQzM3aVZBZHBGZzVJWllVd0VrdjFuSmoydjlpbDdZeTJPZldo?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
