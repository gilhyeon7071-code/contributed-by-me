---
id: verification-2026-05-28-KRX-011200-google-rss-coverage
type: verification
title: KRX 011200 Google RSS Coverage Verification
created: 2026-05-28
updated: 2026-05-28
status: verification
stage: 1

market: KRX
ticker: "011200"
company: HMM
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=011200
    - name=HMM
    - naver_article_count=1
    - google_rss_article_count=56
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 011200
    - HMM
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

# KRX 011200 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-28_KRX_011200_google-rss-coverage-source]]

## Facts Checked
- `code=011200`
- `name=HMM`
- `naver_article_count=1`
- `google_rss_article_count=56`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `'HMM 나무호' 공격 발사체 대함미사일로 결론‥곧 조사 결과 발표 - MBC 뉴스`
- Source: `MBC 뉴스`
- Published at: `2026-05-27T13:27:48+09:00`
- Link: `https://news.google.com/rss/articles/CBMieEFVX3lxTE9mRkp3RUU5dlhnSVhrSndfdmF6cHdWWkgwWkFhQ0tMNXFCR1lrNUZPUi13YUoySDFhVGQtRGw4SExPV1oxR1R5YmgwbXpqSm5pSzh5bXlLc3k3SWxuQ2dILU5aZ2hyaWNRQ3lZdjFrUjlUZlpNbUd3R9IBeEFVX3lxTE1GXzZPYUgwVVBQVUdVQlRNQ1NqWU9GbWJSWDc3OXBjQ3BZTDFBOU8weUszZW1JWU5YY3k0MWRnSWRWVE13RF9aaDVHeFJwSzk4V0FmS044TzBoQ1AwV3RqbkFiR3Y2c0dZVWdIcVZNTzhmaVMxVkZ6cQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-28T21:05:04+09:00`
- Company: [[KRX_011200_HMM]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `HMM 나무호, 두바이 현지서 수리작업 본격화…선원 3명 하선 - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-05-28T15:48:22+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE53Q2Zma3BmZkdlUnZ6YTJ5NGdaOWpGTEhUN1ZsZ0dfc09mNklvbmlWaXJsY3QtUG5yY2JqbW9tOUFISzZFSjVDd0FyRDlMUkFxa0pqRzVxSnFkMDDSAWBBVV95cUxOa3ZERFNVSFN6OHdKdHE2S2pEVzl0N1ZfdC1hSWJ3LU9OaU0yS29SMVNpWE1OSDF1TzZNekpQQ3hGWFZ4QkFOZW5OamY1VEoxQW5Fa2dRVmc3STBRc1BOeGY?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
