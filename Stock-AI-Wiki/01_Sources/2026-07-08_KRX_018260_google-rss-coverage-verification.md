---
id: verification-2026-07-08-KRX-018260-google-rss-coverage
type: verification
title: KRX 018260 Google RSS Coverage Verification
created: 2026-07-08
updated: 2026-07-08
status: verification
stage: 1

market: KRX
ticker: "018260"
company: 삼성에스디에스
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
    - code=018260
    - name=삼성에스디에스
    - naver_article_count=2
    - google_rss_article_count=2
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 018260
    - 삼성에스디에스
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

# KRX 018260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-08_KRX_018260_google-rss-coverage-source]]

## Facts Checked
- `code=018260`
- `name=삼성에스디에스`
- `naver_article_count=2`
- `google_rss_article_count=2`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `한화증권 "삼성SDS, 클라우드 성장세에 호실적 기대…목표가↑" - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-07-07T08:28:41+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTFBPYW4tTVVBanp2VXc1Q2FMbVVUMEtCU2dfUGN1Wm1rNk02d0ZOQVYtUzQ2QWlOOThjM3RhVnFXWm1EREhLazNROG5QekI5ZE1DMTh1ZUtQZDg4V2fSAWBBVV95cUxPdll3cGV2LTZMaldMZVhxS2FFSWdHa0lWenRkenoxYnBPMF96VG5nbVh5eUJhYUpSNUdFMU1IVi04RXJBOS0yY2RFRFB3SWZaZXlSc3dDWEpFNXJ1TWFMeHY?oc=5`

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
- Company: [[KRX_018260_삼성에스디에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-08.json`
- Latest observation title: `"삼성에스디에스, 2분기 실적 기대 웃돌 것…목표가↑"-한화 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-07-07T08:27:24+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE5VM1gtbEwzbWJlM1RQS2RtN1lmTHY2ZmdhcmItYWtiLUFhUWE5SnBpQVBTUVJmczhxVUdWMTRmZU9aazVhWDZtd1RjNUdYUDJtY3NZeTQtemlWZ9IBVEFVX3lxTFAxbUNGX1ZCM0l0ZUkzcTc1dmJHMDRwWUY0cFFrZTc3dDNVV2EzcXcxNEJlUGt1dkJUaVo0QUxSSHE3bllSeGdvWEFRMmRHVVpyeUF2SA?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
