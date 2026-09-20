---
id: verification-2026-06-18-KRX-000720-google-rss-coverage
type: verification
title: KRX 000720 Google RSS Coverage Verification
created: 2026-06-18
updated: 2026-06-18
status: verification
stage: 1

market: KRX
ticker: "000720"
company: 현대건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000720
    - name=현대건설
    - naver_article_count=1
    - google_rss_article_count=133
    - kis_title_count=23
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000720
    - 현대건설
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

# KRX 000720 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-18_KRX_000720_google-rss-coverage-source]]

## Facts Checked
- `code=000720`
- `name=현대건설`
- `naver_article_count=1`
- `google_rss_article_count=133`
- `kis_title_count=23`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"현대건설도 한다"…고금리·재무부담에 코스피로 번지는 CB 발행 - 인베스트조선`
- Source: `인베스트조선`
- Published at: `2026-06-15T07:01:14+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxPZ2JIY2ZvaGtaZjlYcmpxeEtXbUpjMksydXlWS3A4TlB4SWoxTmVQNlQ4VGI3SXJQbWpnRjlwQkNlRWttcEdXTUNsODFKNVEtZkI1NEgzNmNBSzdDaV9oRjJwVlZCVm8ySTFjT2RQLXAwU2Fjam0zblZTdWVEME1KWjNhSQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:22:04+09:00`
- Company: [[KRX_000720_현대건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-18.json`
- Latest observation title: `현대건설, 네덜란드서 원전 건설 심포지엄… “현지 사업 수주 만전” - 브릿지경제`
- Latest observation source: `브릿지경제`
- Latest observation published_at: `2026-06-18T09:47:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFBwRDFYeXVFTy0zaGlya1BaVERDY01JOVZYRDM2N0VWekRmM2dIc0hxLUV3al9wSjJDeUtvUUotM3FXZ0p2M1pGV3IyUktDakNId0N0UzFxZ1VLdw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
