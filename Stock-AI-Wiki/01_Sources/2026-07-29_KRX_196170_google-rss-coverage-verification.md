---
id: verification-2026-07-29-KRX-196170-google-rss-coverage
type: verification
title: KRX 196170 Google RSS Coverage Verification
created: 2026-07-29
updated: 2026-07-29
status: verification
stage: 1

market: KRX
ticker: "196170"
company: 알테오젠
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=196170
    - name=알테오젠
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=29
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 196170
    - 알테오젠
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

# KRX 196170 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-29_KRX_196170_google-rss-coverage-source]]

## Facts Checked
- `code=196170`
- `name=알테오젠`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=29`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[혁신형 제약기업 점검]⑪ 알테오젠, 넥스피·넥스맵 성과에 쏠린 눈 - 블로터`
- Source: `블로터`
- Published at: `2026-07-28T16:21:53+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE10S3FBYmdaNFZvMkRZUlJaUDhuZ3JQemkycEw2ZzZoeWFMRmktWEpuZXJ5Tnpqemp3bS0tZmQ4aUY1a1ktVnpKRmM1dE8wczF1TVhWN1AyT19wUm9QTkhDMlFoOUlVbXY20gFsQVVfeXFMUEROSllaWTNGdjN3X2dhWTV3czhVMXFuOTVhbi1RalZOd2pGQlNuMHdPUDlfdUNZQkJJb0phQko4Uk8wdG51NDVFc0lzamhYYndmOEs1STJiWkVOM0dtdkJVbHFXTEkycjdOSzRP?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:32:53+09:00`
- Company: [[KRX_196170_알테오젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-29.json`
- Latest observation title: `알테오젠, 글로벌 SC 프로젝트 잇단 진전…AZ·사노피 개발 속도 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-07-29T06:12:12+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFBFX29YYndlYTRHOVJybFl4d0NSek9nYndPNFhGcHdSSGRmNDRQbXhxSVBKM2tldi15NnNEeTNKX0VFQnA2NWlvd1BhTkdxalE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_exports_수출]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
