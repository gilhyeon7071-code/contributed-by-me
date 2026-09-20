---
id: verification-2026-07-16-KRX-086520-google-rss-coverage
type: verification
title: KRX 086520 Google RSS Coverage Verification
created: 2026-07-16
updated: 2026-07-16
status: verification
stage: 1

market: KRX
ticker: "086520"
company: 에코프로
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-16

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=086520
    - name=에코프로
    - naver_article_count=2
    - google_rss_article_count=5
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 086520
    - 에코프로
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

# KRX 086520 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-16_KRX_086520_google-rss-coverage-source]]

## Facts Checked
- `code=086520`
- `name=에코프로`
- `naver_article_count=2`
- `google_rss_article_count=5`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `1.2조 유상증자 금감원 제동에…에코프로 형제 동반 강세 - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-07-15T09:29:29+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9fQWRBRGZXd1ZJZUFwcC1DazFLaGc1TGRsbXYzeUNzZ0JaTGV3dG9PSGV6UE5MSDB4VDM1TDRzeWV5cGhLalF4NFdPRnNMOFRKVjZtTVNSS09NbHVtcVB0UlZjeUk3V2Rw0gFuQVVfeXFMUFJ4dkN2dDYwajJLUjhUM1lOWDZHb2VMVnk0V3JZajNhUGdMclBDalhBV3hPdXpkekhqVjZHeFg3Q2RkT2pvX0xLRkdveW5Kajd1Zzg5b3o5Vm9VVTlnbWswaU8wZ2U0M3ZCS2FxZ1E?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:29:40+09:00`
- Company: [[KRX_086520_에코프로]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-16.json`
- Latest observation title: `에코프로비엠 1조2000억 유상증자 급제동…금감원 정정요구 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-07-14T21:28:50+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE00aVY1RHptbW4ycU1HOS05VzI1SVJKYWxtSTRPS3FvUVZEZnhJVVd4LUZrbXpEby1HZXFzWVhmaVB3SFJ4bnU0ZGk1clBXTUFpME1scmo1T3N0cVRyaFBDZTVXU001NTBF0gFuQVVfeXFMTTRTeVFVOTV0elVSS0VheGswdlpERmQxQWRFMjI5Nkc5NmU4T2ZGMXl5UXk0eFB5WXJHTXkyNGY5NEJ5a000SHpuNnp2Mm5tRjFkaUxMSFFDYVh4V0JQcVdGeUVyR2tCN05ubmYyeWc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
