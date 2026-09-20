---
id: verification-2026-05-26-KRX-126730-google-rss-coverage
type: verification
title: KRX 126730 Google RSS Coverage Verification
created: 2026-05-26
updated: 2026-05-26
status: verification
stage: 1

market: KRX
ticker: "126730"
company: 코칩
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=126730
    - name=코칩
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 126730
    - 코칩
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

# KRX 126730 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-26_KRX_126730_google-rss-coverage-source]]

## Facts Checked
- `code=126730`
- `name=코칩`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[강세 토픽] 2차전지 생산·판매 테마, 코칩 +12.61%, LG에너지솔루션 +2.89% - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-26T09:12:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNZ2FiaVF3MlZhZDhGTjdEUlRhSjFja3dVLW10aHQxWV9mQ0ptOVBUU0Z4SE9KOWl0VXhqeVI2Yjk3WmpsME1fZDZHWHZfSHdDTGl1VUhtZkY3U19ZeTRjZFFtdWcxQlpUN3ZmUlczT3pMUV9yRS0xbU1TbEthWFNoU0VKONIBlwFBVV95cUxPZF9JaUtQaGxCOThsQzYwRWxsRFJoRmUxNWV5RDFFMjRKdW1EZVNvTDl2c29IOVhzRkE0TDFCdkJLdy1TMmd4RmZwMEd3UWNuMFl1V3dJaFFsc3Bib051WDRsRzBPNUhhQXRrb0xBR1prMmdRVE1OMnRfa0tOdTFPSEw4T2JRN0VmM2FsX1F4LWtwNmpGYnpR?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-26T11:05:06+09:00`
- Company: [[KRX_126730_코칩]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-26.json`
- Latest observation title: `[강세 토픽] 2차전지 생산·판매 테마, 코칩 +12.61%, LG에너지솔루션 +2.89% - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-26T09:12:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNZ2FiaVF3MlZhZDhGTjdEUlRhSjFja3dVLW10aHQxWV9mQ0ptOVBUU0Z4SE9KOWl0VXhqeVI2Yjk3WmpsME1fZDZHWHZfSHdDTGl1VUhtZkY3U19ZeTRjZFFtdWcxQlpUN3ZmUlczT3pMUV9yRS0xbU1TbEthWFNoU0VKONIBlwFBVV95cUxPZF9JaUtQaGxCOThsQzYwRWxsRFJoRmUxNWV5RDFFMjRKdW1EZVNvTDl2c29IOVhzRkE0TDFCdkJLdy1TMmd4RmZwMEd3UWNuMFl1V3dJaFFsc3Bib051WDRsRzBPNUhhQXRrb0xBR1prMmdRVE1OMnRfa0tOdTFPSEw4T2JRN0VmM2FsX1F4LWtwNmpGYnpR?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
