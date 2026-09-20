---
id: verification-2026-07-23-KRX-086520-google-rss-coverage
type: verification
title: KRX 086520 Google RSS Coverage Verification
created: 2026-07-23
updated: 2026-07-23
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
  collected_at: 2026-07-23

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=086520
    - name=에코프로
    - naver_article_count=1
    - google_rss_article_count=48
    - kis_title_count=18
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
- [[2026-07-23_KRX_086520_google-rss-coverage-source]]

## Facts Checked
- `code=086520`
- `name=에코프로`
- `naver_article_count=1`
- `google_rss_article_count=48`
- `kis_title_count=18`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `에코프로, 화장품 ODM 화성코스메틱 인수 추진… 한투PE와 컨소시엄 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-07-22T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMihwFBVV95cUxPV3h5OW9uOUJrcGdFV2RZQXZkSkRfQ2hsdzBqZzk1YmhzTFhjcVVWRl93NXZrYlpTTTZyWmgwcVlTRGNxWERjbzkzWjAwTWgtY2c1OHZtTzJIQ3dtQmFGenotZ1R4WmZYMTh1S0xnd1c0c3pDOVRiN1Vnak04TVpnbHdnd3AycHfSAZsBQVVfeXFMUG5BaFlzS1otMmRhd1hJZGFmLUdkUmU2ZVBreTltSzNrVVJmR0R2TDFDVHFQWW1UYlF1US1ILVB3V0VEb2xud2pRMjUxdjF0a1FmcVdUT1BIU2t5Q1ozSlIwang2WjRYNllveEJUbWhvNDRLbmw3dEpxYWlYTWFjTXBOSlZwMEJmVnh1a2lCazk0UUF5eDNiV0JmMGs?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:30:57+09:00`
- Company: [[KRX_086520_에코프로]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-23.json`
- Latest observation title: `에코프로, 화장품 ODM 화성코스메틱 인수 추진… 한투PE와 컨소시엄 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-07-22T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMihwFBVV95cUxPV3h5OW9uOUJrcGdFV2RZQXZkSkRfQ2hsdzBqZzk1YmhzTFhjcVVWRl93NXZrYlpTTTZyWmgwcVlTRGNxWERjbzkzWjAwTWgtY2c1OHZtTzJIQ3dtQmFGenotZ1R4WmZYMTh1S0xnd1c0c3pDOVRiN1Vnak04TVpnbHdnd3AycHfSAZsBQVVfeXFMUG5BaFlzS1otMmRhd1hJZGFmLUdkUmU2ZVBreTltSzNrVVJmR0R2TDFDVHFQWW1UYlF1US1ILVB3V0VEb2xud2pRMjUxdjF0a1FmcVdUT1BIU2t5Q1ozSlIwang2WjRYNllveEJUbWhvNDRLbmw3dEpxYWlYTWFjTXBOSlZwMEJmVnh1a2lCazk0UUF5eDNiV0JmMGs?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
