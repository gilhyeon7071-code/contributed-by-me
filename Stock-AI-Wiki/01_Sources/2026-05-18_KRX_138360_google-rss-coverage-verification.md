---
id: verification-2026-05-18-KRX-138360-google-rss-coverage
type: verification
title: KRX 138360 Google RSS Coverage Verification
created: 2026-05-18
updated: 2026-05-18
status: verification
stage: 1

market: KRX
ticker: "138360"
company: Hyupjin
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - coverage_row_exists=True
    - google_rss_article_count=1
    - kis_title_count=7
  related_entities:
    - KRX 138360
    - Hyupjin
  possible_impact: unknown
  uncertainty:
    - Original article text unavailable.
    - Latest Google RSS probe JSON parses successfully when read as UTF-8.

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
  change_reason: real local coverage verification sample
---

# KRX 138360 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-18_KRX_138360_google-rss-coverage-source]]

## Facts Checked
- CSV row exists for `code=138360`.
- CSV row has `google_rss_article_count=1`.
- CSV row has `kis_title_count=7`.
- CSV row has `any_covered=True`.

## Numbers Checked
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=7`

## Dates Checked
- Coverage report file timestamp was current in this run.
- Exact original article published date was not verified from the CSV row.

## Entities Checked
- `code=138360` checked from CSV row.
- Company name was normalized in this wiki note as `Hyupjin`.
- Original Korean source name was not used as a verified entity because source text verification is incomplete.

## Conflicts
- No conflicting source was checked.

## Remaining Uncertainty
- Original article text is unavailable.
- Google RSS probe JSON parses successfully with PowerShell `Get-Content -Raw -Encoding UTF8 | ConvertFrom-Json`.
- Default PowerShell read encoding may display mojibake and fail parsing.
- This coverage row cannot support sentiment, thesis, candidate, shadow, paper, or production state.

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:22+09:00`
- Company: [[KRX_138360_협진]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-18.json`
- Latest observation title: `농어촌 취약지 응급환자 '원격협진' 시스템 확대 - 메디칼타임즈`
- Latest observation source: `메디칼타임즈`
- Latest observation published_at: `2026-05-17T09:30:10+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTE94SEdjR2hoZEhZTU95TjZNRHFZQVFCbjByTmdyR3hTV1prV082eGI0RThPM1FvV3BJcWE2NEx1YW05RnRtaHpaYXZOamNrc2pHN2QyRjVzejZIZ2p4MGJUZTJUSU5iSVF3WXdxVnh3?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_medical-cooperation_의료-협진]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
