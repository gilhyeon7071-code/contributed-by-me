---
id: verification-2026-08-07-KRX-058610-google-rss-coverage
type: verification
title: KRX 058610 Google RSS Coverage Verification
created: 2026-08-07
updated: 2026-08-07
status: verification
stage: 1

market: KRX
ticker: "058610"
company: 에스피지
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-07

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=058610
    - name=에스피지
    - naver_article_count=1
    - google_rss_article_count=3
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 058610
    - 에스피지
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

# KRX 058610 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-07_KRX_058610_google-rss-coverage-source]]

## Facts Checked
- `code=058610`
- `name=에스피지`
- `naver_article_count=1`
- `google_rss_article_count=3`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `에스피지, 로봇 액추에이터 'SDD' 9월 양산 돌입 - 지디넷코리아`
- Source: `지디넷코리아`
- Published at: `2026-07-23T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFBkWXlpZ0JVX1BxSkVWZHVXUmUxU1hvZlBEMHFsYnBULU44d3RDQ1R0c1lLWE9zOFFLell0X1pyOXVpYjFzU3BDeEFtalBYMEMwMm90cUpB?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:33:51+09:00`
- Company: [[KRX_058610_에스피지]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-07.json`
- Latest observation title: `에스피지 주가, 8월 7일 101,700원 6.18% 하락 마감 - 톱스타뉴스`
- Latest observation source: `톱스타뉴스`
- Latest observation published_at: `2026-08-07T15:40:10+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE5rUlBJa1ZrdUhuWFcxdS1KUExPcENETXdJeUwtb1Jidmc4aG5vb1B4WFJhLXNXWXIxb25Tb3BOQ2czZEtHM3pKbzhzMnBYeWROZGlOV1NvWVNCQnpGR0JwbkM1N3hmN0lnYUhVcFZHbExudw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
