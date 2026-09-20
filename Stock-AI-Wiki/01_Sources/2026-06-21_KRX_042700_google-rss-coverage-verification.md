---
id: verification-2026-06-21-KRX-042700-google-rss-coverage
type: verification
title: KRX 042700 Google RSS Coverage Verification
created: 2026-06-21
updated: 2026-06-21
status: verification
stage: 1

market: KRX
ticker: "042700"
company: 한미반도체
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=042700
    - name=한미반도체
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 042700
    - 한미반도체
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

# KRX 042700 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-21_KRX_042700_google-rss-coverage-source]]

## Facts Checked
- `code=042700`
- `name=한미반도체`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `한미반도체, 스페이스X 주식 500억원어치 취득 완료…지분율 0.002% - 연합인포맥스`
- Source: `연합인포맥스`
- Published at: `2026-06-15T14:16:38+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTE5iU25rZHNva2tzN1lPRlRvTHQ0eTgySDVxZE02cDB1QzlNckJwb21OdjdoYno2RG04d3hHa1k5T2lmNmptdVZ1NlY4ZnBoaW13VWttMHhuaGFiS2psOFBwN2NIbWFCby1TRGthLWhLeWc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:23:01+09:00`
- Company: [[KRX_042700_한미반도체]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-21.json`
- Latest observation title: `K반도체 장비, 머스크 테라팹 생태계 입성 - 매일경제`
- Latest observation source: `매일경제`
- Latest observation published_at: `2026-06-18T17:58:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFB0M3NiMVh0S1dkMmRidG9wTHpCbk16eVh5RHhteGhkVjNLWS1ET2JVZGZVVXEwd2YyTW5idkxSUEM0NjByLURCRmFkb3FFb3hTOE53N3J3?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
