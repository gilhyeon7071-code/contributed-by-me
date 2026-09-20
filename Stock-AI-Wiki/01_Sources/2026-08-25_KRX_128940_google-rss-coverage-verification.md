---
id: verification-2026-08-25-KRX-128940-google-rss-coverage
type: verification
title: KRX 128940 Google RSS Coverage Verification
created: 2026-08-25
updated: 2026-08-25
status: verification
stage: 1

market: KRX
ticker: "128940"
company: 한미약품
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=128940
    - name=한미약품
    - naver_article_count=12
    - google_rss_article_count=80
    - kis_title_count=40
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 128940
    - 한미약품
  possible_impact: unknown
  uncertainty:
    - Article body archive is available locally, but entity/event verification is still unknown.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: true
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

# KRX 128940 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-25_KRX_128940_google-rss-coverage-source]]

## Facts Checked
- `code=128940`
- `name=한미약품`
- `naver_article_count=12`
- `google_rss_article_count=80`
- `kis_title_count=40`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `근육 지키는 비만신약…한미약품, 3.2조 수출 - 한국경제`
- Source: `한국경제`
- Published at: `2026-08-24T17:54:53+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9Ydm50UVFjZmsxOXdPN19mMmtld1lZQ19GREFVakhpNjJqZjNValIxTi1uTUZUWjFoQWt1VHFERW5TNHpZOU9ocUcteHdzdU9TOXYtRnZLTVJmZw?oc=5`

## Article Body Archive Checked
- Title: `근육 지키는 비만신약…한미약품, 3.2조 수출 - 한국경제`
- Source: `한국경제`
- Published at: `2026-08-24T17:54:53+09:00`
- URL: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9Ydm50UVFjZmsxOXdPN19mMmtld1lZQ19GREFVakhpNjJqZjNValIxTi1uTUZUWjFoQWt1VHFERW5TNHpZOU9ocUcteHdzdU9TOXYtRnZLTVJmZw?oc=5`
- Evidence path: `https://finance.naver.com/research/company_read.naver?nid=95849`
- Body excerpt: 제넨텍에 HM17321 최대 총 3.2조원에 기술이전 전일(8/24) 동사는 자체 개발한 비인크레틴(non-incretin) 계열 UCN2(Urocortin-2) 유사체 HM17321을 로슈 그룹의 제넨텍에 기술이전하였다. 계약금(Upfront) $190mn(약 2,629억원), 최대 마일스톤 $2.1bn(약 2조 9,263억원)으로 총 계약규모는 $2.3bn(약 3.2조원)이다. 이번 물질은 한미약품이 단독 개발한 파이프라인이라는 점에서, 계약금 배분 관련 한미사이언스와의 이익배분 이슈가 없다. ClinicalTrials.gov에 따르면 현재 진행 중인 1상은 ’27.3월 종료 예정으로 알려져 있다.

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-25T23:05:22+09:00`
- Company: [[KRX_128940_한미약품]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-25.json`
- Latest observation title: `“비만약 하나로 3조원 잭팟”…한미약품, 비만 신약 대체 뭐길래 - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-08-25T09:01:53+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE5ER1otbjBMZGNjamJNeU1xcTdWc0J5Sk8tUzFjNHE1NTlkWFFNTjlUdVR0ZTFWa2MtUEFQNUZSUG9hTTQ2aVh6NExoVk1vY29wVHc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
