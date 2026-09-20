---
id: verification-2026-08-10-KRX-119850-google-rss-coverage
type: verification
title: KRX 119850 Google RSS Coverage Verification
created: 2026-08-10
updated: 2026-08-10
status: verification
stage: 1

market: KRX
ticker: "119850"
company: 지엔씨에너지
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-10

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=119850
    - name=지엔씨에너지
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 119850
    - 지엔씨에너지
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

# KRX 119850 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-10_KRX_119850_google-rss-coverage-source]]

## Facts Checked
- `code=119850`
- `name=지엔씨에너지`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[김민정 기자의 '코스뽀'] 지엔씨에너지 AI 붐에 비상발전기 호황, 친환경 에너지 발전전문기업 향한다 - 비즈니스포스트`
- Source: `비즈니스포스트`
- Published at: `2026-08-10T08:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1BNVV5UEVtQTNxOGQ3WkNLN2tWTGxpNkl5bTZCeFp5UWplUkpBc2E4VVJZazItYlVpX2YxNHZ6c2dDSTFfZVRrUmhRZU5WU1NxZDRiTWY1TDVMckVyaFYySFcyY0drOFpuOWZ1aVl6YmJTTzQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:34:49+09:00`
- Company: [[KRX_119850_지엔씨에너지]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-10.json`
- Latest observation title: `[IB토마토](크레딧시그널)지엔씨에너지, AI 데이터센터 타고 외형 확대 - 뉴스토마토`
- Latest observation source: `뉴스토마토`
- Latest observation published_at: `2026-08-10T17:29:04+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE5GR1V1Xzh1TjdzMzNfM2x1ZkI3NWdPZ3lFRmpTSEhWZGhPd3ljOTM0TWptd2c2am5SWUJnbC15a09qR2poOHY4QlZwemRSU19CUk5TdjZyaEt3NFJ4R2V0bA?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
