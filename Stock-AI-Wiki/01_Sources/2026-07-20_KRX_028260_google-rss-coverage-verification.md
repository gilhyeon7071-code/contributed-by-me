---
id: verification-2026-07-20-KRX-028260-google-rss-coverage
type: verification
title: KRX 028260 Google RSS Coverage Verification
created: 2026-07-20
updated: 2026-07-20
status: verification
stage: 1

market: KRX
ticker: "028260"
company: 삼성물산
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=028260
    - name=삼성물산
    - naver_article_count=1
    - google_rss_article_count=111
    - kis_title_count=23
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 028260
    - 삼성물산
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

# KRX 028260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-20_KRX_028260_google-rss-coverage-source]]

## Facts Checked
- `code=028260`
- `name=삼성물산`
- `naver_article_count=1`
- `google_rss_article_count=111`
- `kis_title_count=23`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[건설 DC 조직은] ①삼성물산, '최초 전담 조직'이 이뤄낸 기술 격차 - fetv.co.kr`
- Source: `fetv.co.kr`
- Published at: `2026-07-20T07:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE83ZHFzcWwzenMtRVRfNU00VFJDTHpyN2o3bHdmTDBVNU0zZzBzZDE5SkxtODZsZGxKXzFTZ3BqeVhDbVpQMDJQU3pJZXF6ZVZEUkV2LUs3MmcwV3l3U1htWXlWdjVWOVk1?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:29:58+09:00`
- Company: [[KRX_028260_삼성물산]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-20.json`
- Latest observation title: `삼성물산 패션부문, 8월7일까지 '삼성패션디자인펀드' 참여할 신진 디자이너 모집 - 비즈니스포스트`
- Latest observation source: `비즈니스포스트`
- Latest observation published_at: `2026-07-20T16:49:11+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE5QM01nREFpVHRDcGhTaC0xa05va2RpX3FJYnU1TzY0bTFsNC1BeGljVUJOaUQ2cFRacU5hOUxCWnc3OUxmcGNQOW9oYTR4eXlSeG1lTV9CVWxmU1Zlb3AyLWowLUxVYm9OQUR5VnVDcWlHQ1E?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
