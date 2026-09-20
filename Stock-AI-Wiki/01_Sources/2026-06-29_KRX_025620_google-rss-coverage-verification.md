---
id: verification-2026-06-29-KRX-025620-google-rss-coverage
type: verification
title: KRX 025620 Google RSS Coverage Verification
created: 2026-06-29
updated: 2026-06-29
status: verification
stage: 1

market: KRX
ticker: "025620"
company: 차AI헬스케어
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=025620
    - name=차AI헬스케어
    - naver_article_count=5
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 025620
    - 차AI헬스케어
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

# KRX 025620 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-29_KRX_025620_google-rss-coverage-source]]

## Facts Checked
- `code=025620`
- `name=차AI헬스케어`
- `naver_article_count=5`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `차AI헬스케어, 324만주 의무보유 해제 초읽기...오버행 리스크 점증① - 녹색경제신문`
- Source: `녹색경제신문`
- Published at: `2026-06-29T06:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE9JOEVrVVRDbUpYM1FLVFlfaDNnMzBrTVBUTWtCcHJjdTZUb0RwOHZIRnVBaFpEUUxlMElGTmdqelp4THFiWWc1cjJfNF8xTjI3ZnZsUFIzdGZiblpIbG1qb1JDVQ?oc=5`

## Article Body Archive Checked
- Title: `차AI헬스케어, 324만주 의무보유 해제 초읽기...오버행 리스크 점증① - 녹색경제신문`
- Source: `녹색경제신문`
- Published at: `2026-06-29T06:00:00+09:00`
- URL: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE9JOEVrVVRDbUpYM1FLVFlfaDNnMzBrTVBUTWtCcHJjdTZUb0RwOHZIRnVBaFpEUUxlMElGTmdqelp4THFiWWc1cjJfNF8xTjI3ZnZsUFIzdGZiblpIbG1qb1JDVQ?oc=5`
- Evidence path: `https://finance.naver.com/news/news_read.naver?article_id=0006313572&office_id=018&mode=LSS2D&type=0%C2%A7ion_id%3D101%C2%A7ion_id2%3D258%C2%A7ion_id3%3D&date=20260624&page=1`
- Body excerpt: 방문하시려는 페이지의 주소가 잘못 입력되었거나, 페이지의 주소가 변경 혹은 삭제되어 요청하신 페이지를 찾을 수 없습니다. 입력하신 주소가 정확한지 다시 한번 확인해 주시기 바랍니다. 관련 문의사항은 고객센터에 알려주시면 친절히 안내해드리겠습니다. 감사합니다.

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
