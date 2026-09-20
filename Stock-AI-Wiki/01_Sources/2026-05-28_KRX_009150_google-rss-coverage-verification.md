---
id: verification-2026-05-28-KRX-009150-google-rss-coverage
type: verification
title: KRX 009150 Google RSS Coverage Verification
created: 2026-05-28
updated: 2026-05-28
status: verification
stage: 1

market: KRX
ticker: "009150"
company: 삼성전기
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=009150
    - name=삼성전기
    - naver_article_count=7
    - google_rss_article_count=55
    - kis_title_count=40
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 009150
    - 삼성전기
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

# KRX 009150 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-28_KRX_009150_google-rss-coverage-source]]

## Facts Checked
- `code=009150`
- `name=삼성전기`
- `naver_article_count=7`
- `google_rss_article_count=55`
- `kis_title_count=40`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `“삼전·하이닉스 다음은 부품주”…AI 훈풍에 삼성전기·LG이노텍 질주 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-28T06:02:40+09:00`
- Link: `https://news.google.com/rss/articles/CBMiiAFBVV95cUxPdmg2b1A1VzRzVzA1UzN5VnNCUzlId2I4MUdNcUlqX01JNU94cXI0b25LQ0hBSjVJX0V1cE9Sa0pWeWRXM0xydnp3MFdKbGc5dDZWeWZXbkdRQm8zempFTDVTeml0ZjhQX3BIZGE5T0RsbzR2WER6bGpabVdGV2tYV1ltWHBiYnBR0gGcAUFVX3lxTE5JeEUxdlVpUzdSMkktZ0ROZmFkRTZvSGJHSHE4QWRxeW03S19PZmR3cWlVRno3SVBwV2dXc01UOUNoVFBsYWFkcmR2R3F4bnZXWWU0V1k4bm5ZZUo0U2VfclVPTjRlcUtiT0xnU0pfMlRzeXpJLXB1Y2NjMWxnVHhuUm9yS3FpUkEyM1hubzNMU01pOGRTcFNoNHBweg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:16:01+09:00`
- Company: [[KRX_009150_삼성전기]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `“삼전·하이닉스 다음은 부품주”…AI 훈풍에 삼성전기·LG이노텍 질주 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-28T06:31:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiiAFBVV95cUxPdmg2b1A1VzRzVzA1UzN5VnNCUzlId2I4MUdNcUlqX01JNU94cXI0b25LQ0hBSjVJX0V1cE9Sa0pWeWRXM0xydnp3MFdKbGc5dDZWeWZXbkdRQm8zempFTDVTeml0ZjhQX3BIZGE5T0RsbzR2WER6bGpabVdGV2tYV1ltWHBiYnBR0gGcAUFVX3lxTE5JeEUxdlVpUzdSMkktZ0ROZmFkRTZvSGJHSHE4QWRxeW03S19PZmR3cWlVRno3SVBwV2dXc01UOUNoVFBsYWFkcmR2R3F4bnZXWWU0V1k4bm5ZZUo0U2VfclVPTjRlcUtiT0xnU0pfMlRzeXpJLXB1Y2NjMWxnVHhuUm9yS3FpUkEyM1hubzNMU01pOGRTcFNoNHBweg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
