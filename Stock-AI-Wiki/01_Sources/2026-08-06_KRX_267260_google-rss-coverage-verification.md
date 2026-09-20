---
id: verification-2026-08-06-KRX-267260-google-rss-coverage
type: verification
title: KRX 267260 Google RSS Coverage Verification
created: 2026-08-06
updated: 2026-08-06
status: verification
stage: 1

market: KRX
ticker: "267260"
company: HD현대일렉트릭
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=267260
    - name=HD현대일렉트릭
    - naver_article_count=11
    - google_rss_article_count=139
    - kis_title_count=36
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 267260
    - HD현대일렉트릭
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

# KRX 267260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-06_KRX_267260_google-rss-coverage-source]]

## Facts Checked
- `code=267260`
- `name=HD현대일렉트릭`
- `naver_article_count=11`
- `google_rss_article_count=139`
- `kis_title_count=36`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `하나증권 "AI 데이터센터 증설 장기간 이어질 가능성 높아, HD현대일렉트릭 효성중공업 LS일렉트릭 수혜" - 비즈니스포스트`
- Source: `비즈니스포스트`
- Published at: `2026-08-05T08:53:38+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE40dDl1RVRFemxIZFpEMF83dTQ1V0RZZ1czV05NTlJQRmxwSTd5c1pyaXV6aHIyYmdVU3BoS2JWNjA4d2J1RmdnZkNsbEZKZl83bmhHSVFYeExOZnd1Z0JocTdWR1VRTFh2cmM4N3dOeWNkc28?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-06T16:05:15+09:00`
- Company: [[KRX_267260_HD현대일렉트릭]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `HD현대일렉트릭 배전기기 실적 반등…데이터센터 수주↑ - 데이터뉴스`
- Latest observation source: `데이터뉴스`
- Latest observation published_at: `2026-08-06T08:22:45+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiX0FVX3lxTE5IVDV4TGItVm5fU1gwSkcwUWFrc2NZV3NBa25WNU13S1ZnNkNaMW9lLUhDaV9vd2lCQTFLbEhOWHVNRTNWT1NwZnVxMmJscFNBbXhDanFfQUdLdWhHY3Iw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
