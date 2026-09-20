---
id: verification-2026-07-30-KRX-005490-google-rss-coverage
type: verification
title: KRX 005490 Google RSS Coverage Verification
created: 2026-07-30
updated: 2026-07-30
status: verification
stage: 1

market: KRX
ticker: "005490"
company: POSCO홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-30

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005490
    - name=POSCO홀딩스
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005490
    - POSCO홀딩스
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

# KRX 005490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-30_KRX_005490_google-rss-coverage-source]]

## Facts Checked
- `code=005490`
- `name=POSCO홀딩스`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `POSCO홀딩스 주가, 7월 29일 288,500원 3.50% 하락 마감 - 톱스타뉴스`
- Source: `톱스타뉴스`
- Published at: `2026-07-29T16:18:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTE56TjY4ZkNINjJOeDRJZzBOcnd3a2JwUDZGb0RkZ011STg2NXNmMjVjckVNZ0M2XzBjdmd2U0NVeUFsWDJsNDlwMzY3THlFd3NfNkJUTTVhV25PRmZ6SWo5X191RjhyR3VfT0F5QzBQLWZiUQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:33:12+09:00`
- Company: [[KRX_005490_POSCO홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-30.json`
- Latest observation title: `포스코홀딩스, 계열사 지분 3.5兆 처분에 배당↓…'4년 뒤 리튬'이 메울까 - KB Think`
- Latest observation source: `KB Think`
- Latest observation published_at: `2026-07-30T17:44:36+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE9PbE1fQkg2RmRtTUNha2pUWEN0TGFtWHAwNVFkRTI5RzN0aWJCbEM4UWd4NHljcHNneXZKT2tSSl9NNW9aNnE2VEdzV0FBSllvT1RsWnFHbERGVVhKSzBTY01SVUd3Q2JVU1V6ZV80ZkVNdw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
