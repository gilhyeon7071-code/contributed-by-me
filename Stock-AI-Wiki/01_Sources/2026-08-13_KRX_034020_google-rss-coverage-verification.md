---
id: verification-2026-08-13-KRX-034020-google-rss-coverage
type: verification
title: KRX 034020 Google RSS Coverage Verification
created: 2026-08-13
updated: 2026-08-13
status: verification
stage: 1

market: KRX
ticker: "034020"
company: 두산에너빌리티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-13

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=034020
    - name=두산에너빌리티
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034020
    - 두산에너빌리티
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

# KRX 034020 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-13_KRX_034020_google-rss-coverage-source]]

## Facts Checked
- `code=034020`
- `name=두산에너빌리티`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[시선강탈] 두산에너빌리티 vs 가온칩스 vs 두산테스나, 공략법은? - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-08-13T06:38:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE0xMHNaMXZsQzRVYWoxUUNsLVNRa3NOeks1UEU5T1lwSVgxVXJSMTVPamsyd0NYS0gyeDhBREswb0xoT0h5NENidzI1eC1ibEZ0c3oxNnRhTlR0T0w1M0N0M1hwQ3Y2TFVP0gFuQVVfeXFMTWx1dzJpNUxyLW1BeVZ6UlVKbzRkcVBEN3VYSzg4U3lJS2hVN2FNLXNSZmV3ZmZKWEpKZUFLRnhMRmprbEI5aDJmMFl6UEpCMXNoWk12eVk2ak82WTlURFIzekdMTjA3UFhGWjBZeVE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:35:49+09:00`
- Company: [[KRX_034020_두산에너빌리티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `AI주식상승확률분석 : 두산에너빌리티 (034020) 주가 전망과 상승확률 분석 - Investing.com 한국어`
- Latest observation source: `Investing.com 한국어`
- Latest observation published_at: `2026-08-12T14:09:54+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE1UTHF1TllyTllWekExTkx0TEZ6VmY5andmS0FCdm8zbldRTmxNUVVnbE9zcXFqb3o0ZUlMa1dDcHUtcGVWbDFLNjhtUm5pdzdTZjhBa3ZwVy1KQWhqbHNfdg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
