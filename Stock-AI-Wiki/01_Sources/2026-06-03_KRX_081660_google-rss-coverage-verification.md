---
id: verification-2026-06-03-KRX-081660-google-rss-coverage
type: verification
title: KRX 081660 Google RSS Coverage Verification
created: 2026-06-03
updated: 2026-06-03
status: verification
stage: 1

market: KRX
ticker: "081660"
company: 미스토홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-03

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=081660
    - name=미스토홀딩스
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 081660
    - 미스토홀딩스
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

# KRX 081660 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-03_KRX_081660_google-rss-coverage-source]]

## Facts Checked
- `code=081660`
- `name=미스토홀딩스`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `휠라, 스니커즈 ‘리트모 슬릭’, ‘글리오’ 전개 - 패션포스트`
- Source: `패션포스트`
- Published at: `2026-06-01T06:09:18+09:00`
- Link: `https://news.google.com/rss/articles/CBMiekFVX3lxTE8yOFZuQ3ZSRndxYTBUcmFQd1ZLaXdnbFFhQXNqNmk5MGRsUW1wVTNoUUdqb1Q0NlRBU1ItWktzRVVWOUZvTXhaMWg3VVFKSm92OXpqcVBxbndkMEtkSHp1ODQ4Vk9hX2xJbHhpcDVYd0prY2o3Wmw3bWF3?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:17:43+09:00`
- Company: [[KRX_081660_미스토홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-03.json`
- Latest observation title: `미스토홀딩스, 지난해 연결 영업이익 31.6% 증가한 4748억 원 기록 - 패션포스트`
- Latest observation source: `패션포스트`
- Latest observation published_at: `2026-06-02T03:52:57+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiekFVX3lxTE9DR2VBN19uUXJ0Skc1Vm5LN19LaFhJb0VXa2Z3ZzkyTnU1cXNKOEo2dEl5SVg1MEdrZExPd05QdS1ndjdnNlB0UXJZaE5fMmV1bXZWMFFSZk42cU1hUU8xWW9taU5TQktqZFNfQ1Z4b0JiWDhzRzFTbHdR?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
