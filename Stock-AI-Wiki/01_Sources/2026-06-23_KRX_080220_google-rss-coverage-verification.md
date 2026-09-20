---
id: verification-2026-06-23-KRX-080220-google-rss-coverage
type: verification
title: KRX 080220 Google RSS Coverage Verification
created: 2026-06-23
updated: 2026-06-23
status: verification
stage: 1

market: KRX
ticker: "080220"
company: 제주반도체
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-23

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=080220
    - name=제주반도체
    - naver_article_count=2
    - google_rss_article_count=18
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 080220
    - 제주반도체
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

# KRX 080220 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-23_KRX_080220_google-rss-coverage-source]]

## Facts Checked
- `code=080220`
- `name=제주반도체`
- `naver_article_count=2`
- `google_rss_article_count=18`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `제주반도체 "LPDDR5도 SK하이닉스 공장서 만든다" - 디일렉`
- Source: `디일렉`
- Published at: `2026-06-17T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE15clBuSXZPTmx1SlBINTh0MVZJVWlXMU9OZzhXNHo0UllpRThCa3JOQlRzQ3hXdjcxLTdQMnNfOW05LWxVdk9LVUhWTW9lY3VPTmkyOTBsZDQ3QUdUOENGd3hjTF9YUQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-23T08:05:17+09:00`
- Company: [[KRX_080220_제주반도체]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-23.json`
- Latest observation title: `[특징주] 제주반도체, 반도체 수출 호조 속 12% 급등 - 이투데이`
- Latest observation source: `이투데이`
- Latest observation published_at: `2026-06-22T10:27:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVEFVX3lxTE5NMVdkeVpqakoxYlRPc2E3SW5zVk9hU0wzVjUxdXFYNzR1eW5KQ2NkVk9PVXpidk5iTjRpaXlVNk4ydWYxOUJzOGJCcUNzX0JtaE5QRg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
