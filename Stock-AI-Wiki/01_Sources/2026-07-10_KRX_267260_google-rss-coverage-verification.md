---
id: verification-2026-07-10-KRX-267260-google-rss-coverage
type: verification
title: KRX 267260 Google RSS Coverage Verification
created: 2026-07-10
updated: 2026-07-10
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
  collected_at: 2026-07-10

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=267260
    - name=HD현대일렉트릭
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
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
- [[2026-07-10_KRX_267260_google-rss-coverage-source]]

## Facts Checked
- `code=267260`
- `name=HD현대일렉트릭`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `에너닷·HD현대일렉 컨소, ‘AI 배전망 ESS 구축 사업’ 최종 선정 - 전기신문`
- Source: `전기신문`
- Published at: `2026-07-10T12:07:47+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE9waTliN1k1RmUwVlhCZmh5aEo0WUl3ZFl6eEQxOG81RDhRTTFEZ29ZdEdjRmduRkF3MHAybW9MU0k1c0pHRUVBNElqbVdiWjl3MjJUSDYxOFlVTkRxRlpCVnVwbDJQWU1ZOTc4N9IBcEFVX3lxTFBZbXd1NjJOMGJNalRUME9NelRuR0czai04VjMwV0N1bzBndjdqaHljdzJBczVxNEZ0V003ODl4SVZQaVVPcGotQ0VsZm5UckhTWlVNY0M1Nl8zWjZpdzF5Y0pPX0tYUVhweExXekV0U0Q?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:28:24+09:00`
- Company: [[KRX_267260_HD현대일렉트릭]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-10.json`
- Latest observation title: `‘데이터센터 겨냥 스마트 분전반부터 통합 BESS까지’...HD현대일렉트릭, 베트남 현지서 공개 - 전기신문`
- Latest observation source: `전기신문`
- Latest observation published_at: `2026-07-10T13:51:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE1WWmVQZXRoU0o1eTV3Wm9hUjJtLVM0RDhsbDdwUFo2VllwYzVjNFBPd0k2ZGFnV2hCRnNybFBKblFQUERHc3M1bkN0TUZvVHo5SV82c2tjS0U1Zkw1WU1fdWZLRExoMTg3Wkp5a9IBcEFVX3lxTE9GcnBkV1VodGhHWUVDckNvZGNUY3hJYjVhQkdEUF91cTY3SUQ0WE9vQlNnWVRuam56WVFlTTVVbWVoamhOVU9EVlVEWVdVdHktTGFqaGtFTUR5bTlhTmFmbTRTY3cxdENNQXg3Rm1zcWw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
