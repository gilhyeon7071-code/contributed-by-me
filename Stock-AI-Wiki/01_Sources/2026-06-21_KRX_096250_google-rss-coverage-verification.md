---
id: verification-2026-06-21-KRX-096250-google-rss-coverage
type: verification
title: KRX 096250 Google RSS Coverage Verification
created: 2026-06-21
updated: 2026-06-21
status: verification
stage: 1

market: KRX
ticker: "096250"
company: 와이즈넛
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=096250
    - name=와이즈넛
    - naver_article_count=2
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 096250
    - 와이즈넛
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

# KRX 096250 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-21_KRX_096250_google-rss-coverage-source]]

## Facts Checked
- `code=096250`
- `name=와이즈넛`
- `naver_article_count=2`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `와이즈넛, 멀티모달 AI 모델 출시…‘산업 현장형 AI 에이전트’ 강화 - AI타임스`
- Source: `AI타임스`
- Published at: `2026-06-04T17:30:47+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTFAxSzR3YkRsZk94NVltZnkwSGxxeVpCbFJ2cXpvbGtvS0FTSkdabTVKcm14cFk3bGFuUHJEendWc21OQnZvNGJDZHBiNzlWQXlWN1hCaXZ2X1BwMHowZHFHZVR2WUVVOEo1OWc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-21T14:05:17+09:00`
- Company: [[KRX_096250_와이즈넛]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-21.json`
- Latest observation title: `와이즈넛, 멀티모달 AI 모델 출시…‘산업 현장형 AI 에이전트’ 강화 - AI타임스`
- Latest observation source: `AI타임스`
- Latest observation published_at: `2026-06-04T17:30:47+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTFAxSzR3YkRsZk94NVltZnkwSGxxeVpCbFJ2cXpvbGtvS0FTSkdabTVKcm14cFk3bGFuUHJEendWc21OQnZvNGJDZHBiNzlWQXlWN1hCaXZ2X1BwMHowZHFHZVR2WUVVOEo1OWc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
