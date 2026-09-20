---
id: verification-2026-06-05-KRX-105840-google-rss-coverage
type: verification
title: KRX 105840 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "105840"
company: 우진
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=105840
    - name=우진
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 105840
    - 우진
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

# KRX 105840 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_105840_google-rss-coverage-source]]

## Facts Checked
- `code=105840`
- `name=우진`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `수급업자 기술자료 무단 요구…우진산전, 과징금 1억2천600만원 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-06-04T12:00:07+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9PVjc0VURYZDZYS3RlSWZKUklPRFVIaXoyQ3FNcGh0WDczcWpiTy1RQ3hHVlVqZlFKcUE5NEZDMWVUR2o4b3RTekpWd09SZDNZSnAwTU5GaEYzZjU4RUZUX9IBYEFVX3lxTE9PVjc0VURYZDZYS3RlSWZKUklPRFVIaXoyQ3FNcGh0WDczcWpiTy1RQ3hHVlVqZlFKcUE5NEZDMWVUR2o4b3RTekpWd09SZDNZSnAwTU5GaEYzZjU4RUZUXw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-05T08:11:49+09:00`
- Company: [[KRX_105840_우진]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `수급업자 기술자료 무단 요구…우진산전, 과징금 1억2천600만원 - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-06-04T12:00:07+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9PVjc0VURYZDZYS3RlSWZKUklPRFVIaXoyQ3FNcGh0WDczcWpiTy1RQ3hHVlVqZlFKcUE5NEZDMWVUR2o4b3RTekpWd09SZDNZSnAwTU5GaEYzZjU4RUZUX9IBYEFVX3lxTE9PVjc0VURYZDZYS3RlSWZKUklPRFVIaXoyQ3FNcGh0WDczcWpiTy1RQ3hHVlVqZlFKcUE5NEZDMWVUR2o4b3RTekpWd09SZDNZSnAwTU5GaEYzZjU4RUZUXw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
