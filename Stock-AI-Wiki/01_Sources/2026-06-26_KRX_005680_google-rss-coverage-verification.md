---
id: verification-2026-06-26-KRX-005680-google-rss-coverage
type: verification
title: KRX 005680 Google RSS Coverage Verification
created: 2026-06-26
updated: 2026-06-26
status: verification
stage: 1

market: KRX
ticker: "005680"
company: 삼영전자공업
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005680
    - name=삼영전자공업
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005680
    - 삼영전자공업
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

# KRX 005680 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-26_KRX_005680_google-rss-coverage-source]]

## Facts Checked
- `code=005680`
- `name=삼영전자공업`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼영전자공업, 불성실공시법인 지정→10,000,000원 제재 - TopStarNews`
- Source: `TopStarNews`
- Published at: `2026-06-24T16:01:51+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTFBZZkdFZGxMRnAyZTFkZmlDejJfSWNQMlRPVk5HZkFmZmVIemJzSWJ5dHQ0YWp3bk1hbVd5VDJXUkJuRGhMb0Vadkh2c293WFNkTnJHZG1UZ19ZTG1yZFJmQmlRNGJxaGY3Y21tSWVxRzc2Zw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:24:38+09:00`
- Company: [[KRX_005680_삼영전자공업]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-26.json`
- Latest observation title: `삼영전자공업, 불성실공시법인 지정→10,000,000원 제재 - TopStarNews`
- Latest observation source: `TopStarNews`
- Latest observation published_at: `2026-06-24T16:01:51+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTFBZZkdFZGxMRnAyZTFkZmlDejJfSWNQMlRPVk5HZkFmZmVIemJzSWJ5dHQ0YWp3bk1hbVd5VDJXUkJuRGhMb0Vadkh2c293WFNkTnJHZG1UZ19ZTG1yZFJmQmlRNGJxaGY3Y21tSWVxRzc2Zw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
