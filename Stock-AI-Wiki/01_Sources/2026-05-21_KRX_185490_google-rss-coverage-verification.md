---
id: verification-2026-05-21-KRX-185490-google-rss-coverage
type: verification
title: KRX 185490 Google RSS Coverage Verification
created: 2026-05-21
updated: 2026-05-21
status: verification
stage: 1

market: KRX
ticker: "185490"
company: 아이진
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=185490
    - name=아이진
    - naver_article_count=3
    - google_rss_article_count=27
    - kis_title_count=20
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 185490
    - 아이진
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

# KRX 185490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-21_KRX_185490_google-rss-coverage-source]]

## Facts Checked
- `code=185490`
- `name=아이진`
- `naver_article_count=3`
- `google_rss_article_count=27`
- `kis_title_count=20`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `아이진, 'mRNA기반' 한타바이러스 백신 개발추진 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-05-20T08:52:55+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE85WkJvdG1oZ3diNWdFVk9DWXlMLW9sbEx6V3JnX1BXNGFXMHhZUEw2NlhZUHRweS1MZnNqMGtDNUE0TGZRQ1pfbEtHUEpEeEIyYmxxdno1MUR6clNjQm9fQtIBeEFVX3lxTE9IamJicURQZHJnOWNMOFI3QlJSN0hVSzk0YndPMVNtd01lNjhvRWxvY0k5TTFfWERlaFBVNHZXX0NGVFFaQ29CR050cHQ2MTNFM3lHZW5XRDdzVkZ2WU5uSkVqV3JjbkhNUHpTaGxRZXJaYU5yOU1XbQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-21T14:05:06+09:00`
- Company: [[KRX_185490_아이진]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `아이진, 'mRNA기반' 한타바이러스 백신 개발추진 - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-05-20T08:52:55+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE85WkJvdG1oZ3diNWdFVk9DWXlMLW9sbEx6V3JnX1BXNGFXMHhZUEw2NlhZUHRweS1MZnNqMGtDNUE0TGZRQ1pfbEtHUEpEeEIyYmxxdno1MUR6clNjQm9fQtIBeEFVX3lxTE9IamJicURQZHJnOWNMOFI3QlJSN0hVSzk0YndPMVNtd01lNjhvRWxvY0k5TTFfWERlaFBVNHZXX0NGVFFaQ29CR050cHQ2MTNFM3lHZW5XRDdzVkZ2WU5uSkVqV3JjbkhNUHpTaGxRZXJaYU5yOU1XbQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
