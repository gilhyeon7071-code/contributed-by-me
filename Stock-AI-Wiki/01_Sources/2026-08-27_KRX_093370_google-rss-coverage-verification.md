---
id: verification-2026-08-27-KRX-093370-google-rss-coverage
type: verification
title: KRX 093370 Google RSS Coverage Verification
created: 2026-08-27
updated: 2026-08-27
status: verification
stage: 1

market: KRX
ticker: "093370"
company: 후성
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=093370
    - name=후성
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 093370
    - 후성
  possible_impact: unknown
  uncertainty:
    - Article body archive is available locally, but entity/event verification is still unknown.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: true
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

# KRX 093370 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-27_KRX_093370_google-rss-coverage-source]]

## Facts Checked
- `code=093370`
- `name=후성`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[장중수급포착] 후성, 외국인/기관 동시 순매수… 주가 +4.99% - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-08-27T11:31:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE1qaFR5a3d1UE5iOURHSzIwMGRiUXQ3RDJuUFI5ZkRMamxoRk1Jb1dCczBKM29qeFNkRnEyYWwxcXQyS0h5SDVSUWNjdkxPU0ROdzZuSldmdGJ3Z1o5?oc=5`

## Article Body Archive Checked
- Title: `[장중수급포착] 후성, 외국인/기관 동시 순매수… 주가 +4.99% - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-08-27T11:31:00+09:00`
- URL: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE1qaFR5a3d1UE5iOURHSzIwMGRiUXQ3RDJuUFI5ZkRMamxoRk1Jb1dCczBKM29qeFNkRnEyYWwxcXQyS0h5SDVSUWNjdkxPU0ROdzZuSldmdGJ3Z1o5?oc=5`
- Evidence path: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE1qaFR5a3d1UE5iOURHSzIwMGRiUXQ3RDJuUFI5ZkRMamxoRk1Jb1dCczBKM29qeFNkRnEyYWwxcXQyS0h5SDVSUWNjdkxPU0ROdzZuSldmdGJ3Z1o5?oc=5`
- Body excerpt: [장중수급포착] 후성, 외국인/기관 동시 순매수… 주가 +4.99% 뉴스핌

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-27T23:05:22+09:00`
- Company: [[KRX_093370_후성]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-27.json`
- Latest observation title: `[장중수급포착] 후성, 외국인/기관 동시 순매수… 주가 +4.99% - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-08-27T11:31:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE1qaFR5a3d1UE5iOURHSzIwMGRiUXQ3RDJuUFI5ZkRMamxoRk1Jb1dCczBKM29qeFNkRnEyYWwxcXQyS0h5SDVSUWNjdkxPU0ROdzZuSldmdGJ3Z1o5?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
