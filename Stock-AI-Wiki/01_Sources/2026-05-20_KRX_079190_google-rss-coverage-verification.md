---
id: verification-2026-05-20-KRX-079190-google-rss-coverage
type: verification
title: KRX 079190 Google RSS Coverage Verification
created: 2026-05-20
updated: 2026-05-20
status: verification
stage: 1

market: KRX
ticker: "079190"
company: 케스피온
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=079190
    - name=케스피온
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 079190
    - 케스피온
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

# KRX 079190 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-20_KRX_079190_google-rss-coverage-source]]

## Facts Checked
- `code=079190`
- `name=케스피온`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[위기의 상장사] 액면병합한 케스피온…신사업 성과 지연에 시총 방어 '부담' - 딜사이트`
- Source: `딜사이트`
- Published at: `2026-05-15T08:15:13+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE11eDFnZ21ZeTJfNUpfeTZTa3owSGxpeWstVFlCbV9ULUJhTWtqaWwxcWVIemVFbnd6MTFjaTQyaktycXRBcnlVQ1o2dkx5Qjg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-20T08:45:13+09:00`
- Company: [[KRX_079190_케스피온]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `[위기의 상장사] 액면병합한 케스피온…신사업 성과 지연에 시총 방어 '부담' - 딜사이트`
- Latest observation source: `딜사이트`
- Latest observation published_at: `2026-05-15T08:15:13+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE11eDFnZ21ZeTJfNUpfeTZTa3owSGxpeWstVFlCbV9ULUJhTWtqaWwxcWVIemVFbnd6MTFjaTQyaktycXRBcnlVQ1o2dkx5Qjg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
