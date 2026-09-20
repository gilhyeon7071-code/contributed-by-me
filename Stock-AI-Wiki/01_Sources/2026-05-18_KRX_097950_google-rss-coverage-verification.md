---
id: verification-2026-05-18-KRX-097950-google-rss-coverage
type: verification
title: KRX 097950 Google RSS Coverage Verification
created: 2026-05-18
updated: 2026-05-18
status: verification
stage: 1

market: KRX
ticker: "097950"
company: CJ제일제당
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=097950
    - name=CJ제일제당
    - naver_article_count=0
    - google_rss_article_count=8
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 097950
    - CJ제일제당
  possible_impact: unknown
  uncertainty:
    - Original article text unavailable.

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

# KRX 097950 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-18_KRX_097950_google-rss-coverage-source]]

## Facts Checked
- `code=097950`
- `name=CJ제일제당`
- `naver_article_count=0`
- `google_rss_article_count=8`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:22+09:00`
- Company: [[KRX_097950_CJ제일제당]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-18.json`
- Latest observation title: `CJ제일제당, 1분기 매출 4조271억 원…해외 만두·K-푸드 성장세 지속 - 한국식품의약신문`
- Latest observation source: `한국식품의약신문`
- Latest observation published_at: `2026-05-15T11:38:13+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTE1JdF9zM0ZWTHZXbEY1aV8yUzgwZVBRTF9KQmM1MHoyQmFrbEhtS3c0aHZSdWtXRVpuM3l1MDhBbVZMVC0wSmM1c3BERGRZbi1FMXFQRkdHbTBPbW5EREVXREZYc2ZSdVE?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
