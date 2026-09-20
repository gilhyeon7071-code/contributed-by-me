---
id: verification-2026-06-07-KRX-071050-google-rss-coverage
type: verification
title: KRX 071050 Google RSS Coverage Verification
created: 2026-06-07
updated: 2026-06-07
status: verification
stage: 1

market: KRX
ticker: "071050"
company: 한국금융지주
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-07

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=071050
    - name=한국금융지주
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 071050
    - 한국금융지주
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

# KRX 071050 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-07_KRX_071050_google-rss-coverage-source]]

## Facts Checked
- `code=071050`
- `name=한국금융지주`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `기업은행, 한국투자금융 지분 매각 재개…정부 승인 추진 - 연합인포맥스`
- Source: `연합인포맥스`
- Published at: `2026-06-04T08:54:53+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTFBnWVBVMU1sWi00ZEtsTjdEdjJVVEt6eHJMU0cwdVV2cDVaZmlnUHVNLWRTWEs5cHA4SVhSNnJFamlHV1pKWFQ0V0VKbUMtZmRnanlTNXlqODdsamtaT0pORWw0WUlnZnlXd2l0TmdNMWw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:18:54+09:00`
- Company: [[KRX_071050_한국금융지주]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-07.json`
- Latest observation title: `기업은행, 한국투자금융 지분 매각 재개…정부 승인 추진 - 연합인포맥스`
- Latest observation source: `연합인포맥스`
- Latest observation published_at: `2026-06-04T08:54:53+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTFBnWVBVMU1sWi00ZEtsTjdEdjJVVEt6eHJMU0cwdVV2cDVaZmlnUHVNLWRTWEs5cHA4SVhSNnJFamlHV1pKWFQ0V0VKbUMtZmRnanlTNXlqODdsamtaT0pORWw0WUlnZnlXd2l0TmdNMWw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
