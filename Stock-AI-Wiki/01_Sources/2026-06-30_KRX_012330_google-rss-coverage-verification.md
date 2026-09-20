---
id: verification-2026-06-30-KRX-012330-google-rss-coverage
type: verification
title: KRX 012330 Google RSS Coverage Verification
created: 2026-06-30
updated: 2026-06-30
status: verification
stage: 1

market: KRX
ticker: "012330"
company: 현대모비스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-30

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=012330
    - name=현대모비스
    - naver_article_count=5
    - google_rss_article_count=3
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 012330
    - 현대모비스
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

# KRX 012330 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-30_KRX_012330_google-rss-coverage-source]]

## Facts Checked
- `code=012330`
- `name=현대모비스`
- `naver_article_count=5`
- `google_rss_article_count=3`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `현대모비스, 동반성장 성과 담은 '지속가능보고서 2026' 발간 - 더팩트`
- Source: `더팩트`
- Published at: `2026-06-29T10:08:50+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE40QWtTbnl0NGMyZGN0UFFobUltR0R5cTNscnBHQXppS2VWOHl4dVg0QnV1amRBTkxhb0Y1TVpQOS1aOU1sZlJTSlplRkZQcVA5d01UZGMtZDNJQdIBVEFVX3lxTE91T2tIMTFnWVpwSW1tb2FsS0NlaHA0Ql90VkZWMTVmSmEtMC1SbGZWREhLUXdkWEd2bVRfS0I1QTJzSGdpRzF1NFdwa0ROX19VNDNvcw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:25:36+09:00`
- Company: [[KRX_012330_현대모비스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-30.json`
- Latest observation title: `현대모비스, 동반성장 성과 담은 '지속가능보고서 2026' 발간 - 더팩트`
- Latest observation source: `더팩트`
- Latest observation published_at: `2026-06-29T10:08:50+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE40QWtTbnl0NGMyZGN0UFFobUltR0R5cTNscnBHQXppS2VWOHl4dVg0QnV1amRBTkxhb0Y1TVpQOS1aOU1sZlJTSlplRkZQcVA5d01UZGMtZDNJQdIBVEFVX3lxTE91T2tIMTFnWVpwSW1tb2FsS0NlaHA0Ql90VkZWMTVmSmEtMC1SbGZWREhLUXdkWEd2bVRfS0I1QTJzSGdpRzF1NFdwa0ROX19VNDNvcw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
