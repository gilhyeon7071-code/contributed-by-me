---
id: verification-2026-06-19-KRX-063160-google-rss-coverage
type: verification
title: KRX 063160 Google RSS Coverage Verification
created: 2026-06-19
updated: 2026-06-19
status: verification
stage: 1

market: KRX
ticker: "063160"
company: 종근당바이오
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-19

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=063160
    - name=종근당바이오
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=9
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 063160
    - 종근당바이오
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

# KRX 063160 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-19_KRX_063160_google-rss-coverage-source]]

## Facts Checked
- `code=063160`
- `name=종근당바이오`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=9`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `종근당바이오, 연내 보톡스 中당국 허가 추진....경쟁력 분석해보니 - 팜이데일리`
- Source: `팜이데일리`
- Published at: `2026-05-24T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE94VHNPN00tTEJkTHF4enJNaVE0Z2hDblNhaTdGbklOWEdtcW9YaHNIRTRVa3JZcWhJc0NXY0ZrRDZaNkpzMHVCOVZvZWpCaDdMbTVfaXFNNmF1NGFXRWlkWEpaeEFVUW15NWpRTU5B?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:22:23+09:00`
- Company: [[KRX_063160_종근당바이오]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-19.json`
- Latest observation title: `종근당바이오, 연내 보톡스 中당국 허가 추진....경쟁력 분석해보니 - 팜이데일리`
- Latest observation source: `팜이데일리`
- Latest observation published_at: `2026-05-24T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTE94VHNPN00tTEJkTHF4enJNaVE0Z2hDblNhaTdGbklOWEdtcW9YaHNIRTRVa3JZcWhJc0NXY0ZrRDZaNkpzMHVCOVZvZWpCaDdMbTVfaXFNNmF1NGFXRWlkWEpaeEFVUW15NWpRTU5B?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
