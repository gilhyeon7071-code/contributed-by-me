---
id: verification-2026-06-01-KRX-094170-google-rss-coverage
type: verification
title: KRX 094170 Google RSS Coverage Verification
created: 2026-06-01
updated: 2026-06-01
status: verification
stage: 1

market: KRX
ticker: "094170"
company: 동운아나텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-01

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=094170
    - name=동운아나텍
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 094170
    - 동운아나텍
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

# KRX 094170 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-01_KRX_094170_google-rss-coverage-source]]

## Facts Checked
- `code=094170`
- `name=동운아나텍`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `동운아나텍, 올해 타액 기반 혈당측정기 'D-SaLife' 판매 - 디일렉`
- Source: `디일렉`
- Published at: `2026-03-27T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE1BR1pPa1lxVThaS1BHcnRvTXJnOGpKMFF3dXRmaFhSSHhHRXFZUTkxRnVqWC1obnV0Y2tWLTc0eHNROUNKOUFncFRtUUF2N0tQX0ZHdG00VTcwRjFEZWdGTjNYWlBwQQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:17:10+09:00`
- Company: [[KRX_094170_동운아나텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-01.json`
- Latest observation title: `동운아나텍, 올해 타액 기반 혈당측정기 'D-SaLife' 판매 - 디일렉`
- Latest observation source: `디일렉`
- Latest observation published_at: `2026-03-27T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE1BR1pPa1lxVThaS1BHcnRvTXJnOGpKMFF3dXRmaFhSSHhHRXFZUTkxRnVqWC1obnV0Y2tWLTc0eHNROUNKOUFncFRtUUF2N0tQX0ZHdG00VTcwRjFEZWdGTjNYWlBwQQ?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
