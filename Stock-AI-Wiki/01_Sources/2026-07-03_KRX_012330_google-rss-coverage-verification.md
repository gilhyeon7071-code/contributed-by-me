---
id: verification-2026-07-03-KRX-012330-google-rss-coverage
type: verification
title: KRX 012330 Google RSS Coverage Verification
created: 2026-07-03
updated: 2026-07-03
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
  collected_at: 2026-07-03

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=012330
    - name=현대모비스
    - naver_article_count=4
    - google_rss_article_count=19
    - kis_title_count=11
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
- [[2026-07-03_KRX_012330_google-rss-coverage-source]]

## Facts Checked
- `code=012330`
- `name=현대모비스`
- `naver_article_count=4`
- `google_rss_article_count=19`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `신한證, 현대모비스 목표주가 하향…“피어그룹 부진 탓” [이런국장 저런주식] - 서울경제`
- Source: `서울경제`
- Published at: `2026-07-02T10:56:23+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE51LW80NkZhOXRiVFJ6emcwdGZTejVxYWtuM1NEXzVud0I0N3VrR3BXOWU3N0RJd3dFcXFnc05WamlfakptR3VPYmJMWU5FVzZUZHfSAVNBVV95cUxNYmgzVTJ4QTBWQnVCbzNKM0VBeVdwZDE2dnl0dWk1LTdwR2xRVDJqbTZOSldua2tBM2dqVjFqRHVDdHl3bDlFNE9Bakd0OWllZlBsZw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:26:32+09:00`
- Company: [[KRX_012330_현대모비스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-03.json`
- Latest observation title: `현대모비스, 해외 범퍼사업부 5천억에 판다 - 매일경제`
- Latest observation source: `매일경제`
- Latest observation published_at: `2026-07-02T22:58:50+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE9pX3AxYlJtMFRxdWstcDduOUZWbG45eUhXSHlSTTFKQVZTWU1xcTlRbmxKV01udkFZU1Z2aTQxYnpmSzFESzFkeHdzN29zUG80Szl6VUpn?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_exports_수출]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
