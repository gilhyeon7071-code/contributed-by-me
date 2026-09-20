---
id: verification-2026-07-03-KRX-095340-google-rss-coverage
type: verification
title: KRX 095340 Google RSS Coverage Verification
created: 2026-07-03
updated: 2026-07-03
status: verification
stage: 1

market: KRX
ticker: "095340"
company: ISC
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
    - code=095340
    - name=ISC
    - naver_article_count=8
    - google_rss_article_count=1
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 095340
    - ISC
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

# KRX 095340 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-03_KRX_095340_google-rss-coverage-source]]

## Facts Checked
- `code=095340`
- `name=ISC`
- `naver_article_count=8`
- `google_rss_article_count=1`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `01일, 외국인 코스닥에서 리노공업(-2.74%), ISC(-5.95%) 등 순매수 - 씽크풀 AI`
- Source: `씽크풀 AI`
- Published at: `2026-07-02T11:25:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9KVWRreHZtRlYzX3ZISG92UXhyWVZZeFdHR3IyaFNTZ2RoUFBtQnRxMTdZVHZpQ1FuVEY4ZGQ3SlVYM0xQNUlVdWgwU2Jzall4UzhZamdDYjQ3cWZuSTdwNEp5MklHY2xf?oc=5`

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
- Company: [[KRX_095340_ISC]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-03.json`
- Latest observation title: `[장중수급포착] ISC, 외국인 7일 연속 순매수행진... 주가 +1.69% - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-07-03T10:16:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE1xblpXWjUyWVduSzQ1by1Ta21ndTN1UnkyZmFNekZ1aTJoM2tEazBsWDA5Y0tUUDRpbnhtUUk1U0Q5dk4zTXNLR0NmOG53SDFDeWtyQ1dVNVMtcDBZ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_eco-packaging_친환경-패키징]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
