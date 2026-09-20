---
id: verification-2026-07-01-KRX-108490-google-rss-coverage
type: verification
title: KRX 108490 Google RSS Coverage Verification
created: 2026-07-01
updated: 2026-07-01
status: verification
stage: 1

market: KRX
ticker: "108490"
company: 로보티즈
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-01

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=108490
    - name=로보티즈
    - naver_article_count=3
    - google_rss_article_count=5
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 108490
    - 로보티즈
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

# KRX 108490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-01_KRX_108490_google-rss-coverage-source]]

## Facts Checked
- `code=108490`
- `name=로보티즈`
- `naver_article_count=3`
- `google_rss_article_count=5`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[리포트 브리핑]로보티즈, '대규모 양산체제 구축/운영 선도' 목표가 320,000원 - 한화투자증권 - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-07-01T09:47:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE12T2l3eElxTF80d0VTcW1WLTNaXzZuM0RKREtXVlFnWHJrTEQ1NXhrblR3UHdIQkotSmw3MTl6Umk4WFhfZENhNEd6VlI2clgyZXVwVXhaMC1vRFZx?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-01T15:05:12+09:00`
- Company: [[KRX_108490_로보티즈]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-01.json`
- Latest observation title: `[리포트 브리핑]로보티즈, '대규모 양산체제 구축/운영 선도' 목표가 320,000원 - 한화투자증권 - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-07-01T09:47:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE12T2l3eElxTF80d0VTcW1WLTNaXzZuM0RKREtXVlFnWHJrTEQ1NXhrblR3UHdIQkotSmw3MTl6Umk4WFhfZENhNEd6VlI2clgyZXVwVXhaMC1vRFZx?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
