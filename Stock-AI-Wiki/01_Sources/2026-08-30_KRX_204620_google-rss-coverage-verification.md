---
id: verification-2026-08-30-KRX-204620-google-rss-coverage
type: verification
title: KRX 204620 Google RSS Coverage Verification
created: 2026-08-30
updated: 2026-08-30
status: verification
stage: 1

market: KRX
ticker: "204620"
company: 글로벌텍스프리
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-30

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=204620
    - name=글로벌텍스프리
    - naver_article_count=0
    - google_rss_article_count=34
    - kis_title_count=11
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 204620
    - 글로벌텍스프리
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

# KRX 204620 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-30_KRX_204620_google-rss-coverage-source]]

## Facts Checked
- `code=204620`
- `name=글로벌텍스프리`
- `naver_article_count=0`
- `google_rss_article_count=34`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[장중수급포착] 글로벌텍스프리, 외국인/기관 동시 순매수… 주가 +15.09% - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-08-28T13:30:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5HVDV1Wnhwb0RXVkNSVUw0NHZtUTgydkNxUHBQb3VfU2hYci1jdEFJSUpRRWRDM0VGbEdRdWpHYXZjb19jTUFwV2I0OXZTZW9fc1lIcFVOajZ4SWNF?oc=5`

## Article Body Archive Checked
- Title: `[장중수급포착] 글로벌텍스프리, 외국인/기관 동시 순매수… 주가 +15.09% - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-08-28T13:30:00+09:00`
- URL: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5HVDV1Wnhwb0RXVkNSVUw0NHZtUTgydkNxUHBQb3VfU2hYci1jdEFJSUpRRWRDM0VGbEdRdWpHYXZjb19jTUFwV2I0OXZTZW9fc1lIcFVOajZ4SWNF?oc=5`
- Evidence path: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5HVDV1Wnhwb0RXVkNSVUw0NHZtUTgydkNxUHBQb3VfU2hYci1jdEFJSUpRRWRDM0VGbEdRdWpHYXZjb19jTUFwV2I0OXZTZW9fc1lIcFVOajZ4SWNF?oc=5`
- Body excerpt: [장중수급포착] 글로벌텍스프리, 외국인/기관 동시 순매수… 주가 +15.09% 뉴스핌

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-30T23:05:30+09:00`
- Company: [[KRX_204620_글로벌텍스프리]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-30.json`
- Latest observation title: `[장중수급포착] 글로벌텍스프리, 외국인/기관 동시 순매수… 주가 +15.09% - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-08-28T13:30:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5HVDV1Wnhwb0RXVkNSVUw0NHZtUTgydkNxUHBQb3VfU2hYci1jdEFJSUpRRWRDM0VGbEdRdWpHYXZjb19jTUFwV2I0OXZTZW9fc1lIcFVOajZ4SWNF?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
