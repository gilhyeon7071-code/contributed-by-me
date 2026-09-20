---
id: verification-2026-05-21-KRX-012330-google-rss-coverage
type: verification
title: KRX 012330 Google RSS Coverage Verification
created: 2026-05-21
updated: 2026-05-21
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
  collected_at: 2026-05-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=012330
    - name=현대모비스
    - naver_article_count=2
    - google_rss_article_count=22
    - kis_title_count=20
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
- [[2026-05-21_KRX_012330_google-rss-coverage-source]]

## Facts Checked
- `code=012330`
- `name=현대모비스`
- `naver_article_count=2`
- `google_rss_article_count=22`
- `kis_title_count=20`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `현대모비스 램프사업부 매각 갈등 고조…삭발식 이어 연대 파업 - 지디넷코리아`
- Source: `지디넷코리아`
- Published at: `2026-05-19T08:00:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE00b0xkbEhPS0doOE16dDE4LXZvR1I2Zkg4ODlTMGdyV05sLXJSbGtiblVqOG1FNUFJOUdsQkxkbkhIbnhKRkh6LWRtR21wUE9rRlplcEhB?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:51+09:00`
- Company: [[KRX_012330_현대모비스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `현대모비스 램프사업부 매각 갈등 고조…삭발식 이어 연대 파업 - 지디넷코리아`
- Latest observation source: `지디넷코리아`
- Latest observation published_at: `2026-05-19T08:00:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE00b0xkbEhPS0doOE16dDE4LXZvR1I2Zkg4ODlTMGdyV05sLXJSbGtiblVqOG1FNUFJOUdsQkxkbkhIbnhKRkh6LWRtR21wUE9rRlplcEhB?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
