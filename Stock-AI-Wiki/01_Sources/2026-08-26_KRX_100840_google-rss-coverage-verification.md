---
id: verification-2026-08-26-KRX-100840-google-rss-coverage
type: verification
title: KRX 100840 Google RSS Coverage Verification
created: 2026-08-26
updated: 2026-08-26
status: verification
stage: 1

market: KRX
ticker: "100840"
company: SNT에너지
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=100840
    - name=SNT에너지
    - naver_article_count=0
    - google_rss_article_count=6
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 100840
    - SNT에너지
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

# KRX 100840 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-26_KRX_100840_google-rss-coverage-source]]

## Facts Checked
- `code=100840`
- `name=SNT에너지`
- `naver_article_count=0`
- `google_rss_article_count=6`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[N2 증시 풍향계] 건설株 '들썩'…SNT에너지·한전산업 '급등' 시그네틱스 '上' - 뉴스투데이`
- Source: `뉴스투데이`
- Published at: `2026-08-26T10:29:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE5HdlNqUHQxb1V2WkVyOHRfd1JVaTdMWFhVUWhZenNnMDg0Sk9jbkpwX1daMWYxVHhveXVoeVA2SjM3OVlzR3JSTjdielV2ZUdvVXdJZmloOElJeUxscEE?oc=5`

## Article Body Archive Checked
- Title: `[N2 증시 풍향계] 건설株 '들썩'…SNT에너지·한전산업 '급등' 시그네틱스 '上' - 뉴스투데이`
- Source: `뉴스투데이`
- Published at: `2026-08-26T10:29:00+09:00`
- URL: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE5HdlNqUHQxb1V2WkVyOHRfd1JVaTdMWFhVUWhZenNnMDg0Sk9jbkpwX1daMWYxVHhveXVoeVA2SjM3OVlzR3JSTjdielV2ZUdvVXdJZmloOElJeUxscEE?oc=5`
- Evidence path: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE5HdlNqUHQxb1V2WkVyOHRfd1JVaTdMWFhVUWhZenNnMDg0Sk9jbkpwX1daMWYxVHhveXVoeVA2SjM3OVlzR3JSTjdielV2ZUdvVXdJZmloOElJeUxscEE?oc=5`
- Body excerpt: [N2 증시 풍향계] 건설株 '들썩'…SNT에너지·한전산업 '급등' 시그네틱스 '上' 뉴스투데이

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-26T23:05:22+09:00`
- Company: [[KRX_100840_SNT에너지]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-26.json`
- Latest observation title: `[N2 증시 풍향계] 건설株 '들썩'…SNT에너지·한전산업 '급등' 시그네틱스 '上' - 뉴스투데이`
- Latest observation source: `뉴스투데이`
- Latest observation published_at: `2026-08-26T10:29:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE5HdlNqUHQxb1V2WkVyOHRfd1JVaTdMWFhVUWhZenNnMDg0Sk9jbkpwX1daMWYxVHhveXVoeVA2SjM3OVlzR3JSTjdielV2ZUdvVXdJZmloOElJeUxscEE?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
