---
id: verification-2026-06-18-KRX-002700-google-rss-coverage
type: verification
title: KRX 002700 Google RSS Coverage Verification
created: 2026-06-18
updated: 2026-06-18
status: verification
stage: 1

market: KRX
ticker: "002700"
company: 신일전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=002700
    - name=신일전자
    - naver_article_count=0
    - google_rss_article_count=8
    - kis_title_count=10
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 002700
    - 신일전자
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

# KRX 002700 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-18_KRX_002700_google-rss-coverage-source]]

## Facts Checked
- `code=002700`
- `name=신일전자`
- `naver_article_count=0`
- `google_rss_article_count=8`
- `kis_title_count=10`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[자사주 매입]신일전자, 20억원 자사주 취득 결정 - 데일리인베스트`
- Source: `데일리인베스트`
- Published at: `2026-06-17T14:30:51+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE90RzlLSFVHWDZIc2FDZjRYekVtS093RXpvWWJuMXNRTTVnUmRBVmNhWEltcDRBQ0c0ZkxNTFAyQ29YY01DcUtZNExScVVxSzFtMXRmUUtpazRycVRIQ21ZX21sckZ5UXVaMGdB0gFvQVVfeXFMTkpDZFpMY2ZXRWp6UG9SUGlSaktCQTlmR0JLRC01Z0V0d3JBb0Z1N1JmbWFUMV9acVY1b1J5d3o3SER5bXgta21NVDExaVVDUEc3bXFMdEl2M1YwcnJiQ21rS1d0YWZJc2RRLWtycG9r?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:22:04+09:00`
- Company: [[KRX_002700_신일전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-18.json`
- Latest observation title: `"동전주 벗어나자" 中企 임원·오너가, 주가부양 총력전 - 이데일리`
- Latest observation source: `이데일리`
- Latest observation published_at: `2026-06-18T15:59:50+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigAFBVV95cUxNMldiSUo1eGFDNnhaV21yRDUwVVQxY0Z4ZkFRajR1X3RSbmR0clRBTGhPdVNQX3JQdVBFVUotdUNYUzZzV2hFY1JyQ3lOVVlCeTUzaUNFd1BuY0RKb3UwSF9vUTVIOWNVYlBSdjBPVVRCVE1aZmplc2NLbXVuZld0Qg?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
