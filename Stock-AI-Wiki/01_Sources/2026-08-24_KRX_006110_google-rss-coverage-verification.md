---
id: verification-2026-08-24-KRX-006110-google-rss-coverage
type: verification
title: KRX 006110 Google RSS Coverage Verification
created: 2026-08-24
updated: 2026-08-24
status: verification
stage: 1

market: KRX
ticker: "006110"
company: 삼아알미늄
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-24

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=006110
    - name=삼아알미늄
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 006110
    - 삼아알미늄
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

# KRX 006110 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-24_KRX_006110_google-rss-coverage-source]]

## Facts Checked
- `code=006110`
- `name=삼아알미늄`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `삼아알미늄 주가, 급등세... 무슨 회사길래? - 금강일보`
- Source: `금강일보`
- Published at: `2026-08-24T14:11:18+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5Vd3hlTUJ6TGtFWEppY1EtMkFHeE8xV1BlNUJLMV9zOUo0Yk04bmN0NUhMd0RvZUw4RmhyRG1wX3V1TFdZZ1hVTUtFRVd3T29qNmM2dkxDOGFzLTk3R0ZUTVhZS2JfREwy?oc=5`

## Article Body Archive Checked
- Title: `삼아알미늄 주가, 급등세... 무슨 회사길래? - 금강일보`
- Source: `금강일보`
- Published at: `2026-08-24T14:11:18+09:00`
- URL: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5Vd3hlTUJ6TGtFWEppY1EtMkFHeE8xV1BlNUJLMV9zOUo0Yk04bmN0NUhMd0RvZUw4RmhyRG1wX3V1TFdZZ1hVTUtFRVd3T29qNmM2dkxDOGFzLTk3R0ZUTVhZS2JfREwy?oc=5`
- Evidence path: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5Vd3hlTUJ6TGtFWEppY1EtMkFHeE8xV1BlNUJLMV9zOUo0Yk04bmN0NUhMd0RvZUw4RmhyRG1wX3V1TFdZZ1hVTUtFRVd3T29qNmM2dkxDOGFzLTk3R0ZUTVhZS2JfREwy?oc=5`
- Body excerpt: 삼아알미늄 주가, 급등세... 무슨 회사길래? 금강일보

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-24T23:05:34+09:00`
- Company: [[KRX_006110_삼아알미늄]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-24.json`
- Latest observation title: `삼아알미늄, 투자주의종목 지정 사유 공시 - 톱스타뉴스`
- Latest observation source: `톱스타뉴스`
- Latest observation published_at: `2026-08-24T20:20:56+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE16dXBiazJLUXV3LS1TNlJjWWRKVU9PSjJRaWNKck1Hd1dQcUl2eUVfVWRrdTJ5MUlJRnpIdmZJVWtSUlQ5d0pzV25rU1ZYUHI3UHlYcXV2UXNVYkxwSjlMR0h3VGFMZ2xoSTViY3lRdWxPUQ?oc=5`
- Body status: `fetch_failed_no_body`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
