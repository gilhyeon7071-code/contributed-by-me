---
id: verification-2026-05-19-KRX-212710-google-rss-coverage
type: verification
title: KRX 212710 Google RSS Coverage Verification
created: 2026-05-19
updated: 2026-05-19
status: verification
stage: 1

market: KRX
ticker: "212710"
company: 아이에스티이
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-19

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=212710
    - name=아이에스티이
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 212710
    - 아이에스티이
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

# KRX 212710 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-19_KRX_212710_google-rss-coverage-source]]

## Facts Checked
- `code=212710`
- `name=아이에스티이`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `아이에스티이, +7.82% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-18T14:06:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxQOFMtd3ZVa2JrQ3ZzYU1SM1luUnpWRklGdVBoeUR6SHFvSzJSMXdqbVV3M3J2RTVoRnJETmh2TTJjMEJ1V0Fvb01yNWtPVXBzSm5FY1AyQTdncHJ2X0JqMVZOdkk5Zk00Y0lpckhIalFkT25neGVSVVhKaVRrVXhPcWlMWdIBlwFBVV95cUxPN0dWbTY5Y3pCak5sYlhIYk02VVROdnBfVXJDLS16d25MUTRqQWI0Q1RNR1dTTFJ4MGo5NDBkaS0xdlBBdXZpU0JlTDdtaVFNamp0TjNaTXA3Tll2ZnhGMWJxVkdaVW52LVkydEdzeWtxcXhlZmJ6bm03VWVvbk5UdUFvaGFpanc0MVZZRmRleUgxejltNVFF?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:34+09:00`
- Company: [[KRX_212710_아이에스티이]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-19.json`
- Latest observation title: `아이에스티이, +7.82% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-18T14:01:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxPN0dWbTY5Y3pCak5sYlhIYk02VVROdnBfVXJDLS16d25MUTRqQWI0Q1RNR1dTTFJ4MGo5NDBkaS0xdlBBdXZpU0JlTDdtaVFNamp0TjNaTXA3Tll2ZnhGMWJxVkdaVW52LVkydEdzeWtxcXhlZmJ6bm03VWVvbk5UdUFvaGFpanc0MVZZRmRleUgxejltNVFF0gGXAUFVX3lxTE83R1ZtNjljekJqTmxiWEhiTTZVVE52cF9VckMtLXp3bkxRNGpBYjRDVE1HV1NMUngwajk0MGRpLTF2UEF1dmlTQmVMN21pUU1qanROM1pNcDdOWXZmeEYxYnFWR1pVbnYtWTJ0R3N5a3FxeGVmYnpubTdVZW9uTlR1QW9oYWlqdzQxVllGZGV5SDF6OW01UUU?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
