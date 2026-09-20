---
id: verification-2026-07-26-KRX-277810-google-rss-coverage
type: verification
title: KRX 277810 Google RSS Coverage Verification
created: 2026-07-26
updated: 2026-07-26
status: verification
stage: 1

market: KRX
ticker: "277810"
company: 레인보우로보틱스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=277810
    - name=레인보우로보틱스
    - naver_article_count=2
    - google_rss_article_count=13
    - kis_title_count=28
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 277810
    - 레인보우로보틱스
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

# KRX 277810 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-26_KRX_277810_google-rss-coverage-source]]

## Facts Checked
- `code=277810`
- `name=레인보우로보틱스`
- `naver_article_count=2`
- `google_rss_article_count=13`
- `kis_title_count=28`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼성전자 'RX'사업부 신설...레인보우로보틱스 없이 독자적 로봇 사업 승부수 - 팝콘뉴스`
- Source: `팝콘뉴스`
- Published at: `2026-07-24T07:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE1temhiZWNJNTB4dDN5UDZMamZsNTh0dXNMeFpuRkhCUW1pcmpJdFhfV0lDY2ZkMHNaMkxTbk1ockY2NFpFVS00WndNQXpXbWp5UGZxYU9aZjJGdDA3ODlyREJlOG5vWGdGSHRNZ2pn0gFyQVVfeXFMUG51VjRjSlB0WDNGclRrTEhWT2lBVTNMQlhDcHhvM00yY2htUkxFTzNzeVo4WUpibVc5NVFrXzAtSFhkOFlTOS0zM2JIQm9YLU5wOEM4RkdfSnlhU2JKQmkwS1JkUjE5Y1JLNUFMY29ZQU9R?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:31:54+09:00`
- Company: [[KRX_277810_레인보우로보틱스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-26.json`
- Latest observation title: `기관 '파마리서치'·외인 '알테오젠'·개인 '레인보우로보틱스' 1위 [주간 코스닥 순매수- 2026년 7월20일~7월24일] - 한국금융신문`
- Latest observation source: `한국금융신문`
- Latest observation published_at: `2026-07-26T12:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMifEFVX3lxTE9RbEEwUDJucU1DbEJPUk5NbWY5eXE2bzc4WkdSVmNLRXhEdEdRYmxTNkFlTW9VU1FRdDdjXzBnTklzeTVJUktfaklQNFE2eS1va3dwQjJHQ1lBdnItS0ZsMG5YT3E0Ul9QRnE4dy1ZSGE5cFJFUTFGOXYzNi0?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
