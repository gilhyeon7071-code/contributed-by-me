---
id: source-2026-06-16-KRX-000720-google-rss-coverage
type: source
title: KRX 000720 Google RSS Coverage Source
created: 2026-06-16
updated: 2026-06-16
status: raw
stage: 0

market: KRX
ticker: "000720"
company: 현대건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-16

analysis:
  summary: Local coverage report row shows news coverage for KRX 000720.
  key_facts:
    - code=000720
    - name=현대건설
    - naver_article_count=1
    - google_rss_article_count=28
    - kis_title_count=26
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000720
    - 현대건설
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 000720 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=000720`
- `name=현대건설`
- `naver_article_count=1`
- `google_rss_article_count=28`
- `kis_title_count=26`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 000720.

## RSS Item Metadata
- Title: `"현대건설도 한다"…고금리·재무부담에 코스피로 번지는 CB 발행 - 인베스트조선`
- Source: `인베스트조선`
- Published at: `2026-06-15T07:01:14+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxPZ2JIY2ZvaGtaZjlYcmpxeEtXbUpjMksydXlWS3A4TlB4SWoxTmVQNlQ4VGI3SXJQbWpnRjlwQkNlRWttcEdXTUNsODFKNVEtZkI1NEgzNmNBSzdDaV9oRjJwVlZCVm8ySTFjT2RQLXAwU2Fjam0zblZTdWVEME1KWjNhSQ?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:21:26+09:00`
- Company: [[KRX_000720_현대건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-16.json`
- Latest observation title: `"현대건설도 한다"…고금리·재무부담에 코스피로 번지는 CB 발행 - 인베스트조선`
- Latest observation source: `인베스트조선`
- Latest observation published_at: `2026-06-15T07:01:14+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxPZ2JIY2ZvaGtaZjlYcmpxeEtXbUpjMksydXlWS3A4TlB4SWoxTmVQNlQ4VGI3SXJQbWpnRjlwQkNlRWttcEdXTUNsODFKNVEtZkI1NEgzNmNBSzdDaV9oRjJwVlZCVm8ySTFjT2RQLXAwU2Fjam0zblZTdWVEME1KWjNhSQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
