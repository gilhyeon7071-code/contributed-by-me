---
id: source-2026-05-21-KRX-147760-google-rss-coverage
type: source
title: KRX 147760 Google RSS Coverage Source
created: 2026-05-21
updated: 2026-05-21
status: raw
stage: 0

market: KRX
ticker: "147760"
company: 피엠티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 147760.
  key_facts:
    - code=147760
    - name=피엠티
    - naver_article_count=1
    - google_rss_article_count=10
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 147760
    - 피엠티
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

# KRX 147760 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=147760`
- `name=피엠티`
- `naver_article_count=1`
- `google_rss_article_count=10`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 147760.

## RSS Item Metadata
- Title: `피엠티, 삼성 中 반도체법인에 65억원 규모 프로브카드 공급 - 마켓인`
- Source: `마켓인`
- Published at: `2026-05-20T14:59:51+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE8ybkNoQ0kzNnFTWTNXZlVnMzJJR21kVlFHRXVOMzByV0FtdHU2QlFRZ0MydTJmTzlERVF4Rl9sQUQzVzFCc1AyOFd4THd6bzZ2MklsY2RRSy1zM0xxRDBfa244UjlzcEM4MUtQa2I1LVU2WWM?oc=5`

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
- Updated at: `2026-08-21T19:14:51+09:00`
- Company: [[KRX_147760_피엠티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `반도체 재료·부품株 유니트론텍 네패스 피엠티 '불기둥'...무슨 호재 있나 - 핀포인트뉴스`
- Latest observation source: `핀포인트뉴스`
- Latest observation published_at: `2026-05-21T15:48:30+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9CRWE3enN3dWRrbDYyZ3I4NUxINjBxXzR6c2F4NWhQcnpVUDUyaWFjc0w3aXdWdmxVcFlSM3hhMU9mZ2V3cmV6MXp1UVJORG5ReDlYOTNOQlNwc3VENVY4Q2RDNFlHX3p6Nk9fenZOLVA3bDDSAXdBVV95cUxOdE1hQW9LR3ctMDJHNUpMTmVER0VLZXVhX254eXBrYUdBbUJzUkVkY2lWZ3luRGV3cTVWQkJMTGw4RjRINm5wR2xNTnExbGhuQXQxX2R3ZURlNVhnMEhMUnd4SERBbmRkdVA0NW81OFpsbUxWUWppRQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
