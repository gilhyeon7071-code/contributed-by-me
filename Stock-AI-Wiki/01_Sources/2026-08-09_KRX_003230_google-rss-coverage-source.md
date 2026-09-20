---
id: source-2026-08-09-KRX-003230-google-rss-coverage
type: source
title: KRX 003230 Google RSS Coverage Source
created: 2026-08-09
updated: 2026-08-09
status: raw
stage: 0

market: KRX
ticker: "003230"
company: 삼양식품
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-09

analysis:
  summary: Local coverage report row shows news coverage for KRX 003230.
  key_facts:
    - code=003230
    - name=삼양식품
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 003230
    - 삼양식품
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

# KRX 003230 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=003230`
- `name=삼양식품`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 003230.

## RSS Item Metadata
- Title: `“구독자 99%가 외국인”…불닭 페포, ‘수출 캐릭터’될 수 있을까 - 에너지경제신문`
- Source: `에너지경제신문`
- Published at: `2026-08-08T16:30:27+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE16eWZfSzRSeEI5RGF1X2Nla1JnNEZYZ0pFNjZOczRWSUlhQmREQkdmUFBNT242TjlsX3dEalJDZzB5RVpheldYY1BfRUJiX05BZUppSVBZLU4yWmRnaDFubF93NA?oc=5`

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
- Updated at: `2026-08-21T19:34:30+09:00`
- Company: [[KRX_003230_삼양식품]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-09.json`
- Latest observation title: `원가 뛰어도 가격 그대로…삼양·오리온의 ‘가격 방어력’ - 서울경제TV`
- Latest observation source: `서울경제TV`
- Latest observation published_at: `2026-08-09T08:00:06+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE9GQ0RWX0NhTDNXRmwtTFpPM1Q3dlk2NXBBZmxENFpTOU5RbVNIUTdfSzJQM1hIazZPTVZ4b1dJTXNfeDZhR015enNyUjZiT3dodW9XRmcxaWh2N3hjLXo0WHM3V2I?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
