---
id: verification-2026-06-01-KRX-105840-google-rss-coverage
type: verification
title: KRX 105840 Google RSS Coverage Verification
created: 2026-06-01
updated: 2026-06-01
status: verification
stage: 1

market: KRX
ticker: "105840"
company: 우진
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-01

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=105840
    - name=우진
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 105840
    - 우진
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

# KRX 105840 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-01_KRX_105840_google-rss-coverage-source]]

## Facts Checked
- `code=105840`
- `name=우진`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `'강동원 아역' 이효제·'박보검 아역' 문우진…존재감 확장한 2000년대생 남배우들 [TEN스타필드] - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-31T18:31:15+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5uak13MUMtV1I4N0Zib2xvanNLWnpHSVFXa25rdmRDLUdFNHh3czk1MndDbVpCOHVwVmZkMXJKSktEdjFmOVlmSVVZUnFrdm8?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:17:10+09:00`
- Company: [[KRX_105840_우진]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-01.json`
- Latest observation title: `우진문화재단, '2026 젊은춤판' 개최…청년 안무가 3인 무대 - 전라일보`
- Latest observation source: `전라일보`
- Latest observation published_at: `2026-06-01T09:05:47+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTFA4TWxjSHpybzQ3VUhQNUd1a21ZOGVHdVNOYks1ZFdWYWJFNjlpSmhLcy1GYWdTN0ZVaERrWW1ERUlFQVRrSkxmQko2dDJULUlzRW94TE9XcDBfLVdWNDUycTZrSU5ZQXcxY3lqatIBcEFVX3lxTFBlbTdwNHNGTHZSUVdURWtUbHVsTElFYkE3dGZ5Uzd1dDk5VjRYMV9LOGs3bjVwWDExMlVLV2pPQlpyUWpPN1pMX1R4VUc0YkY2MHVWS2ZiRHdvRnR4Tlh2TFBINmZ5Z3F1QlJRUFc3NVo?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
