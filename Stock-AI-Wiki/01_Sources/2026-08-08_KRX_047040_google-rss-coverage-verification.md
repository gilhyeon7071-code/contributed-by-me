---
id: verification-2026-08-08-KRX-047040-google-rss-coverage
type: verification
title: KRX 047040 Google RSS Coverage Verification
created: 2026-08-08
updated: 2026-08-08
status: verification
stage: 1

market: KRX
ticker: "047040"
company: 대우건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=047040
    - name=대우건설
    - naver_article_count=1
    - google_rss_article_count=78
    - kis_title_count=18
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 047040
    - 대우건설
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

# KRX 047040 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-08_KRX_047040_google-rss-coverage-source]]

## Facts Checked
- `code=047040`
- `name=대우건설`
- `naver_article_count=1`
- `google_rss_article_count=78`
- `kis_title_count=18`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[단독] 김보현 대우건설 사장 사의…후임 대표이사 선임 절차 착수 - 대한경제`
- Source: `대한경제`
- Published at: `2026-08-06T14:03:16+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1wek5FckJ1N2t2YV9hR0xyaEJ0dldBTDhvM0pJOEVEN2F2c2ZNN3hqcXJybGhUMmJhRTFvMmxfaHY3a2w4cnpQV2pMU0J1Vk0xYi00ZlBSTWhmeWZxVmRfMkdlcVZGZGdibHBzTmk0WmtsX1U?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-08T12:07:14+09:00`
- Company: [[KRX_047040_대우건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-08.json`
- Latest observation title: `대우건설, 이강석 부사장 대표이사 추천 예정 - 동아일보`
- Latest observation source: `동아일보`
- Latest observation published_at: `2026-08-06T17:11:43+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE15aHM4WjV4ek9SQlN0Ujg1Qmw1QUlzcnlBYkwxM0g1TUZ5VXU4Szk1VWt3WUdYMmtjM3NpemhzTVIzT0Zlcjk2UmFCRHM1NmJRWW9GdmRsaXhlTk9zMWtqLTB1WUF6VS1lc2dV0gFmQVVfeXFMUGZoaXNfMWZ4R0lPRUNDUURRWVJjMW9rWlA2SU40QWFNbnYzNzFmMzVxSmE1OGlFTTBaUkdoNTlIbnVZNGZjN2h4RGRqNmFILUg3S2RvR3VOczR6Zks1RHd6Z0tVUTB3?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
