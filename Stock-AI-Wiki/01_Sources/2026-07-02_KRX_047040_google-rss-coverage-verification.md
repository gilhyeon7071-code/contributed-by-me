---
id: verification-2026-07-02-KRX-047040-google-rss-coverage
type: verification
title: KRX 047040 Google RSS Coverage Verification
created: 2026-07-02
updated: 2026-07-02
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
  collected_at: 2026-07-02

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=047040
    - name=대우건설
    - naver_article_count=2
    - google_rss_article_count=49
    - kis_title_count=11
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
- [[2026-07-02_KRX_047040_google-rss-coverage-source]]

## Facts Checked
- `code=047040`
- `name=대우건설`
- `naver_article_count=2`
- `google_rss_article_count=49`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `대우건설, 하반기 신입사원에 ‘안전모’ 수여… “안전 최우선 강조” - 브릿지경제`
- Source: `브릿지경제`
- Published at: `2026-07-02T15:18:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE01a01CZTRGS012VnNQZWxKUTVUQ3pGUEpCQTdhdW1ud29ZRy1yZlY4aThXb0t4cFVGSW1CaDJWLWJyZUhucTBaekwzZVFtR3FCSWEyUW5mTXFNdw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-02T16:10:30+09:00`
- Company: [[KRX_047040_대우건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-02.json`
- Latest observation title: `대우건설, 하반기 신입사원에 ‘안전모’ 수여… “안전 최우선 강조” - 브릿지경제`
- Latest observation source: `브릿지경제`
- Latest observation published_at: `2026-07-02T15:18:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE01a01CZTRGS012VnNQZWxKUTVUQ3pGUEpCQTdhdW1ud29ZRy1yZlY4aThXb0t4cFVGSW1CaDJWLWJyZUhucTBaekwzZVFtR3FCSWEyUW5mTXFNdw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
