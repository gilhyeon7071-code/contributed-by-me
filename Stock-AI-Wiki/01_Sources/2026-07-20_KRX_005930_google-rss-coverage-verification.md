---
id: verification-2026-07-20-KRX-005930-google-rss-coverage
type: verification
title: KRX 005930 Google RSS Coverage Verification
created: 2026-07-20
updated: 2026-07-20
status: verification
stage: 1

market: KRX
ticker: "005930"
company: 삼성전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005930
    - name=삼성전자
    - naver_article_count=18
    - google_rss_article_count=0
    - kis_title_count=105
    - google_rss_covered=False
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005930
    - 삼성전자
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

# KRX 005930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-20_KRX_005930_google-rss-coverage-source]]

## Facts Checked
- `code=005930`
- `name=삼성전자`
- `naver_article_count=18`
- `google_rss_article_count=0`
- `kis_title_count=105`
- `google_rss_covered=False`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `4조 ‘잭팟’ 터진 삼성전자, 온누리상품권 8000억원 푼다 - 매일경제`
- Source: `매일경제`
- Published at: `2026-07-18T10:17:27+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFB6dkJLY1hZaVdfUGI1eEM5NUhxeUxNTDFVbzZFa096V2QyOUJPeS11d3RYUTBLSnduWmZiNFRyZ21UVHVaQnNaLWlkWGlsTWs4Tm1BOGlR?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:29:58+09:00`
- Company: [[KRX_005930_삼성전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-20.json`
- Latest observation title: `삼성전자, 프리미엄 ‘폴드8 울트라'·AI 안경 출격 준비 - AI타임스`
- Latest observation source: `AI타임스`
- Latest observation published_at: `2026-07-20T05:13:48+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE5Hd0hrSjcwS21mbnFOYjg2Z0RVYy03aE9iYkRCZkdUZ0c3RkptejZuZlB3Z2pUR1JzNXFTZDFHRURTZnhtUDJIOUNUc1FueDUwRHFyYmpKWFg5QmtuTWRuenJGUVpYYzVqcVE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
