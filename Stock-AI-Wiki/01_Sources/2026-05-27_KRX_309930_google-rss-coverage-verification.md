---
id: verification-2026-05-27-KRX-309930-google-rss-coverage
type: verification
title: KRX 309930 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "309930"
company: 조이웍스앤코
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=309930
    - name=조이웍스앤코
    - naver_article_count=0
    - google_rss_article_count=5
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 309930
    - 조이웍스앤코
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

# KRX 309930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_309930_google-rss-coverage-source]]

## Facts Checked
- `code=309930`
- `name=조이웍스앤코`
- `naver_article_count=0`
- `google_rss_article_count=5`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `조이웍스앤코, 5대 1 주식병합 완료…거래 재개 - 마켓인`
- Source: `마켓인`
- Published at: `2026-05-27T08:50:16+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE91bEI3YlEzQWtQZFBGajB5YktxVEtWcEdnQ0xtNlhNT3VTQVlzMEIwLUtnMzQzQVB5bHlxVmMzZDV4cGw1VF9lVFFpUXpwSFRoaFFGZDhNcE5ReDNGeWY2bWQzb3FoQzRiRHJscWRRSF9yYlU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:44+09:00`
- Company: [[KRX_309930_조이웍스앤코]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `조이웍스앤코, 주식병합 완료…무증·CB 소각으로 주주가치 제고 - 녹색경제신문`
- Latest observation source: `녹색경제신문`
- Latest observation published_at: `2026-05-27T10:53:51+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9SZ3ZhSVZCdldoS3FqUF8zTTBjaHVlbExvQllid25uenVfbE5KaEVzMERBR3M3WXJValYyM0VPM1J5eDh5dXBuSFBCU2pPZGNwZkZtR2kzWUM2SlJPRzNmVVBzcV92eVV1?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
