---
id: verification-2026-08-18-KRX-066570-google-rss-coverage
type: verification
title: KRX 066570 Google RSS Coverage Verification
created: 2026-08-18
updated: 2026-08-18
status: verification
stage: 1

market: KRX
ticker: "066570"
company: LG전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=066570
    - name=LG전자
    - naver_article_count=2
    - google_rss_article_count=29
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 066570
    - LG전자
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

# KRX 066570 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-18_KRX_066570_google-rss-coverage-source]]

## Facts Checked
- `code=066570`
- `name=LG전자`
- `naver_article_count=2`
- `google_rss_article_count=29`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[증시키워드] 삼성전자·SK하이닉스 반등…LG전자 ‘로봇’, 두산에너빌리티 ‘원전’ 주목 - 이투데이`
- Source: `이투데이`
- Published at: `2026-08-18T07:46:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVEFVX3lxTE5YdnR3ZklYalBjTmtaaExXcUV6ME5ISmttRVo0WUJxTVBvMVUwV2pLLUVodHFOYUx6Y1h1LWtCSVNJTE45ZkZoczhYQmo2SkVWdGlrRw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:51+09:00`
- Company: [[KRX_066570_LG전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-18.json`
- Latest observation title: `[포토] LG전자, 엔비디아와 로보틱스 협업 가속화 - 전자신문`
- Latest observation source: `전자신문`
- Latest observation published_at: `2026-08-18T17:16:10+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiTkFVX3lxTE5Pejc5WHgxUko5RmJ3c20waWNIREtHdUs2NzBHVXpiMHpNejFuM0hkQTFHT2JtT09UdmdLRFVORzl6N0tZLW5ybkx6ZHo1UQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
