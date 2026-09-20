---
id: verification-2026-06-20-KRX-042700-google-rss-coverage
type: verification
title: KRX 042700 Google RSS Coverage Verification
created: 2026-06-20
updated: 2026-06-20
status: verification
stage: 1

market: KRX
ticker: "042700"
company: 한미반도체
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=042700
    - name=한미반도체
    - naver_article_count=4
    - google_rss_article_count=7
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 042700
    - 한미반도체
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

# KRX 042700 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-20_KRX_042700_google-rss-coverage-source]]

## Facts Checked
- `code=042700`
- `name=한미반도체`
- `naver_article_count=4`
- `google_rss_article_count=7`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `K반도체 장비, 머스크 테라팹 생태계 입성 - 매일경제`
- Source: `매일경제`
- Published at: `2026-06-18T17:58:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFB0M3NiMVh0S1dkMmRidG9wTHpCbk16eVh5RHhteGhkVjNLWS1ET2JVZGZVVXEwd2YyTW5idkxSUEM0NjByLURCRmFkb3FFb3hTOE53N3J3?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:22:42+09:00`
- Company: [[KRX_042700_한미반도체]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-20.json`
- Latest observation title: `1분기 실적 반토막 난 한미반도체…곽동신 회장, 어닝 쇼크에도 사재 80억 털었다 - 뉴스퀘스트`
- Latest observation source: `뉴스퀘스트`
- Latest observation published_at: `2026-06-18T17:02:46+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMib0FVX3lxTFBVVjhvNDd3bmdweExENG5uU3lJbDRLSFhVWlJsMjh2NjZDcl9QRjMyd0F3OG5aOUJ5dmJSSDhobC1kb2o2MmJyRy1GeDhaTW1xMHI1ZXpFSEhrNk16bXlXbnkyNFVRS1NtQ29TS2tDMNIBc0FVX3lxTE50X3NFNFgxaFVXUWFLSG9BWXp0QkV5NGxEVW5qVWtqNVBjSlUxaDFtbmhaOHpoX1RQUHV4aTZJWUZYbDA1NHFUemI2cWxPRl9jT3IwQnllbFdoaEoxampqNGxiM0JSdUM3ZFMzMVVmOUt3Q0E?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
