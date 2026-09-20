---
id: verification-2026-07-15-KRX-147760-google-rss-coverage
type: verification
title: KRX 147760 Google RSS Coverage Verification
created: 2026-07-15
updated: 2026-07-15
status: verification
stage: 1

market: KRX
ticker: "147760"
company: 피엠티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=147760
    - name=피엠티
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 147760
    - 피엠티
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

# KRX 147760 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-15_KRX_147760_google-rss-coverage-source]]

## Facts Checked
- `code=147760`
- `name=피엠티`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `피엠티 이사 여황진, 소유 주식 수량 8290주 증가…소유 지분율 0.01%p 상승 - 디지털투데이`
- Source: `디지털투데이`
- Published at: `2026-07-14T16:11:01+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9GaFVTUWJsY1ZoTndRNFAyb0lTOFloRC1XVW5TRmdUNTNXUzZkU0FOeEVhUmZyQ0txOXlhcjRTR1FiRkZPSDhEaUFrd056clFXRGFSRTRqQTNQUDdPY2phOGI1VFE5VmEzZjh1M1VHVld4OWc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-15T21:05:34+09:00`
- Company: [[KRX_147760_피엠티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `[장중수급포착] 피엠티, 외국인/기관 동시 순매수… 주가 +16.12% - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-07-10T13:31:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE50NkNkdEFoZlBLeTlkSmdKS2ZaYUJyWFZESi1pbk5SblhmMHRzMWRDOW5UX1kxUVRHd0FLOFZDN2xod0xmOC15a3I3Tkx3bm9PYmpyZTZOOGRkR2k3?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
