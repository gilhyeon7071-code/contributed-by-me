---
id: verification-2026-06-26-KRX-000720-google-rss-coverage
type: verification
title: KRX 000720 Google RSS Coverage Verification
created: 2026-06-26
updated: 2026-06-26
status: verification
stage: 1

market: KRX
ticker: "000720"
company: 현대건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000720
    - name=현대건설
    - naver_article_count=2
    - google_rss_article_count=62
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000720
    - 현대건설
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

# KRX 000720 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-26_KRX_000720_google-rss-coverage-source]]

## Facts Checked
- `code=000720`
- `name=현대건설`
- `naver_article_count=2`
- `google_rss_article_count=62`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[단독]현대건설, GTX-A 철근 누락 벌점에 이의신청 안 한다…"보강에 총력" - MTN 머니투데이방송`
- Source: `MTN 머니투데이방송`
- Published at: `2026-06-24T14:07:45+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE1tX0xjYTZsdklVazB3V2w0SXBkZmN3b0hza0N1emEzYzlHNGNqNHpGMktfbDVKUV9CTTRfaFd2QWp2SjdGSE9RUWhaNkpGMTJqNzA1WmdvT0hsYkhXSVIxRUs4SW0?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:24:38+09:00`
- Company: [[KRX_000720_현대건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-26.json`
- Latest observation title: `현대건설 에쓰오일 샤힌프로젝트 현장서 근로자 사망… 굴착 중 토사 붕괴 - 동아일보`
- Latest observation source: `동아일보`
- Latest observation published_at: `2026-06-26T14:17:40+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMidkFVX3lxTE50SExfclJOMWF4ekVWR1BJRVoxeElFMGQwdDhXdk44LVB4NDVnYzRhb2dfSnJFWDdhVHFTZ2dZSHhLSm1HNWNTMUxMMHVxSEc1Vi15R0lmRFpjLUZzOGNiemt6T3kzUnZHamx6dldNN3RwdldTLVHSAWZBVV95cUxQMElTTEN5SW83VWNPRnV6X1NoVDMxMkVDY0dXTTdfaHNmU2tVeW1XVUhuMkQ2UEl5SzFfUGVya1hPdDk5RkNGY0hTMFFmS2lTV2twZ0dJd3BUZ294Vkd0UmVtSG53cXc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
