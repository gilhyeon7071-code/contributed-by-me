---
id: verification-2026-06-01-KRX-003550-google-rss-coverage
type: verification
title: KRX 003550 Google RSS Coverage Verification
created: 2026-06-01
updated: 2026-06-01
status: verification
stage: 1

market: KRX
ticker: "003550"
company: LG
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
    - code=003550
    - name=LG
    - naver_article_count=48
    - google_rss_article_count=267
    - kis_title_count=50
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003550
    - LG
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

# KRX 003550 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-01_KRX_003550_google-rss-coverage-source]]

## Facts Checked
- `code=003550`
- `name=LG`
- `naver_article_count=48`
- `google_rss_article_count=267`
- `kis_title_count=50`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[LG가 달라졌어요①] AI 밸류체인 확장 맞춰 그룹 주요계열사 주가 급등 - 연합인포맥스`
- Source: `연합인포맥스`
- Published at: `2026-05-31T09:03:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTE9USE9nYU1LeHFaVTcyZncwRTFocGo5RWF5SEtCQ013cGRuSTdRUlhnRkdmTVM1S3NISU1LOFJ2QkNleG9STUNFSUR0TThNZ2FLdDZtQjYyRDZzSHJnaWxMaGlJQWZjRTdPYmc5VGktZUQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-01T08:42:44+09:00`
- Company: [[KRX_003550_LG]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-01.json`
- Latest observation title: `[LG가 달라졌어요①] AI 밸류체인 확장 맞춰 그룹 주요계열사 주가 급등 - 연합인포맥스`
- Latest observation source: `연합인포맥스`
- Latest observation published_at: `2026-05-31T09:03:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE9USE9nYU1LeHFaVTcyZncwRTFocGo5RWF5SEtCQ013cGRuSTdRUlhnRkdmTVM1S3NISU1LOFJ2QkNleG9STUNFSUR0TThNZ2FLdDZtQjYyRDZzSHJnaWxMaGlJQWZjRTdPYmc5VGktZUQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
