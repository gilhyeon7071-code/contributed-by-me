---
id: verification-2026-06-16-KRX-026960-google-rss-coverage
type: verification
title: KRX 026960 Google RSS Coverage Verification
created: 2026-06-16
updated: 2026-06-16
status: verification
stage: 1

market: KRX
ticker: "026960"
company: 동서
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-16

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=026960
    - name=동서
    - naver_article_count=1
    - google_rss_article_count=17
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 026960
    - 동서
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

# KRX 026960 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-16_KRX_026960_google-rss-coverage-source]]

## Facts Checked
- `code=026960`
- `name=동서`
- `naver_article_count=1`
- `google_rss_article_count=17`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `서천공주선 서부여IC∼동서천JCT 23∼25일 동서천방향 주간 통제 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-06-15T17:51:13+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE5iRndtWXh4Rm5aRU1DRjU3c1dhZUZwQXFCbm5mYklTVWhiNkRTTmJXUUpVY09DZ0ZQR1dpYnprUHZpMGg2WWZPb1ZVVElKSkNrQTlmejFNUUl5Tl9mSUxuNtIBYEFVX3lxTE5iRndtWXh4Rm5aRU1DRjU3c1dhZUZwQXFCbm5mYklTVWhiNkRTTmJXUUpVY09DZ0ZQR1dpYnprUHZpMGg2WWZPb1ZVVElKSkNrQTlmejFNUUl5Tl9mSUxuNg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-16T09:05:36+09:00`
- Company: [[KRX_026960_동서]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-16.json`
- Latest observation title: `서천공주선 서부여IC∼동서천JCT 23∼25일 동서천방향 주간 통제 - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-06-15T17:51:13+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE5iRndtWXh4Rm5aRU1DRjU3c1dhZUZwQXFCbm5mYklTVWhiNkRTTmJXUUpVY09DZ0ZQR1dpYnprUHZpMGg2WWZPb1ZVVElKSkNrQTlmejFNUUl5Tl9mSUxuNtIBYEFVX3lxTE5iRndtWXh4Rm5aRU1DRjU3c1dhZUZwQXFCbm5mYklTVWhiNkRTTmJXUUpVY09DZ0ZQR1dpYnprUHZpMGg2WWZPb1ZVVElKSkNrQTlmejFNUUl5Tl9mSUxuNg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
