---
id: verification-2026-05-28-KRX-024060-google-rss-coverage
type: verification
title: KRX 024060 Google RSS Coverage Verification
created: 2026-05-28
updated: 2026-05-28
status: verification
stage: 1

market: KRX
ticker: "024060"
company: 흥구석유
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=024060
    - name=흥구석유
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 024060
    - 흥구석유
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

# KRX 024060 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-28_KRX_024060_google-rss-coverage-source]]

## Facts Checked
- `code=024060`
- `name=흥구석유`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `흥구석유 투자분석 2026. 05. 10 - 주달`
- Source: `주달`
- Published at: `2026-05-10T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9FOUlQN2ZublN2WE8yZnpiUHFPZUVzanhRU0xlQjVHVFZSUlF5Y3JNMG5UT2dsTC1oWFNYMDZSMHZyeEpEN2JzNUZscnhncjVYd2h4aTNXaUp0S09NSV9xLXJ4S0pYOUU1d2FkbUdwSTJkQXM?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-28T11:05:06+09:00`
- Company: [[KRX_024060_흥구석유]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `흥구석유 투자분석 2026. 05. 10 - 주달`
- Latest observation source: `주달`
- Latest observation published_at: `2026-05-10T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9FOUlQN2ZublN2WE8yZnpiUHFPZUVzanhRU0xlQjVHVFZSUlF5Y3JNMG5UT2dsTC1oWFNYMDZSMHZyeEpEN2JzNUZscnhncjVYd2h4aTNXaUp0S09NSV9xLXJ4S0pYOUU1d2FkbUdwSTJkQXM?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
