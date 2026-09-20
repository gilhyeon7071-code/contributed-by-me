---
id: verification-2026-06-03-KRX-454910-google-rss-coverage
type: verification
title: KRX 454910 Google RSS Coverage Verification
created: 2026-06-03
updated: 2026-06-03
status: verification
stage: 1

market: KRX
ticker: "454910"
company: 두산로보틱스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-03

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=454910
    - name=두산로보틱스
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 454910
    - 두산로보틱스
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

# KRX 454910 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-03_KRX_454910_google-rss-coverage-source]]

## Facts Checked
- `code=454910`
- `name=두산로보틱스`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `적자에도 두산로보틱스 시가총액 10조 돌파…‘젠슨 황 효과’ - 서울경제`
- Source: `서울경제`
- Published at: `2026-06-02T14:58:22+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE5WeUhSeG5UdmszREM3bU9TUlhwdm1xeUtuU3pKYy1ha2VNNzJyQ1NxS3VqV2RDaGRFclE0SXp0empLTUtvX3M4ejVXZWwzT1ZWZ3fSAVNBVV95cUxPY3dJVjc4bno5WFlzREJoOUtpQjhaZFF2dG50SHBCUVl0b0FSbHUyYmFDQzU0U01zWEVSc2ZUZFhOdU5mRGFFQWZIOHZySjlfXzZUcw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:17:43+09:00`
- Company: [[KRX_454910_두산로보틱스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-03.json`
- Latest observation title: `적자에도 두산로보틱스 시가총액 10조 돌파…‘젠슨 황 효과’ - 서울경제`
- Latest observation source: `서울경제`
- Latest observation published_at: `2026-06-02T14:58:22+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE5WeUhSeG5UdmszREM3bU9TUlhwdm1xeUtuU3pKYy1ha2VNNzJyQ1NxS3VqV2RDaGRFclE0SXp0empLTUtvX3M4ejVXZWwzT1ZWZ3fSAVNBVV95cUxPY3dJVjc4bno5WFlzREJoOUtpQjhaZFF2dG50SHBCUVl0b0FSbHUyYmFDQzU0U01zWEVSc2ZUZFhOdU5mRGFFQWZIOHZySjlfXzZUcw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
