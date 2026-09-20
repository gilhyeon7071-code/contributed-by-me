---
id: verification-2026-05-21-KRX-009420-google-rss-coverage
type: verification
title: KRX 009420 Google RSS Coverage Verification
created: 2026-05-21
updated: 2026-05-21
status: verification
stage: 1

market: KRX
ticker: "009420"
company: 한올바이오파마
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=009420
    - name=한올바이오파마
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=0
    - google_rss_covered=True
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 009420
    - 한올바이오파마
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

# KRX 009420 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-21_KRX_009420_google-rss-coverage-source]]

## Facts Checked
- `code=009420`
- `name=한올바이오파마`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=0`
- `google_rss_covered=True`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `'제약 공룡' J&J 실패한 류머티즘 시장 여나…한올바이오파마, 상한가 - 한국경제`
- Source: `한국경제`
- Published at: `2026-05-21T14:28:49+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFBMZGo1SW5yNTBnWTRFNlk4cFlBS2dpcmstRzU1b05LNV82M3FPRGRsYlptR0lKcXcySHl0NGFvV1VTVlFrNXRhTHg2WWlUc3ZvWWxxMkh6cjZ2d9IBVEFVX3lxTFBXa192cE9PUTIzZmgtTTNBWnY3Y1ZRNEg4M1lBenRJU193S1VhakpEUGItZVQyVGcxTkpBRE0yanVSNTZDaG96V2s0QmJwYVVHV296Ng?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-21T15:05:23+09:00`
- Company: [[KRX_009420_한올바이오파마]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `'제약 공룡' J&J 실패한 류머티즘 시장 여나…한올바이오파마, 상한가 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-05-21T14:28:49+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFBMZGo1SW5yNTBnWTRFNlk4cFlBS2dpcmstRzU1b05LNV82M3FPRGRsYlptR0lKcXcySHl0NGFvV1VTVlFrNXRhTHg2WWlUc3ZvWWxxMkh6cjZ2d9IBVEFVX3lxTFBXa192cE9PUTIzZmgtTTNBWnY3Y1ZRNEg4M1lBenRJU193S1VhakpEUGItZVQyVGcxTkpBRE0yanVSNTZDaG96V2s0QmJwYVVHV296Ng?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
