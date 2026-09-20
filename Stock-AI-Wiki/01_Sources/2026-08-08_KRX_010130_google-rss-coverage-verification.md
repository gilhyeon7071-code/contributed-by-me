---
id: verification-2026-08-08-KRX-010130-google-rss-coverage
type: verification
title: KRX 010130 Google RSS Coverage Verification
created: 2026-08-08
updated: 2026-08-08
status: verification
stage: 1

market: KRX
ticker: "010130"
company: 고려아연
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=010130
    - name=고려아연
    - naver_article_count=2
    - google_rss_article_count=68
    - kis_title_count=40
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 010130
    - 고려아연
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

# KRX 010130 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-08_KRX_010130_google-rss-coverage-source]]

## Facts Checked
- `code=010130`
- `name=고려아연`
- `naver_article_count=2`
- `google_rss_article_count=68`
- `kis_title_count=40`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `고려아연·영풍, 임시주총 1달 앞두고 다시 짙어지는 전운 - 비즈워치`
- Source: `비즈워치`
- Published at: `2026-08-08T08:55:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE12WklFYWVmTmNhWVY1S2ZGNlZxb2xfbmxBNnRvWWdlLW55dzVWeS11ZXJiMzU2VmpIM0dSMjNBNllGcWtENUxkRXAzWndDcHBZSkY5dER6SHhiNUp1ZjNfWE1JR2RVU0pvcGh3?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-08T12:07:14+09:00`
- Company: [[KRX_010130_고려아연]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-08.json`
- Latest observation title: `고려아연 ‘크루서블 명칭 무단 사용’ 고발…美 행사 나선 MBK 윤종하·영풍 장세환 ‘자격’ 논란 - 경기일보`
- Latest observation source: `경기일보`
- Latest observation published_at: `2026-08-07T11:24:06+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE1HcWFLbW1xMkprUnMxdkx4WFhrLWFaVjlXTTZYMjVncTRrQkNxclpCSWh1WGxQclV2UEViT0JfNjNpb2FpYXRWbU0td2xKY0lyV05SU2tIN01LcWM?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
