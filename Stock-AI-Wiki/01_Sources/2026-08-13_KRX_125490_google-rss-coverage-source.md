---
id: source-2026-08-13-KRX-125490-google-rss-coverage
type: source
title: KRX 125490 Google RSS Coverage Source
created: 2026-08-13
updated: 2026-08-13
status: raw
stage: 0

market: KRX
ticker: "125490"
company: 한라캐스트
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-13

analysis:
  summary: Local coverage report row shows news coverage for KRX 125490.
  key_facts:
    - code=125490
    - name=한라캐스트
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 125490
    - 한라캐스트
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 125490 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=125490`
- `name=한라캐스트`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 125490.

## RSS Item Metadata
- Title: `한라캐스트 투자분석 2026. 08. 11 - 주달`
- Source: `주달`
- Published at: `2026-08-11T18:23:30+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9FaFVmbVhEcE9MZElhMk5Vczh2MGtCajF6RWJBU3phcmphT05aRUVWZFotUW85MEcxMEpsdEhZZUY5UzZ6eTl6TGZrd0lOT3hIQk9fT0FqbjY2NzlpREM2SWJBZndFVmEtamhXV1dEZVRwTm8?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-13T10:05:56+09:00`
- Company: [[KRX_125490_한라캐스트]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `한라캐스트, AI 모빌리티·휴머노이드 핵심 밸류체인 진입…퀀텀점프 기대 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-05-26T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE94MEtsaDRLQkdKSG8weUR4SFpXbjl1MGZjdDR2Rl91alpZWXYtWjE2MmdQRm9CeU8xRS0tQXdXMFpUeFpJeDZfaHRaeC11c2pqNGFkV0NGZGE1M1NQM0JoYUNLRGxMYWoy0gFuQVVfeXFMT0w4aGFfSVRTQzYxWmpHQ2pLUGRxSFpCdGVXWXkyUXVKQnJ4ZWhlYUNiZ2lHVU15Und4TGU3YWZ5aGM0RHBnYjNQcXhFN0tMdkNERkZwTklDaGtnN0FycmY2VDBQWjBpZ1BENVpEeVE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
