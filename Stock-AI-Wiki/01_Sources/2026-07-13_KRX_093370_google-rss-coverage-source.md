---
id: source-2026-07-13-KRX-093370-google-rss-coverage
type: source
title: KRX 093370 Google RSS Coverage Source
created: 2026-07-13
updated: 2026-07-13
status: raw
stage: 0

market: KRX
ticker: "093370"
company: 후성
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-13

analysis:
  summary: Local coverage report row shows news coverage for KRX 093370.
  key_facts:
    - code=093370
    - name=후성
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 093370
    - 후성
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

# KRX 093370 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=093370`
- `name=후성`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 093370.

## RSS Item Metadata
- Title: `[속보] 후성 VI 발동, 주가 털썩…전 2거래일 급등 뒤 급락 전환 - CBC뉴스`
- Source: `CBC뉴스`
- Published at: `2026-06-16T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5EQ0JPWURnMVE1MkpPaU4xSVJWbkEzQVBFb3Z1N0FYRFRNTmtDemx2SjgwVmphdUplam9lLVhiYk9SNFVBWmZCbGFVM051cGNBRHhyN3hIQVkyTXdoTDJpZzlwTzFCWFhN?oc=5`

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
- Updated at: `2026-08-21T19:29:02+09:00`
- Company: [[KRX_093370_후성]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-13.json`
- Latest observation title: `[속보] 후성 VI 발동, 주가 털썩…전 2거래일 급등 뒤 급락 전환 - CBC뉴스`
- Latest observation source: `CBC뉴스`
- Latest observation published_at: `2026-06-16T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5EQ0JPWURnMVE1MkpPaU4xSVJWbkEzQVBFb3Z1N0FYRFRNTmtDemx2SjgwVmphdUplam9lLVhiYk9SNFVBWmZCbGFVM051cGNBRHhyN3hIQVkyTXdoTDJpZzlwTzFCWFhN?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
