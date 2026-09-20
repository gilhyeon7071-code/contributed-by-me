---
id: verification-2026-07-14-KRX-093370-google-rss-coverage
type: verification
title: KRX 093370 Google RSS Coverage Verification
created: 2026-07-14
updated: 2026-07-14
status: verification
stage: 1

market: KRX
ticker: "093370"
company: 후성
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-14

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=093370
    - name=후성
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 093370
    - 후성
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

# KRX 093370 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-14_KRX_093370_google-rss-coverage-source]]

## Facts Checked
- `code=093370`
- `name=후성`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[속보] 후성 VI 발동, 주가 털썩…전 2거래일 급등 뒤 급락 전환 - CBC뉴스`
- Source: `CBC뉴스`
- Published at: `2026-06-16T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5EQ0JPWURnMVE1MkpPaU4xSVJWbkEzQVBFb3Z1N0FYRFRNTmtDemx2SjgwVmphdUplam9lLVhiYk9SNFVBWmZCbGFVM051cGNBRHhyN3hIQVkyTXdoTDJpZzlwTzFCWFhN?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:29:21+09:00`
- Company: [[KRX_093370_후성]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-14.json`
- Latest observation title: `경상국립대 연구진, 한·미 반려견 후성유전학적 노화 차이 규명 - 경남뉴스통신`
- Latest observation source: `경남뉴스통신`
- Latest observation published_at: `2026-07-14T18:19:36+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE1MV25KS1dSNDN5a2xQOVFWMFFmaG5odExlRndsbHlJbmZ2Q3BEcGU1Z2xvS1JrX1dDTERkdzVtSEFPNU8tdnUxd0h2LTFXN1BpeGlMX1UyV0k1TFVkenp4RHQzNWZ1UdIBaEFVX3lxTFBGWGJjNlpRRFRqanFSempuZGtibHcwQ1pPYjQwQW1lWEtkVzkzOE9Jb252NUgwVF9ya3l1VVAxVklLMVdMSVJrY2xIX2ttWjJCcDNZTmJwN212YkRwRHJ1cV8zbmh3ejVt?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_eco-packaging_친환경-패키징]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
