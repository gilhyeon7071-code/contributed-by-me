---
id: verification-2026-05-28-KRX-003280-google-rss-coverage
type: verification
title: KRX 003280 Google RSS Coverage Verification
created: 2026-05-28
updated: 2026-05-28
status: verification
stage: 1

market: KRX
ticker: "003280"
company: 흥아해운
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
    - code=003280
    - name=흥아해운
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003280
    - 흥아해운
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

# KRX 003280 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-28_KRX_003280_google-rss-coverage-source]]

## Facts Checked
- `code=003280`
- `name=흥아해운`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[속보] 흥아해운 VI 발동, 주가 오후들어 급등흐름…10%↑ - CBC뉴스`
- Source: `CBC뉴스`
- Published at: `2026-05-28T13:10:11+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5jVEc3NEd4NDhSVmIxd1dGdy1LWFl5M2lmUDB6bHRKekRicTdiZl9UOGhqTWo2bjZ1Y3N2LWk1aERUTThmZG5ZSW1tcGxFY19QUFJGNVlQLUJya1d4OHdmUUJQazUyTlNP?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:16:01+09:00`
- Company: [[KRX_003280_흥아해운]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `[속보] 흥아해운 VI 발동, 주가 오후들어 급등흐름…10%↑ - CBC뉴스`
- Latest observation source: `CBC뉴스`
- Latest observation published_at: `2026-05-28T13:10:11+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5jVEc3NEd4NDhSVmIxd1dGdy1LWFl5M2lmUDB6bHRKekRicTdiZl9UOGhqTWo2bjZ1Y3N2LWk1aERUTThmZG5ZSW1tcGxFY19QUFJGNVlQLUJya1d4OHdmUUJQazUyTlNP?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
