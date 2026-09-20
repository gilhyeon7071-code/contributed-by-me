---
id: verification-2026-07-01-KRX-051910-google-rss-coverage
type: verification
title: KRX 051910 Google RSS Coverage Verification
created: 2026-07-01
updated: 2026-07-01
status: verification
stage: 1

market: KRX
ticker: "051910"
company: LG화학
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-01

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=051910
    - name=LG화학
    - naver_article_count=1
    - google_rss_article_count=97
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 051910
    - LG화학
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

# KRX 051910 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-01_KRX_051910_google-rss-coverage-source]]

## Facts Checked
- `code=051910`
- `name=LG화학`
- `naver_article_count=1`
- `google_rss_article_count=97`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LG화학, 고형암 신약후보물질 美FDA 임상승인 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-06-30T08:55:29+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9yZWxaRHZqb3diOWEyelc1RTRkbmdTOWZFNEYyV3NLeHAwMk4wZDloUjBhT1RIWHdLLVBjSnlVSmFRM0g3T09OUDdnX0UyQjNQNjk4dThIVmRfdGxFY1prR9IBeEFVX3lxTE1UT3dCRV9fdUtBVTlkdEJES2RvZUhiNFpCdGlaMW5qNlh2NDBtY2QxRW5UblZoOGVFVXpBNzhuaUd2X213c1AzN3o4TTFWVHY5ZW0yRmVPMHQtdkdTSTZVM1NnRGprZVptdy1Hb2k1djJsdnJMaXhGZw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:25:55+09:00`
- Company: [[KRX_051910_LG화학]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-01.json`
- Latest observation title: `곳간 빈 석유화학·배터리…'신용등급 줄강등' 된서리 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-07-01T17:27:20+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE5vS2ZQMFN1N2ZnUng2RlJqVVBXOFMyN08tVHJfYjZKUFN0LUFCWjlPWnRGMWJ3NXdEaktOME5oYm9sQUx0cTRIOGZsVnB4NTlobkFRdm9UMlN2QdIBVEFVX3lxTE5wS0gzQ0pVQjM0anpDQ3NERlRmdFoyc253QXVlOEhCTGxTalROYzBieVEyTTdBSVNncExJemRrRVZtTUc1YjBzSkt5N0JjQ3NUQm9kZg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
