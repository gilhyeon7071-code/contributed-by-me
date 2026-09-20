---
id: verification-2026-05-27-KRX-321260-google-rss-coverage
type: verification
title: KRX 321260 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "321260"
company: 프로이천
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=321260
    - name=프로이천
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 321260
    - 프로이천
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

# KRX 321260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_321260_google-rss-coverage-source]]

## Facts Checked
- `code=321260`
- `name=프로이천`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `프로이천, +13.95% 52주 신고가 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-26T10:54:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxNd2VJMVotWnA4Sm53TlJtTWZTQkxQNGdhelZnX0I3RFNpZUhmUDdmYTRzYVhaMGFGM214TVh4emJ2eVpBeHpHM0pTTkNqakpSMXFWZThSay1tLXFMVHREREFWazhhNjM0RTBjLVZUNVZzQXRnQ3dFN1BsQUd2YkdiUmFGUVY4UkdWYWFtQTJ3Uzk2N0ktTzNJ0gGXAUFVX3lxTE13ZUkxWi1acDhKbndOUm1NZlNCTFA0Z2F6VmdfQjdEU2llSGZQN2ZhNHNhWFowYUYzbXhNWHh6YnZ5WkF4ekczSlNOQ2pqSlIxcVZlOFJrLW0tcUxUdEREQVZrOGE2MzRFMGMtVlQ1VnNBdGdDd0U3UGxBR3ZiR2JSYUZRVjhSR1ZhYW1BMndTOTY3SS1PM0k?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-27T16:05:05+09:00`
- Company: [[KRX_321260_프로이천]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `프로이천, +1.65% 52주 신고가 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-27T10:07:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxNcFB4b0dRVnNaM0RBeTdYWjNuN2I3RFVGZnpFeEJoYTNSdGhpdjdxQjB5Zl92bnl1Qm5WQ1lPQVBXVHNObWxmUHB2dk5fc25WUy1LTThtb2ZSUGtkdHZYR1ZGd1ByWTducm80OTAyZUNtMzlTdFk4VkhXRG1KT2Q0UFpXWS1QV2lRd2FNSzR6dTVrVjBOWTBJ0gGXAUFVX3lxTE1wUHhvR1FWc1ozREF5N1haM243YjdEVUZmekV4QmhhM1J0aGl2N3FCMHlmX3ZueXVCblZDWU9BUFdUc05tbGZQcHZ2Tl9zblZTLUtNOG1vZlJQa2R0dlhHVkZ3UHJZN25ybzQ5MDJlQ20zOVN0WThWSFdEbUpPZDRQWldZLVBXaVF3YU1LNHp1NWtWME5ZMEk?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
