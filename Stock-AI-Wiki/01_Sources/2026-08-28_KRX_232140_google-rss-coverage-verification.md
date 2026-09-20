---
id: verification-2026-08-28-KRX-232140-google-rss-coverage
type: verification
title: KRX 232140 Google RSS Coverage Verification
created: 2026-08-28
updated: 2026-08-28
status: verification
stage: 1

market: KRX
ticker: "232140"
company: 와이씨
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=232140
    - name=와이씨
    - naver_article_count=3
    - google_rss_article_count=9
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 232140
    - 와이씨
  possible_impact: unknown
  uncertainty:
    - Article body archive is available locally, but entity/event verification is still unknown.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: true
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

# KRX 232140 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-28_KRX_232140_google-rss-coverage-source]]

## Facts Checked
- `code=232140`
- `name=와이씨`
- `naver_article_count=3`
- `google_rss_article_count=9`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[공시분석] 와이씨, 삼성전자와 622억원 웨이퍼 테스터 공급계약…작년 매출 60% 육박 - 알파경제`
- Source: `알파경제`
- Published at: `2026-08-27T22:31:37+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE9GcnlJRjNKd0xOWmNEbUpXOVpmN3kxS2xOVkZYajJuUmdGbUZyWXJDNk5nYk1HQUY4bk9WSFZNZXByTm9xU3ZxUGF3VkRCbkh3ekdfM1BBSFhoSzhQYnVhVjdpR2l6ckFkLTNFWEdR?oc=5`

## Article Body Archive Checked
- Title: `[공시분석] 와이씨, 삼성전자와 622억원 웨이퍼 테스터 공급계약…작년 매출 60% 육박 - 알파경제`
- Source: `알파경제`
- Published at: `2026-08-27T22:31:37+09:00`
- URL: `https://news.google.com/rss/articles/CBMibkFVX3lxTE9GcnlJRjNKd0xOWmNEbUpXOVpmN3kxS2xOVkZYajJuUmdGbUZyWXJDNk5nYk1HQUY4bk9WSFZNZXByTm9xU3ZxUGF3VkRCbkh3ekdfM1BBSFhoSzhQYnVhVjdpR2l6ckFkLTNFWEdR?oc=5`
- Evidence path: `https://www.etoday.co.kr/news/view/2619010`
- Body excerpt: △ 코윈테크, 85억원 규모 ESS 로봇 수주 △ 미래에셋증권, 일본 증권사 인수 추진…인수 후보군은 미정 △ 동원수산, 기업가치 제고 계획 공시 △ 피델릭스, 124억원 규모 메모리 반도체 공급계약 체결 △ 와이씨, 1622억원 규모 반도체 검사장비 공급계약 체결 △ 젝시믹스, 20억원 규모 자사주 소각 결정 △ SSG닷컴, 신세계몰 인적분할 예고 △ 휴맥스, 합병신고서 효력 발생 △더코디, 15억원 제3자배정 유상증자 △ 코윈테크, 85억원 규모 ESS 로봇 수주 △ 미래에셋증권, 일본 증권사 인수 추진…인수 후보군은 미정 △ 동원수산, 기업가치 제고 계획 공시 △ 피델릭스, 124억원 규모 메모리 반도체 공급계약 체결 △ 와이씨, 1622억원 규모 반도체 검사장비 공급계약 체결 △ 젝시믹스, 20억원 규모 자사주 소각 결정 △ SSG닷컴, 신세계몰 인적분할 예고 △ 휴맥스, 합병신고서 효력 발생 △더코디, 15억원 제3자배정 유상증자 관련 뉴스 주요 뉴스 많이 본 뉴스 마켓 뉴스

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-28T21:05:33+09:00`
- Company: [[KRX_232140_와이씨]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-28.json`
- Latest observation title: `와이씨, 삼성전자 수주 잭팟...아이디벤처스도 ‘방긋’ - 뉴스톱`
- Latest observation source: `뉴스톱`
- Latest observation published_at: `2026-08-28T10:59:15+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMib0FVX3lxTFB5UDJ6eXlLdzdSdjRJRVo0Ylo3ZjNRNjRIZ0ZtQkJnTWJ5NWVCSjFWM0xBQWh3QTJpNTAwamhjaTJDUGY2M2Y1VzV1RUFtRUJHUk9MeEdHUmV3QnY5d0w1UTBUaHNtX2Y5SWFPMXRXTQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
