---
id: verification-2026-08-28-KRX-078340-google-rss-coverage
type: verification
title: KRX 078340 Google RSS Coverage Verification
created: 2026-08-28
updated: 2026-08-28
status: verification
stage: 1

market: KRX
ticker: "078340"
company: 컴투스
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
    - code=078340
    - name=컴투스
    - naver_article_count=1
    - google_rss_article_count=86
    - kis_title_count=32
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 078340
    - 컴투스
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

# KRX 078340 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-28_KRX_078340_google-rss-coverage-source]]

## Facts Checked
- `code=078340`
- `name=컴투스`
- `naver_article_count=1`
- `google_rss_article_count=86`
- `kis_title_count=32`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `컴투스 '제우스: 오만의 신', 양대 마켓 매출 1위 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-08-27T10:26:47+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFA0eXFicHYxY0dybHl1RlhKOTc3ZVplZ2tNdG1NZmZRbjdGd1F5dkJ5RU1tTTlxMzNQRDR1WWx6cFBvbzAyYVJSZlJaSjdZR0k?oc=5`

## Article Body Archive Checked
- Title: `컴투스 '제우스: 오만의 신', 양대 마켓 매출 1위 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-08-27T10:26:47+09:00`
- URL: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFA0eXFicHYxY0dybHl1RlhKOTc3ZVplZ2tNdG1NZmZRbjdGd1F5dkJ5RU1tTTlxMzNQRDR1WWx6cFBvbzAyYVJSZlJaSjdZR0k?oc=5`
- Evidence path: `https://www.etoday.co.kr/news/view/2617842`
- Body excerpt: DAT 3사 보유 비트코인 평가가치 공시 당시보다 151억원 증가 거래소 보유자산·수수료 회복 기대…지분 보유 상장사도 주목 가상자산 가격이 급반등하면서 상반기 보유자산 가치 하락과 거래 감소로 부진했던 관련 기업의 재무 부담도 완화될 전망이다. 가상자산을 직접 보유한 디지털자산 트레저리(DAT) 기업부터 거래소와 거래소 지분 보유 상장사까지 효과가 번지는 모습이다. 25일 가상자산 시황정보 사이트 코인게코에 따르면 비트코인(BTC)은 7만9000달러 안팎에서 거래 중이다. 최근 일주일간 상승률은 22.9%에 달한다. 이더리움(ETH)도 같은 기간 30.4% 급등하며 2480달러대로 올라섰다. 투자심리도 빠르게 되살아났다. 가상자산 데이터 플랫폼 얼터너티브닷미의 공포·탐욕지수는 지난주 41로 ‘공포’ 구간에 머물렀지만 이날 74까지 오르며 ‘탐욕’으로 전환했다. 가장 직접적인 수혜 대상으로는 가상자산을 재무자산으로 축적하는 국내 DAT 기업이 꼽힌다. 금융감독원 전자공시시스템(DART)에 따르면 비트맥스는 비트코인 551개, 비트플래닛은 300개, 파라택시스코리아는 200개를 보유 중이다. 현재 원화 시세를 적용한 평가가치는 각각 약 593억원, 323억원, 215억원으로 공시 기준 시점인 6월 말보다 합산 약 178억원 증가했다. 가상자산거래소는 보유자산 가치와 수수료 수익 양쪽에서 개선 효과를 기대할 수 있다. 상반기 말 두나무의 보유 가상자산 평가가치는 1조3779억원으로 지난해 말보다 39.6% 감소했다. 빗썸도 같은 기간 30.5% 줄어든 1941억원에 그쳤다. 최근 가격 상승으로 당시 감소분 가운데 일부를 만회했을 가능성이 크다. 국내 거래소 영업수익 대부분이 거래·출금 수수료에서 발생하는 만큼 거래대금 증가도 실적 회복 요인으로 작용한다. 거래소 지분을 보유한 상장사...

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-28T21:05:33+09:00`
- Company: [[KRX_078340_컴투스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-28.json`
- Latest observation title: `컴투스 '제우스: 오만의 신', 통신 3사 결제 청구 할인 프로모션 실시 - 지디넷코리아`
- Latest observation source: `지디넷코리아`
- Latest observation published_at: `2026-08-28T16:30:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE8tTWFlZzZzM2ZfWTkzbzh6My1rOXRHSzhMY0lNSlBhZ3dSRkFya1RHNXVIbVdsLW9NbU44WDB0QVdrSjlCQUZ6UVNTOElWdUJHakVmeG5n?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
