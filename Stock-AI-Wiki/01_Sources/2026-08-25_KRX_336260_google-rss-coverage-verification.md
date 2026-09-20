---
id: verification-2026-08-25-KRX-336260-google-rss-coverage
type: verification
title: KRX 336260 Google RSS Coverage Verification
created: 2026-08-25
updated: 2026-08-25
status: verification
stage: 1

market: KRX
ticker: "336260"
company: 두산퓨얼셀
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=336260
    - name=두산퓨얼셀
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 336260
    - 두산퓨얼셀
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

# KRX 336260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-25_KRX_336260_google-rss-coverage-source]]

## Facts Checked
- `code=336260`
- `name=두산퓨얼셀`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `27조원 SOFC 시장 쟁탈전..국내 기업 시장 확대 위해 속도전 - 전기신문`
- Source: `전기신문`
- Published at: `2026-08-10T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTFBHbk9ZcXlzMmZoNTBzVXkzZmFUYV9Wc3ZicHZQeG9lbWlFeWNETEhBNHdSLXpCaHktb0NqX2h4U2hDUzdwNHpBYldWclZFMTBVMjQ2LUpxTlROS0xGQVZMMUNLTFgtejlvc1FqU9IBcEFVX3lxTFBwbkRGUlhFZF9teU05NHpQUm0xN0N0LUtqQ2l1UjhITmk3TXkyVnBmaUJFdzdEVE1lWGNWcUhISWpjeTVGcmRiTXZrU041bENoY1J6R1gxQXdwSXcyQUFmcFcwYVJPcTF0UTIwX1o1cm0?oc=5`

## Article Body Archive Checked
- Title: `27조원 SOFC 시장 쟁탈전..국내 기업 시장 확대 위해 속도전 - 전기신문`
- Source: `전기신문`
- Published at: `2026-08-10T16:00:00+09:00`
- URL: `https://news.google.com/rss/articles/CBMibEFVX3lxTFBHbk9ZcXlzMmZoNTBzVXkzZmFUYV9Wc3ZicHZQeG9lbWlFeWNETEhBNHdSLXpCaHktb0NqX2h4U2hDUzdwNHpBYldWclZFMTBVMjQ2LUpxTlROS0xGQVZMMUNLTFgtejlvc1FqU9IBcEFVX3lxTFBwbkRGUlhFZF9teU05NHpQUm0xN0N0LUtqQ2l1UjhITmk3TXkyVnBmaUJFdzdEVE1lWGNWcUhISWpjeTVGcmRiTXZrU041bENoY1J6R1gxQXdwSXcyQUFmcFcwYVJPcTF0UTIwX1o1cm0?oc=5`
- Evidence path: `https://n.news.naver.com/mnews/article/011/0004652570?sid=101`
- Body excerpt: 美 30년물 국채 낙찰금리 5.216% 日 10년물 2.930%…메자닌 발행 9조 육박 두산퓨얼셀 8월 54.2% 반등 [주요 이슈 브리핑] ■ 국채금리 급등: 주요국의 재정 확대 기조가 국채 매도를 자극하며 미국·영국·일본의 장기금리를 동시에 밀어올렸다. 한국은 아직 재정 상태가 양호하다는 평가를 받지만 의무지출 증가와 반도체 세수 의존을 감안하면 재정 여력을 과신할 수 없다는 지적이 나오는 상황이다. ■ 조달 창구 이동: 회사채 금리가 높은 수준을 유지하고 증시가 강세를 보이면서 기업들이 메자닌(주식과 채권의 중간 성격을 지닌 자금조달 수단)으로 눈을 돌렸다. 또한 원자재 시장에서는 공급 부족과 인공지능(AI) 수요가 겹친 인듐 가격이 뛰며 관련 기업 실적을 끌어올린 모습이다. ■ 종목 온도차: 국내 증시가 반등하는 가운데 수주 모멘텀을 확보한 종목과 성장 재료가 소진된 종목의 주가 흐름이 뚜렷하게 갈렸다. 특히 엔터 업종은 대형 아티스트 활동 재개라는 호재가 이미 현실화된 뒤 목표주가 하향이 잇따르며 관련 상장지수펀드(ETF)까지 손실을 키운 양상이다. [금융상품 투자자 관심 뉴스] 1. 국채금리 폭등 美·엔저 못 막는 日…韓도 재정여력 과신 안 돼 - 핵심 요약: 주요국의 재정 확대가 국채금리 상승이라는 청구서로 돌아오고 있다. 미국 국가부채는 도널드 트럼프 미국 대통령 재집권 직전 35조 4647억 달러에서 올해 1분기 39조 650억 달러로 10.1% 불어났고, 최근 250억 달러 규모 30년물 국채 경매 낙찰 금리는 5.216%로 25년 만에 가장 높았다. 영국은 국내총생산(GDP) 대비 정부 부채 비율이 2019년 85%에서 올해 103.6%까지 오를 전망 속에 10년물 금리가 5.04%로 2007년 7월 이후 최고치를 기록했다. 한편 한국의 국가채무는 1412조...

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-25T23:05:22+09:00`
- Company: [[KRX_336260_두산퓨얼셀]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-25.json`
- Latest observation title: `27조원 SOFC 시장 쟁탈전..국내 기업 시장 확대 위해 속도전 - 전기신문`
- Latest observation source: `전기신문`
- Latest observation published_at: `2026-08-10T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTFBHbk9ZcXlzMmZoNTBzVXkzZmFUYV9Wc3ZicHZQeG9lbWlFeWNETEhBNHdSLXpCaHktb0NqX2h4U2hDUzdwNHpBYldWclZFMTBVMjQ2LUpxTlROS0xGQVZMMUNLTFgtejlvc1FqU9IBcEFVX3lxTFBwbkRGUlhFZF9teU05NHpQUm0xN0N0LUtqQ2l1UjhITmk3TXkyVnBmaUJFdzdEVE1lWGNWcUhISWpjeTVGcmRiTXZrU041bENoY1J6R1gxQXdwSXcyQUFmcFcwYVJPcTF0UTIwX1o1cm0?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
