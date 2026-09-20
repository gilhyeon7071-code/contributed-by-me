---
id: verification-2026-08-29-KRX-003350-google-rss-coverage
type: verification
title: KRX 003350 Google RSS Coverage Verification
created: 2026-08-29
updated: 2026-08-29
status: verification
stage: 1

market: KRX
ticker: "003350"
company: 한국화장품제조
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=003350
    - name=한국화장품제조
    - naver_article_count=1
    - google_rss_article_count=3
    - kis_title_count=18
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003350
    - 한국화장품제조
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

# KRX 003350 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-29_KRX_003350_google-rss-coverage-source]]

## Facts Checked
- `code=003350`
- `name=한국화장품제조`
- `naver_article_count=1`
- `google_rss_article_count=3`
- `kis_title_count=18`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `한국화장품제조 투자분석 2026. 08. 28 - 주달`
- Source: `주달`
- Published at: `2026-08-28T17:12:55+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE5hQzdPb1Bob0lnZTEwWVAxcGVTbC0yRFdFdGJ3M2ZhOWQ2elhfcFVfMzNhYlBRX0hXOWsyb3IwSy1ORWdsa1ZXQTNHZlBOWnRYN3JrUmZjWjh4YjVLcWQ5S3pScVBVUXpJeWxOTzRNTGI2QjQ?oc=5`

## Article Body Archive Checked
- Title: `한국화장품제조 투자분석 2026. 08. 28 - 주달`
- Source: `주달`
- Published at: `2026-08-28T17:12:55+09:00`
- URL: `https://news.google.com/rss/articles/CBMic0FVX3lxTE5hQzdPb1Bob0lnZTEwWVAxcGVTbC0yRFdFdGJ3M2ZhOWQ2elhfcFVfMzNhYlBRX0hXOWsyb3IwSy1ORWdsa1ZXQTNHZlBOWnRYN3JrUmZjWjh4YjVLcWQ5S3pScVBVUXpJeWxOTzRNTGI2QjQ?oc=5`
- Evidence path: `https://www.newspim.com/news/view/20260824000523`
- Body excerpt: AI 핵심 요약 beta- 화장품주가 24일 호실적 기대에 강세를 보였다. - 오가닉티코스메틱이 상한가, 에이피알도 크게 올랐다. - 해외 매출 확대와 미국 성장 기대가 투자심리를 자극했다. !AI가 자동 생성한 요약으로 정확하지 않을 수 있어요. 에이피알·한국화장품제조 등 상승 [서울=뉴스핌] 이정아 기자 = 화장품주가 호실적과 해외 시장 성장 기대감에 힘입어 24일 장중 일제히 강세를 보이고 있다. 이날 오전 10시 57분 기준 오가닉티코스메틱은 전 거래일보다 960원(29.86%) 오른 4175원에 거래되며 상한가를 기록했다. 에이피알도 2만8500원(7.40%) 오른 41만3500원을 나타내고 있다. 관련 종목도 동반 상승세다. 한국화장품제조(8.33%), 코스맥스(4.98%), 제이투케이바이오(4.34%), 케어젠(4.43%), 실리콘투(3.99%), 달바글로벌(3.93%), 차AI헬스케어(5.00%), 미원상사(3.54%) 등이 오름세를 보이고 있다. 시장에서는 화장품 업종의 실적 개선세가 이어지는 가운데 해외 매출 확대가 투자심리를 자극한 것으로 보고 있다. 김지은 BNK투자증권 연구원은 "K-화장품 성장축이 비중국과 미국으로 교체되고 있다"며 "지난 7월 미국 단일국이 중화권 전체를 처음으로 추월했다"고 분석했다. 그러면서 "C-뷰티의 추격은 사실이지만, 추월은 아직"이라며 "K-뷰티의 방어선은 가격이 아니라 기술과 신뢰"라고 강조했다. plum@newspim.com

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-29T23:05:31+09:00`
- Company: [[KRX_003350_한국화장품제조]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-29.json`
- Latest observation title: `특징주, 한국화장품제조-화장품 테마 상승세에 9.02% ↑ - 씽크풀 AI`
- Latest observation source: `씽크풀 AI`
- Latest observation published_at: `2026-08-28T14:13:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVEFVX3lxTE9seWh5XzljQ2hmck5KSTdaUTB6WEh6V1VnSVZPeUMxbEZmSE1JNjdrQUhRQ3djSHVrcGlqbkZrc3JhTmNRa1ViT3BzMG5HdDFwb2tBNQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
