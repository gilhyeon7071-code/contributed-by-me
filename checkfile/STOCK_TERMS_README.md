# Stock Terms Glossary

- File: `E:\1_Data\checkfile\stock_terms_glossary.json`
- Loader: `E:\1_Data\checkfile\stock_terms.py`
- Applied by: `generate_html_report()` in `E:\1_Data\checkfile\orchestrator.py`

## Purpose

This glossary is the single external source for stock-operation wording used in the verification report.
The file is pre-filled with the current full term set (item/message/expected/token), so future runs reuse the same wording.

## How to update

1. Edit `stock_terms_glossary.json`.
2. Run `E:\1_Data\run_system_verification.bat`.
3. Open the latest HTML in `E:\1_Data\checkfile\outputs\`.

## Main sections

- `item_name_map`: verification item name translation
- `message_map`: full-message translation
- `expected_map`: exact expected-value translation
- `token_map`: key-token translation for actual/expected fields
- `phase_title_map`: phase title override by enum name
- `api_provider_map`: API provider label
- `message_prefix_map`: prefix-based message replacement
- `expected_post_replacements`: post replacements for expected text

- domain_term_catalog: 현재 검증 로직 기준 필수 주식/퀀트 용어집(한글/영문/설명)

## Domain Terms

- domain_term_catalog.categories: 실사용 중심 카테고리별 용어 목록
- domain_term_catalog.aliases: 현재 검증 항목명과 표준 용어 매핑
- 기존 리포트 렌더링 로직에는 영향 없이, 용어 표준화/문서화 용도로 재사용 가능
