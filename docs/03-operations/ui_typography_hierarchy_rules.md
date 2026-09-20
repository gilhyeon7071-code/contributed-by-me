# UI Typography Hierarchy Rules (SSOT)

## 1) 목적
- 메뉴는 네비게이션 역할만 수행한다.
- 메인 KPI/리스크/액션이 시선 우선순위 1순위를 가진다.
- 활성/비활성 상태를 텍스트 굵기보다 상태 배경/보더로 구분한다.

## 2) 공통 위계
- H1 페이지 타이틀: `font-size 30px`, `font-weight 900`
- 섹션 타이틀: `font-size 18~22px`, `font-weight 700~800`
- KPI 라벨: `font-size 13px`, `font-weight 500`, 대비 중간
- KPI 값: `font-size 24px+`, `font-weight 800~900`, 숫자 폰트
- 운영/로직판정/시스템헬스 요약 카드 제목: `font-size 14px`, `font-weight 700`
- 운영/로직판정/시스템헬스 요약 카드 값(일반): `font-size 18px`, `font-weight 600`
- 운영/로직판정/시스템헬스 요약 카드 값(compact): `font-size 16px`, `font-weight 500`
- 운영/로직판정/시스템헬스 요약 카드 힌트: `font-size 12px`, `font-weight 400`
- 운영/로직판정/시스템헬스 요약 카드 상세: `font-size 13px`, `font-weight 400`
- 운영/로직판정/시스템헬스 HTML 표 헤더: `font-size 12px`, `font-weight 700`
- 운영/로직판정/시스템헬스 HTML 표 본문: `font-size 12.5px`, `font-weight 400`
- 운영/로직판정/시스템헬스 HTML 표 캡션: `font-size 12px`, `font-weight 400`
- 사이드 메뉴 비활성: `font-size 14px`, `font-weight 500`, `opacity 0.68`
- 사이드 메뉴 활성: `font-size 14px`, `font-weight 600`, `opacity 1.0`

## 3) 사이드 메뉴 규칙
- 비활성 메뉴는 밝은 흰색 사용 금지 (`#b3c4df` 권장 범위).
- 비활성 메뉴는 색상/투명도로 디엠퍼시스한다.
- 활성 메뉴는 좌측 보더, 배경 하이라이트, 라벨 색상으로 강조한다.
- 메뉴 행간은 `line-height 1.25`로 고정한다.
- 라디오/불릿 아이콘은 기본 대비 10~20% 축소한다.

## 4) 상태별 스타일
- 비활성: 배경 투명, 얇은 보더, 낮은 대비
- Hover: 대비를 소폭 상승
- 활성: 진한 배경 + 좌측 포커스 보더 + 높은 대비
- Disabled: `opacity <= 0.5`

## 5) 적용 범위
- 통합 대시보드 (`E:\vibe\buffett\dashboard.py`)
- 주식 대시보드 (`E:\vibe\buffett\dashboard_stock_v2.py`)

## 6) 검증 체크리스트
- 메뉴보다 KPI가 먼저 보이는가
- 비활성 메뉴가 계속 튀지 않는가
- 활성 메뉴만 명확히 강조되는가
- H1/메뉴/KPI 라벨의 크기-굵기 위계가 유지되는가
- 한글/영문 혼용 시 메뉴의 굵기 체감 과다가 없는가
- HTML 표가 `st.dataframe`보다 우선 적용 대상인지 확인했는가
- 표 본문/헤더/캡션이 같은 기준값으로 유지되는가




