from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"

CANDIDATES_CSV = LOGS / "candidates_latest_data.with_final_score.csv"
SURGE_REALTIME_CSV = LOGS / "surge_realtime_latest.csv"
STATE_SHADOW_CSV = LOGS / "surge_state_machine_shadow_latest.csv"
OHLCV_PARQUET = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"

OUT_JSON = LOGS / "surge_precursor_coverage_matrix_latest.json"
OUT_CSV = LOGS / "surge_precursor_coverage_matrix_latest.csv"


def _csv_columns(path: Path) -> set[str]:
    if not path.exists() or path.stat().st_size <= 5:
        return set()
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return set(csv.DictReader(fp).fieldnames or [])


def _has_ohlcv() -> bool:
    return OHLCV_PARQUET.exists() and OHLCV_PARQUET.stat().st_size > 5


def _item(
    category: str,
    name: str,
    status: str,
    source_type: str,
    fields: list[str],
    note: str,
) -> dict[str, Any]:
    return {
        "category": category,
        "item": name,
        "coverage_status": status,
        "source_type": source_type,
        "evidence_fields": "|".join(fields),
        "note": note,
    }


def _build_items() -> list[dict[str, Any]]:
    return [
        _item("가격 전조", "전일 대비 갭률", "PARTIAL_REFLECTED", "market_rising", ["open_gap_pct"], "market_rising open_gap_pct를 state shadow 출력/점수에 부분 반영"),
        _item("가격 전조", "시가 대비 상승률", "REFLECTED", "intraday_prices", ["open", "current_price"], "OPEN_TO_CURRENT_GE_4PCT/FADE로 state shadow 반영"),
        _item("가격 전조", "장중 고가 돌파 여부", "PARTIAL_REFLECTED", "surge_realtime+intraday_prices", ["cond_15d_highest_close", "current_price", "high"], "15일 고가 돌파와 장중 고가 근접을 state shadow 반영"),
        _item("가격 전조", "5일/20일 신고가", "PARTIAL_REFLECTED", "surge_realtime+ohlcv", ["ret_5d", "prev_close_5d", "rs", "rs_slope"], "5/20일 상대강도는 반영, 신고가 자체는 미분리"),
        _item("가격 전조", "52주 신고가 근접률", "REFLECTED", "candidate+ohlcv", ["high_52w_gap"], "NEAR_52W_HIGH로 전조 점수 반영"),
        _item("가격 전조", "전고점 돌파", "REFLECTED", "ohlcv", ["high", "close"], "BREAK_PREV_20D_HIGH/BREAK_PREV_60D_HIGH로 state shadow 반영"),
        _item("가격 전조", "박스권 상단 돌파", "REFLECTED", "ohlcv", ["high", "low", "close"], "BOX20_UPPER_BREAK로 state shadow 반영"),
        _item("가격 전조", "저항선 돌파 후 유지", "INTRADAY_HISTORY_NEEDED", "intraday_history", ["current_price"], "돌파 후 유지시간 추적 필요"),
        _item("가격 전조", "VWAP 위 체류 시간", "INTRADAY_HISTORY_NEEDED", "intraday_history", [], "VWAP/체류시간 시계열 미수집 또는 미연결"),
        _item("가격 전조", "분봉 기준 연속 양봉 수", "INTRADAY_HISTORY_NEEDED", "intraday_history", [], "분봉 OHLC 연속성 필요"),
        _item("가격 전조", "급락 후 V자 반등률", "PARTIAL_REFLECTED", "surge_realtime+intraday_prices", ["intraday_low_rebound_pct", "intraday_range_position_pct"], "저점 이후 현재 회복률/장중 범위 위치는 반영, 완전한 V자 시계열 패턴은 미분리"),
        _item("가격 전조", "장대양봉 출현", "PARTIAL_REFLECTED", "ohlcv+intraday", ["open", "close", "high", "low"], "일봉 LONG_BULL_CANDLE로 부분 반영"),
        _item("가격 전조", "윗꼬리 비율", "PARTIAL_REFLECTED", "surge_realtime", ["intraday_high_drawdown_pct"], "고점반락은 반영, 캔들 윗꼬리 비율은 미분리"),
        _item("가격 전조", "종가 고가 근접도", "REFLECTED", "ohlcv", ["close", "high"], "DAILY_CLOSE_NEAR_HIGH로 state shadow 반영"),

        _item("거래량·거래대금 전조", "1분 거래량 / 20봉 평균", "INTRADAY_HISTORY_NEEDED", "intraday_1m", [], "1분봉 평균 시계열 필요"),
        _item("거래량·거래대금 전조", "5분 거래량 / 20봉 평균", "INTRADAY_HISTORY_NEEDED", "intraday_5m", [], "5분봉 평균 시계열 필요"),
        _item("거래량·거래대금 전조", "금일 누적 거래량 / 20일 평균", "REFLECTED", "surge_realtime", ["rvol20", "volume_now", "avg_vol20"], "RVOL_GE_2/3로 intraday 점수 반영"),
        _item("거래량·거래대금 전조", "금일 거래대금 순위", "PARTIAL_REFLECTED", "market_rising", ["trading_value", "rank"], "MARKET_RISING_VALUE_RANK_TOP30 proxy로 부분 반영"),
        _item("거래량·거래대금 전조", "거래대금 증가율", "PARTIAL_REFLECTED", "candidate+surge_realtime", ["value", "trading_value"], "절대 거래대금 반영, 증가율은 미분리"),
        _item("거래량·거래대금 전조", "전일 대비 거래량 증가율", "REFLECTED", "candidate+ohlcv", ["v_accel"], "VOLUME_ACCELERATION으로 전조 점수 반영"),
        _item("거래량·거래대금 전조", "첫 30분 거래대금 집중도", "INTRADAY_HISTORY_NEEDED", "intraday_history", [], "시간대별 누적 거래대금 필요"),
        _item("거래량·거래대금 전조", "거래량 없이 가격 상승 여부", "REFLECTED", "surge_realtime", ["change_pct", "rvol20"], "PRICE_UP_WITH_WEAK_RVOL로 state shadow risk 반영"),
        _item("거래량·거래대금 전조", "가격 상승 대비 거래량 동반 여부", "REFLECTED", "surge_realtime", ["change_pct", "rvol20"], "change_pct와 rvol20이 intraday 점수에 동시 반영"),
        _item("거래량·거래대금 전조", "거래량 재폭발 2차/3차 여부", "INTRADAY_HISTORY_NEEDED", "intraday_history", [], "다중 burst 상태 전이 추적 필요"),
        _item("거래량·거래대금 전조", "대량 체결 발생 횟수", "INTRADAY_TRADE_TICK_NEEDED", "ticks", [], "체결 tick 또는 대량체결 이벤트 필요"),
        _item("거래량·거래대금 전조", "체결 빈도 증가율", "INTRADAY_TRADE_TICK_NEEDED", "ticks", [], "체결 빈도 시계열 필요"),

        _item("호가·체결 전조", "매수잔량 / 매도잔량 비율", "REFLECTED", "surge_realtime", ["order_imbalance_l1"], "BID_IMBALANCE_L1_POSITIVE/ASK_SUPPLY_IMBALANCE_L1로 state shadow 반영"),
        _item("호가·체결 전조", "상위 5호가 매도벽 얇음", "PARTIAL_REFLECTED", "surge_realtime", ["ask_depth_levels", "askq1", "askq2", "askq3", "askq4", "askq5"], "ask_depth_levels 반영, 5호가 벽 두께 모델은 미분리"),
        _item("호가·체결 전조", "매도벽 소화 속도", "INTRADAY_LOB_HISTORY_NEEDED", "lob_history", ["askq1", "askq5"], "속도 계산에는 연속 LOB 스냅샷 필요"),
        _item("호가·체결 전조", "매수호가 따라붙기 속도", "INTRADAY_LOB_HISTORY_NEEDED", "lob_history", [], "bid side 연속 호가 추적 필요"),
        _item("호가·체결 전조", "스프레드 확대 후 축소", "INTRADAY_LOB_HISTORY_NEEDED", "lob_history", ["spread_bps"], "현재 spread 수준은 반영, 확대 후 축소 패턴은 미구현"),
        _item("호가·체결 전조", "체결강도", "PARTIAL_REFLECTED", "surge_realtime", ["orderflow_tag", "orderflow_risk_score"], "ORDERFLOW_STRENGTH_OK / ORDERFLOW_RISK_HIGH proxy로 부분 반영"),
        _item("호가·체결 전조", "시장가 매수 비율", "INTRADAY_TRADE_TICK_NEEDED", "ticks", [], "매수/매도 체결 aggressor 분류 필요"),
        _item("호가·체결 전조", "대량 매수 체결 비율", "INTRADAY_TRADE_TICK_NEEDED", "ticks", [], "대량 매수 tick 분류 필요"),
        _item("호가·체결 전조", "호가 공백 구간 존재", "PARTIAL_REFLECTED", "surge_realtime", ["ask_depth_levels", "spread_bps"], "depth/spread로 부분 반영"),
        _item("호가·체결 전조", "VI 근접 속도", "EXTERNAL_OR_RULE_NEEDED", "market_rules+intraday", [], "VI 기준가/속도 모델 필요"),
        _item("호가·체결 전조", "상한가 잔량 형성 여부", "PARTIAL_REFLECTED", "surge_realtime", ["cond_limit_near", "ask_depth_levels"], "LIMIT_NEAR_WITH_ASK_DEPTH proxy로 부분 반영"),
        _item("호가·체결 전조", "허매수/허매도 의심 패턴", "INTRADAY_LOB_HISTORY_NEEDED", "lob_history", [], "호가 취소/재등장 패턴 추적 필요"),

        _item("변동성 전조", "ATR 급증", "SOURCE_AVAILABLE_NOT_REFLECTED", "candidate+surge_realtime", ["atr14_pct", "atr_entry_cap_pct"], "ATR 수준은 존재, 급증률은 미분리"),
        _item("변동성 전조", "분봉 변동폭 확대", "INTRADAY_HISTORY_NEEDED", "intraday_bar", [], "분봉 range 평균 필요"),
        _item("변동성 전조", "고가-저가 범위 / 평균 범위", "REFLECTED", "surge_realtime+intraday_prices", ["day_range_pct", "high", "low"], "DAY_RANGE_EXPANSION_GE_12PCT로 state shadow 반영"),
        _item("변동성 전조", "변동성 압축 후 확장", "PARTIAL_REFLECTED", "candidate+ohlcv", ["bb_width", "atr14_pct"], "압축은 반영, 이후 확장은 미분리"),
        _item("변동성 전조", "Bollinger Band 상단 돌파", "PARTIAL_REFLECTED", "candidate", ["boll_mid_pos", "bb_width"], "BOLLINGER_UPPER_ZONE proxy로 부분 반영"),
        _item("변동성 전조", "밴드폭 확대율", "PARTIAL_REFLECTED", "candidate", ["bb_width", "squeeze_on", "squeeze_momentum"], "SQUEEZE_MOMENTUM_POSITIVE proxy로 부분 반영"),
        _item("변동성 전조", "갭 후 변동성 유지", "INTRADAY_HISTORY_NEEDED", "intraday_history", [], "gap과 이후 range 유지 추적 필요"),
        _item("변동성 전조", "눌림폭 제한", "PARTIAL_REFLECTED", "surge_realtime", ["intraday_high_drawdown_pct"], "고점 대비 되돌림으로 부분 반영"),
        _item("변동성 전조", "급등 후 되돌림률", "REFLECTED", "surge_realtime", ["intraday_high_drawdown_pct"], "HIGH_REJECTION_DRAWDOWN으로 반영"),

        _item("수급 전조", "외국인 순매수", "REFLECTED", "candidate", ["foreign_net", "foreign_net_20d"], "FOREIGN_NET_BUY로 state shadow 반영"),
        _item("수급 전조", "기관 순매수", "REFLECTED", "candidate", ["institution_net", "institution_net_20d"], "INSTITUTION_NET_BUY로 state shadow 반영"),
        _item("수급 전조", "개인 매수 집중", "PARTIAL_REFLECTED", "candidate", ["personal_net", "personal_net_20d"], "PERSONAL_ONLY_BUY_PRESSURE risk proxy로 부분 반영"),
        _item("수급 전조", "프로그램 순매수", "SOURCE_MISSING", "program_trade", [], "현재 확인 컬럼 없음"),
        _item("수급 전조", "연속 순매수 일수", "DERIVABLE_NOT_REFLECTED", "flow_history", ["foreign_net_20d", "institution_net_20d"], "일수 계산용 히스토리 구조 필요"),
        _item("수급 전조", "외인·기관 동시 매수", "REFLECTED", "candidate", ["foreign_net", "institution_net"], "FOREIGN_INSTITUTION_COBUY로 state shadow 반영"),
        _item("수급 전조", "전일 대비 수급 방향 전환", "SOURCE_MISSING", "flow_history", [], "전일/당일 수급 변화 히스토리 필요"),
        _item("수급 전조", "신용잔고 증가율", "PARTIAL_REFLECTED", "candidate", ["credit_balance_qty", "credit_balance_ratio"], "증가율은 미구현, high credit risk proxy 반영"),
        _item("수급 전조", "대차잔고 감소", "PARTIAL_REFLECTED", "candidate", ["lend_balance_qty", "lend_balance_amount"], "감소율은 미구현, lend amount vs turnover risk proxy 반영"),
        _item("수급 전조", "공매도 비중 급감", "PARTIAL_REFLECTED", "candidate", ["short_balance_ratio", "short_balance_qty"], "급감은 미구현, high short balance risk proxy 반영"),
        _item("수급 전조", "공매도 잔고 대비 거래대금", "REFLECTED", "candidate", ["short_balance_amount", "value", "trading_value"], "SHORT_BALANCE_AMOUNT_HIGH_VS_TURNOVER로 state shadow 반영"),

        _item("뉴스·공시 촉매", "실적 서프라이즈", "PARTIAL_REFLECTED", "candidate", ["eps_revision_delta", "op_consensus_change", "news_implication_top_actions"], "뉴스 implication boost/risk proxy로 부분 반영"),
        _item("뉴스·공시 촉매", "대형 수주", "PARTIAL_REFLECTED", "candidate", ["news_implication_top_actions", "news_entity_top_tags"], "NEWS_CONTRACT_OR_SUPPLY keyword proxy로 부분 반영"),
        _item("뉴스·공시 촉매", "공급계약", "PARTIAL_REFLECTED", "candidate", ["news_implication_top_actions"], "NEWS_CONTRACT_OR_SUPPLY keyword proxy로 부분 반영"),
        _item("뉴스·공시 촉매", "투자유치", "PARTIAL_REFLECTED", "candidate", ["news_implication_top_actions"], "NEWS_INVESTMENT_FUNDING keyword proxy로 부분 반영"),
        _item("뉴스·공시 촉매", "정부정책 수혜", "PARTIAL_REFLECTED", "candidate", ["news_implication_top_scopes", "macro_score", "policy_score"], "뉴스 implication positive/boost proxy로 부분 반영"),
        _item("뉴스·공시 촉매", "테마 뉴스 발생", "REFLECTED", "candidate+surge_realtime", ["news_score", "news_implication_rows"], "news_score와 news implication positive/boost를 전조 점수 반영"),
        _item("뉴스·공시 촉매", "특허/승인/인허가", "PARTIAL_REFLECTED", "candidate", ["news_implication_top_actions", "news_entity_top_tags"], "NEWS_APPROVAL_OR_PATENT keyword proxy로 부분 반영"),
        _item("뉴스·공시 촉매", "자사주 매입", "PARTIAL_REFLECTED", "candidate", ["news_implication_top_actions"], "NEWS_BUYBACK keyword proxy로 부분 반영"),
        _item("뉴스·공시 촉매", "최대주주 변경", "SOURCE_AVAILABLE_NOT_REFLECTED", "candidate", ["news_implication_top_actions"], "뉴스 taxonomy 세분화 필요"),
        _item("뉴스·공시 촉매", "경영권 분쟁", "SOURCE_AVAILABLE_NOT_REFLECTED", "candidate", ["news_implication_top_actions"], "뉴스 taxonomy 세분화 필요"),
        _item("뉴스·공시 촉매", "M&A/매각설", "PARTIAL_REFLECTED", "candidate", ["news_implication_top_actions"], "NEWS_MA keyword proxy로 부분 반영"),
        _item("뉴스·공시 촉매", "신규사업 진출", "PARTIAL_REFLECTED", "candidate", ["news_implication_top_actions"], "NEWS_NEW_BUSINESS keyword proxy로 부분 반영"),
        _item("뉴스·공시 촉매", "거래소 조회공시", "EXTERNAL_OR_RULE_NEEDED", "disclosure", [], "공시 원문/조회공시 분류 원천 필요"),
        _item("뉴스·공시 촉매", "감사보고서 이슈", "PARTIAL_REFLECTED", "candidate", ["krx_admin", "krx_warning", "krx_risk", "junk_flags"], "AUDIT_REPORT_RISK keyword proxy와 KRX risk로 부분 반영"),

        _item("테마·섹터 전조", "동일 테마 대장주 상승", "REFLECTED", "candidate", ["sector_leader_code", "sector_leader_effect", "sector_leader_coupling"], "SECTOR_LEADER_COUPLING으로 state shadow 반영"),
        _item("테마·섹터 전조", "후발주 동반 상승", "DERIVABLE_NOT_REFLECTED", "candidate", ["sector_code", "sector_strength"], "섹터 그룹 계산 필요"),
        _item("테마·섹터 전조", "섹터 평균 상승률", "REFLECTED", "candidate", ["sector_score", "sector_strength"], "SECTOR_SUPPORT/SECTOR_STRENGTH로 state shadow 반영"),
        _item("테마·섹터 전조", "테마 내 거래대금 집중", "DERIVABLE_NOT_REFLECTED", "candidate+surge_realtime", ["sector_code", "value", "trading_value"], "섹터별 거래대금 집계 필요"),
        _item("테마·섹터 전조", "테마 뉴스 빈도 증가", "SOURCE_AVAILABLE_NOT_REFLECTED", "candidate", ["news_article_count", "news_entity_top_tags"], "빈도 증가율 미구현"),
        _item("테마·섹터 전조", "관련 ETF 상승", "EXTERNAL_SOURCE_NEEDED", "etf/global", [], "관련 ETF 매핑/가격 원천 필요"),
        _item("테마·섹터 전조", "글로벌 관련주 상승", "EXTERNAL_SOURCE_NEEDED", "global_equity", [], "글로벌 peer 매핑/가격 원천 필요"),
        _item("테마·섹터 전조", "전일 미국장 관련 섹터 강세", "EXTERNAL_SOURCE_NEEDED", "us_sector", [], "미국 섹터/테마 원천 필요"),
        _item("테마·섹터 전조", "테마 순환매 초입", "DERIVABLE_NOT_REFLECTED", "theme_history", ["sector_strength", "sector_score"], "과거 섹터 순환 히스토리 필요"),
        _item("테마·섹터 전조", "과거 급등 테마 재점화", "DERIVABLE_NOT_REFLECTED", "theme_history", ["sector_code", "surge_type"], "테마별 과거 급등 이력 필요"),

        _item("구조적 전조", "유통주식수 낮음", "PARTIAL_REFLECTED", "candidate", ["listed_shares", "market_cap"], "LOW_LISTED_SHARES_PROXY로 부분 반영"),
        _item("구조적 전조", "시가총액 작음", "REFLECTED", "candidate", ["market_cap"], "SMALL_MARKET_CAP로 state shadow 반영"),
        _item("구조적 전조", "품절주 성격", "PARTIAL_REFLECTED", "candidate", ["listed_shares", "market_cap", "value"], "listed shares/market cap proxy로 부분 반영"),
        _item("구조적 전조", "대주주 지분율 높음", "SOURCE_MISSING", "shareholder", [], "대주주 지분율 원천 미확인"),
        _item("구조적 전조", "최근 거래정지 해제", "EXTERNAL_OR_RULE_NEEDED", "krx_status_history", ["krx_admin", "krx_warning", "krx_risk"], "현재 상태 flag는 있으나 해제 이력은 없음"),
        _item("구조적 전조", "보호예수 해제 전후", "EXTERNAL_SOURCE_NEEDED", "lockup_schedule", [], "보호예수 일정 원천 필요"),
        _item("구조적 전조", "신규상장주", "REFLECTED", "candidate", ["listing_days"], "RECENT_LISTING으로 state shadow 반영"),
        _item("구조적 전조", "스팩/우선주/관리종목 여부", "PARTIAL_REFLECTED", "candidate", ["market", "krx_admin", "krx_warning", "krx_risk", "krx_caution"], "KRX hard risk는 반영, SPAC/우선주 분류는 미분리"),
        _item("구조적 전조", "과거 급등 이력", "DERIVABLE_NOT_REFLECTED", "surge_history", ["surge_type", "surge_score_final"], "과거 surge 로그 집계 필요"),
        _item("구조적 전조", "상한가 이력", "DERIVABLE_NOT_REFLECTED", "ohlcv", ["high", "close"], "parquet 기반 단순 pct_change가 과다 검출되어 state shadow 반영 제외"),
        _item("구조적 전조", "최근 20일 거래대금 바닥 후 증가", "REFLECTED", "ohlcv", ["value"], "VALUE_BOTTOM_REBOUND_20D로 state shadow 반영"),
    ]


def main() -> int:
    candidate_cols = _csv_columns(CANDIDATES_CSV)
    realtime_cols = _csv_columns(SURGE_REALTIME_CSV)
    state_cols = _csv_columns(STATE_SHADOW_CSV)
    ohlcv_exists = _has_ohlcv()

    rows = _build_items()
    by_status = Counter(row["coverage_status"] for row in rows)
    by_category = Counter(row["category"] for row in rows)
    reflected = sum(1 for row in rows if row["coverage_status"] in {"REFLECTED", "PARTIAL_REFLECTED"})
    available_not_reflected = sum(
        1
        for row in rows
        if row["coverage_status"] in {"SOURCE_AVAILABLE_NOT_REFLECTED", "DERIVABLE_NOT_REFLECTED"}
    )

    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "surge_precursor_coverage_matrix",
        "summary": {
            "total_items": len(rows),
            "reflected_or_partial_items": reflected,
            "not_reflected_but_source_or_derivable_items": available_not_reflected,
            "status_counts": dict(by_status),
            "category_counts": dict(by_category),
            "candidate_columns": len(candidate_cols),
            "surge_realtime_columns": len(realtime_cols),
            "state_shadow_columns": len(state_cols),
            "ohlcv_parquet_exists": ohlcv_exists,
        },
        "source_files": {
            "candidates": str(CANDIDATES_CSV),
            "surge_realtime": str(SURGE_REALTIME_CSV),
            "state_shadow": str(STATE_SHADOW_CSV),
            "ohlcv_parquet": str(OHLCV_PARQUET),
        },
        "risk_contract": {
            "read_only_diagnostic": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "dispatch_enabled": False,
            "trading_allowed": False,
        },
        "rows": rows,
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as fp:
        fields = ["category", "item", "coverage_status", "source_type", "evidence_fields", "note"]
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(json.dumps({"status": "OK", **payload["summary"], "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
