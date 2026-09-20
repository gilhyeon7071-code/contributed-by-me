import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import IntEnum


# ============================================================================
# 기업 분석 모듈 (장세 판별과 연동)
# ============================================================================

class FundamentalGrade(IntEnum):
    """펀더멘털 등급"""
    F = 0  # 매우 나쁨
    D = 1  # 나쁨
    C = 2  # 보통
    B = 3  # 좋음
    A = 4  # 매우 좋음
    S = 5  # 최상급


@dataclass
class CompanyScore:
    """기업 종합 점수"""
    code: str
    name: str
    total_score: float          # 종합 점수 (0~100)
    grade: FundamentalGrade     # 등급

    # 세부 점수 (각 0~100)
    forward_value_score: float = 0.0  # 미래 투자가치 (추정PER + PEG)
    value_score: float = 0.0          # 현재 밸류에이션
    quality_score: float = 0.0        # 퀄리티 (수익성)
    growth_score: float = 0.0         # 성장성
    momentum_score: float = 0.0       # 모멘텀
    stability_score: float = 0.0      # 재무 안정성
    supply_score: float = 0.0         # 수급

    details: Dict[str, Any] = field(default_factory=dict)
    recommendation: str = ""


class CompanyAnalyzer:
    """
    기업 분석기
    
    장세별 가중치 연동:
        상승장: 성장 + 모멘텀 중시
        조정장: 가치 + 퀄리티 중시
        횡보장: 안정성 + 배당 중시
        하락장: 현금 보유 권장
    """
    
    # 장세별 팩터 가중치
    # forward_value: 미래 투자가치 (추정PER + PEG) — 상승장/조정장에서 비중 높임
    REGIME_WEIGHTS = {
        'BULL': {       # 상승장: 미래가치 + 성장 + 모멘텀
            'forward_value': 0.15,
            'value': 0.05,
            'quality': 0.15,
            'growth': 0.25,
            'momentum': 0.25,
            'stability': 0.05,
            'supply': 0.10,
        },
        'CORRECTION': { # 조정장: 미래가치 + 가치 + 퀄리티 (저평가 발굴)
            'forward_value': 0.20,
            'value': 0.10,
            'quality': 0.20,
            'growth': 0.10,
            'momentum': 0.10,
            'stability': 0.20,
            'supply': 0.10,
        },
        'SIDEWAYS': {   # 횡보장: 안정성 중심, 미래가치 소폭 반영
            'forward_value': 0.10,
            'value': 0.15,
            'quality': 0.20,
            'growth': 0.05,
            'momentum': 0.05,
            'stability': 0.35,
            'supply': 0.10,
        },
        'BEAR': {       # 하락장: 안정성 최우선, 미래가치 최소
            'forward_value': 0.05,
            'value': 0.10,
            'quality': 0.20,
            'growth': 0.00,
            'momentum': 0.05,
            'stability': 0.50,
            'supply': 0.10,
        },
    }
    
    # 밸류에이션 기준 (현재/과거 실적 기준)
    VALUE_CRITERIA = {
        'PER': {'low': 5, 'mid': 15, 'high': 30},
        'PBR': {'low': 0.5, 'mid': 1.5, 'high': 3.0},
        'PSR': {'low': 0.5, 'mid': 2.0, 'high': 5.0},
        'EV_EBITDA': {'low': 5, 'mid': 12, 'high': 20},
    }

    # 미래 투자가치 기준 (추정/TTM 기반)
    FORWARD_VALUE_CRITERIA = {
        # 추정PER: 미래 이익 기준 — 현재 PER보다 엄격한 기준 적용
        'forward_per': {'low': 8, 'mid': 15, 'high': 25},
        # PEG ratio: 성장 대비 밸류에이션 — 1 이하면 저평가 성장주
        'peg_ratio': {'excellent': 0.5, 'good': 1.0, 'fair': 2.0},
    }
    
    # 퀄리티 기준
    QUALITY_CRITERIA = {
        'ROE': {'low': 5, 'mid': 15, 'high': 25},
        'ROA': {'low': 2, 'mid': 8, 'high': 15},
        'OPM': {'low': 5, 'mid': 15, 'high': 25},     # 영업이익률
        'NPM': {'low': 3, 'mid': 10, 'high': 20},     # 순이익률
    }
    
    # 성장성 기준 (YoY %)
    GROWTH_CRITERIA = {
        'revenue_growth': {'low': 0, 'mid': 15, 'high': 30},
        'op_growth': {'low': 0, 'mid': 20, 'high': 40},
        'np_growth': {'low': 0, 'mid': 20, 'high': 40},
    }
    
    # 안정성 기준
    STABILITY_CRITERIA = {
        'debt_ratio': {'low': 50, 'mid': 100, 'high': 200},      # 낮을수록 좋음
        'current_ratio': {'low': 100, 'mid': 150, 'high': 200},  # 높을수록 좋음
        'interest_coverage': {'low': 3, 'mid': 10, 'high': 20},
    }
    
    def __init__(self):
        pass
    
    # ========== 개별 팩터 점수 계산 ==========
    
    def _score_value(self, data: Dict) -> Tuple[float, Dict]:
        """밸류에이션 점수 (낮을수록 좋음)"""
        scores = []
        details = {}
        
        # PER
        per = data.get('PER')
        if per is not None and per > 0:
            c = self.VALUE_CRITERIA['PER']
            if per <= c['low']:
                s = 100
            elif per <= c['mid']:
                s = 100 - (per - c['low']) / (c['mid'] - c['low']) * 50
            elif per <= c['high']:
                s = 50 - (per - c['mid']) / (c['high'] - c['mid']) * 40
            else:
                s = 10
            scores.append(s)
            details['PER'] = {'value': per, 'score': s}
        
        # PBR
        pbr = data.get('PBR')
        if pbr is not None and pbr > 0:
            c = self.VALUE_CRITERIA['PBR']
            if pbr <= c['low']:
                s = 100
            elif pbr <= c['mid']:
                s = 100 - (pbr - c['low']) / (c['mid'] - c['low']) * 50
            elif pbr <= c['high']:
                s = 50 - (pbr - c['mid']) / (c['high'] - c['mid']) * 40
            else:
                s = 10
            scores.append(s)
            details['PBR'] = {'value': pbr, 'score': s}
        
        # PSR
        psr = data.get('PSR')
        if psr is not None and psr > 0:
            c = self.VALUE_CRITERIA['PSR']
            if psr <= c['low']:
                s = 100
            elif psr <= c['mid']:
                s = 100 - (psr - c['low']) / (c['mid'] - c['low']) * 50
            else:
                s = max(10, 50 - (psr - c['mid']) / c['mid'] * 30)
            scores.append(s)
            details['PSR'] = {'value': psr, 'score': s}
        
        final_score = np.mean(scores) if scores else 50.0
        return final_score, details
    
    def _score_quality(self, data: Dict) -> Tuple[float, Dict]:
        """퀄리티 점수 (높을수록 좋음)"""
        scores = []
        details = {}
        
        # ROE
        roe = data.get('ROE')
        if roe is not None:
            c = self.QUALITY_CRITERIA['ROE']
            if roe <= 0:
                s = 0
            elif roe <= c['low']:
                s = roe / c['low'] * 30
            elif roe <= c['mid']:
                s = 30 + (roe - c['low']) / (c['mid'] - c['low']) * 40
            elif roe <= c['high']:
                s = 70 + (roe - c['mid']) / (c['high'] - c['mid']) * 30
            else:
                s = 100
            scores.append(s)
            details['ROE'] = {'value': roe, 'score': s}
        
        # ROA
        roa = data.get('ROA')
        if roa is not None:
            c = self.QUALITY_CRITERIA['ROA']
            if roa <= 0:
                s = 0
            elif roa <= c['mid']:
                s = roa / c['mid'] * 70
            else:
                s = min(100, 70 + (roa - c['mid']) / c['mid'] * 30)
            scores.append(s)
            details['ROA'] = {'value': roa, 'score': s}
        
        # 영업이익률
        opm = data.get('OPM')
        if opm is not None:
            c = self.QUALITY_CRITERIA['OPM']
            if opm <= 0:
                s = 0
            elif opm <= c['mid']:
                s = opm / c['mid'] * 70
            else:
                s = min(100, 70 + (opm - c['mid']) / c['mid'] * 30)
            scores.append(s)
            details['OPM'] = {'value': opm, 'score': s}
        
        final_score = np.mean(scores) if scores else 50.0
        return final_score, details
    
    def _score_growth(self, data: Dict) -> Tuple[float, Dict]:
        """성장성 점수"""
        scores = []
        details = {}
        
        # 매출 성장률
        rev_g = data.get('revenue_growth')
        if rev_g is not None:
            c = self.GROWTH_CRITERIA['revenue_growth']
            if rev_g <= c['low']:
                s = max(0, 30 + rev_g)  # 음수면 감점
            elif rev_g <= c['mid']:
                s = 30 + (rev_g - c['low']) / (c['mid'] - c['low']) * 40
            elif rev_g <= c['high']:
                s = 70 + (rev_g - c['mid']) / (c['high'] - c['mid']) * 30
            else:
                s = 100
            scores.append(s)
            details['revenue_growth'] = {'value': rev_g, 'score': s}
        
        # 영업이익 성장률
        op_g = data.get('op_growth')
        if op_g is not None:
            c = self.GROWTH_CRITERIA['op_growth']
            if op_g <= c['low']:
                s = max(0, 30 + op_g / 2)
            elif op_g <= c['mid']:
                s = 30 + (op_g - c['low']) / (c['mid'] - c['low']) * 40
            else:
                s = min(100, 70 + (op_g - c['mid']) / c['mid'] * 30)
            scores.append(s)
            details['op_growth'] = {'value': op_g, 'score': s}
        
        # 순이익 성장률
        np_g = data.get('np_growth')
        if np_g is not None:
            c = self.GROWTH_CRITERIA['np_growth']
            if np_g <= c['low']:
                s = max(0, 30 + np_g / 2)
            elif np_g <= c['mid']:
                s = 30 + (np_g - c['low']) / (c['mid'] - c['low']) * 40
            else:
                s = min(100, 70 + (np_g - c['mid']) / c['mid'] * 30)
            scores.append(s)
            details['np_growth'] = {'value': np_g, 'score': s}
        
        final_score = np.mean(scores) if scores else 50.0
        return final_score, details
    
    def _score_momentum(self, price_df: pd.DataFrame) -> Tuple[float, Dict]:
        """모멘텀 점수 (가격 데이터 기반)"""
        if price_df is None or price_df.empty or len(price_df) < 60:
            return 50.0, {}
        
        close = price_df['Close']
        details = {}
        scores = []
        
        # 20일 수익률
        ret_20 = (close.iloc[-1] / close.iloc[-20] - 1) * 100 if len(close) >= 20 else 0
        s_20 = min(100, max(0, 50 + ret_20 * 3))
        scores.append(s_20)
        details['return_20d'] = {'value': ret_20, 'score': s_20}
        
        # 60일 수익률
        ret_60 = (close.iloc[-1] / close.iloc[-60] - 1) * 100 if len(close) >= 60 else 0
        s_60 = min(100, max(0, 50 + ret_60 * 1.5))
        scores.append(s_60)
        details['return_60d'] = {'value': ret_60, 'score': s_60}
        
        # 52주 신고가 대비
        high_52w = close.rolling(252).max().iloc[-1] if len(close) >= 252 else close.max()
        pct_from_high = (close.iloc[-1] / high_52w - 1) * 100
        s_high = min(100, max(0, 100 + pct_from_high * 2))
        scores.append(s_high)
        details['from_52w_high'] = {'value': pct_from_high, 'score': s_high}
        
        # 이평선 배열 (정배열 보너스)
        ma20 = close.rolling(20).mean().iloc[-1]
        ma60 = close.rolling(60).mean().iloc[-1]
        ma120 = close.rolling(120).mean().iloc[-1] if len(close) >= 120 else ma60
        
        if close.iloc[-1] > ma20 > ma60 > ma120:
            ma_score = 100  # 완전 정배열
        elif close.iloc[-1] > ma20 > ma60:
            ma_score = 80
        elif close.iloc[-1] > ma20:
            ma_score = 60
        else:
            ma_score = 30
        scores.append(ma_score)
        details['ma_alignment'] = {'score': ma_score}
        
        final_score = np.mean(scores)
        return final_score, details
    
    def _score_stability(self, data: Dict) -> Tuple[float, Dict]:
        """재무 안정성 점수"""
        scores = []
        details = {}
        
        # 부채비율 (낮을수록 좋음)
        debt = data.get('debt_ratio')
        if debt is not None:
            c = self.STABILITY_CRITERIA['debt_ratio']
            if debt <= c['low']:
                s = 100
            elif debt <= c['mid']:
                s = 100 - (debt - c['low']) / (c['mid'] - c['low']) * 30
            elif debt <= c['high']:
                s = 70 - (debt - c['mid']) / (c['high'] - c['mid']) * 40
            else:
                s = max(0, 30 - (debt - c['high']) / 100 * 30)
            scores.append(s)
            details['debt_ratio'] = {'value': debt, 'score': s}
        
        # 유동비율 (높을수록 좋음)
        current = data.get('current_ratio')
        if current is not None:
            c = self.STABILITY_CRITERIA['current_ratio']
            if current >= c['high']:
                s = 100
            elif current >= c['mid']:
                s = 70 + (current - c['mid']) / (c['high'] - c['mid']) * 30
            elif current >= c['low']:
                s = 30 + (current - c['low']) / (c['mid'] - c['low']) * 40
            else:
                s = max(0, current / c['low'] * 30)
            scores.append(s)
            details['current_ratio'] = {'value': current, 'score': s}
        
        # 이자보상배율
        icr = data.get('interest_coverage')
        if icr is not None:
            c = self.STABILITY_CRITERIA['interest_coverage']
            if icr >= c['high']:
                s = 100
            elif icr >= c['mid']:
                s = 70 + (icr - c['mid']) / (c['high'] - c['mid']) * 30
            elif icr >= c['low']:
                s = 30 + (icr - c['low']) / (c['mid'] - c['low']) * 40
            else:
                s = max(0, icr / c['low'] * 30)
            scores.append(s)
            details['interest_coverage'] = {'value': icr, 'score': s}
        
        final_score = np.mean(scores) if scores else 50.0
        return final_score, details
    
    def _score_supply(self, data: Dict) -> Tuple[float, Dict]:
        """수급 점수 (외국인/기관)"""
        scores = []
        details = {}
        
        # 외국인 순매수 (최근 20일)
        foreign_net = data.get('foreign_net_20d')
        if foreign_net is not None:
            # 시총 대비 비율로 정규화 가정
            if foreign_net > 0:
                s = min(100, 50 + foreign_net * 10)
            else:
                s = max(0, 50 + foreign_net * 10)
            scores.append(s)
            details['foreign_net'] = {'value': foreign_net, 'score': s}
        
        # 기관 순매수
        inst_net = data.get('institution_net_20d')
        if inst_net is not None:
            if inst_net > 0:
                s = min(100, 50 + inst_net * 10)
            else:
                s = max(0, 50 + inst_net * 10)
            scores.append(s)
            details['institution_net'] = {'value': inst_net, 'score': s}
        
        final_score = np.mean(scores) if scores else 50.0
        return final_score, details

    def _score_forward_value(self, data: Dict) -> Tuple[float, Dict]:
        """미래 투자가치 점수 (추정PER + PEG ratio 기반)"""
        scores = []
        details = {}

        # 추정PER (Forward PER): 미래 이익 기준 밸류에이션
        fwd_per = data.get('forward_per')
        if fwd_per is not None and fwd_per > 0:
            c = self.FORWARD_VALUE_CRITERIA['forward_per']
            if fwd_per <= c['low']:
                s = 100
            elif fwd_per <= c['mid']:
                s = 100 - (fwd_per - c['low']) / (c['mid'] - c['low']) * 50
            elif fwd_per <= c['high']:
                s = 50 - (fwd_per - c['mid']) / (c['high'] - c['mid']) * 35
            else:
                s = 10
            scores.append(s)
            details['forward_per'] = {'value': fwd_per, 'score': round(s, 1)}

        # PEG ratio: 성장 대비 밸류에이션 (낮을수록 저평가 성장주)
        peg = data.get('peg_ratio')
        if peg is not None and peg > 0:
            c = self.FORWARD_VALUE_CRITERIA['peg_ratio']
            if peg <= c['excellent']:      # ≤ 0.5: 매우 저평가
                s = 100
            elif peg <= c['good']:         # ≤ 1.0: 저평가
                s = 100 - (peg - c['excellent']) / (c['good'] - c['excellent']) * 20
            elif peg <= c['fair']:         # ≤ 2.0: 적정
                s = 80 - (peg - c['good']) / (c['fair'] - c['good']) * 40
            else:                          # > 2.0: 고평가 성장주
                s = max(10, 40 - (peg - c['fair']) * 10)
            scores.append(s)
            details['peg_ratio'] = {'value': peg, 'score': round(s, 1)}

        # forward 데이터가 없으면 현재 PER로 fallback (50% 신뢰도)
        if not scores:
            per = data.get('PER')
            if per is not None and per > 0:
                c = self.FORWARD_VALUE_CRITERIA['forward_per']
                if per <= c['low']:
                    s = 80
                elif per <= c['mid']:
                    s = 80 - (per - c['low']) / (c['mid'] - c['low']) * 30
                elif per <= c['high']:
                    s = 50 - (per - c['mid']) / (c['high'] - c['mid']) * 25
                else:
                    s = 10
                scores.append(s * 0.7)  # fallback이므로 30% 할인
                details['forward_per_fallback_from_per'] = {'value': per, 'score': round(s * 0.7, 1)}

        final_score = np.mean(scores) if scores else 50.0
        return final_score, details

    # ========== 종합 분석 ==========

    def analyze(
        self,
        code: str,
        name: str,
        fundamental_data: Dict,
        price_df: Optional[pd.DataFrame] = None,
        regime: str = 'SIDEWAYS'
    ) -> CompanyScore:
        """
        기업 종합 분석
        
        Args:
            code: 종목 코드
            name: 종목명
            fundamental_data: 재무 데이터 딕셔너리
            price_df: 가격 데이터 (OHLCV)
            regime: 현재 장세 ('BULL', 'CORRECTION', 'SIDEWAYS', 'BEAR')
        
        Returns:
            CompanyScore: 종합 분석 결과
        """
        
        # 개별 팩터 점수 계산
        forward_value_score, fwd_details = self._score_forward_value(fundamental_data)
        value_score, value_details = self._score_value(fundamental_data)
        quality_score, quality_details = self._score_quality(fundamental_data)
        growth_score, growth_details = self._score_growth(fundamental_data)
        momentum_score, momentum_details = self._score_momentum(price_df)
        stability_score, stability_details = self._score_stability(fundamental_data)
        supply_score, supply_details = self._score_supply(fundamental_data)

        # 장세별 가중치 적용
        weights = self.REGIME_WEIGHTS.get(regime, self.REGIME_WEIGHTS['SIDEWAYS'])

        total_score = (
            forward_value_score * weights.get('forward_value', 0.0) +
            value_score * weights['value'] +
            quality_score * weights['quality'] +
            growth_score * weights['growth'] +
            momentum_score * weights['momentum'] +
            stability_score * weights['stability'] +
            supply_score * weights['supply']
        )
        
        # 등급 판정
        if total_score >= 85:
            grade = FundamentalGrade.S
        elif total_score >= 70:
            grade = FundamentalGrade.A
        elif total_score >= 55:
            grade = FundamentalGrade.B
        elif total_score >= 40:
            grade = FundamentalGrade.C
        elif total_score >= 25:
            grade = FundamentalGrade.D
        else:
            grade = FundamentalGrade.F
        
        # 추천 생성
        recommendation = self._generate_recommendation(
            grade, regime, value_score, growth_score, momentum_score, stability_score,
            forward_value_score=forward_value_score,
        )
        
        return CompanyScore(
            code=code,
            name=name,
            total_score=total_score,
            grade=grade,
            forward_value_score=forward_value_score,
            value_score=value_score,
            quality_score=quality_score,
            growth_score=growth_score,
            momentum_score=momentum_score,
            stability_score=stability_score,
            supply_score=supply_score,
            details={
                'forward_value': fwd_details,
                'value': value_details,
                'quality': quality_details,
                'growth': growth_details,
                'momentum': momentum_details,
                'stability': stability_details,
                'supply': supply_details,
                'weights': weights,
                'regime': regime,
            },
            recommendation=recommendation
        )
    
    def _generate_recommendation(
        self,
        grade: FundamentalGrade,
        regime: str,
        value_score: float,
        growth_score: float,
        momentum_score: float,
        stability_score: float,
        forward_value_score: float = 50.0,
    ) -> str:
        """투자 추천 생성"""
        
        grade_text = {
            FundamentalGrade.S: "최상급",
            FundamentalGrade.A: "우수",
            FundamentalGrade.B: "양호",
            FundamentalGrade.C: "보통",
            FundamentalGrade.D: "주의",
            FundamentalGrade.F: "위험",
        }
        
        regime_action = {
            'BULL': "적극 매수 고려",
            'CORRECTION': "분할 매수 고려",
            'SIDEWAYS': "관망 또는 소량 매수",
            'BEAR': "매수 보류",
        }
        
        # 강점/약점 분석
        strengths = []
        weaknesses = []
        
        if forward_value_score >= 75:
            strengths.append("미래가치 저평가")
        elif forward_value_score <= 30:
            weaknesses.append("미래가치 고평가")

        if value_score >= 70:
            strengths.append("현재 저평가")
        elif value_score <= 30:
            weaknesses.append("현재 고평가")
        
        if growth_score >= 70:
            strengths.append("고성장")
        elif growth_score <= 30:
            weaknesses.append("성장 둔화")
        
        if momentum_score >= 70:
            strengths.append("강한 모멘텀")
        elif momentum_score <= 30:
            weaknesses.append("약한 모멘텀")
        
        if stability_score >= 70:
            strengths.append("재무 안정")
        elif stability_score <= 30:
            weaknesses.append("재무 불안")
        
        # 추천문 생성
        parts = [f"[{grade_text[grade]}]"]
        
        if strengths:
            parts.append(f"강점: {', '.join(strengths)}")
        if weaknesses:
            parts.append(f"약점: {', '.join(weaknesses)}")
        
        parts.append(f"현재 장세({regime}): {regime_action.get(regime, '관망')}")
        
        return " | ".join(parts)
    
    def rank_companies(
        self,
        companies: List[CompanyScore],
        top_n: int = 10
    ) -> List[CompanyScore]:
        """기업 순위 정렬"""
        sorted_companies = sorted(companies, key=lambda x: x.total_score, reverse=True)
        return sorted_companies[:top_n]


# ============================================================================
# 장세 + 기업 분석 통합 시스템
# ============================================================================

class IntegratedAnalysisSystem:
    """
    장세 판별 + 기업 분석 통합 시스템
    
    사용법:
        system = IntegratedAnalysisSystem()
        
        # 1. 장세 판별 (시장 지수 데이터)
        regime = system.get_market_regime(kospi_df)
        
        # 2. 기업 분석 (장세 연동)
        score = system.analyze_company(code, name, fund_data, price_df)
        
        # 3. 포트폴리오 추천
        recommendations = system.get_recommendations(company_list)
    """
    
    def __init__(self, regime_classifier=None, company_analyzer=None):
        # 장세 판별기 (외부 주입 또는 기본값)
        self.regime_classifier = regime_classifier
        self.company_analyzer = company_analyzer or CompanyAnalyzer()
        self.current_regime = 'SIDEWAYS'
        self.current_regime_label = '횡보장'
    
    def set_regime_classifier(self, classifier):
        """장세 판별기 설정 (기존 MarketRegimeClassifier 연결)"""
        self.regime_classifier = classifier
    
    def update_market_regime(self, market_df: pd.DataFrame) -> str:
        """
        시장 장세 업데이트
        
        Args:
            market_df: 시장 지수 OHLCV 데이터
        
        Returns:
            regime: 'BULL', 'CORRECTION', 'SIDEWAYS', 'BEAR'
        """
        if self.regime_classifier is None:
            # 간단한 내장 로직 사용
            self.current_regime = self._simple_regime_detect(market_df)
        else:
            # 외부 분류기 사용
            result = self.regime_classifier.get_current_regime(market_df)
            regime_map = {0: 'BEAR', 1: 'SIDEWAYS', 2: 'CORRECTION', 3: 'BULL'}
            self.current_regime = regime_map.get(result.regime, 'SIDEWAYS')
            self.current_regime_label = result.label
        
        return self.current_regime
    
    def _simple_regime_detect(self, df: pd.DataFrame) -> str:
        """간단한 장세 판별 (분류기 없을 때)"""
        if len(df) < 200:
            return 'SIDEWAYS'
        
        close = df['Close']
        ma200 = close.rolling(200).mean().iloc[-1]
        high_60 = close.rolling(60).max().iloc[-1]
        drawdown = (close.iloc[-1] / high_60 - 1) * 100
        
        above_ma = close.iloc[-1] > ma200
        
        if drawdown <= -20 or (not above_ma and drawdown <= -15):
            return 'BEAR'
        elif above_ma and drawdown > -5:
            return 'BULL'
        elif above_ma and -15 <= drawdown <= -5:
            return 'CORRECTION'
        else:
            return 'SIDEWAYS'
    
    def analyze_company(
        self,
        code: str,
        name: str,
        fundamental_data: Dict,
        price_df: Optional[pd.DataFrame] = None
    ) -> CompanyScore:
        """
        기업 분석 (현재 장세 자동 연동)
        """
        return self.company_analyzer.analyze(
            code=code,
            name=name,
            fundamental_data=fundamental_data,
            price_df=price_df,
            regime=self.current_regime
        )
    
    def analyze_batch(
        self,
        companies: List[Dict]
    ) -> List[CompanyScore]:
        """
        다수 기업 일괄 분석
        
        Args:
            companies: [{'code': '005930', 'name': '삼성전자', 
                         'fundamental': {...}, 'price_df': df}, ...]
        """
        results = []
        for company in companies:
            score = self.analyze_company(
                code=company['code'],
                name=company['name'],
                fundamental_data=company.get('fundamental', {}),
                price_df=company.get('price_df')
            )
            results.append(score)
        
        return results
    
    def get_recommendations(
        self,
        scores: List[CompanyScore],
        top_n: int = 10
    ) -> pd.DataFrame:
        """추천 종목 DataFrame 반환"""
        
        ranked = self.company_analyzer.rank_companies(scores, top_n)
        
        data = []
        for r in ranked:
            data.append({
                '코드': r.code,
                '종목명': r.name,
                '종합점수': f"{r.total_score:.1f}",
                '등급': r.grade.name,
                '밸류': f"{r.value_score:.0f}",
                '퀄리티': f"{r.quality_score:.0f}",
                '성장': f"{r.growth_score:.0f}",
                '모멘텀': f"{r.momentum_score:.0f}",
                '안정성': f"{r.stability_score:.0f}",
                '수급': f"{r.supply_score:.0f}",
                '추천': r.recommendation,
            })
        
        return pd.DataFrame(data)
    
    def get_position_size(self, grade: FundamentalGrade) -> float:
        """
        등급 + 장세 기반 포지션 비중 제안
        
        Returns:
            position_ratio: 0.0 ~ 1.0
        """
        # 등급별 기본 비중
        grade_base = {
            FundamentalGrade.S: 1.0,
            FundamentalGrade.A: 0.8,
            FundamentalGrade.B: 0.6,
            FundamentalGrade.C: 0.4,
            FundamentalGrade.D: 0.2,
            FundamentalGrade.F: 0.0,
        }
        
        # 장세별 조정 계수
        regime_multiplier = {
            'BULL': 1.0,
            'CORRECTION': 0.7,
            'SIDEWAYS': 0.5,
            'BEAR': 0.2,
        }
        
        base = grade_base.get(grade, 0.5)
        mult = regime_multiplier.get(self.current_regime, 0.5)
        
        return base * mult


# ============================================================================
# 테스트
# ============================================================================

def run_test():
    """테스트 실행"""
    print("=" * 80)
    print("기업 분석 시스템 테스트 (장세 연동)")
    print("=" * 80)
    
    # 시스템 초기화
    system = IntegratedAnalysisSystem()
    
    # 샘플 시장 데이터 생성
    np.random.seed(42)
    n_days = 300
    prices = np.cumprod(1 + np.random.randn(n_days) * 0.01 + 0.0003) * 2500
    
    market_df = pd.DataFrame({
        'Open': prices,
        'High': prices * 1.005,
        'Low': prices * 0.995,
        'Close': prices,
        'Volume': np.random.randint(1e8, 1e9, n_days)
    })
    
    # 장세 판별
    regime = system.update_market_regime(market_df)
    print(f"\n[현재 장세] {regime}")
    
    # 샘플 기업 데이터
    sample_companies = [
        {
            'code': '005930',
            'name': '삼성전자',
            'fundamental': {
                'PER': 12, 'PBR': 1.2, 'PSR': 1.5,
                'ROE': 15, 'ROA': 10, 'OPM': 18,
                'revenue_growth': 8, 'op_growth': 12, 'np_growth': 10,
                'debt_ratio': 40, 'current_ratio': 200, 'interest_coverage': 50,
                'foreign_net_20d': 2, 'institution_net_20d': 1,
            },
        },
        {
            'code': '000660',
            'name': 'SK하이닉스',
            'fundamental': {
                'PER': 8, 'PBR': 1.8, 'PSR': 2.0,
                'ROE': 22, 'ROA': 12, 'OPM': 25,
                'revenue_growth': 25, 'op_growth': 40, 'np_growth': 45,
                'debt_ratio': 60, 'current_ratio': 150, 'interest_coverage': 30,
                'foreign_net_20d': 3, 'institution_net_20d': 2,
            },
        },
        {
            'code': '035420',
            'name': 'NAVER',
            'fundamental': {
                'PER': 25, 'PBR': 2.5, 'PSR': 4.0,
                'ROE': 12, 'ROA': 6, 'OPM': 15,
                'revenue_growth': 15, 'op_growth': 10, 'np_growth': 8,
                'debt_ratio': 30, 'current_ratio': 250, 'interest_coverage': 100,
                'foreign_net_20d': -1, 'institution_net_20d': 0,
            },
        },
        {
            'code': '068270',
            'name': '셀트리온',
            'fundamental': {
                'PER': 35, 'PBR': 4.0, 'PSR': 8.0,
                'ROE': 8, 'ROA': 4, 'OPM': 20,
                'revenue_growth': 5, 'op_growth': -5, 'np_growth': -10,
                'debt_ratio': 80, 'current_ratio': 120, 'interest_coverage': 15,
                'foreign_net_20d': -2, 'institution_net_20d': -1,
            },
        },
    ]
    
    # 분석 실행
    scores = system.analyze_batch(sample_companies)
    
    # 결과 출력
    print(f"\n[기업 분석 결과] (장세: {regime})")
    print("-" * 80)
    
    for score in scores:
        print(f"\n{score.name} ({score.code})")
        print(f"  종합: {score.total_score:.1f}점 [{score.grade.name}등급]")
        print(f"  세부: 밸류={score.value_score:.0f}, 퀄리티={score.quality_score:.0f}, "
              f"성장={score.growth_score:.0f}, 모멘텀={score.momentum_score:.0f}, "
              f"안정={score.stability_score:.0f}, 수급={score.supply_score:.0f}")
        print(f"  추천: {score.recommendation}")
        print(f"  포지션 비중: {system.get_position_size(score.grade):.0%}")
    
    # 추천 테이블
    print(f"\n[추천 종목 순위]")
    print("-" * 80)
    recommendations = system.get_recommendations(scores, top_n=10)
    print(recommendations.to_string(index=False))
    
    print("\n" + "=" * 80)
    print("✅ 기업 분석 시스템 테스트 완료")
    print("=" * 80)


if __name__ == "__main__":
    run_test()


# ============================================================================
# 기존 장세 판별기와 연동 예시
# ============================================================================

def integration_example():
    """
    기존 market_regime_4state.py와 연동 예시
    """
    print("\n" + "=" * 80)
    print("기존 장세 판별기 연동 예시")
    print("=" * 80)
    
    # 기존 장세 판별기 import
    try:
        from market_regime_4state import MarketRegimeClassifier
        print("✅ market_regime_4state.py 로드 성공")
        has_regime_classifier = True
    except ImportError:
        print("⚠ market_regime_4state.py 없음 - 내장 로직 사용")
        has_regime_classifier = False
    
    # 샘플 시장 데이터
    np.random.seed(123)
    n_days = 300
    prices = np.cumprod(1 + np.random.randn(n_days) * 0.012) * 2500
    
    market_df = pd.DataFrame({
        'Open': prices * 0.998,
        'High': prices * 1.008,
        'Low': prices * 0.992,
        'Close': prices,
        'Volume': np.random.randint(1e8, 1e9, n_days)
    })
    
    # 통합 시스템 초기화
    if has_regime_classifier:
        regime_clf = MarketRegimeClassifier()
        system = IntegratedAnalysisSystem(regime_classifier=regime_clf)
    else:
        system = IntegratedAnalysisSystem()
    
    # 장세 업데이트
    regime = system.update_market_regime(market_df)
    print(f"\n[현재 장세] {regime}")
    
    # 샘플 기업 분석
    company = {
        'code': '005930',
        'name': '삼성전자',
        'fundamental': {
            'PER': 10, 'PBR': 1.0, 'ROE': 18, 'ROA': 12, 'OPM': 20,
            'revenue_growth': 12, 'op_growth': 15,
            'debt_ratio': 35, 'current_ratio': 220,
            'foreign_net_20d': 3,
        },
    }
    
    score = system.analyze_company(
        code=company['code'],
        name=company['name'],
        fundamental_data=company['fundamental']
    )
    
    print(f"\n[분석 결과]")
    print(f"  종목: {score.name}")
    print(f"  등급: {score.grade.name} ({score.total_score:.1f}점)")
    print(f"  추천: {score.recommendation}")
    print(f"  포지션 비중: {system.get_position_size(score.grade):.0%}")
    
    print("\n" + "=" * 80)
    print("""
[사용법 요약]

1. 기존 장세 판별기 연결:
   from market_regime_4state import MarketRegimeClassifier
   from company_analyzer import IntegratedAnalysisSystem
   
   regime_clf = MarketRegimeClassifier()
   system = IntegratedAnalysisSystem(regime_classifier=regime_clf)

2. 장세 업데이트:
   regime = system.update_market_regime(kospi_df)  # 'BULL', 'CORRECTION', ...

3. 기업 분석:
   score = system.analyze_company(
       code='005930',
       name='삼성전자',
       fundamental_data={...},
       price_df=stock_df  # 선택사항
   )

4. 결과 활용:
   print(score.grade)           # FundamentalGrade.A
   print(score.total_score)     # 75.3
   print(score.recommendation)  # "[우수] 강점: 고성장..."
   position = system.get_position_size(score.grade)  # 0.8 (80%)
""")
    print("=" * 80)


if __name__ == "__main__":
    run_test()
    integration_example()


# ============================================================================
# 위험 필터 통합
# ============================================================================

# 위험 필터 import (같은 디렉토리에 있다고 가정)
try:
    from risk_stock_filter import RiskStockFilter, RiskAssessment, RiskLevel
    HAS_RISK_FILTER = True
except ImportError:
    HAS_RISK_FILTER = False


class FullAnalysisSystem(IntegratedAnalysisSystem):
    """
    완전한 분석 시스템 (장세 + 기업분석 + 위험필터)
    
    사용법:
        system = FullAnalysisSystem()
        
        # 장세 판별
        regime = system.update_market_regime(kospi_df)
        
        # 종합 분석 (기업분석 + 위험필터)
        result = system.full_analyze(
            code='005930',
            name='삼성전자',
            price=70000,
            market_cap=400e12,
            fundamental_data={...},
            price_df=stock_df
        )
        
        # 결과
        print(result['score'])       # CompanyScore
        print(result['risk'])        # RiskAssessment
        print(result['investable'])  # True/False
        print(result['position'])    # 0.0 ~ 1.0
    """
    
    def __init__(self, regime_classifier=None, company_analyzer=None, risk_filter=None):
        super().__init__(regime_classifier, company_analyzer)
        
        if HAS_RISK_FILTER:
            self.risk_filter = risk_filter or RiskStockFilter()
        else:
            self.risk_filter = None
            print("⚠️ risk_stock_filter.py 없음 - 위험 필터 비활성화")
    
    def set_warning_lists(self, administrative=None, warning=None, delisting=None):
        """관리종목/투자위험 종목 리스트 설정"""
        if self.risk_filter:
            self.risk_filter.set_warning_lists(administrative, warning, delisting)
    
    def full_analyze(
        self,
        code: str,
        name: str,
        price: float = None,
        market_cap: float = None,
        fundamental_data: Dict = None,
        price_df: Optional[pd.DataFrame] = None,
        sector: str = None,
        news_keywords: List[str] = None
    ) -> Dict[str, Any]:
        """
        종합 분석 (기업분석 + 위험필터)
        
        Returns:
            {
                'score': CompanyScore,
                'risk': RiskAssessment,
                'investable': bool,
                'position': float,
                'final_grade': str,
                'summary': str
            }
        """
        fundamental_data = fundamental_data or {}
        
        # 1. 기업 분석
        score = self.analyze_company(
            code=code,
            name=name,
            fundamental_data=fundamental_data,
            price_df=price_df
        )
        
        # 2. 위험 평가
        if self.risk_filter:
            risk = self.risk_filter.assess(
                code=code,
                name=name,
                price=price,
                market_cap=market_cap,
                fundamental_data=fundamental_data,
                price_df=price_df,
                sector=sector,
                news_keywords=news_keywords
            )
        else:
            # 위험 필터 없으면 기본값
            risk = None
        
        # 3. 투자 가능 여부
        if risk:
            investable = risk.is_investable
        else:
            investable = True
        
        # 4. 최종 포지션 비중
        base_position = self.get_position_size(score.grade)
        
        if risk:
            # 위험 등급에 따른 조정
            risk_multiplier = {
                0: 1.0,   # SAFE
                1: 0.8,   # CAUTION
                2: 0.3,   # WARNING
                3: 0.0,   # DANGER
                4: 0.0,   # CRITICAL
            }.get(risk.risk_level, 0.5)
            
            final_position = base_position * risk_multiplier
        else:
            final_position = base_position
        
        # 5. 최종 등급
        if not investable:
            final_grade = "⛔ 투자금지"
        elif final_position >= 0.6:
            final_grade = "🟢 적극매수"
        elif final_position >= 0.4:
            final_grade = "🟡 매수고려"
        elif final_position >= 0.2:
            final_grade = "🟠 주의매수"
        else:
            final_grade = "🔴 관망"
        
        # 6. 요약
        summary_parts = [
            f"{name}({code})",
            f"기업등급: {score.grade.name}({score.total_score:.0f}점)",
        ]
        
        if risk:
            summary_parts.append(f"위험등급: {risk.risk_level.name}({risk.risk_score:.0f}점)")
        
        summary_parts.extend([
            f"장세: {self.current_regime}",
            f"최종: {final_grade}",
            f"비중: {final_position:.0%}"
        ])
        
        summary = " | ".join(summary_parts)
        
        return {
            'code': code,
            'name': name,
            'score': score,
            'risk': risk,
            'investable': investable,
            'position': final_position,
            'final_grade': final_grade,
            'summary': summary,
            'regime': self.current_regime,
        }
    
    def full_analyze_batch(
        self,
        companies: List[Dict]
    ) -> List[Dict]:
        """
        다수 기업 일괄 종합 분석
        
        Args:
            companies: [{
                'code': '005930',
                'name': '삼성전자',
                'price': 70000,
                'market_cap': 400e12,
                'fundamental': {...},
                'price_df': df,
                'sector': '반도체',
                'news_keywords': [...]
            }, ...]
        """
        results = []
        for c in companies:
            result = self.full_analyze(
                code=c['code'],
                name=c['name'],
                price=c.get('price'),
                market_cap=c.get('market_cap'),
                fundamental_data=c.get('fundamental', {}),
                price_df=c.get('price_df'),
                sector=c.get('sector'),
                news_keywords=c.get('news_keywords')
            )
            results.append(result)
        
        return results
    
    def get_full_recommendations(
        self,
        results: List[Dict],
        investable_only: bool = True,
        top_n: int = 10
    ) -> pd.DataFrame:
        """종합 추천 테이블"""
        
        if investable_only:
            results = [r for r in results if r['investable']]
        
        # 점수 기준 정렬
        results = sorted(results, key=lambda x: x['score'].total_score, reverse=True)[:top_n]
        
        data = []
        for r in results:
            score = r['score']
            risk = r['risk']
            
            data.append({
                '코드': r['code'],
                '종목명': r['name'],
                '기업등급': score.grade.name,
                '기업점수': f"{score.total_score:.0f}",
                '위험등급': risk.risk_level.name if risk else '-',
                '위험점수': f"{risk.risk_score:.0f}" if risk else '-',
                '최종판정': r['final_grade'],
                '비중': f"{r['position']:.0%}",
                '장세': r['regime'],
            })
        
        return pd.DataFrame(data)


# ============================================================================
# 통합 테스트
# ============================================================================

def run_full_system_test():
    """전체 시스템 통합 테스트"""
    
    print("\n" + "=" * 80)
    print("전체 분석 시스템 통합 테스트 (장세 + 기업분석 + 위험필터)")
    print("=" * 80)
    
    # 시스템 초기화
    try:
        from market_regime_4state import MarketRegimeClassifier
        regime_clf = MarketRegimeClassifier()
        print("✅ 장세 판별기 로드")
    except ImportError:
        regime_clf = None
        print("⚠️ 장세 판별기 없음")
    
    system = FullAnalysisSystem(regime_classifier=regime_clf)
    
    # 관리종목 설정
    system.set_warning_lists(
        administrative=['999999'],
        warning=['888888']
    )
    
    # 시장 데이터 생성
    np.random.seed(42)
    n_days = 300
    prices = np.cumprod(1 + np.random.randn(n_days) * 0.01 + 0.0003) * 2500
    
    market_df = pd.DataFrame({
        'Open': prices * 0.998,
        'High': prices * 1.008,
        'Low': prices * 0.992,
        'Close': prices,
        'Volume': np.random.randint(1e8, 1e9, n_days)
    })
    
    # 장세 업데이트
    regime = system.update_market_regime(market_df)
    print(f"\n[현재 장세] {regime}")
    
    # 테스트 기업들
    test_companies = [
        {
            'code': '005930', 'name': '삼성전자',
            'price': 70000, 'market_cap': 400e12,
            'fundamental': {
                'PER': 12, 'PBR': 1.2, 'ROE': 15, 'ROA': 10, 'OPM': 18,
                'revenue_growth': 8, 'op_growth': 12,
                'debt_ratio': 40, 'current_ratio': 200,
                'foreign_net_20d': 2,
            },
            'sector': '반도체',
        },
        {
            'code': '000660', 'name': 'SK하이닉스',
            'price': 180000, 'market_cap': 130e12,
            'fundamental': {
                'PER': 8, 'PBR': 1.8, 'ROE': 22, 'ROA': 12, 'OPM': 25,
                'revenue_growth': 25, 'op_growth': 40,
                'debt_ratio': 60, 'current_ratio': 150,
                'foreign_net_20d': 3,
            },
            'sector': '반도체',
        },
        {
            'code': '123456', 'name': '정치테마주',
            'price': 800, 'market_cap': 200e8,
            'fundamental': {
                'PER': 50, 'PBR': 5, 'ROE': 3,
                'debt_ratio': 200, 'current_ratio': 80,
            },
            'sector': '대선관련',
        },
        {
            'code': '234567', 'name': 'AI신기술주',
            'price': 30000, 'market_cap': 3000e8,
            'fundamental': {
                'PER': 80, 'PBR': 8, 'ROE': 5,
                'revenue_growth': 50, 'op_growth': 100,
                'debt_ratio': 100, 'current_ratio': 150,
            },
            'news_keywords': ['AI', '인공지능', '로봇'],
        },
        {
            'code': '999999', 'name': '관리종목회사',
            'price': 1500, 'market_cap': 300e8,
            'fundamental': {
                'PER': -10, 'debt_ratio': 500, 'current_ratio': 30,
                'consecutive_loss_years': 4, 'audit_opinion': '의견거절',
            },
        },
    ]
    
    # 일괄 분석
    results = system.full_analyze_batch(test_companies)
    
    # 개별 결과 출력
    print(f"\n[개별 분석 결과]")
    print("-" * 80)
    
    for r in results:
        print(f"\n{r['summary']}")
        
        if not r['investable']:
            print(f"  ⛔ 위험 요인: {', '.join(r['risk'].flags[:3])}")
        else:
            score = r['score']
            print(f"  강점: 밸류={score.value_score:.0f}, 퀄리티={score.quality_score:.0f}, "
                  f"성장={score.growth_score:.0f}, 모멘텀={score.momentum_score:.0f}")
    
    # 추천 테이블
    print(f"\n{'=' * 80}")
    print("[종합 추천 (투자 가능 종목)]")
    print("-" * 80)
    
    recommendations = system.get_full_recommendations(results, investable_only=True)
    print(recommendations.to_string(index=False))
    
    # 통계
    investable_count = sum(1 for r in results if r['investable'])
    print(f"\n[통계]")
    print(f"  전체 종목: {len(results)}개")
    print(f"  투자 가능: {investable_count}개")
    print(f"  투자 불가: {len(results) - investable_count}개")
    
    print(f"\n{'=' * 80}")
    print("✅ 전체 시스템 통합 테스트 완료")
    print("=" * 80)
    
    return system, results


if __name__ == "__main__":
    run_test()
    integration_example()
    run_full_system_test()
