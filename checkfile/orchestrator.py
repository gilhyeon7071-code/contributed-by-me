"""Main orchestrator and report generation for the verification framework."""

from __future__ import annotations

import html
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from .base import (
    BaseVerifier,
    PhaseReport,
    TradingSystemConfig,
    VerificationPhase,
    VerificationStatus,
)
from .phase_1_2_verifiers import DesignVerifier, PlanningVerifier
from .phase_3_4_verifiers import DataVerifier, StrategyVerifier
from .phase_5_6_verifiers import ExecutionVerifier, RiskVerifier
from .phase_7_8_verifiers import OperationsVerifier, TestingVerifier
from .stock_terms import load_stock_terms


logger = logging.getLogger(__name__)


@dataclass
class FullVerificationReport:
    system_config: TradingSystemConfig
    phase_reports: Dict[VerificationPhase, PhaseReport] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    overall_status: VerificationStatus = VerificationStatus.PASSED
    runtime_meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def total_passed(self) -> int:
        return sum(r.passed_count for r in self.phase_reports.values())

    @property
    def total_failed(self) -> int:
        return sum(r.failed_count for r in self.phase_reports.values())

    @property
    def total_warnings(self) -> int:
        return sum(r.warning_count for r in self.phase_reports.values())

    @property
    def total_skipped(self) -> int:
        return sum(r.skipped_count for r in self.phase_reports.values())

    @property
    def total_tests(self) -> int:
        return sum(r.total_count for r in self.phase_reports.values())

    @property
    def overall_pass_rate(self) -> float:
        if self.total_tests == 0:
            return 0.0
        return (self.total_passed / self.total_tests) * 100

    @property
    def is_deployment_ready(self) -> bool:
        return self.total_failed == 0 and self.total_skipped == 0

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "system_config": self.system_config.to_dict(),
            "summary": {
                "overall_status": self.overall_status.value,
                "total_tests": self.total_tests,
                "passed": self.total_passed,
                "failed": self.total_failed,
                "warnings": self.total_warnings,
                "skipped": self.total_skipped,
                "pass_rate": f"{self.overall_pass_rate:.1f}%",
                "is_deployment_ready": self.is_deployment_ready,
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat() if self.end_time else None,
            },
            "phases": {
                phase.name: {
                    "phase_name": phase.value,
                    "passed": report.passed_count,
                    "failed": report.failed_count,
                    "warnings": report.warning_count,
                    "skipped": report.skipped_count,
                    "total": report.total_count,
                    "pass_rate": f"{report.pass_rate:.1f}%",
                    "is_phase_passed": report.is_phase_passed,
                    "results": [r.to_dict() for r in report.results],
                }
                for phase, report in self.phase_reports.items()
            },
        }

        if isinstance(self.runtime_meta, dict) and self.runtime_meta:
            payload["runtime_meta"] = self.runtime_meta
            refs = self.runtime_meta.get("regulatory_references")
            if isinstance(refs, dict) and refs:
                payload["regulatory_references"] = refs

        return payload

class TradingSystemVerifier:
    def __init__(
        self,
        config: TradingSystemConfig,
        evidence_by_phase: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
    ):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.evidence_by_phase = evidence_by_phase or {}
        self.runtime_meta = {}
        if isinstance(self.evidence_by_phase.get("_meta"), dict):
            self.runtime_meta = dict(self.evidence_by_phase.get("_meta", {}))
        self.verifiers: Dict[VerificationPhase, BaseVerifier] = {
            VerificationPhase.PLANNING: PlanningVerifier(config),
            VerificationPhase.DESIGN: DesignVerifier(config),
            VerificationPhase.DATA: DataVerifier(config),
            VerificationPhase.STRATEGY: StrategyVerifier(config),
            VerificationPhase.EXECUTION: ExecutionVerifier(config),
            VerificationPhase.RISK: RiskVerifier(config),
            VerificationPhase.TESTING: TestingVerifier(config),
            VerificationPhase.OPERATIONS: OperationsVerifier(config),
        }

        for phase, verifier in self.verifiers.items():
            verifier.set_evidence(self.evidence_by_phase.get(phase.name, {}))

    def run_full_verification(self, phases: Optional[List[VerificationPhase]] = None) -> FullVerificationReport:
        report = FullVerificationReport(system_config=self.config, runtime_meta=self.runtime_meta)
        phases_to_run = phases or list(VerificationPhase)

        for phase in phases_to_run:
            verifier = self.verifiers.get(phase)
            if verifier is None:
                continue
            phase_report = verifier.verify_all()
            report.phase_reports[phase] = phase_report
            self._print_phase_summary(phase_report)

        report.end_time = datetime.now()
        if report.total_failed > 0:
            report.overall_status = VerificationStatus.FAILED
        elif report.total_skipped > 0 or report.total_warnings > 0:
            report.overall_status = VerificationStatus.WARNING
        else:
            report.overall_status = VerificationStatus.PASSED

        self._print_final_summary(report)
        return report

    def run_phase_verification(self, phase: VerificationPhase) -> PhaseReport:
        verifier = self.verifiers.get(phase)
        if verifier is None:
            raise ValueError(f"Unknown phase: {phase}")
        return verifier.verify_all()

    def _print_phase_summary(self, report: PhaseReport) -> None:
        icon = "OK" if report.is_phase_passed else "WARN"
        self.logger.info(
            "[%s] %s | pass=%s fail=%s warn=%s skip=%s",
            icon,
            report.phase.name,
            report.passed_count,
            report.failed_count,
            report.warning_count,
            report.skipped_count,
        )

    def _print_final_summary(self, report: FullVerificationReport) -> None:
        self.logger.info(
            "summary | total=%s pass=%s fail=%s warn=%s skip=%s ready=%s",
            report.total_tests,
            report.total_passed,
            report.total_failed,
            report.total_warnings,
            report.total_skipped,
            report.is_deployment_ready,
        )


def generate_html_report(report: FullVerificationReport) -> str:
    def esc(value: Any) -> str:
        if value is None:
            return "-"
        return html.escape(str(value))

    status_label_map = {
        "PASSED": "통과",
        "FAILED": "실패",
        "WARNING": "경고",
        "SKIPPED": "건너뜀",
        "ERROR": "오류",
        "N/A": "해당없음",
    }
    status_chip_map = {
        "PASSED": "chip-pass",
        "FAILED": "chip-fail",
        "WARNING": "chip-warn",
        "SKIPPED": "chip-skip",
        "ERROR": "chip-fail",
        "N/A": "chip-na",
    }

    phase_title_map = {
        VerificationPhase.PLANNING: "1단계: 요구사항 분석 및 타당성 검토",
        VerificationPhase.DESIGN: "2단계: 시스템 아키텍처 설계",
        VerificationPhase.DATA: "3단계: 데이터 엔지니어링 및 전처리",
        VerificationPhase.STRATEGY: "4단계: 퀀트 전략 구현 및 백테스팅",
        VerificationPhase.EXECUTION: "5단계: 주문 집행 시스템(OEMS) 구현",
        VerificationPhase.RISK: "6단계: 리스크 관리 시스템 구축",
        VerificationPhase.TESTING: "7단계: 테스트 및 모의 투자",
        VerificationPhase.OPERATIONS: "8단계: 배포, 운영 및 유지보수",
    }

    item_name_map = {
        "OS compatibility": "운영체제 호환성",
        "Cost structure": "비용 구조 분석",
        "Architecture suitability": "아키텍처 적합성",
        "API rate-limit compliance": "API 제약사항",
        "Market calendar alignment": "시장 캘린더 정합성",
        "Process isolation": "프로세스 격리",
        "Message queue performance": "메시지 큐 성능",
        "DB schema efficiency": "DB 스키마 효율",
        "Module separation": "모듈 분리 설계",
        "Exactly-once idempotency": "정확히 한 번 처리(Idempotency)",
        "Draft check": "초안 논리 점검",
        "Flowcharting": "흐름도 점검",
        "Dry run": "드라이런 검증",
        "Edge case test": "엣지 케이스 테스트",
        "Cross-check": "교차 대사 검증",
        "Stress test": "스트레스 테스트",
        "Survivorship bias": "생존 편향 검사",
        "Look-ahead bias": "전방 참조 편향",
        "Point-in-time snapshot": "시점고정(PIT) 스냅샷",
        "Data integrity": "데이터 무결성",
        "Adjusted price handling": "수정 주가 처리",
        "Overfitting": "과최적화",
        "Walk-forward regime robustness": "워크포워드/국면 강건성",
        "Randomness test": "무작위성 테스트",
        "Max drawdown": "최대 낙폭(MDD)",
        "Slippage modeling": "슬리피지 모델링",
        "Liquidity constraints": "유동성 제약",
        "Partial-fill handling": "부분 체결 처리",
        "Order rejection handling": "주문 거부 처리",
        "Network disconnection handling": "네트워크 단절",
        "Order state machine": "주문 상태 머신",
        "Event sequence integrity": "이벤트 시퀀스 무결성",
        "API error-code handling": "API 에러 코드 처리",
        "Order limits": "주문 한도 테스트",
        "Portfolio exposure limits": "포트폴리오 노출 한도",
        "Kill switch": "킬 스위치",
        "Duplicate-order prevention": "중복 주문 방지",
        "Price deviation guard": "가격 괴리율 검증",
        "Cost optimization": "비용 최적화",
        "API connection stability": "API 연결 유지",
        "Execution consistency": "체결 일치성",
        "Broker statement reconciliation": "브로커 명세서 대사",
        "Tax/fee calculation": "세금/수수료 계산",
        "Paper-trading awareness": "모의 투자 한계 인식",
        "Canary readiness": "카나리아 배포 준비",
        "Realtime websocket pipeline": "실시간 웹소켓 파이프라인",
        "Emergency full liquidation": "긴급 전체 매도 준비",
        "Alert channel delivery": "알림 채널 전송 검증",
        "Soak test automation": "장시간 Soak 테스트 자동화",
        "Auto reconnection": "자동 재접속",
        "Log integrity": "로그 무결성",
        "Data backup": "데이터 백업",
        "Backup drill runbook": "백업 드릴/런북",
        "Operations scheduler": "운영 스케줄러",
        "Monitoring alerts": "모니터링 및 알림",
    }

    message_map = {
        "Compatible runtime": "호환 가능한 실행 환경",
        "Viable but not optimal": "사용 가능하나 최적은 아님",
        "Session calendar controls validated": "세션 캘린더 통제 검증 완료",
        "No look-ahead detected": "전방 참조 없음",
        "Point-in-time snapshot validated": "PIT 스냅샷 검증 완료",
        "OHLCV integrity clean": "OHLCV 무결성 정상",
        "Adjustment handling complete": "수정주가 처리 완료",
        "Onboarding mode: overfitting baseline/stat gates bypassed": "온보딩 모드: 과최적화 기준/통계 게이트 우회",
        "Onboarding mode: risk-adjusted gate bypassed": "온보딩 모드: 리스크 보정 게이트 우회",
        "Onboarding mode: performance gate bypassed": "온보딩 모드: 성능 게이트 우회",
        "Onboarding mode: dry-run gate bypassed": "온보딩 모드: 드라이런 게이트 우회",
        "Statistically significant edge": "통계적으로 유의한 우위",
        "Drawdown within limit": "낙폭 한도 이내",
        "Slippage assumption acceptable": "슬리피지 가정 적정",
        "Liquidity constraint respected": "유동성 제약 준수",
        "Partial fill handling correct": "부분 체결 처리 정상",
        "Rejection handling complete": "주문 거부 처리 완료",
        "Outage handling acceptable": "단절 대응 정상",
        "State transitions valid": "상태 전이 정상",
        "Event sequence integrity validated": "이벤트 시퀀스 무결성 검증 완료",
        "Error handling complete": "에러 처리 완료",
        "Order-limit handling correct": "주문 한도 처리 정상",
        "Exposure limits validated": "포트폴리오 노출 한도 검증 완료",
        "Kill-switch behavior correct": "킬 스위치 동작 정상",
        "Duplicate-order protection active": "중복 주문 방지 정상",
        "Price-deviation guard correct": "가격 괴리율 방어 정상",
        "Connection recovery validated": "연결 복구 검증 완료",
        "Execution records fully consistent": "체결 기록 일치",
        "Broker statement reconciliation validated": "브로커 명세서 대사 검증 완료",
        "Cost calculation accurate": "세금/수수료 계산 정확",
        "Awareness checklist complete": "한계 인식 체크리스트 완료",
        "Canary checklist complete": "카나리아 체크리스트 완료",
        "Realtime websocket stream healthy": "실시간 웹소켓 스트림 정상",
        "Emergency liquidation readiness validated": "긴급 전체매도 준비 검증 완료",
        "Alert channel delivery validated": "알림 채널 전송 검증 완료",
        "Soak test automation validated": "Soak 테스트 자동화 검증 완료",
        "Auto reconnection acceptable": "자동 재접속 정상",
        "Log integrity checks passed": "로그 무결성 검사 통과",
        "Backup and recovery acceptable": "백업/복구 체계 정상",
        "Backup drill/runbook validated": "백업 드릴/런북 검증 완료",
        "Scheduler configuration complete": "스케줄러 구성 완료",
        "Monitoring alerts configured": "모니터링 알림 구성 완료",
        "Flowchart integrity validated": "흐름도 기반 검증 완료",
        "Draft logic validated": "초안 논리 검증 완료",
        "Cross-check reconciliation validated": "교차 대사 검증 완료",
        "Edge-case robustness validated": "엣지 케이스 대응 확인",
        "Stress-test performance validated": "스트레스 테스트 성능 확인",
        "Idempotency controls validated": "Idempotency 통제 검증 완료",
        "Coverage policy satisfied; missing_critical=-; missing_normal=-; failed_periods=-": "커버리지 정책 충족; 누락 핵심=-; 누락 일반=-; 실패 구간=-",
    }

    expected_map = {
        "Windows 10+ (64-bit)": "Windows 10+ (64비트)",
        "trading_day=True and controls all true": "거래일여부=예 및 통제항목 모두 예",
        "chronology_ok=True, asof_match=True, universe_fixed=True": "시계열정합=예, asof일치=예, 유니버스고정=예",
        "all true": "모두 예",
        "all passed": "모두 통과",
        "blocked_dup>=2, persistence=True": "중복차단>=2, 영속저장=예",
        "trades_match=True, pnl_diff<=1.00": "trades_match=예, pnl_diff<=1.00",
        "disconnect=0 or reconnect>=disconnect and success": "끊김=0 또는 재연결>=끊김 및 성공",
        "present=True, artifacts>=1, loop_violations=0, dead_ends=0": "다이어그램존재=예, 산출물수>=1, 순환오류=0, 단절흐름=0",
        "tested>0, passed=tested, critical_fail=0, null_case=True, extreme_case=True": "테스트케이스>0, 통과케이스=테스트케이스, 치명실패=0, 결측치케이스=예, 극단값케이스=예",
        "within threshold and all true": "임계 이내 + 모두 예",
        "sources>=2, mismatch=0, tol_breach=0": "대조원천>=2, 대조불일치=0, 허용오차초과=0",
        "records>=100, p95<=500.0ms, error_rate<=1.00%": "처리레코드>=100, P95지연<=500.0ms, 오류율<=1.00%",
        "doc=True, coverage=100%, gaps=0": "문서=예, 커버리지=100%, 누락항목=0",
        ">=1 active channel and all required alerts": "활성 채널>=1 및 필수 알림 모두 활성",
        "drill<=30d, rpo<=15.0m, rto<=60.0m, checklist=all true": "드릴경과일<=30d, RPO<=15.0m, RTO<=60.0m, 체크리스트=모두 예",
        "missing=0, backoff_ok=True": "누락=0, 백오프정상=예",
        "critical>=90%, normal>=70%, periods>=50%": "핵심>=90%, 일반>=70%, 구간>=50%",
        "0 violations": "위반 0건",
        "invalid=0": "오류 0건",
    }

    actual_token_map = {
        "rate_limit_429": "429횟수",
        "negative_ev_executed": "음수기대값집행",
        "reconnect_success": "재연결성공",
        "internal_trades": "내부체결건",
        "stmt_trades": "명세서체결건",
        "internal_pnl": "내부손익",
        "stmt_pnl": "명세서손익",
        "match_rate": "체결일치율",
        "observed_mdd": "관측MDD",
        "invalid_transitions": "무효전이",
        "blocked_dup": "중복차단",
        "unique_keys": "멱등키",
        "checklist": "체크리스트",
        "disconnects": "끊김횟수",
        "reconnects": "재연결횟수",
        "reconnect": "재연결시간",
        "mismatches": "불일치건수",
        "sequences": "시퀀스수",
        "issues": "이슈건수",
        "coverage": "커버리지",
        "present": "다이어그램존재",
        "artifacts": "산출물수",
        "loop_violations": "순환오류",
        "dead_ends": "단절흐름",
        "tested": "테스트케이스",
        "critical_fail": "치명실패",
        "null_case": "결측치케이스",
        "extreme_case": "극단값케이스",
        "records": "처리레코드",
        "error_rate": "오류율",
        "drill_days": "드릴경과일",
        "schedule": "스케줄항목",
        "channels": "알림채널",
        "required": "필수이벤트",
        "alert_rate": "알림전송률",
        "restore": "복구시간",
        "loss": "손실률",
        "exceeded": "한도초과",
        "blocked": "차단건수",
        "checks": "점검건수",
        "expectancy": "거래기대값",
        "cost_floor": "최소비용",
        "sample": "샘플수",
        "matched": "일치건수",
        "errors": "오류건수",
        "target_tps": "주문요청TPS",
        "ratio": "주문비율",
        "gross": "총익스포저",
        "single": "단일종목익스포저",
        "sector": "섹터익스포저",
        "leverage": "레버리지",
        "violations": "위반건수",
        "valid_rate": "유효율",
        "invalid": "오류건수",
        "snapshot": "스냅샷시각",
        "latest_data": "최신데이터시각",
        "asof_match": "asof일치",
        "universe_fixed": "유니버스고정",
        "submit": "주문제출",
        "persistence": "영속저장",
        "passed": "통과케이스",
        "failed": "실패건수",
        "critical": "핵심",
        "normal": "일반",
        "failed_periods": "미달구간",
        "status": "주문상태",
        "qty": "수량",
        "time": "소요시간",
        "steps": "절차",
        "login": "로그인점검",
        "limits": "한계항목",
        "guides": "가이드항목",
        "actual": "실제",
        "expected": "기대",
        "diff": "차이",
        "trading_day": "거래일여부",
        "controls": "통제항목",
        "doc": "문서",
        "gaps": "누락항목",
        "sources": "대조원천",
        "mismatch": "대조불일치",
        "tol_breach": "허용오차초과",
        "regimes": "국면수",
        "strategy": "전략수익",
        "configured": "설정값",
        "pf": "수익인자(PF)",
        "is_drift": "체결드리프트",
        "rejected": "거부여부",
        "limit": "한도",
        "threshold": "임계치",
        "p95": "P95지연",
        "rpo": "RPO",
        "rto": "RTO",
    }

    stock_terms = load_stock_terms()
    if isinstance(stock_terms, dict):
        status_label_map.update(stock_terms.get("status_label_map", {}))
        status_chip_map.update(stock_terms.get("status_chip_map", {}))
        item_name_map.update(stock_terms.get("item_name_map", {}))
        message_map.update(stock_terms.get("message_map", {}))
        expected_map.update(stock_terms.get("expected_map", {}))
        actual_token_map.update(stock_terms.get("token_map", {}))
        phase_title_override = stock_terms.get("phase_title_map", {})
        if isinstance(phase_title_override, dict):
            for phase in VerificationPhase:
                if phase.name in phase_title_override:
                    phase_title_map[phase] = str(phase_title_override[phase.name])
    message_prefix_map = dict(stock_terms.get("message_prefix_map", {})) if isinstance(stock_terms, dict) else {}
    expected_post_replacements = dict(stock_terms.get("expected_post_replacements", {})) if isinstance(stock_terms, dict) else {}

    def localize_actual_tokens(text: str) -> str:
        out = text
        for token in sorted(actual_token_map.keys(), key=len, reverse=True):
            out = out.replace(f"{token}=", f"{actual_token_map[token]}=")
        return out
    def localize_expected_tokens(text: str) -> str:
        out = text
        for token in sorted(actual_token_map.keys(), key=len, reverse=True):
            label = actual_token_map[token]
            out = out.replace(f"{token}>=", f"{label}>=")
            out = out.replace(f"{token}<=", f"{label}<=")
            out = out.replace(f"{token}=", f"{label}=")
            out = out.replace(f"{token}>", f"{label}>")
            out = out.replace(f"{token}<", f"{label}<")
        out = out.replace(" and ", " 및 ")
        out = out.replace("all true", "모두 예")
        for src, dst in expected_post_replacements.items():
            out = out.replace(str(src), str(dst))
        out = out.replace("pnl_diff", "손익차이")
        out = out.replace("pnl_차이", "손익차이")
        out = out.replace("diff <=", "차이 <=")
        out = out.replace("diff<=", "차이<=")
        out = out.replace("=sample", "=샘플수")
        return out
    def localize_text(value: Any, kind: str) -> str:
        if value is None:
            return "-"
        text = str(value)
        if kind == "message":
            text = message_map.get(text, text)
            for prefix, replacement in message_prefix_map.items():
                if text.startswith(prefix):
                    text = text.replace(prefix, replacement)
                    break
            if text.startswith("Round-trip with slippage="):
                text = text.replace("Round-trip with slippage=", "왕복 비용(슬리피지 포함)=")
            if text.startswith("utilization="):
                text = text.replace("utilization=", "사용률=")
            if text.startswith("Coverage policy satisfied;"):
                text = text.replace("Coverage policy satisfied;", "커버리지 정책 충족;")
                text = text.replace("missing_critical=", "누락 핵심=")
                text = text.replace("missing_normal=", "누락 일반=")
                text = text.replace("failed_periods=", "실패 구간=")
        elif kind == "actual":
            text = localize_actual_tokens(text)
        elif kind == "expected":
            text = expected_map.get(text, text)
            text = localize_expected_tokens(text)

        text = text.replace("=True", "=예").replace("=False", "=아니오")
        if text == "True":
            text = "예"
        elif text == "False":
            text = "아니오"
        return text

    api_provider_raw = str(report.system_config.api_provider or "").strip()
    api_provider_map = {
        "KIS": "한국투자증권",
        "KIWOOM": "키움증권",
        "EBEST": "LS증권",
        "LS": "LS증권",
    }
    api_provider_map.update(stock_terms.get("api_provider_map", {}))
    api_provider_label = api_provider_map.get(api_provider_raw.upper(), api_provider_raw or "-")

    rows_by_phase: List[str] = []
    for phase, phase_report in report.phase_reports.items():
        phase_name = esc(phase_title_map.get(phase, phase.value))
        badge_class = "badge-pass" if phase_report.is_phase_passed else "badge-fail"

        table_rows: List[str] = []
        for result in phase_report.results:
            status_raw = result.status.value if result.status else "N/A"
            status_label = status_label_map.get(status_raw, status_raw)
            status_chip_class = status_chip_map.get(status_raw, "chip-na")
            item_name = item_name_map.get(result.item_name, result.item_name)
            actual_value = localize_text(result.actual_value, "actual")
            expected_value = localize_text(result.expected_value, "expected")
            message = localize_text(result.message, "message")
            table_rows.append(
                "<tr>"
                f"<td class='item'>{esc(item_name)}</td>"
                f"<td><span class='chip {status_chip_class}'>{esc(status_label)}</span></td>"
                f"<td>{esc(actual_value)}</td>"
                f"<td>{esc(expected_value)}</td>"
                f"<td>{esc(message)}</td>"
                "</tr>"
            )

        phase_html = (
            "<section class='phase-card'>"
            "<div class='phase-header'>"
            f"<h2>{phase_name}</h2>"
            f"<div class='phase-badge {badge_class}'>통과:{phase_report.passed_count} | 실패:{phase_report.failed_count} | 건너뜀:{phase_report.skipped_count}</div>"
            "</div>"
            "<div class='table-wrap'>"
            "<table>"
            "<colgroup>"
            "<col class='col-item'><col class='col-status'><col class='col-actual'><col class='col-expected'><col class='col-message'>"
            "</colgroup>"
            "<thead><tr><th>검증 항목</th><th>상태</th><th>실제값</th><th>기대값</th><th>메시지</th></tr></thead>"
            f"<tbody>{''.join(table_rows)}</tbody>"
            "</table>"
            "</div>"
            "</section>"
        )
        rows_by_phase.append(phase_html)

    deployment_class = "deploy-pass" if report.is_deployment_ready else "deploy-fail"
    deployment_title = "배포 가능 - 자동운용 승인" if report.is_deployment_ready else "배포 불가 - 실패 항목 해결 필요"
    pass_rate_pct = f"{report.overall_pass_rate:.0f}%"

    return f"""<!doctype html>
<html lang='ko'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>알고리즘 트레이딩 시스템 검증 보고서</title>
<style>
  :root {{
    --bg: #eef2f7;
    --card: #ffffff;
    --ink: #1f2d3d;
    --muted: #5f7185;
    --navy: #2d435a;
    --line: #d8dee8;
    --pass: #28a745;
    --warn: #ffc107;
    --fail: #dc3545;
    --skip: #6c757d;
    --accent: #3a8ed0;
  }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; font-family: "Segoe UI", "Malgun Gothic", sans-serif; background: var(--bg); color: var(--ink); }}
  .container {{ max-width: 1300px; margin: 20px auto; padding: 0 14px 24px; }}
  .hero {{
    background: linear-gradient(135deg, #2d435a 0%, #3a8ed0 100%);
    color: #fff;
    border-radius: 12px;
    padding: 24px 28px;
    box-shadow: 0 4px 14px rgba(28, 42, 56, 0.2);
    margin-bottom: 18px;
  }}
  .hero h1 {{ margin: 0 0 8px; font-size: 42px; letter-spacing: -0.5px; }}
  .hero p {{ margin: 0; opacity: 0.95; }}
  .summary-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 14px;
    margin-bottom: 18px;
  }}
  .metric {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 16px;
    text-align: center;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
  }}
  .metric .value {{ font-size: 42px; font-weight: 800; line-height: 1; margin-bottom: 8px; }}
  .metric .label {{ color: var(--muted); font-size: 14px; }}
  .v-pass {{ color: var(--pass); }}
  .v-fail {{ color: var(--fail); }}
  .v-warn {{ color: #d59f00; }}
  .v-skip {{ color: #5b6a78; }}
  .settings {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 14px 18px;
    margin-bottom: 18px;
  }}
  .settings h3 {{ margin: 0 0 12px; font-size: 28px; }}
  .cfg-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
    gap: 8px 14px;
    color: var(--muted);
    font-size: 14px;
  }}
  .cfg-grid b {{ color: var(--ink); margin-left: 6px; }}
  .phase-card {{
    background: var(--card);
    border: 1px solid var(--line);
    border-radius: 12px;
    overflow: hidden;
    margin-bottom: 14px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
  }}
  .phase-header {{
    background: var(--navy);
    color: #fff;
    padding: 12px 16px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
  }}
  .phase-header h2 {{ margin: 0; font-size: 26px; }}
  .phase-badge {{
    border-radius: 999px;
    padding: 6px 12px;
    font-size: 13px;
    font-weight: 700;
    white-space: nowrap;
  }}
  .badge-pass {{ background: #22b35f; color: #fff; }}
  .badge-fail {{ background: #6c757d; color: #fff; }}
  .table-wrap {{ overflow-x: auto; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; table-layout: fixed; }}
  th, td {{ border-bottom: 1px solid var(--line); padding: 10px 12px; text-align: left; vertical-align: top; word-break: break-word; }}
  thead th {{ background: #f6f8fb; color: #374a60; font-weight: 700; }}
  td.item {{ font-weight: 700; min-width: 180px; }}
  col.col-item {{ width: 19%; }}
  col.col-status {{ width: 10%; }}
  col.col-actual {{ width: 24%; }}
  col.col-expected {{ width: 19%; }}
  col.col-message {{ width: 28%; }}
  .chip {{
    display: inline-block;
    border-radius: 6px;
    padding: 3px 10px;
    font-size: 12px;
    font-weight: 800;
    color: #fff;
    min-width: 74px;
    text-align: center;
  }}
  .chip-pass {{ background: var(--pass); }}
  .chip-fail {{ background: var(--fail); }}
  .chip-warn {{ background: var(--warn); color: #111; }}
  .chip-skip {{ background: var(--skip); }}
  .chip-na {{ background: #95a1ae; }}
  .deploy-box {{
    border-radius: 12px;
    padding: 18px;
    margin-top: 16px;
    text-align: center;
    background: #fff;
    border: 3px solid;
  }}
  .deploy-fail {{ border-color: #dc3545; color: #a01828; }}
  .deploy-pass {{ border-color: #28a745; color: #1c7e35; }}
  .deploy-box h3 {{ margin: 0 0 6px; font-size: 34px; }}
  .deploy-box p {{ margin: 0; color: #33495f; }}
  @media (max-width: 900px) {{
    .hero h1 {{ font-size: 32px; }}
    .phase-header h2 {{ font-size: 20px; }}
  }}
</style>
</head>
<body>
<main class='container'>
  <section class='hero'>
    <h1>알고리즘 트레이딩 시스템 검증 보고서</h1>
    <p>{esc(report.system_config.system_name)} v{esc(report.system_config.version)} | 시장: {esc(report.system_config.market)} | 생성: {esc(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}</p>
  </section>

  <section class='summary-grid'>
    <div class='metric'><div class='value'>{report.total_tests}</div><div class='label'>총 검증 항목</div></div>
    <div class='metric'><div class='value v-pass'>{report.total_passed}</div><div class='label'>통과</div></div>
    <div class='metric'><div class='value v-fail'>{report.total_failed}</div><div class='label'>실패</div></div>
    <div class='metric'><div class='value v-warn'>{report.total_warnings}</div><div class='label'>경고</div></div>
    <div class='metric'><div class='value v-skip'>{report.total_skipped}</div><div class='label'>건너뜀</div></div>
    <div class='metric'><div class='value'>{esc(pass_rate_pct)}</div><div class='label'>통과율</div></div>
  </section>

  <section class='settings'>
    <h3>시스템 설정</h3>
    <div class='cfg-grid'>
      <div>아키텍처 <b>{esc(report.system_config.architecture_type)}</b></div>
      <div>API 제공자 <b>{esc(api_provider_label)}</b></div>
      <div>목표 지연시간 <b>{esc(f"{report.system_config.target_latency_ms:.1f}ms")}</b></div>
      <div>단일 주문 한도 <b>{esc(f"{report.system_config.max_single_order_ratio*100:.1f}%")}</b></div>
      <div>일일 손실 한도 <b>{esc(f"{report.system_config.max_daily_loss_ratio*100:.1f}%")}</b></div>
      <div>MDD 한도 <b>{esc(f"{report.system_config.max_drawdown_limit*100:.1f}%")}</b></div>
    </div>
  </section>

  {''.join(rows_by_phase)}

  <section class='deploy-box {deployment_class}'>
    <h3>{deployment_title}</h3>
    <p>통과율: {report.overall_pass_rate:.1f}% | 실패: {report.total_failed}건 | 경고: {report.total_warnings}건 | 건너뜀: {report.total_skipped}건</p>
  </section>
</main>
</body>
</html>
"""

def generate_json_report(report: FullVerificationReport) -> str:
    return json.dumps(report.to_dict(), ensure_ascii=False, indent=2)























