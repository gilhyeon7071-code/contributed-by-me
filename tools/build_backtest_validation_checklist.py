from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest_module_progress import build_module_progress
import logging


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


TITLE_MAP = {
    "data_close_integrity": "데이터 종가 무결성",
    "data_return_finite": "데이터 수익률 유한값",
    "data_time_index_integrity": "데이터 시간축 무결성",
    "look_ahead_proxy": "Look-ahead 편향",
    "position_lag": "포지션 랙(신호 지연)",
    "signal_quality_ic_ir": "신호 품질(IC/IR)",
    "strategy_parameter_validation": "전략 파라미터 검증",
    "walk_forward": "워크포워드(WFE)",
    "monte_carlo": "몬테카를로(MDD)",
    "deflated_sharpe_ratio": "DSR 게이트",
    "market_regime_response": "과거 시장 대응 검증(레짐)",
    "historical_scenario_response": "과거 시장 대응 검증(시나리오)",
    "inflation_real_return": "인플레이션 반영 검증",
    "temporal_consistency": "시간적 일관성 검증",
    "psychological_tolerance": "심리적 허용치 검증",
    "outlier_concentration": "아웃라이어 집중도 검증",
    "cpcv_pbo": "CPCV/PBO",
    "statistical_power_mde": "사전등록 MDE/검출력",
    "acceptance_pnl_turnover": "PnL/회전율 수용성",
}

ACTION_MAP = {
    "data_close_integrity": "휴장일/오류행(close<=0) 제거 후 재생성",
    "data_return_finite": "Inf/NaN 수익률 발생 원인(0분모/결측) 정리",
    "data_time_index_integrity": "중복/역순 타임스탬프 정리 후 재검증",
    "look_ahead_proxy": "signal 생성 시점과 체결 시점 분리(shift) 재검토",
    "position_lag": "position=signal.shift(1) 강제 및 예외 케이스 점검",
    "walk_forward": "파라미터 단순화/탐색축소 후 OOS 재검증",
    "monte_carlo": "포지션 사이즈 축소, 리스크 한도 및 손절 재조정",
    "deflated_sharpe_ratio": "탐색횟수/표본 수 대비 통계적 우위(DSR) 재검증 및 과최적화 축소",
    "cpcv_pbo": "탐색공간 축소 + 교차검증 강도 상향",
    "strategy_parameter_validation": "파라미터 범위/경계값/강건성 점검 후 재탐색",
    "market_regime_response": "상승/하락/고변동 레짐별 진입/청산 규칙 보정",
    "historical_scenario_response": "시나리오별 실패구간에 보호 규칙(익절/손절/리밸런싱) 보강",
    "inflation_real_return": "명목수익 대비 실질수익 개선(비용/회전율/현금비중 조정)",
    "temporal_consistency": "연도별 편차가 큰 구간의 규칙 과적합 축소",
    "psychological_tolerance": "허용 MDD 초과 시 포지션 크기/손실한도 하향",
    "outlier_concentration": "소수 거래/종목 의존도 완화(분산/필터 재설계)",
    "statistical_power_mde": "표본 수/독립 윈도우 확충 또는 경제적 MDE 기준 재사전등록",
    "acceptance_pnl_turnover": "PnL 신뢰구간 하단이 0을 넘도록 표본/진입품질 보강 후 재검증",
}

CRITICAL_GATES = {
    "data_close_integrity",
    "data_return_finite",
    "look_ahead_proxy",
    "position_lag",
    "deflated_sharpe_ratio",
    "psychological_tolerance",
}
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_NOT_EVALUABLE = "NOT_EVALUABLE"




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _fmt_num(v: Any) -> str:
    try:
        f = float(v)
    except Exception:
        return "-"
    if math.isnan(f):
        return "NaN"
    if math.isinf(f):
        return "Inf" if f > 0 else "-Inf"
    return f"{f:.6g}"


def _has_value(details: Dict[str, Any], key: str) -> bool:
    try:
        f = float(details.get(key))
    except Exception:
        return False
    return math.isfinite(f)


def _float_or_nan(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return math.nan


def _metric_and_threshold(name: str, details: Dict[str, Any]) -> Tuple[str, str]:
    if name == "data_close_integrity":
        return (
            f"nonpositive_close_count={_fmt_num(details.get('nonpositive_close_count'))}, close_min={_fmt_num(details.get('close_min'))}",
            "nonpositive_close_count=0",
        )
    if name == "data_return_finite":
        return (
            f"nonfinite_return_count={_fmt_num(details.get('nonfinite_return_count'))}, excess_nonfinite_count={_fmt_num(details.get('excess_nonfinite_count'))}",
            "excess_nonfinite_count=0",
        )
    if name == "data_time_index_integrity":
        return (
            f"index_monotonic={details.get('index_monotonic')}, duplicate_index_count={_fmt_num(details.get('duplicate_index_count'))}",
            "index_monotonic=True and duplicate_index_count=0",
        )
    if name == "look_ahead_proxy":
        n_obs = details.get("n_obs")
        min_n = details.get("min_n")
        suffix = ""
        if n_obs is not None or min_n is not None:
            suffix = f", n_obs={_fmt_num(n_obs)}, min_n={_fmt_num(min_n)}"
        return f"corr={_fmt_num(details.get('corr'))}{suffix}", f"abs(corr)<={_fmt_num(details.get('threshold', 0.2))}"
    if name == "position_lag":
        return f"mismatch_ratio={_fmt_num(details.get('mismatch_ratio'))}", "<=0.10"
    if name == "signal_quality_ic_ir":
        return (
            f"ic_mean={_fmt_num(details.get('ic_mean'))}, ic_std={_fmt_num(details.get('ic_std'))}, ess={_fmt_num(details.get('ess'))}, min_ess={_fmt_num(details.get('min_ess'))}",
            f"ess>={_fmt_num(details.get('min_ess', 200))}",
        )
    if name == "walk_forward":
        required_rows = details.get("required_rows")
        if not _has_value(details, "median_wfe"):
            return (
                f"len={_fmt_num(details.get('len'))}, required_rows={_fmt_num(required_rows)}, median_wfe=검증 데이터 없음",
                f"len>={_fmt_num(required_rows)}",
            )
        return f"median_wfe={_fmt_num(details.get('median_wfe'))}", ">=50"
    if name == "monte_carlo":
        if _has_value(details, "mc_tail_mdd"):
            return f"mc_tail_mdd={_fmt_num(details.get('mc_tail_mdd'))}, alpha={_fmt_num(details.get('mc_alpha'))}", f">={_fmt_num(details.get('limit', -0.3))}"
        return f"mc95_mdd={_fmt_num(details.get('mc95_mdd'))}", f">={_fmt_num(details.get('limit', -0.3))}"
    if name == "deflated_sharpe_ratio":
        return (
            f"dsr={_fmt_num(details.get('deflated_sharpe_ratio'))}, n_obs={_fmt_num(details.get('n_obs'))}, n_trials={_fmt_num(details.get('n_trials'))}",
            f">={_fmt_num(details.get('min_dsr', 0.1))}",
        )
    if name == "cpcv_pbo":
        if not (_has_value(details, "pbo_approx") and _has_value(details, "median_oos_sharpe")):
            return (
                f"len={_fmt_num(details.get('len'))}, min_n={_fmt_num(details.get('min_n'))}, pbo=검증 데이터 없음, med_oos_sharpe=검증 데이터 없음",
                f"len>={_fmt_num(details.get('min_n'))}, pbo<=0.50 and med_oos_sharpe>=0",
            )
        pbo = _fmt_num(details.get("pbo_approx"))
        med = _fmt_num(details.get("median_oos_sharpe"))
        return f"pbo={pbo}, med_oos_sharpe={med}", "pbo<=0.50 and med_oos_sharpe>=0"
    if name == "strategy_parameter_validation":
        rr = _fmt_num(details.get("robust_ratio"))
        bs = _fmt_num(details.get("base_sharpe"))
        br = _fmt_num(details.get("boundary_ratio"))
        dk = details.get("domain_ok")
        return f"robust_ratio={rr}, base_sharpe={bs}, boundary_ratio={br}, domain_ok={dk}", "domain_ok=True, boundary_ratio<=0.50, robust_ratio>=0.60"
    if name == "market_regime_response":
        vr = _fmt_num(details.get("valid_regimes"))
        wm = _fmt_num(details.get("worst_mdd"))
        ms = _fmt_num(details.get("median_regime_sharpe"))
        cr = _fmt_num(details.get("catastrophic_regimes"))
        return f"valid_regimes={vr}, catastrophic_regimes={cr}, worst_mdd={wm}, median_regime_sharpe={ms}", "valid_regimes>=2, worst_mdd>-0.60, median_regime_sharpe>=-0.50"
    if name == "historical_scenario_response":
        cv = _fmt_num(details.get("covered_scenarios"))
        try:
            covered = int(float(details.get("covered_scenarios")))
        except Exception:
            covered = 0
        if covered <= 0:
            return f"covered_scenarios={cv}, scenario_result=검증 데이터 없음", "covered_scenarios>=2, worst_mdd>-0.65"
        wm = _fmt_num(details.get("worst_mdd"))
        ms = _fmt_num(details.get("median_sharpe"))
        return f"covered_scenarios={cv}, worst_mdd={wm}, median_sharpe={ms}", "covered_scenarios>=2, worst_mdd>-0.65"
    if name == "inflation_real_return":
        yr = _fmt_num(details.get("years_covered"))
        med = _fmt_num(details.get("median_real_return"))
        wr = _fmt_num(details.get("worst_real_return"))
        return f"years_covered={yr}, median_real_return={med}, worst_real_return={wr}", "years_covered>=2, median_real_return>=0"
    if name == "temporal_consistency":
        ny = _fmt_num(details.get("n_years"))
        pr = _fmt_num(details.get("positive_year_ratio"))
        wy = _fmt_num(details.get("worst_year_return"))
        return f"n_years={ny}, positive_year_ratio={pr}, worst_year_return={wy}", "n_years>=3, positive_year_ratio>=0.40, worst_year_return>-0.50"
    if name == "psychological_tolerance":
        mdd = _fmt_num(details.get("max_drawdown"))
        lim = _fmt_num(details.get("mdd_limit"))
        return f"max_drawdown={mdd}, mdd_limit={lim}", "max_drawdown>=mdd_limit"
    if name == "outlier_concentration":
        tr = _fmt_num(details.get("top_contrib_ratio"))
        sn = _fmt_num(details.get("sample_n"))
        return f"top_contrib_ratio={tr}, sample_n={sn}", "top_contrib_ratio<=0.80, sample_n>=20"
    if name == "statistical_power_mde":
        ne = _fmt_num(details.get("n_eff"))
        sig = _fmt_num(details.get("sigma"))
        mr = _fmt_num(details.get("mde_return"))
        ms = _fmt_num(details.get("mde_sharpe"))
        ers = _fmt_num(details.get("economic_mde_sharpe"))
        scale = details.get("sharpe_scale", "-")
        return f"n_eff={ne}, sigma={sig}, mde_return={mr}, mde_sharpe={ms}({scale})", f"mde_sharpe<=economic_mde_sharpe({ers}) or mde_return<=economic_mde_return"
    if name == "acceptance_pnl_turnover":
        low = _fmt_num(details.get("pnl_ci95_low"))
        med = _fmt_num(details.get("pnl_ci95_med"))
        high = _fmt_num(details.get("pnl_ci95_high"))
        turnover = _fmt_num(details.get("turnover_monthly"))
        turnover_limit = _fmt_num(details.get("turnover_limit_monthly"))
        return (
            f"pnl_ci95_low={low}, pnl_ci95_med={med}, pnl_ci95_high={high}, turnover_monthly={turnover}",
            f"pnl_ci95_low>0 and turnover_monthly<={turnover_limit}",
        )

    keys = [k for k in details.keys()][:2]
    if not keys:
        return "-", "-"
    metric = ", ".join([f"{k}={_fmt_num(details.get(k))}" for k in keys])
    return metric, "-"


def _scenario_coverage_suffix(report: Dict[str, Any]) -> str:
    artifacts = report.get("artifacts", {}) if isinstance(report.get("artifacts"), dict) else {}
    hs = artifacts.get("historical_scenario_response", {}) if isinstance(artifacts.get("historical_scenario_response"), dict) else {}
    scenarios = hs.get("scenarios", []) if isinstance(hs.get("scenarios"), list) else []
    if not scenarios:
        return ""

    target_tokens = {
        "2008": ("gfc_2008",),
        "2020": ("covid_crash",),
        "2022": ("rate_hike_2022",),
    }

    out: List[str] = []
    for year, keys in target_tokens.items():
        matched = None
        for row in scenarios:
            if not isinstance(row, dict):
                continue
            name = str(row.get("scenario", "")).strip()
            if name in keys:
                matched = row
                break
        if matched is None:
            out.append(f"{year}=MISSING")
            continue

        covered = bool(matched.get("covered", False))
        if covered:
            mdd = _fmt_num(matched.get("max_drawdown"))
            out.append(f"{year}=Y(mdd={mdd})")
        else:
            out.append(f"{year}=N")

    return ", scenario_coverage[" + "; ".join(out) + "]"

def _is_regime_coverage_limited(name: str, details: Dict[str, Any]) -> bool:
    if name != "market_regime_response":
        return False

    valid_regimes = _float_or_nan(details.get("valid_regimes"))
    catastrophic = _float_or_nan(details.get("catastrophic_regimes"))
    worst_mdd = _float_or_nan(details.get("worst_mdd"))
    median_sharpe = _float_or_nan(details.get("median_regime_sharpe"))

    if math.isfinite(valid_regimes) and valid_regimes >= 2:
        return False
    if math.isfinite(catastrophic) and catastrophic > 0:
        return False
    if math.isfinite(worst_mdd) and worst_mdd <= -0.60:
        return False
    if math.isfinite(median_sharpe) and median_sharpe < -0.50:
        return False
    return True


def _is_not_evaluable(name: str, details: Dict[str, Any], metric: str, summary: str = "") -> bool:
    text = f"{metric} {summary}"
    return (
        _is_regime_coverage_limited(name, details)
        or
        ("NaN" in text)
        or ("Inf" in text)
        or ("검증 데이터 없음" in text)
        or ("deferred:" in text.lower())
        or ("insufficient operating" in text.lower())
        or ("insufficient data" in text.lower())
        or ("no valid split" in text.lower())
    )


def _status_label(name: str, passed: bool, details: Dict[str, Any], metric: str, summary: str = "") -> str:
    if _is_not_evaluable(name, details, metric, summary):
        return STATUS_NOT_EVALUABLE
    return STATUS_PASS if passed else STATUS_FAIL


def _action_for(name: str, status: str) -> str:
    if name == "look_ahead_proxy" and status == STATUS_NOT_EVALUABLE:
        return "운영 표본을 min_n까지 확충한 뒤 signal-future 상관 재검증"
    if name == "signal_quality_ic_ir" and status == STATUS_NOT_EVALUABLE:
        return "운영 표본 ESS를 min_ess까지 확충한 뒤 IC/IR 재검증"
    if name == "market_regime_response" and status == STATUS_NOT_EVALUABLE:
        return "레짐별 최소 표본(valid_regimes>=2) 확보 후 재검증"
    if name == "walk_forward" and status == STATUS_NOT_EVALUABLE:
        return "운영 표본을 required_rows까지 확충한 뒤 WFE 재검증"
    if name == "historical_scenario_response" and status == STATUS_NOT_EVALUABLE:
        return "운영 구간에 시나리오 커버리지(covered_scenarios>=2)가 확보된 뒤 재검증"
    if name == "inflation_real_return" and status == STATUS_NOT_EVALUABLE:
        return "운영 구간의 연도 커버리지(years_covered>=2)가 확보된 뒤 실질수익 재검증"
    if name == "temporal_consistency" and status == STATUS_NOT_EVALUABLE:
        return "운영 연도 표본(n_years>=3)이 확보된 뒤 시간적 일관성 재검증"
    if name == "cpcv_pbo" and status == STATUS_NOT_EVALUABLE:
        return "운영 표본을 min_n까지 확충한 뒤 CPCV/PBO 재검증"
    return ACTION_MAP.get(name, "세부지표 기반 보정 후 재검증")


def _issue_tag(status: str) -> str:
    if status == STATUS_PASS:
        return "-"
    if status == STATUS_NOT_EVALUABLE:
        return "ISSUE(DATA)"
    return "ISSUE"


def _operation_judgment(items: List[Dict[str, Any]]) -> str:
    fail_items = [x for x in items if x.get("status") == STATUS_FAIL]
    not_eval_items = [x for x in items if x.get("status") == STATUS_NOT_EVALUABLE]
    critical_fail = any(x.get("name") in CRITICAL_GATES for x in fail_items)

    if len(fail_items) == 0 and len(not_eval_items) == 0:
        return "운영가능"
    if critical_fail:
        return "운영보류"
    if len(fail_items) <= 2 and len(not_eval_items) <= 2:
        return "조건부운영"
    if len(fail_items) == 0 and len(not_eval_items) > 0:
        return "판정보강필요"
    return "운영보류"


def _evaluation_scope(report: Dict[str, Any]) -> Dict[str, Any]:
    artifacts = report.get("artifacts", {}) if isinstance(report.get("artifacts"), dict) else {}
    base_meta = artifacts.get("base_meta", {}) if isinstance(artifacts.get("base_meta"), dict) else {}
    source = base_meta.get("source", {}) if isinstance(base_meta.get("source"), dict) else {}
    engine = str(base_meta.get("engine", "") or "").strip()
    ledger_csv = str(source.get("ledger_csv", "") or "").strip()
    is_paper_ledger = bool(ledger_csv) and "paper_fills_ledger" in ledger_csv.replace("\\", "/")
    if engine == "real_strategy_backtest" and is_paper_ledger:
        return {
            "scope": "paper_early_logic_check",
            "label": "초기 가상매매 데이터 기반 백테스트 로직 검증",
            "interpretation": "현재 결과는 전략 성과 확정 판정이 아니라, 원장 입력을 기준으로 계산/차단 로직이 정직하게 동작하는지 보는 검증이다.",
            "strategy_conclusion_allowed": False,
        }
    return {
        "scope": "strategy_backtest_validation",
        "label": "전략 백테스트 검증",
        "interpretation": "백테스트 입력 범위 기준의 전략 검증 결과다.",
        "strategy_conclusion_allowed": True,
    }


def _render_md(data: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# 백테스트 검증 체크리스트 ({data['generated_at']})")
    lines.append("")
    lines.append(f"- overall: **{'PASS' if data['passed'] else 'FAIL'}**")
    lines.append(f"- operation_judgment: **{data.get('operation_judgment','-')}**")
    scope = data.get("evaluation_scope", {}) if isinstance(data.get("evaluation_scope"), dict) else {}
    if scope:
        lines.append(f"- evaluation_scope: **{scope.get('label', '-')}**")
        lines.append(f"- interpretation: {scope.get('interpretation', '-')}")
    lines.append(
        f"- total: **{data['total']}**, pass: **{data['pass_n']}**, fail: **{data['fail_n']}**, not_evaluable: **{data['not_evaluable_n']}**"
    )
    lines.append(f"- source_json: `{data['source_json']}`")
    module_progress = data.get("module_progress", {}) if isinstance(data.get("module_progress"), dict) else {}
    if module_progress:
        lines.append(
            f"- module_optimizer_ready: **{module_progress.get('optimizer_ready', False)}**"
        )
        blockers = module_progress.get("optimizer_blockers", []) or []
        if blockers:
            lines.append(f"- module_optimizer_blockers: **{', '.join(str(x) for x in blockers)}**")
    lines.append("")
    if module_progress:
        lines.append("## 모듈별 개발진행")
        lines.append("")
        lines.append("| 탭 | 모듈 | 상태 | 완료율 | 최적화 반영 | 보완 포인트 |")
        lines.append("|---:|---|---|---:|---|---|")
        for module in module_progress.get("modules", []) or []:
            blockers = ", ".join(module.get("blockers", [])[:3]) or "-"
            lines.append(
                "| {tab} | {title} | {status} | {pct:.1f}% | {ready} | {blockers} |".format(
                    tab=int(module.get("tab_no", 0)),
                    title=module.get("title", "-"),
                    status=module.get("status", "-"),
                    pct=float(module.get("completion_pct", 0.0)),
                    ready="READY" if bool(module.get("ready_for_optimize", False)) else "WAIT",
                    blockers=blockers,
                )
            )
        lines.append("")
    lines.append("## 항목별 상태")
    lines.append("")
    lines.append("| No | 검증항목 | 상태 | 문제표시 | 핵심지표 | 기준 | 권고조치 |")
    lines.append("|---:|---|---|---|---|---|---|")

    for i, row in enumerate(data["items"], start=1):
        lines.append(
            "| {i} | {item} | {status} | {issue} | {metric} | {thr} | {action} |".format(
                i=i,
                item=row["item"],
                status=row["status"],
                issue=row["issue"],
                metric=row["metric"],
                thr=row["threshold"],
                action=row["action"],
            )
        )

    problematic = [x for x in data["items"] if x["status"] != STATUS_PASS]
    if problematic:
        lines.append("")
        lines.append("## 문제/판정불가 항목 요약")
        lines.append("")
        for x in problematic:
            lines.append(f"- [{x['status']}] [{x['name']}] {x['item']} -> {x['metric']} / {x['threshold']}")

    return "\n".join(lines) + "\n"


def build_checklist(report: Dict[str, Any], source_json: Path) -> Dict[str, Any]:
    gate_results = report.get("gate_results", []) or []
    items: List[Dict[str, Any]] = []

    for g in gate_results:
        name = str(g.get("name", "")).strip()
        passed = bool(g.get("passed", False))
        details = g.get("details", {}) or {}
        metric, threshold = _metric_and_threshold(name, details)
        if name == "historical_scenario_response":
            metric = f"{metric}{_scenario_coverage_suffix(report)}"
        summary = str(g.get("summary", "") or "")
        status = _status_label(name, passed, details, metric, summary)

        row = {
            "name": name,
            "item": TITLE_MAP.get(name, name),
            "status": status,
            "passed": status == STATUS_PASS,
            "issue": _issue_tag(status),
            "metric": metric,
            "threshold": threshold,
            "action": _action_for(name, status),
        }
        items.append(row)

    pass_n = sum(1 for x in items if x["status"] == STATUS_PASS)
    fail_n = sum(1 for x in items if x["status"] == STATUS_FAIL)
    not_eval_n = sum(1 for x in items if x["status"] == STATUS_NOT_EVALUABLE)
    total = len(items)
    operation_judgment = _operation_judgment(items)
    module_progress = build_module_progress(items)
    evaluation_scope = _evaluation_scope(report)

    return {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_json": str(source_json),
        "passed": (fail_n == 0 and not_eval_n == 0),
        "total": total,
        "pass_n": pass_n,
        "fail_n": fail_n,
        "not_evaluable_n": not_eval_n,
        "operation_judgment": operation_judgment,
        "evaluation_scope": evaluation_scope,
        "module_progress": module_progress,
        "items": items,
    }

def main() -> int:
    ap = argparse.ArgumentParser(description="Build checklist-style backtest validation report")
    ap.add_argument("--report-json", default=str(LOG_DIR / "backtest_validation_latest.json"))
    ap.add_argument("--out-json", default="")
    ap.add_argument("--out-md", default="")
    ap.add_argument("--out-csv", default="")
    args = ap.parse_args()

    report_json = Path(args.report_json)
    if not report_json.exists():
        raise FileNotFoundError(f"report json not found: {report_json}")

    report = json.loads(report_json.read_text(encoding="utf-8-sig"))
    checklist = build_checklist(report, report_json)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"backtest_validation_checklist_{stamp}.json")
    out_json_latest = LOG_DIR / "backtest_validation_checklist_latest.json"

    out_md = Path(args.out_md) if args.out_md else (LOG_DIR / f"backtest_validation_checklist_{stamp}.md")
    out_md_latest = LOG_DIR / "backtest_validation_checklist_latest.md"

    out_csv = Path(args.out_csv) if args.out_csv else (LOG_DIR / f"backtest_validation_checklist_{stamp}.csv")
    out_csv_latest = LOG_DIR / "backtest_validation_checklist_latest.csv"

    out_json.write_text(json.dumps(checklist, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    out_json_latest.write_text(json.dumps(checklist, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    md_text = _render_md(checklist)
    out_md.write_text(md_text, encoding="utf-8-sig")
    out_md_latest.write_text(md_text, encoding="utf-8-sig")

    df = pd.DataFrame(checklist["items"])
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    df.to_csv(out_csv_latest, index=False, encoding="utf-8-sig")

    _log_print(
        f"[CHK] overall={'PASS' if checklist['passed'] else 'FAIL'} pass={checklist['pass_n']} fail={checklist['fail_n']} not_evaluable={checklist['not_evaluable_n']}"
    )
    _log_print(f"[CHK] json={out_json}")
    _log_print(f"[CHK] md={out_md}")
    _log_print(f"[CHK] csv={out_csv}")
    _log_print(f"[CHK] latest_json={out_json_latest}")
    _log_print(f"[CHK] latest_md={out_md_latest}")
    _log_print(f"[CHK] latest_csv={out_csv_latest}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())









