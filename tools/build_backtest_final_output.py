from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Tuple
import logging


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

PASS = "PASS"
FAIL = "FAIL"
NE = "NOT_EVALUABLE"




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
def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _fmt(v: Any) -> str:
    try:
        f = float(v)
    except Exception:
        return "-"
    if math.isnan(f) or math.isinf(f):
        return "NaN"
    return f"{f:.4g}"


def _safe_float(v: Any) -> float:
    try:
        f = float(v)
    except Exception:
        return float("nan")
    if math.isnan(f) or math.isinf(f):
        return float("nan")
    return f


def _is_finite(v: float) -> bool:
    return not (math.isnan(v) or math.isinf(v))


def _pct(v: Any, digits: int = 2) -> str:
    f = _safe_float(v)
    if not _is_finite(f):
        return "-"
    return f"{(f * 100.0):.{digits}f}%"


def _pick_item(items: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
    hit = next((x for x in items if x.get("name") == name), None)
    return hit or {}


def _sample_count_proxy(report: Dict[str, Any]) -> float:
    artifacts = report.get("artifacts") or {}
    base = artifacts.get("base_metrics") or {}
    dsr = artifacts.get("deflated_sharpe_ratio") or {}
    outlier = next((x for x in report.get("gate_results", []) if x.get("name") == "outlier_concentration"), {})
    outlier_details = outlier.get("details") if isinstance(outlier, dict) else {}
    candidates = [
        base.get("n_trade_proxy") if isinstance(base, dict) else None,
        base.get("n_buy_trades") if isinstance(base, dict) else None,
        base.get("n_obs") if isinstance(base, dict) else None,
        dsr.get("n_obs") if isinstance(dsr, dict) else None,
        outlier_details.get("sample_n") if isinstance(outlier_details, dict) else None,
    ]
    vals = [_safe_float(x) for x in candidates]
    vals = [x for x in vals if _is_finite(x)]
    return max(vals) if vals else float("nan")


def _domain_status(items: List[Dict[str, Any]], names: List[str]) -> str:
    selected = [_pick_item(items, n) for n in names]
    selected = [x for x in selected if x]
    if not selected:
        return NE
    if any(x.get("status") == FAIL for x in selected):
        return FAIL
    if any(x.get("status") == NE for x in selected):
        return NE
    return PASS


def _build_domains(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    specs = [
        ("1. 로직 안정성", ["data_close_integrity", "data_return_finite", "data_time_index_integrity", "position_lag"]),
        ("2. 편향 탐지", ["look_ahead_proxy", "cpcv_pbo"]),
        ("3. 파라미터 강건성", ["strategy_parameter_validation"]),
        ("4. 교차검증/OOS", ["walk_forward", "cpcv_pbo"]),
        ("5. 시장 레짐", ["market_regime_response", "historical_scenario_response"]),
        ("6. 위기 시나리오", ["historical_scenario_response"]),
        ("7. 몬테카를로", ["monte_carlo"]),
        ("8. 실행비용 현실성", ["outlier_concentration"]),
        ("9. 통계 유의성", ["temporal_consistency", "outlier_concentration", "deflated_sharpe_ratio"]),
        ("10. 재현성/운영", ["data_time_index_integrity", "strategy_parameter_validation"]),
    ]

    out: List[Dict[str, Any]] = []
    for title, names in specs:
        status = _domain_status(items, names)
        refs: List[Dict[str, Any]] = []
        for n in names:
            hit = _pick_item(items, n)
            if hit:
                refs.append(
                    {
                        "name": n,
                        "status": hit.get("status", NE),
                        "metric": hit.get("metric", "-"),
                        "threshold": hit.get("threshold", "-"),
                    }
                )
            else:
                refs.append({"name": n, "status": NE, "metric": "missing", "threshold": "-"})
        out.append({"domain": title, "status": status, "checks": refs})
    return out


def _final_gate(items: List[Dict[str, Any]], report: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
    def gs(name: str) -> str:
        return _pick_item(items, name).get("status", NE)

    min_trade_proxy = _sample_count_proxy(report)
    min_trade_ok = _is_finite(min_trade_proxy) and min_trade_proxy >= 100
    min_trade_status = PASS if min_trade_ok else (FAIL if _is_finite(min_trade_proxy) else NE)
    min_trade_evidence = f"n_obs_proxy={int(min_trade_proxy)}" if _is_finite(min_trade_proxy) else "n_obs_proxy=검증 데이터 없음"

    criteria = [
        {
            "name": "DSR 게이트",
            "status": gs("deflated_sharpe_ratio"),
            "evidence": _pick_item(items, "deflated_sharpe_ratio").get("metric", "dsr 미실행"),
        },
        {
            "name": "MC 95% MDD 허용범위",
            "status": gs("monte_carlo"),
            "evidence": _pick_item(items, "monte_carlo").get("metric", "-"),
        },
        {
            "name": "운영구간 PnL/회전율",
            "status": gs("acceptance_pnl_turnover"),
            "evidence": _pick_item(items, "acceptance_pnl_turnover").get("metric", "-"),
        },
        {
            "name": "파라미터 고원(plateau)",
            "status": gs("strategy_parameter_validation"),
            "evidence": _pick_item(items, "strategy_parameter_validation").get("metric", "-"),
        },
        {
            "name": "독립 거래 100회+ (프록시)",
            "status": min_trade_status,
            "evidence": min_trade_evidence,
        },
    ]

    if any(x["status"] == FAIL for x in criteria):
        decision = "NO_GO"
    elif any(x["status"] == NE for x in criteria):
        decision = "HOLD"
    else:
        decision = "GO"

    return criteria, decision


def _long_horizon_reference_criteria(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cpcv = _pick_item(items, "cpcv_pbo")
    cpcv_status = NE
    if cpcv:
        metric = str(cpcv.get("metric", ""))
        cpcv_status = PASS if cpcv.get("status") == PASS and "pbo=" in metric else cpcv.get("status", NE)

    return [
        {
            "name": "PBO < 0.5",
            "status": cpcv_status,
            "evidence": _pick_item(items, "cpcv_pbo").get("metric", "cpcv 미실행"),
            "blocking": False,
        },
        {
            "name": "위기 시나리오 생존(2008/2020/2022)",
            "status": _pick_item(items, "historical_scenario_response").get("status", NE),
            "evidence": _pick_item(items, "historical_scenario_response").get("metric", "-"),
            "blocking": False,
        },
        {
            "name": "전 레짐 치명손실 부재",
            "status": _pick_item(items, "market_regime_response").get("status", NE),
            "evidence": _pick_item(items, "market_regime_response").get("metric", "-"),
            "blocking": False,
        },
        {
            "name": "WFE 표본 충족 및 산출 가능",
            "status": _pick_item(items, "walk_forward").get("status", NE),
            "evidence": _pick_item(items, "walk_forward").get("metric", "-"),
            "blocking": False,
        },
    ]


def _advisory_checks(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    artifacts = report.get("artifacts") or {}
    base = artifacts.get("base_metrics") or {}
    integ = artifacts.get("integration") or {}
    walk_forward = artifacts.get("walk_forward") or []

    median_wfe = float("nan")
    if isinstance(walk_forward, list):
        vals: List[float] = []
        for row in walk_forward:
            if not isinstance(row, dict):
                continue
            v = _safe_float(row.get("wfe"))
            valid = bool(row.get("wfe_valid", True))
            if _is_finite(v) and valid:
                vals.append(v)
        if vals:
            vals = sorted(vals)
            n = len(vals)
            median_wfe = vals[n // 2] if n % 2 == 1 else (vals[n // 2 - 1] + vals[n // 2]) / 2.0
    wfe_margin_ok = _is_finite(median_wfe) and median_wfe >= 55.0

    annual_return = _safe_float(base.get("annual_return"))
    max_drawdown = _safe_float(base.get("max_drawdown"))
    rr_ratio = float("nan")
    if _is_finite(annual_return) and annual_return > 0 and _is_finite(max_drawdown):
        rr_ratio = abs(max_drawdown) / annual_return
    rr_ok = _is_finite(rr_ratio) and rr_ratio <= 3.0

    cost_model = integ.get("cost_model") if isinstance(integ.get("cost_model"), dict) else {}
    cost_total_bps = float("nan")
    if isinstance(cost_model, dict):
        cb = _safe_float(cost_model.get("commission_bps"))
        sb = _safe_float(cost_model.get("slippage_bps"))
        pb = _safe_float(cost_model.get("spread_bps"))
        if _is_finite(cb) and _is_finite(sb) and _is_finite(pb):
            cost_total_bps = cb + sb + pb
    cost_conservative_ok = _is_finite(cost_total_bps) and cost_total_bps >= 15.0

    return [
        {
            "name": "WFE 안전마진(권고>=55%)",
            "status": "OK" if wfe_margin_ok else "WARN",
            "evidence": f"median_wfe={_fmt(median_wfe)} (threshold>=55)",
        },
        {
            "name": "수익/낙폭 균형 권고(MDD/연수익<=3.0)",
            "status": "OK" if rr_ok else "WARN",
            "evidence": f"annual_return={_pct(annual_return)}, max_drawdown={_pct(max_drawdown)}, ratio={_fmt(rr_ratio)}",
        },
        {
            "name": "실전비용 보수성 권고(총비용>=15bps)",
            "status": "OK" if cost_conservative_ok else "WARN",
            "evidence": f"cost_total_bps={_fmt(cost_total_bps)} (commission+slippage+spread)",
        },
    ]


def _weakness_map(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    bad = [x for x in items if x.get("status") != PASS]
    out: List[Dict[str, Any]] = []
    for x in bad:
        out.append(
            {
                "item": x.get("item", x.get("name", "-")),
                "status": x.get("status", NE),
                "metric": x.get("metric", "-"),
                "threshold": x.get("threshold", "-"),
                "action": x.get("action", "-"),
            }
        )
    return out


def _profitability_summary(checklist: Dict[str, Any], expected: Dict[str, Any], final_decision: str) -> Dict[str, Any]:
    ar = _safe_float(expected.get("annual_return"))
    sr = _safe_float(expected.get("sharpe"))
    mdd = _safe_float(expected.get("max_drawdown"))
    wr = _safe_float(expected.get("win_rate"))

    score = 0
    reasons: List[str] = []

    if _is_finite(ar):
        if ar >= 0.12:
            score += 2
            reasons.append(f"연환산 수익률 양호({_pct(ar)})")
        elif ar >= 0.05:
            score += 1
            reasons.append(f"연환산 수익률 보통({_pct(ar)})")
        elif ar >= 0.0:
            reasons.append(f"연환산 수익률 낮음({_pct(ar)})")
        else:
            score -= 2
            reasons.append(f"연환산 수익률 음수({_pct(ar)})")
    else:
        reasons.append("연환산 수익률 데이터 없음")

    if _is_finite(sr):
        if sr >= 1.0:
            score += 2
            reasons.append("샤프 우수")
        elif sr >= 0.5:
            score += 1
            reasons.append("샤프 보통")
        elif sr >= 0.0:
            reasons.append("샤프 낮음")
        else:
            score -= 1
            reasons.append("샤프 음수")
    else:
        reasons.append("샤프 데이터 없음")

    if _is_finite(mdd):
        if mdd >= -0.20:
            score += 2
            reasons.append(f"낙폭 관리 양호({_pct(mdd)})")
        elif mdd >= -0.30:
            score += 1
            reasons.append(f"낙폭 관리 보통({_pct(mdd)})")
        elif mdd >= -0.40:
            reasons.append(f"낙폭 주의({_pct(mdd)})")
        else:
            score -= 2
            reasons.append(f"낙폭 과다({_pct(mdd)})")
    else:
        reasons.append("낙폭 데이터 없음")

    if _is_finite(wr):
        if wr >= 0.55:
            score += 1
            reasons.append(f"승률 양호({_pct(wr)})")
        elif wr >= 0.45:
            reasons.append(f"승률 보통({_pct(wr)})")
        else:
            score -= 1
            reasons.append(f"승률 낮음({_pct(wr)})")

    fail_n = int(checklist.get("fail_n", 0))
    ne_n = int(checklist.get("not_evaluable_n", 0))

    if final_decision == "NO_GO" or fail_n >= 2:
        level = "나쁨"
        action = "운영 보류 후 전체 검증으로 문제를 도출하고 수정값을 반영한 뒤 재검증"
    elif score >= 4 and fail_n == 0 and ne_n <= 1:
        level = "좋음"
        action = "현재 로직 유지, 리스크 한도만 유지 점검하면서 모니터링 지속"
    else:
        level = "보통"
        action = "조건부 유지, 약점 항목을 우선순위로 보정하고 재검증 결과를 반영"

    return {
        "level": level,
        "score": score,
        "message": action,
        "reasons": reasons[:6],
    }


def _management_summary(checklist: Dict[str, Any], weaknesses: List[Dict[str, Any]]) -> Dict[str, Any]:
    items = checklist.get("items", []) or []
    fail_n = int(checklist.get("fail_n", 0))
    ne_n = int(checklist.get("not_evaluable_n", 0))

    critical = {"data_close_integrity", "data_return_finite", "look_ahead_proxy", "position_lag", "psychological_tolerance"}
    critical_fail = any((x.get("name") in critical and x.get("status") == FAIL) for x in items)

    if critical_fail or fail_n >= 3:
        level = "개선필요"
        msg = "핵심 관리 항목에 실패가 있어 운영 투입 전 수정이 필요"
    elif fail_n == 0 and ne_n <= 1:
        level = "안정"
        msg = "관리 항목은 전반적으로 안정적이며 현재 체계를 유지 가능"
    else:
        level = "주의"
        msg = "치명 실패는 없지만 판정불가/약점 항목 보강이 필요"

    needed = []
    for w in weaknesses:
        status = str(w.get("status", ""))
        if status in {FAIL, NE}:
            needed.append(f"{w.get('item','-')} -> {w.get('action','-')}")
        if len(needed) >= 4:
            break

    return {
        "level": level,
        "message": msg,
        "needed_actions": needed,
    }


def _build_summary(checklist: Dict[str, Any], report: Dict[str, Any]) -> Dict[str, Any]:
    items = checklist.get("items", []) or []
    domains = _build_domains(items)
    final_criteria, final_decision = _final_gate(items, report)
    long_horizon_reference = _long_horizon_reference_criteria(items)

    artifacts = report.get("artifacts") or {}
    integ = artifacts.get("integration") or {}
    base = artifacts.get("base_metrics") or {}
    mc = artifacts.get("monte_carlo") or {}

    expected = {
        "annual_return": base.get("annual_return"),
        "sharpe": base.get("sharpe"),
        "max_drawdown": base.get("max_drawdown"),
        "win_rate": base.get("win_rate"),
        "profit_factor": base.get("profit_factor"),
        "mc_mdd_95": ((mc.get("mdd_pct") or {}).get("95") if isinstance(mc, dict) else None),
        "mc_final_50": ((mc.get("final_pct") or {}).get("50") if isinstance(mc, dict) else None),
        "mdd_to_return_ratio": (abs(_safe_float(base.get("max_drawdown"))) / _safe_float(base.get("annual_return")))
        if (_is_finite(_safe_float(base.get("max_drawdown"))) and _is_finite(_safe_float(base.get("annual_return"))) and _safe_float(base.get("annual_return")) > 0)
        else None,
        "cost_total_bps": (
            _safe_float(((integ.get("cost_model") or {}).get("commission_bps")))
            + _safe_float(((integ.get("cost_model") or {}).get("slippage_bps")))
            + _safe_float(((integ.get("cost_model") or {}).get("spread_bps")))
        ) if (
            _is_finite(_safe_float(((integ.get("cost_model") or {}).get("commission_bps"))))
            and _is_finite(_safe_float(((integ.get("cost_model") or {}).get("slippage_bps"))))
            and _is_finite(_safe_float(((integ.get("cost_model") or {}).get("spread_bps"))))
        ) else None,
    }

    operating = {
        "strategy_source": integ.get("strategy_source"),
        "backtest_source": integ.get("backtest_source"),
        "params": integ.get("params"),
        "cost_model": integ.get("cost_model"),
        "base_meta": artifacts.get("base_meta"),
        "max_tolerable_mdd": integ.get("max_tolerable_mdd"),
    }

    weaknesses = _weakness_map(items)
    result_summary = {
        "profitability": _profitability_summary(checklist, expected, final_decision),
        "management": _management_summary(checklist, weaknesses),
    }
    advisories = _advisory_checks(report)

    stop_rules = [
        "실시간 MDD가 mdd_limit를 하회하면 자동 중지",
        "연속 20거래일 수익률이 음수면 파라미터 재검증",
        "운영평가구간 필수 항목 FAIL 발생 시 실전 전환 금지",
        "Look-ahead/Position-lag FAIL 발생 시 즉시 배포 중단",
    ]

    if final_decision == "GO":
        operation_judgment = "운영평가구간 통과"
    elif final_decision == "NO_GO":
        operation_judgment = "운영보류"
    else:
        operation_judgment = checklist.get("operation_judgment", "-")

    return {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operation_judgment": operation_judgment,
        "evaluation_scope": checklist.get("evaluation_scope", {}),
        "overall_pass": bool(checklist.get("passed", False)),
        "final_gate_decision": final_decision,
        "domain_results": domains,
        "final_gate_criteria": final_criteria,
        "long_horizon_reference_criteria": long_horizon_reference,
        "advisory_checks": advisories,
        "expected_range": expected,
        "weakness_map": weaknesses,
        "result_summary": result_summary,
        "operating_parameters": operating,
        "stop_triggers": stop_rules,
    }


def _render_md(summary: Dict[str, Any], checklist: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# 백테스트 최종 산출결과 ({summary.get('generated_at')})")
    lines.append("")
    lines.append(f"- operation_judgment: **{summary.get('operation_judgment','-')}**")
    lines.append(f"- final_gate_decision: **{summary.get('final_gate_decision','HOLD')}**")
    scope = summary.get("evaluation_scope", {}) if isinstance(summary.get("evaluation_scope"), dict) else {}
    if scope:
        lines.append(f"- evaluation_scope: **{scope.get('label', '-')}**")
        lines.append(f"- interpretation: {scope.get('interpretation', '-')}")
    lines.append(
        f"- checklist: total={checklist.get('total',0)}, pass={checklist.get('pass_n',0)}, fail={checklist.get('fail_n',0)}, not_evaluable={checklist.get('not_evaluable_n',0)}"
    )
    lines.append("")
    rs = summary.get("result_summary", {})
    pf = rs.get("profitability", {})
    mg = rs.get("management", {})
    lines.append("## 의사결정 요약")
    lines.append("")
    lines.append(f"- 수익률 판정: **{pf.get('level','-')}**")
    lines.append(f"- 수익률 코멘트: {pf.get('message','-')}")
    for r in (pf.get("reasons", []) or [])[:5]:
        lines.append(f"  - 근거: {r}")
    lines.append(f"- 관리 판정: **{mg.get('level','-')}**")
    lines.append(f"- 관리 코멘트: {mg.get('message','-')}")
    for r in (mg.get("needed_actions", []) or [])[:4]:
        lines.append(f"  - 필요조치: {r}")

    lines.append("")

    lines.append("## 10개 검증영역 상태")
    lines.append("")
    lines.append("| 영역 | 상태 |")
    lines.append("|---|---|")
    for d in summary.get("domain_results", []):
        lines.append(f"| {d.get('domain')} | {d.get('status')} |")

    lines.append("")
    lines.append("## Final Gate")
    lines.append("")
    lines.append("| 기준 | 상태 | 근거 |")
    lines.append("|---|---|---|")
    for c in summary.get("final_gate_criteria", []):
        lines.append(f"| {c.get('name')} | {c.get('status')} | {c.get('evidence')} |")

    refs = summary.get("long_horizon_reference_criteria", []) or []
    if refs:
        lines.append("")
        lines.append("## 장기 강건성 참고")
        lines.append("")
        lines.append("| 기준 | 상태 | 최종차단 | 근거 |")
        lines.append("|---|---|---|---|")
        for r in refs:
            blocking = "Y" if bool(r.get("blocking", False)) else "N"
            lines.append(f"| {r.get('name')} | {r.get('status')} | {blocking} | {r.get('evidence')} |")

    adv = summary.get("advisory_checks", []) or []
    if adv:
        lines.append("")
        lines.append("## 권고 점검")
        lines.append("")
        lines.append("| 항목 | 상태 | 근거 |")
        lines.append("|---|---|---|")
        for a in adv:
            lines.append(f"| {a.get('name')} | {a.get('status')} | {a.get('evidence')} |")

    lines.append("")
    lines.append("## 기대치(현실화)")
    er = summary.get("expected_range", {})
    lines.append(f"- annual_return: {_fmt(er.get('annual_return'))}")
    lines.append(f"- sharpe: {_fmt(er.get('sharpe'))}")
    lines.append(f"- max_drawdown: {_fmt(er.get('max_drawdown'))}")
    lines.append(f"- win_rate: {_fmt(er.get('win_rate'))}")
    lines.append(f"- mc_mdd_95: {_fmt(er.get('mc_mdd_95'))}")
    lines.append(f"- mc_final_50: {_fmt(er.get('mc_final_50'))}")
    lines.append(f"- mdd_to_return_ratio: {_fmt(er.get('mdd_to_return_ratio'))}")
    lines.append(f"- cost_total_bps: {_fmt(er.get('cost_total_bps'))}")

    lines.append("")
    lines.append("## 약점 맵")
    for w in summary.get("weakness_map", [])[:12]:
        lines.append(
            f"- [{w.get('status')}] {w.get('item')}: {w.get('metric')} / 기준 {w.get('threshold')} / 조치 {w.get('action')}"
        )

    lines.append("")
    lines.append("## 중단 기준")
    for r in summary.get("stop_triggers", []):
        lines.append(f"- {r}")

    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Build final backtest decision output")
    ap.add_argument("--checklist-json", default=str(LOG_DIR / "backtest_validation_checklist_latest.json"))
    ap.add_argument("--report-json", default=str(LOG_DIR / "backtest_validation_latest.json"))
    ap.add_argument("--out-json", default="")
    ap.add_argument("--out-md", default="")
    args = ap.parse_args()

    checklist_path = Path(args.checklist_json)
    report_path = Path(args.report_json)
    checklist = _load_json(checklist_path)
    report = _load_json(report_path)

    if not checklist:
        raise FileNotFoundError(f"missing checklist json: {checklist_path}")
    if not report:
        raise FileNotFoundError(f"missing report json: {report_path}")

    summary = _build_summary(checklist, report)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"backtest_final_output_{stamp}.json")
    out_json_latest = LOG_DIR / "backtest_final_output_latest.json"
    out_md = Path(args.out_md) if args.out_md else (LOG_DIR / f"backtest_final_output_{stamp}.md")
    out_md_latest = LOG_DIR / "backtest_final_output_latest.md"

    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    out_json_latest.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    md_text = _render_md(summary, checklist)
    out_md.write_text(md_text, encoding="utf-8-sig")
    out_md_latest.write_text(md_text, encoding="utf-8-sig")

    _log_print(f"[BTFINAL] decision={summary.get('final_gate_decision','HOLD')} op={summary.get('operation_judgment','-')}")
    _log_print(f"[BTFINAL] json={out_json}")
    _log_print(f"[BTFINAL] md={out_md}")
    _log_print(f"[BTFINAL] latest_json={out_json_latest}")
    _log_print(f"[BTFINAL] latest_md={out_md_latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
