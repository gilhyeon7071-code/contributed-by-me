from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


TITLE_MAP = {
    "data_close_integrity": "데이터 종가 무결성",
    "data_return_finite": "데이터 수익률 유한값",
    "data_time_index_integrity": "데이터 시간축 무결성",
    "look_ahead_proxy": "Look-ahead 편향",
    "position_lag": "포지션 랙(신호 지연)",
    "strategy_parameter_validation": "전략 파라미터 검증",
    "walk_forward": "워크포워드(WFE)",
    "monte_carlo": "몬테카를로(MDD)",
    "market_regime_response": "과거 시장 대응 검증(레짐)",
    "historical_scenario_response": "과거 시장 대응 검증(시나리오)",
    "inflation_real_return": "인플레이션 반영 검증",
    "temporal_consistency": "시간적 일관성 검증",
    "psychological_tolerance": "심리적 허용치 검증",
    "outlier_concentration": "아웃라이어 집중도 검증",
    "cpcv_pbo": "CPCV/PBO",
}

ACTION_MAP = {
    "data_close_integrity": "휴장일/오류행(close<=0) 제거 후 재생성",
    "data_return_finite": "Inf/NaN 수익률 발생 원인(0분모/결측) 정리",
    "data_time_index_integrity": "중복/역순 타임스탬프 정리 후 재검증",
    "look_ahead_proxy": "signal 생성 시점과 체결 시점 분리(shift) 재검토",
    "position_lag": "position=signal.shift(1) 강제 및 예외 케이스 점검",
    "walk_forward": "파라미터 단순화/탐색축소 후 OOS 재검증",
    "monte_carlo": "포지션 사이즈 축소, 리스크 한도 및 손절 재조정",
    "cpcv_pbo": "탐색공간 축소 + 교차검증 강도 상향",
    "strategy_parameter_validation": "파라미터 범위/경계값/강건성 점검 후 재탐색",
    "market_regime_response": "상승/하락/고변동 레짐별 진입/청산 규칙 보정",
    "historical_scenario_response": "시나리오별 실패구간에 보호 규칙(익절/손절/리밸런싱) 보강",
    "inflation_real_return": "명목수익 대비 실질수익 개선(비용/회전율/현금비중 조정)",
    "temporal_consistency": "연도별 편차가 큰 구간의 규칙 과적합 축소",
    "psychological_tolerance": "허용 MDD 초과 시 포지션 크기/손실한도 하향",
    "outlier_concentration": "소수 거래/종목 의존도 완화(분산/필터 재설계)",
}

CRITICAL_GATES = {
    "data_close_integrity",
    "data_return_finite",
    "look_ahead_proxy",
    "position_lag",
    "psychological_tolerance",
}
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_NOT_EVALUABLE = "NOT_EVALUABLE"


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
        return f"corr={_fmt_num(details.get('corr'))}", f"abs(corr)<={_fmt_num(details.get('threshold', 0.2))}"
    if name == "position_lag":
        return f"mismatch_ratio={_fmt_num(details.get('mismatch_ratio'))}", "<=0.10"
    if name == "walk_forward":
        return f"median_wfe={_fmt_num(details.get('median_wfe'))}", ">=50"
    if name == "monte_carlo":
        return f"mc95_mdd={_fmt_num(details.get('mc95_mdd'))}", f">={_fmt_num(details.get('limit', -0.3))}"
    if name == "cpcv_pbo":
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
        return f"valid_regimes={vr}, worst_mdd={wm}, median_regime_sharpe={ms}", "valid_regimes>=2, worst_mdd>-0.60, median_regime_sharpe>=-0.50"
    if name == "historical_scenario_response":
        cv = _fmt_num(details.get("covered_scenarios"))
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

    keys = [k for k in details.keys()][:2]
    if not keys:
        return "-", "-"
    metric = ", ".join([f"{k}={_fmt_num(details.get(k))}" for k in keys])
    return metric, "-"

def _is_not_evaluable(metric: str) -> bool:
    return ("NaN" in metric) or ("Inf" in metric)


def _status_label(passed: bool, metric: str) -> str:
    if _is_not_evaluable(metric):
        return STATUS_NOT_EVALUABLE
    return STATUS_PASS if passed else STATUS_FAIL


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


def _render_md(data: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# 백테스트 검증 체크리스트 ({data['generated_at']})")
    lines.append("")
    lines.append(f"- overall: **{'PASS' if data['passed'] else 'FAIL'}**")
    lines.append(f"- operation_judgment: **{data.get('operation_judgment','-')}**")
    lines.append(
        f"- total: **{data['total']}**, pass: **{data['pass_n']}**, fail: **{data['fail_n']}**, not_evaluable: **{data['not_evaluable_n']}**"
    )
    lines.append(f"- source_json: `{data['source_json']}`")
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
        status = _status_label(passed, metric)

        row = {
            "name": name,
            "item": TITLE_MAP.get(name, name),
            "status": status,
            "passed": status == STATUS_PASS,
            "issue": _issue_tag(status),
            "metric": metric,
            "threshold": threshold,
            "action": ACTION_MAP.get(name, "세부지표 기반 보정 후 재검증"),
        }
        items.append(row)

    pass_n = sum(1 for x in items if x["status"] == STATUS_PASS)
    fail_n = sum(1 for x in items if x["status"] == STATUS_FAIL)
    not_eval_n = sum(1 for x in items if x["status"] == STATUS_NOT_EVALUABLE)
    total = len(items)
    operation_judgment = _operation_judgment(items)

    return {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_json": str(source_json),
        "passed": (fail_n == 0 and not_eval_n == 0),
        "total": total,
        "pass_n": pass_n,
        "fail_n": fail_n,
        "not_evaluable_n": not_eval_n,
        "operation_judgment": operation_judgment,
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

    print(
        f"[CHK] overall={'PASS' if checklist['passed'] else 'FAIL'} pass={checklist['pass_n']} fail={checklist['fail_n']} not_evaluable={checklist['not_evaluable_n']}"
    )
    print(f"[CHK] json={out_json}")
    print(f"[CHK] md={out_md}")
    print(f"[CHK] csv={out_csv}")
    print(f"[CHK] latest_json={out_json_latest}")
    print(f"[CHK] latest_md={out_md_latest}")
    print(f"[CHK] latest_csv={out_csv_latest}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())









