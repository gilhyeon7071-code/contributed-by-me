from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List
import logging


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"


STATUS_LABEL = {
    "PASS": "통과",
    "FAIL": "실패",
    "NOT_EVALUABLE": "판정불가",
}

STATUS_CLASS = {
    "PASS": "pass",
    "FAIL": "fail",
    "NOT_EVALUABLE": "warn",
}

REVIEW_STATUS_LABEL = {
    "blocked": "보류",
    "monitor": "유지관찰",
    "candidate": "수정후보",
}

REVIEW_STATUS_CLASS = {
    "blocked": "warn",
    "monitor": "pass",
    "candidate": "fail",
}

MODULE_STATUS_LABEL = {
    "READY": "준비완료",
    "IN_PROGRESS": "진행중",
    "BLOCKED": "보완필요",
}

MODULE_STATUS_CLASS = {
    "READY": "pass",
    "IN_PROGRESS": "warn",
    "BLOCKED": "fail",
}

FOCUS_LABELS = {
    "ALL": "전체",
    "DATA": "데이터",
    "BIAS": "편향/타이밍",
    "PARAM": "파라미터",
    "REGIME": "레짐/시나리오",
    "RISK": "리스크",
    "STABILITY": "통계/재현성",
}

ITEM_FOCUS_MAP = {
    "data_close_integrity": "DATA",
    "data_return_finite": "DATA",
    "data_time_index_integrity": "DATA",
    "look_ahead_proxy": "BIAS",
    "position_lag": "BIAS",
    "strategy_parameter_validation": "PARAM",
    "walk_forward": "PARAM",
    "cpcv_pbo": "PARAM",
    "deflated_sharpe_ratio": "STABILITY",
    "market_regime_response": "REGIME",
    "historical_scenario_response": "REGIME",
    "monte_carlo": "RISK",
    "inflation_real_return": "RISK",
    "psychological_tolerance": "RISK",
    "outlier_concentration": "RISK",
    "temporal_consistency": "STABILITY",
}

PHASE_MAP = {
    1: {
        "title": "1단계: 로직 안정성 검증",
        "names": [
            "data_close_integrity",
            "data_return_finite",
            "data_time_index_integrity",
            "position_lag",
        ],
    },
    2: {
        "title": "2단계: 편향 및 파라미터 검증",
        "names": [
            "look_ahead_proxy",
            "strategy_parameter_validation",
            "cpcv_pbo",
            "deflated_sharpe_ratio",
        ],
    },
    3: {
        "title": "3단계: 교차검증/OOS/레짐",
        "names": [
            "walk_forward",
            "market_regime_response",
            "historical_scenario_response",
            "temporal_consistency",
        ],
    },
    4: {
        "title": "4단계: 리스크/실행 현실성",
        "names": [
            "monte_carlo",
            "inflation_real_return",
            "psychological_tolerance",
            "outlier_concentration",
        ],
    },
}




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


def _escape(s: Any) -> str:
    text = "" if s is None else str(s)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _fmt(v: Any, digits: int = 4) -> str:
    try:
        f = float(v)
    except Exception:
        return "-"
    if str(f) in {"nan", "inf", "-inf"}:
        return "-"
    return f"{f:.{digits}g}"


def _fmt_pct(v: Any, scale: float = 100.0, digits: int = 1) -> str:
    try:
        f = float(v)
    except Exception:
        return "-"
    if str(f) in {"nan", "inf", "-inf"}:
        return "-"
    return f"{f * scale:.{digits}f}%"


def _fmt_int(v: Any) -> str:
    try:
        f = float(v)
    except Exception:
        return "-"
    if str(f) in {"nan", "inf", "-inf"}:
        return "-"
    return f"{int(round(f)):,}"


def _fmt_money(v: Any) -> str:
    try:
        f = float(v)
    except Exception:
        return "-"
    if str(f) in {"nan", "inf", "-inf"}:
        return "-"
    return f"{f:,.0f}"


def _fmt_pct_rule(v: Any, scale: float = 100.0, digits: int = 2, signed: bool = True) -> str:
    try:
        f = float(v)
    except Exception:
        return "-"
    if str(f) in {"nan", "inf", "-inf"}:
        return "-"
    sign = "+" if signed else ""
    return f"{f * scale:{sign}.{digits}f}%"


def _fmt_code(v: Any) -> str:
    text = "".join(ch for ch in str(v or "") if ch.isdigit())
    if not text:
        return "-"
    return text[-6:].zfill(6)


def _looks_like_ratio(v: Any) -> bool:
    try:
        f = float(v)
    except Exception:
        return False
    if str(f) in {"nan", "inf", "-inf"}:
        return False
    return abs(f) <= 1.0


def _sample_cell_html(col: str, value: Any) -> str:
    key = str(col or "").strip().lower()
    raw = value if value is not None else "-"

    if key == "code":
        return f"<td class='code'>{_escape(_fmt_code(raw))}</td>"
    if key in {"entry_date", "exit_date", "date", "window_start", "window_end", "oper_start_ymd"}:
        return f"<td class='time'>{_escape(raw)}</td>"
    if key in {"trade_id", "ret_col_live", "ret_col_bt"}:
        return f"<td class='num'>{_escape(raw)}</td>"
    if key in {"ok", "passed", "execute", "ready", "mock"}:
        return f"<td>{_escape(_user_text(raw))}</td>"
    if key in {"returncode", "pass_n", "fail_n", "not_evaluable_n", "iterations_done"}:
        return f"<td class='num'>{_escape(_user_text(raw))}</td>"
    if key in {"actual", "expected", "error", "note"}:
        return f"<td>{_escape(_user_text(raw))}</td>"
    if key in {"live_n", "bt_n", "rows", "n_symbols", "source_file_count"}:
        return f"<td class='num'>{_escape(_fmt_int(raw))}</td>"
    if key in {"pnl_krw", "price", "fill_price", "entry_price", "exit_price", "qty", "fill_qty"}:
        return f"<td class='num'>{_escape(_fmt_money(raw))}</td>"
    if key in {"ret_sample", "pnl_pct", "ret", "gross_ret", "net_ret", "annual_return", "max_drawdown"} and _looks_like_ratio(raw):
        return f"<td class='num'>{_escape(_fmt_pct_rule(raw, digits=2, signed=True))}</td>"
    return f"<td>{_escape(raw)}</td>"


def _source_label(key: str) -> str:
    mapping = {
        "checklist_json": "체크리스트 기준",
        "market_json": "시장 OHLC",
        "symbol_panel_json": "종목 패널",
        "rate_json": "금리 시계열",
        "paper_parameter_review_json": "paper 파라미터 리뷰",
        "e2e": "E2E 결과",
        "fault": "장애주입 결과",
        "canary": "Canary 결과",
        "e2e_snapshot": "E2E 요약",
        "canary_snapshot": "Canary 요약",
        "fault_snapshot": "장애주입 요약",
        "alignment_snapshot": "정렬 비교 요약",
    }
    return mapping.get(str(key or ""), str(key or "-"))


def _user_text(value: Any) -> str:
    text = str(value if value is not None else "-")
    replacements = [
        ("source_json", "기준 산출물"),
        ("generated_at=", "생성 시각="),
        ("next=", "다음 조치="),
        ("blocker=", "대기 사유="),
        ("status=", "상태="),
        ("ready=", "준비 상태="),
        ("ok=", "정상="),
        ("pass_n=", "통과 수="),
        ("fail_n=", "실패 수="),
        ("not_evaluable_n=", "판정불가 수="),
        ("returncode=", "종료코드="),
        ("age_days=", "경과일="),
        ("execute=", "실주문 실행="),
        ("mode=", "실행 모드="),
        ("mock=", "모의 실행="),
        ("iter=", "반복 수="),
        ("trades_used=", "표본 거래 수="),
        ("alignment_ready=", "정렬 준비="),
        ("optimize_gate_ok=", "품질 게이트="),
        ("abs_diff=", "괴리 차이="),
        ("risk_off=", "리스크오프="),
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    text = re.sub(r"\bPASS\b", "통과", text)
    text = re.sub(r"\bFAIL\b", "실패", text)
    text = re.sub(r"\bNOT_EVALUABLE\b", "판정불가", text)
    text = re.sub(r"\bTrue\b", "예", text)
    text = re.sub(r"\bFalse\b", "아니오", text)
    return text


def _status_chip(status: str) -> str:
    cls = STATUS_CLASS.get(status, "warn")
    label = STATUS_LABEL.get(status, status)
    return f"<span class='chip chip-{cls}'>{_escape(label)}</span>"


def _kv_html(data: Dict[str, Any]) -> str:
    if not data:
        return "<p>-</p>"
    lines = []
    for key, value in data.items():
        lines.append(f"<div><b>{_escape(key)}</b>: {_escape(value)}</div>")
    return "".join(lines)


def _is_probable_path(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if text.startswith(("http://", "https://", "file:///")):
        return True
    return (":\\" in text or text.startswith("\\\\")) and ("." in Path(text).name)


def _path_to_href(path_text: str) -> str:
    text = str(path_text or "").strip()
    if text.startswith(("http://", "https://", "file:///")):
        return text
    normalized = text.replace("\\", "/")
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    return "file://" + urllib.parse.quote(normalized, safe="/:()_-.,")


def _render_path_link(path_text: Any, label: str | None = None) -> str:
    text = str(path_text or "").strip()
    if not text:
        return "-"
    shown = label or Path(text).name or text
    href = _path_to_href(text)
    return f"<a href='{_escape(href)}' target='_blank' rel='noreferrer'>{_escape(shown)}</a>"


def _collect_source_links(sources: Dict[str, Any]) -> List[Dict[str, str]]:
    links: List[Dict[str, str]] = []
    for key, value in (sources or {}).items():
        if isinstance(value, str) and _is_probable_path(value):
            links.append({"label": str(key), "path": value})
    return links


def _render_source_links(title: str, sources: Dict[str, Any]) -> str:
    links = _collect_source_links(sources)
    if not links:
        return (
            "<section class='panel-card'>"
            f"<h4>{_escape(title)}</h4>"
            "<p>바로 열 수 있는 원본 로그 경로가 없습니다.</p>"
            "</section>"
        )

    items = "".join(
        [
            f"<li>{_escape(_source_label(link['label']))}: {_render_path_link(link['path'])}</li>"
            for link in links
        ]
    )
    return (
        "<section class='panel-card'>"
        f"<h4>{_escape(title)}</h4>"
        "<ul class='plain'>"
        f"{items}"
        "</ul>"
        "</section>"
    )


def _focus_for_item_name(name: str) -> str:
    return ITEM_FOCUS_MAP.get(str(name or "").strip(), "STABILITY")


def _focus_for_domain_name(name: str) -> str:
    text = str(name or "")
    if "로직" in text or "데이터" in text:
        return "DATA"
    if "편향" in text:
        return "BIAS"
    if "파라미터" in text or "교차검증" in text or "OOS" in text:
        return "PARAM"
    if "레짐" in text or "시나리오" in text:
        return "REGIME"
    if "몬테카를로" in text or "실행비용" in text or "위기" in text:
        return "RISK"
    return "STABILITY"


def _focus_for_criterion_name(name: str) -> str:
    text = str(name or "")
    if "PBO" in text or "WFE" in text or "plateau" in text:
        return "PARAM"
    if "레짐" in text or "시나리오" in text:
        return "REGIME"
    if "MDD" in text:
        return "RISK"
    if "DSR" in text or "독립 거래" in text:
        return "STABILITY"
    return "BIAS"


def _render_focus_options() -> str:
    return "".join([f"<option value='{k}'>{_escape(v)}</option>" for k, v in FOCUS_LABELS.items()])


def _phase_rows(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    claimed = set()

    for pno, obj in PHASE_MAP.items():
        for name in obj["names"]:
            hit = next((x for x in items if x.get("name") == name), None)
            if hit is not None:
                rows.append({"phase": pno, **hit})
                claimed.add(name)

    for x in items:
        if x.get("name") not in claimed:
            rows.append({"phase": 5, **x})
    return rows


def _count_text(stage: Dict[str, Any]) -> str:
    counts = stage.get("counts", {}) or {}
    return (
        f"PASS {int(counts.get('pass_n', 0))}"
        f" / FAIL {int(counts.get('fail_n', 0))}"
        f" / NE {int(counts.get('not_evaluable_n', 0))}"
    )


def _render_final_outputs(final_output: Dict[str, Any], bt_report: Dict[str, Any]) -> str:
    if not final_output:
        return "<div class='simple-card'><h4>최종 산출결과물</h4><p>최종 JSON이 없습니다.</p></div>"

    expected = final_output.get("expected_range", {}) or {}
    integ = ((bt_report.get("artifacts") or {}).get("integration") or {})
    params = integ.get("params", {}) or {}

    return (
        "<div class='simple-grid'>"
        f"<div class='simple-card'><h4>Go/No-Go</h4><p>{_escape(final_output.get('final_gate_decision', '-'))}</p></div>"
        f"<div class='simple-card'><h4>운영판정</h4><p>{_escape(final_output.get('operation_judgment', '-'))}</p></div>"
        f"<div class='simple-card'><h4>기대치</h4><p>AnnRet={_fmt_pct_rule(expected.get('annual_return'), digits=2, signed=True)}<br>Sharpe={_fmt(expected.get('sharpe'))}<br>MDD={_fmt_pct_rule(expected.get('max_drawdown'), digits=2, signed=True)}</p></div>"
        f"<div class='simple-card'><h4>핵심 파라미터</h4><p>{_escape(params)}</p></div>"
        "</div>"
    )


def _render_backtest_overview(
    bt_report: Dict[str, Any],
    final_output: Dict[str, Any],
    market_data: Dict[str, Any],
    symbol_panel: Dict[str, Any],
    rate_series: Dict[str, Any],
) -> str:
    integ = ((bt_report.get("artifacts") or {}).get("integration") or {})
    operating = final_output.get("operating_parameters", {}) or {}
    params = operating.get("params") or integ.get("params") or {}
    grid_spec = integ.get("grid_spec", {}) or {}
    cost_model = operating.get("cost_model") or integ.get("cost_model") or {}
    market_meta = integ.get("market_meta", {}) or {}

    return (
        "<div class='split'>"
        "<section class='panel-card side'>"
        "<h4>무엇을 백테스트했나</h4>"
        "<p>과거 일봉 이력에 전략 규칙과 비용모델을 적용해, 성과와 리스크가 재현되는지 검증합니다.</p>"
        f"<p><b>전략 소스:</b> {_escape(operating.get('strategy_source') or integ.get('strategy_source') or '-')}</p>"
        f"<p><b>백테스트 엔진:</b> {_escape(operating.get('backtest_source') or integ.get('backtest_source') or '-')}</p>"
        f"<p><b>운영 파라미터:</b> {_escape(params)}</p>"
        f"<p><b>파라미터 탐색 수:</b> {_escape(integ.get('n_param_grid', '-'))}</p>"
        f"<p><b>탐색 범위:</b> {_escape(grid_spec)}</p>"
        f"<p><b>비용 모델:</b> {_escape(cost_model)}</p>"
        f"<p><b>허용 MDD 한도:</b> {_escape(operating.get('max_tolerable_mdd', integ.get('max_tolerable_mdd', '-')))}</p>"
        "</section>"
        "<section class='panel-card'>"
        "<h4>어떤 과거이력을 사용했나</h4>"
        "<div class='simple-grid'>"
        f"<div class='simple-card'><h4>시장 OHLC</h4><p>기간: {_escape(market_data.get('date_min', '-'))} ~ {_escape(market_data.get('date_max', '-'))}<br>행 수: {_escape(_fmt_int(market_data.get('rows', market_meta.get('output_rows', '-'))))}<br>원천 파일: {_escape(_fmt_int(market_data.get('source_file_count', '-')))}개</p></div>"
        f"<div class='simple-card'><h4>종목 패널</h4><p>심볼 수: {_escape(_fmt_int(symbol_panel.get('n_symbols', '-')))}<br>섹터 커버리지: {_fmt_pct_rule(symbol_panel.get('sector_coverage'), digits=2, signed=False)}<br>시총 커버리지: {_fmt_pct_rule(symbol_panel.get('market_cap_coverage'), digits=2, signed=False)}</p></div>"
        f"<div class='simple-card'><h4>금리 시계열</h4><p>기간: {_escape(rate_series.get('date_min', '-'))} ~ {_escape(rate_series.get('date_max', '-'))}<br>행 수: {_escape(_fmt_int(rate_series.get('rows', '-')))}<br>소스: {_escape(rate_series.get('source', '-'))}</p></div>"
        f"<div class='simple-card'><h4>컬럼 기준</h4><p>date={_escape((market_meta.get('used_column_map') or {}).get('date', '-'))}<br>close={_escape((market_meta.get('used_column_map') or {}).get('close', '-'))}<br>rows={_escape(_fmt_int(market_meta.get('source_rows', '-')))}</p></div>"
        "</div>"
        "</section>"
        "</div>"
    )


def _render_backtest_result_explainer(final_output: Dict[str, Any]) -> str:
    summary = final_output.get("result_summary", {}) or {}
    profitability = summary.get("profitability", {}) or {}
    management = summary.get("management", {}) or {}
    stop_triggers = final_output.get("stop_triggers", []) or []
    reasons = profitability.get("reasons", []) or []
    actions = management.get("needed_actions", []) or []
    final_gate = final_output.get("final_gate_decision", "-")
    reason_html = "".join([f"<li>{_escape(x)}</li>" for x in reasons[:5]]) if reasons else "<li>-</li>"
    stop_html = "".join([f"<li>{_escape(x)}</li>" for x in stop_triggers]) if stop_triggers else "<li>-</li>"
    action_html = "".join([f"<li>{_escape(x)}</li>" for x in actions]) if actions else "<li>현재 추가 조치 없음</li>"

    return (
        "<div class='split'>"
        "<section class='panel-card side'>"
        "<h4>왜 이런 최종판정이 나왔나</h4>"
        f"<p><b>최종판정:</b> {_escape(final_gate)}</p>"
        f"<p><b>수익성 해석:</b> {_escape(profitability.get('message', '-'))}</p>"
        f"<p><b>운영 해석:</b> {_escape(management.get('message', '-'))}</p>"
        f"<ul class='plain'>{reason_html}</ul>"
        "</section>"
        "<section class='panel-card'>"
        "<h4>중지/보류 조건</h4>"
        f"<ul class='plain'>{stop_html}</ul>"
        "<h4>추가 관리 포인트</h4>"
        f"<ul class='plain'>{action_html}</ul>"
        "</section>"
        "</div>"
    )


def _criterion_source_names(name: str) -> List[str]:
    text = str(name or "")
    pairs = [
        ("PBO", ["cpcv_pbo"]),
        ("plateau", ["strategy_parameter_validation"]),
        ("파라미터", ["strategy_parameter_validation"]),
        ("WFE", ["walk_forward"]),
        ("워크포워드", ["walk_forward"]),
        ("레짐", ["market_regime_response"]),
        ("시나리오", ["historical_scenario_response"]),
        ("MC", ["monte_carlo"]),
        ("MDD", ["monte_carlo", "psychological_tolerance"]),
        ("심리", ["psychological_tolerance"]),
        ("DSR", []),
        ("독립 거래", ["outlier_concentration"]),
    ]
    for needle, names in pairs:
        if needle in text:
            return names
    return []


def _find_check_item(items: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
    return next((item for item in items if str(item.get("name", "")) == name), {})


def _render_backtest_provenance(
    checklist: Dict[str, Any],
    bt_report: Dict[str, Any],
    final_output: Dict[str, Any],
    market_data: Dict[str, Any],
    symbol_panel: Dict[str, Any],
    rate_series: Dict[str, Any],
) -> str:
    artifacts = (bt_report.get("artifacts") or {})
    artifact_names = ", ".join(sorted(str(name) for name in artifacts.keys())) or "-"
    checklist_time = checklist.get("generated_at", "-")
    final_time = final_output.get("generated_at", "-")
    source_json = checklist.get("source_json", "-")
    market_range = f"{market_data.get('date_min', '-')} ~ {market_data.get('date_max', '-')}"
    rate_range = f"{rate_series.get('date_min', '-')} ~ {rate_series.get('date_max', '-')}"
    body = (
        "<section class='panel-card'>"
        "<h4>산출물 출처와 생성 시각</h4>"
        "<p>지금 보는 판정이 어떤 산출물에서 왔는지 먼저 확인할 수 있습니다.</p>"
        "<div class='simple-grid'>"
        f"<div class='simple-card'><h4>체크리스트 기준</h4><p>생성 시각: {_escape(checklist_time)}<br>기준 산출물: {_render_path_link(source_json, 'backtest_validation_latest.json') if _is_probable_path(source_json) else _escape(source_json)}</p></div>"
        f"<div class='simple-card'><h4>최종판정 기준</h4><p>생성 시각: {_escape(final_time)}<br>최종판정: {_escape(final_output.get('final_gate_decision', '-'))}</p></div>"
        f"<div class='simple-card'><h4>검증 산출물</h4><p>artifact 수: {_escape(len(artifacts))}<br>{_escape(artifact_names)}</p></div>"
        f"<div class='simple-card'><h4>과거이력 범위</h4><p>시장 데이터: {_escape(market_range)}<br>금리 데이터: {_escape(rate_range)}<br>종목 수: {_escape(_fmt_int(symbol_panel.get('n_symbols', '-')))}</p></div>"
        "</div>"
        "</section>"
    )
    source_links = {
        "checklist_json": source_json,
        "market_json": str(LOG_DIR / "backtest_market_ohlc_latest.json"),
        "symbol_panel_json": str(LOG_DIR / "backtest_symbol_panel_latest.json"),
        "rate_json": str(LOG_DIR / "rate_series_latest.json"),
    }
    return body + _render_source_links("원본 로그 바로가기", source_links)


def _render_criterion_trace(final_output: Dict[str, Any], items: List[Dict[str, Any]]) -> str:
    criteria = final_output.get("final_gate_criteria", []) or []
    if not criteria:
        return "<section class='panel-card'><h4>최종판정 기준의 원본 검증 추적</h4><p>추적할 기준 데이터가 없습니다.</p></section>"

    rows: List[str] = []
    for criterion in criteria:
        name = str(criterion.get("name", "-"))
        status = str(criterion.get("status", "NOT_EVALUABLE"))
        focus = _focus_for_criterion_name(name)
        source_names = _criterion_source_names(name)
        linked = [_find_check_item(items, source_name) for source_name in source_names]
        linked = [row for row in linked if row]
        if not linked:
            rows.append(
                "<tr class='bt-trace-row bt-focus-item'"
                f" data-focus='{_escape(focus)}'"
                f" data-status='{_escape(status)}'"
                f" data-text='{_escape((name + ' ' + str(criterion.get('evidence', '-'))).lower())}'>"
                f"<td class='item'>{_escape(name)}</td>"
                f"<td>{_status_chip(status)}</td>"
                f"<td>{_escape(_user_text(criterion.get('evidence', '-')))}</td>"
                "<td>-</td><td>-</td><td>-</td><td>-</td>"
                "</tr>"
            )
            continue
        for idx, linked_item in enumerate(linked):
            rows.append(
                "<tr class='bt-trace-row bt-focus-item'"
                f" data-focus='{_escape(focus)}'"
                f" data-status='{_escape(status)}'"
                f" data-text='{_escape((name + ' ' + str(criterion.get('evidence', '-')) + ' ' + str(linked_item.get('item', '')) + ' ' + str(linked_item.get('metric', '')) + ' ' + str(linked_item.get('threshold', ''))).lower())}'>"
                f"<td class='item'>{_escape(name if idx == 0 else '-')}</td>"
                f"<td>{_status_chip(status) if idx == 0 else ''}</td>"
                f"<td>{_escape(_user_text(criterion.get('evidence', '-')) if idx == 0 else '')}</td>"
                f"<td>{_escape(linked_item.get('item', linked_item.get('name', '-')))}</td>"
                f"<td>{_escape(_user_text(linked_item.get('metric', '-')))}</td>"
                f"<td>{_escape(_user_text(linked_item.get('threshold', '-')))}</td>"
                f"<td>{_escape(linked_item.get('action', '-'))}</td>"
                "</tr>"
            )
    return (
        "<section class='panel-card'>"
        "<h4>최종판정 기준의 원본 검증 추적</h4>"
        "<p>최종 기준이 어떤 세부 검증 항목과 연결되는지, 실제값과 기준값을 한 화면에서 확인할 수 있습니다.</p>"
        "<div class='table-wrap'><table>"
        "<thead><tr><th>최종 기준</th><th>상태</th><th>최종 근거</th><th>연결 검증항목</th><th>실제값</th><th>기준값</th><th>권고조치</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
        "</section>"
    )


def _render_backtest_profile_panel() -> str:
    return (
        "<section class='panel-card'>"
        "<h4>화면 해석 프로필</h4>"
        "<p>이 프로필은 7.백테스트 탭의 필터와 보기 방식만 저장합니다. 파이프라인 판정값이나 최적화 기준은 바뀌지 않습니다.</p>"
        "<div class='filter-row'>"
        "<label>프로필<select id='bt-profile-select'><option value='builtin:all'>전체 보기</option><option value='builtin:data'>데이터 품질</option><option value='builtin:param'>파라미터 검증</option><option value='builtin:risk'>리스크 점검</option><option value='builtin:regime'>레짐/시나리오</option><option value='builtin:stability'>통계/재현성</option></select></label>"
        "<label>새 프로필 이름<input id='bt-profile-name' type='text' placeholder='예: 내가 보는 기준'></label>"
        "</div>"
        "<div class='ctrl'>"
        "<button class='act-btn' id='bt-profile-apply'>불러오기</button>"
        "<button class='act-btn' id='bt-profile-save'>현재 화면 저장</button>"
        "<button class='act-btn' id='bt-profile-reset'>전체 기본값</button>"
        "<button class='act-btn danger' id='bt-profile-delete'>사용자 프로필 삭제</button>"
        "</div>"
        "<p class='hint'>저장되는 항목: 관심영역, 상태, 단계, 검색어. 브라우저 로컬에만 저장되며 다른 PC나 검증 배치에는 적용되지 않습니다.</p>"
        "</section>"
    )


def _render_final_gate_criteria(final_output: Dict[str, Any]) -> str:
    criteria = final_output.get("final_gate_criteria", []) or []
    if not criteria:
        return "<section class='panel-card'><h4>최종판정 기준</h4><p>기준 데이터가 없습니다.</p></section>"

    rows = []
    for row in criteria:
        status = str(row.get("status", "NOT_EVALUABLE"))
        focus = _focus_for_criterion_name(str(row.get("name", "")))
        rows.append(
            "<tr class='bt-focus-item bt-criteria-row'"
            f" data-focus='{_escape(focus)}'"
            f" data-status='{_escape(status)}'"
            f" data-text='{_escape((str(row.get('name', '')) + ' ' + str(row.get('evidence', ''))).lower())}'>"
            f"<td class='item'>{_escape(row.get('name', '-'))}</td>"
            f"<td>{_status_chip(status)}</td>"
            f"<td>{_escape(_user_text(row.get('evidence', '-')))}</td>"
            "</tr>"
        )
    return (
        "<section class='panel-card'>"
        "<h4>최종판정 기준</h4>"
        "<p>최종값 하나만 보는 대신, 실제로 어떤 기준을 통과했는지 바로 확인할 수 있습니다.</p>"
        "<div class='filter-row'>"
        f"<label>관심영역<select class='bt-focus-select'>{_render_focus_options()}</select></label>"
        "<label>상태<select id='bt-criteria-status'><option value='ALL'>전체</option><option value='PASS'>통과</option><option value='FAIL'>실패</option><option value='NOT_EVALUABLE'>판정불가</option></select></label>"
        "<label>검색<input id='bt-criteria-search' type='text' placeholder='기준명, 근거 검색'></label>"
        "</div>"
        "<div class='table-wrap'><table>"
        "<thead><tr><th>기준</th><th>상태</th><th>근거</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
        "</section>"
    )


def _render_domain_results(final_output: Dict[str, Any]) -> str:
    domain_results = final_output.get("domain_results", []) or []
    if not domain_results:
        return "<section class='panel-card'><h4>도메인별 결과</h4><p>도메인 요약이 없습니다.</p></section>"

    cards = []
    for domain in domain_results:
        checks = domain.get("checks", []) or []
        preview = "".join(
            [f"<li>{_escape(ch.get('name', '-'))}: {_escape(_user_text(ch.get('metric', '-')))}</li>" for ch in checks[:3]]
        ) or "<li>-</li>"
        status = str(domain.get("status", "NOT_EVALUABLE"))
        focus = _focus_for_domain_name(str(domain.get("domain", "")))
        cards.append(
            "<section class='simple-card bt-focus-item bt-domain-card'"
            f" data-focus='{_escape(focus)}'"
            f" data-status='{_escape(status)}'"
            f" data-text='{_escape((str(domain.get('domain', '')) + ' ' + ' '.join(str(ch.get('name', '')) for ch in checks)).lower())}'>"
            f"<h4>{_escape(domain.get('domain', '-'))}</h4>"
            f"<p>상태: {_escape(status)}<br>체크 수: {_escape(len(checks))}</p>"
            f"<ul class='plain'>{preview}</ul>"
            "</section>"
        )
    return "<section class='panel-card'><h4>도메인별 결과</h4><div class='simple-grid'>" + "".join(cards) + "</div></section>"


def _render_backtest_detail_table(items: List[Dict[str, Any]]) -> str:
    rows = _phase_rows(items)
    phases = {k: v["title"] for k, v in PHASE_MAP.items()}
    phases[5] = "5단계: 기타/확장 항목"
    phase_options = "".join(
        [f"<option value='{_escape(phases[p])}'>{_escape(phases[p])}</option>" for p in sorted(phases.keys())]
    )
    tr: List[str] = []
    for r in rows:
        status = str(r.get("status", "NOT_EVALUABLE"))
        cls = STATUS_CLASS.get(status, "warn")
        phase_label = phases.get(int(r.get("phase", 5)), "기타")
        focus = _focus_for_item_name(str(r.get("name", "")))
        text_blob = " ".join(
            [
                str(r.get("item", "")),
                str(r.get("metric", "")),
                str(r.get("threshold", "")),
                str(r.get("issue", "")),
                str(r.get("action", "")),
            ]
        ).lower()
        tr.append(
            "<tr class='bt-detail-row bt-focus-item'"
            f" data-status='{_escape(status)}'"
            f" data-phase='{_escape(phase_label)}'"
            f" data-focus='{_escape(focus)}'"
            f" data-text='{_escape(text_blob)}'>"
            f"<td>{_escape(phase_label)}</td>"
            f"<td class='item'>{_escape(r.get('item', '-'))}</td>"
            f"<td><span class='chip chip-{cls}'>{_escape(STATUS_LABEL.get(status, status))}</span></td>"
            f"<td>{_escape(_user_text(r.get('metric', '-')))}</td>"
            f"<td>{_escape(_user_text(r.get('threshold', '-')))}</td>"
            f"<td>{_escape(r.get('issue', '-'))}</td>"
            f"<td>{_escape(r.get('action', '-'))}</td>"
            "</tr>"
        )
    return (
        "<section class='panel-card'>"
        "<h4>세부 검증 항목</h4>"
        "<p>원하는 상태나 단계만 골라서 볼 수 있습니다.</p>"
        "<div class='filter-row'>"
        f"<label>관심영역<select class='bt-focus-select'>{_render_focus_options()}</select></label>"
        "<label>상태<select id='bt-filter-status'><option value='ALL'>전체</option><option value='PASS'>통과</option><option value='FAIL'>실패</option><option value='NOT_EVALUABLE'>판정불가</option></select></label>"
        f"<label>단계<select id='bt-filter-phase'><option value='ALL'>전체</option>{phase_options}</select></label>"
        "<label>검색<input id='bt-filter-search' type='text' placeholder='항목명, 실제값, 기준 검색'></label>"
        "</div>"
        "<div class='table-wrap'><table>"
        "<thead><tr><th>단계</th><th>검증 항목</th><th>상태</th><th>실제값</th><th>기준</th><th>문제표시</th><th>권고조치</th></tr></thead>"
        f"<tbody>{''.join(tr)}</tbody></table></div>"
        "</section>"
    )


def _render_parameter_search(bt_report: Dict[str, Any], final_output: Dict[str, Any]) -> str:
    spv = (((bt_report.get("artifacts") or {}).get("strategy_parameter_validation") or {}))
    grid_eval = spv.get("grid_eval", []) or []
    if not grid_eval:
        return "<section class='panel-card bt-focus-item' data-focus='PARAM' data-status='NOT_EVALUABLE' data-text='parameter search missing'><h4>파라미터 탐색 결과</h4><p>탐색 결과가 없습니다.</p></section>"

    selected_params = ((final_output.get("operating_parameters") or {}).get("params") or {})
    ranked = sorted(
        [row for row in grid_eval if isinstance(row, dict)],
        key=lambda row: float(row.get("sharpe", -1e18)),
        reverse=True,
    )
    selected_sharpe = None
    for row in ranked:
        if (row.get("params") or {}) == selected_params:
            try:
                selected_sharpe = float(row.get("sharpe"))
            except Exception:
                selected_sharpe = None
            break
    best_sharpe = None
    if ranked:
        try:
            best_sharpe = float(ranked[0].get("sharpe"))
        except Exception:
            best_sharpe = None
    gap = None
    if best_sharpe is not None and selected_sharpe is not None:
        gap = best_sharpe - selected_sharpe

    rows = []
    for idx, row in enumerate(ranked[:8], start=1):
        params = row.get("params") or {}
        is_selected = params == selected_params
        rows.append(
            "<tr>"
            f"<td>{idx}</td>"
            f"<td>{'현재선택' if is_selected else '-'}</td>"
            f"<td>{_escape(params)}</td>"
            f"<td>{_escape(_fmt(row.get('sharpe')))}</td>"
            "</tr>"
        )
    return (
        "<section class='panel-card bt-focus-item' data-focus='PARAM' data-status='PASS' data-text='parameter search grid sharpe'>"
        "<h4>파라미터 탐색 결과</h4>"
        f"<p>현재 선택 파라미터: {_escape(selected_params)}<br>현재 샤프: {_escape(_fmt(selected_sharpe))} / 최고 샤프: {_escape(_fmt(best_sharpe))} / 차이: {_escape(_fmt(gap))}</p>"
        "<div class='table-wrap'><table>"
        "<thead><tr><th>순위</th><th>현재 사용</th><th>파라미터</th><th>샤프</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
        "</section>"
    )


def _render_walk_forward_detail(bt_report: Dict[str, Any]) -> str:
    wf_rows = (((bt_report.get("artifacts") or {}).get("walk_forward") or []))
    if not wf_rows:
        return "<section class='panel-card bt-focus-item' data-focus='PARAM' data-status='NOT_EVALUABLE' data-text='walk forward missing'><h4>워크포워드 상세</h4><p>워크포워드 데이터가 없습니다.</p></section>"

    valid_rows = [row for row in wf_rows if isinstance(row, dict) and bool(row.get("wfe_valid", False))]
    positive_oos_n = sum(1 for row in valid_rows if float(row.get("oos_sharpe", 0.0)) >= 0.0)
    table_rows = []
    for row in valid_rows[:8]:
        table_rows.append(
            "<tr>"
            f"<td>{_escape(row.get('fold', '-'))}</td>"
            f"<td>{_escape(_fmt(row.get('is_sharpe')))}</td>"
            f"<td>{_escape(_fmt(row.get('oos_sharpe')))}</td>"
            f"<td>{_escape(_fmt(row.get('wfe')))}</td>"
            f"<td>{_escape(row.get('best_params', '-'))}</td>"
            "</tr>"
        )
    median_wfe = "-"
    try:
        values = [float(row.get("wfe")) for row in valid_rows]
        values.sort()
        if values:
            mid = len(values) // 2
            median_wfe = _fmt(values[mid] if len(values) % 2 == 1 else (values[mid - 1] + values[mid]) / 2.0)
    except Exception:
        median_wfe = "-"
    return (
        "<section class='panel-card bt-focus-item' data-focus='PARAM' data-status='PASS' data-text='walk forward oos wfe folds'>"
        "<h4>워크포워드 상세</h4>"
        f"<p>유효 폴드: {_escape(len(valid_rows))} / OOS 샤프 양수 폴드: {_escape(positive_oos_n)} / 중앙 WFE: {_escape(median_wfe)}</p>"
        "<div class='table-wrap'><table>"
        "<thead><tr><th>폴드</th><th>IS 샤프</th><th>OOS 샤프</th><th>WFE</th><th>선택 파라미터</th></tr></thead>"
        f"<tbody>{''.join(table_rows)}</tbody></table></div>"
        "</section>"
    )


def _fmt_param_value(name: str, value: Any) -> str:
    key = str(name or "").strip()
    if key in {"stop_loss", "gap_up_max_pct", "entry_gap_down_stop_pct"} and _looks_like_ratio(value):
        return _fmt_pct_rule(value, digits=2, signed=True)
    if key in {"hold", "max_pos"}:
        return _fmt_int(value)
    if key in {"value_min"}:
        return _fmt_money(value)
    return _fmt(value)


def _review_issue_text(reason: str) -> str:
    mapping = {
        "insufficient_sample": "표본 부족으로 수정 보류",
        "monitor": "현재 기준 유지 관찰",
        "candidate_ready": "수정 후보 검토 가능",
    }
    return mapping.get(str(reason or ""), str(reason or "-"))


def _render_paper_review_summary(review: Dict[str, Any]) -> str:
    if not review:
        return (
            "<section class='panel-card'>"
            "<h4>paper 기준 재검토</h4>"
            "<p>paper 파라미터 리뷰 산출물이 없습니다.</p>"
            "</section>"
        )

    sample_gate = review.get("sample_gate", {}) or {}
    params = review.get("parameters", []) or []
    blocked_n = sum(1 for row in params if str(((row.get("paper_review") or {}).get("status", ""))) == "blocked")
    monitor_n = sum(1 for row in params if str(((row.get("paper_review") or {}).get("status", ""))) == "monitor")
    candidate_n = sum(1 for row in params if str(((row.get("paper_review") or {}).get("status", ""))) == "candidate")
    risk = ((review.get("paper_observations") or {}).get("risk") or {})
    comparison = ((review.get("paper_observations") or {}).get("comparison") or {})
    generated_at = str(review.get("generated_at", "-"))
    source_json = str(review.get("source_json", "") or "")
    ready = bool(sample_gate.get("enough_for_feedback", False))

    source_line = (
        f"<p><b>기준 산출물:</b> {_render_path_link(source_json, 'paper_parameter_review_latest.json')}</p>"
        if _is_probable_path(source_json)
        else ""
    )
    return (
        "<section class='panel-card'>"
        "<h4>paper 기준 재검토</h4>"
        "<p>백테스트 기준값을 그대로 유지할지, paper 결과를 보고 재검토만 할지 읽기 전용으로 보여줍니다.</p>"
        f"<p><b>생성 시각:</b> {_escape(generated_at)}</p>"
        f"{source_line}"
        "<section class='metric-row'>"
        f"<div class='metric'><div class='v'>{_escape(_fmt_int(sample_gate.get('paper_trades_used')))}</div><div class='k'>paper 거래 수</div></div>"
        f"<div class='metric'><div class='v'>{_escape(_fmt_int(sample_gate.get('warmup_trades')))}</div><div class='k'>재검토 기준 표본</div></div>"
        f"<div class='metric'><div class='v {'ok' if ready else 'warn'}'>{'READY' if ready else 'WARMUP'}</div><div class='k'>재검토 가능 여부</div></div>"
        f"<div class='metric'><div class='v warn'>{blocked_n}</div><div class='k'>보류</div></div>"
        f"<div class='metric'><div class='v ok'>{monitor_n}</div><div class='k'>유지관찰</div></div>"
        f"<div class='metric'><div class='v bad'>{candidate_n}</div><div class='k'>수정후보</div></div>"
        "</section>"
        f"<p><b>핵심 관측:</b> paper MDD {_escape(_fmt_pct_rule(risk.get('paper_mdd'), digits=2, signed=True))}, "
        f"일손익 {_escape(_fmt_pct_rule(risk.get('last_day_ret'), digits=2, signed=True))}, "
        f"정렬준비 {_escape(_user_text(comparison.get('alignment_ready', '-')))}, "
        f"품질게이트 {_escape(_user_text(comparison.get('quality_gate_ok', '-')))}</p>"
        "</section>"
    )


def _render_paper_review_table(review: Dict[str, Any], title: str, prefix: str) -> str:
    params = (review.get("parameters", []) or []) if review else []
    if not params:
        return (
            "<section class='panel-card'>"
            f"<h4>{_escape(title)}</h4>"
            "<p>표시할 파라미터 재검토 항목이 없습니다.</p>"
            "</section>"
        )

    rows: List[str] = []
    for row in params:
        name = str(row.get("name", "-"))
        paper_review = row.get("paper_review", {}) or {}
        candidate_update = row.get("candidate_update", {}) or {}
        review_status = str(paper_review.get("status", "blocked"))
        review_cls = REVIEW_STATUS_CLASS.get(review_status, "warn")
        evidence = paper_review.get("evidence", {}) or {}
        evidence_text = ", ".join(
            [
                f"MDD {_fmt_pct_rule(evidence.get('paper_mdd'), digits=2, signed=True)}",
                f"일손익 {_fmt_pct_rule(evidence.get('last_day_ret'), digits=2, signed=True)}",
                f"정렬 {_user_text(evidence.get('alignment_ready', '-'))}",
                f"품질 {_user_text(evidence.get('quality_gate_ok', '-'))}",
            ]
        )
        proposed_value = candidate_update.get("proposed_value", None)
        proposed_zone = candidate_update.get("proposed_zone", None)
        if proposed_value is not None:
            proposal_text = _fmt_param_value(name, proposed_value)
        elif proposed_zone is not None:
            proposal_text = _source_preview(proposed_zone)
        else:
            proposal_text = "-"
        text_blob = " ".join(
            [
                name,
                str(review_status),
                str(paper_review.get("reason", "")),
                str(evidence_text),
                str(proposal_text),
            ]
        ).lower()
        rows.append(
            f"<tr class='{_escape(prefix)}-row' data-status='{_escape(review_status)}' data-text='{_escape(text_blob)}'>"
            f"<td class='item'>{_escape(name)}</td>"
            f"<td class='num'>{_escape(_fmt_param_value(name, row.get('current_value')))}</td>"
            f"<td><span class='chip chip-{review_cls}'>{_escape(REVIEW_STATUS_LABEL.get(review_status, review_status))}</span></td>"
            f"<td>{_escape(_review_issue_text(str(paper_review.get('reason', '-'))))}</td>"
            f"<td>{_escape(evidence_text)}</td>"
            f"<td>{_escape(proposal_text)}</td>"
            f"<td>{_escape(str(candidate_update.get('confidence', '-')))}</td>"
            "</tr>"
        )

    return (
        "<section class='panel-card'>"
        f"<h4>{_escape(title)}</h4>"
        "<p>현재 기준값을 바로 바꾸지 않고, paper 결과를 보고 재검토만 합니다.</p>"
        "<div class='filter-row'>"
        f"<label>상태<select id='{_escape(prefix)}-filter-status'><option value='ALL'>전체</option><option value='blocked'>보류</option><option value='monitor'>유지관찰</option><option value='candidate'>수정후보</option></select></label>"
        f"<label>검색<input id='{_escape(prefix)}-filter-search' type='text' placeholder='파라미터명, 근거, 제안값 검색'></label>"
        "</div>"
        "<div class='table-wrap'><table>"
        "<thead><tr><th>파라미터</th><th>현재값</th><th>재검토 상태</th><th>판정</th><th>paper 근거</th><th>수정 후보</th><th>신뢰도</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
        "</section>"
    )


def _render_phase_tables(items: List[Dict[str, Any]]) -> str:
    rows = _phase_rows(items)
    phases = {k: v["title"] for k, v in PHASE_MAP.items()}
    phases[5] = "5단계: 기타/확장 항목"

    blocks: List[str] = []
    for pno in sorted({r["phase"] for r in rows}):
        part = [r for r in rows if r["phase"] == pno]
        p_pass = sum(1 for r in part if r.get("status") == "PASS")
        p_fail = sum(1 for r in part if r.get("status") == "FAIL")
        p_ne = sum(1 for r in part if r.get("status") == "NOT_EVALUABLE")

        tr: List[str] = []
        for r in part:
            status = str(r.get("status", "NOT_EVALUABLE"))
            cls = STATUS_CLASS.get(status, "warn")
            tr.append(
                "<tr>"
                f"<td class='item'>{_escape(r.get('item'))}</td>"
                f"<td><span class='chip chip-{cls}'>{_escape(STATUS_LABEL.get(status, status))}</span></td>"
                f"<td>{_escape(_user_text(r.get('metric')))}</td>"
                f"<td>{_escape(_user_text(r.get('threshold')))}</td>"
                f"<td>{_escape(r.get('issue'))}</td>"
                f"<td>{_escape(r.get('action'))}</td>"
                "</tr>"
            )

        blocks.append(
            "<section class='panel-card'>"
            f"<h4>{_escape(phases.get(pno, f'{pno}단계'))} <span class='minor'>통과:{p_pass} 실패:{p_fail} 판정불가:{p_ne}</span></h4>"
            "<div class='table-wrap'><table>"
            "<thead><tr><th>검증 항목</th><th>상태</th><th>실제값</th><th>기준</th><th>문제표시</th><th>권고조치</th></tr></thead>"
            f"<tbody>{''.join(tr)}</tbody></table></div>"
            "</section>"
        )

    return "".join(blocks)


def _render_stage_rows(stage: Dict[str, Any]) -> str:
    tr: List[str] = []
    for it in stage.get("items", []) or []:
        status = str(it.get("status", "NOT_EVALUABLE"))
        cls = STATUS_CLASS.get(status, "warn")
        tr.append(
            "<tr>"
            f"<td class='item'>{_escape(it.get('name', '-'))}</td>"
            f"<td><span class='chip chip-{cls}'>{_escape(STATUS_LABEL.get(status, status))}</span></td>"
            f"<td>{_escape(_user_text(it.get('metric', '-')))}</td>"
            f"<td>{_escape(_user_text(it.get('threshold', '-')))}</td>"
            f"<td>{_escape(it.get('issue', '-'))}</td>"
            f"<td>{_escape(it.get('action', '-'))}</td>"
            "</tr>"
        )
    return "".join(tr)


def _render_actions(stage: Dict[str, Any]) -> str:
    actions = stage.get("key_actions", []) or []
    if not actions:
        return "<li>-</li>"
    return "".join([f"<li>{_escape(x)}</li>" for x in actions])


def _required_chip(required: Any) -> str:
    label = "필수" if bool(required) else "선택"
    cls = "fail" if bool(required) else "warn"
    return f"<span class='chip chip-{cls}'>{_escape(label)}</span>"


def _source_preview(value: Any) -> str:
    if isinstance(value, dict):
        pairs = list(value.items())[:4]
        return ", ".join([_user_text(f"{k}={v}") for k, v in pairs]) or "-"
    if isinstance(value, list):
        return ", ".join([_user_text(v) for v in value[:4]]) or "-"
    return _user_text(value if value is not None else "-")


def _render_stage_item_table(items: List[Dict[str, Any]], prefix: str, include_required: bool = True) -> str:
    rows: List[str] = []
    for item in items:
        status = str(item.get("status", "NOT_EVALUABLE"))
        cls = STATUS_CLASS.get(status, "warn")
        text_blob = " ".join(
            [
                str(item.get("name", "")),
                str(item.get("metric", "")),
                str(item.get("threshold", "")),
                str(item.get("issue", "")),
                str(item.get("action", "")),
            ]
        ).lower()
        required_cell = f"<td>{_required_chip(item.get('required', False))}</td>" if include_required else ""
        rows.append(
            f"<tr class='{_escape(prefix)}-row'"
            f" data-status='{_escape(status)}'"
            f" data-text='{_escape(text_blob)}'>"
            f"<td class='item'>{_escape(item.get('name', '-'))}</td>"
            f"<td><span class='chip chip-{cls}'>{_escape(STATUS_LABEL.get(status, status))}</span></td>"
            f"<td>{_escape(_user_text(item.get('metric', '-')))}</td>"
            f"<td>{_escape(_user_text(item.get('threshold', '-')))}</td>"
            f"<td>{_escape(item.get('issue', '-'))}</td>"
            f"<td>{_escape(item.get('action', '-'))}</td>"
            f"{required_cell}"
            "</tr>"
        )
    if not rows:
        colspan = 7 if include_required else 6
        rows.append(f"<tr><td class='item' colspan='{colspan}'>표시할 검증 항목이 없습니다.</td></tr>")
    required_head = "<th>필수여부</th>" if include_required else ""
    return (
        "<div class='filter-row'>"
        f"<label>상태<select id='{_escape(prefix)}-filter-status'><option value='ALL'>전체</option><option value='PASS'>통과</option><option value='FAIL'>실패</option><option value='NOT_EVALUABLE'>판정불가</option></select></label>"
        f"<label>검색<input id='{_escape(prefix)}-filter-search' type='text' placeholder='항목명, 실제값, 기준 검색'></label>"
        "</div>"
        "<div class='table-wrap'><table>"
        f"<thead><tr><th>검증 항목</th><th>상태</th><th>실제값</th><th>기준</th><th>문제표시</th><th>권고조치</th>{required_head}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
    )


def _render_stage_overview(
    stage: Dict[str, Any],
    generated_at: str,
    stage_label: str,
    description: str,
    extra_cards: List[str] | None = None,
) -> str:
    counts = stage.get("counts", {}) or {}
    total = int(counts.get("total", len(stage.get("items", []) or [])))
    pass_n = int(counts.get("pass_n", 0))
    fail_n = int(counts.get("fail_n", 0))
    ne_n = int(counts.get("not_evaluable_n", 0))
    judgment = str(stage.get("judgment", "-"))
    cards = [
        f"<div class='metric'><div class='v'>{_escape(judgment)}</div><div class='k'>현재 판정</div></div>",
        f"<div class='metric'><div class='v'>{total}</div><div class='k'>총 검증 항목</div></div>",
        f"<div class='metric'><div class='v ok'>{pass_n}</div><div class='k'>통과</div></div>",
        f"<div class='metric'><div class='v bad'>{fail_n}</div><div class='k'>실패</div></div>",
        f"<div class='metric'><div class='v warn'>{ne_n}</div><div class='k'>판정불가</div></div>",
    ]
    if extra_cards:
        cards.extend(extra_cards)
    return (
        "<section class='panel-card'>"
        f"<h4>{_escape(stage_label)}</h4>"
        f"<p>{_escape(description)}</p>"
        f"<p><b>생성 시각:</b> {_escape(generated_at)}</p>"
        f"<section class='metric-row'>{''.join(cards)}</section>"
        "</section>"
    )


def _render_stage_result_explainer(stage: Dict[str, Any], header: str) -> str:
    judgment = str(stage.get("judgment", "-"))
    items = stage.get("items", []) or []
    fail_items = [item for item in items if str(item.get("status")) == "FAIL"]
    ne_items = [item for item in items if str(item.get("status")) == "NOT_EVALUABLE"]
    reasons = fail_items[:4] or ne_items[:4]
    reason_html = "".join(
        [f"<li>{_escape(item.get('issue', item.get('name', '-')))} / {_escape(_user_text(item.get('metric', '-')))}</li>" for item in reasons]
    ) or "<li>-</li>"
    actions_html = _render_actions(stage)
    return (
        "<div class='split'>"
        "<section class='panel-card side'>"
        f"<h4>{_escape(header)}</h4>"
        f"<p><b>현재 판정:</b> {_escape(judgment)}</p>"
        f"<p><b>해석:</b> 실패 항목과 판정불가 항목을 우선 보강한 뒤 다음 단계 전환 여부를 다시 확인합니다.</p>"
        f"<ul class='plain'>{reason_html}</ul>"
        "</section>"
        "<section class='panel-card'>"
        "<h4>즉시 우선조치</h4>"
        f"<ul class='plain'>{actions_html}</ul>"
        "</section>"
        "</div>"
    )


def _stage_auto_guidance_rows(stage: Dict[str, Any], gate: Dict[str, Any], stage_kind: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    blocker_map = {
        str(item.get("name")): item
        for item in (gate.get("blocker_details", []) or [])
        if isinstance(item, dict)
    }

    def append_row(kind: str, label: str, name: str, metric: str, action: str) -> None:
        rows.append(
            {
                "kind": kind,
                "label": label,
                "name": name,
                "metric": metric,
                "action": action,
            }
        )

    for item in (stage.get("items", []) or []):
        name = str(item.get("name", "-"))
        status = str(item.get("status", "NOT_EVALUABLE"))
        metric = str(item.get("metric", "-"))
        action = str(item.get("action", "-"))
        blocker = blocker_map.get(name, {})
        blocker_type = str(blocker.get("blocker_type", "") or "")

        if blocker_type == "warmup":
            append_row("warmup", "워밍업", name, metric, action)
            continue
        if blocker_type == "policy_lock":
            append_row("policy_lock", "정책잠금", name, metric, action)
            continue
        if blocker_type == "data_gap":
            append_row("data_gap", "데이터부족", name, metric, action)
            continue

        if status == "FAIL":
            append_row("validation_fail", "실제실패", name, metric, action)
            continue
        if status == "NOT_EVALUABLE":
            if stage_kind == "paper" and name in {"paper_mdd", "paper_bt_alignment", "paper_quality_gate"}:
                append_row("warmup", "워밍업", name, metric, action)
            elif stage_kind == "live" and name == "canary_execute_mode":
                append_row("policy_lock", "정책잠금", name, metric, action)
            else:
                append_row("data_gap", "데이터부족", name, metric, action)

    seen = set()
    unique_rows: List[Dict[str, Any]] = []
    for row in rows:
        key = (row["kind"], row["name"])
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    return unique_rows


def _render_stage_auto_guidance(stage: Dict[str, Any], gate: Dict[str, Any], stage_kind: str) -> str:
    rows = _stage_auto_guidance_rows(stage, gate, stage_kind)
    if not rows:
        return (
            "<section class='panel-card'>"
            "<h4>자동 해석 안내</h4>"
            "<p>지금 단계에서 즉시 경고할 항목이 없습니다.</p>"
            "</section>"
        )

    grouped: Dict[str, List[Dict[str, Any]]] = {
        "warmup": [],
        "policy_lock": [],
        "validation_fail": [],
        "data_gap": [],
    }
    for row in rows:
        grouped.setdefault(row["kind"], []).append(row)

    help_text = {
        "warmup": "표본이나 워밍업이 아직 부족합니다. 실패라기보다 누적 대기 상태로 보는 편이 맞습니다.",
        "policy_lock": "정책상 일부러 잠가둔 상태입니다. 실전 전환 직전 설정값을 확인해야 합니다.",
        "validation_fail": "실제 검증 실패입니다. 해당 step 또는 로직을 수정한 뒤 재실행이 필요합니다.",
        "data_gap": "근거 산출물이나 로그가 부족합니다. 원본 파일 생성부터 다시 확인해야 합니다.",
    }
    title_map = {
        "warmup": "워밍업",
        "policy_lock": "정책잠금",
        "validation_fail": "실제실패",
        "data_gap": "데이터부족",
    }
    card_order = ["warmup", "policy_lock", "validation_fail", "data_gap"]
    cards: List[str] = []
    for key in card_order:
        subset = grouped.get(key, [])
        if not subset:
            continue
        names = ", ".join(str(row.get("name", "-")) for row in subset[:3])
        action = str(subset[0].get("action", "-"))
        cards.append(
            "<div class='simple-card'>"
            f"<h4>{_escape(title_map[key])}</h4>"
            f"<p>{_escape(help_text[key])}<br>대상: {_escape(names)}<br>우선조치: {_escape(action)}</p>"
            "</div>"
        )

    detail_rows: List[str] = []
    for row in rows:
        detail_rows.append(
            "<tr>"
            f"<td class='item'>{_escape(row.get('label', '-'))}</td>"
            f"<td>{_escape(row.get('name', '-'))}</td>"
            f"<td>{_escape(_user_text(row.get('metric', '-')))}</td>"
            f"<td>{_escape(row.get('action', '-'))}</td>"
            "</tr>"
        )

    return (
        "<section class='panel-card'>"
        "<h4>자동 해석 안내</h4>"
        "<p>현재 단계에서 무엇이 워밍업인지, 정책 잠금인지, 실제 실패인지 자동으로 구분해 보여줍니다.</p>"
        f"<div class='simple-grid'>{''.join(cards)}</div>"
        "<div class='table-wrap'><table>"
        "<thead><tr><th>구분</th><th>항목</th><th>현재값</th><th>우선조치</th></tr></thead>"
        f"<tbody>{''.join(detail_rows)}</tbody></table></div>"
        "</section>"
    )


def _render_stage_summary_header(stage: Dict[str, Any], gate: Dict[str, Any], stage_kind: str) -> str:
    rows = _stage_auto_guidance_rows(stage, gate, stage_kind)
    judgment = str(stage.get("judgment", "-"))
    if rows:
        top_rows = rows[:2]
        headline = f"{judgment} - " + ", ".join(f"{row.get('label', '-')}: {row.get('name', '-')}" for row in top_rows)
        next_action = str(top_rows[0].get("action", "-"))
        detail = " / ".join(_user_text(row.get("metric", "-")) for row in top_rows)
    else:
        headline = f"{judgment} - 즉시 막는 핵심 항목 없음"
        next_action = ", ".join(str(x) for x in (stage.get("key_actions", []) or [])[:1]) or "추가 조치 없음"
        detail = "현재 표시된 실패/판정불가 핵심 항목이 없습니다."

    stage_note = {
        "paper": "가상매매 표본과 정렬 비교 준비 상태를 먼저 확인합니다.",
        "live": "실전 진입 직전 E2E, canary, 정책 잠금 상태를 먼저 확인합니다.",
    }.get(stage_kind, "현재 단계 핵심 상태를 먼저 확인합니다.")

    return (
        "<section class='panel-card'>"
        "<h4>10초 요약</h4>"
        f"<p><b>한 줄 결론:</b> {_escape(headline)}</p>"
        f"<p><b>핵심 상태:</b> {_escape(detail)}</p>"
        f"<p><b>다음 액션:</b> {_escape(next_action)}</p>"
        f"<p class='hint'>{_escape(stage_note)}</p>"
        "</section>"
    )


def _render_stage_provenance(
    stage: Dict[str, Any],
    generated_at: str,
    title: str,
    sources: Dict[str, Any],
    extra_sections: List[str] | None = None,
) -> str:
    cards: List[str] = [
        f"<div class='simple-card'><h4>판정 시각</h4><p>{_escape(generated_at)}</p></div>",
        f"<div class='simple-card'><h4>현재 단계</h4><p>{_escape(stage.get('title', title))}<br>판정: {_escape(stage.get('judgment', '-'))}</p></div>",
    ]
    for key, value in list((sources or {}).items())[:6]:
        cards.append(f"<div class='simple-card'><h4>{_escape(_source_label(key))}</h4><p>{_escape(_source_preview(value))}</p></div>")
    body = "<section class='panel-card'><h4>산출물 출처와 운영 범위</h4><div class='simple-grid'>" + "".join(cards) + "</div></section>"
    body += _render_source_links("원본 로그 바로가기", sources)
    if extra_sections:
        body += "".join(extra_sections)
    return body


def _render_transition_gate(panel_id: str, gate: Dict[str, Any], title: str) -> str:
    if not gate:
        return f"<section class='panel-card'><h4>{_escape(title)}</h4><p>전환 게이트 데이터가 없습니다.</p></section>"
    blockers = gate.get("blockers", []) or []
    blocker_details = gate.get("blocker_details", []) or []
    blocker_summary = gate.get("blocker_summary", {}) or {}
    policy = gate.get("policy", {}) or {}
    policy_notes = gate.get("policy_notes", []) or []
    counts = gate.get("counts", {}) or {}
    checks = gate.get("checks", []) or []
    blocker_html = "".join(
        [
            f"<li>{_escape(x.get('name', '-'))} [{_escape(x.get('blocker_label', '-'))}] - {_escape(x.get('summary', '-'))}</li>"
            for x in blocker_details
        ]
    ) if blocker_details else ("".join([f"<li>{_escape(x)}</li>" for x in blockers]) if blockers else "<li>-</li>")
    policy_note_html = "".join([f"<li>{_escape(x)}</li>" for x in policy_notes]) if policy_notes else "<li>-</li>"
    return (
        "<section class='panel-card'>"
        f"<h4>{_escape(title)}</h4>"
        f"<p><b>상태:</b> {_escape(gate.get('status', '-'))} / <b>준비 상태:</b> {_escape(_user_text(gate.get('ready', '-')))} / <b>다음 조치:</b> {_escape(gate.get('next_step', '-'))}</p>"
        f"<p><b>카운트:</b> 총계={_escape(counts.get('total', '-'))}, 통과={_escape(counts.get('pass_n', '-'))}, 실패={_escape(counts.get('fail_n', '-'))}</p>"
        f"<p><b>정책 프로필:</b> {_escape(gate.get('policy_profile', '-'))} / <b>워밍업 기준:</b> {_escape(policy.get('warmup_trades', '-'))} / <b>실주문 실행 필요:</b> {_escape(_user_text(policy.get('require_canary_execute', '-')))}</p>"
        f"<p><b>차단 요약:</b> 워밍업={_escape(blocker_summary.get('warmup_n', 0))}, 정책잠금={_escape(blocker_summary.get('policy_lock_n', 0))}, 실제실패={_escape(blocker_summary.get('validation_fail_n', 0))}, 데이터부족={_escape(blocker_summary.get('data_gap_n', 0))}</p>"
        f"<ul class='plain'>{blocker_html}</ul>"
        f"<ul class='plain'>{policy_note_html}</ul>"
        f"{_render_stage_item_table(checks, panel_id, include_required=True)}"
        "</section>"
    )


def _sample_trade_columns(rows: List[Dict[str, Any]]) -> List[str]:
    preferred = ["trade_id", "code", "entry_date", "exit_date", "exit_reason", "ret_sample", "pnl_krw", "note"]
    hidden = {"stdout_tail", "stderr_tail", "detail"}
    keys = {str(key) for row in rows for key in row.keys() if str(key) not in hidden}
    ordered = [key for key in preferred if key in keys]
    extras = sorted(key for key in keys if key not in ordered)
    return ordered + extras


def _render_sample_trade_table(title: str, rows: List[Dict[str, Any]], empty_text: str) -> str:
    if not rows:
        return (
            "<section class='panel-card'>"
            f"<h4>{_escape(title)}</h4>"
            f"<p>{_escape(empty_text)}</p>"
            "</section>"
        )

    cols = _sample_trade_columns(rows)
    header = "".join([f"<th>{_escape(col)}</th>" for col in cols])
    body_rows: List[str] = []
    for row in rows:
        cells = "".join([_sample_cell_html(col, row.get(col, "-")) for col in cols])
        body_rows.append(f"<tr>{cells}</tr>")

    return (
        "<section class='panel-card'>"
        f"<h4>{_escape(title)}</h4>"
        "<div class='table-wrap'><table>"
        f"<thead><tr>{header}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table></div>"
        "</section>"
    )


def _render_alignment_snapshot(stage: Dict[str, Any]) -> str:
    sources = stage.get("sources", {}) or {}
    snap = sources.get("alignment_snapshot", {}) or {}
    if not any(snap.get(key) for key in ("window_start", "window_end", "live_n", "bt_n", "reason")):
        return (
            "<section class='panel-card'>"
            "<h4>정렬 비교 원본 샘플</h4>"
            "<p>정렬 비교 샘플 데이터가 아직 없습니다. 먼저 live_vs_bt_feedback 산출물을 생성해야 합니다.</p>"
            "</section>"
        )

    summary_cards = [
        f"<div class='simple-card'><h4>정렬 비교 윈도우</h4><p>{_escape(snap.get('window_start', '-'))} ~ {_escape(snap.get('window_end', '-'))}</p></div>",
        f"<div class='simple-card'><h4>표본 수</h4><p>live_n={_escape(_fmt_int(snap.get('live_n', '-')))}<br>bt_n={_escape(_fmt_int(snap.get('bt_n', '-')))}</p></div>",
        f"<div class='simple-card'><h4>수익률 컬럼</h4><p>live={_escape(snap.get('ret_col_live', '-'))}<br>bt={_escape(snap.get('ret_col_bt', '-'))}</p></div>",
        f"<div class='simple-card'><h4>정렬 상태</h4><p>준비 상태={_escape(_user_text(snap.get('ready', '-')))}<br>사유={_escape(snap.get('reason', '-'))}</p></div>",
        f"<div class='simple-card'><h4>운영 시작일</h4><p>{_escape(snap.get('oper_start_ymd', '-'))}</p></div>",
        f"<div class='simple-card'><h4>원본 경로</h4><p>live={_escape(snap.get('live_path', '-'))}<br>bt={_escape(snap.get('bt_path', '-'))}</p></div>",
    ]

    return (
        "<section class='panel-card'>"
        "<h4>정렬 비교 원본 샘플</h4>"
        "<p>가상매매와 백테스트를 같은 exit-date 구간으로 맞춰 비교한 원본 샘플입니다.</p>"
        f"<div class='simple-grid'>{''.join(summary_cards)}</div>"
        "</section>"
        "<div class='split'>"
        f"{_render_sample_trade_table('live 샘플 거래', snap.get('live_sample_rows', []) or [], '운영 구간 live 샘플이 없습니다.')}"
        f"{_render_sample_trade_table('backtest 샘플 거래', snap.get('bt_sample_rows', []) or [], '같은 구간 backtest 샘플이 없습니다.')}"
        "</div>"
    )


def _render_live_execution_snapshot(stage: Dict[str, Any]) -> str:
    sources = stage.get("sources", {}) or {}
    e2e = sources.get("e2e_snapshot", {}) or {}
    canary = sources.get("canary_snapshot", {}) or {}
    fault = sources.get("fault_snapshot", {}) or {}

    if not any([e2e, canary, fault]):
        return (
            "<section class='panel-card'>"
            "<h4>실행 단계 원본 추적</h4>"
            "<p>실행 단계 원본 데이터가 아직 없습니다. E2E / canary / fault 산출물을 먼저 생성해야 합니다.</p>"
            "</section>"
        )

    summary_cards = [
        f"<div class='simple-card'><h4>E2E</h4><p>정상={_escape(_user_text(e2e.get('ok', '-')))}<br>통과={_escape(e2e.get('pass_n', '-'))} / 실패={_escape(e2e.get('fail_n', '-'))}<br>반복 수={_escape(e2e.get('iterations_done', '-'))}</p></div>",
        f"<div class='simple-card'><h4>Canary</h4><p>정상={_escape(_user_text(canary.get('ok', '-')))}<br>실행 모드={_escape(canary.get('mode', '-'))}<br>실주문 실행={_escape(_user_text(canary.get('execute', '-')))}</p></div>",
        f"<div class='simple-card'><h4>장애주입</h4><p>정상={_escape(_user_text(fault.get('ok', '-')))}<br>통과={_escape(fault.get('pass_n', '-'))} / 실패={_escape(fault.get('fail_n', '-'))}</p></div>",
        f"<div class='simple-card'><h4>생성 시각</h4><p>E2E={_escape(e2e.get('generated_at', '-'))}<br>Canary={_escape(canary.get('generated_at', '-'))}<br>장애주입={_escape(fault.get('generated_at', '-'))}</p></div>",
    ]

    return (
        "<section class='panel-card'>"
        "<h4>실행 단계 원본 추적</h4>"
        "<p>실전 단계에서 어떤 step이 통과했고, 어디서 막혔는지 원본 실행 결과를 바로 볼 수 있습니다.</p>"
        f"<div class='simple-grid'>{''.join(summary_cards)}</div>"
        "</section>"
        "<div class='split'>"
        f"{_render_sample_trade_table('E2E step 상세', e2e.get('steps', []) or [], 'E2E step 기록이 없습니다.')}"
        f"{_render_sample_trade_table('Canary step 상세', canary.get('steps', []) or [], 'Canary step 기록이 없습니다.')}"
        "</div>"
        f"{_render_sample_trade_table('Fault case 상세', fault.get('results', []) or [], 'Fault case 기록이 없습니다.')}"
    )


def _render_module_summary_header(module: Dict[str, Any]) -> str:
    status = str(module.get("status", "BLOCKED"))
    blockers = module.get("blockers", []) or []
    missing = module.get("missing_names", []) or []
    ready = bool(module.get("ready_for_optimize", False))
    title = str(module.get("title", "-"))
    headline = f"{title} - {MODULE_STATUS_LABEL.get(status, status)}"
    if blockers:
        headline += f" / blocker: {blockers[0]}"
    next_action = blockers[0] if blockers else (missing[0] if missing else "현재 구조 유지")
    detail = f"완료율={_fmt(module.get('completion_pct', 0.0), 4)}%, 최적화={'준비완료' if ready else '대기'}"
    return (
        "<section class='panel-card'>"
        "<h4>10초 요약</h4>"
        f"<p><b>한 줄 결론:</b> {_escape(headline)}</p>"
        f"<p><b>핵심 상태:</b> {_escape(detail)}</p>"
        f"<p><b>다음 액션:</b> {_escape(next_action)}</p>"
        f"<p class='hint'>{_escape(module.get('objective', '-'))}</p>"
        "</section>"
    )


def _render_backtest_summary_header(checklist: Dict[str, Any], final_output: Dict[str, Any]) -> str:
    final_gate = str(final_output.get("final_gate_decision", "-"))
    op_judgment = str(checklist.get("operation_judgment", "-"))
    scope = checklist.get("evaluation_scope", {}) if isinstance(checklist.get("evaluation_scope"), dict) else {}
    scope_label = str(scope.get("label", "백테스트 검증") or "백테스트 검증")
    scope_interpretation = str(scope.get("interpretation", "") or "")
    criteria = final_output.get("final_gate_criteria", []) or []
    top = criteria[0] if criteria else {}
    headline = f"{final_gate} - {scope_label}"
    detail = f"{top.get('name', '핵심 기준 없음')} / {_user_text(top.get('evidence', '-'))}"
    next_action = ", ".join(str(x) for x in (final_output.get("stop_triggers", []) or [])[:1]) or "세부 검증 확인"
    return (
        "<section class='panel-card'>"
        "<h4>10초 요약</h4>"
        f"<p><b>한 줄 결론:</b> {_escape(headline)}</p>"
        f"<p><b>판정 범위:</b> {_escape(scope_interpretation or op_judgment)}</p>"
        f"<p><b>핵심 상태:</b> {_escape(detail)}</p>"
        f"<p><b>다음 액션:</b> {_escape(next_action)}</p>"
        "<p class='hint'>현재 범위를 먼저 보고, 최종판정과 세부 검증으로 내려갑니다.</p>"
        "</section>"
    )


def _render_backtest_panel(
    checklist: Dict[str, Any],
    bt_report: Dict[str, Any],
    final_output: Dict[str, Any],
    market_data: Dict[str, Any],
    symbol_panel: Dict[str, Any],
    rate_series: Dict[str, Any],
    paper_parameter_review: Dict[str, Any] | None = None,
) -> str:
    items = checklist.get("items", []) or []
    total = int(checklist.get("total", len(items)))
    pass_n = int(checklist.get("pass_n", 0))
    fail_n = int(checklist.get("fail_n", 0))
    ne_n = int(checklist.get("not_evaluable_n", 0))
    pass_rate = round((pass_n / total) * 100.0, 1) if total > 0 else 0.0
    op_judgment = str(checklist.get("operation_judgment", "-"))

    return (
        "<section class='main-panel' id='panel-7'>"
        f"{_render_backtest_profile_panel()}"
        "<section class='subtabs'>"
        "<button class='subtab-btn active' data-sub-group='bt' data-sub-target='bt-setup'>무엇을 테스트했나</button>"
        "<button class='subtab-btn' data-sub-group='bt' data-sub-target='bt-run'>결과요약</button>"
        "<button class='subtab-btn' data-sub-group='bt' data-sub-target='bt-analysis'>판정근거</button>"
        "<button class='subtab-btn' data-sub-group='bt' data-sub-target='bt-validate'>세부검증</button>"
        "</section>"
        "<section class='subpanel active' id='bt-setup'>"
        f"{_render_backtest_summary_header(checklist, final_output)}"
        f"{_render_backtest_overview(bt_report, final_output, market_data, symbol_panel, rate_series)}"
        "</section>"
        "<section class='subpanel' id='bt-run'>"
        "<section class='metric-row'>"
        f"<div class='metric'><div class='v'>{total}</div><div class='k'>총 검증 항목</div></div>"
        f"<div class='metric'><div class='v ok'>{pass_n}</div><div class='k'>통과</div></div>"
        f"<div class='metric'><div class='v bad'>{fail_n}</div><div class='k'>실패</div></div>"
        f"<div class='metric'><div class='v warn'>{ne_n}</div><div class='k'>판정불가</div></div>"
        f"<div class='metric'><div class='v'>{_escape(op_judgment)}</div><div class='k'>1단계 게이트</div></div>"
        "</section>"
        f"{_render_final_outputs(final_output, bt_report)}"
        f"{_render_backtest_result_explainer(final_output)}"
        "</section>"
        "<section class='subpanel' id='bt-analysis'>"
        f"{_render_backtest_provenance(checklist, bt_report, final_output, market_data, symbol_panel, rate_series)}"
        f"{_render_final_gate_criteria(final_output)}"
        f"{_render_criterion_trace(final_output, items)}"
        f"{_render_domain_results(final_output)}"
        f"{_render_parameter_search(bt_report, final_output)}"
        f"{_render_walk_forward_detail(bt_report)}"
        f"{_render_paper_review_summary(paper_parameter_review or {})}"
        f"{_render_paper_review_table(paper_parameter_review or {}, '백테스트 기준값 대비 paper 재검토', 'bt-paper-review')}"
        "</section>"
        "<section class='subpanel' id='bt-validate'>"
        f"{_render_backtest_detail_table(items)}"
        f"{_render_phase_tables(items)}"
        "</section>"
        "</section>"
    )


def _render_paper_panel(stage: Dict[str, Any], trading_stage: Dict[str, Any], paper_parameter_review: Dict[str, Any] | None = None) -> str:
    generated_at = str((trading_stage or {}).get("generated_at", "-"))
    meta = (trading_stage or {}).get("meta", {}) or {}
    p2l = (((trading_stage or {}).get("transition_gate") or {}).get("paper_to_live") or {})
    scope = (((stage.get("sources") or {}).get("operational_scope")) or (meta.get("operational_scope") or {}))
    scope_card = f"<div class='metric'><div class='v'>{_escape(scope.get('rows_after', '-'))}</div><div class='k'>운영 구간 표본</div></div>"
    return (
        "<section class='main-panel' id='panel-8'>"
        "<section class='subtabs'>"
        "<button class='subtab-btn active' data-sub-group='paper' data-sub-target='paper-setup'>무엇을 보는가</button>"
        "<button class='subtab-btn' data-sub-group='paper' data-sub-target='paper-run'>결과요약</button>"
        "<button class='subtab-btn' data-sub-group='paper' data-sub-target='paper-analysis'>판정근거</button>"
        "<button class='subtab-btn' data-sub-group='paper' data-sub-target='paper-validate'>세부검증</button>"
        "</section>"
        "<section class='subpanel active' id='paper-setup'>"
        f"{_render_stage_summary_header(stage, p2l, 'paper')}"
        f"{_render_stage_overview(stage, generated_at, '2단계 가상매매 검증', '백테스트 이후 실제 운영 전 단계에서, 표본 수와 리스크오프 상태, 백테스트 정렬 여부를 확인합니다.', [scope_card])}"
        "</section>"
        "<section class='subpanel' id='paper-run'>"
        f"{_render_stage_result_explainer(stage, '왜 아직 가상매매 보강이 필요한가')}"
        f"{_render_stage_auto_guidance(stage, p2l, 'paper')}"
        "</section>"
        "<section class='subpanel' id='paper-analysis'>"
        f"{_render_stage_provenance(stage, generated_at, '가상매매', stage.get('sources', {}) or {}, [ _render_transition_gate('paper-gate', p2l, '실전 전환 게이트'), _render_alignment_snapshot(stage) ])}"
        f"{_render_paper_review_summary(paper_parameter_review or {})}"
        f"{_render_paper_review_table(paper_parameter_review or {}, '가상매매 기반 기준값 재검토', 'paper-review')}"
        "</section>"
        "<section class='subpanel' id='paper-validate'>"
        "<section class='panel-card'><h4>가상매매 세부 검증</h4>"
        f"{_render_stage_item_table(stage.get('items', []) or [], 'paper-detail', include_required=True)}"
        "</section>"
        "</section>"
        "</section>"
    )


def _render_live_panel(stage: Dict[str, Any], gate2_pass: bool, trading_stage: Dict[str, Any]) -> str:
    generated_at = str((trading_stage or {}).get("generated_at", "-"))
    p2l = (((trading_stage or {}).get("transition_gate") or {}).get("paper_to_live") or {})
    lock = "" if gate2_pass else "<section class='lock-banner'>2단계 게이트 통과 후 9.실전매매 탭이 활성화됩니다.</section>"
    gate_card = f"<div class='metric'><div class='v'>{'READY' if gate2_pass else 'LOCKED'}</div><div class='k'>실전 전환 상태</div></div>"
    return (
        "<section class='main-panel' id='panel-9'>"
        f"{lock}"
        "<section class='subtabs'>"
        "<button class='subtab-btn active' data-sub-group='live' data-sub-target='live-setup'>무엇을 보는가</button>"
        "<button class='subtab-btn' data-sub-group='live' data-sub-target='live-run'>결과요약</button>"
        "<button class='subtab-btn' data-sub-group='live' data-sub-target='live-analysis'>판정근거</button>"
        "<button class='subtab-btn' data-sub-group='live' data-sub-target='live-validate'>세부검증</button>"
        "</section>"
        "<section class='subpanel active' id='live-setup'>"
        f"{_render_stage_summary_header(stage, p2l, 'live')}"
        f"{_render_stage_overview(stage, generated_at, '3단계 실전매매 검증', '실주문 직전 또는 canary 운영 단계에서 E2E, 장애주입, 헬스체크, canary 준비 상태를 확인합니다.', [gate_card])}"
        "</section>"
        "<section class='subpanel' id='live-run'>"
        f"{_render_stage_result_explainer(stage, '왜 실전 단계가 아직 보류인지')}"
        f"{_render_stage_auto_guidance(stage, p2l, 'live')}"
        "</section>"
        "<section class='subpanel' id='live-analysis'>"
        f"{_render_stage_provenance(stage, generated_at, '실전매매', stage.get('sources', {}) or {}, [ _render_transition_gate('live-gate', p2l, '실전 진입 기준 참조'), _render_live_execution_snapshot(stage) ])}"
        "</section>"
        "<section class='subpanel' id='live-validate'>"
        "<section class='panel-card'><h4>실전매매 세부 검증</h4>"
        f"{_render_stage_item_table(stage.get('items', []) or [], 'live-detail', include_required=True)}"
        "</section>"
        "</section>"
        "</section>"
    )


def _module_cards(checklist: Dict[str, Any]) -> List[Dict[str, Any]]:
    module_progress = checklist.get("module_progress", {}) if isinstance(checklist.get("module_progress"), dict) else {}
    modules = module_progress.get("modules", []) if isinstance(module_progress.get("modules"), list) else []
    by_tab = {int(m.get("tab_no", 0)): m for m in modules if isinstance(m, dict)}
    defaults = [
        (1, "전략개요"),
        (2, "진입"),
        (3, "청산"),
        (4, "사이징"),
        (5, "리스크"),
        (6, "필터"),
    ]
    out: List[Dict[str, Any]] = []
    for tab_no, title in defaults:
        fallback = {
            "tab_no": tab_no,
            "title": title,
            "objective": "공용 체크리스트 연동 필요",
            "status": "BLOCKED",
            "completion_pct": 0.0,
            "ready_for_optimize": False,
            "pass_n": 0,
            "fail_n": 0,
            "not_evaluable_n": 0,
            "missing_n": 0,
            "blockers": ["module_progress_missing"],
            "evidence": [],
        }
        out.append(by_tab.get(tab_no, fallback))
    return out


def _render_strategy_panel(module: Dict[str, Any], checklist_generated_at: str, checklist_source: str) -> str:
    status = str(module.get("status", "BLOCKED"))
    cls = MODULE_STATUS_CLASS.get(status, "warn")
    blockers = module.get("blockers", []) or []
    missing_names = module.get("missing_names", []) or []
    evidence = module.get("evidence", []) or []
    blockers_text = ", ".join([str(x) for x in blockers[:4]]) if blockers else "-"
    missing_text = ", ".join([str(x) for x in missing_names[:4]]) if missing_names else "-"
    tab_no = int(module.get("tab_no", 0))
    evidence_rows: List[str] = []
    for row in evidence:
        row_cls = STATUS_CLASS.get(str(row.get("status", "NOT_EVALUABLE")), "warn")
        evidence_rows.append(
            f"<tr class='module-{tab_no}-detail-row'"
            f" data-status='{_escape(str(row.get('status', 'NOT_EVALUABLE')))}'"
            f" data-text='{_escape((str(row.get('item', '')) + ' ' + str(row.get('metric', '')) + ' ' + str(row.get('threshold', '')) + ' ' + str(row.get('action', ''))).lower())}'>"
            f"<td class='item'>{_escape(row.get('item', '-'))}</td>"
            f"<td><span class='chip chip-{row_cls}'>{_escape(STATUS_LABEL.get(str(row.get('status')), str(row.get('status'))))}</span></td>"
            f"<td>{_escape(_user_text(row.get('metric', '-')))}</td>"
            f"<td>{_escape(_user_text(row.get('threshold', '-')))}</td>"
            f"<td>{_escape(row.get('action', '-'))}</td>"
            "</tr>"
        )
    if not evidence_rows:
        evidence_rows.append(
            "<tr><td class='item'>연동 대기</td><td><span class='chip chip-fail'>보완필요</span></td><td>-</td><td>-</td><td>module_progress 산출 필요</td></tr>"
        )

    return (
        f"<section class='main-panel' id='panel-{tab_no}'>"
        "<section class='subtabs'>"
        f"<button class='subtab-btn active' data-sub-group='module-{tab_no}' data-sub-target='module-{tab_no}-setup'>무엇을 보는가</button>"
        f"<button class='subtab-btn' data-sub-group='module-{tab_no}' data-sub-target='module-{tab_no}-run'>결과요약</button>"
        f"<button class='subtab-btn' data-sub-group='module-{tab_no}' data-sub-target='module-{tab_no}-validate'>세부근거</button>"
        "</section>"
        f"<section class='subpanel active' id='module-{tab_no}-setup'>"
        f"{_render_module_summary_header(module)}"
        "<section class='panel-card'>"
        f"<h4>{_escape(module.get('title', '-'))}</h4>"
        "<section class='metric-row'>"
        f"<div class='metric'><div class='v {cls}'>{_escape(MODULE_STATUS_LABEL.get(status, status))}</div><div class='k'>모듈 상태</div></div>"
        f"<div class='metric'><div class='v'>{_escape(_fmt(module.get('completion_pct', 0.0), 4))}%</div><div class='k'>완료율</div></div>"
        f"<div class='metric'><div class='v'>{int(module.get('pass_n', 0))}/{int(module.get('total_items', 0))}</div><div class='k'>PASS / 전체</div></div>"
        f"<div class='metric'><div class='v'>{'READY' if bool(module.get('ready_for_optimize', False)) else 'WAIT'}</div><div class='k'>최적화 반영</div></div>"
        "</section>"
        f"<p><b>무엇을 판단하나:</b> {_escape(module.get('objective', '-'))}</p>"
        f"<p><b>공용 산출물 시각:</b> {_escape(checklist_generated_at)}</p>"
        "</section>"
        "</section>"
        f"<section class='subpanel' id='module-{tab_no}-run'>"
        "<div class='split'>"
        "<section class='panel-card side'>"
        "<h4>현재 판정 해석</h4>"
        f"<p><b>보완 포인트:</b> {_escape(blockers_text)}</p>"
        f"<p><b>누락 항목:</b> {_escape(missing_text)}</p>"
        f"<p><b>최적화 반영:</b> {_escape('READY' if bool(module.get('ready_for_optimize', False)) else 'WAIT')}</p>"
        "</section>"
        "<section class='panel-card'>"
        "<h4>출처</h4>"
        f"<p><b>기준 산출물:</b> {_render_path_link(checklist_source, 'backtest_validation_latest.json') if _is_probable_path(checklist_source) else _escape(checklist_source)}</p>"
        "<p class='hint'>이 화면은 공용 체크리스트 JSON을 읽는 읽기 전용 판정 화면입니다.</p>"
        "</section>"
        "</div>"
        "</section>"
        f"<section class='subpanel' id='module-{tab_no}-validate'>"
        "<section class='panel-card'>"
        "<h4>모듈 세부 근거</h4>"
        "<div class='filter-row'>"
        f"<label>상태<select id='module-{tab_no}-filter-status'><option value='ALL'>전체</option><option value='PASS'>통과</option><option value='FAIL'>실패</option><option value='NOT_EVALUABLE'>판정불가</option></select></label>"
        f"<label>검색<input id='module-{tab_no}-filter-search' type='text' placeholder='항목명, 실제값, 기준 검색'></label>"
        "</div>"
        "<div class='table-wrap'><table>"
        "<thead><tr><th>근거 항목</th><th>상태</th><th>실제값</th><th>기준</th><th>권고조치</th></tr></thead>"
        f"<tbody>{''.join(evidence_rows)}</tbody></table></div>"
        "</section>"
        "</section>"
        "</section>"
    )


def _render(
    checklist: Dict[str, Any],
    bt_report: Dict[str, Any],
    final_output: Dict[str, Any],
    trading_stage: Dict[str, Any],
    market_data: Dict[str, Any] | None = None,
    symbol_panel: Dict[str, Any] | None = None,
    rate_series: Dict[str, Any] | None = None,
    paper_parameter_review: Dict[str, Any] | None = None,
) -> str:
    fail_n = int(checklist.get("fail_n", 0))
    ne_n = int(checklist.get("not_evaluable_n", 0))
    op_judgment = str(checklist.get("operation_judgment", "-"))

    paper = trading_stage.get("paper", {}) if isinstance(trading_stage, dict) else {}
    live = trading_stage.get("live", {}) if isinstance(trading_stage, dict) else {}
    overall = trading_stage.get("overall", {}) if isinstance(trading_stage, dict) else {}
    transition = trading_stage.get("transition_gate", {}) if isinstance(trading_stage, dict) else {}
    p2l = (transition.get("paper_to_live") or {}) if isinstance(transition, dict) else {}

    gate1_pass = (fail_n == 0 and ne_n == 0 and op_judgment in {"운영가능", "조건부운영"})
    if isinstance(p2l, dict) and ("ready" in p2l):
        gate2_pass = bool(p2l.get("ready", False))
    else:
        gate2_pass = str(paper.get("judgment", "보류")) == "운영가능"

    active_tab = 7
    if gate1_pass and not gate2_pass:
        active_tab = 8
    elif gate1_pass and gate2_pass:
        active_tab = 9

    created = checklist.get("generated_at") or dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    gate2_status = str(p2l.get("status", "PASS" if gate2_pass else "HOLD")) if isinstance(p2l, dict) else ("PASS" if gate2_pass else "HOLD")
    module_cards = _module_cards(checklist)
    module_progress = checklist.get("module_progress", {}) if isinstance(checklist.get("module_progress"), dict) else {}
    module_optimizer_ready = bool(module_progress.get("optimizer_ready", False))
    module_blockers = module_progress.get("optimizer_blockers", []) or []
    validation_status = (
        f"1단계:{'PASS' if gate1_pass else 'HOLD'} | "
        f"2단계:{gate2_status} | "
        f"3단계:{'READY' if gate2_pass else 'LOCKED'} | "
        f"OPT:{'READY' if module_optimizer_ready else 'WAIT'}"
    )
    top_labels = [(int(m.get("tab_no", 0)), str(m.get("title", "-"))) for m in module_cards] + [
        (7, "백테스트"),
        (8, "가상매매"),
        (9, "실전매매"),
    ]

    top_buttons: List[str] = []
    for no, label in top_labels:
        active = " active" if no == active_tab else ""
        locked = ""
        attrs = ""
        if no == 8 and not gate1_pass:
            locked = " locked"
            attrs = " title='1단계 게이트 통과 후 활성화' disabled"
        if no == 9 and not gate2_pass:
            locked = " locked"
            attrs = " title='2단계 게이트 통과 후 활성화' disabled"
        top_buttons.append(
            f"<button class='top-tab{active}{locked}' data-target='panel-{no}'{attrs}>{no}.{_escape(label)}</button>"
        )

    checklist_source = str(checklist.get("source_json", "-"))
    strategy_panels = "".join([_render_strategy_panel(module, created, checklist_source) for module in module_cards])

    overall_j = str(overall.get("judgment", "-"))
    overall_next = str(overall.get("next_step", "-"))

    return f"""<!doctype html>
<html lang='ko'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>검증 콘솔</title>
<style>
@import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/variable/pretendardvariable.css');
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700;800&family=Roboto+Mono:wght@400;500;700&display=swap');
:root {{ --bg:#eef2f7; --card:#fff; --ink:#1f2d3d; --muted:#5f7185; --line:#d8dee8; --ok:#22b35f; --bad:#dc3545; --warn:#d59f00; --nav:#24374a; --font-ui:"Pretendard Variable","Pretendard","Noto Sans KR","Apple SD Gothic Neo",sans-serif; --font-num:"JetBrains Mono","Roboto Mono","D2Coding","Consolas",monospace; --font-time:"Roboto Mono","JetBrains Mono","Consolas",monospace; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:var(--font-ui); }}
.wrap {{ max-width:1400px; margin:12px auto 0; padding:0 12px 72px; }}
.top-tabs {{ display:flex; flex-wrap:wrap; gap:8px; background:#fff; border:1px solid var(--line); border-radius:12px; padding:10px; margin-bottom:10px; }}
.top-tab {{ border:1px solid #bfd0e0; background:#f8fbff; color:#1e3550; border-radius:8px; padding:8px 12px; font-size:14px; font-weight:700; cursor:pointer; }}
.top-tab.active {{ background:var(--nav); color:#fff; border-color:var(--nav); }}
.top-tab.locked {{ opacity:.55; cursor:not-allowed; }}
.main-area {{ min-height:560px; }}
.main-panel {{ display:none; }}
.main-panel.active {{ display:block; }}
.subtabs {{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:10px; }}
.subtab-btn {{ border:1px solid #c6d7e8; background:#fff; color:#27435f; border-radius:8px; padding:6px 11px; font-size:13px; font-weight:700; cursor:pointer; }}
.subtab-btn.active {{ background:#2d435a; color:#fff; border-color:#2d435a; }}
.subpanel {{ display:none; }}
.subpanel.active {{ display:block; }}
.panel-card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; margin-bottom:10px; }}
.panel-card h4 {{ margin:0 0 8px; font-size:19px; color:#1f3650; }}
.panel-card p {{ margin:6px 0; font-size:14px; }}
.minor {{ font-size:13px; color:#4f6780; margin-left:8px; }}
.split {{ display:grid; grid-template-columns:320px 1fr; gap:10px; }}
@media (max-width: 920px) {{ .split {{ grid-template-columns:1fr; }} }}
.chart-ph {{ height:160px; border:1px dashed #97b3cc; border-radius:10px; background:#f8fbff; display:flex; align-items:center; justify-content:center; color:#4f6880; font-size:14px; margin-bottom:8px; }}
.metric-row {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; margin-bottom:10px; }}
.metric {{ background:#fff; border:1px solid var(--line); border-radius:10px; padding:10px; text-align:center; }}
.metric .v {{ font-size:26px; font-weight:800; margin-bottom:4px; font-family:var(--font-num); font-variant-numeric:tabular-nums lining-nums; font-feature-settings:"tnum" 1, "lnum" 1; }}
.metric .k {{ font-size:13px; color:var(--muted); }}
.ok {{ color:var(--ok); }} .bad {{ color:var(--bad); }} .warn {{ color:var(--warn); }}
.simple-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:10px; }}
.simple-card {{ background:#f8fbff; border:1px solid var(--line); border-radius:10px; padding:10px; }}
.simple-card h4 {{ margin:0 0 6px; font-size:14px; color:#27435f; }}
.simple-card p {{ margin:0; font-size:13px; color:#24384b; line-height:1.45; }}
.table-wrap {{ overflow-x:auto; }}
table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
th,td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; word-break:break-word; }}
th {{ background:#f5f8fc; color:#38506b; font-size:13px; }}
td {{ font-size:13px; }}
.num {{ font-family:var(--font-num); font-variant-numeric:tabular-nums lining-nums; font-feature-settings:"tnum" 1, "lnum" 1; }}
.time {{ font-family:var(--font-time); font-variant-numeric:tabular-nums lining-nums; font-feature-settings:"tnum" 1, "lnum" 1; }}
.code {{ font-family:var(--font-time); font-variant-numeric:tabular-nums lining-nums; font-feature-settings:"tnum" 1, "lnum" 1; letter-spacing:.02em; }}
.item {{ font-weight:700; }}
.chip {{ display:inline-block; min-width:74px; text-align:center; border-radius:999px; padding:3px 8px; font-size:12px; font-weight:800; color:#fff; }}
.chip-pass {{ background:var(--ok); }} .chip-fail {{ background:var(--bad); }} .chip-warn {{ background:var(--warn); color:#111; }}
.plain {{ margin:0; padding-left:18px; font-size:14px; line-height:1.5; }}
.lock-banner {{ margin-bottom:10px; padding:10px; border:1px solid #f2c9cf; background:#fff4f6; color:#8f2130; border-radius:10px; font-size:13px; }}
.ctrl {{ display:flex; gap:8px; margin:8px 0; flex-wrap:wrap; }}
.act-btn {{ border:1px solid #b9cde0; background:#fff; color:#1f3650; border-radius:8px; padding:6px 10px; font-size:13px; font-weight:700; cursor:pointer; }}
.act-btn.danger {{ border-color:#e7b0b7; color:#8f2130; }}
.filter-row {{ display:flex; gap:10px; flex-wrap:wrap; margin-bottom:10px; }}
.filter-row label {{ display:flex; flex-direction:column; gap:4px; font-size:12px; color:#4c6276; }}
.filter-row select, .filter-row input {{ border:1px solid #c7d8e8; border-radius:8px; padding:7px 8px; font-size:13px; min-width:180px; }}
.form-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:8px 12px; }}
.form-grid label {{ display:flex; flex-direction:column; gap:5px; font-size:13px; color:#4c6276; }}
.form-grid input {{ border:1px solid #c7d8e8; border-radius:8px; padding:7px 8px; font-size:14px; }}
.hint {{ color:#5a7288; font-size:12px; }}
.statusbar {{ position:fixed; left:0; right:0; bottom:0; background:#1d2b38; color:#eaf2f8; border-top:1px solid #3b4d60; }}
.status-inner {{ max-width:1400px; margin:0 auto; padding:9px 12px; display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; font-size:12px; }}
.status-chip {{ background:#2a3d50; border:1px solid #3d546a; border-radius:7px; padding:6px 8px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; font-family:var(--font-ui); }}
</style>
</head>
<body>
<main class='wrap'>
<section class='top-tabs'>
{''.join(top_buttons)}
</section>
<section class='main-area'>
{strategy_panels}
{_render_backtest_panel(checklist, bt_report, final_output, market_data, symbol_panel, rate_series, paper_parameter_review)}
{_render_paper_panel(paper, trading_stage, paper_parameter_review)}
{_render_live_panel(live, gate2_pass, trading_stage)}
</section>
</main>
<footer class='statusbar'>
  <div class='status-inner'>
    <div class='status-chip' id='sb-mode'>현재 모드: {active_tab}.탭</div>
    <div class='status-chip'>전략 상태: {_escape(overall_j)} / 다음 조치: {_escape(overall_next)}</div>
    <div class='status-chip' id='sb-save'>마지막 저장: {_escape(created)}</div>
    <div class='status-chip'>모듈 최적화기: {'READY' if module_optimizer_ready else 'WAIT'} / 대기 사유: {_escape(','.join(str(x) for x in module_blockers[:2]) or '-')}</div>
    <div class='status-chip' id='sb-val'>검증 상태: {_escape(validation_status)}</div>
  </div>
</footer>
<script>
(function () {{
  const BT_PROFILE_STORAGE_KEY = 'bt_view_profiles_v1';
  const BT_BUILTIN_PROFILES = {{
    'builtin:all': {{
      label: '전체 보기',
      state: {{ focus: 'ALL', detailStatus: 'ALL', detailPhase: 'ALL', detailSearch: '', criteriaStatus: 'ALL', criteriaSearch: '' }},
    }},
    'builtin:data': {{
      label: '데이터 품질',
      state: {{ focus: 'DATA', detailStatus: 'ALL', detailPhase: 'ALL', detailSearch: '', criteriaStatus: 'ALL', criteriaSearch: '' }},
    }},
    'builtin:param': {{
      label: '파라미터 검증',
      state: {{ focus: 'PARAM', detailStatus: 'ALL', detailPhase: 'ALL', detailSearch: '', criteriaStatus: 'ALL', criteriaSearch: '' }},
    }},
    'builtin:risk': {{
      label: '리스크 점검',
      state: {{ focus: 'RISK', detailStatus: 'ALL', detailPhase: 'ALL', detailSearch: '', criteriaStatus: 'ALL', criteriaSearch: '' }},
    }},
    'builtin:regime': {{
      label: '레짐/시나리오',
      state: {{ focus: 'REGIME', detailStatus: 'ALL', detailPhase: 'ALL', detailSearch: '', criteriaStatus: 'ALL', criteriaSearch: '' }},
    }},
    'builtin:stability': {{
      label: '통계/재현성',
      state: {{ focus: 'STABILITY', detailStatus: 'ALL', detailPhase: 'ALL', detailSearch: '', criteriaStatus: 'ALL', criteriaSearch: '' }},
    }},
  }};
  const topTabs = Array.from(document.querySelectorAll('.top-tab'));
  const mainPanels = Array.from(document.querySelectorAll('.main-panel'));
  const sbMode = document.getElementById('sb-mode');
  const sbVal = document.getElementById('sb-val');

  function switchMain(targetId, label) {{
    topTabs.forEach((t) => t.classList.remove('active'));
    mainPanels.forEach((p) => p.classList.remove('active'));
    const btn = topTabs.find((b) => b.getAttribute('data-target') === targetId);
    const panel = document.getElementById(targetId);
    if (btn) btn.classList.add('active');
    if (panel) panel.classList.add('active');
    if (sbMode) sbMode.textContent = '현재 모드: ' + label;
  }}

  topTabs.forEach((btn) => {{
    btn.addEventListener('click', () => {{
      if (btn.disabled) return;
      const target = btn.getAttribute('data-target');
      const label = btn.textContent || target;
      switchMain(target, label);
    }});
  }});

  const subButtons = Array.from(document.querySelectorAll('.subtab-btn'));
  subButtons.forEach((btn) => {{
    btn.addEventListener('click', () => {{
      const grp = btn.getAttribute('data-sub-group');
      const target = btn.getAttribute('data-sub-target');
      document.querySelectorAll(".subtab-btn[data-sub-group='" + grp + "']").forEach((b) => b.classList.remove('active'));
      document.querySelectorAll('.subpanel').forEach((p) => {{
        if (p.id.startsWith(grp + '-')) p.classList.remove('active');
      }});
      btn.classList.add('active');
      const panel = document.getElementById(target);
      if (panel) panel.classList.add('active');
    }});
  }});

  document.querySelectorAll('.act-btn').forEach((btn) => {{
    btn.addEventListener('click', () => {{
      const act = btn.getAttribute('data-act') || 'noop';
      const kind = btn.getAttribute('data-kind') || 'system';
      if (sbVal) sbVal.textContent = '검증 상태: ' + kind + ' ' + act + ' 요청됨';
    }});
  }});

  function readCustomProfiles() {{
    try {{
      const raw = localStorage.getItem(BT_PROFILE_STORAGE_KEY);
      if (!raw) return {{}};
      const obj = JSON.parse(raw);
      return obj && typeof obj === 'object' ? obj : {{}};
    }} catch (_) {{
      return {{}};
    }}
  }}

  function writeCustomProfiles(profiles) {{
    try {{
      localStorage.setItem(BT_PROFILE_STORAGE_KEY, JSON.stringify(profiles));
    }} catch (_) {{
      // UI-only storage best-effort
    }}
  }}

  function currentFilterState() {{
    return {{
      focus: document.querySelector('.bt-focus-select')?.value || 'ALL',
      detailStatus: document.getElementById('bt-filter-status')?.value || 'ALL',
      detailPhase: document.getElementById('bt-filter-phase')?.value || 'ALL',
      detailSearch: document.getElementById('bt-filter-search')?.value || '',
      criteriaStatus: document.getElementById('bt-criteria-status')?.value || 'ALL',
      criteriaSearch: document.getElementById('bt-criteria-search')?.value || '',
    }};
  }}

  function applyFilterState(state) {{
    if (!state) return;
    const focus = state.focus || 'ALL';
    document.querySelectorAll('.bt-focus-select').forEach((el) => {{ el.value = focus; }});
    const detailStatus = document.getElementById('bt-filter-status');
    const detailPhase = document.getElementById('bt-filter-phase');
    const detailSearch = document.getElementById('bt-filter-search');
    const criteriaStatus = document.getElementById('bt-criteria-status');
    const criteriaSearch = document.getElementById('bt-criteria-search');
    if (detailStatus) detailStatus.value = state.detailStatus || 'ALL';
    if (detailPhase) detailPhase.value = state.detailPhase || 'ALL';
    if (detailSearch) detailSearch.value = state.detailSearch || '';
    if (criteriaStatus) criteriaStatus.value = state.criteriaStatus || 'ALL';
    if (criteriaSearch) criteriaSearch.value = state.criteriaSearch || '';
    filterBacktestDetails();
    filterBacktestAnalysis();
  }}

  function rebuildProfileOptions() {{
    const select = document.getElementById('bt-profile-select');
    if (!select) return;
    const current = select.value || 'builtin:all';
    const custom = readCustomProfiles();
    const entries = Object.entries(BT_BUILTIN_PROFILES).map(([key, meta]) => `<option value="${{key}}">${{meta.label}}</option>`);
    Object.keys(custom).sort().forEach((name) => {{
      entries.push(`<option value="custom:${{name}}">${{name}} (사용자)</option>`);
    }});
    select.innerHTML = entries.join('');
    select.value = current in BT_BUILTIN_PROFILES || current.startsWith('custom:') ? current : 'builtin:all';
  }}

  function profileStateFromKey(key) {{
    if (!key) return BT_BUILTIN_PROFILES['builtin:all'].state;
    if (BT_BUILTIN_PROFILES[key]) return BT_BUILTIN_PROFILES[key].state;
    if (key.startsWith('custom:')) {{
      const custom = readCustomProfiles();
      return custom[key.slice(7)] || null;
    }}
    return null;
  }}

  function filterBacktestDetails() {{
    const status = document.getElementById('bt-filter-status')?.value || 'ALL';
    const phase = document.getElementById('bt-filter-phase')?.value || 'ALL';
    const focus = document.querySelector('.bt-focus-select')?.value || 'ALL';
    const search = (document.getElementById('bt-filter-search')?.value || '').toLowerCase().trim();
    document.querySelectorAll('.bt-detail-row').forEach((row) => {{
      const rowStatus = row.getAttribute('data-status') || '';
      const rowPhase = row.getAttribute('data-phase') || '';
      const rowFocus = row.getAttribute('data-focus') || 'ALL';
      const rowText = row.getAttribute('data-text') || '';
      const matchStatus = status === 'ALL' || rowStatus === status;
      const matchPhase = phase === 'ALL' || rowPhase === phase;
      const matchFocus = focus === 'ALL' || rowFocus === focus;
      const matchSearch = !search || rowText.includes(search);
      row.style.display = (matchStatus && matchPhase && matchFocus && matchSearch) ? '' : 'none';
    }});
  }}

  function filterBacktestAnalysis() {{
    const focus = document.querySelector('.bt-focus-select')?.value || 'ALL';
    const status = document.getElementById('bt-criteria-status')?.value || 'ALL';
    const search = (document.getElementById('bt-criteria-search')?.value || '').toLowerCase().trim();
    document.querySelectorAll('.bt-criteria-row').forEach((row) => {{
      const rowFocus = row.getAttribute('data-focus') || 'ALL';
      const rowStatus = row.getAttribute('data-status') || '';
      const rowText = row.getAttribute('data-text') || '';
      const matchFocus = focus === 'ALL' || rowFocus === focus;
      const matchStatus = status === 'ALL' || rowStatus === status;
      const matchSearch = !search || rowText.includes(search);
      row.style.display = (matchFocus && matchStatus && matchSearch) ? '' : 'none';
    }});
    document.querySelectorAll('.bt-domain-card, .bt-focus-item[data-focus]').forEach((card) => {{
      if (card.classList.contains('bt-detail-row') || card.classList.contains('bt-criteria-row')) return;
      const cardFocus = card.getAttribute('data-focus') || 'ALL';
      const cardStatus = card.getAttribute('data-status') || 'PASS';
      const cardText = card.getAttribute('data-text') || '';
      const matchFocus = focus === 'ALL' || cardFocus === focus;
      const matchStatus = status === 'ALL' || cardStatus === status || status === 'PASS';
      const matchSearch = !search || cardText.includes(search);
      card.style.display = (matchFocus && matchStatus && matchSearch) ? '' : 'none';
    }});
  }}

  function filterSimpleRows(rowSelector, statusId, searchId) {{
    const status = document.getElementById(statusId)?.value || 'ALL';
    const search = (document.getElementById(searchId)?.value || '').toLowerCase().trim();
    document.querySelectorAll(rowSelector).forEach((row) => {{
      const rowStatus = row.getAttribute('data-status') || '';
      const rowText = row.getAttribute('data-text') || '';
      const matchStatus = status === 'ALL' || rowStatus === status;
      const matchSearch = !search || rowText.includes(search);
      row.style.display = (matchStatus && matchSearch) ? '' : 'none';
    }});
  }}

  function wireSimpleFilters(statusId, searchId, rowSelector) {{
    const run = () => filterSimpleRows(rowSelector, statusId, searchId);
    document.getElementById(statusId)?.addEventListener('change', run);
    document.getElementById(searchId)?.addEventListener('input', run);
    run();
  }}

  document.querySelectorAll('.bt-focus-select').forEach((el) => {{
    el.addEventListener('change', () => {{
      const value = el.value;
      document.querySelectorAll('.bt-focus-select').forEach((other) => {{ if (other !== el) other.value = value; }});
      filterBacktestDetails();
      filterBacktestAnalysis();
    }});
  }});

  document.getElementById('bt-filter-status')?.addEventListener('change', filterBacktestDetails);
  document.getElementById('bt-filter-phase')?.addEventListener('change', filterBacktestDetails);
  document.getElementById('bt-filter-search')?.addEventListener('input', filterBacktestDetails);
  document.getElementById('bt-criteria-status')?.addEventListener('change', filterBacktestAnalysis);
  document.getElementById('bt-criteria-search')?.addEventListener('input', filterBacktestAnalysis);

  document.getElementById('bt-profile-apply')?.addEventListener('click', () => {{
    const key = document.getElementById('bt-profile-select')?.value || 'builtin:all';
    const state = profileStateFromKey(key);
    applyFilterState(state);
    if (sbVal) sbVal.textContent = '검증 상태: backtest profile ' + key + ' 적용됨';
  }});

  document.getElementById('bt-profile-save')?.addEventListener('click', () => {{
    const input = document.getElementById('bt-profile-name');
    const name = (input?.value || '').trim();
    if (!name) {{
      if (sbVal) sbVal.textContent = '검증 상태: backtest profile 이름 입력 필요';
      return;
    }}
    const custom = readCustomProfiles();
    custom[name] = currentFilterState();
    writeCustomProfiles(custom);
    rebuildProfileOptions();
    const select = document.getElementById('bt-profile-select');
    if (select) select.value = 'custom:' + name;
    if (sbVal) sbVal.textContent = '검증 상태: backtest profile 저장됨 (' + name + ')';
  }});

  document.getElementById('bt-profile-reset')?.addEventListener('click', () => {{
    const select = document.getElementById('bt-profile-select');
    if (select) select.value = 'builtin:all';
    applyFilterState(BT_BUILTIN_PROFILES['builtin:all'].state);
    if (sbVal) sbVal.textContent = '검증 상태: backtest profile 기본값 복원';
  }});

  document.getElementById('bt-profile-delete')?.addEventListener('click', () => {{
    const select = document.getElementById('bt-profile-select');
    const key = select?.value || '';
    if (!key.startsWith('custom:')) {{
      if (sbVal) sbVal.textContent = '검증 상태: 삭제할 사용자 프로필 선택 필요';
      return;
    }}
    const name = key.slice(7);
    const custom = readCustomProfiles();
    delete custom[name];
    writeCustomProfiles(custom);
    rebuildProfileOptions();
    if (select) select.value = 'builtin:all';
    applyFilterState(BT_BUILTIN_PROFILES['builtin:all'].state);
    if (sbVal) sbVal.textContent = '검증 상태: backtest profile 삭제됨 (' + name + ')';
  }});

  rebuildProfileOptions();
  filterBacktestDetails();
  filterBacktestAnalysis();
  wireSimpleFilters('paper-detail-filter-status', 'paper-detail-filter-search', '.paper-detail-row');
  wireSimpleFilters('paper-gate-filter-status', 'paper-gate-filter-search', '.paper-gate-row');
  wireSimpleFilters('paper-review-filter-status', 'paper-review-filter-search', '.paper-review-row');
  wireSimpleFilters('live-detail-filter-status', 'live-detail-filter-search', '.live-detail-row');
  wireSimpleFilters('live-gate-filter-status', 'live-gate-filter-search', '.live-gate-row');
  wireSimpleFilters('bt-paper-review-filter-status', 'bt-paper-review-filter-search', '.bt-paper-review-row');
  for (let i = 1; i <= 6; i += 1) {{
    wireSimpleFilters(`module-${{i}}-filter-status`, `module-${{i}}-filter-search`, `.module-${{i}}-detail-row`);
  }}

  switchMain('panel-{active_tab}', (document.querySelector(".top-tab[data-target='panel-{active_tab}']")?.textContent || '{active_tab}.탭'));
}})();
</script>
</body>
</html>
"""


def main() -> int:
    ap = argparse.ArgumentParser(description="Build backtest validation HTML screen")
    ap.add_argument("--checklist-json", default=str(LOG_DIR / "backtest_validation_checklist_latest.json"))
    ap.add_argument("--report-json", default=str(LOG_DIR / "backtest_validation_latest.json"))
    ap.add_argument("--final-json", default=str(LOG_DIR / "backtest_final_output_latest.json"))
    ap.add_argument("--trading-stage-json", default=str(LOG_DIR / "trading_stage_validation_latest.json"))
    ap.add_argument("--market-json", default=str(LOG_DIR / "backtest_market_ohlc_latest.json"))
    ap.add_argument("--symbol-panel-json", default=str(LOG_DIR / "backtest_symbol_panel_latest.json"))
    ap.add_argument("--rate-json", default=str(LOG_DIR / "rate_series_latest.json"))
    ap.add_argument("--paper-review-json", default=str(LOG_DIR / "paper_parameter_review_latest.json"))
    ap.add_argument("--out-html", default="")
    args = ap.parse_args()

    checklist_path = Path(args.checklist_json)
    report_path = Path(args.report_json)
    final_path = Path(args.final_json)
    trading_stage_path = Path(args.trading_stage_json)
    market_path = Path(args.market_json)
    symbol_panel_path = Path(args.symbol_panel_json)
    rate_path = Path(args.rate_json)
    paper_review_path = Path(args.paper_review_json)

    checklist = _load_json(checklist_path)
    report = _load_json(report_path)
    final_output = _load_json(final_path)
    trading_stage = _load_json(trading_stage_path)
    market_data = _load_json(market_path)
    symbol_panel = _load_json(symbol_panel_path)
    rate_series = _load_json(rate_path)
    paper_parameter_review = _load_json(paper_review_path)
    if paper_parameter_review:
        paper_parameter_review["source_json"] = str(paper_review_path)

    if not checklist:
        raise FileNotFoundError(f"checklist json not found or empty: {checklist_path}")

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_html = Path(args.out_html) if args.out_html else (LOG_DIR / f"backtest_validation_screen_{stamp}.html")
    latest_html = LOG_DIR / "backtest_validation_screen_latest.html"

    html = _render(checklist, report, final_output, trading_stage, market_data, symbol_panel, rate_series, paper_parameter_review)
    out_html.write_text(html, encoding="utf-8")
    latest_html.write_text(html, encoding="utf-8")

    _log_print(f"[BTSCR] html={out_html}")
    _log_print(f"[BTSCR] latest={latest_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
