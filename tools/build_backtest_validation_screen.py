from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List


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
        f"<div class='simple-card'><h4>기대치</h4><p>AnnRet={_fmt(expected.get('annual_return'))}<br>Sharpe={_fmt(expected.get('sharpe'))}<br>MDD={_fmt(expected.get('max_drawdown'))}</p></div>"
        f"<div class='simple-card'><h4>핵심 파라미터</h4><p>{_escape(params)}</p></div>"
        "</div>"
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
                f"<td>{_escape(r.get('metric'))}</td>"
                f"<td>{_escape(r.get('threshold'))}</td>"
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
            f"<td>{_escape(it.get('metric', '-'))}</td>"
            f"<td>{_escape(it.get('threshold', '-'))}</td>"
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


def _render_backtest_panel(checklist: Dict[str, Any], bt_report: Dict[str, Any], final_output: Dict[str, Any]) -> str:
    items = checklist.get("items", []) or []
    total = int(checklist.get("total", len(items)))
    pass_n = int(checklist.get("pass_n", 0))
    fail_n = int(checklist.get("fail_n", 0))
    ne_n = int(checklist.get("not_evaluable_n", 0))
    pass_rate = round((pass_n / total) * 100.0, 1) if total > 0 else 0.0
    op_judgment = str(checklist.get("operation_judgment", "-"))

    integ = ((bt_report.get("artifacts") or {}).get("integration") or {})
    params = integ.get("params", {}) or {}
    market_meta = integ.get("market_meta", {}) or {}

    return (
        "<section class='main-panel' id='panel-7'>"
        "<section class='subtabs'>"
        "<button class='subtab-btn active' data-sub-group='bt' data-sub-target='bt-setup'>설정</button>"
        "<button class='subtab-btn' data-sub-group='bt' data-sub-target='bt-run'>실행</button>"
        "<button class='subtab-btn' data-sub-group='bt' data-sub-target='bt-analysis'>분석</button>"
        "<button class='subtab-btn' data-sub-group='bt' data-sub-target='bt-validate'>검증</button>"
        "</section>"
        "<section class='subpanel active' id='bt-setup'>"
        "<div class='split'>"
        "<section class='panel-card side'>"
        "<h4>기간/비용/기본 설정</h4>"
        f"<p>전략 소스: <b>{_escape(integ.get('strategy_source', '-'))}</b></p>"
        f"<p>백테스트 엔진: <b>{_escape(integ.get('backtest_source', '-'))}</b></p>"
        f"<p>데이터 행 수: <b>{_escape(market_meta.get('output_rows', '-'))}</b></p>"
        f"<p>핵심 파라미터: <b>{_escape(params)}</b></p>"
        "</section>"
        "<section class='panel-card'>"
        "<h4>자산 곡선 차트</h4>"
        "<div class='chart-ph'>자산 곡선 / 벤치마크 차트 영역</div>"
        "<h4>성과 지표 요약</h4>"
        f"<p>CAGR | SR | MDD | 승률 | 손익비 (통과율 {pass_rate}%)</p>"
        "</section>"
        "</div>"
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
        "</section>"
        "<section class='subpanel' id='bt-analysis'>"
        "<section class='panel-card'><h4>분석</h4>"
        "<ul class='plain'><li>월별/분기별/연도별 성과</li><li>레짐별 성과 분해</li><li>롤링 샤프/낙폭</li><li>종목/섹터 기여도</li></ul>"
        "</section></section>"
        "<section class='subpanel' id='bt-validate'>"
        f"{_render_phase_tables(items)}"
        "</section>"
        "</section>"
    )


def _render_paper_panel(stage: Dict[str, Any]) -> str:
    judgment = str(stage.get("judgment", "-"))
    return (
        "<section class='main-panel' id='panel-8'>"
        "<section class='subtabs'>"
        "<button class='subtab-btn active' data-sub-group='paper' data-sub-target='paper-dashboard'>대시보드</button>"
        "<button class='subtab-btn' data-sub-group='paper' data-sub-target='paper-signal'>시그널</button>"
        "<button class='subtab-btn' data-sub-group='paper' data-sub-target='paper-trades'>거래내역</button>"
        "<button class='subtab-btn' data-sub-group='paper' data-sub-target='paper-compare'>비교분석</button>"
        "<button class='subtab-btn' data-sub-group='paper' data-sub-target='paper-system'>시스템</button>"
        "</section>"
        "<section class='subpanel active' id='paper-dashboard'>"
        f"<section class='panel-card'><h4>상태: {_escape(judgment)} | {_escape(_count_text(stage))}</h4>"
        "<div class='ctrl'><button class='act-btn' data-kind='paper' data-act='start'>시작</button><button class='act-btn' data-kind='paper' data-act='stop'>중지</button><button class='act-btn' data-kind='paper' data-act='reset'>리셋</button></div>"
        "<div class='chart-ph'>실시간 자산곡선(예상 vs 실제) 영역</div></section>"
        "<section class='panel-card'><h4>검증 테이블</h4><div class='table-wrap'><table>"
        "<thead><tr><th>검증 항목</th><th>상태</th><th>실제값</th><th>기준</th><th>문제표시</th><th>권고조치</th></tr></thead>"
        f"<tbody>{_render_stage_rows(stage)}</tbody></table></div></section>"
        "<section class='panel-card'><h4>즉시 우선조치</h4><ul class='plain'>"
        f"{_render_actions(stage)}</ul></section>"
        "</section>"
        "<section class='subpanel' id='paper-signal'><section class='panel-card'><h4>시그널</h4><p>실시간 시그널 로그 영역</p></section></section>"
        "<section class='subpanel' id='paper-trades'><section class='panel-card'><h4>거래내역</h4><p>체결/손익/보유기간 영역</p></section></section>"
        "<section class='subpanel' id='paper-compare'><section class='panel-card'><h4>비교분석</h4><p>백테스트 vs 가상매매 비교 영역</p></section></section>"
        "<section class='subpanel' id='paper-system'><section class='panel-card'><h4>시스템</h4><p>데이터 수신/오류/리소스 모니터링 영역</p></section></section>"
        "</section>"
    )


def _render_live_panel(stage: Dict[str, Any], gate2_pass: bool) -> str:
    judgment = str(stage.get("judgment", "-"))
    lock = "" if gate2_pass else "<section class='lock-banner'>2단계 게이트 통과 후 9.실전매매 탭이 활성화됩니다.</section>"
    return (
        "<section class='main-panel' id='panel-9'>"
        f"{lock}"
        "<section class='subtabs'>"
        "<button class='subtab-btn active' data-sub-group='live' data-sub-target='live-dashboard'>대시보드</button>"
        "<button class='subtab-btn' data-sub-group='live' data-sub-target='live-order'>주문</button>"
        "<button class='subtab-btn' data-sub-group='live' data-sub-target='live-fill'>체결</button>"
        "<button class='subtab-btn' data-sub-group='live' data-sub-target='live-performance'>성과</button>"
        "<button class='subtab-btn' data-sub-group='live' data-sub-target='live-risk'>리스크</button>"
        "<button class='subtab-btn' data-sub-group='live' data-sub-target='live-ops'>운영</button>"
        "</section>"
        "<section class='subpanel active' id='live-dashboard'>"
        f"<section class='panel-card'><h4>상태: {_escape(judgment)} | {_escape(_count_text(stage))}</h4>"
        "<div class='ctrl'><button class='act-btn' data-kind='live' data-act='start'>시작</button><button class='act-btn' data-kind='live' data-act='stop'>중지</button><button class='act-btn danger' data-kind='live' data-act='emergency'>긴급청산</button></div>"
        "<div class='chart-ph'>3단계 비교(백테스트 vs 가상매매 vs 실전매매) 영역</div></section>"
        "<section class='panel-card'><h4>실행 품질 모니터링</h4><div class='table-wrap'><table>"
        "<thead><tr><th>검증 항목</th><th>상태</th><th>실제값</th><th>기준</th><th>문제표시</th><th>권고조치</th></tr></thead>"
        f"<tbody>{_render_stage_rows(stage)}</tbody></table></div></section>"
        "<section class='panel-card'><h4>즉시 우선조치</h4><ul class='plain'>"
        f"{_render_actions(stage)}</ul></section>"
        "</section>"
        "<section class='subpanel' id='live-order'><section class='panel-card'><h4>주문</h4><p>대기/체결/취소 주문 현황 영역</p></section></section>"
        "<section class='subpanel' id='live-fill'><section class='panel-card'><h4>체결</h4><p>실제 체결 내역, 슬리피지 분석 영역</p></section></section>"
        "<section class='subpanel' id='live-performance'><section class='panel-card'><h4>성과</h4><p>일/주/월 성과, 증액 단계별 성과 영역</p></section></section>"
        "<section class='subpanel' id='live-risk'><section class='panel-card'><h4>리스크</h4><p>일일 손실, 낙폭, 리스크 지표 영역</p></section></section>"
        "<section class='subpanel' id='live-ops'><section class='panel-card'><h4>운영</h4><p>브로커 연결, 로그, 알림 설정 영역</p></section></section>"
        "</section>"
    )


def _render_strategy_panel(no: int, title: str) -> str:
    prefix = f"s{no}"
    return (
        f"<section class='main-panel' id='panel-{no}'>"
        "<section class='panel-card'>"
        f"<h4>{_escape(title)}</h4>"
        "<div class='form-grid'>"
        f"<label>항목 A<input data-sync='cfg_{prefix}_a' data-default=''></label>"
        f"<label>항목 B<input data-sync='cfg_{prefix}_b' data-default=''></label>"
        f"<label>항목 C<input data-sync='cfg_{prefix}_c' data-default=''></label>"
        "</div>"
        "<div class='ctrl'><button class='act-btn' data-act='save-config'>저장</button><button class='act-btn' data-act='reset-config'>초기화</button></div>"
        "<p class='hint'>저장된 설정은 7~9 탭에서 읽기 전용 요약으로 공유됩니다.</p>"
        "</section>"
        "</section>"
    )


def _render(
    checklist: Dict[str, Any],
    bt_report: Dict[str, Any],
    final_output: Dict[str, Any],
    trading_stage: Dict[str, Any],
) -> str:
    fail_n = int(checklist.get("fail_n", 0))
    ne_n = int(checklist.get("not_evaluable_n", 0))
    op_judgment = str(checklist.get("operation_judgment", "-"))

    paper = trading_stage.get("paper", {}) if isinstance(trading_stage, dict) else {}
    live = trading_stage.get("live", {}) if isinstance(trading_stage, dict) else {}
    overall = trading_stage.get("overall", {}) if isinstance(trading_stage, dict) else {}

    gate1_pass = (fail_n == 0 and ne_n == 0 and op_judgment in {"운영가능", "조건부운영"})
    gate2_pass = str(paper.get("judgment", "보류")) == "운영가능"

    active_tab = 7
    if gate1_pass and not gate2_pass:
        active_tab = 8
    elif gate1_pass and gate2_pass:
        active_tab = 9

    created = checklist.get("generated_at") or dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    validation_status = (
        f"1단계:{'PASS' if gate1_pass else 'HOLD'} | "
        f"2단계:{'PASS' if gate2_pass else 'HOLD'} | "
        f"3단계:{'READY' if gate2_pass else 'LOCKED'}"
    )

    top_labels = [
        (1, "전략개요"),
        (2, "진입"),
        (3, "청산"),
        (4, "사이징"),
        (5, "리스크"),
        (6, "필터"),
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

    strategy_panels = "".join(
        [
            _render_strategy_panel(1, "전략개요"),
            _render_strategy_panel(2, "진입"),
            _render_strategy_panel(3, "청산"),
            _render_strategy_panel(4, "사이징"),
            _render_strategy_panel(5, "리스크"),
            _render_strategy_panel(6, "필터"),
        ]
    )

    overall_j = str(overall.get("judgment", "-"))
    overall_next = str(overall.get("next_step", "-"))

    default_cfg = {
        "cfg_s1_a": "전략명",
        "cfg_s1_b": "벤치마크",
        "cfg_s1_c": "거래시장",
        "cfg_s2_a": "진입조건1",
        "cfg_s2_b": "진입조건2",
        "cfg_s2_c": "진입조건3",
        "cfg_s3_a": "청산조건1",
        "cfg_s3_b": "청산조건2",
        "cfg_s3_c": "청산조건3",
        "cfg_s4_a": "기본비중",
        "cfg_s4_b": "최대비중",
        "cfg_s4_c": "증액단계",
        "cfg_s5_a": "일손실한도",
        "cfg_s5_b": "MDD한도",
        "cfg_s5_c": "긴급청산조건",
        "cfg_s6_a": "유동성필터",
        "cfg_s6_b": "변동성필터",
        "cfg_s6_c": "뉴스필터",
    }

    return f"""<!doctype html>
<html lang='ko'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>검증 콘솔</title>
<style>
:root {{ --bg:#eef2f7; --card:#fff; --ink:#1f2d3d; --muted:#5f7185; --line:#d8dee8; --ok:#22b35f; --bad:#dc3545; --warn:#d59f00; --nav:#24374a; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Segoe UI","Malgun Gothic",sans-serif; }}
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
.metric .v {{ font-size:26px; font-weight:700; margin-bottom:4px; }}
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
.item {{ font-weight:700; }}
.chip {{ display:inline-block; min-width:74px; text-align:center; border-radius:999px; padding:3px 8px; font-size:12px; font-weight:800; color:#fff; }}
.chip-pass {{ background:var(--ok); }} .chip-fail {{ background:var(--bad); }} .chip-warn {{ background:var(--warn); color:#111; }}
.plain {{ margin:0; padding-left:18px; font-size:14px; line-height:1.5; }}
.lock-banner {{ margin-bottom:10px; padding:10px; border:1px solid #f2c9cf; background:#fff4f6; color:#8f2130; border-radius:10px; font-size:13px; }}
.ctrl {{ display:flex; gap:8px; margin:8px 0; flex-wrap:wrap; }}
.act-btn {{ border:1px solid #b9cde0; background:#fff; color:#1f3650; border-radius:8px; padding:6px 10px; font-size:13px; font-weight:700; cursor:pointer; }}
.act-btn.danger {{ border-color:#e7b0b7; color:#8f2130; }}
.form-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:8px 12px; }}
.form-grid label {{ display:flex; flex-direction:column; gap:5px; font-size:13px; color:#4c6276; }}
.form-grid input {{ border:1px solid #c7d8e8; border-radius:8px; padding:7px 8px; font-size:14px; }}
.hint {{ color:#5a7288; font-size:12px; }}
.statusbar {{ position:fixed; left:0; right:0; bottom:0; background:#1d2b38; color:#eaf2f8; border-top:1px solid #3b4d60; }}
.status-inner {{ max-width:1400px; margin:0 auto; padding:9px 12px; display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; font-size:12px; }}
.status-chip {{ background:#2a3d50; border:1px solid #3d546a; border-radius:7px; padding:6px 8px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
</style>
</head>
<body>
<main class='wrap'>
<section class='top-tabs'>
{''.join(top_buttons)}
</section>
<section class='main-area'>
{strategy_panels}
{_render_backtest_panel(checklist, bt_report, final_output)}
{_render_paper_panel(paper)}
{_render_live_panel(live, gate2_pass)}
</section>
</main>
<footer class='statusbar'>
  <div class='status-inner'>
    <div class='status-chip' id='sb-mode'>현재 모드: {active_tab}.탭</div>
    <div class='status-chip'>전략 상태: {_escape(overall_j)} / next={_escape(overall_next)}</div>
    <div class='status-chip' id='sb-save'>마지막 저장: {_escape(created)}</div>
    <div class='status-chip' id='sb-val'>검증 상태: {_escape(validation_status)}</div>
  </div>
</footer>
<script>
(function () {{
  const STORAGE_KEY = 'trading_console_strategy_config_v1';
  const DEFAULT_CFG = {json.dumps(default_cfg, ensure_ascii=False)};

  const topTabs = Array.from(document.querySelectorAll('.top-tab'));
  const mainPanels = Array.from(document.querySelectorAll('.main-panel'));
  const sbMode = document.getElementById('sb-mode');
  const sbSave = document.getElementById('sb-save');
  const sbVal = document.getElementById('sb-val');
  const cfgInputs = Array.from(document.querySelectorAll('[data-sync]'));

  function nowText() {{
    const d = new Date();
    const p = (n) => String(n).padStart(2, '0');
    return `${{d.getFullYear()}}-${{p(d.getMonth()+1)}}-${{p(d.getDate())}} ${{p(d.getHours())}}:${{p(d.getMinutes())}}:${{p(d.getSeconds())}}`;
  }}

  function readCfg() {{
    try {{
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return {{ ...DEFAULT_CFG }};
      const obj = JSON.parse(raw);
      return {{ ...DEFAULT_CFG, ...obj }};
    }} catch (_) {{
      return {{ ...DEFAULT_CFG }};
    }}
  }}

  function writeCfg(cfg) {{
    localStorage.setItem(STORAGE_KEY, JSON.stringify(cfg));
    if (sbSave) sbSave.textContent = '마지막 저장: ' + nowText();
  }}

  function bindCfgToInputs(cfg) {{
    cfgInputs.forEach((el) => {{
      const k = el.getAttribute('data-sync');
      el.value = (cfg[k] ?? '').toString();
    }});
  }}

  function collectCfgFromInputs() {{
    const cfg = readCfg();
    cfgInputs.forEach((el) => {{
      const k = el.getAttribute('data-sync');
      cfg[k] = el.value || '';
    }});
    return cfg;
  }}

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
      const act = btn.getAttribute('data-act');
      if (act === 'save-config') {{
        const cfg = collectCfgFromInputs();
        writeCfg(cfg);
        return;
      }}
      if (act === 'reset-config') {{
        const cfg = {{ ...DEFAULT_CFG }};
        writeCfg(cfg);
        bindCfgToInputs(cfg);
        return;
      }}
      const kind = btn.getAttribute('data-kind') || 'system';
      if (sbVal) sbVal.textContent = '검증 상태: ' + kind + ' ' + act + ' 요청됨';
    }});
  }});

  const cfg = readCfg();
  bindCfgToInputs(cfg);
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
    ap.add_argument("--out-html", default="")
    args = ap.parse_args()

    checklist_path = Path(args.checklist_json)
    report_path = Path(args.report_json)
    final_path = Path(args.final_json)
    trading_stage_path = Path(args.trading_stage_json)

    checklist = _load_json(checklist_path)
    report = _load_json(report_path)
    final_output = _load_json(final_path)
    trading_stage = _load_json(trading_stage_path)

    if not checklist:
        raise FileNotFoundError(f"checklist json not found or empty: {checklist_path}")

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_html = Path(args.out_html) if args.out_html else (LOG_DIR / f"backtest_validation_screen_{stamp}.html")
    latest_html = LOG_DIR / "backtest_validation_screen_latest.html"

    html = _render(checklist, report, final_output, trading_stage)
    out_html.write_text(html, encoding="utf-8")
    latest_html.write_text(html, encoding="utf-8")

    print(f"[BTSCR] html={out_html}")
    print(f"[BTSCR] latest={latest_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

