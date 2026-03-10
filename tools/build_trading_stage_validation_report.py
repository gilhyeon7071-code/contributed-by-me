from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

PASS = "PASS"
FAIL = "FAIL"
NE = "NOT_EVALUABLE"


# Freshness window for auxiliary state logs (p0/gate/after_close)
RISK_STATE_FRESH_DAYS = 7.0


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _latest_by_glob(pattern: str) -> Optional[Path]:
    files = sorted(LOG_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _age_days(path: Path) -> float:
    if not path.exists():
        return float("nan")
    ts = dt.datetime.fromtimestamp(path.stat().st_mtime)
    return (dt.datetime.now() - ts).total_seconds() / 86400.0


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


def _fmt_num(v: Any) -> str:
    f = _safe_float(v)
    return "-" if not _is_finite(f) else f"{f:.4g}"


def _fmt_pct(v: Any) -> str:
    f = _safe_float(v)
    return "-" if not _is_finite(f) else f"{(f * 100.0):.2f}%"


def _make_item(
    name: str,
    status: str,
    metric: str,
    threshold: str,
    issue: str,
    action: str,
    required: bool = True,
) -> Dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "metric": metric,
        "threshold": threshold,
        "issue": issue,
        "action": action,
        "required": required,
    }


def _judge(items: List[Dict[str, Any]], stage: str) -> Tuple[str, Dict[str, int]]:
    req = [x for x in items if bool(x.get("required", True))]
    pass_n = sum(1 for x in req if x.get("status") == PASS)
    fail_n = sum(1 for x in req if x.get("status") == FAIL)
    ne_n = sum(1 for x in req if x.get("status") == NE)
    total = len(req)

    if fail_n > 0:
        judgment = "보류"
    elif ne_n > 0:
        judgment = "조건부"
    else:
        judgment = "운영가능"

    if stage == "live":
        canary_exec = next((x for x in items if x.get("name") == "canary_execute_mode"), None)
        if canary_exec and canary_exec.get("status") == NE and judgment == "운영가능":
            judgment = "실주문대기"

    return judgment, {"total": total, "pass_n": pass_n, "fail_n": fail_n, "not_evaluable_n": ne_n}


def _resolve_paper_risk_off(p_pnl: Path, pnl: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve risk_off state from freshest reliable source.

    Priority by freshness among known sources:
    - p0_daily_check_*.json (risk_off.enabled)
    - gate_daily_*.json (snapshot.flags.risk_off_enabled or gate1 status)
    - after_close_summary_last.json (p0.risk_off or risk_off)
    - embedded paper_pnl_summary_last.after_close.risk_off
    """
    candidates: List[Dict[str, Any]] = []

    # 1) p0 latest
    p0_path = _latest_by_glob("p0_daily_check_*.json")
    p0 = _read_json(p0_path) if p0_path else {}
    p0_val = ((p0.get("risk_off") or {}).get("enabled") if p0 else None)
    if isinstance(p0_val, bool) and p0_path:
        candidates.append({"source": "p0_daily_check", "value": p0_val, "path": p0_path, "age_days": _age_days(p0_path)})

    # 2) gate latest
    gate_path = _latest_by_glob("gate_daily_*.json")
    gate = _read_json(gate_path) if gate_path else {}
    gate_val: Optional[bool] = None
    if gate:
        flags = (((gate.get("snapshot") or {}).get("flags") or {}))
        if isinstance(flags.get("risk_off_enabled"), bool):
            gate_val = bool(flags.get("risk_off_enabled"))
        else:
            g1 = ((gate.get("gate1") or {}).get("status"))
            if isinstance(g1, str):
                gate_val = (g1.upper() != "PASS")
    if isinstance(gate_val, bool) and gate_path:
        candidates.append({"source": "gate_daily", "value": gate_val, "path": gate_path, "age_days": _age_days(gate_path)})

    # 3) after_close_summary_last
    ac_path = LOG_DIR / "after_close_summary_last.json"
    ac = _read_json(ac_path)
    ac_val: Optional[bool] = None
    if ac:
        v1 = (ac.get("p0") or {}).get("risk_off")
        v2 = ac.get("risk_off")
        if isinstance(v1, bool):
            ac_val = v1
        elif isinstance(v2, bool):
            ac_val = v2
    if isinstance(ac_val, bool) and ac_path.exists():
        candidates.append({"source": "after_close_summary_last", "value": ac_val, "path": ac_path, "age_days": _age_days(ac_path)})

    # 4) embedded in pnl summary
    emb_val = ((pnl.get("after_close") or {}).get("risk_off") if pnl else None)
    if isinstance(emb_val, bool) and p_pnl.exists():
        candidates.append({"source": "paper_pnl_summary_embedded", "value": emb_val, "path": p_pnl, "age_days": _age_days(p_pnl)})

    if not candidates:
        return {
            "known": False,
            "value": None,
            "source": None,
            "age_days": float("nan"),
            "inputs": [],
            "p0_path": str(p0_path) if p0_path else None,
            "gate_path": str(gate_path) if gate_path else None,
            "after_close_path": str(ac_path),
        }

    # Prefer fresh candidates; if none are fresh, still choose newest as fail-closed visibility.
    fresh = [c for c in candidates if _is_finite(_safe_float(c.get("age_days"))) and float(c.get("age_days", 9999.0)) <= RISK_STATE_FRESH_DAYS]
    pool = fresh if fresh else candidates
    pool = sorted(pool, key=lambda c: float(c.get("age_days", 9999.0)))
    chosen = pool[0]

    return {
        "known": True,
        "value": bool(chosen.get("value")),
        "source": str(chosen.get("source")),
        "age_days": float(chosen.get("age_days", float("nan"))),
        "inputs": [
            {
                "source": str(c.get("source")),
                "value": bool(c.get("value")) if isinstance(c.get("value"), bool) else None,
                "age_days": float(c.get("age_days", float("nan"))),
                "path": str(c.get("path")),
            }
            for c in sorted(candidates, key=lambda x: float(x.get("age_days", 9999.0)))
        ],
        "p0_path": str(p0_path) if p0_path else None,
        "gate_path": str(gate_path) if gate_path else None,
        "after_close_path": str(ac_path),
    }


def _build_paper_stage() -> Dict[str, Any]:
    p_pnl = LOG_DIR / "paper_pnl_summary_last.json"
    p_fb = LOG_DIR / "live_vs_bt_feedback_latest.json"

    pnl = _read_json(p_pnl)
    fb = _read_json(p_fb)

    items: List[Dict[str, Any]] = []

    age = _age_days(p_pnl)
    if _is_finite(age):
        st = PASS if age <= 3.0 else FAIL
        items.append(
            _make_item(
                "paper_freshness",
                st,
                f"age_days={_fmt_num(age)}",
                "<=3",
                "최근 가상매매 결과 파일 최신성",
                "run_paper_daily.bat 재실행",
            )
        )
    else:
        items.append(_make_item("paper_freshness", NE, "age_days=-", "<=3", "가상매매 요약 파일 없음", "run_paper_daily.bat 실행"))

    trades = int(pnl.get("trades_used", 0)) if pnl else 0
    if pnl:
        items.append(
            _make_item(
                "paper_trade_count",
                PASS if trades >= 50 else FAIL,
                f"trades_used={trades}",
                ">=50",
                "가상매매 표본 거래 수",
                "표본 50회 이상 누적",
            )
        )
    else:
        items.append(_make_item("paper_trade_count", NE, "trades_used=-", ">=50", "가상매매 요약 파일 없음", "run_paper_daily.bat 실행"))

    mdd = _safe_float(((pnl.get("equity") or {}).get("max_drawdown_pct")) if pnl else float("nan"))
    if _is_finite(mdd):
        items.append(
            _make_item(
                "paper_mdd",
                PASS if mdd >= -0.30 else FAIL,
                f"max_drawdown={_fmt_pct(mdd)}",
                ">=-30.00%",
                "가상매매 최대 낙폭",
                "포지션 축소/손절 강화",
            )
        )
    else:
        items.append(_make_item("paper_mdd", NE, "max_drawdown=-", ">=-30.00%", "낙폭 계산값 없음", "paper_pnl_summary 생성"))

    risk_state = _resolve_paper_risk_off(p_pnl=p_pnl, pnl=pnl)
    if bool(risk_state.get("known")):
        risk_off = bool(risk_state.get("value"))
        src = str(risk_state.get("source") or "-")
        src_age = _fmt_num(risk_state.get("age_days"))
        items.append(
            _make_item(
                "paper_risk_off_state",
                PASS if not risk_off else FAIL,
                f"risk_off={risk_off} (src={src}, age_days={src_age})",
                "False",
                "현재 리스크오프 상태",
                "kill-switch 원인 해소 후 재검증",
            )
        )
    else:
        items.append(_make_item("paper_risk_off_state", NE, "risk_off=-", "False", "리스크 상태 데이터 없음", "after_close_summary 생성"))

    align_ready = bool(((fb.get("comparison") or {}).get("alignment_ready")) if fb else False)
    if fb:
        items.append(
            _make_item(
                "paper_bt_alignment",
                PASS if align_ready else FAIL,
                f"alignment_ready={align_ready}",
                "True",
                "백테스트 대비 정렬비교 준비 상태",
                "백테스트/라이브 비교 윈도우 재구성",
            )
        )
    else:
        items.append(_make_item("paper_bt_alignment", NE, "alignment_ready=-", "True", "비교 피드백 파일 없음", "run_live_vs_bt_paper_daily.bat 실행"))

    abs_diff = _safe_float(((fb.get("divergence") or {}).get("abs_diff")) if fb else float("nan"))
    if _is_finite(abs_diff):
        items.append(
            _make_item(
                "paper_return_divergence",
                PASS if abs_diff <= 0.30 else FAIL,
                f"abs_diff={_fmt_pct(abs_diff)}",
                "<=30.00%",
                "가상매매 vs 백테스트 수익률 괴리",
                "전략/비용/체결 가정 재보정",
            )
        )
    else:
        items.append(_make_item("paper_return_divergence", NE, "abs_diff=-", "<=30.00%", "괴리 계산 데이터 부족", "비교 표본 확장"))

    optimize_gate_ok = bool(((fb.get("optimize") or {}).get("gate_ok")) if fb else False)
    if fb:
        items.append(
            _make_item(
                "paper_quality_gate",
                PASS if optimize_gate_ok else FAIL,
                f"optimize_gate_ok={optimize_gate_ok}",
                "True",
                "가상매매 품질 게이트",
                "oos 품질 기준 재검토/전략 보정",
            )
        )
    else:
        items.append(_make_item("paper_quality_gate", NE, "optimize_gate_ok=-", "True", "품질 게이트 데이터 없음", "live_vs_bt_feedback 생성"))

    judgment, counts = _judge(items, stage="paper")
    key_actions = [x["action"] for x in items if x["status"] in {FAIL, NE}][:4]

    return {
        "stage": "paper",
        "title": "2단계 가상매매 검증",
        "judgment": judgment,
        "counts": counts,
        "items": items,
        "key_actions": key_actions,
        "sources": {
            "paper_pnl_summary": str(p_pnl),
            "live_vs_bt_feedback": str(p_fb),
            "after_close_summary_last": str(LOG_DIR / "after_close_summary_last.json"),
            "p0_daily_check_latest": str(_latest_by_glob("p0_daily_check_*.json") or ""),
            "gate_daily_latest": str(_latest_by_glob("gate_daily_*.json") or ""),
            "risk_state_fresh_days": RISK_STATE_FRESH_DAYS,
            "risk_state_inputs": risk_state.get("inputs", []),
        },
    }


def _build_live_stage() -> Dict[str, Any]:
    p_e2e = LOG_DIR / "kis_intraday_e2e_latest.json"
    p_fault = LOG_DIR / "kis_fault_injection_latest.json"
    p_canary = LOG_DIR / "kis_live_canary_first_latest.json"

    e2e = _read_json(p_e2e)
    fault = _read_json(p_fault)
    canary = _read_json(p_canary)

    items: List[Dict[str, Any]] = []

    age_e2e = _age_days(p_e2e)
    if _is_finite(age_e2e):
        items.append(
            _make_item(
                "live_e2e_freshness",
                PASS if age_e2e <= 7.0 else FAIL,
                f"age_days={_fmt_num(age_e2e)}",
                "<=7",
                "실전 E2E 최신성",
                "run_kis_intraday_e2e.bat 재실행",
            )
        )
    else:
        items.append(_make_item("live_e2e_freshness", NE, "age_days=-", "<=7", "E2E 결과 없음", "run_kis_intraday_e2e.bat 실행"))

    if e2e:
        ok = bool(e2e.get("ok", False))
        items.append(
            _make_item(
                "live_e2e_ok",
                PASS if ok else FAIL,
                f"ok={ok}, pass_n={e2e.get('pass_n','-')}, fail_n={e2e.get('fail_n','-')}",
                "ok=True",
                "장중 E2E 시나리오",
                "E2E 실패 단계 원인 수정",
            )
        )
    else:
        items.append(_make_item("live_e2e_ok", NE, "ok=-", "ok=True", "E2E 결과 없음", "run_kis_intraday_e2e.bat 실행"))

    if fault:
        ok = bool(fault.get("ok", False))
        items.append(
            _make_item(
                "live_fault_injection",
                PASS if ok else FAIL,
                f"ok={ok}, pass_n={fault.get('pass_n','-')}, fail_n={fault.get('fail_n','-')}",
                "ok=True",
                "장애주입 시나리오",
                "fault 시나리오 실패 케이스 수정",
            )
        )
    else:
        items.append(_make_item("live_fault_injection", NE, "ok=-", "ok=True", "fault 결과 없음", "run_kis_fault_injection.bat 실행"))

    if canary:
        ok = bool(canary.get("ok", False))
        items.append(
            _make_item(
                "live_canary_gate",
                PASS if ok else FAIL,
                f"ok={ok}, mode={canary.get('mode','-')}, mock={canary.get('mock','-')}",
                "ok=True",
                "소액 실전 canary 게이트",
                "canary step 실패 항목 수정",
            )
        )

        execute = canary.get("execute", None)
        items.append(
            _make_item(
                "canary_execute_mode",
                PASS if execute is True else NE,
                f"execute={execute}",
                "True",
                "실주문 실행 여부",
                "실주문 전환 시 CANARY_EXECUTE=1 설정",
                required=False,
            )
        )

        pre = next((x for x in (canary.get("steps") or []) if str(x.get("name")) == "preflight_healthcheck"), None)
        if isinstance(pre, dict):
            pre_ok = bool(pre.get("ok", False))
            items.append(
                _make_item(
                    "live_preflight_health",
                    PASS if pre_ok else FAIL,
                    f"ok={pre_ok}, returncode={pre.get('returncode','-')}",
                    "ok=True",
                    "실전 전 헬스체크",
                    "API 계정/키/잔고 환경 재확인",
                )
            )
        else:
            items.append(_make_item("live_preflight_health", NE, "ok=-", "ok=True", "canary preflight 기록 없음", "canary 재실행"))
    else:
        items.append(_make_item("live_canary_gate", NE, "ok=-", "ok=True", "canary 결과 없음", "run_live_canary_first_test.bat 실행"))
        items.append(_make_item("canary_execute_mode", NE, "execute=-", "True", "실주문 실행 여부 확인 불가", "canary 결과 생성", required=False))
        items.append(_make_item("live_preflight_health", NE, "ok=-", "ok=True", "preflight 기록 없음", "canary 재실행"))

    judgment, counts = _judge(items, stage="live")
    key_actions = [x["action"] for x in items if x["status"] in {FAIL, NE}][:4]

    return {
        "stage": "live",
        "title": "3단계 실전매매 검증",
        "judgment": judgment,
        "counts": counts,
        "items": items,
        "key_actions": key_actions,
        "sources": {"e2e": str(p_e2e), "fault": str(p_fault), "canary": str(p_canary)},
    }


def _overall(paper: Dict[str, Any], live: Dict[str, Any]) -> Dict[str, Any]:
    pj = str(paper.get("judgment", "보류"))
    lj = str(live.get("judgment", "보류"))

    if pj == "보류":
        return {"judgment": "가상매매 보류", "message": "가상매매 게이트 미통과. 실전 전환 불가", "next_step": "paper_fix"}
    if pj in {"조건부"}:
        return {"judgment": "가상매매 보강", "message": "가상매매 조건부 상태. 부족 항목 보강 필요", "next_step": "paper_recheck"}
    if lj in {"보류"}:
        return {"judgment": "실전준비 보류", "message": "실전 게이트 미통과. canary/e2e/fault 보강 필요", "next_step": "live_fix"}
    if lj in {"조건부", "실주문대기"}:
        return {"judgment": "실전준비 조건부", "message": "실전 실행 전 마지막 조건 확인 필요", "next_step": "live_canary"}
    return {"judgment": "실전전환 가능", "message": "가상/실전 게이트 기준 충족", "next_step": "go_live"}


def build_report() -> Dict[str, Any]:
    paper = _build_paper_stage()
    live = _build_live_stage()
    overall = _overall(paper, live)
    return {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "paper": paper,
        "live": live,
        "overall": overall,
    }


def _render_md(rep: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# 가상/실전 검증 리포트 ({rep.get('generated_at')})")
    lines.append("")
    ov = rep.get("overall", {})
    lines.append(f"- overall: **{ov.get('judgment','-')}**")
    lines.append(f"- message: {ov.get('message','-')}")
    lines.append(f"- next_step: `{ov.get('next_step','-')}`")

    for key in ("paper", "live"):
        st = rep.get(key, {})
        cnt = st.get("counts", {})
        lines.append("")
        lines.append(f"## {st.get('title', key)}")
        lines.append("")
        lines.append(f"- judgment: **{st.get('judgment','-')}**")
        lines.append(
            f"- required checks: total={cnt.get('total',0)}, pass={cnt.get('pass_n',0)}, fail={cnt.get('fail_n',0)}, ne={cnt.get('not_evaluable_n',0)}"
        )
        lines.append("")
        lines.append("| 항목 | 상태 | 지표 | 기준 | 이슈 | 조치 |")
        lines.append("|---|---|---|---|---|---|")
        for it in st.get("items", []):
            lines.append(
                f"| {it.get('name')} | {it.get('status')} | {it.get('metric')} | {it.get('threshold')} | {it.get('issue')} | {it.get('action')} |"
            )
        acts = st.get("key_actions", []) or []
        if acts:
            lines.append("")
            lines.append("- 우선 조치")
            for a in acts:
                lines.append(f"  - {a}")

    return "\n".join(lines) + "\n"


def _status_class(status: str) -> str:
    s = str(status)
    if s == PASS:
        return "pass"
    if s == FAIL:
        return "fail"
    return "warn"


def _render_stage_table(stage: Dict[str, Any]) -> str:
    rows = []
    for it in stage.get("items", []):
        cls = _status_class(str(it.get("status", NE)))
        rows.append(
            "<tr>"
            f"<td>{it.get('name','-')}</td>"
            f"<td><span class='chip chip-{cls}'>{it.get('status','-')}</span></td>"
            f"<td>{it.get('metric','-')}</td>"
            f"<td>{it.get('threshold','-')}</td>"
            f"<td>{it.get('issue','-')}</td>"
            f"<td>{it.get('action','-')}</td>"
            "</tr>"
        )
    cnt = stage.get("counts", {})
    return (
        "<section class='card'>"
        f"<h2>{stage.get('title','-')}</h2>"
        f"<p class='judge'>판정: <b>{stage.get('judgment','-')}</b> | total={cnt.get('total',0)} pass={cnt.get('pass_n',0)} fail={cnt.get('fail_n',0)} ne={cnt.get('not_evaluable_n',0)}</p>"
        "<div class='table-wrap'><table>"
        "<thead><tr><th>항목</th><th>상태</th><th>지표</th><th>기준</th><th>이슈</th><th>조치</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
        "</section>"
    )


def _render_html(rep: Dict[str, Any]) -> str:
    ov = rep.get("overall", {})
    paper = rep.get("paper", {})
    live = rep.get("live", {})
    return f"""<!doctype html>
<html lang='ko'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>가상/실전 검증 리포트</title>
<style>
:root {{ --bg:#eef2f7; --card:#fff; --line:#d8dee8; --ink:#1f2d3d; --muted:#5f7185; --pass:#22b35f; --fail:#dc3545; --warn:#d59f00; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Segoe UI","Malgun Gothic",sans-serif; }}
.container {{ max-width:1320px; margin:14px auto 24px; padding:0 12px; }}
.hero {{ background:linear-gradient(135deg,#2d435a,#3a8ed0); color:#fff; border-radius:12px; padding:18px 22px; margin-bottom:12px; }}
.hero h1 {{ margin:0 0 6px; font-size:36px; }}
.hero p {{ margin:0; opacity:.95; }}
.summary {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:10px; margin-bottom:12px; }}
.metric {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; }}
.metric .k {{ font-size:13px; color:var(--muted); margin-bottom:6px; }}
.metric .v {{ font-size:26px; font-weight:700; }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; margin-bottom:12px; }}
.card h2 {{ margin:0 0 6px; font-size:24px; }}
.judge {{ margin:0 0 10px; color:#314a62; }}
.table-wrap {{ overflow-x:auto; }}
table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
th,td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; font-size:13px; word-break:break-word; }}
th {{ background:#f6f9fd; color:#38506b; }}
.chip {{ display:inline-block; min-width:90px; text-align:center; border-radius:999px; padding:3px 8px; color:#fff; font-size:12px; font-weight:700; }}
.chip-pass {{ background:var(--pass); }} .chip-fail {{ background:var(--fail); }} .chip-warn {{ background:var(--warn); color:#111; }}
</style>
</head>
<body>
<main class='container'>
<section class='hero'>
  <h1>가상/실전 검증 보고서</h1>
  <p>생성: {rep.get('generated_at','-')}</p>
</section>
<section class='summary'>
  <div class='metric'><div class='k'>전체 판정</div><div class='v'>{ov.get('judgment','-')}</div></div>
  <div class='metric'><div class='k'>다음 단계</div><div class='v'>{ov.get('next_step','-')}</div></div>
  <div class='metric'><div class='k'>가상매매</div><div class='v'>{paper.get('judgment','-')}</div></div>
  <div class='metric'><div class='k'>실전매매</div><div class='v'>{live.get('judgment','-')}</div></div>
</section>
{_render_stage_table(paper)}
{_render_stage_table(live)}
</main>
</body>
</html>
"""


def main() -> int:
    ap = argparse.ArgumentParser(description="Build paper/live validation report from latest logs")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--out-md", default="")
    ap.add_argument("--out-html", default="")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    rep = build_report()

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"trading_stage_validation_{stamp}.json")
    out_md = Path(args.out_md) if args.out_md else (LOG_DIR / f"trading_stage_validation_{stamp}.md")
    out_html = Path(args.out_html) if args.out_html else (LOG_DIR / f"trading_stage_validation_{stamp}.html")

    latest_json = LOG_DIR / "trading_stage_validation_latest.json"
    latest_md = LOG_DIR / "trading_stage_validation_latest.md"
    latest_html = LOG_DIR / "trading_stage_validation_latest.html"

    out_json.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    latest_json.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    md = _render_md(rep)
    out_md.write_text(md, encoding="utf-8-sig")
    latest_md.write_text(md, encoding="utf-8-sig")

    html = _render_html(rep)
    out_html.write_text(html, encoding="utf-8")
    latest_html.write_text(html, encoding="utf-8")

    print(f"[TRVAL] overall={rep.get('overall',{}).get('judgment','-')} next={rep.get('overall',{}).get('next_step','-')}")
    print(f"[TRVAL] json={out_json}")
    print(f"[TRVAL] md={out_md}")
    print(f"[TRVAL] html={out_html}")
    print(f"[TRVAL] latest_json={latest_json}")
    print(f"[TRVAL] latest_md={latest_md}")
    print(f"[TRVAL] latest_html={latest_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
