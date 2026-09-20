from __future__ import annotations

import json
import os
import re
import sys
import csv
from datetime import datetime
from pathlib import Path
from typing import Any
import logging


BASE = Path(os.getenv("ROOTA", str(Path(__file__).resolve().parents[1])))
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
from holiday_manager import HolidayManager

LOGS = BASE / "2_Logs"
ROOT_B = Path(os.getenv("ROOTB", str(BASE.parent / "vibe" / "buffett")))
RUNS = ROOT_B / "runs"
LOGIC_ROOT = Path(os.getenv("LOGIC_JUDGEMENT_ROOT", str(BASE.parent / "vibe" / "checking_logic")))
CHECKING = LOGIC_ROOT / "artifacts" / "system_completeness"
PLANS_PATH = BASE / "PLANS.md"

RUN_LOG_PATH = LOGS / "run_paper_daily_last.txt"
INTRADAY_PREFLIGHT_STATUS_PATH = LOGS / "intraday_preflight_run_paper_daily_status_latest.json"
PAPER_STATE_PATH = BASE / "paper" / "paper_state.json"
PENDING_PATH = LOGS / "pending_entry_status_latest.json"
PENDING_SIGNALS_PATH = LOGS / "pending_entry_signals_latest.csv"
LIQUIDITY_PATH = LOGS / "liquidity_filter_daily_last.json"
NEWS_COLLECT_PATH = LOGS / "news_collect_status_latest.json"
NEWS_SCORE_PATH = LOGS / "news_score_status_latest.json"
FINAL_SCORE_PATH = LOGS / "final_score_merge_status_latest.json"
P1_GATE_STATUS_PATH = LOGS / "p1_entry_gate_status_latest.json"
LIVE_VS_BT_FEEDBACK_PATH = LOGS / "live_vs_bt_feedback_latest.json"
BACKTEST_CHECKLIST_PATH = LOGS / "backtest_validation_checklist_latest.json"
DASHBOARD_PATH = RUNS / "dashboard_state_latest.json"
SYSTEM_COMPLETENESS_PATH = CHECKING / "system_completeness_report.json"
OUTPUT_LATEST = LOGS / "integrated_ops_snapshot_latest.json"
CANDIDATES_META_PATH = LOGS / "candidates_latest_meta.json"
FRESHNESS_SOURCE_LAST_PATH = LOGS / "freshness_source_last.json"

P0_PATTERN = "p0_daily_check_20*.json"
P0_REPORT_RE = re.compile(r"^p0_daily_check_\d{8}_\d{6}\.json$")
AUDIT_PATTERN = "audit_daily_20*.json"
SIGNAL_PATTERN = "signal_integration_status_20*.json"
GATE_PATTERN = "gate_daily_20*.json"

AUTO_START = "<!-- AUTO-INTEGRATED-OPS:START -->"
AUTO_END = "<!-- AUTO-INTEGRATED-OPS:END -->"




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
def _clean_text(value: Any, default: str = "-") -> str:
    text = str(value if value is not None else default)
    text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text or default


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_json_utf8_sig(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _is_future_close_cutoff_recheck_pending(path: Path, pending_queue_len: int, base_ymd: str) -> bool:
    if pending_queue_len <= 0 or not base_ymd or not path.exists():
        return False
    try:
        rows = _read_csv_rows(path)
    except Exception:
        return False
    if len(rows) != pending_queue_len:
        return False
    for row in rows:
        carry_reason = str(row.get("carry_reason") or "").strip().upper()
        carry_origin = str(row.get("carry_origin_reason") or "").strip().upper()
        entry_day_override = re.sub(r"[^0-9]", "", str(row.get("entry_day_override") or ""))
        if carry_reason != "ENTRY_RECHECK_READY":
            return False
        if carry_origin != "CLOSE_CUTOFF_RECHECK":
            return False
        if len(entry_day_override) < 8 or entry_day_override[:8] <= base_ymd:
            return False
    return True


def _latest(pattern: str) -> Path | None:
    matches = sorted(LOGS.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _latest_p0_report() -> Path | None:
    matches = [p for p in LOGS.glob(P0_PATTERN) if P0_REPORT_RE.match(p.name)]
    matches = sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _read_freshness_source() -> tuple[Path | None, dict[str, Any]]:
    source_path: Path | None = None
    if FRESHNESS_SOURCE_LAST_PATH.exists():
        try:
            pointer = _read_json(FRESHNESS_SOURCE_LAST_PATH)
            raw_last = str(pointer.get("last") or "").strip()
            if raw_last:
                source_path = Path(raw_last)
        except Exception:
            source_path = None
    if source_path is None:
        source_path = _latest("freshness_source_20*.json")
    if source_path and source_path.exists():
        try:
            return source_path, _read_json(source_path)
        except Exception:
            return source_path, {}
    return None, {}


def _status_from_bool(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _dashboard_actionable_alerts(alerts: Any) -> list[dict[str, Any]]:
    rows = alerts if isinstance(alerts, list) else []
    ignored_codes = {"RELAX_HIGH", "RUNTIME_CHAIN_GUARD_WARN", "INTEGRATED_OPS_EFFECTIVE_NOT_PASS"}
    actionable: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("code") or "").strip().upper()
        severity = str(row.get("severity") or "").strip().upper()
        if code in ignored_codes:
            continue
        if severity in {"WARN", "WARNING", "FAIL", "ERROR", "CRITICAL", "BLOCK"}:
            actionable.append(row)
    return actionable


def _extract_ymd_from_path(path: Path | None) -> str:
    if path is None:
        return ""
    m = re.search(r"(\d{8})", path.name)
    return m.group(1) if m else ""


def _extract_obj_ymd(obj: Any, *keys: str) -> str:
    if not isinstance(obj, dict):
        return ""
    for key in keys:
        raw = obj.get(key)
        text = re.sub(r"[^0-9]", "", str(raw or "")).strip()
        if len(text) >= 8:
            return text[:8]
    return ""


def _previous_business_day_ymd(ymd: str) -> str:
    day8 = str(ymd or "").strip()
    if len(day8) != 8 or not day8.isdigit():
        return day8
    return HolidayManager().previous_trading_day(day8) or day8


def _is_trading_day_ymd(ymd: str) -> bool:
    day8 = str(ymd or "").strip()
    if len(day8) != 8 or not day8.isdigit():
        return False
    return HolidayManager().is_market_open(day8)


def _priority_rank(key: str) -> int:
    ranks = {
        "stale_latest_chain": 1,
        "entry_capacity_zero": 2,
        "pending_entry_flow": 1,
        "dashboard_health": 2,
        "sector_pipeline_runtime": 3,
    }
    return ranks.get(key, 99)


def _calc_issue_state(ok: bool, reason: str) -> tuple[str, str]:
    return ("NORMAL", "정상") if ok else ("ISSUE", reason)


def _basename(path: str | None) -> str:
    if not path:
        return "-"
    try:
        return Path(path).name
    except Exception:
        return str(path)


def _run_log_summary(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8", errors="replace")
    last_start_idx = text.rfind("[START] run_paper_daily.bat")
    latest_block = text[last_start_idx:] if last_start_idx >= 0 else text
    start_line = latest_block.splitlines()[0] if latest_block.splitlines() else ""
    start_ts = ""
    m_start = re.search(r"ts=([0-9:\-T]+)", start_line)
    if m_start:
        start_ts = str(m_start.group(1) or "")
    strong_ok_markers = [
        latest_block.rfind("[OK] finished"),
        latest_block.rfind("[WRAPPER_EXIT] rc=0"),
    ]
    fallback_ok_markers = [
        latest_block.rfind("[9/9] END rc=0"),
        latest_block.rfind("[16.85/16] END rc=0"),
        latest_block.rfind("[INNER_EXIT] rc=0"),
    ]
    strong_ok_candidates = [idx for idx in strong_ok_markers if idx >= 0]
    fallback_ok_candidates = [idx for idx in fallback_ok_markers if idx >= 0]
    ok_idx = max(strong_ok_candidates + fallback_ok_candidates) if (strong_ok_candidates or fallback_ok_candidates) else -1
    fail_idx = latest_block.rfind("[FAILED]")
    # Prefer the latest terminal marker after the latest explicit START block.
    # The wrapper can append multiple run fragments into one log; the first OK
    # marker may belong to an older shadow-only fragment.
    terminal = "IN_PROGRESS"
    if ok_idx >= 0 and ok_idx > fail_idx:
        terminal = "OK"
    elif fail_idx >= 0:
        terminal = "FAILED"
    end_idx = len(latest_block)
    if terminal == "OK" and ok_idx >= 0:
        end_idx = ok_idx
    elif terminal == "FAILED" and fail_idx >= 0:
        end_idx = fail_idx
    prev_terminals = [
        latest_block.rfind("[OK] finished", 0, end_idx),
        latest_block.rfind("[INNER_EXIT]", 0, end_idx),
        latest_block.rfind("[WRAPPER_EXIT]", 0, end_idx),
        latest_block.rfind("[FAILED]", 0, end_idx),
    ]
    scope_start = max([idx for idx in prev_terminals if idx >= 0] or [0])
    if scope_start > 0:
        line_end = latest_block.find("\n", scope_start)
        scope_start = line_end + 1 if line_end >= 0 else scope_start
    run_scope = latest_block[scope_start:end_idx] if end_idx > scope_start else latest_block[:end_idx]
    terminal_scope = latest_block[:end_idx] if end_idx > 0 else latest_block

    has_main = "run_label=main" in terminal_scope
    has_shadow = "run_label=shadow" in terminal_scope
    preflight_running = False
    if INTRADAY_PREFLIGHT_STATUS_PATH.exists():
        try:
            preflight_status = _read_json(INTRADAY_PREFLIGHT_STATUS_PATH)
            preflight_running = str(preflight_status.get("status") or "").strip().upper() == "RUNNING"
        except Exception:
            preflight_running = False
    run_in_progress = bool(preflight_running or (ok_idx < 0 and fail_idx < 0))
    # [2026-08-31] shadow 는 2026-08-20 에 공식 은퇴했다.
    #   run_paper_daily.bat [7.1/9] "shadow_collect retired 2026-08-20",
    #   PAPER_SHADOW_ENABLED 기본값 0. 그런데 이 판정은 여전히 shadow 를 **필수로** 요구해서
    #   배치가 아무리 정상 완주해도 ok 가 False 였다.
    #   실측: 08-28 이후 batch_status 가 FAIL/PARTIAL 뿐이고 OK 가 한 번도 없다.
    #   -> dashboard_overall=FAIL -> run_daily_auto_sync.ps1 exit 20 -> STOC_FullAuto rc=20.
    #   은퇴한 부품을 필수로 요구하는 조건이었다. 켜져 있을 때만 요구한다.
    shadow_expected = str(os.environ.get("PAPER_SHADOW_ENABLED", "0")).strip() == "1"
    ok = bool(terminal == "OK" and has_main and (has_shadow or not shadow_expected))
    status = "PARTIAL" if run_in_progress else _status_from_bool(ok)
    current_state = (
        "latest run_paper_daily in_progress"
        if run_in_progress
        else ("latest run_paper_daily completed" if ok else "latest run_paper_daily failed_or_incomplete")
    )

    main_fill = re.search(r"run_label=main.*?new_fills=(\d+).*?open_positions=(\d+)", terminal_scope, re.S)
    shadow_fill = re.search(r"run_label=shadow.*?new_fills=(\d+).*?open_positions=(\d+)", terminal_scope, re.S)
    return {
        "status": status,
        "current_state": current_state,
        "in_progress": run_in_progress,
        "start_ts": start_ts,
        "main_new_fills": int(main_fill.group(1)) if main_fill else None,
        "main_open_positions": int(main_fill.group(2)) if main_fill else None,
        "shadow_new_fills": int(shadow_fill.group(1)) if shadow_fill else None,
        "shadow_open_positions": int(shadow_fill.group(2)) if shadow_fill else None,
        "evidence_path": str(path),
    }


def _paper_state_open_positions(path: Path) -> int | None:
    try:
        state = _read_json_utf8_sig(path)
    except Exception:
        return None
    rows = state.get("open_positions")
    if isinstance(rows, list):
        return len(rows)
    return None


def _build_snapshot() -> dict[str, Any]:
    today_ymd = datetime.now().strftime("%Y%m%d")
    pending = _read_json(PENDING_PATH)
    liquidity = _read_json(LIQUIDITY_PATH)
    news_collect = _read_json(NEWS_COLLECT_PATH)
    news_score = _read_json(NEWS_SCORE_PATH)
    final_score = _read_json(FINAL_SCORE_PATH)
    candidates_meta = _read_json(CANDIDATES_META_PATH) if CANDIDATES_META_PATH.exists() else {}
    p1_gate = _read_json(P1_GATE_STATUS_PATH) if P1_GATE_STATUS_PATH.exists() else {}
    live_vs_bt_feedback = _read_json(LIVE_VS_BT_FEEDBACK_PATH) if LIVE_VS_BT_FEEDBACK_PATH.exists() else {}
    backtest_checklist = _read_json_utf8_sig(BACKTEST_CHECKLIST_PATH) if BACKTEST_CHECKLIST_PATH.exists() else {}
    dashboard = _read_json(DASHBOARD_PATH)
    system_completeness = _read_json(SYSTEM_COMPLETENESS_PATH)
    run_log = _run_log_summary(RUN_LOG_PATH)
    paper_state_open_positions = _paper_state_open_positions(PAPER_STATE_PATH)
    if paper_state_open_positions is not None:
        run_log["main_open_positions"] = paper_state_open_positions

    p0_path = _latest_p0_report()
    audit_path = _latest(AUDIT_PATTERN)
    signal_path = _latest(SIGNAL_PATTERN)
    gate_path = _latest(GATE_PATTERN)

    p0 = _read_json(p0_path) if p0_path else {}
    audit = _read_json(audit_path) if audit_path else {}
    signal = _read_json(signal_path) if signal_path else {}
    gate = _read_json(gate_path) if gate_path else {}

    p0_krx_clean = p0.get("krx_clean") or {}
    freshness_path, freshness = _read_freshness_source()
    freshness_krx_clean = freshness.get("krx_clean") if isinstance(freshness.get("krx_clean"), dict) else {}
    expected_prev_weekday = _previous_business_day_ymd(today_ymd)
    prev_weekday_raw = str(p0_krx_clean.get("prev_weekday") or p0.get("as_of_ymd") or "")
    prev_weekday_raw_ymd = re.sub(r"[^0-9]", "", prev_weekday_raw)[:8]
    prev_weekday = prev_weekday_raw_ymd if (prev_weekday_raw_ymd == expected_prev_weekday and _is_trading_day_ymd(prev_weekday_raw_ymd)) else expected_prev_weekday
    krx_date_max = p0_krx_clean.get("date_max")
    krx_source = "p0_daily_check"
    krx_evidence_path = str(p0_path) if p0_path else None

    freshness_krx_date_max = re.sub(r"[^0-9]", "", str(freshness_krx_clean.get("max_date") or ""))[:8]
    freshness_expected_date = re.sub(r"[^0-9]", "", str(freshness.get("expected_date") or ""))[:8]
    freshness_krx_status = str(freshness_krx_clean.get("status") or "").strip().upper()
    freshness_krx_ok = (
        bool(freshness_path)
        and freshness_krx_status == "PASS"
        and len(freshness_krx_date_max) == 8
        and freshness_krx_date_max >= expected_prev_weekday
    )
    if freshness_krx_ok:
        krx_date_max = freshness_krx_date_max
        prev_weekday = freshness_expected_date if freshness_expected_date else expected_prev_weekday
        krx_source = "freshness_source"
        krx_evidence_path = str(freshness_krx_clean.get("path") or freshness_path)

    # date_max is fresh when it is at least the expected previous trading day.
    krx_ok = bool(krx_date_max) and str(krx_date_max) >= str(prev_weekday)

    liquidity_before = liquidity.get("before")
    liquidity_after = liquidity.get("after")
    if liquidity_before is None:
        liquidity_before = liquidity.get("candidates_before")
    if liquidity_after is None:
        liquidity_after = liquidity.get("candidates_after")
    liquidity_ok = liquidity_before is not None and liquidity_after is not None

    sector_phase2 = signal.get("phase2_sector") or {}
    sector_nonzero = int(sector_phase2.get("nonzero_rows", 0) or 0)
    sector_join_ready = str(sector_phase2.get("score_fill") or "").strip().upper()
    final_nonzero_rows = final_score.get("nonzero_rows") if isinstance(final_score.get("nonzero_rows"), dict) else {}
    final_sector_nonzero = int(final_nonzero_rows.get("sector", 0) or 0)
    sector_fail_soft_zero = sector_join_ready == "FAIL_SOFT_0" and sector_nonzero <= 0 and final_sector_nonzero > 0
    sector_ok = (sector_join_ready == "PASS" and sector_nonzero > 0) or sector_fail_soft_zero

    news_collect_reason = str(news_collect.get("reason") or "")
    news_collect_quota_soft = news_collect_reason in {"naver_quota_exceeded", "quota_budget_guard"}
    news_collect_err_cnt = int(news_collect.get("error_count", 0) or 0)
    news_collect_ok = int(news_collect.get("fetched", 0) or 0) >= int(news_collect.get("saved", 0) or 0) and (
        news_collect_err_cnt == 0 or news_collect_quota_soft
    )
    news_collect_issue_ok = news_collect_ok
    news_score_rows = int(news_score.get("rows", 0) or 0)
    news_score_nonzero = int(news_score.get("nonzero_rows", 0) or 0)
    news_score_mapped = int(news_score.get("mapped_rows", 0) or 0)
    news_score_meta = news_score.get("meta") if isinstance(news_score.get("meta"), dict) else {}
    news_score_reason = str(news_score_meta.get("reason") or news_score.get("reason") or "").strip().lower()
    news_score_fallback_reason = str(news_score_meta.get("fallback_signal_reason") or "").strip().lower()
    news_score_fallback_soft = (
        news_score_reason == "candidate_article_coverage_zero"
        and news_score_fallback_reason == "ok"
    )
    # Separate "calculation failure/empty" from "neutral zero scores".
    # nonzero_rows==0 can be valid when news gate is closed or signals are neutral.
    # Coverage-zero + valid fallback is treated as degraded(WARN/REDUCE), not calc failure.
    news_score_calc_ok = news_score_rows > 0 and (news_score_mapped > 0 or news_score_fallback_soft)
    news_score_ok = news_score_rows > 0 and news_score_nonzero > 0
    news_score_quality = str(news_score.get("quality") or "").strip().upper()
    news_gate_state = str(((final_score.get("news_gate") or {}).get("gate") if isinstance(final_score.get("news_gate"), dict) else final_score.get("news_gate")) or "").strip().upper()
    news_weight_effective = 0.0
    try:
        news_weight_effective = float(((final_score.get("news_dynamic_weight") or {}).get("effective_weight")) or ((final_score.get("weights") or {}).get("news")) or 0.0)
    except Exception:
        news_weight_effective = 0.0
    news_gate_fail_soft = (
        news_gate_state == "CLOSED"
        and float(news_weight_effective) <= 0.0
    )
    news_score_action = "ALLOW"
    if news_score_quality in {"FAIL", "BLOCK"}:
        news_score_action = "BLOCK"
    elif news_score_quality in {"PARTIAL", "WARN", "REDUCE"} or (news_score_rows > 0 and news_score_nonzero == 0):
        news_score_action = "REDUCE"
    ner_summary = news_score.get("ner_summary") if isinstance(news_score.get("ner_summary"), dict) else {}
    ner_rows_with_entity = int(ner_summary.get("rows_with_entity", 0) or 0)
    try:
        ner_coverage = float(ner_summary.get("coverage_rate", 0.0) or 0.0)
    except Exception:
        ner_coverage = 0.0
    ner_top_tags_raw = ner_summary.get("top_tags") if isinstance(ner_summary.get("top_tags"), list) else []
    ner_top_parts = []
    for x in ner_top_tags_raw[:2]:
        if not isinstance(x, dict):
            continue
        tag = str(x.get("tag") or "").strip()
        cnt = int(x.get("count", 0) or 0)
        if tag:
            ner_top_parts.append(f"{tag}:{cnt}")
    ner_top_text = ",".join(ner_top_parts) if ner_top_parts else "-"
    final_nonzero = int(((final_score.get("nonzero_rows") or {}).get("final", 0)) or 0)
    final_ok = int(final_score.get("rows", 0) or 0) > 0 and final_nonzero > 0
    paper_ok = run_log.get("main_open_positions") is not None
    p1_enabled = bool(p1_gate.get("enabled", False))
    p1_decision_raw = str(p1_gate.get("entry_gate_decision_before_p1") or "").strip()
    p1_run_label = str(p1_gate.get("run_label") or "").strip().lower()
    p1_decision_display = "판정보류" if p1_decision_raw.lower() in {"", "unknown"} else p1_decision_raw
    p1_ok = p1_enabled

    audit_summary = audit.get("summary") or {}
    audit_status = audit_summary.get("status") or audit.get("status", "")
    audit_lookahead = audit_summary.get("lookahead_suspects")
    if audit_lookahead is None:
        audit_lookahead = audit.get("lookahead_suspects")
    audit_ok = str(audit_status).upper() == "PASS"

    dashboard_health = dashboard.get("health") if isinstance(dashboard.get("health"), dict) else {}
    dashboard_overall = str(
        dashboard.get("status_overall")
        or dashboard.get("overall")
        or dashboard_health.get("overall")
        or ""
    )
    dashboard_alerts_count = dashboard.get("alerts_count")
    if dashboard_alerts_count is None:
        dashboard_alerts_count = dashboard_health.get("alerts_count")
    dashboard_actionable_alerts = _dashboard_actionable_alerts(dashboard.get("alerts"))
    dashboard_actionable_alerts_count = len(dashboard_actionable_alerts)

    try:
        import sys
        sys.path.insert(0, str(BASE / "tools"))
        import error_pattern_analyzer as epa
        auto_recovered_errors = epa.get_all_errors_today()
        auto_recovered_count = len(auto_recovered_errors)
    except Exception:
        auto_recovered_errors = []
        auto_recovered_count = 0

    dashboard_gate_summary = dashboard.get("gate_summary") if isinstance(dashboard.get("gate_summary"), dict) else {}
    dashboard_runtime_chain_guard = (
        dashboard.get("runtime_chain_guard") if isinstance(dashboard.get("runtime_chain_guard"), dict) else {}
    )
    runtime_chain_guard_status_live = str(
        dashboard_gate_summary.get("runtime_chain_guard_status")
        or dashboard_runtime_chain_guard.get("status")
        or ""
    ).upper()
    dashboard_ok = bool(dashboard_overall)
    entry_ready = int(pending.get("entry_ready", 0) or 0)
    filled = int(pending.get("filled", 0) or 0)
    max_new = pending.get("max_new")
    try:
        max_new_int = int(max_new or 0)
    except Exception:
        max_new_int = 0
    candidates_after_caps = int(pending.get("candidates_after_caps", 0) or 0)
    risk_gate_runtime = p1_gate.get("risk_gate_runtime") if isinstance(p1_gate.get("risk_gate_runtime"), dict) else {}
    p1_entry_gate_runtime = (
        risk_gate_runtime.get("entry_gate") if isinstance(risk_gate_runtime.get("entry_gate"), dict) else {}
    )
    risk_orchestration_runtime = (
        risk_gate_runtime.get("risk_orchestration")
        if isinstance(risk_gate_runtime.get("risk_orchestration"), dict)
        else {}
    )
    production_risk_runtime = (
        risk_gate_runtime.get("production_risk_playbook")
        if isinstance(risk_gate_runtime.get("production_risk_playbook"), dict)
        else {}
    )
    p1_max_new_after_raw = p1_gate.get("max_new_after", p1_entry_gate_runtime.get("max_new_after_p1"))
    try:
        p1_max_new_after = int(p1_max_new_after_raw or 0)
    except Exception:
        p1_max_new_after = 0
    p1_stop_new_orders = bool(p1_entry_gate_runtime.get("stop_new_orders"))
    risk_orch_scale = risk_orchestration_runtime.get("scale")
    risk_orch_scale_zero_causes = risk_orchestration_runtime.get("scale_zero_causes") or []
    production_risk_decision = str(production_risk_runtime.get("decision") or "").strip().upper()

    stage_status = [
        {
            "key": "krx_clean",
            "label": "KRX clean",
            "status": "PASS" if krx_ok else "PARTIAL",
            "current_state": f"date_max={krx_date_max}, prev_weekday={prev_weekday}, ncode={p0_krx_clean.get('ncode')}, source={krx_source}",
            "calc_issue_state": _calc_issue_state(krx_ok, "stale 또는 기준일 불일치")[0],
            "calc_issue_reason": _calc_issue_state(krx_ok, "stale 또는 기준일 불일치")[1],
            "evidence_path": krx_evidence_path,
        },
        {
            "key": "liquidity_filter",
            "label": "유동성 필터",
            "status": "PASS" if liquidity_ok else "PARTIAL",
            "current_state": f"before={liquidity_before} after={liquidity_after}",
            "calc_issue_state": _calc_issue_state(liquidity_ok, "근거 부족")[0],
            "calc_issue_reason": _calc_issue_state(liquidity_ok, "근거 부족")[1],
            "evidence_path": str(LIQUIDITY_PATH),
        },
        {
            "key": "sector",
            "label": "섹터",
            "status": "PASS" if sector_ok else "PARTIAL",
            "current_state": (
                f"join_ready={sector_join_ready} nonzero_rows={sector_nonzero}"
                f" final_sector_nonzero={final_sector_nonzero}"
            ),
            "calc_issue_state": ("WARN" if sector_fail_soft_zero else _calc_issue_state(sector_ok, "불일치 또는 비어 있음")[0]),
            "calc_issue_reason": (
                "phase2 fail-soft zero, final sector signal available"
                if sector_fail_soft_zero
                else _calc_issue_state(sector_ok, "불일치 또는 비어 있음")[1]
            ),
            "evidence_path": str(signal_path) if signal_path else None,
        },
        {
            "key": "news_collect",
            "label": "뉴스 수집",
            "status": "PARTIAL" if news_collect_quota_soft else ("PASS" if news_collect_ok else "PARTIAL"),
            "current_state": f"fetched={news_collect.get('fetched')} saved={news_collect.get('saved')} errors={news_collect_err_cnt} reason={news_collect_reason}",
            "calc_issue_state": _calc_issue_state(news_collect_issue_ok, "계산 실패 또는 저장 오류")[0],
            "calc_issue_reason": _calc_issue_state(news_collect_issue_ok, "계산 실패 또는 저장 오류")[1],
            "evidence_path": str(NEWS_COLLECT_PATH),
        },
        {
            "key": "news_score",
            "label": "뉴스 점수",
            "status": "PASS" if news_score_ok else "PARTIAL",
            "current_state": (
                f"rows={news_score.get('rows')} nonzero_rows={news_score.get('nonzero_rows')} "
                f"ner_rows={ner_rows_with_entity} ner_cov={ner_coverage:.2f} top={ner_top_text}"
            ),
            "calc_issue_state": (
                "WARN" if news_gate_fail_soft else _calc_issue_state(news_score_calc_ok, "계산 실패 또는 비어 있음")[0]
            ),
            "calc_issue_reason": (
                "뉴스 게이트 닫힘으로 비활성화"
                if news_gate_fail_soft
                else _calc_issue_state(news_score_calc_ok, "계산 실패 또는 비어 있음")[1]
            ),
            "evidence_path": str(NEWS_SCORE_PATH),
        },
        {
            "key": "final_score",
            "label": "최종 점수",
            "status": "PASS" if final_ok else "PARTIAL",
            "current_state": f"rows={final_score.get('rows')} policy={final_score.get('policy')}",
            "calc_issue_state": _calc_issue_state(final_ok, "최종 점수 비어 있음")[0],
            "calc_issue_reason": _calc_issue_state(final_ok, "최종 점수 비어 있음")[1],
            "evidence_path": str(FINAL_SCORE_PATH),
        },
        {
            "key": "paper_engine",
            "label": "가상매매 엔진",
            "status": run_log.get("status", "PARTIAL"),
            "current_state": f"main_new_fills={run_log.get('main_new_fills')} main_open_positions={run_log.get('main_open_positions')}",
            "calc_issue_state": _calc_issue_state(paper_ok, "실행 로그 근거 부족")[0],
            "calc_issue_reason": _calc_issue_state(paper_ok, "실행 로그 근거 부족")[1],
            "evidence_path": str(RUN_LOG_PATH),
        },
        {
            "key": "p1_entry_gate",
            "label": "P1 게이트",
            "status": "PASS" if p1_ok else "PARTIAL",
            "current_state": f"run_label={p1_run_label or '-'} decision={p1_decision_display}",
            "calc_issue_state": _calc_issue_state(p1_ok, "p1 gate 상태 근거 부족")[0],
            "calc_issue_reason": _calc_issue_state(p1_ok, "p1 gate 상태 근거 부족")[1],
            "evidence_path": str(P1_GATE_STATUS_PATH),
        },
        {
            "key": "audit",
            "label": "감사",
            "status": "PASS" if audit_ok else "FAIL",
            "current_state": f"lookahead_suspects={audit_lookahead}",
            "calc_issue_state": _calc_issue_state(audit_ok, "lookahead suspect 또는 audit 실패")[0],
            "calc_issue_reason": _calc_issue_state(audit_ok, "lookahead suspect 또는 audit 실패")[1],
            "evidence_path": str(audit_path) if audit_path else None,
        },
        {
            "key": "dashboard",
            "label": "대시보드",
            "status": dashboard_overall,
            "current_state": f"overall={dashboard_overall} alerts={dashboard_alerts_count} auto_recovered={auto_recovered_count}",
            "calc_issue_state": _calc_issue_state(dashboard_ok, "대시보드 상태 근거 부족")[0],
            "calc_issue_reason": _calc_issue_state(dashboard_ok, "대시보드 상태 근거 부족")[1],
            "evidence_path": str(DASHBOARD_PATH),
        },
    ]

    calc_issue_rows = [
        {
            "key": row["key"],
            "label": row["label"],
            "calc_issue_state": row["calc_issue_state"],
            "calc_issue_reason": row["calc_issue_reason"],
        }
        for row in stage_status
    ]

    pending_queue_len = int(pending.get("pending_queue_len", 0) or 0)
    pending_status_reason = str(pending.get("status_reason") or "").strip().upper()
    pending_base_ymd = str(pending.get("today_ymd") or p0.get("as_of_ymd") or today_ymd)
    replay_consistency = pending.get("replay_consistency") if isinstance(pending.get("replay_consistency"), dict) else {}
    replay_metrics = replay_consistency.get("metrics") if isinstance(replay_consistency.get("metrics"), dict) else {}
    carry_origin_reason_counts = pending.get("carry_origin_reason_counts")
    if not isinstance(carry_origin_reason_counts, dict):
        carry_origin_reason_counts = {}
    pending_signals_future_recheck = _is_future_close_cutoff_recheck_pending(
        PENDING_SIGNALS_PATH,
        pending_queue_len,
        pending_base_ymd,
    )
    deferred_future_recheck_pending = (
        pending_queue_len > 0
        and str(replay_consistency.get("status") or "").strip().upper() == "OK"
        and not (replay_consistency.get("issues") or [])
        and int(replay_metrics.get("due_today_rows", 0) or 0) == 0
        and (
            int(replay_metrics.get("future_rows", 0) or 0) >= pending_queue_len
            or pending_signals_future_recheck
        )
        and set(str(k).strip().upper() for k in carry_origin_reason_counts.keys()) <= {"CLOSE_CUTOFF_RECHECK"}
    )
    empty_queue_benign = (
        pending_queue_len <= 0
        and pending_status_reason in {"OPEN_POSITION_ACTIVE", "ENTRY_FILLED_OPEN_POSITION", "NO_PENDING_QUEUE"}
    )
    runtime_rows = system_completeness.get("runtime_operational_checks") or []
    blocking_issues_display = []
    blocking_issues_effective = []
    for row in runtime_rows:
        row_key = str(row.get("key") or "").strip()
        status = str(row.get("status", "")).upper()
        live_current_state = None
        # batch_execution은 stale completeness 값보다 최신 run log 판정을 우선 적용한다.
        if row_key == "batch_execution":
            live_status = str(run_log.get("status") or "").upper()
            if live_status in {"PASS", "PARTIAL", "FAIL", "WARN"}:
                status = live_status
            live_current_state = run_log.get("current_state")
        # dashboard_health must use the latest dashboard_state alerts, not stale runtime rows.
        # Policy/advisory warnings such as RELAX_HIGH must not become an entry hard block.
        if row_key == "dashboard_health":
            live_current_state = (
                f"dashboard_overall={dashboard_overall} alerts_count={dashboard_alerts_count} "
                f"actionable_alerts={dashboard_actionable_alerts_count}"
            )
            status = "PASS" if dashboard_actionable_alerts_count <= 0 else (dashboard_overall.upper() or status)
        if row_key in {"pending_entry_flow", "entry_order_runtime"}:
            pending_classification = (
                "DEFERRED_FUTURE_RECHECK" if deferred_future_recheck_pending else pending_status_reason or "-"
            )
            live_current_state = (
                f"pending_queue_len={pending_queue_len} entry_ready={entry_ready} "
                f"max_new={max_new_int} lifecycle=PASS status_reason={pending_status_reason or '-'} "
                f"classification={pending_classification}"
            )
            if pending_queue_len > 0 or entry_ready <= 0:
                status = "PARTIAL"
            elif max_new_int > 0:
                status = "PASS"
        if row_key == "ddm_entry_cap":
            live_current_state = (
                f"p1_max_new_after={p1_max_new_after} stop_new_orders={p1_stop_new_orders} "
                f"pending_max_new={max_new_int} filled={filled} "
                f"risk_orch_scale={risk_orch_scale if risk_orch_scale is not None else '-'} "
                f"scale_zero_causes={','.join(str(x) for x in risk_orch_scale_zero_causes) or '-'} "
                f"production_risk_decision={production_risk_decision or '-'}"
            )
            if p1_stop_new_orders or (p1_max_new_after <= 0 and max_new_int <= 0 and filled <= 0):
                status = "FAIL"
            elif risk_orch_scale_zero_causes or production_risk_decision in {"REDUCE", "BLOCK"}:
                status = "PARTIAL"
            else:
                status = "PASS"
        # output_consistency_runtime은 latest live_vs_bt 피드백(run_ymd/last_exit)을 우선 반영한다.
        if row_key == "output_consistency_runtime" and live_vs_bt_feedback:
            run_ymd_live = str(live_vs_bt_feedback.get("run_ymd") or "")
            last_exit_live = str((live_vs_bt_feedback.get("live") or {}).get("last_exit_date") or "")
            comparison_live = live_vs_bt_feedback.get("comparison") or {}
            optimize_live = live_vs_bt_feedback.get("optimize") or {}
            alignment_live = optimize_live.get("alignment") if isinstance(optimize_live.get("alignment"), dict) else {}
            alignment_ready_live = bool(comparison_live.get("alignment_ready"))
            alignment_reason_live = str(comparison_live.get("alignment_reason") or "-")
            feedback_loop_state_live = str(alignment_live.get("feedback_loop_state") or "").upper()
            feedback_loop_reason_live = str(alignment_live.get("feedback_loop_reason") or "-")
            bt_live_coverage_ratio_live = alignment_live.get("bt_live_coverage_ratio")
            bt_feedback_loop_diverged = feedback_loop_state_live == "DIVERGED"
            bt_source_mismatch = feedback_loop_state_live == "SOURCE_MISMATCH"
            bt_comparable_series_available = feedback_loop_state_live == "COMPARABLE_SERIES_AVAILABLE"
            bt_sample_insufficient = alignment_reason_live == "bt_too_few_trades_in_window"
            if run_ymd_live and last_exit_live:
                # aligned_window 준비가 끝난 경우(run/bt 정렬 검증 완료)에는
                # run_ymd!=last_exit라도 운영 차단 이슈로 승격하지 않는다.
                status = "PASS" if alignment_ready_live else "PARTIAL"
                live_current_state = (
                    f"last_exit={last_exit_live} run_ymd={run_ymd_live} "
                    f"alignment_ready={alignment_ready_live} alignment_reason={alignment_reason_live}"
                )
                if bt_feedback_loop_diverged:
                    live_current_state = (
                        f"{live_current_state} classification=BT_LIVE_FEEDBACK_LOOP_DIVERGED "
                        f"feedback_loop_reason={feedback_loop_reason_live} "
                        f"live_n={alignment_live.get('live_n', '-')} "
                        f"bt_n={alignment_live.get('bt_n', '-')} "
                        f"coverage_ratio={bt_live_coverage_ratio_live if bt_live_coverage_ratio_live is not None else '-'} "
                        f"required_min={live_vs_bt_feedback.get('thresholds', {}).get('min_shared_trades', '-')}"
                    )
                elif bt_source_mismatch:
                    live_current_state = (
                        f"{live_current_state} classification=BT_COMPARISON_SOURCE_MISMATCH "
                        f"feedback_loop_reason={feedback_loop_reason_live} "
                        f"legacy_bt_n={alignment_live.get('bt_n', '-')} "
                        f"real_validation_bt_n={alignment_live.get('bt_representative_n', '-')} "
                        f"required_min={live_vs_bt_feedback.get('thresholds', {}).get('min_shared_trades', '-')}"
                    )
                elif bt_comparable_series_available:
                    comparable_live = alignment_live.get("real_validation_comparable") if isinstance(alignment_live.get("real_validation_comparable"), dict) else {}
                    live_current_state = (
                        f"{live_current_state} classification=BT_COMPARABLE_SERIES_AVAILABLE_REVIEW_REQUIRED "
                        f"feedback_loop_reason={feedback_loop_reason_live} "
                        f"legacy_bt_n={alignment_live.get('bt_n', '-')} "
                        f"real_validation_bt_n={alignment_live.get('bt_representative_n', '-')} "
                        f"common_days={comparable_live.get('common_days', '-')} "
                        f"abs_diff={comparable_live.get('abs_diff', '-')}"
                    )
                elif bt_sample_insufficient:
                    live_current_state = (
                        f"{live_current_state} classification=BT_ALIGNED_SAMPLE_INSUFFICIENT "
                        f"bt_n={alignment_live.get('bt_n', '-')} "
                        f"required_min={live_vs_bt_feedback.get('thresholds', {}).get('min_shared_trades', '-')}"
                    )
        if status in {"PARTIAL", "FAIL", "WARN"}:
            row_state = _clean_text(live_current_state if live_current_state else row.get("current_state"))
            if row_key == "policy_drift_runtime" and runtime_chain_guard_status_live:
                if "runtime_chain_guard=" in row_state:
                    row_state = re.sub(
                        r"runtime_chain_guard=[A-Z]+",
                        f"runtime_chain_guard={runtime_chain_guard_status_live}",
                        row_state,
                    )
                else:
                    row_state = f"{row_state} runtime_chain_guard={runtime_chain_guard_status_live}".strip()
            row_item = {
                "key": row.get("key"),
                "label": row.get("label"),
                "status": status,
                "current_state": row_state,
            }
            if row_key == "output_consistency_runtime":
                if "classification=BT_LIVE_FEEDBACK_LOOP_DIVERGED" in row_state:
                    row_item["classification"] = "BT_LIVE_FEEDBACK_LOOP_DIVERGED"
                    row_item["policy_state"] = "feedback_loop_diverged_fail_closed"
                    row_item["action"] = "Rebuild/align BT model to live behavior; do not relax threshold or mark PASS in this step."
                elif "classification=BT_COMPARISON_SOURCE_MISMATCH" in row_state:
                    row_item["classification"] = "BT_COMPARISON_SOURCE_MISMATCH"
                    row_item["policy_state"] = "comparison_source_mismatch_fail_closed"
                    row_item["action"] = "Use real-strategy validation as the BT reference source; keep alignment fail-closed until a comparable series exists."
                elif "classification=BT_COMPARABLE_SERIES_AVAILABLE_REVIEW_REQUIRED" in row_state:
                    row_item["classification"] = "BT_COMPARABLE_SERIES_AVAILABLE_REVIEW_REQUIRED"
                    row_item["policy_state"] = "comparable_series_review_required_fail_closed"
                    row_item["action"] = "Review daily comparable series before enabling it for live-vs-BT trigger decisions."
                elif "classification=BT_ALIGNED_SAMPLE_INSUFFICIENT" in row_state:
                    row_item["classification"] = "BT_ALIGNED_SAMPLE_INSUFFICIENT"
                    row_item["policy_state"] = "baseline_insufficient_fail_closed"
                    row_item["action"] = "Collect more aligned BT/live evidence; do not relax threshold or mark PASS in this step."
            blocking_issues_display.append(row_item)
            # If entries are actually flowing, do not keep stale ddm_entry_cap as blocker.
            if row_key == "ddm_entry_cap" and (entry_ready > 0 or filled > 0 or max_new_int > 0):
                continue
            # Empty pending queue is an operational steady state, not an effective ddm block.
            if row_key == "ddm_entry_cap" and empty_queue_benign:
                continue
            # When queue is empty, treat pending flow PARTIAL as non-blocking.
            if (
                row_key in {"pending_entry_flow", "entry_order_runtime"}
                and empty_queue_benign
            ):
                continue
            # Future close-cutoff rechecks are deferred queue state, not a current-day execution failure.
            if (
                row_key in {"pending_entry_flow", "entry_order_runtime"}
                and deferred_future_recheck_pending
            ):
                continue
            # If latest collection chain is healthy, keep stale input_collection_runtime only for display.
            if (
                row_key == "input_collection_runtime"
                and news_collect_issue_ok
                and str(news_collect_reason).strip().lower() == "ok"
                and news_collect_err_cnt == 0
            ):
                continue
            # Quota guard soft-skip is handled in macro/news guard layer, keep display only.
            if (
                row_key == "input_collection_runtime"
                and str(news_collect_reason).strip().lower() in {"quota_budget_guard", "naver_quota_exceeded"}
                and news_collect_err_cnt == 0
            ):
                continue
            # Outside collection time window is expected in non-collection sessions.
            # If runtime evidence itself says news pipeline is healthy, keep as display-only PARTIAL.
            if row_key == "input_collection_runtime":
                row_state_l = str(row_state).lower()
                runtime_news_ok = ("news_reason=ok" in row_state_l) and ("news_quality=pass" in row_state_l)
                outside_window_benign = (
                    str(news_collect_reason).strip().lower() == "outside_time_window"
                    and news_collect_err_cnt == 0
                    and int(news_collect.get("fetched", 0) or 0) == 0
                    and int(news_collect.get("saved", 0) or 0) == 0
                )
                if runtime_news_ok and outside_window_benign:
                    continue
            # policy_drift_runtime with WARN guard is advisory; do not hard-block entry gate.
            if (
                row_key == "policy_drift_runtime"
                and status in {"WARN", "PARTIAL"}
                and (
                    runtime_chain_guard_status_live in {"", "OK"}
                    or "runtime_chain_guard=WARN" in str(row_item.get("current_state") or "")
                )
            ):
                continue
            # Dashboard health is derived from this integrated snapshot again in RootB.
            # Keep it visible, but do not let it become a recursive effective blocker.
            if row_key == "dashboard_health":
                continue
            # Active run window should not hard-block operations as failed batch.
            if row_key == "batch_execution" and bool(run_log.get("in_progress")) and status == "PARTIAL":
                continue
            blocking_issues_effective.append(row_item)

    if news_score_action in {"REDUCE", "BLOCK"}:
        news_score_block_row = {
            "key": "news_score_quality_gate",
            "label": "뉴스 점수 품질 게이트",
            "status": "FAIL" if news_score_action == "BLOCK" else "PARTIAL",
            "current_state": (
                f"quality={news_score_quality or '-'} action={news_score_action} "
                f"rows={news_score_rows} nonzero_rows={news_score_nonzero}"
            ),
        }
        blocking_issues_display.append(news_score_block_row)
        # REDUCE is advisory/degraded quality only; only BLOCK should hard-block effective chain.
        if news_score_action == "BLOCK" and not news_gate_fail_soft:
            blocking_issues_effective.append(news_score_block_row)

    if backtest_checklist:
        checklist_passed = bool(backtest_checklist.get("passed", False))
        checklist_judgment = str(backtest_checklist.get("operation_judgment") or "-")
        checklist_ne = int(backtest_checklist.get("not_evaluable_n", 0) or 0)
        module_progress = backtest_checklist.get("module_progress") if isinstance(backtest_checklist.get("module_progress"), dict) else {}
        optimizer_ready = bool(module_progress.get("optimizer_ready", False))
        optimizer_blockers = module_progress.get("optimizer_blockers") if isinstance(module_progress.get("optimizer_blockers"), list) else []
        if (not checklist_passed) or (not optimizer_ready) or checklist_ne > 0:
            blocking_issues_display.append(
                {
                    "key": "backtest_checklist_readiness",
                    "label": "BT 체크리스트 판정보강필요",
                    "status": "PARTIAL",
                    "current_state": (
                        f"checklist_passed={checklist_passed} operation_judgment={checklist_judgment} "
                        f"not_evaluable_n={checklist_ne} optimizer_ready={optimizer_ready} "
                        f"optimizer_blockers={','.join(str(x) for x in optimizer_blockers) or '-'}"
                    ),
                    "classification": "BT_CHECKLIST_NEEDS_MORE_EVIDENCE",
                    "policy_state": "display_only_validation_gap",
                    "action": "BT 체크리스트의 NOT_EVALUABLE 항목을 표본/검증 보강 대상으로 분리 추적",
                }
            )

    p0_asof = str(p0.get("as_of_ymd") or "")
    gate_asof = str(gate.get("as_of_ymd") or _extract_ymd_from_path(gate_path) or "")
    signal_asof = str(signal.get("as_of_ymd") or "")
    candidate_data_ymd = _extract_obj_ymd(candidates_meta, "latest_date")
    candidate_asof = _extract_obj_ymd(candidates_meta, "as_of_ymd", "as_of")
    final_score_asof = _extract_obj_ymd(final_score, "asof_ymd", "as_of")
    pending_asof = _extract_obj_ymd(pending, "as_of_ymd", "as_of")
    chain_ref_ymd = str(
        pending_asof
        or p0_asof
        or gate_asof
        or signal_asof
        or candidate_asof
        or candidate_data_ymd
        or final_score_asof
        or today_ymd
    )
    # signal_integration_status often lacks explicit as_of_ymd; when empty, align it to the active chain ref.
    if not signal_asof:
        signal_asof = chain_ref_ymd
    prev_bday_ymd = _previous_business_day_ymd(chain_ref_ymd)
    allowed_chain_dates = {d for d in [chain_ref_ymd, prev_bday_ymd] if d}
    stale_parts = []
    if p0_asof and p0_asof not in allowed_chain_dates:
        stale_parts.append(f"p0:{p0_asof}")
    if gate_asof and gate_asof not in allowed_chain_dates:
        stale_parts.append(f"gate:{gate_asof}")
    if signal_asof and signal_asof not in allowed_chain_dates:
        stale_parts.append(f"signal:{signal_asof}")
    if candidate_data_ymd and candidate_data_ymd not in allowed_chain_dates:
        stale_parts.append(f"candidate:{candidate_data_ymd}")
    if final_score_asof and final_score_asof not in allowed_chain_dates:
        stale_parts.append(f"final_score:{final_score_asof}")
    if stale_parts:
        stale_row = {
            "key": "stale_latest_chain",
            "label": "latest 체인 기준일 불일치",
            "status": "FAIL",
            "current_state": f"chain_ref={chain_ref_ymd}, stale={','.join(stale_parts)}",
        }
        blocking_issues_display.append(stale_row)
        blocking_issues_effective.append(stale_row)
    blocking_issues_display.sort(key=lambda r: (_priority_rank(str(r.get("key"))), str(r.get("key"))))
    blocking_issues_effective.sort(key=lambda r: (_priority_rank(str(r.get("key"))), str(r.get("key"))))

    top_blocker = blocking_issues_display[0]["label"] if blocking_issues_display else "현재 확인된 차단 이슈 없음"
    top_blocker_effective = (
        blocking_issues_effective[0]["label"] if blocking_issues_effective else "현재 확인된 차단 이슈 없음"
    )

    pending_queue_len = int(pending.get("pending_queue_len", 0) or 0)
    pending_queue_len_raw = int(pending.get("pending_queue_len_raw", pending_queue_len) or 0)
    pending_split_entry_2nd_rows = int(pending.get("pending_split_entry_2nd_rows", 0) or 0)
    lifecycle = ((pending.get("entry_exit_lifecycle") or {}).get("status")) or pending.get("status")
    if max_new_int <= 0 and entry_ready <= 0 and candidates_after_caps > 0:
        entry_cap_row = {
            "key": "entry_capacity_zero",
            "label": "진입 여력 0 고착",
            "status": "WARN",
            "current_state": f"max_new={max_new_int}, entry_ready={entry_ready}, candidates_after_caps={candidates_after_caps}",
        }
        blocking_issues_display.append(entry_cap_row)
        if not empty_queue_benign:
            blocking_issues_effective.append(entry_cap_row)
        blocking_issues_display.sort(key=lambda r: (_priority_rank(str(r.get("key"))), str(r.get("key"))))
        blocking_issues_effective.sort(key=lambda r: (_priority_rank(str(r.get("key"))), str(r.get("key"))))

    top_blocker = blocking_issues_display[0]["label"] if blocking_issues_display else "현재 확인된 차단 이슈 없음"
    top_blocker_effective = (
        blocking_issues_effective[0]["label"] if blocking_issues_effective else "현재 확인된 차단 이슈 없음"
    )

    pending_market_regime = pending.get("market_regime")
    if isinstance(pending_market_regime, dict):
        pending_market_regime_value = pending_market_regime.get("regime")
    else:
        pending_market_regime_value = pending_market_regime

    dashboard_health = dashboard.get("health") or {}
    dashboard_market_regime = dashboard_health.get("market_regime") if isinstance(dashboard_health, dict) else None
    market_regime = pending_market_regime_value or dashboard_market_regime or "UNKNOWN"

    if deferred_future_recheck_pending:
        root_cause_summary = (
            f"pending 대기열 {pending_queue_len}건은 다음 세션 재확인 대기이며 "
            "replay_consistency 기준 오늘 처리 실패는 아닙니다."
        )
        primary_reason = "CLOSE_CUTOFF_RECHECK 미래 대기 상태로 분리됨"
    elif pending_queue_len > 0:
        root_cause_summary = (
            f"pending 대기열 {pending_queue_len}건과 lifecycle {lifecycle} 상태가 남아 있어 진입 흐름이 완전히 닫히지 않았습니다."
        )
        primary_reason = "진입 대기열이 남아 있고 lifecycle이 CAUTION 계열이라 운영 흐름이 완전히 안정화되지 않음"
    else:
        root_cause_summary = "현재 pending 적체는 크지 않으며 다른 운영 이슈를 우선 확인해야 합니다."
        primary_reason = "현재 최상위 병목은 pending 적체가 아님"
    supporting_facts = [
        f"entry_ready={entry_ready}",
        f"filled={filled}",
        f"pending_raw={pending_queue_len_raw}",
        f"pending_split_2nd={pending_split_entry_2nd_rows}",
        f"max_new={max_new}",
        f"market_regime={market_regime}",
        f"deferred_future_recheck_pending={deferred_future_recheck_pending}",
    ]

    next_actions = []
    if deferred_future_recheck_pending:
        next_actions.append("다음 세션 재확인 대기열은 장 재개 후 재평가")
    elif pending_queue_len > 0:
        next_actions.append("pending 대기열과 entry_ready 상태를 먼저 확인")
    if str(dashboard_overall).upper() == "WARN":
        next_actions.append("dashboard WARN 알림 코드와 runtime_chain_guard 확인")
    if stale_parts:
        next_actions.append("latest 포인터/체인 기준일을 오늘로 재동기화")
    if max_new_int <= 0 and entry_ready <= 0 and candidates_after_caps > 0:
        next_actions.append("진입 여력(max_new) 0 고착 원인 점검")
    if not next_actions:
        next_actions.append("현재 상단 차단 이슈가 적어 세부 로그를 확인")

    evidence_paths = {
        "run_log": str(RUN_LOG_PATH),
        "pending": str(PENDING_PATH),
        "p0_daily_check": str(p0_path) if p0_path else None,
        "freshness_source": str(freshness_path) if freshness_path else None,
        "audit": str(audit_path) if audit_path else None,
        "liquidity_filter": str(LIQUIDITY_PATH),
        "news_collect": str(NEWS_COLLECT_PATH),
        "news_score": str(NEWS_SCORE_PATH),
        "final_score": str(FINAL_SCORE_PATH),
        "signal_integration": str(signal_path) if signal_path else None,
        "gate_daily": str(gate_path) if gate_path else None,
        "backtest_validation_checklist": str(BACKTEST_CHECKLIST_PATH),
        "dashboard_state": str(DASHBOARD_PATH),
        "system_completeness": str(SYSTEM_COMPLETENESS_PATH),
    }

    evidence_rows = [
        {
            "key": key,
            "label": key.replace("_", " "),
            "basename": _basename(path),
            "path": path,
        }
        for key, path in evidence_paths.items()
    ]

    summary = {
        "as_of_ymd": str(pending.get("today_ymd") or p0.get("as_of_ymd") or today_ymd),
        "batch_status": run_log.get("status"),
        "main_open_positions": run_log.get("main_open_positions"),
        "shadow_open_positions": run_log.get("shadow_open_positions"),
        "pending_queue_len": pending_queue_len,
        "pending_queue_len_raw": pending_queue_len_raw,
        "pending_split_entry_2nd_rows": pending_split_entry_2nd_rows,
        "entry_ready": entry_ready,
        "filled": filled,
        "dashboard_overall": dashboard_overall,
        "alerts_count": dashboard_alerts_count,
        "auto_recovered_count": auto_recovered_count,
        "top_blocker": top_blocker,
        "top_blocker_effective": top_blocker_effective,
        "next_step": next_actions[0] if next_actions else "추가 조치 없음",
    }
    summary_cards = [
        {"label": "배치 상태", "value": _clean_text(summary.get("batch_status"))},
        {"label": "메인 포지션", "value": _clean_text(summary.get("main_open_positions"))},
        {"label": "pending", "value": _clean_text(summary.get("pending_queue_len"))},
        {"label": "pending(raw)", "value": _clean_text(summary.get("pending_queue_len_raw"))},
        {"label": "split2nd", "value": _clean_text(summary.get("pending_split_entry_2nd_rows"))},
        {"label": "최상위 병목", "value": _clean_text(summary.get("top_blocker"))},
        {"label": "다음 조치", "value": _clean_text(summary.get("next_step"))},
    ]

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "summary": summary,
        "summary_cards": summary_cards,
        "stage_status": stage_status,
        "calc_issue_rows": calc_issue_rows,
        "blocking_issues": blocking_issues_display,
        "blocking_issues_effective": blocking_issues_effective,
        "root_cause": {
            "summary": root_cause_summary,
            "primary_reason": primary_reason,
            "supporting_facts": supporting_facts,
        },
        "next_actions": next_actions,
        "evidence_paths": evidence_paths,
        "evidence_rows": evidence_rows,
    }


def _write_plans_snapshot(snapshot: dict[str, Any]) -> None:
    if not PLANS_PATH.exists():
        return

    summary = snapshot.get("summary") or {}
    calc_issue_rows = snapshot.get("calc_issue_rows") or []
    blocking_rows = snapshot.get("blocking_issues") or []
    issue_count = sum(1 for row in calc_issue_rows if row.get("calc_issue_state") == "ISSUE")
    blocker_text = _clean_text(summary.get("top_blocker"), "-")
    next_step_text = _clean_text(summary.get("next_step"), "-")

    section = "\n".join(
        [
            AUTO_START,
            "## 자동 갱신 스냅샷",
            f"- 생성 시각: `{_clean_text(snapshot.get('generated_at'), '-')}`",
            f"- 배치 상태: `{_clean_text(summary.get('batch_status'), '-')}`",
            f"- 메인 포지션: `{_clean_text(summary.get('main_open_positions'), '-')}`",
            f"- pending 수: `{_clean_text(summary.get('pending_queue_len'), '-')}`",
            f"- 계산항목 ISSUE 수: `{issue_count}`",
            f"- 최상위 병목: `{blocker_text}`",
            f"- 다음 조치: `{next_step_text}`",
            f"- 막는 문제 건수: `{len(blocking_rows)}`",
            AUTO_END,
        ]
    )

    original = PLANS_PATH.read_text(encoding="utf-8")
    pattern = rf"{re.escape(AUTO_START)}.*?{re.escape(AUTO_END)}"
    if re.search(pattern, original, flags=re.S):
        updated = re.sub(pattern, section, original, flags=re.S)
    else:
        updated = original.rstrip() + "\n\n" + section + "\n"
    PLANS_PATH.write_text(updated, encoding="utf-8-sig")


def _should_update_plans() -> bool:
    raw = str(os.getenv("INTEGRATED_OPS_UPDATE_PLANS", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def main() -> None:
    data = _build_snapshot()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dated_path = LOGS / f"integrated_ops_snapshot_{stamp}.json"
    payload = json.dumps(data, ensure_ascii=True, indent=2)
    OUTPUT_LATEST.write_text(payload, encoding="utf-8")
    dated_path.write_text(payload, encoding="utf-8")
    plans_updated = False
    if _should_update_plans():
        _write_plans_snapshot(data)
        plans_updated = True
    _log_print(f"[OK] wrote {OUTPUT_LATEST}")
    _log_print(f"[OK] wrote {dated_path}")
    if plans_updated:
        _log_print(f"[OK] updated {PLANS_PATH}")
    else:
        _log_print(f"[SKIP] update disabled {PLANS_PATH}")


if __name__ == "__main__":
    main()
