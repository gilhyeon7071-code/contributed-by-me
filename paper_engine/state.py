"""State/runtime orchestration, order/sell validation summaries, sell adapters,
and replay/queue orchestration for paper_engine.

Split out from the legacy ``paper_engine.py`` as Phase 6 of the module refactor.
"""

from __future__ import annotations

import json
import math
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import pandas as pd

from utils.common import latest_file, norm_code, now_ymd

from paper_engine.common import (
    RISK_DIR,
    _norm_ymd_text,
    now_ts,
    _to_float,
    _to_int,
    _get_dict,
    _get_list,
    compute_participation_slo,
)
from paper_engine.io import (
    load_state,
    save_state,
    STATE_PATH,
    PENDING_SIGNALS_PATH,
    REPLAY_ORDERS_PATH,
    REPLAY_QUEUE_STATUS_PATH,
    RECOVERY_STATUS_PATH,
    REPLAY_QUARANTINE_CSV_PATH,
    REPLAY_QUARANTINE_JSON_PATH,
    REPLAY_QUEUE_ARCHIVE_DIR,
    REPLAY_CONSISTENCY_PATH,
    SELL_VALIDATION_REPORT_PATH,
    ORDER_VALIDATION_REPORT_PATH,
    ROOTB_DIR,
    ROOTB_REPLAY_REGEN_SCRIPT,
    ROOTB_REPLAY_SUMMARY_LATEST,
    ROOTB_REGEN_EVAL_DIRS,
    ROOTB_REGEN_LIVE_FILLS,
)
from paper_engine.positions import (
    _collect_persisted_today_sell_artifacts,
    _load_replay_orders_raw,
    _partition_replay_orders_for_recovery,
    _load_pending_signals,
    _save_pending_signals,
    _collect_persisted_today_runtime_metrics,
    _collect_persisted_today_order_artifacts,
    _build_entry_exit_lifecycle,
    _build_state_machine_summary,
    _build_symbol_stop_summary,
    _write_pending_status,
)
from paper_engine.exit import (
    _build_sell_order_lifecycle_summary,
    _build_partial_exit_policy_summary,
    _build_sell_recovery_chain_summary,
    _build_sell_validation_report,
    _build_legacy_derisk_summary,
)
from paper_engine.drawdown import _ddm_pct01, _ddm_to_float


@dataclass
class SellOrderRequest:
    runtime_ymd: str
    order_id: str
    code: str
    sell_qty: int
    exit_reason: str
    source_order_id: str = ""
    lineage_origin: str = ""
    replay_chain_id: str = ""


@dataclass
class SellOrderStatusView:
    runtime_ymd: str
    order_id: str
    code: str
    state: str
    sell_qty: int
    exit_reason: str
    source_order_id: str = ""
    lineage_origin: str = ""
    replay_chain_id: str = ""
    trade_chain_matched: bool = False
class PaperSellOrderAdapter:
    def __init__(self, schema: str, runtime_ymd: str):
        self.schema = str(schema or "")
        self.runtime_ymd = str(_norm_ymd_text(runtime_ymd) or "")

    def submit_sell_order(self, request: SellOrderRequest) -> SellOrderStatusView:
        existing = self.query_order_status(request.order_id)
        if existing is not None:
            return existing
        state = "DECIDED"
        if int(request.sell_qty or 0) > 0:
            state = "SUBMITTED"
        return SellOrderStatusView(
            runtime_ymd=self.runtime_ymd,
            order_id=str(request.order_id or "").strip(),
            code=str(request.code or "").strip(),
            state=state,
            sell_qty=int(_ddm_to_float(request.sell_qty, 0)),
            exit_reason=str(request.exit_reason or "").strip().upper(),
            source_order_id=str(request.source_order_id or "").strip(),
            lineage_origin=str(request.lineage_origin or "").strip().upper(),
            replay_chain_id=str(request.replay_chain_id or "").strip(),
            trade_chain_matched=False,
        )

    def query_order_status(self, order_id: str) -> Optional[SellOrderStatusView]:
        target = str(order_id or "").strip()
        if not target:
            return None
        artifacts = _collect_persisted_today_sell_artifacts(schema=self.schema, runtime_ymd=self.runtime_ymd)
        sell_fills = list(artifacts.get("sell_fills") or [])
        sell_trades = list(artifacts.get("sell_trades") or [])
        fill_row = next((row for row in sell_fills if str(row.get("order_id") or "").strip() == target), None)
        if fill_row is None:
            return None

        code = str(fill_row.get("code") or "").strip()
        exit_reason = str(fill_row.get("exit_reason") or "").strip().upper()
        sell_qty = int(_ddm_to_float(fill_row.get("sell_qty"), 0))
        source_order_id = str(fill_row.get("source_order_id") or "").strip()
        lineage_origin = str(fill_row.get("lineage_origin") or "").strip().upper()
        replay_chain_id = str(fill_row.get("replay_chain_id") or "").strip()
        trade_chain_matched = any(
            str(trade.get("code") or "").strip() == code
            and str(trade.get("exit_reason") or "").strip().upper() == exit_reason
            and int(_ddm_to_float(trade.get("sell_qty"), 0)) == sell_qty
            and str(trade.get("source_order_id") or "").strip() == source_order_id
            and str(trade.get("lineage_origin") or "").strip().upper() == lineage_origin
            and str(trade.get("replay_chain_id") or "").strip() == replay_chain_id
            for trade in sell_trades
        )
        state = "FILLED_PARTIAL" if bool(fill_row.get("partial_exit")) else "FILLED_FULL"
        return SellOrderStatusView(
            runtime_ymd=self.runtime_ymd,
            order_id=target,
            code=code,
            state=state,
            sell_qty=sell_qty,
            exit_reason=exit_reason,
            source_order_id=source_order_id,
            lineage_origin=lineage_origin,
            replay_chain_id=replay_chain_id,
            trade_chain_matched=bool(trade_chain_matched),
        )

    def query_fills(self, order_id: str) -> List[Dict[str, Any]]:
        target = str(order_id or "").strip()
        if not target:
            return []
        artifacts = _collect_persisted_today_sell_artifacts(schema=self.schema, runtime_ymd=self.runtime_ymd)
        return [dict(row) for row in (artifacts.get("sell_fills") or []) if str(row.get("order_id") or "").strip() == target]

    def reconcile_positions(self) -> Dict[str, Any]:
        try:
            st = load_state()
        except Exception:
            st = {}
        open_positions = list((st or {}).get("open_positions") or [])
        return {
            "runtime_ymd": self.runtime_ymd,
            "open_positions": int(len(open_positions)),
            "state_path": str(STATE_PATH),
        }


_P0_DAILY_CHECK_REPORT_RE = re.compile(r"^p0_daily_check_\d{8}_\d{6}\.json$")


def _latest_p0_daily_check_report(log_dir: Path) -> Optional[Path]:
    """Return the latest real p0_daily_check report, excluding status sidecars."""
    try:
        files = [
            p
            for p in log_dir.glob("p0_daily_check_*.json")
            if _P0_DAILY_CHECK_REPORT_RE.match(p.name)
        ]
        files = sorted(files, key=lambda p: p.stat().st_mtime)
        return files[-1] if files else None
    except Exception:
        return None


def load_latest_p0_risk_off(log_dir: Path) -> tuple[bool, list[str]]:
    """Return (enabled, reasons) from the latest p0_daily_check_*.json in log_dir.

    If missing or unreadable, returns (False, []).
    """
    try:
        p0p = _latest_p0_daily_check_report(log_dir)
        if not p0p:
            return False, []
        obj = json.loads(p0p.read_text(encoding='utf-8'))
        ro = _get_dict(obj, 'risk_off')
        enabled = bool(ro.get('enabled'))
        reasons = _get_list(ro, 'reasons')
        reasons = [str(x) for x in reasons]
        return enabled, reasons
    except Exception:
        return False, []


def _build_risk_reason_details(
    reasons: List[str],
    p0_snapshot: Dict[str, Any],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    ks = cast(Dict[str, Any], p0_snapshot.get("kill_switch") or {})
    ks_limits = cast(Dict[str, Any], ks.get("limits") or {})
    ks_metrics = cast(Dict[str, Any], ks.get("metrics") or {})
    ks_hard_metrics = cast(Dict[str, Any], ks_metrics.get("hard_trigger_metrics") or {})
    cr = cast(Dict[str, Any], p0_snapshot.get("crash_risk_off") or {})
    cr_limits = cast(Dict[str, Any], cr.get("limits") or {})
    cr_metrics = cast(Dict[str, Any], cr.get("metrics") or {})

    for raw in list(reasons or []):
        reason = str(raw or "")
        item: Dict[str, Any] = {"reason": reason, "matched": "generic", "source": "risk_off.reasons"}

        if reason.startswith("kill_switch:MAX_DD(") or reason.startswith("MAX_DD("):
            item["matched"] = "kill_switch_max_dd"
            item["source"] = "p0.kill_switch.metrics.hard_trigger_metrics/limits"
            item["observed"] = ks_hard_metrics.get("max_drawdown_pct", ks_metrics.get("max_drawdown_pct"))
            item["basis"] = ks_metrics.get("hard_trigger_basis")
            item["threshold"] = ks_limits.get("max_drawdown_pct")
            item["mode"] = ks_limits.get("mode")
        elif reason.startswith("kill_switch:DAILY_LOSS(") or reason.startswith("DAILY_LOSS("):
            item["matched"] = "kill_switch_daily_loss"
            item["source"] = "p0.kill_switch.metrics.hard_trigger_metrics/limits"
            item["observed"] = ks_hard_metrics.get("daily_loss_pct", ks_hard_metrics.get("last_day_ret", ks_metrics.get("last_day_ret")))
            item["basis"] = ks_hard_metrics.get("daily_loss_basis", ks_metrics.get("hard_trigger_basis"))
            item["threshold"] = ks_limits.get("max_daily_loss_pct")
            item["daily_loss_active"] = ks_metrics.get("hard_daily_loss_active", ks_metrics.get("daily_loss_active"))
            item["mode"] = ks_limits.get("mode")
        elif reason == "kill_switch":
            item["matched"] = "kill_switch_trigger"
            item["source"] = "p0.kill_switch"
            item["triggered"] = ks.get("triggered")
            item["mode"] = ks_limits.get("mode")
            item["thresholds"] = {
                "max_drawdown_pct": ks_limits.get("max_drawdown_pct"),
                "max_daily_loss_pct": ks_limits.get("max_daily_loss_pct"),
            }
        elif reason.startswith("crash_risk_off"):
            item["matched"] = "crash_risk_off"
            item["source"] = "p0.crash_risk_off.metrics/limits"
            item["triggered"] = cr.get("triggered")
            item["mode"] = cr_limits.get("mode")
            item["observed"] = {
                "max_dd": cr_metrics.get("max_dd"),
                "day_ret": cr_metrics.get("day_ret"),
            }
            item["threshold"] = {
                "trigger_max_dd_pct": cr_limits.get("trigger_max_dd_pct"),
                "trigger_day_ret_pct": cr_limits.get("trigger_day_ret_pct"),
                "fallback_trigger_max_dd_pct": cr_limits.get("fallback_trigger_max_dd_pct"),
                "fallback_trigger_day_ret_pct": cr_limits.get("fallback_trigger_day_ret_pct"),
            }
        else:
            m_uni = re.search(r"ncode=(\d+)\s*<\s*MIN_UNI=(\d+)", reason)
            if m_uni:
                item["matched"] = "krx_clean_universe_degraded"
                item["source"] = "p0.risk_off.reason_text"
                item["observed"] = int(m_uni.group(1))
                item["threshold"] = int(m_uni.group(2))

        out.append(item)
    return out


def load_latest_p0_snapshot(log_dir: Path) -> Dict[str, Any]:
    """Load the latest p0_daily_check snapshot for adaptive entry policy."""
    out: Dict[str, Any] = {
        "path": None,
        "as_of_ymd": None,
        "market_regime": None,
        "risk_off_enabled": False,
        "risk_off_reasons": [],
        "kill_switch": {},
        "crash_risk_off": {},
    }
    try:
        p0p = _latest_p0_daily_check_report(log_dir)
        if not p0p:
            return out
        obj = json.loads(p0p.read_text(encoding="utf-8"))
        ro = _get_dict(obj, "risk_off")
        meta = _get_dict(obj, "meta")
        out["path"] = str(p0p)
        out["as_of_ymd"] = str(obj.get("as_of_ymd") or "") or None
        out["market_regime"] = str(meta.get("market_regime") or "").upper() or None
        out["risk_off_enabled"] = bool(ro.get("enabled"))
        rr = _get_list(ro, "reasons")
        out["risk_off_reasons"] = [str(x) for x in rr]
        ks = _get_dict(obj, "kill_switch")
        out["kill_switch"] = ks
        cro = _get_dict(obj, "crash_risk_off")
        out["crash_risk_off"] = cro
        return out
    except Exception:
        return out


def _load_latest_pnl_summary_for_ddm(log_dir: Path) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "path": str(log_dir / "paper_pnl_summary_last.json"),
        "exists": False,
        "as_of": None,
        "last_exit_date": None,
        "generated_at": None,
        "max_drawdown_pct": None,
        "current_mdd_abs": None,
    }
    p = log_dir / "paper_pnl_summary_last.json"
    if not p.exists():
        return out
    out["exists"] = True
    try:
        raw = p.read_text(encoding="utf-8-sig")
        obj = json.loads(raw)
        eq = obj.get("equity") if isinstance(obj.get("equity"), dict) else {}
        mdd_raw = eq.get("max_drawdown_pct")
        mdd_abs = _ddm_pct01(abs(_ddm_to_float(mdd_raw, 0.0)), 0.0)
        out["as_of"] = str(obj.get("as_of") or obj.get("as_of_ymd") or "") or None
        out["last_exit_date"] = str(eq.get("last_exit_date") or "") or None
        out["generated_at"] = obj.get("generated_at")
        out["max_drawdown_pct"] = mdd_raw
        out["current_mdd_abs"] = float(mdd_abs)
    except Exception as e:
        out["error"] = f"{type(e).__name__}:{e}"
    return out


def _align_p0_snapshot_with_latest_pnl_for_ddm(p0_snapshot: Dict[str, Any], log_dir: Path) -> Dict[str, Any]:
    """Keep DDM risk input no less conservative than the latest PnL summary."""
    if not isinstance(p0_snapshot, dict):
        p0_snapshot = {}
    pnl = _load_latest_pnl_summary_for_ddm(log_dir)
    ks = _get_dict(p0_snapshot, "kill_switch")
    metrics = _get_dict(ks, "metrics")
    p0_mdd_abs = _ddm_pct01(abs(_ddm_to_float(metrics.get("max_drawdown_pct"), 0.0)), 0.0)
    pnl_mdd_abs = _ddm_pct01(abs(_ddm_to_float(pnl.get("current_mdd_abs"), 0.0)), 0.0)
    p0_effective_as_of = str(metrics.get("last_exit_date") or p0_snapshot.get("as_of_ymd") or "") or None
    pnl_effective_as_of = str(pnl.get("last_exit_date") or pnl.get("as_of") or "") or None
    used = False
    reason = "pnl_missing_or_not_more_conservative"

    if bool(pnl.get("exists")) and pnl_mdd_abs > 0:
        pnl_is_newer = bool(pnl_effective_as_of and (not p0_effective_as_of or str(pnl_effective_as_of) > str(p0_effective_as_of)))
        pnl_is_more_conservative = float(pnl_mdd_abs) > float(p0_mdd_abs) + 1e-12
        if pnl_is_newer or pnl_is_more_conservative:
            metrics = dict(metrics)
            metrics["max_drawdown_pct"] = -float(max(p0_mdd_abs, pnl_mdd_abs))
            metrics["debug_lifetime_max_drawdown_pct"] = -float(
                max(
                    _ddm_pct01(abs(_ddm_to_float(metrics.get("debug_lifetime_max_drawdown_pct"), 0.0)), 0.0),
                    pnl_mdd_abs,
                )
            )
            metrics["mode"] = str(metrics.get("mode") or "rolling")
            metrics["ddm_pnl_alignment_source"] = str(pnl.get("path") or "")
            metrics["ddm_pnl_alignment_as_of"] = pnl.get("as_of")
            metrics["ddm_pnl_alignment_last_exit_date"] = pnl.get("last_exit_date")
            metrics["ddm_pnl_alignment_generated_at"] = pnl.get("generated_at")
            ks = dict(ks)
            ks["metrics"] = metrics
            p0_snapshot["kill_switch"] = ks
            used = True
            reason = "pnl_newer_or_more_conservative"

    p0_snapshot["ddm_pnl_alignment"] = {
        "used": bool(used),
        "reason": reason,
        "p0_effective_as_of": p0_effective_as_of,
        "p0_mdd_abs": float(p0_mdd_abs),
        "pnl_effective_as_of": pnl_effective_as_of,
        "pnl_mdd_abs": float(pnl_mdd_abs),
        "pnl_summary_path": pnl.get("path"),
        "pnl_summary_generated_at": pnl.get("generated_at"),
    }
    return p0_snapshot


def load_latest_macro_snapshot(log_dir: Path) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "path": None,
        "as_of_ymd": None,
        "regime": None,
        "risk_on": None,
        "crash_prob": None,
        "market_metrics": {},
        "rate_context": {},
        "fx_context": {},
        "exposure_multiplier": None,
        "source": None,
        "reasons": [],
    }
    try:
        p = latest_file(log_dir, "macro_signal_latest.json")
        if not p:
            return out
        obj = json.loads(p.read_text(encoding="utf-8"))
        out["path"] = str(p)
        out["as_of_ymd"] = str(obj.get("as_of_ymd") or "") or None
        out["regime"] = str(obj.get("regime") or "") or None
        out["risk_on"] = bool(obj.get("risk_on")) if ("risk_on" in obj) else None
        out["crash_prob"] = obj.get("crash_prob")
        mm = obj.get("market_metrics") if isinstance(obj.get("market_metrics"), dict) else {}
        out["market_metrics"] = mm
        rc = obj.get("rate_context") if isinstance(obj.get("rate_context"), dict) else {}
        out["rate_context"] = rc
        sources = obj.get("sources") if isinstance(obj.get("sources"), dict) else {}
        out["source"] = str(sources.get("macro") or obj.get("source") or "") or None
        rr = obj.get("reasons") if isinstance(obj.get("reasons"), list) else []
        out["reasons"] = [str(x) for x in rr]
        ext_macro = _get_dict(obj, "external_macro")
        ext_signals = _get_dict(ext_macro, "signals")
        ext_global = _get_dict(ext_signals, "global")
        if "exposure_multiplier" in ext_global:
            out["exposure_multiplier"] = ext_global.get("exposure_multiplier")
        feat = latest_file(log_dir, "macro_feature_external_latest.json")
        if feat:
            feat_obj = json.loads(feat.read_text(encoding="utf-8"))
            out["fx_context"] = _get_dict(feat_obj, "fx_context")
            if out.get("exposure_multiplier") is None:
                feat_signals = _get_dict(feat_obj, "signals")
                feat_global = _get_dict(feat_signals, "global")
                if "exposure_multiplier" in feat_global:
                    out["exposure_multiplier"] = feat_global.get("exposure_multiplier")
        return out
    except Exception:
        return out


def _build_trend_overlay_2026_context(
    *,
    cfg: Dict[str, Any],
    macro_snapshot: Dict[str, Any],
    today_ymd: str,
) -> Dict[str, Any]:
    pol = cfg.get("trend_overlay_2026") if isinstance(cfg.get("trend_overlay_2026"), dict) else {}
    if not isinstance(pol, dict):
        pol = {}
    enabled = bool(pol.get("enabled", False))
    out: Dict[str, Any] = {
        "enabled": enabled,
        "cutting_active": False,
        "rate_hawkish": None,
        "seasonal_multiplier": 1.0,
        "growth_codes": set(),
        "defensive_codes": set(),
        "ai_focus_codes": set(),
        "ai_focus_daily_top_n": 0,
        "ai_focus_max_open_count": 0,
        "ai_focus_single_name_cap_pct": 0.0,
        "growth_entry_weight": 1.0,
        "defensive_entry_weight": 1.0,
    }
    if not enabled:
        return out

    mm = _get_dict(macro_snapshot, "market_metrics")
    rc = _get_dict(macro_snapshot, "rate_context")
    rate_hawkish = _to_float(mm.get("rate_hawkish"), None)
    if rate_hawkish is None:
        rate_hawkish = _to_float(rc.get("hawkish_score"), None)
    out["rate_hawkish"] = rate_hawkish

    cutting_hawkish_max = _to_float(pol.get("cutting_hawkish_max"), -0.20)
    if (rate_hawkish is not None) and (rate_hawkish <= cutting_hawkish_max):
        out["cutting_active"] = True

    seasonal_map = _get_dict(pol, "seasonal_risk_multiplier")
    qtag = "Q1"
    if re.fullmatch(r"\d{8}", str(today_ymd or "")):
        try:
            m = int(str(today_ymd)[4:6])
            if m in (1, 2, 3):
                qtag = "Q1"
            elif m in (4, 5, 6):
                qtag = "Q2"
            elif m in (7, 8, 9):
                qtag = "Q3"
            else:
                qtag = "Q4"
        except Exception:
            qtag = "Q1"
    out["seasonal_multiplier"] = max(0.10, min(_to_float(seasonal_map.get(qtag), 1.0), 1.5))

    cut_cfg = _get_dict(pol, "cutting_overlay")
    out["growth_entry_weight"] = max(0.10, min(_to_float(cut_cfg.get("growth_entry_weight"), 1.15), 2.0))
    out["defensive_entry_weight"] = max(0.10, min(_to_float(cut_cfg.get("defensive_entry_weight"), 0.90), 2.0))

    ai_cfg = _get_dict(pol, "ai_semiconductor_overlay")
    semis = _get_list(ai_cfg, "semiconductor_codes")
    ai_soft = _get_list(ai_cfg, "ai_software_codes")
    it_hw = _get_list(ai_cfg, "it_hardware_codes")
    growth = _get_list(cut_cfg, "growth_codes")
    defensive = _get_list(cut_cfg, "defensive_codes")
    out["growth_codes"] = {norm_code(x) for x in (growth + semis + ai_soft + it_hw) if norm_code(x)}
    out["defensive_codes"] = {norm_code(x) for x in defensive if norm_code(x)}
    out["ai_focus_codes"] = {norm_code(x) for x in (semis + ai_soft + it_hw) if norm_code(x)}
    out["ai_focus_daily_top_n"] = max(0, int(_to_int(ai_cfg.get("daily_top_n"), 2) or 0))
    out["ai_focus_max_open_count"] = max(0, int(_to_int(ai_cfg.get("max_open_positions"), 2) or 0))
    out["ai_focus_single_name_cap_pct"] = max(
        0.0,
        min(_to_float(ai_cfg.get("single_name_cap_pct"), 0.15), 1.0),
    )
    return out


def _write_replay_quarantine(replay_summary: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    replay = replay_summary or {}
    rows = _get_list(replay, "manual_review_snapshot_rows")
    payload = {
        "generated_at": now_ts(),
        "as_of": now_ymd(),
        "status": "empty",
        "rows": int(len(rows)),
        "queue_guard_reason": str(replay.get("queue_guard_reason") or "").strip(),
        "path_csv": str(REPLAY_QUARANTINE_CSV_PATH),
        "path_json": str(REPLAY_QUARANTINE_JSON_PATH),
    }
    try:
        if rows:
            qdf = pd.DataFrame(rows)
            qdf.to_csv(REPLAY_QUARANTINE_CSV_PATH, index=False, encoding="utf-8-sig")
            REPLAY_QUARANTINE_JSON_PATH.write_text(json.dumps({
                **payload,
                "status": "written",
                "sample_rows": rows[:20],
            }, ensure_ascii=False, indent=2), encoding="utf-8")
            payload["status"] = "written"
        else:
            pd.DataFrame([]).to_csv(REPLAY_QUARANTINE_CSV_PATH, index=False, encoding="utf-8-sig")
            REPLAY_QUARANTINE_JSON_PATH.write_text(json.dumps({
                **payload,
                "status": "empty",
                "sample_rows": [],
            }, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        payload["status"] = "write_failed"
        payload["error"] = f"{type(e).__name__}: {e}"
    return payload
def _prune_replay_queue_after_quarantine(raw_df: pd.DataFrame, replay_summary: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    replay = replay_summary or {}
    rows = replay.get("manual_review_snapshot_rows") if isinstance(replay.get("manual_review_snapshot_rows"), list) else []
    queue_guard_reason = str(replay.get("queue_guard_reason") or "").strip()
    result: Dict[str, Any] = {
        "attempted": False,
        "status": "not_needed",
        "removed_rows": 0,
        "remaining_rows": int(len(raw_df)) if raw_df is not None else 0,
        "queue_guard_reason": queue_guard_reason,
        "source_path": str(REPLAY_ORDERS_PATH),
        "backup_path": "",
    }
    if raw_df is None or raw_df.empty or not rows or not queue_guard_reason or not REPLAY_ORDERS_PATH.exists():
        return result

    def _row_key_from_dict(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
        return (
            str(row.get("code") or "").zfill(6),
            str(row.get("next_session_ymd") or "").strip(),
            str(row.get("replay_source_order_id") or "").strip(),
            str(row.get("replay_chain_id") or "").strip(),
        )

    quarantine_keys = {_row_key_from_dict(row) for row in rows if isinstance(row, dict)}
    if not quarantine_keys:
        return result

    work = raw_df.copy()
    code_ser = work["code"].astype(str).str.zfill(6) if "code" in work.columns else pd.Series("", index=work.index)
    next_ser = work["next_session_ymd"].astype(str).str.strip() if "next_session_ymd" in work.columns else pd.Series("", index=work.index)
    order_ser = work["replay_source_order_id"].astype(str).str.strip() if "replay_source_order_id" in work.columns else (
        work["order_id"].astype(str).str.strip() if "order_id" in work.columns else pd.Series("", index=work.index)
    )
    chain_ser = work["replay_chain_id"].astype(str).str.strip() if "replay_chain_id" in work.columns else pd.Series("", index=work.index)
    row_keys = list(zip(code_ser.tolist(), next_ser.tolist(), order_ser.tolist(), chain_ser.tolist()))
    remove_mask = pd.Series([key in quarantine_keys for key in row_keys], index=work.index)
    removed_rows = int(remove_mask.sum())
    if removed_rows <= 0:
        return result

    kept = work.loc[~remove_mask].copy()
    result["attempted"] = True
    result["removed_rows"] = removed_rows
    result["remaining_rows"] = int(len(kept))
    try:
        REPLAY_QUEUE_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        backup_path = REPLAY_QUEUE_ARCHIVE_DIR / f"replay_orders_latest.{now_ymd()}_{datetime.now().strftime('%H%M%S')}.bak.csv"
        shutil.copy2(REPLAY_ORDERS_PATH, backup_path)
        kept.to_csv(REPLAY_ORDERS_PATH, index=False, encoding="utf-8-sig")
        result["backup_path"] = str(backup_path)
        result["status"] = "pruned"
    except Exception as e:
        result["status"] = "write_failed"
        result["error"] = f"{type(e).__name__}: {e}"
    return result
def _replay_queue_has_lineage_gaps(raw_df: pd.DataFrame) -> bool:
    if raw_df is None or raw_df.empty:
        return False
    work = raw_df.copy()
    source_order = work["replay_source_order_id"].astype(str).str.strip() if "replay_source_order_id" in work.columns else (
        work["order_id"].astype(str).str.strip() if "order_id" in work.columns else pd.Series("", index=work.index)
    )
    source_intent = work["replay_source_intent_id"].astype(str).str.strip() if "replay_source_intent_id" in work.columns else (
        work["intent_id"].astype(str).str.strip() if "intent_id" in work.columns else pd.Series("", index=work.index)
    )
    source_trace = work["replay_source_trace_id"].astype(str).str.strip() if "replay_source_trace_id" in work.columns else (
        work["trace_id"].astype(str).str.strip() if "trace_id" in work.columns else pd.Series("", index=work.index)
    )
    chain_id = work["replay_chain_id"].astype(str).str.strip() if "replay_chain_id" in work.columns else pd.Series("", index=work.index)
    gaps = source_order.eq("") | ((source_intent.eq("")) & (source_trace.eq(""))) | chain_id.eq("")
    return bool(gaps.any())
def _replay_queue_regeneration_reason(raw_df: pd.DataFrame) -> str:
    if raw_df is None or raw_df.empty:
        return ""
    if _replay_queue_has_lineage_gaps(raw_df):
        return "replay_queue_lineage_gap"

    expected_asof = _infer_latest_fill_asof(ROOTB_REGEN_LIVE_FILLS)
    if not expected_asof:
        return ""

    if "exec_date" in raw_df.columns:
        exec_dates = (
            raw_df["exec_date"]
            .astype(str)
            .str.replace(r"[^0-9]", "", regex=True)
            .str[:8]
        )
        exec_dates = exec_dates[exec_dates.str.len() == 8]
        if len(exec_dates) and str(exec_dates.max() or "") < expected_asof:
            return "replay_queue_stale_exec_date"

    if "next_session_ymd" in raw_df.columns:
        next_sessions = (
            raw_df["next_session_ymd"]
            .astype(str)
            .str.replace(r"[^0-9]", "", regex=True)
            .str[:8]
        )
        next_sessions = next_sessions[next_sessions.str.len() == 8]
        if len(next_sessions) and str(next_sessions.max() or "") < now_ymd():
            return "replay_queue_expired_only"

    return ""
def _summarize_replay_queue_scan(raw_df: pd.DataFrame) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "rows": 0,
        "trigger_reason": "",
        "latest_exec_date": "",
        "latest_next_session_ymd": "",
        "expired_rows": 0,
        "missing_lineage_rows": 0,
    }
    if raw_df is None or raw_df.empty:
        return summary

    work = raw_df.copy()
    summary["rows"] = int(len(work))

    if "exec_date" in work.columns:
        exec_dates = (
            work["exec_date"]
            .astype(str)
            .str.replace(r"[^0-9]", "", regex=True)
            .str[:8]
        )
        exec_dates = exec_dates[exec_dates.str.len() == 8]
        if len(exec_dates):
            summary["latest_exec_date"] = str(exec_dates.max() or "")

    if "next_session_ymd" in work.columns:
        next_sessions = (
            work["next_session_ymd"]
            .astype(str)
            .str.replace(r"[^0-9]", "", regex=True)
            .str[:8]
        )
        next_sessions = next_sessions[next_sessions.str.len() == 8]
        if len(next_sessions):
            summary["latest_next_session_ymd"] = str(next_sessions.max() or "")
            summary["expired_rows"] = int((next_sessions < now_ymd()).sum())

    source_order = work["replay_source_order_id"].astype(str).str.strip() if "replay_source_order_id" in work.columns else (
        work["order_id"].astype(str).str.strip() if "order_id" in work.columns else pd.Series("", index=work.index)
    )
    source_intent = work["replay_source_intent_id"].astype(str).str.strip() if "replay_source_intent_id" in work.columns else (
        work["intent_id"].astype(str).str.strip() if "intent_id" in work.columns else pd.Series("", index=work.index)
    )
    source_trace = work["replay_source_trace_id"].astype(str).str.strip() if "replay_source_trace_id" in work.columns else (
        work["trace_id"].astype(str).str.strip() if "trace_id" in work.columns else pd.Series("", index=work.index)
    )
    chain_id = work["replay_chain_id"].astype(str).str.strip() if "replay_chain_id" in work.columns else pd.Series("", index=work.index)
    lineage_gaps = source_order.eq("") | ((source_intent.eq("")) & (source_trace.eq(""))) | chain_id.eq("")
    summary["missing_lineage_rows"] = int(lineage_gaps.sum())
    summary["trigger_reason"] = _replay_queue_regeneration_reason(work)
    return summary
def _file_meta_brief(path: Path) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "label": path.name,
        "path": str(path),
        "exists": bool(path.exists()),
        "size_bytes": None,
        "modified_at": None,
    }
    if path.exists():
        try:
            stat = path.stat()
            info["size_bytes"] = int(stat.st_size)
            info["modified_at"] = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return info
def _collect_replay_regeneration_input_files() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    live_meta = _file_meta_brief(ROOTB_REGEN_LIVE_FILLS)
    live_meta["label"] = "live_fills.csv"
    items.append(live_meta)

    replay_meta = _file_meta_brief(REPLAY_ORDERS_PATH)
    replay_meta["label"] = "replay_orders_latest.csv"
    items.append(replay_meta)

    for eval_dir in ROOTB_REGEN_EVAL_DIRS:
        latest_eval: Optional[Path] = None
        if eval_dir.exists():
            matches = sorted(eval_dir.glob("orders_*_eval.xlsx"), reverse=True)
            if matches:
                latest_eval = matches[0]
        eval_meta = _file_meta_brief(latest_eval or eval_dir)
        eval_meta["label"] = f"latest_eval::{eval_dir.name}"
        if latest_eval is None:
            eval_meta["path"] = str(eval_dir / "orders_YYYYMMDD_eval.xlsx")
            eval_meta["exists"] = False
            eval_meta["size_bytes"] = None
            eval_meta["modified_at"] = None
        items.append(eval_meta)

    return items
def _infer_latest_fill_asof(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", usecols=lambda col: str(col).strip().lower() == "date")
    except Exception:
        return ""
    if df is None or df.empty or "date" not in df.columns:
        return ""
    dates = (
        df["date"]
        .astype(str)
        .str.replace(r"[^0-9]", "", regex=True)
        .str[:8]
    )
    dates = dates[dates.str.len() == 8]
    return str(dates.max() or "") if len(dates) else ""
def _expected_eval_candidates(asof: str) -> List[str]:
    if not str(asof or "").strip():
        return []
    return [str(eval_dir / f"orders_{asof}_eval.xlsx") for eval_dir in ROOTB_REGEN_EVAL_DIRS]
def _ensure_expected_eval_template(asof: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "attempted": False,
        "status": "not_needed",
        "expected_asof": str(asof or "").strip(),
        "target_path": "",
        "source_path": "",
        "message": "",
    }
    asof = str(asof or "").strip()
    if not asof:
        result["status"] = "missing_asof"
        result["message"] = "expected_asof missing"
        return result

    candidates = [Path(path) for path in _expected_eval_candidates(asof)]
    existing = next((path for path in candidates if path.exists()), None)
    if existing is not None:
        result["status"] = "exact_exists"
        result["target_path"] = str(existing)
        result["message"] = "expected_asof eval already exists"
        return result

    latest_sources: List[Path] = []
    for eval_dir in ROOTB_REGEN_EVAL_DIRS:
        if not eval_dir.exists():
            continue
        matches = sorted(eval_dir.glob("orders_*_eval.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
        if matches:
            latest_sources.append(matches[0])
    if not latest_sources:
        result["status"] = "no_eval_source"
        result["message"] = "no eval source available for copy"
        return result

    src = sorted(latest_sources, key=lambda p: p.stat().st_mtime, reverse=True)[0]
    dst = ROOTB_DIR / "data" / "orders" / "_hold" / f"orders_{asof}_eval.xlsx"
    result["attempted"] = True
    result["source_path"] = str(src)
    result["target_path"] = str(dst)
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        result["status"] = "copied"
        result["message"] = "latest eval copied to expected_asof hold template"
    except Exception as e:
        result["status"] = "copy_failed"
        result["message"] = f"{type(e).__name__}: {e}"
    return result
def _replay_regeneration_preflight_issue(input_files: List[Dict[str, Any]]) -> str:
    live_meta = next((item for item in input_files if str(item.get("label")) == "live_fills.csv"), {})
    if not bool(live_meta.get("exists")):
        return "live_fills_missing"
    if int(live_meta.get("size_bytes") or 0) <= 0:
        return "live_fills_empty"

    eval_items = [item for item in input_files if str(item.get("label", "")).startswith("latest_eval::")]
    if not any(bool(item.get("exists")) for item in eval_items):
        return "eval_template_missing"
    return ""
def _replay_regeneration_action_hints(
    status: str,
    skip_reason: str,
    expected_asof: str,
    expected_eval_candidates: List[str],
) -> List[str]:
    hints: List[str] = []
    reason = str(skip_reason or "").strip()
    state = str(status or "").strip()
    if state == "script_missing":
        hints.append("make_orders_exec_from_fills.py 경로를 확인하고 RootB 배선을 복구")
    if reason == "live_fills_missing":
        hints.append("vibe_paper_daily.ps1 또는 vibe_broker_daily.ps1를 먼저 실행해 live_fills.csv를 생성")
    elif reason == "live_fills_empty":
        hints.append("live_fills.csv가 비어 있으므로 체결 수집 경로와 당일 fill 적재 여부를 점검")
    elif reason == "eval_template_missing":
        suffix = f"기준일 {expected_asof}" if expected_asof else "기준일 미확정"
        hints.append(f"orders_{{YYYYMMDD}}_eval.xlsx 생성 경로를 점검하고 {suffix}용 eval 템플릿을 준비")
        if expected_eval_candidates:
            hints.append("예상 eval 경로: " + " | ".join(expected_eval_candidates[:3]))
    elif reason == "replay_queue_stale_exec_date":
        hints.append("replay_orders_latest.csv의 exec_date가 live_fills 기준일보다 오래되어 orders_exec/replay queue를 다시 생성")
    elif reason == "replay_queue_expired_only":
        hints.append("재발행 큐가 모두 지난 세션 대상으로 남아 있어 orders_exec/replay queue를 다시 생성")
    elif state == "failed":
        hints.append("make_orders_exec_from_fills.py stderr를 확인하고 fills/eval 입력 스키마를 점검")
    elif state == "exception":
        hints.append("재생성 호출 예외가 발생했으므로 python 실행 경로와 RootB 작업 디렉터리를 점검")
    return hints[:5]
def _run_replay_queue_regeneration(trigger_reason: str) -> Dict[str, Any]:
    started_at = now_ts()
    expected_asof = _infer_latest_fill_asof(ROOTB_REGEN_LIVE_FILLS)
    expected_eval_fix = _ensure_expected_eval_template(expected_asof)
    input_files = _collect_replay_regeneration_input_files()
    expected_eval_candidates = _expected_eval_candidates(expected_asof)
    result: Dict[str, Any] = {
        "attempted": False,
        "ok": False,
        "status": "preflight_pending",
        "trigger_reason": str(trigger_reason or "").strip() or "unknown",
        "skip_reason": "",
        "action_hints": [],
        "expected_asof": expected_asof,
        "expected_eval_candidates": expected_eval_candidates,
        "expected_eval_fix": expected_eval_fix,
        "script_path": str(ROOTB_REPLAY_REGEN_SCRIPT),
        "started_at": started_at,
        "finished_at": started_at,
        "returncode": None,
        "stdout": "",
        "stderr": "",
        "input_files": input_files,
    }
    if not ROOTB_REPLAY_REGEN_SCRIPT.exists():
        result["status"] = "script_missing"
        result["stderr"] = f"missing script: {ROOTB_REPLAY_REGEN_SCRIPT}"
        result["action_hints"] = _replay_regeneration_action_hints(result["status"], result["skip_reason"], expected_asof, expected_eval_candidates)
        result["finished_at"] = now_ts()
        return result
    preflight_issue = _replay_regeneration_preflight_issue(input_files)
    if not str(expected_asof or "").strip():
        preflight_issue = "missing_expected_asof"
    if preflight_issue:
        result["status"] = "skipped_preflight"
        result["skip_reason"] = preflight_issue
        result["stderr"] = f"preflight skipped: {preflight_issue}"
        result["action_hints"] = _replay_regeneration_action_hints(result["status"], result["skip_reason"], expected_asof, expected_eval_candidates)
        result["finished_at"] = now_ts()
        return result
    try:
        result["attempted"] = True
        proc = subprocess.run(
            [sys.executable, str(ROOTB_REPLAY_REGEN_SCRIPT), str(expected_asof)],
            cwd=str(ROOTB_DIR),
            capture_output=True,
            text=True,
            timeout=180,
        )
        result["ok"] = proc.returncode == 0
        result["status"] = "ok" if result["ok"] else "failed"
        result["returncode"] = proc.returncode
        result["stdout"] = (proc.stdout or "").strip()[-4000:]
        result["stderr"] = (proc.stderr or "").strip()[-4000:]
    except Exception as e:
        result["status"] = "exception"
        result["stderr"] = f"{type(e).__name__}: {e}"
    result["action_hints"] = _replay_regeneration_action_hints(result["status"], result["skip_reason"], expected_asof, expected_eval_candidates)
    result["finished_at"] = now_ts()
    return result
def _write_replay_queue_status(max_age_days: int, replay_used_today: int) -> Dict[str, Any]:
    raw = _load_replay_orders_raw(max_age_days=max_age_days)
    today = now_ymd()
    if raw.empty:
        payload = {
            "generated_at": now_ts(),
            "as_of": today,
            "rows_total": 0,
            "due_today_rows": 0,
            "future_rows": 0,
            "expired_rows": 0,
            "replay_used_today": int(replay_used_today),
            "next_session_ymd": [],
            "expired_order_ids": [],
        }
        REPLAY_QUEUE_STATUS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    due_today = raw[raw["next_session_ymd"] == today].copy()
    future = raw[raw["next_session_ymd"] > today].copy()
    expired = raw[raw["next_session_ymd"] < today].copy()
    expired_ids: List[str] = []
    if "order_id" in expired.columns:
        expired_ids = [str(v).strip() for v in expired["order_id"].astype(str).tolist() if str(v).strip()][:20]
    payload = {
        "generated_at": now_ts(),
        "as_of": today,
        "rows_total": int(len(raw)),
        "due_today_rows": int(len(due_today)),
        "future_rows": int(len(future)),
        "expired_rows": int(len(expired)),
        "replay_used_today": int(replay_used_today),
        "next_session_ymd": sorted({str(v).strip() for v in raw["next_session_ymd"].astype(str).tolist() if str(v).strip()}),
        "expired_order_ids": expired_ids,
    }
    REPLAY_QUEUE_STATUS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
def _sync_rootb_replay_summary(queue_status: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = queue_status or {}
    result: Dict[str, Any] = {
        "attempted": False,
        "status": "not_needed",
        "path": str(ROOTB_REPLAY_SUMMARY_LATEST),
        "rows": int(payload.get("rows_total", 0) or 0),
    }
    if not payload:
        return result
    doc = {
        "generated_at": payload.get("generated_at") or now_ts(),
        "asof": payload.get("as_of") or now_ymd(),
        "rows": int(payload.get("rows_total", 0) or 0),
        "due_today_rows": int(payload.get("due_today_rows", 0) or 0),
        "future_rows": int(payload.get("future_rows", 0) or 0),
        "expired_rows": int(payload.get("expired_rows", 0) or 0),
        "replay_used_today": int(payload.get("replay_used_today", 0) or 0),
        "next_session_ymd": payload.get("next_session_ymd") or [],
        "expired_order_ids": payload.get("expired_order_ids") or [],
        "path": str(REPLAY_ORDERS_PATH),
    }
    try:
        ROOTB_REPLAY_SUMMARY_LATEST.parent.mkdir(parents=True, exist_ok=True)
        ROOTB_REPLAY_SUMMARY_LATEST.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
        result["attempted"] = True
        result["status"] = "written"
    except Exception as e:
        result["attempted"] = True
        result["status"] = "write_failed"
        result["error"] = f"{type(e).__name__}: {e}"
    return result
def _reconcile_replay_runtime_artifacts(
    max_age_days: int,
    open_positions: List[Dict[str, Any]],
    replay_used_today: int,
) -> Dict[str, Any]:
    raw = _load_replay_orders_raw(max_age_days=max_age_days)
    scan = _summarize_replay_queue_scan(raw)
    queue_guard_reason = str(scan.get("trigger_reason") or "").strip()
    _, replay_summary = _partition_replay_orders_for_recovery(
        raw,
        open_positions,
        queue_guard_reason=queue_guard_reason,
    )
    quarantine = _write_replay_quarantine(replay_summary)
    prune = {
        "attempted": False,
        "status": "post_regen_reset",
        "removed_rows": 0,
        "remaining_rows": int(len(raw)),
        "queue_guard_reason": queue_guard_reason,
        "source_path": str(REPLAY_ORDERS_PATH),
        "backup_path": "",
    }
    queue_status = _write_replay_queue_status(max_age_days, replay_used_today)
    summary_sync = _sync_rootb_replay_summary(queue_status)
    consistency = _write_replay_consistency_status(
        replay_queue_status=queue_status,
        replay_recovery_summary=replay_summary,
        replay_queue_scan=scan,
        replay_quarantine=quarantine,
        replay_prune=prune,
    )
    return {
        "replay_queue_scan": scan,
        "replay_recovery_summary": replay_summary,
        "replay_quarantine": quarantine,
        "replay_prune": prune,
        "replay_queue_status": queue_status,
        "replay_consistency": consistency,
        "summary_sync": summary_sync,
    }
def _write_recovery_status(
    recovery_summary: Dict[str, Any],
    replay_summary: Optional[Dict[str, Any]] = None,
    replay_queue_scan: Optional[Dict[str, Any]] = None,
    replay_quarantine: Optional[Dict[str, Any]] = None,
    replay_prune: Optional[Dict[str, Any]] = None,
    replay_consistency: Optional[Dict[str, Any]] = None,
    replay_consistency_remediation: Optional[Dict[str, Any]] = None,
    replay_summary_sync: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    open_after = int((recovery_summary or {}).get("open_positions_after", 0) or 0)
    auto_positions = int((recovery_summary or {}).get("auto_recovered_positions", 0) or 0)
    manual_positions = int((recovery_summary or {}).get("manual_review_positions", 0) or 0)
    replay = replay_summary or {}
    payload = {
        "generated_at": now_ts(),
        "as_of": now_ymd(),
        "state_path": str(STATE_PATH),
        "replay_orders_path": str(REPLAY_ORDERS_PATH),
        "open_position_recovery": recovery_summary or {},
        "replay_filter": replay,
        "replay_queue_scan": replay_queue_scan or {},
        "replay_quarantine": replay_quarantine or {},
        "replay_prune": replay_prune or {},
        "replay_consistency": replay_consistency or {},
        "replay_consistency_remediation": replay_consistency_remediation or {},
        "replay_summary_sync": replay_summary_sync or {},
        "policy": {
            "resume_open_positions": open_after,
            "auto_recovered_positions": auto_positions,
            "manual_review_positions": manual_positions,
            "resume_replay_rows": int(replay.get("resume_rows", replay.get("eligible_rows", 0)) or 0),
            "discard_replay_rows": int(replay.get("discard_rows", 0) or 0),
            "manual_review_replay_rows": int(replay.get("manual_review_rows", 0) or 0),
        },
        "details": {
            "auto_recovered_positions": (recovery_summary or {}).get("auto_recovered_details", []) or [],
            "manual_review_positions": (recovery_summary or {}).get("manual_review_details", []) or [],
            "discard_replay_rows": replay.get("discard_details", []) or [],
            "manual_review_replay_rows": replay.get("manual_review_details", []) or [],
            "resume_replay_rows": replay.get("resume_details", []) or [],
        },
    }
    RECOVERY_STATUS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
def _build_order_pre_validation_summary(
    runtime_ymd: str,
    ops_alert: Dict[str, Any],
    max_new: int,
    candidates_after_caps: int,
    entry_ready: int,
    filled_count: int,
    capital_total: float,
    current_open_notional: float,
    gross_cap_krw: Optional[float],
    daily_new_cap_krw: Optional[float],
    max_positions_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    no_fill_reasons = [str(x).strip() for x in (ops_alert.get("no_fill_reasons") or []) if str(x).strip()]
    decision_reason_counts = _get_dict(ops_alert, "entry_decision_reason_counts")
    terminal_no_entry_reasons = {
        "FALLBACK_EXHAUSTED",
        "BLOCK_FALLBACK_STAGE",
        "NO_NEXT_TRADING_DAY",
        "QUOTE_STALE_BLOCK",
        "SIGNAL_TTL_EXPIRED",
        "EXEC_QUALITY_FAIL",
        "GAP_RISK_HISTORY_BLOCK",
        "MAX_POSITIONS_BLOCK",
        "CODE_ALREADY_OPEN",
        "DROP_STALE_SIGNAL",
        "CAP_SIGNALDATE_TOP3_BY_SCORE",
    }
    def _is_terminal_no_entry_reason(reason: Any) -> bool:
        reason_u = str(reason or "").strip().upper()
        if reason_u in terminal_no_entry_reasons:
            return True
        return "CLOSE_CUTOFF" in reason_u or reason_u.startswith("FAIL_CLOSED_PROPAGATE(")

    terminal_no_entry_count = int(
        sum(
            int(_to_int(v, 0) or 0)
            for k, v in decision_reason_counts.items()
            if bool(_is_terminal_no_entry_reason(k))
        )
    )
    cap_block_count = int(_to_int(ops_alert.get("cap_block"), 0))
    idempotent_skip_count = int(_to_int(ops_alert.get("idempotent_skip"), 0))
    max_new_skip_count = int(_to_int(ops_alert.get("max_new_skip"), 0))
    filled_count = int(_to_int(filled_count, 0) or 0)
    effective_entry_ready = max(int(entry_ready), filled_count)
    status = "PASS"
    issues: List[str] = []
    guard_states = {
        "max_new_guard": "PASS" if int(max_new) > 0 or filled_count > 0 else ("WARN" if int(candidates_after_caps) <= 0 else "BLOCK"),
        "candidate_pool_guard": "PASS" if int(candidates_after_caps) > 0 else "WARN",
        "entry_ready_guard": (
            "PASS"
            if int(effective_entry_ready) > 0 or terminal_no_entry_count >= int(candidates_after_caps)
            else ("WARN" if int(candidates_after_caps) > 0 else "NA")
        ),
        "risk_cap_guard": "PASS" if cap_block_count <= 0 else "WARN",
        "idempotency_guard": "PASS" if idempotent_skip_count <= 0 else "WARN",
    }
    if guard_states["max_new_guard"] == "BLOCK":
        status = "WARN"
        issues.append("max_new_zero_with_candidates")
    if int(candidates_after_caps) > 0 and int(effective_entry_ready) <= 0 and terminal_no_entry_count < int(candidates_after_caps):
        status = "WARN"
        issues.append("entry_ready_zero_after_caps")
    if cap_block_count > 0:
        status = "WARN"
        issues.append(f"cap_block_count={cap_block_count}")
    if idempotent_skip_count > 0:
        status = "WARN"
        issues.append(f"idempotent_skip_count={idempotent_skip_count}")
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "counts": {
            "candidates_after_caps": int(candidates_after_caps),
            "entry_ready": int(entry_ready),
            "filled_count": int(filled_count),
            "effective_entry_ready": int(effective_entry_ready),
            "max_new": int(max_new),
            "cap_block_count": int(cap_block_count),
            "idempotent_skip_count": int(idempotent_skip_count),
            "max_new_skip_count": int(max_new_skip_count),
            "terminal_no_entry_count": int(terminal_no_entry_count),
        },
        "limits": {
            "capital_total": float(capital_total or 0.0),
            "current_open_notional": float(current_open_notional or 0.0),
            "gross_cap_krw": (None if gross_cap_krw is None else float(gross_cap_krw)),
            "daily_new_cap_krw": (None if daily_new_cap_krw is None else float(daily_new_cap_krw)),
            "max_positions": max_positions_meta or {},
        },
        "guard_states": guard_states,
        "no_fill_reasons": no_fill_reasons,
        "entry_decision_reason_counts": decision_reason_counts,
        "issues": issues,
    }
def _build_order_lifecycle_summary(
    runtime_ymd: str,
    order_artifacts: Dict[str, Any],
    entry_exit_lifecycle: Dict[str, Any],
) -> Dict[str, Any]:
    order_rows = list(order_artifacts.get("order_rows") or [])
    issues: List[str] = []
    state_counts: Dict[str, int] = {}
    buy_order_rows = 0
    sell_order_rows = 0
    trade_chain_unmatched_rows = 0
    for row in order_rows:
        state = str(row.get("state") or "").strip().upper()
        if state:
            state_counts[state] = int(state_counts.get(state, 0)) + 1
        side = str(row.get("side") or "").strip().upper()
        if side == "BUY":
            buy_order_rows += 1
        elif side == "SELL":
            sell_order_rows += 1
            if not bool(row.get("trade_chain_matched")):
                trade_chain_unmatched_rows += 1
    buy_fill_rows = int(order_artifacts.get("buy_fill_rows", 0) or 0)
    sell_fill_rows = int(order_artifacts.get("sell_fill_rows", 0) or 0)
    entry_fill_rows = int(entry_exit_lifecycle.get("entry_fill_rows", 0) or 0)
    exit_fill_rows = int(entry_exit_lifecycle.get("exit_fill_rows", 0) or 0)
    if buy_fill_rows != entry_fill_rows:
        issues.append(f"buy_fill_mismatch:{buy_fill_rows}!={entry_fill_rows}")
    if sell_fill_rows != exit_fill_rows:
        issues.append(f"sell_fill_mismatch:{sell_fill_rows}!={exit_fill_rows}")
    if trade_chain_unmatched_rows > 0:
        issues.append(f"sell_trade_chain_unmatched_rows:{trade_chain_unmatched_rows}")
    status = "PASS" if not issues else "FAIL"
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "counts": {
            "order_rows": int(len(order_rows)),
            "buy_order_rows": int(buy_order_rows),
            "sell_order_rows": int(sell_order_rows),
            "buy_fill_rows": int(buy_fill_rows),
            "sell_fill_rows": int(sell_fill_rows),
            "trade_rows": int(order_artifacts.get("closed_trade_rows", 0) or 0),
            "trade_chain_unmatched_rows": int(trade_chain_unmatched_rows),
        },
        "state_counts": state_counts,
        "issues": issues,
        "rows": order_rows[:50],
    }
def _build_order_amend_cancel_summary(runtime_ymd: str, max_age_days: int) -> Dict[str, Any]:
    raw = _load_replay_orders_raw(max_age_days=max_age_days)
    if not isinstance(raw, pd.DataFrame) or raw.empty:
        return {
            "generated_at": now_ts(),
            "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
            "status": "PASS",
            "counts": {
                "raw_rows": 0,
                "amend_reference_rows": 0,
                "cancel_reference_rows": 0,
                "missing_original_order_id_rows": 0,
            },
            "issues": [],
        }
    work = raw.copy()
    for col in ["side", "remaining_action", "remaining_qty", "source_order_id", "original_order_id"]:
        if col not in work.columns:
            work[col] = ""
    work["remaining_qty_n"] = pd.to_numeric(work["remaining_qty"], errors="coerce").fillna(0).astype(int)
    amend_reference_rows = int(work["source_order_id"].astype(str).str.strip().ne("").sum())
    cancel_reference_rows = int(((work["remaining_action"].astype(str).str.strip().str.upper() == "CANCEL") | (work["remaining_qty_n"] <= 0)).sum())
    missing_original_order_id_rows = int(
        (
            work["source_order_id"].astype(str).str.strip().ne("")
            & work["original_order_id"].astype(str).str.strip().eq("")
        ).sum()
    )
    issues: List[str] = []
    status = "PASS"
    if missing_original_order_id_rows > 0:
        status = "WARN"
        issues.append(f"missing_original_order_id_rows={missing_original_order_id_rows}")
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "counts": {
            "raw_rows": int(len(work)),
            "amend_reference_rows": int(amend_reference_rows),
            "cancel_reference_rows": int(cancel_reference_rows),
            "missing_original_order_id_rows": int(missing_original_order_id_rows),
        },
        "issues": issues,
    }
def _build_order_chain_ssot_summary(
    runtime_ymd: str,
    order_lifecycle_summary: Dict[str, Any],
    persisted_today_metrics: Dict[str, Any],
    entry_exit_lifecycle: Dict[str, Any],
) -> Dict[str, Any]:
    counts = _get_dict(order_lifecycle_summary, "counts")
    total_fill_rows = int(persisted_today_metrics.get("entry_fill_rows", 0) or 0) + int(persisted_today_metrics.get("exit_fill_rows", 0) or 0)
    order_rows = int(counts.get("order_rows", 0) or 0)
    buy_fill_rows = int(counts.get("buy_fill_rows", 0) or 0)
    sell_fill_rows = int(counts.get("sell_fill_rows", 0) or 0)
    entry_fill_rows = int(entry_exit_lifecycle.get("entry_fill_rows", 0) or 0)
    exit_fill_rows = int(entry_exit_lifecycle.get("exit_fill_rows", 0) or 0)
    issues: List[str] = []
    if order_rows != total_fill_rows:
        issues.append(f"order_rows_vs_fills:{order_rows}!={total_fill_rows}")
    if buy_fill_rows != entry_fill_rows:
        issues.append(f"buy_fill_rows_vs_pending:{buy_fill_rows}!={entry_fill_rows}")
    if sell_fill_rows != exit_fill_rows:
        issues.append(f"sell_fill_rows_vs_pending:{sell_fill_rows}!={exit_fill_rows}")
    status = "PASS" if not issues else "FAIL"
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "counts": {
            "order_rows": int(order_rows),
            "total_fill_rows": int(total_fill_rows),
            "entry_fill_rows": int(entry_fill_rows),
            "exit_fill_rows": int(exit_fill_rows),
            "closed_trade_rows": int(persisted_today_metrics.get("closed_trade_rows", 0) or 0),
        },
        "issues": issues,
    }
def _build_order_recovery_replay_summary(
    runtime_ymd: str,
    replay_recovery_summary: Dict[str, Any],
    replay_queue_status: Dict[str, Any],
    order_artifacts: Dict[str, Any],
) -> Dict[str, Any]:
    order_rows = list(order_artifacts.get("order_rows") or [])
    replay_chain_rows = int(sum(1 for row in order_rows if str(row.get("replay_chain_id") or "").strip()))
    source_order_rows = int(sum(1 for row in order_rows if str(row.get("source_order_id") or "").strip()))
    issues: List[str] = []
    status = "PASS"
    if str(replay_queue_status.get("status") or "").strip().upper() == "FAIL":
        status = "WARN"
        issues.append("replay_queue_status_fail")
    if int(replay_recovery_summary.get("manual_review_rows", 0) or 0) > 0:
        status = "WARN"
        issues.append(f"manual_review_rows={int(replay_recovery_summary.get('manual_review_rows', 0) or 0)}")
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "counts": {
            "replay_due_rows": int(replay_recovery_summary.get("eligible_rows", 0) or 0),
            "replay_future_rows": int(replay_queue_status.get("future_rows", 0) or 0),
            "replay_expired_rows": int(replay_queue_status.get("expired_rows", 0) or 0),
            "replay_discard_rows": int(replay_recovery_summary.get("discard_rows", 0) or 0),
            "manual_review_rows": int(replay_recovery_summary.get("manual_review_rows", 0) or 0),
            "order_rows_with_replay_chain": int(replay_chain_rows),
            "order_rows_with_source_order_id": int(source_order_rows),
        },
        "issues": issues,
    }
def _build_order_session_rule_summary(runtime_ymd: str, order_artifacts: Dict[str, Any], max_age_days: int) -> Dict[str, Any]:
    order_rows = list(order_artifacts.get("order_rows") or [])
    allowed_sessions = {
        "NEXT_OPEN",
        "SAME_CLOSE",
        "INTRADAY_REALTIME",
        "REGULAR_DAILY_EXIT",
    }
    allowed_tif = {"DAY", "", "IOC", "FOK"}
    invalid_session_rows = 0
    invalid_tif_rows = 0
    for row in order_rows:
        if str(row.get("intended_session") or "").strip().upper() not in allowed_sessions:
            invalid_session_rows += 1
        if str(row.get("time_in_force") or "").strip().upper() not in allowed_tif:
            invalid_tif_rows += 1
    replay_raw = _load_replay_orders_raw(max_age_days=max_age_days)
    replay_session_rows = 0
    replay_session_issues = 0
    if isinstance(replay_raw, pd.DataFrame) and not replay_raw.empty:
        replay_session_rows = int(len(replay_raw))
        if "next_session_ymd" in replay_raw.columns:
            next_session = replay_raw["next_session_ymd"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
            replay_session_issues = int((next_session.str.len() != 8).sum())
    issues: List[str] = []
    status = "PASS"
    if invalid_session_rows > 0:
        status = "FAIL"
        issues.append(f"invalid_session_rows={invalid_session_rows}")
    if invalid_tif_rows > 0:
        status = "FAIL"
        issues.append(f"invalid_time_in_force_rows={invalid_tif_rows}")
    if replay_session_issues > 0:
        status = "WARN" if status == "PASS" else status
        issues.append(f"replay_next_session_invalid_rows={replay_session_issues}")
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "counts": {
            "order_rows": int(len(order_rows)),
            "invalid_session_rows": int(invalid_session_rows),
            "invalid_time_in_force_rows": int(invalid_tif_rows),
            "replay_rows": int(replay_session_rows),
            "replay_next_session_invalid_rows": int(replay_session_issues),
        },
        "allowed_sessions": sorted(allowed_sessions),
        "allowed_time_in_force": sorted(x for x in allowed_tif if x),
        "issues": issues,
    }
def _build_order_validation_report(
    runtime_ymd: str,
    order_pre_validation_summary: Dict[str, Any],
    order_lifecycle_summary: Dict[str, Any],
    order_amend_cancel_summary: Dict[str, Any],
    order_chain_ssot_summary: Dict[str, Any],
    order_recovery_replay_summary: Dict[str, Any],
    order_session_rule_summary: Dict[str, Any],
) -> Dict[str, Any]:
    statuses = {
        "pre_validation": str(order_pre_validation_summary.get("status") or ""),
        "lifecycle": str(order_lifecycle_summary.get("status") or ""),
        "amend_cancel": str(order_amend_cancel_summary.get("status") or ""),
        "chain_ssot": str(order_chain_ssot_summary.get("status") or ""),
        "recovery_replay": str(order_recovery_replay_summary.get("status") or ""),
        "session_rule": str(order_session_rule_summary.get("status") or ""),
    }
    issues: List[str] = []
    for key, summary in (
        ("pre_validation", order_pre_validation_summary),
        ("lifecycle", order_lifecycle_summary),
        ("amend_cancel", order_amend_cancel_summary),
        ("chain_ssot", order_chain_ssot_summary),
        ("recovery_replay", order_recovery_replay_summary),
        ("session_rule", order_session_rule_summary),
    ):
        for issue in list(summary.get("issues") or []):
            issues.append(f"{key}:{issue}")
    has_fail = any(v == "FAIL" for v in statuses.values())
    has_warn = any(v == "WARN" for v in statuses.values())
    status = "FAIL" if has_fail else ("WARN" if has_warn else "PASS")
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "status": status,
        "component_status": statuses,
        "issues": issues,
        "pre_validation": order_pre_validation_summary,
        "lifecycle": order_lifecycle_summary,
        "amend_cancel": order_amend_cancel_summary,
        "chain_ssot": order_chain_ssot_summary,
        "recovery_replay": order_recovery_replay_summary,
        "session_rule": order_session_rule_summary,
    }
def _build_sell_adapter_summary(schema: str, runtime_ymd: str) -> Dict[str, Any]:
    adapter = PaperSellOrderAdapter(schema=schema, runtime_ymd=runtime_ymd)
    artifacts = _collect_persisted_today_sell_artifacts(schema=schema, runtime_ymd=runtime_ymd)
    sell_fills = list(artifacts.get("sell_fills") or [])
    rows: List[Dict[str, Any]] = []
    state_counts: Dict[str, int] = {}
    issues: List[str] = []

    for fill in sell_fills:
        req = SellOrderRequest(
            runtime_ymd=str(_norm_ymd_text(runtime_ymd) or ""),
            order_id=str(fill.get("order_id") or "").strip(),
            code=str(fill.get("code") or "").strip(),
            sell_qty=int(_ddm_to_float(fill.get("sell_qty"), 0)),
            exit_reason=str(fill.get("exit_reason") or "").strip().upper(),
            source_order_id=str(fill.get("source_order_id") or "").strip(),
            lineage_origin=str(fill.get("lineage_origin") or "").strip().upper(),
            replay_chain_id=str(fill.get("replay_chain_id") or "").strip(),
        )
        status_view = adapter.submit_sell_order(req)
        state_counts[status_view.state] = int(state_counts.get(status_view.state, 0)) + 1
        row_issues: List[str] = []
        if not status_view.trade_chain_matched:
            row_issues.append("trade_chain_unmatched")
            issues.append(f"adapter_trade_chain_unmatched:{status_view.order_id}")
        rows.append(
            {
                "order_id": status_view.order_id,
                "code": status_view.code,
                "state": status_view.state,
                "sell_qty": int(status_view.sell_qty),
                "exit_reason": status_view.exit_reason,
                "source_order_id": status_view.source_order_id,
                "lineage_origin": status_view.lineage_origin,
                "replay_chain_id": status_view.replay_chain_id,
                "trade_chain_matched": bool(status_view.trade_chain_matched),
                "issues": row_issues,
            }
        )

    reconcile = adapter.reconcile_positions()
    status = "PASS" if not issues else "FAIL"
    return {
        "generated_at": now_ts(),
        "runtime_ymd": str(_norm_ymd_text(runtime_ymd) or ""),
        "adapter": "paper",
        "status": status,
        "methods": [
            "submit_sell_order",
            "query_order_status",
            "query_fills",
            "reconcile_positions",
        ],
        "counts": {
            "sell_order_rows": int(len(rows)),
            "trade_chain_matched_rows": int(sum(1 for row in rows if bool(row.get("trade_chain_matched")))),
            "trade_chain_unmatched_rows": int(sum(1 for row in rows if not bool(row.get("trade_chain_matched")))),
            "open_positions": int(_ddm_to_float(reconcile.get("open_positions"), 0)),
        },
        "state_counts": state_counts,
        "reconcile_positions": reconcile,
        "rows": rows[:50],
        "issues": sorted(set(issues)),
    }
def _write_replay_consistency_status(
    replay_queue_status: Optional[Dict[str, Any]],
    replay_recovery_summary: Optional[Dict[str, Any]],
    replay_queue_scan: Optional[Dict[str, Any]],
    replay_quarantine: Optional[Dict[str, Any]],
    replay_prune: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    queue_status = replay_queue_status or {}
    recovery = replay_recovery_summary or {}
    scan = replay_queue_scan or {}
    quarantine = replay_quarantine or {}
    prune = replay_prune or {}

    queue_rows = int(queue_status.get("rows_total", queue_status.get("rows", 0)) or 0)
    due_today_rows = int(queue_status.get("due_today_rows", 0) or 0)
    future_rows = int(queue_status.get("future_rows", 0) or 0)
    expired_rows = int(queue_status.get("expired_rows", 0) or 0)
    eligible_rows = int(recovery.get("eligible_rows", 0) or 0)
    manual_rows = int(recovery.get("manual_review_rows", 0) or 0)
    quarantine_rows = int(quarantine.get("rows", 0) or 0)
    prune_removed_rows = int(prune.get("removed_rows", 0) or 0)
    prune_remaining_rows = int(prune.get("remaining_rows", 0) or 0)
    guard_rows = int(recovery.get("queue_guard_rows", 0) or 0)

    issues: List[str] = []
    if queue_rows != (due_today_rows + future_rows + expired_rows):
        issues.append("queue_partition_mismatch")
    if quarantine_rows != manual_rows:
        issues.append("quarantine_manual_mismatch")
    if prune.get("status") == "pruned" and prune_remaining_rows != queue_rows:
        issues.append("prune_remaining_mismatch")
    if guard_rows > manual_rows:
        issues.append("guard_rows_exceed_manual")
    if eligible_rows > queue_rows:
        issues.append("eligible_exceeds_queue")
    if int(scan.get("expired_rows", 0) or 0) < expired_rows:
        issues.append("scan_expired_under_reports_queue")

    payload = {
        "generated_at": now_ts(),
        "as_of": now_ymd(),
        "status": "OK" if not issues else "CAUTION",
        "issues": issues,
        "metrics": {
            "queue_rows": queue_rows,
            "due_today_rows": due_today_rows,
            "future_rows": future_rows,
            "expired_rows": expired_rows,
            "eligible_rows": eligible_rows,
            "manual_review_rows": manual_rows,
            "quarantine_rows": quarantine_rows,
            "prune_removed_rows": prune_removed_rows,
            "prune_remaining_rows": prune_remaining_rows,
            "queue_guard_rows": guard_rows,
        },
    }
    REPLAY_CONSISTENCY_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
def _run_replay_consistency_remediation(consistency_status: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    status_doc = consistency_status or {}
    issues = _get_list(status_doc, "issues")
    payload: Dict[str, Any] = {
        "attempted": False,
        "ok": False,
        "status": "not_needed",
        "trigger_issues": issues[:10],
        "started_at": now_ts(),
        "finished_at": now_ts(),
        "returncode": None,
        "stdout": "",
        "stderr": "",
        "action_hints": [],
        "summary_sync": {},
    }
    remediate_issues = {
        "queue_partition_mismatch",
        "quarantine_manual_mismatch",
        "prune_remaining_mismatch",
        "eligible_exceeds_queue",
        "scan_expired_under_reports_queue",
    }
    if not issues or not any(issue in remediate_issues for issue in issues):
        payload["status_reason"] = "no_remediable_issue"
        return payload

    payload["attempted"] = True
    payload["status"] = "pending"
    payload["status_reason"] = "regen_pending"
    payload["action_hints"] = ["replay queue consistency mismatch로 orders_exec/replay queue를 1회 자동 재생성"]
    try:
        regen = _run_replay_queue_regeneration("replay_consistency_mismatch")
        payload["ok"] = bool(regen.get("ok"))
        payload["status"] = "ok" if payload["ok"] else str(regen.get("status") or "failed")
        payload["returncode"] = regen.get("returncode")
        payload["stdout"] = str(regen.get("stdout") or "")[-4000:]
        payload["stderr"] = str(regen.get("stderr") or "")[-4000:]
        hints = _get_list(regen, "action_hints")
        payload["action_hints"] = (payload["action_hints"] + [str(x) for x in hints if str(x).strip()])[:5]
        regen_status = str(regen.get("status") or "").strip()
        if payload["ok"]:
            payload["status_reason"] = "regen_succeeded"
        elif regen_status == "skipped_preflight":
            payload["status_reason"] = f"regen_preflight_blocked:{str(regen.get('skip_reason') or '').strip() or 'unknown'}"
        elif regen_status == "script_missing":
            payload["status_reason"] = "regen_script_missing"
        elif regen_status == "exception":
            payload["status_reason"] = "regen_exception"
        else:
            payload["status_reason"] = f"regen_failed:{regen_status or 'unknown'}"
    except Exception as e:
        payload["status"] = "exception"
        payload["status_reason"] = "remediation_exception"
        payload["stderr"] = f"{type(e).__name__}: {e}"
    payload["finished_at"] = now_ts()
    payload["action_hints"] = _replay_consistency_remediation_hints(
        str(payload.get("status_reason") or ""),
        _get_list(payload, "post_check_issues") or issues,
    )
    return payload
def _replay_consistency_remediation_hints(status_reason: str, issues: List[str]) -> List[str]:
    reason = str(status_reason or "").strip()
    issue_set = {str(x).strip() for x in (issues or []) if str(x).strip()}
    hints: List[str] = []
    if reason == "no_remediable_issue":
        return hints
    if reason.startswith("regen_preflight_blocked:live_fills_missing"):
        hints.append("vibe_paper_daily.ps1 또는 vibe_broker_daily.ps1를 먼저 실행해 live_fills.csv를 생성")
    elif reason.startswith("regen_preflight_blocked:live_fills_empty"):
        hints.append("live_fills.csv가 비어 있으므로 체결 수집과 당일 fill 적재 경로를 점검")
    elif reason.startswith("regen_preflight_blocked:eval_template_missing"):
        hints.append("orders_{YYYYMMDD}_eval.xlsx 생성 경로를 점검하고 기준일 eval 템플릿을 준비")
    elif reason == "regen_script_missing":
        hints.append("make_orders_exec_from_fills.py 경로와 RootB 배선을 복구")
    elif reason == "regen_exception":
        hints.append("python 실행 경로와 RootB 작업 디렉터리 권한/경로를 점검")
    elif reason.startswith("regen_failed:"):
        hints.append("make_orders_exec_from_fills.py stderr와 입력 파일 스키마를 확인")
    elif reason == "post_check_still_mismatch":
        hints.append("자동 재생성 후에도 mismatch가 남아 replay summary와 quarantine/prune 카운트를 교차 점검")
    elif reason == "post_check_cleared":
        hints.append("자동 보정 후 일관성 이슈가 해소되어 추가 조치 없이 진행 가능")

    if "queue_partition_mismatch" in issue_set:
        hints.append("replay_orders_latest.json의 rows/due_today/future/expired 집계를 다시 생성")
    if "quarantine_manual_mismatch" in issue_set:
        hints.append("replay_queue_quarantine_latest.csv와 manual_review_rows 수치를 비교해 격리 누락 여부를 점검")
    if "prune_remaining_mismatch" in issue_set:
        hints.append("replay_orders_latest.csv 백업본과 현재 파일의 행 수를 비교해 prune 반영 여부를 확인")
    if "eligible_exceeds_queue" in issue_set:
        hints.append("eligible_rows 계산이 현재 replay queue rows를 넘지 않는지 replay_filter 분기를 재점검")
    if "scan_expired_under_reports_queue" in issue_set:
        hints.append("replay_queue_scan의 expired_rows 계산과 replay_queue_status의 expired_rows 기준일을 맞춤")
    deduped: List[str] = []
    for hint in hints:
        if hint and hint not in deduped:
            deduped.append(hint)
    return deduped[:6]
def _refresh_replay_consistency(
    *,
    ops_enabled: bool,
    replay_queue_status: Dict[str, Any],
    replay_recovery_summary: Dict[str, Any],
    replay_queue_scan: Dict[str, Any],
    replay_quarantine_status: Dict[str, Any],
    replay_prune_status: Dict[str, Any],
    replay_consistency_status: Dict[str, Any],
    replay_consistency_remediation: Dict[str, Any],
    carry_max_age: int,
    recovered_open_pos: List[Dict[str, Any]],
    open_order_replay_used_count: int,
    recovery_status_doc: Dict[str, Any],
) -> Dict[str, Any]:
    if ops_enabled:
        replay_summary_sync_status = _sync_rootb_replay_summary(replay_queue_status)
        replay_consistency_status = _write_replay_consistency_status(
            replay_queue_status=replay_queue_status,
            replay_recovery_summary=replay_recovery_summary,
            replay_queue_scan=replay_queue_scan,
            replay_quarantine=replay_quarantine_status,
            replay_prune=replay_prune_status,
        )
        replay_consistency_remediation = _run_replay_consistency_remediation(replay_consistency_status)
        if replay_consistency_remediation.get("attempted") and replay_consistency_remediation.get("ok"):
            reconciled = _reconcile_replay_runtime_artifacts(
                max_age_days=max(1, carry_max_age),
                open_positions=recovered_open_pos,
                replay_used_today=open_order_replay_used_count,
            )
            replay_queue_scan = reconciled.get("replay_queue_scan") or replay_queue_scan
            replay_recovery_summary = reconciled.get("replay_recovery_summary") or replay_recovery_summary
            replay_quarantine_status = reconciled.get("replay_quarantine") or replay_quarantine_status
            replay_prune_status = reconciled.get("replay_prune") or replay_prune_status
            replay_queue_status = reconciled.get("replay_queue_status") or replay_queue_status
            replay_consistency_status = reconciled.get("replay_consistency") or replay_consistency_status
            replay_consistency_remediation["summary_sync"] = reconciled.get("summary_sync") or {}
            replay_consistency_remediation["post_check_status"] = replay_consistency_status.get("status")
            replay_consistency_remediation["post_check_issues"] = replay_consistency_status.get("issues") or []
            replay_consistency_remediation["post_check_metrics"] = replay_consistency_status.get("metrics") or {}
            if str(replay_consistency_status.get("status") or "").strip() == "OK":
                replay_consistency_remediation["status_reason"] = "post_check_cleared"
            else:
                replay_consistency_remediation["status_reason"] = "post_check_still_mismatch"
                replay_consistency_remediation["secondary_reconcile"] = {
                    "queue_guard_reason": str(replay_recovery_summary.get("queue_guard_reason") or "").strip(),
                    "manual_review_rows": int(replay_recovery_summary.get("manual_review_rows", 0) or 0),
                    "eligible_rows": int(replay_recovery_summary.get("eligible_rows", 0) or 0),
                    "quarantine_rows": int(replay_quarantine_status.get("rows", 0) or 0),
                }
                replay_consistency_remediation["action_hints"] = _replay_consistency_remediation_hints(
                    str(replay_consistency_remediation.get("status_reason") or ""),
                    _get_list(replay_consistency_status, "issues"),
                )
        recovery_status_doc["replay_filter"] = replay_recovery_summary
        recovery_status_doc["replay_quarantine"] = replay_quarantine_status
        recovery_status_doc["replay_prune"] = replay_prune_status
        recovery_status_doc["replay_queue_scan"] = replay_queue_scan
        recovery_status_doc["replay_consistency"] = replay_consistency_status
        recovery_status_doc["replay_consistency_remediation"] = replay_consistency_remediation
        recovery_status_doc["replay_summary_sync"] = replay_summary_sync_status
        RECOVERY_STATUS_PATH.write_text(json.dumps(recovery_status_doc, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        replay_summary_sync_status = {}

    return {
        "replay_summary_sync_status": replay_summary_sync_status,
        "replay_consistency_status": replay_consistency_status,
        "replay_consistency_remediation": replay_consistency_remediation,
        "replay_recovery_summary": replay_recovery_summary,
        "replay_queue_scan": replay_queue_scan,
        "replay_quarantine_status": replay_quarantine_status,
        "replay_prune_status": replay_prune_status,
        "replay_queue_status": replay_queue_status,
        "recovery_status_doc": recovery_status_doc,
    }
def _build_ops_alert_and_carryover(
    *,
    ops_enabled: bool,
    ops_policy: Dict[str, Any],
    pending_carry_rows: List[Dict[str, Any]],
    carry_max_age: int,
    px: pd.DataFrame,
    market_regime: str,
    regime_info: Dict[str, Any],
    config: Dict[str, Any],
    max_new: int,
    max_new_zero_reason: str,
    candidate_count: int,
    price_universe_codes: int,
    evaluated_count: int,
    entry_ready_count: int,
    new_count: int,
    filled_count: int,
    no_next_day_count: int,
    cap_block_count: int,
    processed_skip_count: int,
    max_new_skip_count: int,
    idempotent_skip_count: int,
    stale_replay_used_count: int,
    open_order_replay_used_count: int,
    entry_decisions: List[Dict[str, Any]],
    entry_candidate_df: Optional[pd.DataFrame] = None,
    universe_shrink_candidates: bool,
    open_slot_count: int,
    max_positions: int,
    max_positions_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _has_split_second_carry = any(
        bool(row.get("split_entry_2nd"))
        or str(row.get("carry_origin_reason", "") or "").upper() == "SPLIT_ENTRY_2ND"
        for row in (pending_carry_rows or [])
        if isinstance(row, dict)
    )
    _has_entry_recheck_carry = any(
        str(row.get("carry_origin_reason", "") or "").upper() == "CLOSE_CUTOFF_RECHECK"
        for row in (pending_carry_rows or [])
        if isinstance(row, dict)
    )
    _existing_split_second_pending = False
    _existing_entry_recheck_pending = False
    if ops_enabled and PENDING_SIGNALS_PATH.exists():
        try:
            _existing_pending = _load_pending_signals(max_age_days=carry_max_age)
            if len(_existing_pending) > 0:
                if "split_entry_2nd" in _existing_pending.columns:
                    _existing_split_second_pending = bool(
                        _existing_pending["split_entry_2nd"].astype(str).str.strip().str.lower().isin({"1", "true", "t", "y", "yes"}).any()
                    )
                if (not _existing_split_second_pending) and "carry_origin_reason" in _existing_pending.columns:
                    _existing_split_second_pending = bool(
                        _existing_pending["carry_origin_reason"].astype(str).str.strip().str.upper().eq("SPLIT_ENTRY_2ND").any()
                    )
                if "carry_origin_reason" in _existing_pending.columns:
                    _existing_entry_recheck_pending = bool(
                        _existing_pending["carry_origin_reason"].astype(str).str.strip().str.upper().eq("CLOSE_CUTOFF_RECHECK").any()
                    )
        except Exception:
            _existing_split_second_pending = False
            _existing_entry_recheck_pending = False
    if ops_enabled and (
        bool(ops_policy.get("carryover_no_next_day_enabled", True))
        or _has_split_second_carry
        or _existing_split_second_pending
        or _has_entry_recheck_carry
        or _existing_entry_recheck_pending
    ):
        _save_pending_signals(pending_carry_rows, max_age_days=carry_max_age, px=px)
        if pending_carry_rows:
            print(f"[OPS] carryover saved: {len(pending_carry_rows)} -> {PENDING_SIGNALS_PATH.name}")

    # Daily fill summary log + FAIL-CLOSED unfilled ratio check
    _strict = bool(ops_enabled and ops_policy.get("strict_same_day_only", False))
    _unfilled = max(0, int(entry_ready_count) - int(filled_count))
    _unfilled_ratio = _unfilled / int(entry_ready_count) if int(entry_ready_count) > 0 else 0.0
    _fc_threshold = float(ops_policy.get("fail_closed_unfilled_ratio_threshold", 0.40) if isinstance(ops_policy, dict) else 0.40)
    print(
        f"[FILL_SUMMARY] attempted={entry_ready_count} filled={filled_count} "
        f"unfilled_expired={_unfilled} unfilled_ratio={_unfilled_ratio:.2f} "
        f"drop_stale_signal={'yes' if _strict else 'no(legacy)'} "
        f"pending_carry={len(pending_carry_rows)}"
    )
    if _strict and int(entry_ready_count) > 0 and _unfilled_ratio > _fc_threshold:
        print(
            f"[FAIL_CLOSED_ALERT] unfilled_ratio={_unfilled_ratio:.2f} > threshold={_fc_threshold:.2f} "
            f"attempted={entry_ready_count} filled={filled_count} → 신규주문 자동 축소 권고"
        )

    slo_target = compute_participation_slo(market_regime, regime_info, config)
    expected_min_fills = int(math.ceil(entry_ready_count * slo_target)) if entry_ready_count > 0 else 0
    entry_decision_reason_counts: Dict[str, int] = {}
    news_implication_watch_codes: List[str] = []
    news_implication_reduce_codes: List[str] = []
    if isinstance(entry_candidate_df, pd.DataFrame) and not entry_candidate_df.empty:
        for _, pool_row in entry_candidate_df.iterrows():
            pool_code = norm_code(pool_row.get("code", ""))
            if not pool_code:
                continue
            if float(_to_float(pool_row.get("news_implication_watch_rows"), 0.0) or 0.0) > 0.0:
                news_implication_watch_codes.append(pool_code)
            if float(_to_float(pool_row.get("news_implication_reduce_size_rows"), 0.0) or 0.0) > 0.0:
                news_implication_reduce_codes.append(pool_code)
    for row in (entry_decisions or []):
        if not isinstance(row, dict):
            continue
        row_code = norm_code(row.get("code", ""))
        if row_code:
            if float(_to_float(row.get("news_implication_watch_rows"), 0.0) or 0.0) > 0.0:
                news_implication_watch_codes.append(row_code)
            if float(_to_float(row.get("news_implication_reduce_size_rows"), 0.0) or 0.0) > 0.0:
                news_implication_reduce_codes.append(row_code)
        reason_key = str(row.get("reason") or "").strip().upper()
        if not reason_key:
            continue
        entry_decision_reason_counts[reason_key] = int(entry_decision_reason_counts.get(reason_key, 0)) + 1
    no_fill_reasons: List[str] = []
    if int(filled_count) <= 0:
        if int(max_new) <= 0:
            no_fill_reasons.append("MAX_NEW_ZERO")
        if str(max_new_zero_reason or "").strip().lower() == "max_positions_full":
            no_fill_reasons.append("MAX_POSITIONS_FULL")
        if int(max_new_skip_count) > 0:
            no_fill_reasons.append("MAX_NEW_REACHED")
        if int(entry_ready_count) <= 0 and int(candidate_count) > 0:
            no_fill_reasons.append("NO_ENTRY_READY")
        if int(cap_block_count) > 0:
            no_fill_reasons.append("CAP_BLOCK")
        if int(processed_skip_count) > 0:
            no_fill_reasons.append("PROCESSED_SKIP")
        if int(idempotent_skip_count) > 0:
            no_fill_reasons.append("IDEMPOTENT_DUPLICATE")
        if int(no_next_day_count) > 0:
            no_fill_reasons.append("NO_NEXT_DAY")
        if int(open_order_replay_used_count) > 0:
            no_fill_reasons.append("OPEN_ORDER_REPLAY_USED")
        if entry_decision_reason_counts:
            no_fill_reasons.extend(
                [
                    f"DECISION_{k}"
                    for k, _ in sorted(entry_decision_reason_counts.items(), key=lambda item: (-int(item[1]), item[0]))[:3]
                ]
            )
        if not no_fill_reasons:
            no_fill_reasons.append("UNCLASSIFIED")
    ops_alert = {
        "generated_at": now_ts(),
        "market_regime": market_regime,
        "day_ret": regime_info.get("day_ret"),
        "max_new": int(max_new),
        "max_new_zero_reason": str(max_new_zero_reason or ""),
        "candidates_after_caps": int(candidate_count),
        "price_universe_codes": int(price_universe_codes),
        "evaluated": int(evaluated_count),
        "entry_ready": int(entry_ready_count),
        "open_slot_count": int(open_slot_count),
        "max_positions": int(max_positions),
        "max_positions_meta": max_positions_meta or {},
        "filled": int(filled_count),
        "no_next_day": int(no_next_day_count),
        "cap_block": int(cap_block_count),
        "processed_skip": int(processed_skip_count),
        "max_new_skip": int(max_new_skip_count),
        "idempotent_skip": int(idempotent_skip_count),
        "stale_replay_used": int(stale_replay_used_count),
        "open_order_replay_used": int(open_order_replay_used_count),
        "slo_target": float(slo_target),
        "expected_min_fills": int(expected_min_fills),
        "slo_pass": bool(int(filled_count) >= expected_min_fills),
        "universe_shrink_candidates": bool(universe_shrink_candidates),
        "no_fill_reasons": no_fill_reasons,
        "entry_decision_reason_counts": entry_decision_reason_counts,
        "news_implication_watch_count": int(len(set(news_implication_watch_codes))),
        "news_implication_watch_codes": sorted(set(news_implication_watch_codes)),
        "news_implication_reduce_size_count": int(len(set(news_implication_reduce_codes))),
        "news_implication_reduce_size_codes": sorted(set(news_implication_reduce_codes)),
    }
    return {
        "slo_target": float(slo_target),
        "expected_min_fills": int(expected_min_fills),
        "ops_alert": ops_alert,
    }
def _persist_state_and_runtime_status(
    *,
    state: Dict[str, Any],
    still_open: List[Dict[str, Any]],
    next_seq: int,
    processed_signals: Set[str],
    carry_max_age: int,
    ops_enabled: bool,
    schema: str,
    fills_new: List[Any],
    trades_new: List[Any],
    initial_open_count: int,
    replay_recovery_summary: Dict[str, Any],
    replay_due_today_count: int,
    recovery_summary: Dict[str, Any],
    replay_queue_status: Dict[str, Any],
    replay_consistency_status: Dict[str, Any],
    ddm_enabled: bool,
    ddm_action: Any,
    ddm_force_liquidate_pct: float,
    ddm_liquidation_targets: Set[str],
    open_pos: List[Dict[str, Any]],
    stop_loss: float,
    take_profit: Any,
    trail_pct: Any,
    recovery_status_doc: Dict[str, Any],
    market_regime: str,
    max_new: int,
    max_new_surge: int,
    max_new_zero_reason: str,
    candidate_df: pd.DataFrame,
    entry_decisions: Optional[List[Dict[str, Any]]],
    surge_inject_status: Optional[Dict[str, Any]],
    entry_ready_count: int,
    no_next_day_count: int,
    open_order_replay_used_count: int,
    replay_regen_result: Dict[str, Any],
    replay_queue_scan: Dict[str, Any],
    replay_quarantine_status: Dict[str, Any],
    replay_prune_status: Dict[str, Any],
    replay_consistency_remediation: Dict[str, Any],
    replay_summary_sync_status: Dict[str, Any],
    carryover_revalidate_summary: Dict[str, Any],
    runtime_ymd: str,
    ops_alert: Dict[str, Any],
    capital_total: float,
    current_open_notional: float,
    gross_cap_krw: Optional[float],
    daily_new_cap_krw: Optional[float],
    max_positions_meta: Optional[Dict[str, Any]],
) -> None:
    cdf = candidate_df
    state["open_positions"] = still_open
    state["next_trade_seq"] = next_seq

    # Persist processed_signals in sorted order.
    ps = sorted(processed_signals)
    MAX_PS = 20000
    if len(ps) > MAX_PS:
        ps = ps[-MAX_PS:]
    state["processed_signals"] = ps

    save_state(state)

    pending_signal_rows_snapshot = len(_load_pending_signals(max_age_days=carry_max_age)) if ops_enabled else 0
    persisted_today_metrics = _collect_persisted_today_runtime_metrics(
        schema=schema,
        runtime_ymd=runtime_ymd,
    )
    entry_fill_rows = int(persisted_today_metrics.get("entry_fill_rows", 0))
    exit_fill_rows = int(persisted_today_metrics.get("exit_fill_rows", 0))
    closed_trade_rows = int(persisted_today_metrics.get("closed_trade_rows", 0))
    before_multiset: Dict[Tuple[str, str], int] = {}
    for pos in (open_pos or []):
        if not isinstance(pos, dict):
            continue
        code = str(pos.get("code") or "").strip().zfill(6)
        entry_date = str(pos.get("entry_date") or "")
        key = (code, entry_date)
        before_multiset[key] = int(before_multiset.get(key, 0)) + 1
    after_multiset: Dict[Tuple[str, str], int] = {}
    for pos in (still_open or []):
        if not isinstance(pos, dict):
            continue
        code = str(pos.get("code") or "").strip().zfill(6)
        entry_date = str(pos.get("entry_date") or "")
        key = (code, entry_date)
        after_multiset[key] = int(after_multiset.get(key, 0)) + 1
    opened_position_rows = 0
    for key, after_cnt in after_multiset.items():
        before_cnt = int(before_multiset.get(key, 0))
        if after_cnt > before_cnt:
            opened_position_rows += int(after_cnt - before_cnt)
    closed_position_rows = 0
    for key, before_cnt in before_multiset.items():
        after_cnt = int(after_multiset.get(key, 0))
        if before_cnt > after_cnt:
            closed_position_rows += int(before_cnt - after_cnt)
    entry_exit_lifecycle = _build_entry_exit_lifecycle(
        open_positions_before=initial_open_count,
        open_positions_after=len(still_open),
        opened_position_rows=opened_position_rows,
        closed_position_rows=closed_position_rows,
        entry_fill_rows=entry_fill_rows,
        exit_fill_rows=exit_fill_rows,
        closed_trade_rows=closed_trade_rows,
        pending_signal_rows=pending_signal_rows_snapshot,
        replay_due_today=int(replay_recovery_summary.get("eligible_rows", replay_due_today_count)),
        replay_manual_review_rows=int(replay_recovery_summary.get("manual_review_rows", 0)),
        recovery_manual_review_positions=int(recovery_summary.get("manual_review_positions", 0)),
        replay_discard_rows=int(replay_recovery_summary.get("discard_rows", 0)),
    )
    state_machine_summary = _build_state_machine_summary(
        entry_exit_lifecycle=entry_exit_lifecycle,
        replay_recovery_summary=replay_recovery_summary,
        replay_queue_status=replay_queue_status,
        replay_consistency=replay_consistency_status,
        recovery_summary=recovery_summary,
    )
    order_artifacts = _collect_persisted_today_order_artifacts(
        schema=schema,
        runtime_ymd=runtime_ymd,
    )
    order_pre_validation_summary = _build_order_pre_validation_summary(
        runtime_ymd=runtime_ymd,
        ops_alert=ops_alert,
        max_new=int(max_new),
        candidates_after_caps=int(len(cdf)),
        entry_ready=int(entry_ready_count),
        filled_count=int(entry_fill_rows),
        capital_total=float(capital_total),
        current_open_notional=float(current_open_notional),
        gross_cap_krw=gross_cap_krw,
        daily_new_cap_krw=daily_new_cap_krw,
        max_positions_meta=max_positions_meta,
    )
    order_lifecycle_summary = _build_order_lifecycle_summary(
        runtime_ymd=runtime_ymd,
        order_artifacts=order_artifacts,
        entry_exit_lifecycle=entry_exit_lifecycle,
    )
    order_amend_cancel_summary = _build_order_amend_cancel_summary(
        runtime_ymd=runtime_ymd,
        max_age_days=int(carry_max_age),
    )
    order_chain_ssot_summary = _build_order_chain_ssot_summary(
        runtime_ymd=runtime_ymd,
        order_lifecycle_summary=order_lifecycle_summary,
        persisted_today_metrics=persisted_today_metrics,
        entry_exit_lifecycle=entry_exit_lifecycle,
    )
    order_recovery_replay_summary = _build_order_recovery_replay_summary(
        runtime_ymd=runtime_ymd,
        replay_recovery_summary=replay_recovery_summary,
        replay_queue_status=replay_queue_status,
        order_artifacts=order_artifacts,
    )
    order_session_rule_summary = _build_order_session_rule_summary(
        runtime_ymd=runtime_ymd,
        order_artifacts=order_artifacts,
        max_age_days=int(carry_max_age),
    )
    sell_order_lifecycle_summary = _build_sell_order_lifecycle_summary(
        schema=schema,
        runtime_ymd=runtime_ymd,
    )
    partial_exit_policy_summary = _build_partial_exit_policy_summary(
        schema=schema,
        runtime_ymd=runtime_ymd,
    )
    sell_recovery_chain_summary = _build_sell_recovery_chain_summary(
        schema=schema,
        runtime_ymd=runtime_ymd,
    )
    sell_adapter_summary = _build_sell_adapter_summary(
        schema=schema,
        runtime_ymd=runtime_ymd,
    )
    symbol_stop_summary = _build_symbol_stop_summary(
        stop_loss=stop_loss,
        take_profit=take_profit,
        trail_pct=trail_pct,
        fills_new=fills_new,
        trades_new=trades_new,
        persisted_stop_sell_rows=int(persisted_today_metrics.get("stop_sell_rows", 0)),
        persisted_stop_trade_rows=int(persisted_today_metrics.get("stop_trade_rows", 0)),
        persisted_stop_reasons=list(persisted_today_metrics.get("stop_reasons", [])),
    )
    sell_validation_report = _build_sell_validation_report(
        runtime_ymd=runtime_ymd,
        persisted_today_metrics=persisted_today_metrics,
        sell_order_lifecycle_summary=sell_order_lifecycle_summary,
        entry_exit_lifecycle=entry_exit_lifecycle,
        partial_exit_policy_summary=partial_exit_policy_summary,
        sell_recovery_chain_summary=sell_recovery_chain_summary,
        symbol_stop_summary=symbol_stop_summary,
    )
    order_validation_report = _build_order_validation_report(
        runtime_ymd=runtime_ymd,
        order_pre_validation_summary=order_pre_validation_summary,
        order_lifecycle_summary=order_lifecycle_summary,
        order_amend_cancel_summary=order_amend_cancel_summary,
        order_chain_ssot_summary=order_chain_ssot_summary,
        order_recovery_replay_summary=order_recovery_replay_summary,
        order_session_rule_summary=order_session_rule_summary,
    )
    legacy_derisk_summary = _build_legacy_derisk_summary(
        schema=schema,
        runtime_ymd=runtime_ymd,
        ddm_enabled=ddm_enabled,
        ddm_action=ddm_action,
        ddm_force_liquidate_pct=ddm_force_liquidate_pct,
        ddm_liquidation_targets=ddm_liquidation_targets,
        open_positions=open_pos,
        still_open_positions=still_open,
        fills_new=fills_new,
    )

    if ops_enabled:
        carryover_revalidate_summary = dict(carryover_revalidate_summary or {})
        recovery_status_doc["entry_exit_lifecycle"] = entry_exit_lifecycle
        recovery_status_doc["state_machine_summary"] = state_machine_summary
        recovery_status_doc["order_pre_validation_summary"] = order_pre_validation_summary
        recovery_status_doc["order_lifecycle_summary"] = order_lifecycle_summary
        recovery_status_doc["order_amend_cancel_summary"] = order_amend_cancel_summary
        recovery_status_doc["order_chain_ssot_summary"] = order_chain_ssot_summary
        recovery_status_doc["order_recovery_replay_summary"] = order_recovery_replay_summary
        recovery_status_doc["order_session_rule_summary"] = order_session_rule_summary
        recovery_status_doc["order_validation_report"] = order_validation_report
        recovery_status_doc["sell_order_lifecycle_summary"] = sell_order_lifecycle_summary
        recovery_status_doc["partial_exit_policy_summary"] = partial_exit_policy_summary
        recovery_status_doc["sell_recovery_chain_summary"] = sell_recovery_chain_summary
        recovery_status_doc["sell_adapter_summary"] = sell_adapter_summary
        recovery_status_doc["sell_validation_report"] = sell_validation_report
        recovery_status_doc["symbol_stop_summary"] = symbol_stop_summary
        recovery_status_doc["legacy_derisk_summary"] = legacy_derisk_summary
        RECOVERY_STATUS_PATH.write_text(json.dumps(recovery_status_doc, ensure_ascii=False, indent=2), encoding="utf-8")
        SELL_VALIDATION_REPORT_PATH.write_text(json.dumps(sell_validation_report, ensure_ascii=False, indent=2), encoding="utf-8")
        ORDER_VALIDATION_REPORT_PATH.write_text(json.dumps(order_validation_report, ensure_ascii=False, indent=2), encoding="utf-8")
        _write_pending_status(
            market_regime=market_regime,
            max_new=max_new,
            max_new_surge=max_new_surge,
            max_new_zero_reason=max_new_zero_reason,
            candidates_after_caps=len(cdf),
            entry_ready=entry_ready_count,
            filled=entry_fill_rows,
            no_next_day=no_next_day_count,
            replay_due_today=int(replay_recovery_summary.get("eligible_rows", replay_due_today_count)),
            replay_future_queue_len=int(replay_queue_status.get("future_rows", 0)),
            replay_expired_rows=int(replay_queue_status.get("expired_rows", 0)),
            replay_used_today=open_order_replay_used_count,
            replay_discard_rows=int(replay_recovery_summary.get("discard_rows", 0)),
            replay_manual_review_rows=int(replay_recovery_summary.get("manual_review_rows", 0)),
            recovery_auto_recovered_positions=int(recovery_summary.get("auto_recovered_positions", 0)),
            recovery_manual_review_positions=int(recovery_summary.get("manual_review_positions", 0)),
            replay_regeneration_reason=str(replay_regen_result.get("trigger_reason") or ""),
            replay_queue_scan=replay_queue_scan,
            replay_quarantine=replay_quarantine_status,
            replay_prune=replay_prune_status,
            replay_consistency=replay_consistency_status,
            replay_consistency_remediation=replay_consistency_remediation,
            replay_summary_sync=replay_summary_sync_status,
            entry_exit_lifecycle=entry_exit_lifecycle,
            state_machine_summary=state_machine_summary,
            sell_order_lifecycle_summary=sell_order_lifecycle_summary,
            partial_exit_policy_summary=partial_exit_policy_summary,
            sell_recovery_chain_summary=sell_recovery_chain_summary,
            sell_adapter_summary=sell_adapter_summary,
            sell_validation_report=sell_validation_report,
            order_pre_validation_summary=order_pre_validation_summary,
            order_lifecycle_summary=order_lifecycle_summary,
            order_amend_cancel_summary=order_amend_cancel_summary,
            order_chain_ssot_summary=order_chain_ssot_summary,
            order_recovery_replay_summary=order_recovery_replay_summary,
            order_session_rule_summary=order_session_rule_summary,
            order_validation_report=order_validation_report,
            symbol_stop_summary=symbol_stop_summary,
            legacy_derisk_summary=legacy_derisk_summary,
            carryover_revalidate_summary=carryover_revalidate_summary,
            surge_inject_status=surge_inject_status,
            max_positions_meta=max_positions_meta,
            entry_decision_rows=(entry_decisions if isinstance(entry_decisions, list) else []),
            max_age_days=carry_max_age,
        )


def read_latest_stable_params() -> Dict[str, Any]:
    p = latest_file(RISK_DIR, "stable_params_v*.json")  # utils.common helper
    if not p:
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _stable_params_usable(stable: Dict[str, Any], cfg: Optional[Dict[str, Any]] = None) -> tuple[bool, str]:
    if not isinstance(stable, dict) or not stable:
        return False, "missing"

    gate = _get_dict(cfg or {}, "stable_params_quality_gate")
    require_promoted = bool(gate.get("require_promoted", True))
    min_oos_trades = _to_int(gate.get("min_oos_trades"), 20)
    min_stable_score = _to_float(gate.get("min_stable_score"), 0.0)
    min_oos_pf = _to_float(gate.get("min_oos_pf"), 0.75)

    promoted = bool(stable.get("promoted", False))
    if require_promoted and not promoted:
        return False, "not_promoted"

    stable_score = _to_float(stable.get("best_score"), -1e18)
    if stable_score < float(min_stable_score):
        return False, f"stable_score_low({stable_score:.4f}<{float(min_stable_score):.4f})"

    windows = _get_list(stable, "windows")
    oos_n_total = 0
    oos_pf_weighted_num = 0.0
    oos_pf_weighted_den = 0
    for row in windows:
        if not isinstance(row, dict):
            continue
        split = str(row.get("split", "")).upper()
        n_trades = _to_int(row.get("n_trades"), 0)
        if split == "OOS" and n_trades > 0:
            oos_n_total += int(n_trades)
            pf = _to_float(row.get("pf"), 0.0)
            oos_pf_weighted_num += float(pf) * int(n_trades)
            oos_pf_weighted_den += int(n_trades)

    if oos_n_total < int(min_oos_trades):
        return False, f"oos_trades_low({oos_n_total}<{int(min_oos_trades)})"
    if oos_pf_weighted_den <= 0:
        return False, "oos_trades_missing"
    oos_pf_weighted = float(oos_pf_weighted_num) / float(oos_pf_weighted_den)
    if oos_pf_weighted < float(min_oos_pf):
        return False, f"oos_pf_low({oos_pf_weighted:.4f}<{float(min_oos_pf):.4f})"
    return True, "ok"

def _max_date8_from_candidates(x: pd.DataFrame) -> str:
    if "date_yyyymmdd" in x.columns:
        ss = x["date_yyyymmdd"].astype(str)
    elif "date" in x.columns:
        ss = x["date"].astype(str).str.replace("-", "", regex=False).str[:8]
    elif "signal_date" in x.columns:
        ss = x["signal_date"].astype(str)
    else:
        return ""
    ss = ss.str.replace(r"[^0-9]", "", regex=True).str[:8]
    ss = ss[ss.str.len() == 8]
    return str(ss.max()) if len(ss) else ""
