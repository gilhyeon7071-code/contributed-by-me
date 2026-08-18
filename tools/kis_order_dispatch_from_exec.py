from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kis_order_client import KISApiError, KISOrderClient
from order_scoring_engine import (
    OrderScoreConfig,
    OrderScoreFeature,
    decide_action,
    score_order,
)
from utils.gate_audit import log_gate_event
from utils.pipeline_audit import log_pipeline_event, count_rows


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
RUNS_DIR = ROOT.parent / "vibe" / "buffett" / "runs"
DEFAULT_HOLIDAYS_PATH = ROOT / "holidays.json"
DEFAULT_ORDERFLOW_OBSERVER_PATH = LOG_DIR / "orderflow_observer_state_latest.json"
DEFAULT_PRODUCTION_RISK_PLAYBOOK_PATH = LOG_DIR / "production_risk_playbook_latest.json"
P0_DAILY_CHECK_REPORT_RE = re.compile(r"^p0_daily_check_\d{8}_\d{6}\.json$")
logger = logging.getLogger("kis_order_dispatch_from_exec")


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


def _read_json(path: Path) -> Dict[str, object]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _load_orderflow_guard(path: Path) -> Dict[str, object]:
    payload = _read_json(path)
    if not payload:
        return {
            "enabled": True,
            "blocked": False,
            "reason": "observer_missing_or_unreadable",
            "observer_path": str(path),
        }
    max_observer_age_sec = max(1.0, _env_float("ORDERFLOW_OBSERVER_MAX_AGE_SEC", 300.0))
    try:
        observer_age_sec = max(0.0, time.time() - path.stat().st_mtime)
    except Exception:
        observer_age_sec = max_observer_age_sec + 1.0
    source_fresh = bool(payload.get("source_fresh", False)) and observer_age_sec <= max_observer_age_sec
    fail_closed_candidate = bool(payload.get("fail_closed_candidate", False))
    blocked = bool(source_fresh and fail_closed_candidate)
    alerts = payload.get("alerts") if isinstance(payload.get("alerts"), list) else []
    alert_codes = sorted(
        {
            str(row.get("code", "")).strip().zfill(6)
            for row in alerts
            if isinstance(row, dict) and str(row.get("code", "")).strip()
        }
    )
    return {
        "enabled": True,
        "blocked": blocked,
        "reason": str(payload.get("reason") or ("orderflow_fail_closed_candidate" if blocked else "no_orderflow_block")),
        "observer_path": str(path),
        "status": str(payload.get("status") or ""),
        "source_fresh": source_fresh,
        "observer_age_sec": round(float(observer_age_sec), 3),
        "max_observer_age_sec": round(float(max_observer_age_sec), 3),
        "fail_closed_candidate": fail_closed_candidate,
        "trade_event_count": payload.get("trade_event_count"),
        "alert_count": payload.get("alert_count"),
        "alert_codes": alert_codes,
    }


def _orderflow_guard_blocks_code(orderflow_guard: Dict[str, object], code: str) -> bool:
    if not bool(orderflow_guard.get("blocked")):
        return False
    alert_codes = orderflow_guard.get("alert_codes")
    if isinstance(alert_codes, list) and alert_codes:
        return str(code).strip().zfill(6) in {str(c).strip().zfill(6) for c in alert_codes}
    return True


def _load_production_risk_guard(path: Path) -> Dict[str, object]:
    payload = _read_json(path)
    if not payload:
        return {
            "enabled": True,
            "blocked": False,
            "action": "NORMAL",
            "reason": "playbook_missing_or_unreadable",
            "playbook_path": str(path),
            "source_fresh": False,
        }
    action = str(payload.get("action") or "NORMAL").upper()
    source_fresh = bool(payload.get("source_fresh", False))
    blocked = bool(source_fresh and action == "HARD")
    return {
        "enabled": True,
        "blocked": blocked,
        "action": action,
        "reason": str(payload.get("reason") or ("production_risk_hard_pause" if blocked else "no_production_risk_block")),
        "playbook_path": str(path),
        "source_fresh": source_fresh,
    }


def _latest_p0_daily_check_report() -> Optional[Path]:
    files = [
        p
        for p in LOG_DIR.glob("p0_daily_check_*.json")
        if P0_DAILY_CHECK_REPORT_RE.match(p.name)
    ]
    files.sort(key=lambda p: p.name, reverse=True)
    return files[0] if files else None


def _pick_float(*values: object) -> Optional[float]:
    for value in values:
        if value is None:
            continue
        try:
            text = str(value).replace(",", "").strip()
            if not text:
                continue
            return float(text)
        except Exception:
            continue
    return None


def _bool_from_obj(value: object, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "1.0", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "0.0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _load_risk_gate_guard() -> Dict[str, object]:
    p0_path = _latest_p0_daily_check_report()
    if p0_path is None:
        return {
            "enabled": True,
            "blocked": True,
            "reason": "p0_daily_check_missing",
            "p0_path": "",
            "basis": "missing",
        }

    p0 = _read_json(p0_path)
    ks = p0.get("kill_switch") if isinstance(p0.get("kill_switch"), dict) else {}
    ro = p0.get("risk_off") if isinstance(p0.get("risk_off"), dict) else {}
    metrics = ks.get("metrics") if isinstance(ks.get("metrics"), dict) else {}
    limits = ks.get("limits") if isinstance(ks.get("limits"), dict) else {}
    hard_metrics = metrics.get("hard_trigger_metrics") if isinstance(metrics.get("hard_trigger_metrics"), dict) else {}
    account_basis = metrics.get("account_basis") if isinstance(metrics.get("account_basis"), dict) else {}
    account_ok = str(account_basis.get("status") or "").upper() == "PASS"

    hard_max_dd = _pick_float(
        hard_metrics.get("max_drawdown_pct"),
        account_basis.get("max_drawdown_pct") if account_ok else None,
        metrics.get("max_drawdown_pct"),
    )
    hard_day_loss = _pick_float(
        hard_metrics.get("daily_loss_pct"),
        hard_metrics.get("last_day_ret"),
        account_basis.get("daily_loss_pct") if account_ok else None,
        account_basis.get("last_day_ret") if account_ok else None,
        metrics.get("last_day_ret"),
    )
    dd_limit = abs(_pick_float(limits.get("max_drawdown_pct")) or 0.0)
    day_limit = abs(_pick_float(limits.get("max_daily_loss_pct")) or 0.0)
    daily_active = _bool_from_obj(metrics.get("hard_daily_loss_active", metrics.get("daily_loss_active")), True)
    expected_dd = bool(hard_max_dd is not None and dd_limit > 0.0 and hard_max_dd <= -dd_limit)
    expected_day = bool(daily_active and hard_day_loss is not None and day_limit > 0.0 and hard_day_loss <= -day_limit)
    risk_off_enabled = bool(ro.get("enabled", False))
    risk_reasons = [str(x) for x in (ro.get("reasons") or [])]
    kill_triggered = bool(ks.get("triggered", False))

    reasons: List[str] = []
    if risk_off_enabled:
        reasons.append("risk_off_enabled")
    if kill_triggered:
        reasons.append("kill_switch_triggered")
    if expected_dd:
        reasons.append("expected_max_dd_trigger")
    if expected_day:
        reasons.append("expected_daily_loss_trigger")

    return {
        "enabled": True,
        "blocked": bool(reasons),
        "reason": ";".join(reasons) if reasons else "no_risk_gate_block",
        "p0_path": str(p0_path),
        "as_of_ymd": str(p0.get("as_of_ymd") or ""),
        "risk_off_enabled": risk_off_enabled,
        "risk_off_reasons": risk_reasons,
        "kill_switch_triggered": kill_triggered,
        "kill_switch_reasons": [str(x) for x in (ks.get("reasons") or [])],
        "basis": str(metrics.get("hard_trigger_basis") or account_basis.get("basis") or "fallback"),
        "hard_max_drawdown_pct": hard_max_dd,
        "hard_daily_loss_pct": hard_day_loss,
        "hard_daily_loss_active": daily_active,
        "max_drawdown_limit": dd_limit,
        "daily_loss_limit": day_limit,
        "expected_max_dd_trigger": expected_dd,
        "expected_daily_loss_trigger": expected_day,
        "account_basis_status": str(account_basis.get("status") or ""),
        "strategy_max_drawdown_pct": (
            (metrics.get("strategy_basis") or {}).get("max_drawdown_pct")
            if isinstance(metrics.get("strategy_basis"), dict)
            else metrics.get("max_drawdown_pct")
        ),
    }


def _api_error_fields(e: Exception) -> Dict[str, object]:
    return {
        "error_type": type(e).__name__,
        "error_code": str(getattr(e, "code", "") or ""),
        "error_category": str(getattr(e, "category", "") or ""),
        "status_code": getattr(e, "status_code", None),
        "path": str(getattr(e, "path", "") or ""),
        "message": str(e),
    }


def _norm_ymd(v: object) -> str:
    s = str(v or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8]


def _to_bool(v: object) -> bool:
    s = str(v or "").strip().lower()
    return s in {"1", "true", "y", "yes", "t"}



def _to_int(v: object, default: int = 0) -> int:
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return int(default)


def _to_float(v: object, default: float = 0.0) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return float(default)


def _extract_first_float(d: Dict[str, object], keys: List[str], default: float = 0.0) -> float:
    for k in keys:
        if k in d:
            val = _to_float(d.get(k), default)
            if val != default:
                return val
    return float(default)


def _hoga_price_keys(prefix: str, level: int) -> List[str]:
    if prefix == "ask":
        return [f"askp{level}", f"ask{level}", f"ask_price{level}", f"askp_rsqn{level}_price"]
    return [f"bidp{level}", f"bid{level}", f"bid_price{level}", f"bidp_rsqn{level}_price"]


def _hoga_qty_keys(prefix: str, level: int) -> List[str]:
    if prefix == "ask":
        return [f"askp_rsqn{level}", f"askq{level}", f"ask_q{level}", f"ask_qty{level}"]
    return [f"bidp_rsqn{level}", f"bidq{level}", f"bid_q{level}", f"bid_qty{level}"]


def _hoga_level(output: Dict[str, object], level: int) -> Tuple[float, float, float, float]:
    ask = _extract_first_float(output, _hoga_price_keys("ask", level), 0.0)
    bid = _extract_first_float(output, _hoga_price_keys("bid", level), 0.0)
    ask_qty = _extract_first_float(output, _hoga_qty_keys("ask", level), 0.0)
    bid_qty = _extract_first_float(output, _hoga_qty_keys("bid", level), 0.0)
    return float(ask), float(bid), float(ask_qty), float(bid_qty)


def _extract_buy_lob_features(
    output: Dict[str, object],
    *,
    qty: int,
    order_type: str,
    limit_price: int,
    max_depth_levels: int,
) -> Dict[str, object]:
    depth_levels = max(1, min(10, int(max_depth_levels or 1)))
    ask1, bid1, askq1, bidq1 = _hoga_level(output, 1)
    if ask1 <= 0.0 or bid1 <= 0.0:
        return {
            "status": "NO_LOB",
            "reason": "missing ask1/bid1",
            "ask1": ask1,
            "bid1": bid1,
            "askq1": askq1,
            "bidq1": bidq1,
            "spread_bps": 0.0,
            "imbalance": 0.0,
            "executable_qty": 0,
            "executable_price": 0.0,
            "ask_depth_levels": 0,
        }

    mid = (ask1 + bid1) / 2.0
    spread_bps = max(0.0, ((ask1 - bid1) / mid) * 10000.0) if mid > 0 else 0.0
    den = askq1 + bidq1
    imbalance = ((bidq1 - askq1) / den) if den > 0 else 0.0

    requested_qty = max(0, int(qty or 0))
    executable_qty = 0
    notional = 0.0
    ask_depth_levels = 0
    order_type_l = str(order_type or "").strip().lower()
    limit_px = max(0, int(limit_price or 0))
    for level in range(1, depth_levels + 1):
        ask, _bid, ask_qty, _bid_qty = _hoga_level(output, level)
        if ask <= 0.0 or ask_qty <= 0.0:
            continue
        ask_depth_levels += 1
        if order_type_l == "limit" and limit_px > 0 and ask > float(limit_px):
            continue
        take_qty = int(max(0.0, ask_qty))
        if requested_qty > 0:
            take_qty = min(take_qty, max(0, requested_qty - executable_qty))
        executable_qty += int(take_qty)
        notional += float(take_qty) * float(ask)
        if requested_qty > 0 and executable_qty >= requested_qty:
            break

    executable_price = (notional / executable_qty) if executable_qty > 0 else 0.0
    return {
        "status": "OK" if executable_qty > 0 else "NO_EXECUTABLE_ASK",
        "reason": "ok" if executable_qty > 0 else "no ask quantity executable at order price",
        "ask1": ask1,
        "bid1": bid1,
        "askq1": askq1,
        "bidq1": bidq1,
        "spread_bps": float(spread_bps),
        "imbalance": float(imbalance),
        "executable_qty": int(executable_qty),
        "executable_price": float(executable_price),
        "ask_depth_levels": int(ask_depth_levels),
    }


def _append_precheck_msg(rec: Dict[str, object], msg: str) -> None:
    old = str(rec.get("precheck_msg", "") or "").strip()
    text = str(msg or "").strip()
    if not text:
        return
    rec["precheck_msg"] = f"{old};{text}" if old else text


def _parse_prefixes(raw: str) -> List[str]:
    vals: List[str] = []
    for x in str(raw or "").split(","):
        t = str(x).strip().upper()
        if t:
            vals.append(t)
    return vals


def _detect_latest_orders_path() -> Optional[Path]:
    cand = sorted(PAPER_DIR.glob("orders_*_exec.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    return cand[0] if cand else None


def _load_orders(
    path: Path,
    d: str,
    *,
    include_blocked: bool = False,
    allow_block_prefixes: Optional[List[str]] = None,
    default_order_type: str = "limit",
) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str)

    need = ["exec_date", "side", "code", "fill_qty"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise ValueError(f"orders_exec missing cols={miss} path={path}")

    out = df.copy()
    out["_source_row"] = out.index.astype(int)
    out["exec_date"] = out["exec_date"].apply(_norm_ymd)
    out["side"] = out["side"].astype(str).str.strip().str.upper()
    out["code"] = out["code"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    out["qty"] = out["fill_qty"].apply(_to_int)
    if "fill_price" in out.columns:
        out["price"] = out["fill_price"].apply(_to_int)
    else:
        out["price"] = 0
    if "order_type" in out.columns:
        out["order_type"] = out["order_type"].astype(str).str.strip().str.lower()
    else:
        out["order_type"] = str(default_order_type).lower()
    if "signal_date" in out.columns:
        out["signal_date"] = out["signal_date"].apply(_norm_ymd)
    else:
        out["signal_date"] = ""
    if "is_stop" in out.columns:
        out["is_stop"] = out["is_stop"].apply(_to_bool)
    else:
        out["is_stop"] = False
    if "entry_blocked" in out.columns:
        out["entry_blocked"] = out["entry_blocked"].apply(_to_bool)
    else:
        out["entry_blocked"] = False
    if "entry_block_reason" in out.columns:
        out["entry_block_reason"] = out["entry_block_reason"].astype(str).fillna("")
    else:
        out["entry_block_reason"] = ""
    if "note" in out.columns:
        out["note"] = out["note"].astype(str)
    else:
        out["note"] = ""

    base_mask = (
        (out["exec_date"] == str(d))
        & (out["side"].isin(["BUY", "SELL"]))
        & (out["qty"] > 0)
    )

    if include_blocked:
        prefixes = [p for p in (allow_block_prefixes or []) if str(p).strip()]
        if len(prefixes) == 0:
            allow_mask = pd.Series([True] * len(out), index=out.index)
        else:
            up = out["entry_block_reason"].astype(str).str.upper()
            allow_blocked = out["entry_blocked"] & up.apply(lambda s: any(str(s).startswith(p) for p in prefixes))
            allow_mask = (~out["entry_blocked"]) | allow_blocked
        keep = out[base_mask & allow_mask].copy()
    else:
        keep = out[base_mask & (~out["entry_blocked"])].copy()

    if len(keep) == 0:
        return keep

    keep = keep.reset_index(drop=True)
    return keep


def _load_slicing_preview(path: Path, expected_d: str = "") -> tuple[str, pd.DataFrame]:
    if not path.exists():
        raise FileNotFoundError(f"slicing preview not found: {path}")
    df = pd.read_csv(path, dtype=str).fillna("")
    required = [
        "D",
        "mode",
        "code",
        "side",
        "slice_qty",
        "order_price",
        "slice_index",
        "slice_count",
        "broker_submit_allowed",
        "kis_api_call",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"slicing preview missing cols={missing} path={path}")
    if df.empty:
        raise ValueError(f"slicing preview has no rows: {path}")

    work = df.copy()
    work["D"] = work["D"].apply(_norm_ymd)
    ds = sorted({str(x) for x in work["D"].tolist() if str(x)})
    if len(ds) != 1:
        raise ValueError(f"slicing preview must contain exactly one D: {ds}")
    d = ds[0]
    if expected_d and str(expected_d) != d:
        raise ValueError(f"slicing preview D mismatch: expected={expected_d} preview={d}")

    mode_ok = work["mode"].astype(str).str.upper().eq("DRY_RUN_PREVIEW")
    broker_ok = work["broker_submit_allowed"].astype(str).str.lower().isin(["false", "0", ""])
    kis_ok = work["kis_api_call"].astype(str).str.lower().isin(["false", "0", ""])
    if not bool(mode_ok.all()):
        raise ValueError("slicing preview contains non DRY_RUN_PREVIEW rows")
    if not bool(broker_ok.all()):
        raise ValueError("slicing preview broker_submit_allowed must be false")
    if not bool(kis_ok.all()):
        raise ValueError("slicing preview kis_api_call must be false")

    work["code"] = work["code"].astype(str).str.replace(".0", "", regex=False).str.strip().str.zfill(6)
    work["side"] = work["side"].astype(str).str.upper().str.strip()
    work["qty"] = work["slice_qty"].apply(_to_int)
    work["price"] = work["order_price"].apply(_to_int)
    bad = work[
        (~work["code"].str.match(r"^\d{6}$", na=False))
        | (~work["side"].isin(["BUY", "SELL"]))
        | (work["qty"] <= 0)
        | (work["price"] <= 0)
    ]
    if len(bad) > 0:
        raise ValueError(f"slicing preview has invalid dispatch rows count={len(bad)}")
    return d, work.reset_index(drop=True)


def _run_slicing_preview_dispatch(args: argparse.Namespace) -> int:
    if bool(args.apply):
        logger.error("[STOP] --slicing-preview cannot be used with --apply")
        return 2

    expected_d = _norm_ymd(args.date)
    preview_path = Path(str(args.slicing_preview))
    try:
        d, preview = _load_slicing_preview(preview_path, expected_d=expected_d)
    except Exception as exc:
        logger.error("[STOP] load_slicing_preview failed: %s", exc)
        return 2

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    now_ts = dt.datetime.now().isoformat(timespec="seconds")
    out_csv = PAPER_DIR / f"orders_{d}_broker_submit_slicing_preview.csv"
    out_json = LOG_DIR / f"kis_order_dispatch_{d}_slicing_preview.json"
    latest_json = LOG_DIR / "kis_order_dispatch_slicing_preview_latest.json"
    records: List[Dict[str, object]] = []

    for idx, row in preview.iterrows():
        note = (
            f"slicing_preview=1;decision_id={row.get('decision_id', '')};"
            f"slice_index={row.get('slice_index', '')};slice_count={row.get('slice_count', '')};"
            f"source_orders_exec={row.get('source_orders_exec', '')};source_row={row.get('source_row', '')}"
        )
        source_row = (_to_int(row.get("source_row"), idx) * 1000) + _to_int(row.get("slice_index"), 1)
        records.append(
            {
                "dispatch_ts": now_ts,
                "exec_date": d,
                "side": str(row.get("side", "")).upper(),
                "code": str(row.get("code", "")).zfill(6),
                "qty": int(row.get("qty", 0)),
                "price": int(row.get("price", 0)),
                "signal_date": "",
                "is_stop": False,
                "note": note,
                "entry_blocked": False,
                "entry_block_reason": "",
                "source_row": int(source_row),
                "dispatch_status": "DRY_RUN_SLICE_PREVIEW",
                "ok": True,
                "rt_cd": "",
                "msg1": "slicing preview dry-run only",
                "ord_no": "",
                "org_no": "",
                "tr_id": "",
                "error": "",
                "error_code": "",
                "error_category": "",
                "error_status_code": "",
                "error_path": "",
                "apply": False,
                "order_type": str(rec.get("order_type") or args.order_type),
                "mode": "slicing_preview",
                "precheck": "SKIP",
                "precheck_msg": "no broker call; preview only",
                "score_policy": False,
                "venue_type": str(args.venue_type),
                "order_score": "",
                "p_fill": "",
                "signed_markout": "",
                "estimated_tc": "",
                "score_action": "",
                "imbalance": "",
                "spread_bps": "",
                "orderflow_guard": False,
                "orderflow_guard_blocked": False,
                "orderflow_guard_reason": "",
                "entry_order_id": "",
                "slice_index": _to_int(row.get("slice_index"), 0),
                "slice_count": _to_int(row.get("slice_count"), 0),
                "decision_id": str(row.get("decision_id", "")),
                "kis_api_call": False,
            }
        )

    out_df = pd.DataFrame(records)
    out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    summary = {
        "generated_at": now_ts,
        "D": d,
        "slicing_preview_path": str(preview_path),
        "submit_log": str(out_csv),
        "apply": False,
        "mode": "slicing_preview",
        "rows_preview": int(len(preview)),
        "rows_new": int(len(out_df)),
        "counts": out_df.get("dispatch_status", pd.Series(dtype=str)).value_counts(dropna=False).to_dict(),
        "kis_api_calls": 0,
        "broker_submit_allowed": False,
        "policy_note": "dry-run slicing preview dispatch only; no KIS API, fills, ledger, stats, Gate, or LOCK mutation",
    }
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[OK] slicing_preview_submit_log=%s rows_new=%s", out_csv, len(out_df))
    logger.info("[OK] slicing_preview_summary=%s", out_json)
    return 0


def _dispatch_key(rec: Dict[str, object]) -> str:
    return "|".join(
        [
            str(rec.get("exec_date", "")),
            str(rec.get("side", "")),
            str(rec.get("code", "")),
            str(rec.get("qty", "")),
            str(rec.get("price", "")),
            str(rec.get("signal_date", "")),
            str(rec.get("source_row", "")),
        ]
    )


def _load_done_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        old = pd.read_csv(path, dtype=str)
    except Exception:
        return set()

    if len(old) == 0:
        return set()

    ok_mask = old.get("dispatch_status", "").astype(str).isin(
        [
            "ACCEPTED",
            "UNKNOWN_PENDING",
            "PENDING_BROKER_QUERY_FAIL",
            "PENDING_BROKER_QUERY_NOT_FOUND",
        ]
    )
    keys = set()
    for _, r in old[ok_mask].iterrows():
        keys.add(
            _dispatch_key(
                {
                    "exec_date": r.get("exec_date", ""),
                    "side": r.get("side", ""),
                    "code": r.get("code", ""),
                    "qty": r.get("qty", ""),
                    "price": r.get("price", ""),
                    "signal_date": r.get("signal_date", ""),
                    "source_row": r.get("source_row", ""),
                }
            )
        )
    return keys


def _load_paper_filled_buy_order_ids(path: Path, d: str) -> set[str]:
    if not path.exists():
        return set()
    try:
        old = pd.read_csv(path, dtype=str)
    except Exception:
        return set()
    if len(old) == 0:
        return set()
    required = {"side", "order_id"}
    if not required.issubset(set(old.columns)):
        return set()
    date_col = "date" if "date" in old.columns else ("datetime" if "datetime" in old.columns else "")
    if not date_col:
        return set()
    work = old.copy()
    work["_ymd"] = work[date_col].apply(_norm_ymd)
    buy_mask = work["side"].astype(str).str.strip().str.upper().eq("BUY")
    d_mask = work["_ymd"].astype(str).eq(str(d))
    return {
        str(x).strip()
        for x in work.loc[buy_mask & d_mask, "order_id"].astype(str).tolist()
        if str(x).strip()
    }


def _row_dispatch_key(row: pd.Series) -> str:
    return _dispatch_key(
        {
            "exec_date": row.get("exec_date", ""),
            "side": row.get("side", ""),
            "code": str(row.get("code", "")).replace(".0", "").strip().zfill(6),
            "qty": row.get("qty", ""),
            "price": row.get("price", ""),
            "signal_date": row.get("signal_date", ""),
            "source_row": row.get("source_row", ""),
        }
    )


def _load_unresolved_pending_rows(path: Path, d: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        old = pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame()
    if len(old) == 0 or "dispatch_status" not in old.columns:
        return pd.DataFrame()

    work = old.copy()
    work["_dispatch_key"] = work.apply(_row_dispatch_key, axis=1)
    work["dispatch_status"] = work["dispatch_status"].astype(str).str.strip().str.upper()
    work["exec_date"] = work.get("exec_date", "").apply(_norm_ymd)

    latest_by_key: Dict[str, pd.Series] = {}
    for _, row in work.iterrows():
        latest_by_key[str(row.get("_dispatch_key", ""))] = row

    pending_rows = []
    emitted_keys: set[str] = set()
    for _, row in work.iterrows():
        k = str(row.get("_dispatch_key", ""))
        if k in emitted_keys:
            continue
        latest = latest_by_key.get(k)
        if latest is None:
            continue
        if str(latest.get("dispatch_status", "")) != "UNKNOWN_PENDING":
            continue
        if str(latest.get("exec_date", "")) != str(d):
            continue
        pending_rows.append(latest.drop(labels=["_dispatch_key"], errors="ignore").to_dict())
        emitted_keys.add(k)
    return pd.DataFrame(pending_rows)


def _normalize_broker_side(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if s in {"BUY", "B", "02"} or "매수" in s:
        return "BUY"
    if s in {"SELL", "S", "01"} or "매도" in s:
        return "SELL"
    return ""


def _broker_row_order_no(row: Dict[str, object]) -> str:
    return str(row.get("odno", "") or row.get("ord_no", "") or row.get("order_no", "") or row.get("ODNO", "")).strip()


def _broker_row_org_no(row: Dict[str, object]) -> str:
    return str(
        row.get("ord_gno_brno", "")
        or row.get("order_branch_no", "")
        or row.get("orgn_odno", "")
        or row.get("KRX_FWDG_ORD_ORGNO", "")
    ).strip()


def _broker_row_code(row: Dict[str, object]) -> str:
    return str(row.get("pdno", "") or row.get("code", "") or row.get("PDNO", "")).strip().zfill(6)


def _broker_row_qty(row: Dict[str, object]) -> int:
    return _extract_first_int(row, ["ord_qty", "tot_ord_qty", "qty", "ORD_QTY"], default=-1)


def _broker_row_side(row: Dict[str, object]) -> str:
    return _normalize_broker_side(row.get("sll_buy_dvsn_cd", "") or row.get("side", "") or row.get("SLL_BUY_DVSN_CD", ""))


def _find_broker_order_for_pending(
    client: KISOrderClient,
    rec: Dict[str, object],
    max_pages: int,
) -> Optional[Dict[str, object]]:
    code = str(rec.get("code", "")).replace(".0", "").strip().zfill(6)
    side = str(rec.get("side", "")).strip().upper()
    qty = _to_int(rec.get("qty", 0), default=0)
    d = _norm_ymd(rec.get("exec_date", ""))
    rsp = client.inquire_daily_ccld(
        start_ymd=d,
        end_ymd=d,
        sll_buy_dvsn_cd="00",
        ccld_dvsn="00",
        inqr_dvsn="00",
        inqr_dvsn_3="00",
        pdno=code,
        max_pages=max_pages,
    )
    rows = rsp.get("rows", []) or []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ord_no = _broker_row_order_no(row)
        if not ord_no:
            continue
        if _broker_row_code(row) != code:
            continue
        broker_side = _broker_row_side(row)
        if broker_side and side and broker_side != side:
            continue
        broker_qty = _broker_row_qty(row)
        if qty > 0 and broker_qty > 0 and broker_qty != qty:
            continue
        return row
    return None


def _resolve_unknown_pending_records(
    pending_df: pd.DataFrame,
    client: KISOrderClient,
    now_ts: str,
    max_pages: int,
) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for _, row in pending_df.iterrows():
        rec = {str(k): row.get(k, "") for k in pending_df.columns}
        rec["dispatch_ts"] = now_ts
        rec["ok"] = False
        rec["broker_reconcile_status"] = "QUERY"
        rec["broker_reconcile_msg"] = ""
        rec["broker_reconcile_rows"] = ""
        rec["broker_reconcile_ord_no"] = ""
        rec["broker_reconcile_org_no"] = ""
        try:
            match = _find_broker_order_for_pending(client, rec, max_pages=max_pages)
        except Exception as e:
            rec["dispatch_status"] = "PENDING_BROKER_QUERY_FAIL"
            rec["precheck"] = "FAIL"
            rec["error"] = str(e)
            rec["broker_reconcile_status"] = "QUERY_FAIL"
            rec["broker_reconcile_msg"] = str(e)
            records.append(rec)
            continue

        if match:
            ord_no = _broker_row_order_no(match)
            org_no = _broker_row_org_no(match)
            rec["dispatch_status"] = "ACCEPTED"
            rec["ok"] = True
            rec["ord_no"] = ord_no
            rec["org_no"] = org_no
            rec["msg1"] = "resolved from broker inquiry after UNKNOWN_PENDING"
            rec["broker_reconcile_status"] = "FOUND"
            rec["broker_reconcile_msg"] = "broker order matched pending dispatch"
            rec["broker_reconcile_ord_no"] = ord_no
            rec["broker_reconcile_org_no"] = org_no
        else:
            rec["dispatch_status"] = "PENDING_BROKER_QUERY_NOT_FOUND"
            rec["precheck"] = "FAIL"
            rec["broker_reconcile_status"] = "NOT_FOUND"
            rec["broker_reconcile_msg"] = "broker order not found for pending dispatch; fail closed"
        records.append(rec)
    return records


def _env_mock_value() -> Optional[bool]:
    raw = str(os.getenv("KIS_MOCK", "")).strip().lower()
    if raw in {"1", "true", "y", "yes"}:
        return True
    if raw in {"0", "false", "n", "no"}:
        return False
    return None


def _resolve_mock_arg(args_mock: str, *, default_auto_mock: bool = True) -> bool:
    if args_mock == "true":
        return True
    if args_mock == "false":
        return False
    env_mock = _env_mock_value()
    return bool(default_auto_mock if env_mock is None else env_mock)


def _env_mock_bool() -> bool:
    return bool(_resolve_mock_arg("auto", default_auto_mock=True))


def _mode_label(args_mock: str) -> str:
    return "mock" if _resolve_mock_arg(args_mock, default_auto_mock=True) else "prod"


def _extract_first_int(d: Dict[str, object], keys: List[str], default: int = -1) -> int:
    for k in keys:
        if k in d:
            v = _to_int(d.get(k), default=-1)
            if v >= 0:
                return v
    return int(default)


def _map_sellable_qty(balance_rows: List[Dict[str, object]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in balance_rows:
        code = str(r.get("pdno", "") or r.get("code", "") or "").strip().zfill(6)
        if not code.isdigit() or len(code) != 6:
            continue
        qty = _extract_first_int(
            r,
            [
                "ord_psbl_qty",
                "ord_psbl_qty1",
                "ord_psbl_qty_1",
                "sell_psbl_qty",
                "hldg_qty",
                "hold_qty",
                "qty",
            ],
            default=-1,
        )
        if qty >= 0:
            out[code] = max(out.get(code, 0), int(qty))
    return out


def _buy_possible_qty(client: KISOrderClient, code: str, order_type: str, price: int) -> int:
    ord_dvsn = "01" if str(order_type).lower() == "market" else "00"
    rsp = client.inquire_psbl_order(code=code, order_price=max(0, int(price)), ord_dvsn=ord_dvsn)
    output = rsp.get("output", {}) or {}
    qty = _extract_first_int(
        output,
        [
            "nrcvb_buy_qty",
            "max_buy_qty",
            "ord_psbl_qty",
            "psbl_qty_calc",
            "buy_psbl_qty",
        ],
        default=-1,
    )
    return int(qty)


def _mask_account(cano: str, prdt: str) -> str:
    c = str(cano or "")
    p = str(prdt or "")
    if len(c) >= 4:
        c = ("*" * (len(c) - 4)) + c[-4:]
    return f"{c}-{p}"


def _extract_note_field(note: str, key: str) -> str:
    raw = str(note or "")
    needle = f"{str(key).strip()}="
    for token in raw.split(";"):
        t = str(token).strip()
        if t.startswith(needle):
            return t[len(needle):].strip()
    return ""


def _source_entry_block_from_note(note: str) -> Dict[str, object]:
    blocked_text = _extract_note_field(note, "source_entry_blocked")
    decision = _extract_note_field(note, "source_entry_decision").strip().upper()
    reason = _extract_note_field(note, "source_entry_reason")
    exclude_reasons = _extract_note_field(note, "source_exclude_reasons")
    is_blocked = _to_bool(blocked_text) or decision == "ENTRY_BLOCKED"
    msg_parts = []
    if decision:
        msg_parts.append(f"source_entry_decision={decision}")
    if blocked_text:
        msg_parts.append(f"source_entry_blocked={blocked_text}")
    if reason:
        msg_parts.append(f"source_entry_reason={reason}")
    if exclude_reasons:
        msg_parts.append(f"source_exclude_reasons={exclude_reasons}")
    return {
        "blocked": bool(is_blocked),
        "decision": decision,
        "blocked_text": blocked_text,
        "reason": reason,
        "exclude_reasons": exclude_reasons,
        "message": ";".join(msg_parts),
    }


def _is_rate_limit_error(err_text: str) -> bool:
    s = str(err_text or "")
    return (
        ("EGW00201" in s)
        or ("초당 거래건수를 초과" in s)
        or ("rate limit" in s.lower())
        or ("too many requests" in s.lower())
    )


def _load_holidays_ymd(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()

    vals: List[str] = []
    if isinstance(obj, dict):
        for k in ["holidays", "krx_holidays", "dates", "holiday_dates"]:
            v = obj.get(k)
            if isinstance(v, list):
                vals.extend([str(x) for x in v])
    elif isinstance(obj, list):
        vals.extend([str(x) for x in obj])

    out = set()
    for x in vals:
        y = _norm_ymd(x)
        if len(y) == 8:
            out.add(y)
    return out


def _is_krx_session_open(holidays_ymd: set[str]) -> tuple[bool, str]:
    try:
        from zoneinfo import ZoneInfo

        now = dt.datetime.now(ZoneInfo("Asia/Seoul"))
    except Exception:
        now = dt.datetime.utcnow() + dt.timedelta(hours=9)

    ymd = now.strftime("%Y%m%d")
    w = now.weekday()  # 0=Mon
    if w >= 5:
        return False, f"weekend weekday={w}"
    if ymd in holidays_ymd:
        return False, f"holiday ymd={ymd}"

    hm = now.hour * 100 + now.minute
    if hm < 900 or hm > 1530:
        return False, f"off-hours hm={hm}"

    return True, f"open ymd={ymd} hm={hm}"


def _merge_scoring_metrics(path: Path, section_name: str, payload: Dict[str, object]) -> bool:
    root: Dict[str, object] = {}
    if path.exists():
        try:
            prev = json.loads(path.read_text(encoding="utf-8-sig"))
            if isinstance(prev, dict):
                root = prev
        except Exception:
            root = {}
    root["generated_at"] = dt.datetime.now().isoformat(timespec="seconds")
    root[section_name] = payload
    try:
        dispatch_ok = bool((root.get("dispatch") or {}).get("new_orders_allowed", True)) if isinstance(root.get("dispatch"), dict) else True
        cancel_ok = bool((root.get("cancel") or {}).get("new_orders_allowed", True)) if isinstance(root.get("cancel"), dict) else True
        root["new_orders_allowed"] = bool(dispatch_ok and cancel_ok)
    except Exception:
        root["new_orders_allowed"] = bool(payload.get("new_orders_allowed", True))
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(root, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        return True
    except OSError as exc:
        logger.warning("scoring metrics write skipped path=%s error=%s", path, exc)
        try:
            fallback = LOG_DIR / f"order_scoring_metrics_write_skip_{section_name}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            fallback.write_text(
                json.dumps(
                    {
                        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
                        "status": "WARN",
                        "section": section_name,
                        "target_path": str(path),
                        "error": f"{type(exc).__name__}: {exc}",
                        "payload": payload,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception as fallback_exc:
            logger.warning("scoring metrics fallback write failed error=%s", fallback_exc)
        return False


def _is_auth_error_fields(fields: Dict[str, object]) -> bool:
    cat = str(fields.get("error_category", "") or "").upper()
    code = str(fields.get("error_code", "") or "").upper()
    msg = str(fields.get("message", "") or "").lower()
    return bool(cat == "AUTH" or "token" in msg or "접근토큰" in msg or "approval" in msg or code.startswith("EGW"))


def _is_unknown_pending_fields(fields: Dict[str, object]) -> bool:
    cat = str(fields.get("error_category", "") or "").upper()
    msg = str(fields.get("message", "") or "").lower()
    typ = str(fields.get("error_type", "") or "").lower()
    return bool(cat == "UNKNOWN_PENDING" or "timeout" in msg or "timeout" in typ)


def main() -> int:
    ap = argparse.ArgumentParser(description="Dispatch KIS live orders from orders_{D}_exec.xlsx")
    ap.add_argument("--date", default="", help="YYYYMMDD. empty => infer from latest orders file name")
    ap.add_argument("--orders-path", default="", help="Path to orders_{D}_exec.xlsx")
    ap.add_argument("--slicing-preview", default="", help="Path to execution_slicing_preview CSV; dry-run dispatch log only")
    ap.add_argument("--apply", action="store_true", help="Actually send orders. default is dry-run")
    ap.add_argument("--order-type", default="limit", choices=["market", "limit"])
    ap.add_argument("--max-orders", type=int, default=0, help="0 means all")
    ap.add_argument("--sleep-ms", type=int, default=120)
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--force-resend", action="store_true", help="Ignore existing submit log idempotency")
    ap.add_argument(
        "--no-pending-broker-check",
        dest="pending_broker_check",
        action="store_false",
        default=True,
        help="Disable broker inquiry reconciliation for prior UNKNOWN_PENDING rows",
    )
    ap.add_argument("--pending-query-max-pages", type=int, default=5)
    ap.add_argument("--allow-non-today", action="store_true", help="Allow apply for non-today date")
    ap.add_argument("--allow-offhours", action="store_true", help="Allow apply outside KRX regular session")
    ap.add_argument("--holidays-file", default=str(DEFAULT_HOLIDAYS_PATH))
    ap.add_argument("--strict-precheck", dest="strict_precheck", action="store_true", default=True)
    ap.add_argument("--no-strict-precheck", dest="strict_precheck", action="store_false")
    ap.add_argument("--validation-mode", action="store_true", help="Validation mode: allow selected blocked rows for sample collection")
    ap.add_argument("--allow-block-prefixes", default="CAP_", help="Comma-separated entry_block_reason prefixes allowed in validation mode")
    ap.add_argument("--min-live-orders", type=int, default=0, help="If validation mode and eligible rows below this value, fallback include all blocked rows")
    ap.add_argument("--score-policy", dest="score_policy", action="store_true", default=True, help="Enable score-based dispatch precheck")
    ap.add_argument("--no-score-policy", dest="score_policy", action="store_false", help="Disable score-based dispatch precheck")
    ap.add_argument("--venue-type", default="price_time", choices=["price_time", "retail_pro_rata"])
    ap.add_argument("--fee-bps", type=float, default=2.0)
    ap.add_argument("--slippage-bps", type=float, default=3.0)
    ap.add_argument("--replace-penalty-bps", type=float, default=1.0)
    ap.add_argument("--pfill-cut-price-time", type=float, default=0.03)
    ap.add_argument("--pfill-cut-retail", type=float, default=0.10)
    ap.add_argument(
        "--general-lob-precheck",
        dest="general_lob_precheck",
        action="store_true",
        default=True,
        help="Fail closed on missing/wide/insufficient BUY hoga before broker dispatch",
    )
    ap.add_argument("--no-general-lob-precheck", dest="general_lob_precheck", action="store_false")
    ap.add_argument("--general-max-spread-bps", type=float, default=_env_float("GENERAL_ENTRY_MAX_SPREAD_BPS", 30.0))
    ap.add_argument("--general-min-signed-markout", type=float, default=_env_float("GENERAL_ENTRY_MIN_SIGNED_MARKOUT", 0.0))
    ap.add_argument("--general-lob-depth-levels", type=int, default=10)
    ap.add_argument("--orderflow-guard", dest="orderflow_guard", action="store_true", default=True)
    ap.add_argument("--no-orderflow-guard", dest="orderflow_guard", action="store_false")
    ap.add_argument("--orderflow-observer-path", default=str(DEFAULT_ORDERFLOW_OBSERVER_PATH))
    ap.add_argument("--production-risk-guard", dest="production_risk_guard", action="store_true", default=True)
    ap.add_argument("--no-production-risk-guard", dest="production_risk_guard", action="store_false")
    ap.add_argument("--production-risk-playbook-path", default=str(DEFAULT_PRODUCTION_RISK_PLAYBOOK_PATH))
    ap.add_argument("--risk-gate-guard", dest="risk_gate_guard", action="store_true", default=True)
    ap.add_argument("--no-risk-gate-guard", dest="risk_gate_guard", action="store_false")
    ap.add_argument(
        "--mock-allow-risk-guarded-buy",
        action="store_true",
        help="In mock apply mode only, record risk guard state but allow BUY dispatch for paper data collection",
    )
    args = ap.parse_args()
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

    _audit_ymd = _norm_ymd(args.date) or dt.datetime.now().strftime("%Y%m%d")
    log_pipeline_event(
        stage="broker_dispatch",
        batch_label="[12.5/14]",
        event="START",
        date=_audit_ymd,
        input_files={
            "orders_exec": str(Path(args.orders_path)) if args.orders_path else "",
        },
    )

    if str(args.slicing_preview or "").strip():
        return _run_slicing_preview_dispatch(args)

    if args.validation_mode and args.apply:
        logger.error("[STOP] validation_mode cannot be used with --apply")
        return 2

    if args.validation_mode and args.strict_precheck:
        logger.info("validation_mode: strict_precheck -> False (sample collection priority)")
        args.strict_precheck = False
    if args.validation_mode and int(args.sleep_ms) < 350:
        logger.info("validation_mode: sleep_ms %s -> 350", args.sleep_ms)
        args.sleep_ms = 350


    d = _norm_ymd(args.date)
    orders_path = Path(args.orders_path) if args.orders_path else None
    if orders_path is None:
        if d:
            orders_path = PAPER_DIR / f"orders_{d}_exec.xlsx"
        else:
            orders_path = _detect_latest_orders_path()
            if orders_path is None:
                logger.error("[STOP] no orders_*_exec.xlsx found")
                return 2

    if not orders_path.exists():
        logger.error("[STOP] orders file not found: %s", orders_path)
        return 2

    if not d:
        stem_digits = "".join(ch for ch in orders_path.stem if ch.isdigit())
        d = stem_digits[:8] if len(stem_digits) >= 8 else ""
    if not d:
        logger.error("[STOP] failed to determine date D")
        return 2

    today_ymd = dt.datetime.now().strftime("%Y%m%d")

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        RUNS_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.warning("scoring metrics dir unavailable path=%s error=%s", RUNS_DIR, exc)

    score_cfg = OrderScoreConfig(
        fee_bps=float(args.fee_bps),
        slippage_bps=float(args.slippage_bps),
        replace_penalty_bps=float(args.replace_penalty_bps),
        pfill_cut_price_time=float(args.pfill_cut_price_time),
        pfill_cut_retail_pro_rata=float(args.pfill_cut_retail),
    )

    try:
        prefixes = _parse_prefixes(args.allow_block_prefixes)
        orders = _load_orders(
            orders_path,
            d,
            include_blocked=bool(args.validation_mode),
            allow_block_prefixes=prefixes,
            default_order_type=str(args.order_type),
        )
    except Exception as e:
        logger.error("[STOP] load_orders failed: %s", e)
        return 2

    mock_apply_expected = bool(
        args.apply
        and _resolve_mock_arg(args.mock, default_auto_mock=True)
    )
    if args.max_orders and args.max_orders > 0 and (not mock_apply_expected):
        orders = orders.head(int(args.max_orders)).copy()

    if args.apply and (not args.allow_non_today) and d != today_ymd:
        if len(orders) > 0:
            logger.error(
                "[STOP] apply is blocked for non-today D=%s today=%s. Use --allow-non-today if intentional.",
                d,
                today_ymd,
            )
            return 2
        else:
            logger.info("non-today D with zero eligible rows: skip apply block D=%s today=%s", d, today_ymd)

    # Merge duplicated rows for the same order lineage inside one batch.
    # This prevents PRECHECK_DUPLICATE_IN_BATCH when engine emits split rows
    # with identical code/side/entry_order_id.
    if len(orders):
        work = orders.copy()
        work["_entry_order_id"] = work["note"].astype(str).apply(lambda s: _extract_note_field(s, "entry_order_id"))
        work["_entry_order_id"] = work["_entry_order_id"].fillna("").astype(str).str.strip()
        work["_merge_key"] = (
            work["code"].astype(str)
            + "|"
            + work["side"].astype(str)
            + "|"
            + work["_entry_order_id"]
        )
        dup_before = int(work["_merge_key"].duplicated(keep=False).sum())
        if dup_before > 0:
            grouped = (
                work.sort_values(["_merge_key", "_source_row"])
                .groupby("_merge_key", as_index=False)
                .agg(
                    {
                        "exec_date": "first",
                        "side": "first",
                        "code": "first",
                        "qty": "sum",
                        "price": "last",
                        "order_type": "first",
                        "signal_date": "first",
                        "is_stop": "max",
                        "note": "first",
                        "entry_blocked": "max",
                        "entry_block_reason": "first",
                        "_source_row": "min",
                    }
                )
            )
            logger.info("merge duplicated batch rows: %s -> %s", len(work), len(grouped))
            orders = grouped.drop(columns=["_merge_key"], errors="ignore").reset_index(drop=True)

    if args.validation_mode and args.min_live_orders > 0 and len(orders) < int(args.min_live_orders):
        try:
            orders_fallback = _load_orders(
                orders_path,
                d,
                include_blocked=True,
                allow_block_prefixes=[],
                default_order_type=str(args.order_type),
            )
            if args.max_orders and args.max_orders > 0:
                orders_fallback = orders_fallback.head(int(args.max_orders)).copy()
            if len(orders_fallback) > len(orders):
                logger.warning(
                    "validation fallback enabled: eligible %s -> %s (min_live_orders=%s)",
                    len(orders),
                    len(orders_fallback),
                    args.min_live_orders,
                )
                orders = orders_fallback
        except Exception as e:
            logger.warning("validation fallback failed: %s", e)

    mock_opt: Optional[bool] = _resolve_mock_arg(args.mock, default_auto_mock=True)

    client: Optional[KISOrderClient] = None
    if args.apply:
        try:
            client = KISOrderClient.from_env(mock=mock_opt)
        except Exception as e:
            logger.error("[STOP] KIS env/config failed: %s", e)
            return 2

    run_mode = _mode_label(args.mock)
    if client is not None:
        run_mode = "mock" if client.cfg.mock else "prod"

    if args.apply and client is not None and bool(client.cfg.mock):
        before_rows = len(orders)
        orders = orders[orders["side"].astype(str).str.upper().str.strip() == "BUY"].copy()
        if len(orders) != before_rows:
            logger.info("mock apply BUY-only filter: %s->%s", before_rows, len(orders))
        if args.max_orders and args.max_orders > 0 and len(orders) > int(args.max_orders):
            before_cap = len(orders)
            orders = orders.head(int(args.max_orders)).copy()
            logger.info("mock apply max_orders cap(after BUY filter): %s->%s", before_cap, len(orders))

    out_csv = PAPER_DIR / f"orders_{d}_broker_submit_{run_mode}.csv"
    out_json = LOG_DIR / f"kis_order_dispatch_{d}_{run_mode}.json"
    orderflow_guard = (
        _load_orderflow_guard(Path(args.orderflow_observer_path))
        if bool(args.orderflow_guard)
        else {"enabled": False, "blocked": False, "reason": "disabled"}
    )
    production_risk_guard = (
        _load_production_risk_guard(Path(args.production_risk_playbook_path))
        if bool(args.production_risk_guard)
        else {"enabled": False, "blocked": False, "reason": "disabled", "action": "NORMAL"}
    )
    risk_gate_guard = (
        _load_risk_gate_guard()
        if bool(args.risk_gate_guard)
        else {"enabled": False, "blocked": False, "reason": "disabled"}
    )
    mock_risk_guarded_buy_allowed = bool(args.apply and str(run_mode).lower() == "mock" and args.mock_allow_risk_guarded_buy)

    logger.info("D=%s today=%s orders_path=%s", d, today_ymd, orders_path)
    logger.info("eligible_rows=%s apply=%s order_type=%s mode=%s", len(orders), bool(args.apply), args.order_type, run_mode)
    if bool(orderflow_guard.get("blocked")):
        logger.warning(
            "[ORDERFLOW_GUARD] BUY dispatch blocked reason=%s observer=%s",
            orderflow_guard.get("reason"),
            orderflow_guard.get("observer_path"),
        )
    if bool(production_risk_guard.get("blocked")):
        logger.warning(
            "[PRODUCTION_RISK_GUARD] BUY dispatch blocked action=%s reason=%s playbook=%s",
            production_risk_guard.get("action"),
            production_risk_guard.get("reason"),
            production_risk_guard.get("playbook_path"),
        )
    if bool(risk_gate_guard.get("blocked")):
        logger.warning(
            "[RISK_GATE_GUARD] BUY dispatch blocked reason=%s basis=%s p0=%s",
            risk_gate_guard.get("reason"),
            risk_gate_guard.get("basis"),
            risk_gate_guard.get("p0_path"),
        )
    if args.validation_mode:
        logger.info(
            "validation_mode=ON allow_block_prefixes=%s min_live_orders=%s",
            _parse_prefixes(args.allow_block_prefixes),
            args.min_live_orders,
        )
    if mock_risk_guarded_buy_allowed:
        logger.warning("mock risk-guarded BUY dispatch is allowed for paper data collection only")
    if client is not None:
        logger.info("broker=KIS mock=%s account=%s", client.cfg.mock, _mask_account(client.cfg.cano, client.cfg.acnt_prdt_cd))

    if args.apply and (not args.allow_offhours):
        if client is not None and bool(client.cfg.mock):
            logger.info("session guard bypassed: mock apply")
        else:
            holidays = _load_holidays_ymd(Path(args.holidays_file))
            ok_sess, msg_sess = _is_krx_session_open(holidays)
            if not ok_sess:
                logger.error("[STOP] session guard blocked apply: %s. Use --allow-offhours if intentional.", msg_sess)
                return 2
            logger.info("session guard pass: %s", msg_sess)

    done_keys = set() if args.force_resend else _load_done_keys(out_csv)
    paper_filled_buy_order_ids = (
        _load_paper_filled_buy_order_ids(PAPER_DIR / "fills.csv", d)
        if bool(args.apply) and str(run_mode).lower() == "mock"
        else set()
    )
    now_ts = dt.datetime.now().isoformat(timespec="seconds")

    records: List[Dict[str, object]] = []
    pending_reconcile_records: List[Dict[str, object]] = []
    if args.apply and bool(args.pending_broker_check) and client is not None and not args.force_resend:
        pending_df = _load_unresolved_pending_rows(out_csv, d)
        if len(pending_df):
            pending_reconcile_records = _resolve_unknown_pending_records(
                pending_df,
                client,
                now_ts=now_ts,
                max_pages=max(1, int(args.pending_query_max_pages or 1)),
            )
            records.extend(pending_reconcile_records)
            for pending_rec in pending_reconcile_records:
                done_keys.add(_dispatch_key(pending_rec))

    conflict_codes = set()
    if len(orders):
        side_map = orders.groupby("code")["side"].apply(lambda s: set(s.astype(str).tolist())).to_dict()
        conflict_codes = {c for c, s in side_map.items() if len(s) > 1}

    sellable_by_code: Dict[str, int] = {}
    reserved_sell_by_code: Dict[str, int] = {}
    if args.apply and client is not None:
        need_sell_check = bool((orders["side"] == "SELL").any()) if len(orders) else False
        if need_sell_check:
            try:
                bal = client.inquire_balance_positions(max_pages=10)
                sellable_by_code = _map_sellable_qty(bal.get("rows", []) or [])
            except Exception as e:
                if args.strict_precheck:
                    logger.error("[STOP] precheck(balance) failed: %s", e)
                    return 2
                logger.warning("precheck(balance) skipped due to error: %s", e)

    seen_batch: set[tuple[str, str, str]] = set()

    for _, row in orders.iterrows():
        rec = {
            "dispatch_ts": now_ts,
            "exec_date": str(row.get("exec_date", "")),
            "side": str(row.get("side", "")),
            "code": str(row.get("code", "")).zfill(6),
            "qty": int(row.get("qty", 0)),
            "price": int(row.get("price", 0)),
            "signal_date": str(row.get("signal_date", "")),
            "is_stop": bool(row.get("is_stop", False)),
            "note": str(row.get("note", "")),
            "entry_blocked": bool(row.get("entry_blocked", False)),
            "entry_block_reason": str(row.get("entry_block_reason", "")),
            "source_row": int(row.get("_source_row", -1)),
            "dispatch_status": "",
            "ok": False,
            "rt_cd": "",
            "msg1": "",
            "ord_no": "",
            "org_no": "",
            "tr_id": "",
            "error": "",
            "error_code": "",
            "error_category": "",
            "error_status_code": "",
            "error_path": "",
            "broker_reconcile_status": "",
            "broker_reconcile_msg": "",
            "broker_reconcile_rows": "",
            "broker_reconcile_ord_no": "",
            "broker_reconcile_org_no": "",
            "apply": bool(args.apply),
            "order_type": str(row.get("order_type") or args.order_type),
            "mode": run_mode,
            "precheck": "SKIP" if not args.apply else "PASS",
            "precheck_msg": "",
            "score_policy": bool(args.score_policy),
            "venue_type": str(args.venue_type),
            "order_score": "",
            "p_fill": "",
            "signed_markout": "",
            "estimated_tc": "",
            "score_action": "",
            "imbalance": "",
            "spread_bps": "",
            "pretrade_lob_check": bool(args.general_lob_precheck),
            "pretrade_lob_status": "",
            "pretrade_lob_reason": "",
            "executable_qty": "",
            "executable_price": "",
            "ask_depth_levels": "",
            "ask1": "",
            "bid1": "",
            "askq1": "",
            "bidq1": "",
            "orderflow_guard": bool(orderflow_guard.get("enabled", False)),
            "orderflow_guard_blocked": bool(orderflow_guard.get("blocked", False)),
            "orderflow_guard_reason": str(orderflow_guard.get("reason", "")),
            "production_risk_guard": bool(production_risk_guard.get("enabled", False)),
            "production_risk_guard_blocked": bool(production_risk_guard.get("blocked", False)),
            "production_risk_guard_action": str(production_risk_guard.get("action", "")),
            "production_risk_guard_reason": str(production_risk_guard.get("reason", "")),
            "risk_gate_guard": bool(risk_gate_guard.get("enabled", False)),
            "risk_gate_guard_blocked": bool(risk_gate_guard.get("blocked", False)),
            "risk_gate_guard_reason": str(risk_gate_guard.get("reason", "")),
            "risk_gate_guard_basis": str(risk_gate_guard.get("basis", "")),
            "risk_gate_guard_p0_path": str(risk_gate_guard.get("p0_path", "")),
            "source_entry_guard_blocked": False,
            "source_entry_decision": "",
            "source_entry_blocked": "",
            "source_entry_reason": "",
            "source_exclude_reasons": "",
            "mock_risk_guard_bypass": False,
        }
        rec["entry_order_id"] = _extract_note_field(rec.get("note", ""), "entry_order_id")
        source_entry_guard = _source_entry_block_from_note(str(rec.get("note", "")))
        rec["source_entry_guard_blocked"] = bool(source_entry_guard.get("blocked", False))
        rec["source_entry_decision"] = str(source_entry_guard.get("decision", ""))
        rec["source_entry_blocked"] = str(source_entry_guard.get("blocked_text", ""))
        rec["source_entry_reason"] = str(source_entry_guard.get("reason", ""))
        rec["source_exclude_reasons"] = str(source_entry_guard.get("exclude_reasons", ""))
        rec_orderflow_blocked = _orderflow_guard_blocks_code(orderflow_guard, rec["code"])
        rec["orderflow_guard_blocked"] = bool(rec_orderflow_blocked)

        k = _dispatch_key(rec)
        paper_entry_order_id = str(rec.get("entry_order_id", "") or "").strip()
        if not paper_entry_order_id and str(rec.get("side", "")).upper() == "BUY":
            paper_entry_order_id = f"PAPER_BUY_{rec['code']}_{rec['signal_date']}"
        if bool(source_entry_guard.get("blocked", False)) and str(rec.get("side", "")).upper() == "BUY":
            rec["dispatch_status"] = "PRECHECK_SOURCE_ENTRY_BLOCKED"
            rec["precheck"] = "FAIL"
            rec["precheck_msg"] = str(source_entry_guard.get("message", "source_entry_blocked"))
            records.append(rec)
            done_keys.add(k)
            continue
        if (
            str(run_mode).lower() == "mock"
            and str(rec.get("side", "")).upper() == "BUY"
            and paper_entry_order_id
            and paper_entry_order_id in paper_filled_buy_order_ids
            and not args.force_resend
        ):
            rec["dispatch_status"] = "SKIP_ALREADY_PAPER_FILLED"
            rec["ok"] = True
            rec["precheck"] = "SKIP"
            rec["precheck_msg"] = (
                f"idempotency_self_skip: paper_engine already wrote fill for order_id={paper_entry_order_id}; "
                f"use --force-resend to override"
            )
            records.append(rec)
            done_keys.add(k)
            continue
        if k in done_keys:
            rec["dispatch_status"] = "SKIP_ALREADY_DISPATCHED"
            records.append(rec)
            continue

        key_side = (rec["code"], rec["side"], str(rec.get("entry_order_id", "")))
        if key_side in seen_batch:
            rec["dispatch_status"] = "PRECHECK_DUPLICATE_IN_BATCH"
            rec["precheck"] = "FAIL"
            rec["precheck_msg"] = "duplicate code+side+entry_order_id in same batch"
            records.append(rec)
            continue
        seen_batch.add(key_side)

        if rec["code"] in conflict_codes:
            rec["dispatch_status"] = "PRECHECK_CONFLICT_SIDE"
            rec["precheck"] = "FAIL"
            rec["precheck_msg"] = "both BUY and SELL exist for same code in batch"
            records.append(rec)
            continue

        mock_risk_bypass_msgs: List[str] = []
        if rec_orderflow_blocked and rec["side"] == "BUY":
            if mock_risk_guarded_buy_allowed:
                mock_risk_bypass_msgs.append(
                    "mock_orderflow_guard_bypass="
                    + str(orderflow_guard.get("reason", "orderflow_fail_closed_candidate"))
                )
            else:
                rec["dispatch_status"] = "PRECHECK_ORDERFLOW_FAIL_CLOSED"
                rec["precheck"] = "FAIL"
                rec["precheck_msg"] = str(orderflow_guard.get("reason", "orderflow_fail_closed_candidate"))
                records.append(rec)
                continue

        if bool(production_risk_guard.get("blocked")) and rec["side"] == "BUY":
            if mock_risk_guarded_buy_allowed:
                mock_risk_bypass_msgs.append(
                    "mock_production_risk_bypass="
                    + str(production_risk_guard.get("reason", "production_risk_hard_pause"))
                )
            else:
                rec["dispatch_status"] = "PRECHECK_PRODUCTION_RISK_HARD_PAUSE"
                rec["precheck"] = "FAIL"
                rec["precheck_msg"] = str(production_risk_guard.get("reason", "production_risk_hard_pause"))
                records.append(rec)
                continue
        if bool(risk_gate_guard.get("blocked")) and rec["side"] == "BUY":
            rec["dispatch_status"] = "PRECHECK_RISK_GATE_HARD_BLOCK"
            rec["precheck"] = "FAIL"
            rec["precheck_msg"] = str(risk_gate_guard.get("reason", "risk_gate_hard_block"))
            records.append(rec)
            continue
        if mock_risk_bypass_msgs:
            rec["mock_risk_guard_bypass"] = True
            rec["precheck_msg"] = "; ".join(mock_risk_bypass_msgs)

        if not args.apply:
            rec["dispatch_status"] = "DRY_RUN"
            rec["ok"] = True
            rec["msg1"] = "dry-run only"
            records.append(rec)
            continue

        assert client is not None

        buy_general_lob_precheck = bool(
            str(rec.get("side", "")).upper() == "BUY"
            and bool(args.general_lob_precheck)
        )
        if bool(args.score_policy) or buy_general_lob_precheck:
            imbalance = 0.0
            spread_bps = 0.0
            hoga_features: Dict[str, object] = {}
            try:
                hoga = client.inquire_hoga(str(rec["code"]))
                output = hoga.get("output", {}) if isinstance(hoga, dict) else {}
                if isinstance(output, dict):
                    hoga_features = _extract_buy_lob_features(
                        output,
                        qty=int(rec["qty"]),
                        order_type=str(args.order_type),
                        limit_price=int(rec["price"]),
                        max_depth_levels=int(args.general_lob_depth_levels),
                    )
                    spread_bps = float(_to_float(hoga_features.get("spread_bps"), 0.0) or 0.0)
                    imbalance = float(_to_float(hoga_features.get("imbalance"), 0.0) or 0.0)
            except Exception as e:
                if buy_general_lob_precheck:
                    rec["dispatch_status"] = "PRECHECK_LOB_UNAVAILABLE"
                    rec["precheck"] = "FAIL"
                    rec["precheck_msg"] = f"hoga inquiry failed: {type(e).__name__}: {e}"
                    records.append(rec)
                    continue

            if buy_general_lob_precheck:
                rec["pretrade_lob_status"] = str(hoga_features.get("status", "") or "")
                rec["pretrade_lob_reason"] = str(hoga_features.get("reason", "") or "")
                rec["executable_qty"] = int(_to_int(hoga_features.get("executable_qty"), 0))
                rec["executable_price"] = round(float(_to_float(hoga_features.get("executable_price"), 0.0) or 0.0), 4)
                rec["ask_depth_levels"] = int(_to_int(hoga_features.get("ask_depth_levels"), 0))
                rec["ask1"] = round(float(_to_float(hoga_features.get("ask1"), 0.0) or 0.0), 4)
                rec["bid1"] = round(float(_to_float(hoga_features.get("bid1"), 0.0) or 0.0), 4)
                rec["askq1"] = round(float(_to_float(hoga_features.get("askq1"), 0.0) or 0.0), 4)
                rec["bidq1"] = round(float(_to_float(hoga_features.get("bidq1"), 0.0) or 0.0), 4)
                rec["imbalance"] = round(float(imbalance), 6)
                rec["spread_bps"] = round(float(spread_bps), 4)
                if str(rec["pretrade_lob_status"]).upper() != "OK":
                    rec["dispatch_status"] = "PRECHECK_LOB_UNAVAILABLE"
                    rec["precheck"] = "FAIL"
                    rec["precheck_msg"] = f"general BUY LOB unavailable: {rec['pretrade_lob_reason']}"
                    records.append(rec)
                    continue
                max_spread = max(0.0, float(args.general_max_spread_bps))
                if max_spread > 0.0 and float(spread_bps) > max_spread:
                    rec["dispatch_status"] = "PRECHECK_LOB_SPREAD_BLOCK"
                    rec["precheck"] = "FAIL"
                    rec["precheck_msg"] = f"spread_bps={float(spread_bps):.4f}>{max_spread:.4f}"
                    records.append(rec)
                    continue
                executable_qty = int(_to_int(hoga_features.get("executable_qty"), 0))
                if executable_qty < int(rec["qty"]):
                    if executable_qty > 0:
                        original_qty = int(rec["qty"])
                        rec.setdefault("requested_qty", original_qty)
                        rec["qty"] = int(executable_qty)
                        _append_precheck_msg(rec, f"lob_executable_qty={executable_qty};qty_truncated={original_qty}->{executable_qty}")
                    else:
                        rec["dispatch_status"] = "PRECHECK_LOB_QTY_BLOCK"
                        rec["precheck"] = "FAIL"
                        rec["precheck_msg"] = "LOB executable ask quantity is zero"
                        records.append(rec)
                        continue

            feat = OrderScoreFeature(
                side=str(rec["side"]),
                age_min=0.0,
                remain_ratio=0.0,
                imbalance=float(imbalance),
                spread_bps=float(spread_bps),
                venue_type=str(args.venue_type),
            )
            sc, pf, mk, tc = score_order(feat, score_cfg)
            action = decide_action(sc, pf, str(args.venue_type), score_cfg)
            rec["order_score"] = round(float(sc), 8)
            rec["p_fill"] = round(float(pf), 6)
            rec["signed_markout"] = round(float(mk), 8)
            rec["estimated_tc"] = round(float(tc), 8)
            rec["score_action"] = action
            rec["imbalance"] = round(float(imbalance), 6)
            rec["spread_bps"] = round(float(spread_bps), 4)
            if buy_general_lob_precheck and float(mk) < float(args.general_min_signed_markout):
                rec["dispatch_status"] = "PRECHECK_MARKOUT_BLOCK"
                rec["precheck"] = "FAIL"
                rec["precheck_msg"] = (
                    f"signed_markout={float(mk):.8f}<"
                    f"{float(args.general_min_signed_markout):.8f}"
                )
                records.append(rec)
                continue
            if bool(args.score_policy) and action == "CANCEL_REPOST":
                rec["dispatch_status"] = "PRECHECK_SCORE_BLOCK"
                rec["precheck"] = "FAIL"
                rec["precheck_msg"] = "score policy blocked dispatch"
                records.append(rec)
                continue

        if rec["side"] == "SELL":
            available = sellable_by_code.get(rec["code"], -1)
            reserved = reserved_sell_by_code.get(rec["code"], 0)
            remain = (available - reserved) if available >= 0 else -1
            if remain >= 0 and int(rec["qty"]) > remain:
                rec["dispatch_status"] = "PRECHECK_REJECT_SELL_QTY"
                rec["precheck"] = "FAIL"
                rec["precheck_msg"] = f"sell qty exceeds available remain={remain}"
                records.append(rec)
                continue
            if remain < 0 and args.strict_precheck:
                rec["dispatch_status"] = "PRECHECK_REJECT_SELL_UNKNOWN"
                rec["precheck"] = "FAIL"
                rec["precheck_msg"] = "sellable qty unavailable"
                records.append(rec)
                continue
            if available >= 0:
                reserved_sell_by_code[rec["code"]] = reserved + int(rec["qty"])

        if rec["side"] == "BUY":
            try:
                psbl_qty = -1
                for pre_attempt in range(2):
                    try:
                        psbl_qty = _buy_possible_qty(
                            client,
                            code=str(rec["code"]),
                            order_type=str(rec.get("order_type") or args.order_type),
                            price=int(rec["price"]),
                        )
                        break
                    except Exception as pre_e:
                        pre_err = str(pre_e)
                        if pre_attempt == 0 and _is_rate_limit_error(pre_err):
                            pre_backoff_sec = max(float(args.sleep_ms) / 1000.0, 0.35)
                            logger.warning(
                                "buy-precheck rate-limit retry: code=%s wait=%.2fs err=%s",
                                rec.get("code", ""),
                                pre_backoff_sec,
                                pre_err,
                            )
                            time.sleep(pre_backoff_sec)
                            continue
                        raise
                if psbl_qty >= 0 and int(rec["qty"]) > psbl_qty:
                    if int(psbl_qty) > 0:
                        original_qty = int(rec["qty"])
                        rec["requested_qty"] = original_qty
                        rec["qty"] = int(psbl_qty)
                        _append_precheck_msg(rec, f"buy_possible_qty={psbl_qty};qty_truncated={original_qty}->{psbl_qty}")
                    else:
                        rec["dispatch_status"] = "PRECHECK_REJECT_BUY_QTY"
                        rec["precheck"] = "FAIL"
                        rec["precheck_msg"] = f"buy qty exceeds possible qty={psbl_qty}"
                        records.append(rec)
                        continue
                if psbl_qty < 0 and args.strict_precheck:
                    rec["dispatch_status"] = "PRECHECK_REJECT_BUY_UNKNOWN"
                    rec["precheck"] = "FAIL"
                    rec["precheck_msg"] = "buy possible qty unavailable"
                    records.append(rec)
                    continue
                _append_precheck_msg(rec, f"buy_possible_qty={psbl_qty}")
            except Exception as e:
                if args.strict_precheck:
                    rec["dispatch_status"] = "PRECHECK_ERROR_BUY_PSBL"
                    rec["precheck"] = "FAIL"
                    rec["precheck_msg"] = str(e)
                    records.append(rec)
                    continue
                rec["precheck_msg"] = f"buy_precheck_warn={e}"

        dispatched = False
        for attempt in range(2):
            try:
                rsp = client.place_order_cash(
                    side=rec["side"],
                    code=rec["code"],
                    qty=int(rec["qty"]),
                    price=int(rec["price"]),
                    order_type=str(rec.get("order_type") or args.order_type),
                    exchange="KRX",
                )
                rec["ok"] = bool(rsp.get("ok", False))
                rec["rt_cd"] = str(rsp.get("rt_cd", ""))
                rec["msg1"] = str(rsp.get("msg1", ""))
                rec["ord_no"] = str(rsp.get("ord_no", ""))
                rec["org_no"] = str(rsp.get("org_no", ""))
                rec["tr_id"] = str(rsp.get("tr_id", ""))
                rec["dispatch_status"] = "ACCEPTED" if rec["ok"] else "REJECTED"
                records.append(rec)
                if rec["ok"]:
                    done_keys.add(k)
                dispatched = True
                break
            except KISApiError as e:
                ef = _api_error_fields(e)
                err_text = str(ef.get("message", ""))
                if attempt == 0 and _is_rate_limit_error(err_text):
                    backoff_sec = max(float(args.sleep_ms) / 1000.0, 0.35)
                    logger.warning(
                        "rate-limit retry: code=%s side=%s wait=%.2fs err=%s",
                        rec.get("code", ""),
                        rec.get("side", ""),
                        backoff_sec,
                        err_text,
                    )
                    time.sleep(backoff_sec)
                    continue
                if attempt == 0 and _is_auth_error_fields(ef):
                    logger.warning(
                        "auth retry after token invalidation: code=%s side=%s err=%s",
                        rec.get("code", ""),
                        rec.get("side", ""),
                        err_text,
                    )
                    try:
                        client.invalidate_token_cache()
                    except Exception:
                        pass
                    continue
                if _is_unknown_pending_fields(ef):
                    rec["dispatch_status"] = "UNKNOWN_PENDING"
                    rec["error"] = err_text
                    rec["error_code"] = str(ef.get("error_code", ""))
                    rec["error_category"] = str(ef.get("error_category", ""))
                    rec["error_status_code"] = str(ef.get("status_code", ""))
                    rec["error_path"] = str(ef.get("path", ""))
                    records.append(rec)
                    done_keys.add(k)
                    dispatched = True
                    break
                rec["dispatch_status"] = "ERROR"
                rec["error"] = err_text
                rec["error_code"] = str(ef.get("error_code", ""))
                rec["error_category"] = str(ef.get("error_category", ""))
                rec["error_status_code"] = str(ef.get("status_code", ""))
                rec["error_path"] = str(ef.get("path", ""))
                records.append(rec)
                dispatched = True
                break
            except Exception as e:
                rec["dispatch_status"] = "ERROR"
                rec["error"] = f"unexpected: {e}"
                records.append(rec)
                dispatched = True
                break
        if not dispatched:
            rec["dispatch_status"] = "ERROR"
            rec["error"] = "unexpected: dispatch loop exited without result"
            records.append(rec)

        _log_dispatch_record(rec, d)

        if args.sleep_ms > 0:
            time.sleep(float(args.sleep_ms) / 1000.0)

    new_df = pd.DataFrame(records)
    if len(new_df) and bool(args.apply) and str(run_mode).lower() == "mock":
        msg_series = new_df.get("msg1", pd.Series("", index=new_df.index)).astype(str)
        market_closed_mask = (
            new_df.get("dispatch_status", pd.Series("", index=new_df.index)).astype(str).eq("REJECTED")
            & msg_series.str.contains("장종료|장 종료|장마감|market\\s*closed", case=False, regex=True, na=False)
        )
        if bool(market_closed_mask.any()):
            new_df.loc[market_closed_mask, "dispatch_status"] = "SKIP_MARKET_CLOSED"
            logger.info("mock apply market-closed skips: %s", int(market_closed_mask.sum()))
    if out_csv.exists():
        try:
            old = pd.read_csv(out_csv, dtype=str)
            out_df = pd.concat([old, new_df], ignore_index=True)
        except Exception:
            out_df = new_df
    else:
        out_df = new_df

    out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    actual_order_type_agg = "unknown"
    if "dispatch_status" in new_df.columns and "order_type" in new_df.columns:
        accepted_df = new_df[new_df["dispatch_status"] == "ACCEPTED"]
        if not accepted_df.empty:
            actual_order_type_agg = str(accepted_df["order_type"].iloc[-1])

    summary = {
        "generated_at": now_ts,
        "D": d,
        "today": today_ymd,
        "orders_path": str(orders_path),
        "submit_log": str(out_csv),
        "apply": bool(args.apply),
        "order_type": actual_order_type_agg,
        "mode": run_mode,
        "strict_precheck": bool(args.strict_precheck),
        "validation_mode": bool(args.validation_mode),
        "allow_block_prefixes": _parse_prefixes(args.allow_block_prefixes),
        "min_live_orders": int(args.min_live_orders),
        "rows_eligible": int(len(orders)),
        "rows_new": int(len(new_df)),
        "pending_reconcile_rows": int(len(pending_reconcile_records)),
        "counts": new_df.get("dispatch_status", pd.Series(dtype=str)).value_counts(dropna=False).to_dict(),
        "score_policy": bool(args.score_policy),
        "venue_type": str(args.venue_type),
        "general_lob_precheck": {
            "enabled": bool(args.general_lob_precheck),
            "max_spread_bps": float(args.general_max_spread_bps),
            "min_signed_markout": float(args.general_min_signed_markout),
            "depth_levels": int(args.general_lob_depth_levels),
        },
        "mock_risk_guarded_buy_allowed": bool(mock_risk_guarded_buy_allowed),
        "risk_gate_guard": risk_gate_guard,
        "orderflow_guard": orderflow_guard,
        "production_risk_guard": production_risk_guard,
    }

    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    reject_mask = new_df.get("dispatch_status", pd.Series(dtype=str)).astype(str).isin(["REJECTED", "ERROR"])
    fail_status = {
        "REJECTED",
        "ERROR",
        "PRECHECK_DUPLICATE_IN_BATCH",
        "PRECHECK_CONFLICT_SIDE",
        "PRECHECK_REJECT_SELL_QTY",
        "PRECHECK_REJECT_SELL_UNKNOWN",
        "PRECHECK_REJECT_BUY_QTY",
        "PRECHECK_REJECT_BUY_UNKNOWN",
        "PRECHECK_ERROR_BUY_PSBL",
        "PRECHECK_SCORE_BLOCK",
        "PRECHECK_LOB_UNAVAILABLE",
        "PRECHECK_LOB_SPREAD_BLOCK",
        "PRECHECK_LOB_QTY_BLOCK",
        "PRECHECK_MARKOUT_BLOCK",
        "PRECHECK_ORDERFLOW_FAIL_CLOSED",
        "PRECHECK_PRODUCTION_RISK_HARD_PAUSE",
        "PRECHECK_RISK_GATE_HARD_BLOCK",
        "PRECHECK_SOURCE_ENTRY_BLOCKED",
        "PENDING_BROKER_QUERY_FAIL",
        "PENDING_BROKER_QUERY_NOT_FOUND",
    }
    fail_mask = new_df.get("dispatch_status", pd.Series(dtype=str)).astype(str).isin(list(fail_status))
    score_series = pd.to_numeric(new_df.get("order_score", pd.Series(dtype=float)), errors="coerce")
    dispatch_metrics = {
        "generated_at": now_ts,
        "date": d,
        "score_policy": bool(args.score_policy),
        "venue_type": str(args.venue_type),
        "rows": int(len(new_df)),
        "reject_rate": float(reject_mask.mean()) if len(new_df) else 0.0,
        "fail_rate": float(fail_mask.mean()) if len(new_df) else 0.0,
        "avg_score": float(score_series.mean()) if len(score_series.dropna()) else None,
        "avg_p_fill": float(pd.to_numeric(new_df.get("p_fill", pd.Series(dtype=float)), errors="coerce").mean()) if len(new_df) else None,
        "new_orders_allowed": bool((not args.apply) or int(fail_mask.sum()) == 0),
        "counts": summary["counts"],
    }
    metrics_dated = RUNS_DIR / f"order_scoring_metrics_{d}.json"
    metrics_latest = RUNS_DIR / "order_scoring_metrics_latest.json"
    metrics_dated_ok = _merge_scoring_metrics(metrics_dated, "dispatch", dispatch_metrics)
    metrics_latest_ok = _merge_scoring_metrics(metrics_latest, "dispatch", dispatch_metrics)
    summary["metrics_write"] = {
        "dated_path": str(metrics_dated),
        "dated_ok": bool(metrics_dated_ok),
        "latest_path": str(metrics_latest),
        "latest_ok": bool(metrics_latest_ok),
    }
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    reject_rows = new_df[reject_mask].copy()
    reject_pattern = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "source": "dispatch",
        "date": d,
        "total_rejects": int(len(reject_rows)),
        "by_status": reject_rows.get("dispatch_status", pd.Series(dtype=str)).value_counts(dropna=False).to_dict(),
        "by_error_code": reject_rows.get("error_code", pd.Series(dtype=str)).astype(str).value_counts(dropna=False).to_dict() if len(reject_rows) else {},
        "by_error_category": reject_rows.get("error_category", pd.Series(dtype=str)).astype(str).value_counts(dropna=False).to_dict() if len(reject_rows) else {},
    }
    (LOG_DIR / "fix_reject_pattern_last.json").write_text(json.dumps(reject_pattern, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("[OK] submit_log=%s rows_new=%s", out_csv, len(new_df))
    logger.info("[OK] summary=%s", out_json)
    logger.info("[OK] scoring_metrics=%s", metrics_dated)
    logger.info("[OK] reject_pattern=%s", LOG_DIR / "fix_reject_pattern_last.json")

    if args.apply:
        fail_n = int(fail_mask.sum()) if len(new_df) else 0
        if fail_n > 0:
            logger.error("[STOP] apply mode had failed orders: %s", fail_n)
            return 2

    dispatch_status_counts = (
        new_df["dispatch_status"].value_counts(dropna=False).to_dict()
        if len(new_df) and "dispatch_status" in new_df.columns
        else {}
    )
    log_pipeline_event(
        stage="broker_dispatch",
        batch_label="[12.5/14]",
        event="END",
        date=_audit_ymd,
        output_files={
            "broker_submit_csv": {"path": str(out_csv), "rows": int(len(new_df))},
        },
        metrics={
            "apply": bool(args.apply),
            "mode": str(run_mode),
            "eligible_orders": int(len(orders)),
            "dispatch_status_counts": dispatch_status_counts,
        },
        status="PASS",
    )

    return 0


def _log_dispatch_record(rec: Dict[str, object], d: str) -> None:
    status = str(rec.get("dispatch_status", ""))
    side = str(rec.get("side", "")).upper()
    if status in {"ACCEPTED"}:
        decision = "PASS"
    elif status in {"DRY_RUN"}:
        decision = "DRY_RUN"
    elif status.startswith("SKIP"):
        decision = "SKIP"
    else:
        decision = "FAIL"
    log_gate_event(
        code=str(rec.get("code", "")).zfill(6),
        gate_layer="dispatch" if decision in {"PASS", "FAIL", "SKIP"} else "broker",
        gate_name="kis_order_dispatch",
        decision=decision,
        reason_code=status,
        reason_detail=str(rec.get("precheck_msg", "") or rec.get("error", "")),
        source_file="tools/kis_order_dispatch_from_exec.py",
        source_key=str(rec.get("source_entry_reason", "") or status),
        date=d,
        extra={
            "side": side,
            "qty": int(rec.get("qty", 0) or 0),
            "price": int(rec.get("price", 0) or 0),
            "apply": bool(rec.get("apply", False)),
            "order_type": str(rec.get("order_type", "")),
            "mode": str(rec.get("mode", "")),
            "ord_no": str(rec.get("ord_no", "")),
            "entry_block_reason": str(rec.get("entry_block_reason", "")),
        },
    )


if __name__ == "__main__":
    raise SystemExit(main())

