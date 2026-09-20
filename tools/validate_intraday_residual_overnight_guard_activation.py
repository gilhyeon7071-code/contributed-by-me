from __future__ import annotations

import hashlib
import importlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
CONFIG_PATH = PAPER_DIR / "paper_engine_config.json"
LOCK_PATH = PAPER_DIR / "paper_engine_config.lock.json"
RISK_AUDIT_JSON = LOG_DIR / "intraday_residual_overnight_risk_audit_latest.json"
PNL_SUMMARY_JSON = LOG_DIR / "paper_pnl_summary_last.json"
OUT_JSON = LOG_DIR / "intraday_residual_overnight_guard_activation_validation_latest.json"
FILLS_CSV = PAPER_DIR / "fills.csv"


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _check_config() -> Dict[str, Any]:
    cfg = _load_json(CONFIG_PATH)
    guard = cfg.get("intraday_residual_overnight_guard", {})
    required = {
        "enabled": True,
        "shadow_only": False,
        "exit_before_overnight": True,
        "trigger_on_same_day_loss": True,
        "apply_to_surge": True,
        "apply_to_non_surge": True,
    }
    failures: List[str] = []
    for key, expected in required.items():
        if guard.get(key) != expected:
            failures.append(f"{key}!={expected!r}")
    if guard.get("scope") != "intraday_realtime":
        failures.append("scope!='intraday_realtime'")
    if guard.get("evidence_artifact") != "2_Logs/intraday_residual_overnight_risk_audit_latest.json":
        failures.append("evidence_artifact_mismatch")
    return {
        "status": "PASS" if not failures else "FAIL",
        "path": str(CONFIG_PATH),
        "guard": guard,
        "failures": failures,
    }


def _check_lock() -> Dict[str, Any]:
    failures: List[str] = []
    if not LOCK_PATH.exists():
        failures.append("lock_missing")
        lock = {}
    else:
        lock = _load_json(LOCK_PATH)
    cfg_sha = _sha256(CONFIG_PATH) if CONFIG_PATH.exists() else ""
    approved_sha = str(lock.get("approved_sha256") or "")
    if not cfg_sha:
        failures.append("config_missing")
    if cfg_sha != approved_sha:
        failures.append("config_lock_sha_mismatch")
    return {
        "status": "PASS" if not failures else "FAIL",
        "config_sha256": cfg_sha,
        "approved_sha256": approved_sha,
        "last_change_log": lock.get("last_change_log"),
        "failures": failures,
    }


def _check_evidence_artifact() -> Dict[str, Any]:
    failures: List[str] = []
    if not RISK_AUDIT_JSON.exists():
        return {"status": "FAIL", "path": str(RISK_AUDIT_JSON), "failures": ["missing"]}
    payload = _load_json(RISK_AUDIT_JSON)
    pnl_summary = _load_json(PNL_SUMMARY_JSON) if PNL_SUMMARY_JSON.exists() else {}
    if payload.get("status") != "PASS":
        failures.append("risk_audit_status_not_pass")
    primary = (payload.get("interpretation") or {}).get("primary_candidate")
    if primary != "remove_all_overnight_after_same_day_loss":
        failures.append("primary_candidate_mismatch")
    baseline = payload.get("baseline") or {}
    expected_rows = pnl_summary.get("trades_used")
    if expected_rows is None:
        failures.append("pnl_summary_trades_used_missing")
    elif int(baseline.get("rows") or -1) != int(expected_rows):
        failures.append("baseline_rows_mismatch")
    return {
        "status": "PASS" if not failures else "FAIL",
        "path": str(RISK_AUDIT_JSON),
        "pnl_summary_path": str(PNL_SUMMARY_JSON),
        "generated_at": payload.get("generated_at"),
        "primary_candidate": primary,
        "baseline_rows": baseline.get("rows"),
        "expected_rows_from_pnl_summary": expected_rows,
        "baseline_max_drawdown_pct": baseline.get("max_drawdown_pct"),
        "failures": failures,
    }


def _check_enforcement_path() -> Dict[str, Any]:
    # [2026-09-11] **패키지가 아니라 paper_engine.exit 을 잡는다.** 두 가지가 걸려 있었다.
    #   (1) paper_engine/__init__.py 는 exit 에서 4개만 재수출한다. 이 함수들은 없다
    #       -> pe._apply_... 가 AttributeError. 2026-09-11 까지 매일 rc=1 advisory 였다
    #   (2) 더 중요한 것: 아래에서 SHADOW_*_PATH 를 tmp 로 갈아끼우는데,
    #       exit.py:106 이 그 이름을 **import 시점에 자기 네임스페이스로 바인딩**한다.
    #       패키지에 설정하면 그 바인딩이 안 바뀌어 패치가 무효가 되고,
    #       _write_..._shadow 가 **진짜 latest.json/csv** 에 합성 데이터를 쓴다.
    #       exit 모듈 자체를 잡아야 패치가 실제로 먹는다.
    #   -> feedback_check_where_the_tool_writes
    pe = importlib.import_module("paper_engine.exit")
    px = pd.DataFrame([{"code": "123456", "date": "20260601", "close": 980.0}])
    pos = {
        "code": "123456",
        "entry_date": "20260601",
        "entry_price": 1000.0,
        "qty": 3,
        "entry_timing": "intraday_realtime",
        "entry_order_id": "BUY1",
        "source_order_id": "BUY1",
        "signal_date": "20260601",
    }
    shadow = {"records": [{"code": "123456", "entry_date": "20260601"}]}

    disabled_fills: List[Any] = []
    disabled_trades: List[Any] = []
    disabled = pe._apply_intraday_residual_overnight_guard_exits(
        config={"intraday_residual_overnight_guard": {"enabled": False, "shadow_only": True, "exit_before_overnight": False}},
        schema="legacy",
        prices_df=px,
        still_open=[dict(pos)],
        shadow_payload=shadow,
        runtime_ymd="20260601",
        fee_pct=0.0,   # [2026-09-10] 브로커 실측 수수료 0 (제세금은 sell_tax_pct)
        slip_pct=0.001,
        sell_tax_pct=0.002,
        fills_new=disabled_fills,
        trades_new=disabled_trades,
        existing_fill_order_ids=set(),
        existing_trade_sigs=set(),
        next_seq_start=7,
    )

    active_fills: List[Any] = []
    active_trades: List[Any] = []
    active = pe._apply_intraday_residual_overnight_guard_exits(
        config={"intraday_residual_overnight_guard": {"enabled": True, "shadow_only": False, "exit_before_overnight": True}},
        schema="legacy",
        prices_df=px,
        still_open=[dict(pos)],
        shadow_payload=shadow,
        runtime_ymd="20260601",
        fee_pct=0.0,   # [2026-09-10] 브로커 실측 수수료 0 (제세금은 sell_tax_pct)
        slip_pct=0.001,
        sell_tax_pct=0.002,
        fills_new=active_fills,
        trades_new=active_trades,
        existing_fill_order_ids=set(),
        existing_trade_sigs=set(),
        next_seq_start=7,
    )

    original_shadow_json = pe.INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH
    original_shadow_csv = pe.INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH
    try:
        pe.INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH = LOG_DIR / "intraday_residual_overnight_guard_shadow_validation_tmp.json"
        pe.INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH = LOG_DIR / "intraday_residual_overnight_guard_shadow_validation_tmp.csv"
        inferred_shadow = pe._write_intraday_residual_overnight_guard_shadow(
            config={"intraday_residual_overnight_guard": {"enabled": True, "shadow_only": False, "scope": "intraday_realtime"}},
            schema="legacy",
            trades_new=[
                ("T_MISSING_ENTRY_TIMING", "654321", "20260601", 1000.0, "20260601", 960.0, -0.04, 1, "STOP", "sell_qty=1;")
            ],
            still_open=[
                {
                    "code": "654321",
                    "entry_date": "20260601",
                    "entry_ts": "20260601T10:05:00",
                    "qty": 1,
                    "entry_price": 1000.0,
                }
            ],
            runtime_ymd="20260601",
        )
        cross_order_shadow = pe._write_intraday_residual_overnight_guard_shadow(
            config={"intraday_residual_overnight_guard": {"enabled": True, "shadow_only": False, "scope": "intraday_realtime"}},
            schema="legacy",
            trades_new=[
                ("T_LOSS_ORDER_A", "403550", "20260514", 20050.0, "20260514", 19603.64, -0.0222, 7, "STOP", "sell_qty=7;")
            ],
            still_open=[
                {
                    "code": "403550",
                    "entry_date": "20260514",
                    "entry_ts": "20260514T11:39:04",
                    "qty": 22,
                    "entry_price": 19980.0,
                    "entry_timing": "intraday_realtime",
                    "entry_order_id": "PAPER_BUY_403550_20260514_R20260514_N3",
                }
            ],
            runtime_ymd="20260514",
        )
    finally:
        pe.INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH = original_shadow_json
        pe.INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH = original_shadow_csv

    failures: List[str] = []
    if disabled.get("trading_effect") is not False or disabled_fills or disabled_trades:
        failures.append("disabled_path_has_effect")
    if active.get("trading_effect") is not True:
        failures.append("active_path_no_trading_effect")
    if int(active.get("applied_count", 0) or 0) != 1:
        failures.append("active_applied_count_not_1")
    if len(active_fills) != 1 or len(active_trades) != 1:
        failures.append("active_fill_trade_count_not_1")
    if active.get("still_open"):
        failures.append("active_still_open_not_empty")
    if int(inferred_shadow.get("candidates", 0) or 0) != 1:
        failures.append("missing_entry_timing_inference_no_candidate")
    if int(cross_order_shadow.get("candidates", 0) or 0) != 1:
        failures.append("cross_order_same_code_entry_date_no_candidate")

    return {
        "status": "PASS" if not failures else "FAIL",
        "disabled": {
            "status": disabled.get("status"),
            "trading_effect": disabled.get("trading_effect"),
            "fills": len(disabled_fills),
            "trades": len(disabled_trades),
        },
        "active": {
            "status": active.get("status"),
            "trading_effect": active.get("trading_effect"),
            "applied_count": active.get("applied_count"),
            "skipped_count": active.get("skipped_count"),
            "fills": len(active_fills),
            "trades": len(active_trades),
            "still_open": len(active.get("still_open") or []),
        },
        "missing_entry_timing_inference": {
            "status": inferred_shadow.get("status"),
            "candidates": inferred_shadow.get("candidates"),
            "records": len(inferred_shadow.get("records") or []),
        },
        "cross_order_same_code_entry_date": {
            "status": cross_order_shadow.get("status"),
            "candidates": cross_order_shadow.get("candidates"),
            "records": len(cross_order_shadow.get("records") or []),
        },
        "failures": failures,
    }


def _check_lineage_reconcile_path() -> Dict[str, Any]:
    # [2026-09-11] 같은 이유. _reconcile_open_positions_with_fills 는 패키지 최상위에 없다
    #   (실측: pkg=False / positions=True). 위 검사를 고치면 여기서 또 죽는다
    pe = importlib.import_module("paper_engine.positions")
    failures: List[str] = []
    if not FILLS_CSV.exists():
        return {"status": "FAIL", "path": str(FILLS_CSV), "failures": ["fills_missing"]}

    fills = pd.read_csv(FILLS_CSV)
    if "datetime" not in fills.columns:
        return {"status": "FAIL", "path": str(FILLS_CSV), "failures": ["datetime_missing"]}
    work = fills.copy()
    work["_ymd"] = work["datetime"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    work = work[work["_ymd"] <= "20260514"].drop(columns=["_ymd"], errors="ignore").copy()
    positions, summary = pe._reconcile_open_positions_with_fills(
        open_positions=[],
        fills_df=work,
        stop_loss=-0.05,
        take_profit=None,
        trail_pct=None,
    )
    rows = [p for p in positions if str(p.get("code") or "").zfill(6) == "403550"]
    by_order = {
        str(p.get("entry_order_id") or p.get("source_order_id") or ""): int(p.get("qty") or 0)
        for p in rows
    }
    expected = {
        "PAPER_BUY_403550_20260514_R20260514": 7,
        "PAPER_BUY_403550_20260514_R20260514_N3": 15,
    }
    if by_order != expected:
        failures.append("lineage_qty_mismatch")
    if len(rows) != 2:
        failures.append("lineage_position_count_not_2")
    if any(qty > expected.get(order_id, qty) for order_id, qty in by_order.items()):
        failures.append("lineage_oversell_risk")
    return {
        "status": "PASS" if not failures else "FAIL",
        "path": str(FILLS_CSV),
        "fixture": "403550_through_20260514",
        "positions": len(rows),
        "by_order": by_order,
        "expected": expected,
        "summary": summary,
        "failures": failures,
    }


def main() -> int:
    checks = {
        "config": _check_config(),
        "lock": _check_lock(),
        "evidence_artifact": _check_evidence_artifact(),
        "enforcement_path": _check_enforcement_path(),
        "lineage_reconcile": _check_lineage_reconcile_path(),
    }
    status = "PASS" if all(v.get("status") == "PASS" for v in checks.values()) else "FAIL"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "trading_effect": False,
        "policy_effect": True,
        "policy_change_applied": status == "PASS",
        "scope": "intraday_residual_overnight_guard_activation",
        "checks": checks,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": status, "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
