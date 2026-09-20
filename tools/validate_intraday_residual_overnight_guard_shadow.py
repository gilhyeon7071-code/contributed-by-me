from __future__ import annotations

import importlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CONFIG_PATH = ROOT / "paper" / "paper_engine_config.json"
OP_JSON = LOG_DIR / "intraday_residual_overnight_guard_shadow_latest.json"
OUT_JSON = LOG_DIR / "intraday_residual_overnight_guard_validation_latest.json"
TMP_JSON = Path(r"C:\tmp\intraday_residual_overnight_guard_shadow_validator.json")
TMP_CSV = Path(r"C:\tmp\intraday_residual_overnight_guard_shadow_validator.csv")


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def _check_config() -> Dict[str, Any]:
    cfg = _load_json(CONFIG_PATH)
    guard = cfg.get("intraday_residual_overnight_guard", {})
    required = {
        "enabled": False,
        "shadow_only": True,
        "exit_before_overnight": False,
    }
    failures: List[str] = []
    for key, expected in required.items():
        if guard.get(key) != expected:
            failures.append(f"{key}!={expected!r}")
    return {
        "status": "PASS" if not failures else "FAIL",
        "path": str(CONFIG_PATH),
        "guard": guard,
        "failures": failures,
    }


def _check_operational_artifact() -> Dict[str, Any]:
    if not OP_JSON.exists():
        return {"status": "FAIL", "path": str(OP_JSON), "reason": "missing"}
    payload = _load_json(OP_JSON)
    failures: List[str] = []
    if payload.get("trading_effect") is not False:
        failures.append("trading_effect_not_false")
    if payload.get("policy_effect") is not False:
        failures.append("policy_effect_not_false")
    if "123456" in json.dumps(payload, ensure_ascii=False):
        failures.append("synthetic_code_leaked")
    return {
        "status": "PASS" if not failures else "FAIL",
        "path": str(OP_JSON),
        "generated_at": payload.get("generated_at"),
        "candidates": payload.get("candidates"),
        "trading_effect": payload.get("trading_effect"),
        "policy_effect": payload.get("policy_effect"),
        "failures": failures,
    }


def _check_synthetic_detection() -> Dict[str, Any]:
    TMP_JSON.parent.mkdir(parents=True, exist_ok=True)
    os.environ["PAPER_INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_JSON_PATH"] = str(TMP_JSON)
    os.environ["PAPER_INTRADAY_RESIDUAL_OVERNIGHT_GUARD_SHADOW_CSV_PATH"] = str(TMP_CSV)
    pe = importlib.import_module("paper_engine")
    cfg = {
        "intraday_residual_overnight_guard": {
            "enabled": False,
            "shadow_only": True,
            "scope": "intraday_realtime",
        }
    }
    trades = [
        [
            "TVALID",
            "2026-06-01 10:00:00",
            "2026-06-01 14:50:00",
            "123456",
            "VALID",
            "LONG",
            5,
            1000,
            950,
            -0.05,
            -0.05,
            0,
            0,
            "1",
            "0",
            "exit_reason=STOP;sell_qty=5;partial_exit=1;",
        ]
    ]
    still_open = [
        {
            "code": "123456",
            "entry_date": "20260601",
            "entry_timing": "intraday_realtime",
            "qty": 5,
            "_surge_immediate": 1,
            "surge_type": "VALID",
            "source_order_id": "OID",
            "entry_order_id": "OID",
        }
    ]
    out = pe._write_intraday_residual_overnight_guard_shadow(
        config=cfg,
        schema="v41.1",
        trades_new=trades,
        still_open=still_open,
        runtime_ymd="20260601",
    )
    failures: List[str] = []
    if out.get("status") != "PASS":
        failures.append("status_not_pass")
    if int(out.get("candidates", 0) or 0) != 1:
        failures.append("candidate_count_not_1")
    if out.get("trading_effect") is not False:
        failures.append("trading_effect_not_false")
    if not TMP_JSON.exists() or not TMP_CSV.exists():
        failures.append("temp_artifact_missing")
    return {
        "status": "PASS" if not failures else "FAIL",
        "temp_json": str(TMP_JSON),
        "temp_csv": str(TMP_CSV),
        "candidates": out.get("candidates"),
        "trading_effect": out.get("trading_effect"),
        "failures": failures,
    }


def _check_enforcement_path() -> Dict[str, Any]:
    pe = importlib.import_module("paper_engine")
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
        "failures": failures,
    }


def main() -> int:
    checks = {
        "config": _check_config(),
        "operational_artifact": _check_operational_artifact(),
        "synthetic_detection": _check_synthetic_detection(),
        "enforcement_path": _check_enforcement_path(),
    }
    status = "PASS" if all(v.get("status") == "PASS" for v in checks.values()) else "FAIL"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "trading_effect": False,
        "policy_effect": False,
        "checks": checks,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": status, "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
