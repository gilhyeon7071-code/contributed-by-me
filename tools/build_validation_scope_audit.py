from __future__ import annotations

import csv
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
ROOTB = Path(r"E:\vibe\buffett")
SCOPE_LABEL = "KIS_API_PAPER_CAPITAL_100M_START"
SCOPE_DESCRIPTION = (
    "한국투자증권 API 연결 후 가상매매 기준 원금 1억원으로 "
    "주문-체결-원장-통계 흐름 검증을 시작한 시점"
)
SCOPE_APPLIES_TO = [
    "paper_pnl",
    "ledger_consistency",
    "live_vs_bt",
    "trading_stage_validation",
    "paper_backtest_logic_validation",
]


def _now_iso() -> str:
    return dt.datetime.now().replace(microsecond=0).isoformat()


def _norm_ymd(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text[:8] if len(text) >= 8 else ""


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    for enc in ("utf-8-sig", "utf-8", "cp949", "mbcs"):
        try:
            return path.read_text(encoding=enc)
        except Exception:
            continue
    return ""


def _csv_min_date(path: Path, col: str = "date") -> tuple[str, int]:
    if not path.exists():
        return "", 0
    min_ymd = ""
    rows = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows += 1
            ymd = _norm_ymd(row.get(col))
            if ymd and (not min_ymd or ymd < min_ymd):
                min_ymd = ymd
    return min_ymd, rows


def _check(name: str, path: Path, expected: str, actual: str, *, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    status = "PASS" if actual == expected else "FAIL"
    return {
        "name": name,
        "status": status,
        "path": str(path),
        "expected_start_ymd": expected,
        "actual_start_ymd": actual or None,
        "extra": extra or {},
    }


def build_audit() -> dict[str, Any]:
    expected = _norm_ymd(os.getenv("PAPER_OPER_START_YMD", "20260301")) or "20260301"
    checks: list[dict[str, Any]] = []

    paper_pnl_path = LOG_DIR / "paper_pnl_summary_last.json"
    paper_pnl = _read_json(paper_pnl_path)
    pnl_scope = (((paper_pnl.get("meta") or {}).get("operational_scope")) or {})
    checks.append(
        _check(
            "paper_pnl_summary.operational_scope",
            paper_pnl_path,
            expected,
            _norm_ymd(pnl_scope.get("start_ymd")),
            extra={
                "applied": bool(pnl_scope.get("applied")),
                "filter_col": pnl_scope.get("filter_col"),
                "rows_before": pnl_scope.get("rows_before"),
                "rows_after": pnl_scope.get("rows_after"),
            },
        )
    )

    live_vs_bt_path = LOG_DIR / "live_vs_bt_feedback_latest.json"
    live_vs_bt = _read_json(live_vs_bt_path)
    thresholds = live_vs_bt.get("thresholds") or {}
    alignment = ((live_vs_bt.get("optimize") or {}).get("alignment")) or {}
    checks.append(
        _check(
            "live_vs_bt_feedback.thresholds",
            live_vs_bt_path,
            expected,
            _norm_ymd(thresholds.get("oper_start_ymd")),
            extra={
                "alignment_oper_start_ymd": _norm_ymd(alignment.get("oper_start_ymd")),
                "alignment_window_start": _norm_ymd(alignment.get("window_start")),
                "alignment_ready": alignment.get("ready"),
            },
        )
    )

    stage_path = LOG_DIR / "trading_stage_validation_latest.json"
    stage = _read_json(stage_path)
    stage_scope = ((stage.get("meta") or {}).get("operational_scope")) or {}
    checks.append(
        _check(
            "trading_stage_validation.operational_scope",
            stage_path,
            expected,
            _norm_ymd(stage_scope.get("start_ymd")),
            extra={
                "applied": bool(stage_scope.get("applied")),
                "scope_mismatch": bool(stage_scope.get("scope_mismatch")),
                "rows_before": stage_scope.get("rows_before"),
                "rows_after": stage_scope.get("rows_after"),
            },
        )
    )

    ledger_monitor_path = LOG_DIR / "ledger_live_fills_dry_run_latest.json"
    ledger_monitor = _read_json(ledger_monitor_path)
    checks.append(
        _check(
            "ledger_live_fills_dry_run.oper_start_ymd",
            ledger_monitor_path,
            expected,
            _norm_ymd(ledger_monitor.get("oper_start_ymd")),
            extra={"status": ledger_monitor.get("status"), "missing_rows": ledger_monitor.get("missing_rows")},
        )
    )

    repair_path = LOG_DIR / "ledger_live_fills_repair_latest.json"
    repair = _read_json(repair_path)
    if repair:
        checks.append(
            _check(
                "ledger_live_fills_repair.oper_start_ymd",
                repair_path,
                expected,
                _norm_ymd(repair.get("oper_start_ymd")),
                extra={"status": repair.get("status"), "missing_rows": repair.get("missing_rows")},
            )
        )

    bt_batch_path = ROOT / "run_backtest_validation_real.bat"
    bt_batch = _read_text(bt_batch_path)
    bt_match = re.search(r"set\s+\"BT_EVAL_START_YMD=(\d{8})\"", bt_batch, flags=re.IGNORECASE)
    bt_start = bt_match.group(1) if bt_match else ""
    checks.append(
        _check(
            "backtest_validation_real.batch_eval_start",
            bt_batch_path,
            expected,
            bt_start,
            extra={"source": "BT_EVAL_START_YMD"},
        )
    )

    bt_market_path = LOG_DIR / "backtest_market_ohlc_real_overlap_latest.csv"
    min_date, rows = _csv_min_date(bt_market_path, "date")
    checks.append(
        {
            "name": "backtest_market_real_overlap.scope_basis",
            "status": "PASS" if min_date and min_date >= expected else "FAIL",
            "path": str(bt_market_path),
            "expected_start_ymd": expected,
            "actual_start_ymd": expected if min_date and min_date >= expected else (min_date or None),
            "extra": {
                "rows": rows,
                "rule": "scope_basis == expected_start_ymd and first_trading_ymd >= scope_basis",
                "scope_basis_ymd": expected,
                "first_trading_ymd": min_date or None,
            },
        }
    )

    dashboard_path = ROOTB / "runs" / "dashboard_state_latest.json"
    dashboard = _read_json(dashboard_path)
    ops_contract = dashboard.get("ops_contract") or {}
    dash_scope = ops_contract.get("validation_scope_contract") or {}
    checks.append(
        _check(
            "rootb_dashboard_state.validation_scope_contract",
            dashboard_path,
            expected,
            _norm_ymd(dash_scope.get("expected_start_ymd")),
            extra={
                "status": dash_scope.get("status"),
                "scope_label": dash_scope.get("scope_label"),
                "source": dash_scope.get("source"),
                "fail_count": dash_scope.get("fail_count"),
                "warn_count": dash_scope.get("warn_count"),
            },
        )
    )

    final_path = LOG_DIR / "backtest_final_output_latest.json"
    final = _read_json(final_path)
    eval_scope = final.get("evaluation_scope") or {}
    checks.append(
        {
            "name": "backtest_final_output.evaluation_scope_label",
            "status": "PASS" if eval_scope.get("scope") == "paper_early_logic_check" else "WARN",
            "path": str(final_path),
            "expected_start_ymd": expected,
            "actual_start_ymd": None,
            "extra": {
                "scope": eval_scope.get("scope"),
                "strategy_conclusion_allowed": eval_scope.get("strategy_conclusion_allowed"),
                "note": "Backtest period is enforced by wrapper and market overlap CSV.",
            },
        }
    )

    fail_count = sum(1 for c in checks if c.get("status") == "FAIL")
    warn_count = sum(1 for c in checks if c.get("status") == "WARN")
    status = "PASS" if fail_count == 0 else "FAIL"
    return {
        "generated_at": _now_iso(),
        "status": status,
        "scope_label": SCOPE_LABEL,
        "scope_description": SCOPE_DESCRIPTION,
        "expected_start_ymd": expected,
        "applies_to": SCOPE_APPLIES_TO,
        "fail_count": fail_count,
        "warn_count": warn_count,
        "checks": checks,
    }


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = build_audit()
    out = LOG_DIR / f"validation_scope_audit_{stamp}.json"
    latest = LOG_DIR / "validation_scope_audit_latest.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    out.write_text(text, encoding="utf-8")
    latest.write_text(text, encoding="utf-8")
    print(f"[VALIDATION_SCOPE] status={payload['status']} scope={payload['scope_label']} expected_start_ymd={payload['expected_start_ymd']} fails={payload['fail_count']} warns={payload['warn_count']}")
    print(f"[VALIDATION_SCOPE] json={out}")
    print(f"[VALIDATION_SCOPE] latest={latest}")
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
