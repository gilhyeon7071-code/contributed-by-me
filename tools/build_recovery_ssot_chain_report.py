from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
ROOTB = Path(r"E:\vibe\buffett")
LOG_DIR = ROOT / "2_Logs"
PAPER = ROOT / "paper"
FILLS = PAPER / "fills.csv"
VIRTUAL_LEDGER = ROOT / "virtual_ledger.csv"
ROOTB_LEDGER = ROOTB / "data" / "ledger" / "paper_fills_ledger.csv"
ROOTB_STATS = ROOTB / "data" / "stats" / "live_vs_bt.json"


def _norm_ymd(v: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(v or ""))
    return s[:8] if len(s) >= 8 else ""


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _derive_d_from_fills(rows: List[Dict[str, str]]) -> str:
    candidates: List[str] = []
    fallback: List[str] = []
    for row in rows:
        side = str(row.get("side") or "").strip().upper()
        raw_dt = row.get("datetime") or row.get("ts") or row.get("date") or ""
        ymd = _norm_ymd(raw_dt)
        if not ymd:
            continue
        fallback.append(ymd)
        if side == "BUY":
            candidates.append(ymd)
    return max(candidates or fallback) if (candidates or fallback) else ""


def _count_rows_by_ymd(path: Path, cols: List[str], d: str) -> int:
    rows = _read_csv(path)
    n = 0
    for row in rows:
        for col in cols:
            if _norm_ymd(row.get(col)) == d:
                n += 1
                break
    return n


def _orders_exec_check(d: str) -> Dict[str, Any]:
    path = PAPER / f"orders_{d}_exec.xlsx"
    out: Dict[str, Any] = {"path": str(path), "exists": path.exists(), "exec_date_unique": [], "status": "FAIL"}
    if not path.exists() or not d:
        return out
    try:
        df = pd.read_excel(path)
        vals = sorted({_norm_ymd(x) for x in df.get("exec_date", pd.Series(dtype=str)).astype(str).tolist() if _norm_ymd(x)})
        out["exec_date_unique"] = vals
        out["rows"] = int(len(df))
        out["status"] = "PASS" if vals == [d] else "FAIL"
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}:{exc}"
    return out


def _ledger_append_report_check(d: str) -> Dict[str, Any]:
    path = LOG_DIR / f"ledger_append_report_{d}.json"
    out: Dict[str, Any] = {"path": str(path), "exists": path.exists()}
    if not path.exists() or not d:
        return out
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}:{exc}"
        return out
    metrics = obj.get("metrics") if isinstance(obj.get("metrics"), dict) else {}
    out.update(
        {
            "status": obj.get("status"),
            "reason": obj.get("reason"),
            "input_buy_rows": int(metrics.get("input_buy_rows") or 0),
            "blocked_buy_rows": int(metrics.get("blocked_buy_rows") or 0),
            "blocked_buy_excluded": bool(metrics.get("blocked_buy_excluded", False)),
            "reflected_scope": metrics.get("reflected_scope"),
        }
    )
    return out


def build_report() -> Dict[str, Any]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fills_rows = _read_csv(FILLS)
    d = _derive_d_from_fills(fills_rows)
    checks = {
        "fills_d_rule": {
            "path": str(FILLS),
            "exists": FILLS.exists(),
            "rows": int(len(fills_rows)),
            "D": d,
            "status": "PASS" if d else "FAIL",
        },
        "orders_exec": _orders_exec_check(d),
        "virtual_ledger": {
            "path": str(VIRTUAL_LEDGER),
            "exists": VIRTUAL_LEDGER.exists(),
            "rows_for_D": _count_rows_by_ymd(VIRTUAL_LEDGER, ["purchase_date", "as_of", "date", "exec_date"], d) if d else 0,
        },
        "ledger_append_report": _ledger_append_report_check(d),
        "rootb_ledger": {
            "path": str(ROOTB_LEDGER),
            "exists": ROOTB_LEDGER.exists(),
            "rows_for_D": _count_rows_by_ymd(ROOTB_LEDGER, ["as_of", "date", "exec_date"], d) if d else 0,
        },
        "rootb_stats": {
            "path": str(ROOTB_STATS),
            "exists": ROOTB_STATS.exists(),
        },
    }
    issues: List[str] = []
    if checks["fills_d_rule"]["status"] != "PASS":
        issues.append("fills_D_missing")
    if checks["orders_exec"]["status"] != "PASS":
        issues.append("orders_exec_missing_or_date_mismatch")
    ledger_report = checks["ledger_append_report"]
    no_unblocked_buy_rows = (
        ledger_report.get("status") == "PASS"
        and int(ledger_report.get("input_buy_rows") or 0) == 0
        and bool(ledger_report.get("blocked_buy_excluded", False))
    )
    if d and checks["virtual_ledger"]["exists"] and int(checks["virtual_ledger"]["rows_for_D"]) <= 0 and not no_unblocked_buy_rows:
        issues.append("virtual_ledger_D_rows_missing")
    if not checks["rootb_stats"]["exists"]:
        issues.append("rootb_stats_missing")

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "D": d,
        "status": "PASS" if not issues else "FAIL",
        "issues": issues,
        "checks": checks,
        "contract": "orders(D) -> fills(D) -> ledger -> stats",
    }


def main() -> int:
    report = build_report()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = LOG_DIR / f"recovery_ssot_chain_{stamp}.json"
    latest = LOG_DIR / "recovery_ssot_chain_latest.json"
    text = json.dumps(report, ensure_ascii=False, indent=2)
    out.write_text(text, encoding="utf-8")
    latest.write_text(text, encoding="utf-8")
    print(f"[RECOVERY_SSOT] status={report['status']} D={report.get('D')} issues={len(report['issues'])}")
    print(f"[RECOVERY_SSOT] json={out}")
    print(f"[RECOVERY_SSOT] latest={latest}")
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
