from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
LEDGER = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")
PNL_SUMMARY = LOG_DIR / "paper_pnl_summary_last.json"
OUTPUT = LOG_DIR / "sell_period_validation_report_latest.json"


def _now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _norm_ymd(value: Any) -> str:
    text = str(value or "").strip()
    digits = re.sub(r"\D", "", text)
    return digits[:8] if len(digits) >= 8 else ""


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _note_fields(note: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in re.split(r"[|;]", str(note or "")):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if key:
            out[key] = value.strip()
    return out


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_sell_rows(start_ymd: str, end_ymd: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not LEDGER.exists():
        return rows
    with LEDGER.open("r", encoding="utf-8-sig", newline="") as f:
        for raw in csv.DictReader(f):
            ymd = _norm_ymd(raw.get("date"))
            if not ymd or ymd < start_ymd or ymd > end_ymd:
                continue
            if str(raw.get("side") or "").strip().upper() != "SELL":
                continue
            note = _note_fields(raw.get("note"))
            reason = (note.get("exit_reason") or note.get("sell_tag") or "UNKNOWN").strip().upper()
            rows.append(
                {
                    "date": ymd,
                    "code": str(raw.get("code") or "").strip().zfill(6),
                    "reason": reason,
                    "partial_exit": str(note.get("partial_exit") or "").strip() in {"1", "true", "TRUE", "True"},
                    "sell_ratio_pct": _float(note.get("sell_ratio_pct"), 0.0),
                    "qty": _float(raw.get("fill_qty"), 0.0),
                    "notional": _float(raw.get("gross_notional_krw"), 0.0),
                    "realized_pnl": _float(raw.get("realized_pnl_krw"), 0.0),
                    "fee_krw": _float(raw.get("fee_krw"), 0.0),
                    "tax_krw": _float(raw.get("tax_krw"), 0.0),
                    "slippage_bps_model": _float(raw.get("slippage_bps_model"), 0.0),
                }
            )
    return rows


def _group_reason(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row["reason"])].append(row)
    out: list[dict[str, Any]] = []
    for reason, vals in groups.items():
        pnl = sum(_float(v.get("realized_pnl")) for v in vals)
        out.append(
            {
                "reason": reason,
                "rows": len(vals),
                "partial_rows": sum(1 for v in vals if bool(v.get("partial_exit"))),
                "notional_krw": round(sum(_float(v.get("notional")) for v in vals), 2),
                "realized_pnl_krw": round(pnl, 2),
                "avg_realized_pnl_krw": round(pnl / len(vals), 2) if vals else 0.0,
            }
        )
    return sorted(out, key=lambda x: (x["realized_pnl_krw"], x["reason"]))


def _group_date(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row["date"])].append(row)
    out: list[dict[str, Any]] = []
    for date, vals in sorted(groups.items()):
        reason_counts: dict[str, int] = defaultdict(int)
        for v in vals:
            reason_counts[str(v["reason"])] += 1
        out.append(
            {
                "date": date,
                "sell_rows": len(vals),
                "partial_rows": sum(1 for v in vals if bool(v.get("partial_exit"))),
                "realized_pnl_krw": round(sum(_float(v.get("realized_pnl")) for v in vals), 2),
                "reason_counts": dict(sorted(reason_counts.items())),
            }
        )
    return out


def _checklist(rows: list[dict[str, Any]], pnl_summary: dict[str, Any]) -> list[dict[str, Any]]:
    reasons = [str(r.get("reason") or "") for r in rows]
    stop_rows = [r for r in rows if str(r.get("reason") or "").startswith("STOP")]
    tp_rows = [r for r in rows if str(r.get("reason") or "").startswith("TP")]
    trail_rows = [r for r in rows if str(r.get("reason") or "").startswith("TRAIL")]
    gap_rows = [r for r in rows if "GAP" in str(r.get("reason") or "")]
    ddm_rows = [r for r in rows if str(r.get("reason") or "").startswith("DDM_LIQUIDATE")]
    partial_rows = [r for r in rows if bool(r.get("partial_exit"))]
    fees = sum(_float(r.get("fee_krw")) for r in rows)
    taxes = sum(_float(r.get("tax_krw")) for r in rows)
    avg_slip = sum(_float(r.get("slippage_bps_model")) for r in rows) / len(rows) if rows else 0.0
    mdd = _float((pnl_summary.get("equity") or {}).get("max_drawdown_pct"), 0.0)
    issues = (pnl_summary.get("meta") or {}).get("rows_validation") or {}
    lifecycle_ok = bool(issues.get("valid", False))
    return [
        {"item": "목표가 익절", "status": "PASS" if tp_rows else "WARN", "evidence": {"rows": len(tp_rows)}},
        {"item": "손절 손실제한", "status": "PASS" if stop_rows else "WARN", "evidence": {"rows": len(stop_rows)}},
        {"item": "부분 청산", "status": "PASS" if partial_rows else "WARN", "evidence": {"rows": len(partial_rows)}},
        {"item": "트레일링 스탑", "status": "PASS" if trail_rows else "WARN", "evidence": {"rows": len(trail_rows)}},
        {"item": "슬리피지 반영", "status": "PASS" if rows and avg_slip >= 0 else "WARN", "evidence": {"avg_bps": round(avg_slip, 4)}},
        {"item": "수수료 세금 반영", "status": "PASS" if rows and (fees > 0 or taxes > 0) else "WARN", "evidence": {"fee_krw": round(fees, 2), "tax_krw": round(taxes, 2)}},
        {"item": "비중 위험관리", "status": "WARN" if abs(mdd) >= 0.15 else "PASS", "evidence": {"max_drawdown_pct": mdd}},
        {"item": "동시 신호 우선순위", "status": "WARN", "evidence": {"unique_reasons": len(set(reasons)), "note": "기간 로그로 매도 사유는 확인되나 동시 발생 우선순위 기준은 별도 검증 필요"}},
        {"item": "갭/급락 예외처리", "status": "PASS" if gap_rows or ddm_rows else "WARN", "evidence": {"gap_rows": len(gap_rows), "ddm_rows": len(ddm_rows)}},
        {"item": "모니터링 로그", "status": "PASS" if lifecycle_ok else "WARN", "evidence": {"pnl_rows_validation": lifecycle_ok}},
    ]


def build_report(start_ymd: str, end_ymd: str) -> dict[str, Any]:
    pnl_summary = _load_json(PNL_SUMMARY)
    rows = _read_sell_rows(start_ymd, end_ymd)
    min_date = min((str(r["date"]) for r in rows), default="")
    max_date = max((str(r["date"]) for r in rows), default="")
    partial_count = sum(1 for r in rows if bool(r.get("partial_exit")))
    fee = sum(_float(r.get("fee_krw")) for r in rows)
    tax = sum(_float(r.get("tax_krw")) for r in rows)
    return {
        "generated_at": _now_ts(),
        "status": "PASS" if rows else "WARN",
        "period": {
            "requested_start_ymd": start_ymd,
            "requested_end_ymd": end_ymd,
            "actual_start_ymd": min_date,
            "actual_end_ymd": max_date,
        },
        "source": {
            "ledger": str(LEDGER),
            "pnl_summary": str(PNL_SUMMARY),
        },
        "counts": {
            "sell_rows": len(rows),
            "partial_sell_rows": partial_count,
            "full_sell_rows": len(rows) - partial_count,
        },
        "costs": {
            "fee_krw": round(fee, 2),
            "tax_krw": round(tax, 2),
            "avg_slippage_bps_model": round(sum(_float(r.get("slippage_bps_model")) for r in rows) / len(rows), 4) if rows else 0.0,
        },
        "pnl": {
            "realized_pnl_krw": round(sum(_float(r.get("realized_pnl")) for r in rows), 2),
            "max_drawdown_pct": _float((pnl_summary.get("equity") or {}).get("max_drawdown_pct"), 0.0),
            "dd_end_pct": _float((pnl_summary.get("equity") or {}).get("dd_end_pct"), 0.0),
            "avg_ret": _float(pnl_summary.get("avg_ret"), 0.0),
            "gross_pf": _float(pnl_summary.get("gross_pf"), 0.0),
        },
        "reason_summary": _group_reason(rows),
        "daily_summary": _group_date(rows),
        "checklist": _checklist(rows, pnl_summary),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument("--output", default=str(OUTPUT))
    args = parser.parse_args()

    pnl_summary = _load_json(PNL_SUMMARY)
    scope = ((pnl_summary.get("meta") or {}).get("operational_scope") or {})
    start_ymd = _norm_ymd(args.start) or _norm_ymd(scope.get("start_ymd")) or "00000000"
    end_ymd = _norm_ymd(args.end) or _norm_ymd(pnl_summary.get("as_of_ymd")) or _now_ts()[:10].replace("-", "")
    report = build_report(start_ymd, end_ymd)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[SELL_PERIOD] status={report['status']} sells={report['counts']['sell_rows']} output={out}")
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
