from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TARGET_YMD = "20260428"
TARGET_CODES = {"006360", "322000", "109080", "036200"}


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def to_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def sum_float(rows: list[dict[str, str]], key: str) -> float:
    total = 0.0
    for row in rows:
        value = to_float(row.get(key))
        if value is not None:
            total += value
    return total


def linked_trades(trades: list[dict[str, str]], trace_id: str, code: str, entry_ymd: str) -> list[dict[str, str]]:
    out = []
    entry_date = f"{entry_ymd[:4]}-{entry_ymd[4:6]}-{entry_ymd[6:]}"
    for row in trades:
        note = row.get("note", "")
        if trace_id and trace_id in note:
            out.append(row)
            continue
        if row.get("code") == code and str(row.get("entry_ts", "")).startswith(entry_date):
            out.append(row)
    return out


def linked_ledger(ledger: list[dict[str, str]], trace_id: str, code: str) -> list[dict[str, str]]:
    keys = ("entry_trace_id", "source_trace_id", "trace_id")
    return [
        row
        for row in ledger
        if row.get("code") == code and any(row.get(k) == trace_id for k in keys)
    ]


def classify(row: dict[str, str], context: dict[str, str] | None) -> dict[str, object]:
    entry_class = row.get("entry_class", "")
    exit_reasons = row.get("exit_reasons", "")
    risk_flags = (context or {}).get("risk_flags", "")

    if entry_class == "surge_entry" and "STOP_GAP" in exit_reasons:
        return {
            "driver_group": "SURGE_STOP_GAP",
            "current_policy_bug": False,
            "classification": "policy_candidate_surge_stop_gap_or_entry_cap",
            "why": "급등 진입 후 STOP_GAP 손실이다. 일반 intraday REDUCE 차단 문제와 분리해야 한다.",
            "next_check": "급등 진입 전 품질, 진입 규모, STOP_GAP 잔존 위험을 별도 what-if로 검증한다.",
        }

    if entry_class == "normal_intraday_realtime" and "p1_entry_gate_not_allow" in risk_flags:
        return {
            "driver_group": "NORMAL_P1_NOT_ALLOW_REDUCE",
            "current_policy_bug": False,
            "classification": "policy_change_required_to_block_reduce",
            "why": "현재 계약은 BLOCK만 차단하고 REDUCE는 허용한다. 이 손실은 버그라기보다 REDUCE 차단 정책 후보이다.",
            "next_check": "p1_entry_gate_not_allow + REDUCE 일반 intraday만 read-only 차단 what-if로 검증한다.",
        }

    return {
        "driver_group": "UNCLASSIFIED",
        "current_policy_bug": None,
        "classification": "needs_manual_review",
        "why": "현재 규칙으로 자동 분류되지 않았다.",
        "next_check": "trace/context/ledger 연결을 수동 확인한다.",
    }


def main() -> int:
    trace_path = LOG_DIR / "entry_trace_performance_latest.csv"
    context_path = LOG_DIR / "normal_intraday_entry_context_latest.csv"
    trades_path = ROOT / "paper" / "trades_calc.csv"
    ledger_path = Path(r"E:\vibe\buffett\data\ledger\paper_fills_ledger.csv")

    traces = read_csv(trace_path)
    contexts = read_csv(context_path)
    trades = read_csv(trades_path)
    ledger = read_csv(ledger_path)

    context_by_trace = {row.get("entry_trace_id", ""): row for row in contexts}
    rows: list[dict[str, object]] = []

    for row in traces:
        if row.get("entry_ymd") != TARGET_YMD or row.get("code") not in TARGET_CODES:
            continue
        trace_id = row.get("entry_trace_id", "")
        code = row.get("code", "")
        context = context_by_trace.get(trace_id)
        paper_rows = linked_trades(trades, trace_id, code, TARGET_YMD)
        ledger_rows = linked_ledger(ledger, trace_id, code)
        classification = classify(row, context)

        rows.append(
            {
                "entry_trace_id": trace_id,
                "code": code,
                "entry_ts": row.get("entry_ts", ""),
                "entry_class": row.get("entry_class", ""),
                "horizon": row.get("horizon", ""),
                "split_entry": row.get("split_entry", ""),
                "surge_immediate": row.get("surge_immediate", ""),
                "entry_qty": to_float(row.get("entry_qty")),
                "entry_price": to_float(row.get("entry_price")),
                "entry_notional": to_float(row.get("entry_notional")),
                "net_ret_sum": to_float(row.get("net_ret_sum")),
                "exit_reasons": row.get("exit_reasons", ""),
                "last_exit_ymd": row.get("last_exit_ymd", ""),
                "p1_entry_gate_decision_before_p1": (context or {}).get("p1_entry_gate_decision_before_p1", ""),
                "risk_flags": (context or {}).get("risk_flags", ""),
                "weak_context": (context or {}).get("weak_context", ""),
                "paper_trade_rows": len(paper_rows),
                "paper_net_ret_sum": sum_float(paper_rows, "net_ret"),
                "ledger_rows": len(ledger_rows),
                "ledger_buy_rows": sum(1 for r in ledger_rows if r.get("side") == "BUY"),
                "ledger_sell_rows": sum(1 for r in ledger_rows if r.get("side") == "SELL"),
                "ledger_realized_pnl_sum": sum_float(ledger_rows, "realized_pnl_krw"),
                **classification,
            }
        )

    rows.sort(key=lambda r: str(r.get("entry_ts", "")))
    by_group: dict[str, dict[str, object]] = {}
    for row in rows:
        group = str(row.get("driver_group", "UNKNOWN"))
        item = by_group.setdefault(group, {"entries": 0, "net_ret_sum": 0.0, "ledger_realized_pnl_sum": 0.0})
        item["entries"] = int(item["entries"]) + 1
        item["net_ret_sum"] = float(item["net_ret_sum"]) + float(row.get("net_ret_sum") or 0.0)
        item["ledger_realized_pnl_sum"] = float(item["ledger_realized_pnl_sum"]) + float(row.get("ledger_realized_pnl_sum") or 0.0)

    output = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if len(rows) == 4 else "WARN",
        "target_ymd": TARGET_YMD,
        "target_codes": sorted(TARGET_CODES),
        "inputs": {
            "trace": str(trace_path),
            "context": str(context_path),
            "trades": str(trades_path),
            "ledger": str(ledger_path),
        },
        "summary": {
            "target_entries": len(rows),
            "by_group": by_group,
            "current_policy_bug_rows": sum(1 for r in rows if r.get("current_policy_bug") is True),
            "policy_candidate_rows": sum(1 for r in rows if str(r.get("classification", "")).startswith("policy_")),
        },
        "rows": rows,
    }

    json_path = LOG_DIR / "entry_split_driver_20260428_latest.json"
    csv_path = LOG_DIR / "entry_split_driver_20260428_latest.csv"
    json_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    if rows:
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(json.dumps({"status": output["status"], "json": str(json_path), "csv": str(csv_path)}, ensure_ascii=False))
    return 0 if output["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
