from __future__ import annotations

import csv
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
VALIDATION_JSON = LOG_DIR / "backtest_validation_latest.json"
TRADES_CALC_CSV = ROOT / "paper" / "trades_calc.csv"
OUT_JSON = LOG_DIR / "backtest_acceptance_return_trade_attribution_latest.json"
OUT_CSV = LOG_DIR / "backtest_acceptance_return_trade_attribution_latest.csv"


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "date",
        "validation_return",
        "validation_loss",
        "entry_rows",
        "exit_rows",
        "entry_net_sum",
        "exit_net_sum",
        "entry_exit_net_sum",
        "attribution_alignment",
        "top_entry_codes",
        "top_exit_codes",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})
    tmp.replace(path)


def _float(value: Any, default: float = math.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _date8(value: Any) -> str:
    raw = str(value or "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _validation_returns(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    artifacts = doc.get("artifacts") if isinstance(doc.get("artifacts"), dict) else {}
    base_series = artifacts.get("base_series") if isinstance(artifacts.get("base_series"), dict) else {}
    rows = base_series.get("returns") if isinstance(base_series.get("returns"), list) else []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ymd = _date8(row.get("date") or row.get("timestamp"))
        ret = _float(row.get("return"))
        if ymd and math.isfinite(ret):
            out.append({"date": ymd, "return": ret})
    return out


def _recent_loss_dates(vrows: List[Dict[str, Any]], window: int = 10) -> List[str]:
    return [row["date"] for row in vrows[-int(window) :] if float(row["return"]) < 0.0]


def _net_ret(row: Dict[str, str]) -> float:
    for key in ("net_ret", "return", "ret", "gross_ret"):
        value = _float(row.get(key))
        if math.isfinite(value):
            return value
    return math.nan


def _sum_net(rows: List[Dict[str, str]]) -> float:
    vals = [_net_ret(row) for row in rows]
    vals = [value for value in vals if math.isfinite(value)]
    return float(sum(vals)) if vals else 0.0


def _counts(rows: List[Dict[str, str]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, "") or "").zfill(6) if key == "code" else str(row.get(key, "") or "")
        if not value:
            continue
        out[value] = out.get(value, 0) + 1
    return dict(sorted(out.items(), key=lambda kv: (-kv[1], kv[0])))


def _top(counts: Dict[str, int], n: int = 5) -> str:
    return ";".join(f"{key}:{value}" for key, value in list(counts.items())[:n])


def _alignment(validation_return: float, entry_sum: float, exit_sum: float, entry_rows: int, exit_rows: int) -> str:
    linked = entry_rows + exit_rows
    total = entry_sum + exit_sum
    if linked == 0:
        return "NO_PAPER_TRADE_LINK"
    if validation_return < 0.0 and total < 0.0:
        return "DIRECTION_MATCH_LOSS"
    if validation_return < 0.0 and total >= 0.0:
        return "VALIDATION_LOSS_BUT_TRADE_LINK_NONNEGATIVE"
    if validation_return >= 0.0 and total < 0.0:
        return "VALIDATION_NONNEGATIVE_BUT_TRADE_LINK_LOSS"
    return "DIRECTION_MATCH_NONNEGATIVE"


def _acceptance_gate(doc: Dict[str, Any]) -> Dict[str, Any]:
    for gate in doc.get("gate_results", []):
        if isinstance(gate, dict) and gate.get("name") == "acceptance_pnl_turnover":
            return gate
    return {}


def build_payload() -> Dict[str, Any]:
    doc = _read_json(VALIDATION_JSON)
    trades = _read_csv(TRADES_CALC_CSV)
    returns = _validation_returns(doc)
    targets = _recent_loss_dates(returns)
    ret_by_date = {row["date"]: float(row["return"]) for row in returns}
    rows: List[Dict[str, Any]] = []
    for ymd in targets:
        entry_rows = [row for row in trades if _date8(row.get("entry_ts")) == ymd]
        exit_rows = [row for row in trades if _date8(row.get("exit_ts")) == ymd]
        entry_sum = _sum_net(entry_rows)
        exit_sum = _sum_net(exit_rows)
        vret = ret_by_date.get(ymd, math.nan)
        rows.append(
            {
                "date": ymd,
                "validation_return": vret,
                "validation_loss": bool(math.isfinite(vret) and vret < 0.0),
                "entry_rows": len(entry_rows),
                "exit_rows": len(exit_rows),
                "entry_net_sum": entry_sum,
                "exit_net_sum": exit_sum,
                "entry_exit_net_sum": entry_sum + exit_sum,
                "attribution_alignment": _alignment(vret, entry_sum, exit_sum, len(entry_rows), len(exit_rows)),
                "top_entry_codes": _top(_counts(entry_rows, "code")),
                "top_exit_codes": _top(_counts(exit_rows, "code")),
            }
        )

    alignment_counts: Dict[str, int] = {}
    for row in rows:
        key = str(row.get("attribution_alignment", ""))
        alignment_counts[key] = alignment_counts.get(key, 0) + 1
    gate = _acceptance_gate(doc)
    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "status": "PASS",
        "scope": "read_only_validation_return_vs_paper_trade_link_attribution",
        "source_validation_json": str(VALIDATION_JSON),
        "source_trades_calc_csv": str(TRADES_CALC_CSV),
        "policy_change": False,
        "entry_approval_changed": False,
        "trading_effect": False,
        "acceptance_gate": gate,
        "recent_loss_dates": targets,
        "alignment_counts": dict(sorted(alignment_counts.items())),
        "rows": rows,
        "interpretation": {
            "primary_alignment": max(alignment_counts.items(), key=lambda kv: kv[1])[0] if alignment_counts else "",
            "boundary": "This separates attribution only; it does not change validation returns or paper trade records.",
            "next_evidence_need": "For rows with validation loss but nonnegative trade-link sum, inspect the validation backtest engine series source before changing guards.",
        },
    }
    return payload


def main() -> int:
    payload = build_payload()
    rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": payload["status"], "rows": len(rows), "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
