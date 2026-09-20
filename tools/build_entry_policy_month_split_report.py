"""Build a read-only month split report for entry policy groups.

The report compares realized `trades_calc` outcomes for April, May, and the
combined April+May window. It does not change policy, orders, fills, or ledger.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCE_CSV = LOG_DIR / "entry_policy_group_report_latest.csv"
OUT_JSON = LOG_DIR / "entry_policy_month_split_report_latest.json"
OUT_CSV = LOG_DIR / "entry_policy_month_split_report_latest.csv"

KST = dt.timezone(dt.timedelta(hours=9))
WINDOWS = {
    "202604": {"start": "20260401", "end": "20260430"},
    "202605": {"start": "20260501", "end": "20260531"},
    "202604_202605": {"start": "20260401", "end": "20260531"},
}


def _now() -> str:
    return dt.datetime.now(tz=KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _f(value: Any) -> float | None:
    try:
        text = str(value).strip()
        if text == "":
            return None
        return float(text)
    except Exception:
        return None


def _ymd(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text[:8] if len(text) >= 8 else ""


def _stats(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    items = list(rows)
    vals = [float(row["net_ret"]) for row in items if row.get("net_ret") is not None]
    pos = sum(1 for v in vals if v > 0)
    neg = sum(1 for v in vals if v < 0)
    zero = sum(1 for v in vals if v == 0)
    return {
        "n": len(items),
        "ret_n": len(vals),
        "avg_net_ret": round(sum(vals) / len(vals), 8) if vals else None,
        "median_net_ret": round(float(median(vals)), 8) if vals else None,
        "win_rate": round(pos / len(vals), 8) if vals else None,
        "positive_n": pos,
        "negative_n": neg,
        "zero_n": zero,
        "min_net_ret": round(min(vals), 8) if vals else None,
        "max_net_ret": round(max(vals), 8) if vals else None,
    }


def _group(rows: List[Dict[str, Any]], field: str) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get(field) or "UNKNOWN")
        groups.setdefault(key, []).append(row)
    return [{"group": key, **_stats(items)} for key, items in sorted(groups.items())]


def _filter_window(rows: List[Dict[str, Any]], start: str, end: str) -> List[Dict[str, Any]]:
    return [row for row in rows if start <= str(row.get("ymd") or "") <= end]


def _load_realized_rows() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in _read_csv(SOURCE_CSV):
        if str(row.get("source") or "") != "trades_calc":
            continue
        ymd = _ymd(row.get("ymd"))
        ret = _f(row.get("net_ret"))
        if not ymd or ret is None:
            continue
        out.append(
            {
                "ymd": ymd,
                "code": str(row.get("code") or "").zfill(6)[-6:],
                "order_id": str(row.get("order_id") or ""),
                "entry_policy_group": str(row.get("entry_policy_group") or "UNKNOWN"),
                "sample_eligibility": str(row.get("sample_eligibility") or "UNKNOWN"),
                "entry_timing": str(row.get("entry_timing") or ""),
                "surge_immediate": str(row.get("surge_immediate") or ""),
                "promotion_candidate": str(row.get("promotion_candidate") or ""),
                "label_reason": str(row.get("label_reason") or ""),
                "net_ret": ret,
            }
        )
    return out


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "window",
        "section",
        "group",
        "n",
        "ret_n",
        "avg_net_ret",
        "median_net_ret",
        "win_rate",
        "positive_n",
        "negative_n",
        "zero_n",
        "min_net_ret",
        "max_net_ret",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    rows = _load_realized_rows()
    window_payload: Dict[str, Any] = {}
    flat: List[Dict[str, Any]] = []
    for name, bounds in WINDOWS.items():
        wr = _filter_window(rows, bounds["start"], bounds["end"])
        sections = {
            "overall": [{"group": "ALL", **_stats(wr)}],
            "entry_policy_group": _group(wr, "entry_policy_group"),
            "sample_eligibility": _group(wr, "sample_eligibility"),
            "entry_timing": _group(wr, "entry_timing"),
            "promotion_candidate": _group(wr, "promotion_candidate"),
        }
        window_payload[name] = {
            "start": bounds["start"],
            "end": bounds["end"],
            "sections": sections,
        }
        for section, section_rows in sections.items():
            for row in section_rows:
                flat.append({"window": name, "section": section, **row})

    payload = {
        "generated_at": _now(),
        "schema_version": "entry_policy_month_split_report_v1",
        "trading_effect": False,
        "policy_effect": False,
        "source_csv": str(SOURCE_CSV),
        "windows": window_payload,
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "interpretation_hint": {
            "basis": "realized trades_calc rows from entry_policy_group_report_latest.csv",
            "net_ret_unit": "fractional return, e.g. 0.01 means +1%",
        },
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, flat)
    print(json.dumps({"status": "OK", "windows": list(WINDOWS.keys()), "out_json": str(OUT_JSON), "out_csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
