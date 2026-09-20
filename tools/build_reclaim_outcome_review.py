from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
HISTORY_CSV = LOG_DIR / "reclaim_observe_sampler_history.csv"
OUT_JSON = LOG_DIR / "reclaim_outcome_review_latest.json"
OUT_CSV = LOG_DIR / "reclaim_outcome_review_latest.csv"
OUT_MD = LOG_DIR / "reclaim_outcome_review_latest.md"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _code(v: Any) -> str:
    return str(v or "").strip().zfill(6)


def _float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _ymd_from_ts(ts: str) -> str:
    digits = "".join(ch for ch in str(ts or "") if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _load_intraday_by_date(required_ymds: set[str]) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = {}
    for ymd in sorted(x for x in required_ymds if x):
        path = LOG_DIR / f"intraday_prices_history_{ymd}.csv"
        if not path.exists():
            continue
        out[ymd] = _read_csv(path)
    return out


def _price_field(row: dict[str, str]) -> float:
    for key in ("price", "current_price", "last_price", "close"):
        val = _float(row.get(key), float("nan"))
        if val == val and val > 0:
            return val
    return 0.0


def _ts_field(row: dict[str, str]) -> str:
    for key in ("ts", "timestamp", "datetime", "time"):
        val = str(row.get(key) or "").strip()
        if val:
            return val
    return ""


def _intraday_outcome(code: str, sampled_at: str, intraday_rows: list[dict[str, str]]) -> dict[str, Any]:
    rows = [r for r in intraday_rows if _code(r.get("code")) == code]
    if not rows:
        return {"status": "NO_INTRADAY_PRICE", "observations": 0}
    sampled_key = str(sampled_at or "")
    after = [r for r in rows if _ts_field(r) >= sampled_key] if sampled_key else rows
    use_rows = after or rows
    first = use_rows[0]
    last = use_rows[-1]
    first_px = _price_field(first)
    last_px = _price_field(last)
    prices = [_price_field(r) for r in use_rows]
    prices = [p for p in prices if p > 0]
    ret = ((last_px / first_px) - 1.0) * 100.0 if first_px > 0 and last_px > 0 else 0.0
    high_ret = ((max(prices) / first_px) - 1.0) * 100.0 if prices and first_px > 0 else 0.0
    low_ret = ((min(prices) / first_px) - 1.0) * 100.0 if prices and first_px > 0 else 0.0
    return {
        "status": "OBSERVED",
        "observations": len(use_rows),
        "first_ts": _ts_field(first),
        "last_ts": _ts_field(last),
        "first_price": first_px,
        "last_price": last_px,
        "return_first_to_last_pct": round(ret, 6),
        "intraday_high_from_first_pct": round(high_ret, 6),
        "intraday_low_from_first_pct": round(low_ret, 6),
    }


def _dedupe_samples(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, str]] = []
    for row in rows:
        key = (_code(row.get("code")), str(row.get("sampled_at") or ""), str(row.get("checklist_status") or ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _unique_policy_samples(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str, str]] = set()
    out: list[dict[str, str]] = []
    for row in rows:
        sampled_at = str(row.get("sampled_at") or "")
        key = (
            _code(row.get("code")),
            _ymd_from_ts(sampled_at),
            str(row.get("checklist_stage") or ""),
            str(row.get("checklist_status") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _build_rows(history_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    history = _dedupe_samples(history_rows)
    required_ymds = {_ymd_from_ts(str(row.get("sampled_at") or "")) for row in history}
    intraday = _load_intraday_by_date(required_ymds)
    out: list[dict[str, Any]] = []
    for row in history:
        sampled_at = str(row.get("sampled_at") or "")
        code = _code(row.get("code"))
        ymd = _ymd_from_ts(sampled_at)
        outcome = _intraday_outcome(code, sampled_at, intraday.get(ymd, []))
        out.append(
            {
                "sampled_at": sampled_at,
                "ymd": ymd,
                "code": code,
                "source_review_class": row.get("source_review_class"),
                "checklist_stage": row.get("checklist_stage"),
                "checklist_status": row.get("checklist_status"),
                "defense_signal_score": row.get("defense_signal_score"),
                "surge_active_response_label": row.get("surge_active_response_label"),
                "surge_entry_decision": row.get("surge_entry_decision"),
                "order_connection": row.get("order_connection"),
                "paper_order_route": row.get("paper_order_route"),
                "broker_order_route": row.get("broker_order_route"),
                "policy_change_allowed": row.get("policy_change_allowed"),
                **outcome,
            }
        )
    return out


FIELDS = [
    "sampled_at",
    "ymd",
    "code",
    "source_review_class",
    "checklist_stage",
    "checklist_status",
    "defense_signal_score",
    "surge_active_response_label",
    "surge_entry_decision",
    "order_connection",
    "paper_order_route",
    "broker_order_route",
    "policy_change_allowed",
    "status",
    "observations",
    "first_ts",
    "last_ts",
    "first_price",
    "last_price",
    "return_first_to_last_pct",
    "intraday_high_from_first_pct",
    "intraday_low_from_first_pct",
]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Reclaim Outcome Review",
        "",
        f"- generated_at: `{payload['generated_at']}`",
        f"- raw_history_rows: `{payload['summary']['raw_history_rows']}`",
        f"- rows: `{payload['summary']['rows']}`",
        f"- unique_policy_rows: `{payload['summary']['unique_policy_rows']}`",
        f"- unique_observed_rows: `{payload['summary']['unique_observed_rows']}`",
        f"- unique_watch_reclaim_rows: `{payload['summary']['unique_watch_reclaim_rows']}`",
        "",
        "## Checklist Status Counts",
    ]
    for k, v in sorted(payload["summary"]["checklist_status_counts"].items()):
        lines.append(f"- {k}: `{v}`")
    lines.extend(["", "## Guardrails"])
    lines.append("- This is outcome review only.")
    lines.append("- It does not approve orders or alter defense policy.")
    lines.append("- Unique policy rows exclude repeated manual sampler runs for the same code/date/stage/status.")
    lines.append("- Small sample counts are not policy evidence.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    history_rows = _read_csv(HISTORY_CSV)
    rows = _build_rows(history_rows)
    unique_rows = _build_rows(_unique_policy_samples(history_rows))
    observed = [r for r in rows if r.get("status") == "OBSERVED"]
    unique_observed = [r for r in unique_rows if r.get("status") == "OBSERVED"]
    returns = [_float(r.get("return_first_to_last_pct")) for r in observed]
    unique_returns = [_float(r.get("return_first_to_last_pct")) for r in unique_observed]
    status_counts = Counter(str(r.get("checklist_status") or "") for r in rows)
    unique_status_counts = Counter(str(r.get("checklist_status") or "") for r in unique_rows)
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "reclaim_outcome_review_v1",
        "source_files": {"history_csv": str(HISTORY_CSV), "intraday_history_glob": str(LOG_DIR / "intraday_prices_history_*.csv")},
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV), "md": str(OUT_MD)},
        "scope": {
            "policy_effect": "read_only_outcome_review_only",
            "full_logic_application": "NOT_APPLIED",
            "order_connection": "NONE",
        },
        "summary": {
            "raw_history_rows": len(history_rows),
            "rows": len(rows),
            "observed_rows": len(observed),
            "watch_reclaim_rows": sum(1 for r in rows if r.get("checklist_status") == "WATCH_RECLAIM"),
            "checklist_status_counts": dict(status_counts),
            "avg_return_first_to_last_pct": round(sum(returns) / len(returns), 6) if returns else None,
            "unique_policy_rows": len(unique_rows),
            "unique_observed_rows": len(unique_observed),
            "unique_watch_reclaim_rows": sum(1 for r in unique_rows if r.get("checklist_status") == "WATCH_RECLAIM"),
            "unique_checklist_status_counts": dict(unique_status_counts),
            "unique_avg_return_first_to_last_pct": round(sum(unique_returns) / len(unique_returns), 6) if unique_returns else None,
            "policy_change_applied": False,
            "paper_order_route": False,
            "broker_order_route": False,
        },
        "rows": rows,
        "unique_rows": unique_rows,
    }
    _write_csv(OUT_CSV, rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({"status": "PASS", "rows": len(rows), "observed_rows": len(observed), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
