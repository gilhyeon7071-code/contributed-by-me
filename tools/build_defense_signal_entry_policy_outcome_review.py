from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

IMPACT_CSV = LOG_DIR / "defense_signal_entry_policy_impact_latest.csv"
INTRADAY_HISTORY_CSV = LOG_DIR / "intraday_prices_history_20260709.csv"
RUN_LOGS = [
    LOG_DIR / "run_paper_daily_last.txt",
    LOG_DIR / "run_paper_daily_hidden_last.txt",
    LOG_DIR / "run_intraday_paper_last.txt",
]
OUT_JSON = LOG_DIR / "defense_signal_entry_policy_outcome_review_latest.json"
OUT_CSV = LOG_DIR / "defense_signal_entry_policy_outcome_review_latest.csv"


APPLIED_RE = re.compile(
    r"defense_signal_entry_policy applied:\s+"
    r"(?P<before>\d+)->(?P<after>\d+)\s+matched=(?P<matched>\d+)\s+blocked=(?P<blocked>\d+)"
)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _to_float(value: Any) -> float | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _norm_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def _scan_log_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in RUN_LOGS:
        if not path.exists():
            continue
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue
        for i, line in enumerate(lines, start=1):
            m = APPLIED_RE.search(line)
            if not m:
                continue
            event = {
                "path": str(path),
                "line": i,
                "before": int(m.group("before")),
                "after": int(m.group("after")),
                "matched": int(m.group("matched")),
                "blocked": int(m.group("blocked")),
                "raw": line.strip(),
            }
            events.append(event)
    return events


def _intraday_by_code() -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in _read_csv(INTRADAY_HISTORY_CSV):
        code = _norm_code(row.get("code"))
        price = _to_float(row.get("current_price"))
        if not code or price is None or price <= 0:
            continue
        out.setdefault(code, []).append(
            {
                "ts": row.get("ts") or "",
                "price": price,
                "open": _to_float(row.get("open")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "volume": _to_float(row.get("volume")),
            }
        )
    for rows in out.values():
        rows.sort(key=lambda r: str(r.get("ts") or ""))
    return out


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "code",
        "name",
        "route_scope",
        "in_current_candidates",
        "would_block_surge",
        "defense_signal_score",
        "observation_status",
        "first_ts",
        "last_ts",
        "first_price",
        "last_price",
        "return_first_to_last_pct",
        "return_open_to_last_pct",
        "intraday_high_from_first_pct",
        "intraday_low_from_first_pct",
        "active_policy_result",
        "observe_only_result",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _pct(new: float | None, old: float | None) -> float | None:
    if new is None or old is None or old == 0:
        return None
    return round((new / old - 1.0) * 100.0, 6)


def main() -> int:
    impact_rows = [
        row for row in _read_csv(IMPACT_CSV)
        if str(row.get("scope") or "") == "defense_shadow_potential"
    ]
    price_by_code = _intraday_by_code()
    out_rows: list[dict[str, Any]] = []
    for row in impact_rows:
        code = _norm_code(row.get("code"))
        points = price_by_code.get(code, [])
        if not points:
            out_rows.append(
                {
                    "code": code,
                    "name": row.get("name") or "",
                    "route_scope": row.get("route_scope") or "",
                    "in_current_candidates": row.get("in_current_candidates") or "",
                    "would_block_surge": row.get("would_block_surge") or "",
                    "defense_signal_score": row.get("defense_signal_score") or "",
                    "observation_status": "NO_INTRADAY_PRICE_OBSERVED",
                    "active_policy_result": row.get("active_policy_result") or "",
                    "observe_only_result": row.get("observe_only_result") or "",
                }
            )
            continue
        first = points[0]
        last = points[-1]
        high = max((_to_float(p.get("high")) or p["price"] for p in points), default=first["price"])
        low = min((_to_float(p.get("low")) or p["price"] for p in points), default=first["price"])
        open_price = first.get("open")
        out_rows.append(
            {
                "code": code,
                "name": row.get("name") or "",
                "route_scope": row.get("route_scope") or "",
                "in_current_candidates": row.get("in_current_candidates") or "",
                "would_block_surge": row.get("would_block_surge") or "",
                "defense_signal_score": row.get("defense_signal_score") or "",
                "observation_status": "INTRADAY_PRICE_OBSERVED",
                "first_ts": first["ts"],
                "last_ts": last["ts"],
                "first_price": first["price"],
                "last_price": last["price"],
                "return_first_to_last_pct": _pct(last["price"], first["price"]),
                "return_open_to_last_pct": _pct(last["price"], open_price),
                "intraday_high_from_first_pct": _pct(high, first["price"]),
                "intraday_low_from_first_pct": _pct(low, first["price"]),
                "active_policy_result": row.get("active_policy_result") or "",
                "observe_only_result": row.get("observe_only_result") or "",
            }
        )

    log_events = _scan_log_events()
    event_counts = Counter(str(event["blocked"]) for event in log_events)
    observed_returns = [
        _to_float(row.get("return_first_to_last_pct"))
        for row in out_rows
        if row.get("observation_status") == "INTRADAY_PRICE_OBSERVED"
    ]
    observed_returns = [x for x in observed_returns if x is not None]
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "defense_signal_entry_policy_outcome_review_v1",
        "source_files": {
            "impact_csv": str(IMPACT_CSV),
            "intraday_history_csv": str(INTRADAY_HISTORY_CSV),
            "logs": [str(p) for p in RUN_LOGS],
        },
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "log_scan": {
            "events": len(log_events),
            "blocked_sum": sum(int(event["blocked"]) for event in log_events),
            "blocked_count_distribution": dict(event_counts),
            "events_detail": log_events[-20:],
            "history_limit": "only currently retained run logs were scanned",
        },
        "potential_outcomes": {
            "rows": len(out_rows),
            "intraday_observed_rows": sum(1 for row in out_rows if row.get("observation_status") == "INTRADAY_PRICE_OBSERVED"),
            "no_intraday_price_rows": sum(1 for row in out_rows if row.get("observation_status") == "NO_INTRADAY_PRICE_OBSERVED"),
            "avg_return_first_to_last_pct": round(sum(observed_returns) / len(observed_returns), 6) if observed_returns else None,
            "positive_rows": sum(1 for x in observed_returns if x > 0),
            "negative_rows": sum(1 for x in observed_returns if x < 0),
        },
        "effect_scope": {
            "score_effect": False,
            "order_dispatch_effect": False,
            "entry_pool_filter_effect": True,
            "policy_change_applied": False,
        },
        "interpretation": {
            "actual_current_log_blocks": "blocked_sum counts retained log lines only; validation synthetic block is included in logs and must not be treated as real candidate blocking",
            "outcome_basis": "intraday observed rows use first-to-last observed price in intraday_prices_history_20260709.csv",
        },
    }
    _write_csv(OUT_CSV, out_rows)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "PASS", "rows": len(out_rows), "out_json": str(OUT_JSON)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
