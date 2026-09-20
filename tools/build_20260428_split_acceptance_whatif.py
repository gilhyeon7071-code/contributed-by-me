from __future__ import annotations

import csv
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
VALIDATION_JSON = LOG_DIR / "backtest_validation_latest.json"
SPLIT_JSON = LOG_DIR / "entry_split_driver_20260428_latest.json"
OUT_JSON = LOG_DIR / "entry_split_acceptance_whatif_20260428_latest.json"
OUT_CSV = LOG_DIR / "entry_split_acceptance_whatif_20260428_latest.csv"
TARGET_YMD = "20260428"


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
    return {}


def finite(value: object, default: float = math.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def validation_returns(doc: dict) -> list[dict[str, float | str]]:
    artifacts = doc.get("artifacts") if isinstance(doc.get("artifacts"), dict) else {}
    base_series = artifacts.get("base_series") if isinstance(artifacts.get("base_series"), dict) else {}
    rows = base_series.get("returns") if isinstance(base_series.get("returns"), list) else []
    out: list[dict[str, float | str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        date = str(row.get("date") or row.get("timestamp") or "")[:8]
        ret = finite(row.get("return"))
        if len(date) == 8 and math.isfinite(ret):
            out.append({"date": date, "return": ret})
    return out


def bootstrap_totals(returns: Iterable[float], *, n_boot: int = 1200, block_size: int = 21) -> np.ndarray:
    rr = np.asarray([float(v) for v in returns if math.isfinite(float(v))], dtype=float)
    n = len(rr)
    if n == 0:
        return np.asarray([], dtype=float)
    if n == 1:
        return np.asarray([float((1.0 + rr[0]) - 1.0)], dtype=float)
    bs = max(2, min(int(block_size), n))
    rng = np.random.default_rng(42)
    totals: list[float] = []
    for _ in range(max(100, int(n_boot))):
        chunks: list[np.ndarray] = []
        while sum(len(c) for c in chunks) < n:
            start = int(rng.integers(0, n))
            end = start + bs
            if end <= n:
                block = rr[start:end]
            else:
                block = np.concatenate([rr[start:n], rr[0 : end - n]])
            chunks.append(block)
        sample = np.concatenate(chunks)[:n]
        totals.append(float(np.prod(1.0 + sample) - 1.0))
    return np.asarray(totals, dtype=float)


def ci95(returns: Iterable[float]) -> dict[str, float]:
    totals = bootstrap_totals(returns)
    if len(totals) == 0:
        return {"low": math.nan, "med": math.nan, "high": math.nan, "negative_bootstrap_rate": math.nan}
    return {
        "low": float(np.percentile(totals, 2.5)),
        "med": float(np.percentile(totals, 50.0)),
        "high": float(np.percentile(totals, 97.5)),
        "negative_bootstrap_rate": float(np.mean(totals <= 0.0)),
    }


def group_weights(rows: list[dict], value_key: str) -> dict[str, float]:
    losses: dict[str, float] = {}
    for row in rows:
        group = str(row.get("driver_group") or "UNKNOWN")
        value = finite(row.get(value_key), 0.0)
        loss = abs(value) if value < 0.0 else 0.0
        losses[group] = losses.get(group, 0.0) + loss
    total = sum(losses.values())
    if total <= 0.0:
        return {k: 0.0 for k in losses}
    return {k: v / total for k, v in losses.items()}


def adjusted_returns(base_rows: list[dict[str, float | str]], target_share: float) -> list[float]:
    out: list[float] = []
    share = max(0.0, min(1.0, float(target_share)))
    for row in base_rows:
        date = str(row["date"])
        ret = float(row["return"])
        if date == TARGET_YMD and ret < 0.0:
            ret = ret * (1.0 - share)
        out.append(ret)
    return out


def scenario_row(name: str, method: str, group: str, share: float, base_rows: list[dict[str, float | str]], baseline_low: float) -> dict:
    ci = ci95(adjusted_returns(base_rows, share))
    return {
        "scenario": name,
        "method": method,
        "target_ymd": TARGET_YMD,
        "group": group,
        "removed_loss_share": share,
        "baseline_low": baseline_low,
        "ci95_low": ci["low"],
        "ci95_med": ci["med"],
        "ci95_high": ci["high"],
        "negative_bootstrap_rate": ci["negative_bootstrap_rate"],
        "would_pass": bool(math.isfinite(ci["low"]) and ci["low"] > 0.0),
        "policy_change_applied": False,
        "threshold_relaxation_applied": False,
        "official_backtest": False,
    }


def write_csv(path: Path, rows: list[dict]) -> None:
    fields = [
        "scenario",
        "method",
        "target_ymd",
        "group",
        "removed_loss_share",
        "baseline_low",
        "ci95_low",
        "ci95_med",
        "ci95_high",
        "negative_bootstrap_rate",
        "would_pass",
        "policy_change_applied",
        "threshold_relaxation_applied",
        "official_backtest",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    validation = read_json(VALIDATION_JSON)
    split = read_json(SPLIT_JSON)
    base_rows = validation_returns(validation)
    split_rows = split.get("rows") if isinstance(split.get("rows"), list) else []
    base_return = next((float(r["return"]) for r in base_rows if r["date"] == TARGET_YMD), math.nan)
    baseline = ci95([float(r["return"]) for r in base_rows])
    baseline_low = baseline["low"]

    trace_weights = group_weights(split_rows, "net_ret_sum")
    ledger_weights = group_weights(split_rows, "ledger_realized_pnl_sum")
    groups = sorted(set(trace_weights) | set(ledger_weights))

    rows: list[dict] = []
    for method, weights in (("trace_net_ret_weight", trace_weights), ("ledger_realized_pnl_weight", ledger_weights)):
        for group in groups:
            share = weights.get(group, 0.0)
            rows.append(scenario_row(f"remove_{group.lower()}_{method}", method, group, share, base_rows, baseline_low))
        rows.append(scenario_row(f"remove_all_20260428_target_groups_{method}", method, "ALL_TARGET_GROUPS", 1.0, base_rows, baseline_low))

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if base_rows and split_rows and len(split_rows) == 4 else "WARN",
        "scope": "read_only_20260428_split_acceptance_whatif",
        "method_note": "This is a date-return proportional what-if, not an official backtest rerun.",
        "inputs": {
            "validation": str(VALIDATION_JSON),
            "split": str(SPLIT_JSON),
        },
        "baseline": {
            "n_days": len(base_rows),
            "target_ymd": TARGET_YMD,
            "target_validation_return": base_return,
            **baseline,
        },
        "weights": {
            "trace_net_ret_weight": trace_weights,
            "ledger_realized_pnl_weight": ledger_weights,
        },
        "scenarios": rows,
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(OUT_CSV, rows)
    print(json.dumps({"status": payload["status"], "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
