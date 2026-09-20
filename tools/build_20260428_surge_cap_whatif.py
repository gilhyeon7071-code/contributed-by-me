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
QUALITY_JSON = LOG_DIR / "surge_stop_gap_quality_20260428_latest.json"
OUT_JSON = LOG_DIR / "surge_cap_whatif_20260428_latest.json"
OUT_CSV = LOG_DIR / "surge_cap_whatif_20260428_latest.csv"
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


def adjusted(base_rows: list[dict[str, float | str]], removed_share: float) -> list[float]:
    share = max(0.0, min(1.0, float(removed_share)))
    out: list[float] = []
    for row in base_rows:
        ret = float(row["return"])
        if str(row["date"]) == TARGET_YMD and ret < 0:
            ret *= 1.0 - share
        out.append(ret)
    return out


def scenario(name: str, cap_mult: float | None, current_mult: float, surge_loss_weight: float, base_rows: list[dict[str, float | str]], baseline_low: float) -> dict:
    if cap_mult is None:
        loss_reduction_within_surge = 1.0
    else:
        retained = min(1.0, max(0.0, cap_mult / current_mult)) if current_mult > 0 else 1.0
        loss_reduction_within_surge = 1.0 - retained
    removed_share = max(0.0, min(1.0, surge_loss_weight * loss_reduction_within_surge))
    ci = ci95(adjusted(base_rows, removed_share))
    return {
        "scenario": name,
        "target_ymd": TARGET_YMD,
        "current_sector_hrp_mult": current_mult,
        "candidate_sector_hrp_mult": cap_mult if cap_mult is not None else 0.0,
        "loss_reduction_within_surge": loss_reduction_within_surge,
        "removed_20260428_loss_share": removed_share,
        "baseline_low": baseline_low,
        "ci95_low": ci["low"],
        "ci95_med": ci["med"],
        "ci95_high": ci["high"],
        "negative_bootstrap_rate": ci["negative_bootstrap_rate"],
        "would_pass": bool(math.isfinite(ci["low"]) and ci["low"] > 0.0),
        "policy_change_applied": False,
        "official_backtest": False,
    }


def write_csv(path: Path, rows: list[dict]) -> None:
    fields = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    validation = read_json(VALIDATION_JSON)
    quality = read_json(QUALITY_JSON)
    qrows = quality.get("rows") if isinstance(quality.get("rows"), list) else []
    base_rows = validation_returns(validation)
    baseline = ci95([float(r["return"]) for r in base_rows])
    target_ret = next((float(r["return"]) for r in base_rows if r["date"] == TARGET_YMD), math.nan)

    current_mults = [finite(r.get("sector_hrp_mult")) for r in qrows if math.isfinite(finite(r.get("sector_hrp_mult")))]
    current_mult = min(current_mults) if current_mults else 0.35
    surge_trace_loss = sum(abs(finite(r.get("trace_net_ret_sum"), 0.0)) for r in qrows)
    total_20260428_loss_trace = surge_trace_loss
    # The prior split diagnostic showed non-surge 20260428 losses too; keep this
    # screen focused on how much of the date loss the surge axis can remove.
    split_path = LOG_DIR / "entry_split_driver_20260428_latest.json"
    split = read_json(split_path)
    split_rows = split.get("rows") if isinstance(split.get("rows"), list) else []
    all_trace_loss = sum(abs(finite(r.get("net_ret_sum"), 0.0)) for r in split_rows if finite(r.get("net_ret_sum"), 0.0) < 0.0)
    surge_loss_weight = total_20260428_loss_trace / all_trace_loss if all_trace_loss > 0 else 0.0

    rows = [
        scenario("surge_cap_to_0_25", 0.25, current_mult, surge_loss_weight, base_rows, baseline["low"]),
        scenario("surge_cap_to_0_15", 0.15, current_mult, surge_loss_weight, base_rows, baseline["low"]),
        scenario("surge_cap_to_0_10", 0.10, current_mult, surge_loss_weight, base_rows, baseline["low"]),
        scenario("block_20260428_surge_stop_gap_entries", None, current_mult, surge_loss_weight, base_rows, baseline["low"]),
    ]

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "PASS" if base_rows and len(qrows) == 2 else "WARN",
        "scope": "read_only_20260428_surge_cap_whatif",
        "method_note": "Date-return proportional screen, not an official backtest rerun.",
        "inputs": {
            "validation": str(VALIDATION_JSON),
            "quality": str(QUALITY_JSON),
            "split": str(split_path),
        },
        "baseline": {
            "n_days": len(base_rows),
            "target_ymd": TARGET_YMD,
            "target_validation_return": target_ret,
            **baseline,
        },
        "surge_loss_weight_in_20260428_split": surge_loss_weight,
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(OUT_CSV, rows)
    print(json.dumps({"status": payload["status"], "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
