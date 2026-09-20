from __future__ import annotations

import csv
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
VALIDATION_JSON = LOG_DIR / "backtest_validation_latest.json"
RECENT_LOSS_CSV = LOG_DIR / "backtest_acceptance_recent_loss_review_latest.csv"
LOSS_BREAKDOWN_JSON = LOG_DIR / "normal_intraday_realtime_loss_breakdown_latest.json"
OUT_JSON = LOG_DIR / "backtest_acceptance_intraday_shadow_whatif_latest.json"
OUT_CSV = LOG_DIR / "backtest_acceptance_intraday_shadow_whatif_latest.csv"


def _now_ts() -> str:
    return dt.datetime.now().replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
        except Exception:
            return {}
    return {}


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as fh:
                return list(csv.DictReader(fh))
        except UnicodeDecodeError:
            continue
    return []


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "scenario",
        "kind",
        "affected_dates",
        "affected_n",
        "baseline_low",
        "ci95_low",
        "ci95_med",
        "ci95_high",
        "negative_bootstrap_rate",
        "would_pass",
        "policy_change_applied",
        "threshold_relaxation_applied",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _finite_float(value: Any, default: float = math.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _returns_from_validation(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    artifacts = doc.get("artifacts") if isinstance(doc.get("artifacts"), dict) else {}
    base_series = artifacts.get("base_series") if isinstance(artifacts.get("base_series"), dict) else {}
    rows = base_series.get("returns") if isinstance(base_series.get("returns"), list) else []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ret = _finite_float(row.get("return"))
        date = str(row.get("date") or row.get("timestamp") or "")
        if len(date) >= 8 and math.isfinite(ret):
            out.append({"date": date[:8], "return": ret})
    return out


def _bootstrap_totals(returns: Iterable[float], *, n_boot: int = 1200, block_size: int = 21) -> np.ndarray:
    rr = np.asarray([float(v) for v in returns if math.isfinite(float(v))], dtype=float)
    n = len(rr)
    if n == 0:
        return np.asarray([], dtype=float)
    if n == 1:
        return np.asarray([float((1.0 + rr[0]) - 1.0)], dtype=float)
    bs = max(2, min(int(block_size), n))
    rng = np.random.default_rng(42)
    totals: List[float] = []
    for _ in range(max(100, int(n_boot))):
        chunks: List[np.ndarray] = []
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


def _ci95(returns: Iterable[float]) -> Dict[str, float]:
    totals = _bootstrap_totals(returns)
    if len(totals) == 0:
        return {"low": math.nan, "med": math.nan, "high": math.nan, "negative_bootstrap_rate": math.nan}
    return {
        "low": float(np.percentile(totals, 2.5)),
        "med": float(np.percentile(totals, 50.0)),
        "high": float(np.percentile(totals, 97.5)),
        "negative_bootstrap_rate": float(np.mean(totals <= 0.0)),
    }


def _recent_intraday_split_loss_dates(rows: List[Dict[str, str]]) -> List[str]:
    out: List[str] = []
    for row in rows:
        date = str(row.get("date") or "")[:8]
        if not date:
            continue
        if _truthy(row.get("has_intraday_realtime")) and _truthy(row.get("has_split_entry")):
            out.append(date)
    return sorted(set(out))


def _apply_scenario(base_rows: List[Dict[str, Any]], affected_dates: set[str], mode: str, factor: float) -> List[float]:
    out: List[float] = []
    for row in base_rows:
        date = str(row["date"])
        ret = float(row["return"])
        if date in affected_dates and ret < 0.0:
            if mode == "zero":
                ret = 0.0
            elif mode == "scale_loss":
                ret = ret * float(factor)
        out.append(float(ret))
    return out


def build_payload() -> Dict[str, Any]:
    validation = _read_json(VALIDATION_JSON)
    base_rows = _returns_from_validation(validation)
    recent_rows = _read_csv(RECENT_LOSS_CSV)
    loss_breakdown = _read_json(LOSS_BREAKDOWN_JSON)
    affected = _recent_intraday_split_loss_dates(recent_rows)
    base_returns = [float(r["return"]) for r in base_rows]
    baseline = _ci95(base_returns)

    scenarios = [
        ("reduce_intraday_split_loss_days_25pct", "scale_loss", 0.75),
        ("reduce_intraday_split_loss_days_50pct", "scale_loss", 0.50),
        ("reduce_intraday_split_loss_days_75pct", "scale_loss", 0.25),
        ("block_intraday_split_loss_days", "zero", 0.0),
    ]
    rows: List[Dict[str, Any]] = []
    for name, mode, factor in scenarios:
        adjusted = _apply_scenario(base_rows, set(affected), mode, factor)
        ci = _ci95(adjusted)
        rows.append(
            {
                "scenario": name,
                "kind": "read_only_shadow_whatif",
                "affected_dates": ",".join(affected),
                "affected_n": len(affected),
                "baseline_low": baseline.get("low"),
                "ci95_low": ci.get("low"),
                "ci95_med": ci.get("med"),
                "ci95_high": ci.get("high"),
                "negative_bootstrap_rate": ci.get("negative_bootstrap_rate"),
                "would_pass": bool(math.isfinite(float(ci.get("low", math.nan))) and float(ci["low"]) > 0.0),
                "policy_change_applied": False,
                "threshold_relaxation_applied": False,
            }
        )

    return {
        "generated_at": _now_ts(),
        "status": "OK" if base_rows and affected else "NO_EVIDENCE",
        "scope": "read_only_acceptance_intraday_shadow_whatif",
        "source_files": {
            "validation": str(VALIDATION_JSON),
            "recent_loss_review": str(RECENT_LOSS_CSV),
            "normal_intraday_loss_breakdown": str(LOSS_BREAKDOWN_JSON),
        },
        "baseline": {
            "n_days": len(base_rows),
            **baseline,
        },
        "affected_dates": affected,
        "normal_intraday_realtime_metrics": loss_breakdown.get("metrics", {}) if isinstance(loss_breakdown, dict) else {},
        "scenarios": rows,
        "implemented_changes": {
            "policy_change_applied": False,
            "threshold_relaxation_applied": False,
            "entry_approval_changed": False,
            "order_fill_ledger_changed": False,
        },
        "interpretation": {
            "usage": "Evidence for whether a future policy change is worth testing; not an operational approval.",
            "limitation": "Daily returns are adjusted at loss-day level, not by replaying the full order engine.",
        },
    }


def main() -> int:
    payload = build_payload()
    rows = payload.get("scenarios") if isinstance(payload.get("scenarios"), list) else []
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"status": payload.get("status"), "rows": len(rows), "json": str(OUT_JSON), "csv": str(OUT_CSV)}, ensure_ascii=False))
    return 0 if payload.get("status") == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
