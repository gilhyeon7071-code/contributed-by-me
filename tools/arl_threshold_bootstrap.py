from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
LATEST_JSON = LOG_DIR / "arl_threshold_bootstrap_latest.json"
LATEST_CSV = LOG_DIR / "arl_threshold_bootstrap_latest.csv"


def _now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _parse_float_csv(raw: str) -> List[float]:
    vals: List[float] = []
    for part in str(raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        vals.append(float(part))
    return vals


def _parse_targets(raw: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for part in str(raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"invalid target item: {part}")
        name, value = part.split(":", 1)
        out[str(name).strip()] = float(value)
    return out


def _parse_limits(raw: str) -> List[float]:
    text = str(raw or "").strip()
    if ":" in text and "," not in text:
        start_s, stop_s, step_s = text.split(":", 2)
        start, stop, step = float(start_s), float(stop_s), float(step_s)
        if step <= 0:
            raise ValueError("limit step must be positive")
        vals = []
        x = start
        while x <= stop + (step / 10.0):
            vals.append(round(x, 10))
            x += step
        return vals
    return _parse_float_csv(text)


def _apply_normal_filter(
    df: pd.DataFrame,
    *,
    filter_col: Optional[str],
    filter_value: Optional[str],
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if not filter_col:
        return df, {"enabled": False}
    if filter_col not in df.columns:
        raise ValueError(f"normal filter column not found: {filter_col}")
    keep = df[filter_col].astype(str) == str(filter_value)
    return df.loc[keep].copy(), {
        "enabled": True,
        "column": filter_col,
        "value": str(filter_value),
        "kept_rows": int(keep.sum()),
    }


def _standardized_values(df: pd.DataFrame, value_col: str, symbol_col: Optional[str]) -> np.ndarray:
    if value_col not in df.columns:
        raise ValueError(f"value column not found: {value_col}")
    work = df.copy()
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=[value_col])
    if work.empty:
        return np.array([], dtype=float)

    if symbol_col and symbol_col in work.columns:
        pieces: List[pd.Series] = []
        for _, group in work.groupby(symbol_col, sort=False):
            vals = pd.to_numeric(group[value_col], errors="coerce").dropna()
            if len(vals) < 3:
                continue
            sd = float(vals.std(ddof=1))
            if sd <= 0 or math.isnan(sd):
                continue
            pieces.append((vals - float(vals.mean())) / sd)
        if not pieces:
            return np.array([], dtype=float)
        z = pd.concat(pieces, ignore_index=True)
    else:
        vals = pd.to_numeric(work[value_col], errors="coerce").dropna()
        sd = float(vals.std(ddof=1))
        if sd <= 0 or math.isnan(sd):
            return np.array([], dtype=float)
        z = (vals - float(vals.mean())) / sd

    z = z.replace([np.inf, -np.inf], np.nan).dropna()
    return z.to_numpy(dtype=float)


def _block_bootstrap(values: np.ndarray, *, block_size: int, series_length: int, rng: np.random.Generator) -> np.ndarray:
    n = len(values)
    if n == 0:
        return np.array([], dtype=float)
    if n <= block_size:
        sampled = rng.choice(values, size=series_length, replace=True)
        return np.asarray(sampled, dtype=float)

    starts = rng.integers(0, n, size=int(math.ceil(series_length / block_size)))
    chunks = []
    for start in starts:
        idx = (np.arange(start, start + block_size) % n).astype(int)
        chunks.append(values[idx])
    return np.concatenate(chunks)[:series_length].astype(float)


def _ewma_run_length(values: np.ndarray, *, lam: float, limit: float) -> Tuple[int, bool]:
    z = 0.0
    for i, x in enumerate(values, start=1):
        z = lam * float(x) + (1.0 - lam) * z
        sigma = math.sqrt((lam / (2.0 - lam)) * (1.0 - (1.0 - lam) ** (2 * i)))
        if sigma > 0 and abs(z) / sigma >= limit:
            return i, True
    return len(values), False


def _summarize_runs(runs: List[int], alarms: List[bool], *, bars_per_day: float, window_minutes: float) -> Dict[str, Any]:
    run_arr = np.asarray(runs, dtype=float)
    alarm_arr = np.asarray(alarms, dtype=bool)
    observed_bars = float(run_arr.mean()) if len(run_arr) else 0.0
    return {
        "observed_arl_bars": round(observed_bars, 6),
        "observed_arl_minutes": round(observed_bars * window_minutes, 6),
        "observed_arl_trading_days": round(observed_bars / bars_per_day, 6) if bars_per_day > 0 else None,
        "alarm_rate": round(float(alarm_arr.mean()), 6) if len(alarm_arr) else 0.0,
        "censored_rate": round(1.0 - float(alarm_arr.mean()), 6) if len(alarm_arr) else 1.0,
        "p50_run_bars": round(float(np.percentile(run_arr, 50)), 6) if len(run_arr) else 0.0,
        "p10_run_bars": round(float(np.percentile(run_arr, 10)), 6) if len(run_arr) else 0.0,
    }


def estimate_ewma_arl(
    values: np.ndarray,
    *,
    lambdas: Iterable[float],
    limits: Iterable[float],
    bootstrap_runs: int,
    block_size: int,
    series_length: int,
    bars_per_day: float,
    window_minutes: float,
    seed: int,
) -> List[Dict[str, Any]]:
    rng = np.random.default_rng(seed)
    rows: List[Dict[str, Any]] = []
    for lam in lambdas:
        if not (0 < lam <= 1):
            raise ValueError(f"lambda must be in (0, 1]: {lam}")
        sampled_series = [
            _block_bootstrap(values, block_size=block_size, series_length=series_length, rng=rng)
            for _ in range(bootstrap_runs)
        ]
        for limit in limits:
            runs: List[int] = []
            alarms: List[bool] = []
            for sample in sampled_series:
                rl, alarmed = _ewma_run_length(sample, lam=lam, limit=limit)
                runs.append(int(rl))
                alarms.append(bool(alarmed))
            rows.append(
                {
                    "method": "EWMA",
                    "lambda": float(lam),
                    "limit": float(limit),
                    **_summarize_runs(runs, alarms, bars_per_day=bars_per_day, window_minutes=window_minutes),
                }
            )
    return rows


def _recommendations(rows: List[Dict[str, Any]], targets: Dict[str, float]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for label, target in targets.items():
        label_rows = []
        for row in rows:
            if float(row["observed_arl_bars"]) >= float(target):
                label_rows.append(row)
        by_lambda: Dict[str, Any] = {}
        for lam in sorted({float(r["lambda"]) for r in rows}):
            candidates = [r for r in label_rows if float(r["lambda"]) == lam]
            if not candidates:
                by_lambda[str(lam)] = None
                continue
            best = sorted(candidates, key=lambda r: float(r["limit"]))[0]
            by_lambda[str(lam)] = {
                "limit": best["limit"],
                "observed_arl_bars": best["observed_arl_bars"],
                "observed_arl_minutes": best["observed_arl_minutes"],
                "observed_arl_trading_days": best["observed_arl_trading_days"],
                "alarm_rate": best["alarm_rate"],
                "censored_rate": best["censored_rate"],
            }
        out[label] = {"target_arl_bars": float(target), "by_lambda": by_lambda}
    return out


def run(args: argparse.Namespace) -> Dict[str, Any]:
    input_path = Path(args.input)
    output_json = Path(args.output_json) if args.output_json else LATEST_JSON
    output_csv = Path(args.output_csv) if args.output_csv else LATEST_CSV

    payload_base: Dict[str, Any] = {
        "generated_at": _now_ts(),
        "input": str(input_path),
        "outputs": {"json": str(output_json), "csv": str(output_csv), "latest_json": str(LATEST_JSON), "latest_csv": str(LATEST_CSV)},
        "policy": {
            "read_only_for_trading": True,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
            "stop_meaning_modified": False,
        },
    }

    if not input_path.exists():
        payload = {**payload_base, "status": "FAIL", "reason": "INPUT_MISSING"}
        _write_json(output_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    df = _read_csv(input_path)
    filtered, normal_filter = _apply_normal_filter(df, filter_col=args.normal_filter_col, filter_value=args.normal_filter_value)
    values = _standardized_values(filtered, args.value_col, args.symbol_col)
    targets = _parse_targets(args.targets)
    lambdas = _parse_float_csv(args.lambdas)
    limits = _parse_limits(args.limits)

    if len(values) < max(10, int(args.block_size)):
        payload = {
            **payload_base,
            "status": "FAIL",
            "reason": "INSUFFICIENT_NORMAL_ROWS",
            "row_count": int(len(df)),
            "normal_row_count": int(len(filtered)),
            "usable_value_count": int(len(values)),
            "normal_filter": normal_filter,
        }
        _write_json(output_json, payload)
        _write_json(LATEST_JSON, payload)
        return payload

    rows = estimate_ewma_arl(
        values,
        lambdas=lambdas,
        limits=limits,
        bootstrap_runs=int(args.bootstrap_runs),
        block_size=int(args.block_size),
        series_length=int(args.series_length),
        bars_per_day=float(args.bars_per_day),
        window_minutes=float(args.window_minutes),
        seed=int(args.seed),
    )
    table = pd.DataFrame(rows)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(output_csv, index=False, encoding="utf-8-sig")
    if output_csv != LATEST_CSV:
        table.to_csv(LATEST_CSV, index=False, encoding="utf-8-sig")

    payload = {
        **payload_base,
        "status": "PASS",
        "reason": "EWMA_ARL_BOOTSTRAP_ESTIMATED",
        "row_count": int(len(df)),
        "normal_row_count": int(len(filtered)),
        "usable_value_count": int(len(values)),
        "normal_filter": normal_filter,
        "settings": {
            "method": "EWMA",
            "window_minutes": float(args.window_minutes),
            "bars_per_day": float(args.bars_per_day),
            "bootstrap_runs": int(args.bootstrap_runs),
            "block_size": int(args.block_size),
            "series_length": int(args.series_length),
            "lambdas": [float(x) for x in lambdas],
            "limits": [float(x) for x in limits],
            "targets_arl_bars": targets,
            "seed": int(args.seed),
        },
        "recommendations": _recommendations(rows, targets),
    }
    _write_json(output_json, payload)
    if output_json != LATEST_JSON:
        _write_json(LATEST_JSON, payload)
    return payload


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Estimate EWMA threshold to ARL mapping with block bootstrap.")
    p.add_argument("--input", required=True, help="CSV containing normal-window signal values.")
    p.add_argument("--value-col", required=True, help="Numeric signal column to standardize and monitor.")
    p.add_argument("--symbol-col", default=None, help="Optional symbol/code column for per-symbol standardization.")
    p.add_argument("--normal-filter-col", default=None, help="Optional column used to keep normal rows only.")
    p.add_argument("--normal-filter-value", default=None, help="Value required in --normal-filter-col.")
    p.add_argument("--window-minutes", type=float, default=1.0, help="Minutes represented by one bar.")
    p.add_argument("--bars-per-day", type=float, default=390.0, help="Trading bars per day for ARL day conversion.")
    p.add_argument("--targets", default="soft:120,warning:300,critical:500", help="Target ARL bars by gate label.")
    p.add_argument("--lambdas", default="0.1,0.2,0.3", help="Comma-separated EWMA lambdas.")
    p.add_argument("--limits", default="2.5:4.0:0.1", help="Comma list or start:stop:step control limits.")
    p.add_argument("--bootstrap-runs", type=int, default=500)
    p.add_argument("--block-size", type=int, default=30)
    p.add_argument("--series-length", type=int, default=390)
    p.add_argument("--seed", type=int, default=20260502)
    p.add_argument("--output-json", default=None)
    p.add_argument("--output-csv", default=None)
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    payload = run(args)
    print(json.dumps({"status": payload.get("status"), "reason": payload.get("reason"), "json": payload["outputs"]["json"], "csv": payload["outputs"]["csv"]}, ensure_ascii=True))
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
