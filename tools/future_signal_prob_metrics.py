from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd


def _finite(values: Iterable[float]) -> List[float]:
    out: List[float] = []
    for value in values:
        try:
            v = float(value)
        except Exception:
            continue
        if math.isfinite(v):
            out.append(v)
    return out


def crps_ensemble(samples: Iterable[float], actual: float) -> float:
    xs = _finite(samples)
    y = float(actual)
    if not xs or not math.isfinite(y):
        return math.nan
    n = len(xs)
    term1 = sum(abs(x - y) for x in xs) / n
    pair_sum = 0.0
    for x in xs:
        for z in xs:
            pair_sum += abs(x - z)
    return float(term1 - (0.5 * pair_sum / (n * n)))


def pit_value(samples: Iterable[float], actual: float) -> float:
    xs = _finite(samples)
    y = float(actual)
    if not xs or not math.isfinite(y):
        return math.nan
    below = sum(1 for x in xs if x < y)
    equal = sum(1 for x in xs if x == y)
    return float((below + 0.5 * equal) / len(xs))


def ks_uniform_pvalue(values: Iterable[float]) -> Dict[str, float]:
    xs = sorted(x for x in _finite(values) if 0.0 <= x <= 1.0)
    n = len(xs)
    if n == 0:
        return {"n": 0.0, "statistic": math.nan, "p_value": math.nan}
    d_plus = max(((i + 1) / n) - x for i, x in enumerate(xs))
    d_minus = max(x - (i / n) for i, x in enumerate(xs))
    d = max(d_plus, d_minus)
    z = (math.sqrt(n) + 0.12 + 0.11 / math.sqrt(n)) * d
    p = 0.0
    for k in range(1, 101):
        p += ((-1) ** (k - 1)) * math.exp(-2.0 * (k * k) * (z * z))
    p = max(0.0, min(1.0, 2.0 * p))
    return {"n": float(n), "statistic": float(d), "p_value": float(p)}


def score_sample_frame(samples: pd.DataFrame, actuals: pd.DataFrame) -> Dict[str, Any]:
    required_samples = {"row_id", "sample_index", "step", "value"}
    required_actuals = {"row_id", "step", "actual"}
    missing_samples = sorted(required_samples - set(samples.columns))
    missing_actuals = sorted(required_actuals - set(actuals.columns))
    if missing_samples or missing_actuals:
        return {
            "status": "FAIL",
            "reason": "REQUIRED_COLUMN_MISSING",
            "missing_samples": missing_samples,
            "missing_actuals": missing_actuals,
        }

    work = samples.copy()
    truth = actuals.copy()
    work["row_id"] = work["row_id"].astype(str)
    truth["row_id"] = truth["row_id"].astype(str)
    work["step"] = pd.to_numeric(work["step"], errors="coerce")
    truth["step"] = pd.to_numeric(truth["step"], errors="coerce")
    work["value"] = pd.to_numeric(work["value"], errors="coerce")
    truth["actual"] = pd.to_numeric(truth["actual"], errors="coerce")

    rows: List[Dict[str, Any]] = []
    for (row_id, step), g in work.dropna(subset=["step", "value"]).groupby(["row_id", "step"], dropna=False):
        t = truth[(truth["row_id"] == str(row_id)) & (truth["step"] == step)]
        if len(t) == 0:
            continue
        actual = float(t.iloc[0]["actual"])
        sample_values = g["value"].tolist()
        rows.append(
            {
                "row_id": str(row_id),
                "step": int(step),
                "sample_count": int(len(sample_values)),
                "actual": actual,
                "crps": crps_ensemble(sample_values, actual),
                "pit": pit_value(sample_values, actual),
            }
        )

    scored = pd.DataFrame(rows)
    if len(scored) == 0:
        return {"status": "FAIL", "reason": "SCORABLE_ROWS_ZERO", "rows": 0}

    crps = pd.to_numeric(scored["crps"], errors="coerce")
    pits = pd.to_numeric(scored["pit"], errors="coerce")
    ks = ks_uniform_pvalue(pits.dropna().tolist())
    return {
        "status": "PASS" if crps.notna().any() and math.isfinite(float(ks.get("p_value", math.nan))) else "FAIL",
        "reason": "ok",
        "rows": int(len(scored)),
        "mean_crps": round(float(crps.mean()), 10),
        "pit_ks": {
            "n": int(ks["n"]),
            "statistic": round(float(ks["statistic"]), 10),
            "p_value": round(float(ks["p_value"]), 10),
        },
        "scored_rows": rows,
    }


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def main() -> int:
    ap = argparse.ArgumentParser(description="Score probabilistic future-signal samples with CRPS and PIT KS.")
    ap.add_argument("--samples-csv", required=True)
    ap.add_argument("--actuals-csv", required=True)
    ap.add_argument("--output-json", required=True)
    args = ap.parse_args()

    payload = score_sample_frame(_read_csv(Path(args.samples_csv)), _read_csv(Path(args.actuals_csv)))
    out = Path(args.output_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"[FUTURE_PROB_METRICS] status={payload.get('status')} reason={payload.get('reason')} rows={payload.get('rows', 0)}")
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
