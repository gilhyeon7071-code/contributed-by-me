from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

import pandas as pd


ROOT = Path("E:/1_Data")
LOGS = ROOT / "2_Logs"
DEFAULT_CROSS_D = LOGS / "future_signal_cross_d_validation_latest.csv"
LATEST_JSON = LOGS / "future_signal_overlay_search_latest.json"
LATEST_CSV = LOGS / "future_signal_overlay_search_latest.csv"


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype={"code": str})
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype={"code": str})


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _norm_date8(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text[:8] if len(text) >= 8 else ""


def _norm_code(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6) if text else ""


def _load_candidate_scores() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in sorted(LOGS.glob("candidates_v41_1_*.csv")):
        m = re.search(r"(\d{8})$", path.stem)
        d = m.group(1) if m else ""
        if not d:
            continue
        try:
            df = _read_csv(path)
        except Exception:
            continue
        if "code" not in df.columns:
            continue
        df = df.copy()
        df["D"] = d
        df["code"] = df["code"].map(_norm_code)
        keep = [c for c in ["D", "code", "score", "final_score", "horizon_label", "candidate_origin"] if c in df.columns]
        frames.append(df[keep])
    if not frames:
        return pd.DataFrame(columns=["D", "code"])
    out = pd.concat(frames, ignore_index=True)
    for col in ("score", "final_score"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _prepare(cross_d_csv: Path) -> pd.DataFrame:
    raw = _read_csv(cross_d_csv)
    required = {"D", "code", "horizon_type", "pred_up_prob", "expected_return", "confidence", "realized_return", "actual_up"}
    missing = sorted(required - set(raw.columns))
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    hmap = {"D0": "INTRADAY", "D2": "SHORT", "D5": "SWING", "D15": "MID", "D30": "LONG"}
    raw = raw.copy()
    raw["D"] = raw["D"].map(_norm_date8)
    raw["code"] = raw["code"].map(_norm_code)
    raw["horizon_type_norm"] = raw["horizon_type"].astype(str).str.upper().replace(hmap)
    for col in ("pred_up_prob", "expected_return", "confidence", "realized_return", "actual_up"):
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    raw["abstain_bool"] = raw.get("abstain", False)
    raw["abstain_bool"] = raw["abstain_bool"].astype(str).str.lower().isin({"true", "1", "yes", "y"})

    candidates = _load_candidate_scores()
    work = raw.merge(candidates, on=["D", "code"], how="left", suffixes=("", "_candidate"))
    work = work.dropna(subset=["D", "code", "horizon_type_norm", "pred_up_prob", "realized_return", "actual_up"]).copy()
    work["pred_rank_pct"] = work.groupby(["D", "horizon_type_norm"])["pred_up_prob"].rank(pct=True, method="average")
    if "final_score" in work.columns:
        work["final_rank_pct"] = work.groupby("D")["final_score"].rank(pct=True, method="average")
    else:
        work["final_rank_pct"] = math.nan
    return work


def _metrics(df: pd.DataFrame) -> Dict[str, Any]:
    if len(df) == 0:
        return {
            "rows": 0,
            "unique_D": 0,
            "hit_rate": None,
            "mean_return": None,
            "median_return": None,
            "q10_return": None,
            "q90_return": None,
            "max_loss": None,
        }
    ret = pd.to_numeric(df["realized_return"], errors="coerce")
    actual = pd.to_numeric(df["actual_up"], errors="coerce")
    return {
        "rows": int(len(df)),
        "unique_D": int(df["D"].nunique()),
        "hit_rate": round(float(actual.mean()), 6),
        "mean_return": round(float(ret.mean()), 6),
        "median_return": round(float(ret.median()), 6),
        "q10_return": round(float(ret.quantile(0.10)), 6),
        "q90_return": round(float(ret.quantile(0.90)), 6),
        "max_loss": round(float(ret.min()), 6),
    }


def _condition_specs() -> List[tuple[str, Callable[[pd.DataFrame], pd.Series]]]:
    specs: List[tuple[str, Callable[[pd.DataFrame], pd.Series]]] = []
    for top in (0.10, 0.20, 0.30, 0.40):
        cutoff = 1.0 - top
        specs.append((f"pred_top{int(top * 100)}", lambda x, cutoff=cutoff: x["pred_rank_pct"] >= cutoff))
        specs.append(
            (
                f"pred_top{int(top * 100)}_non_abstain",
                lambda x, cutoff=cutoff: (x["pred_rank_pct"] >= cutoff) & (~x["abstain_bool"]),
            )
        )
        specs.append(
            (
                f"pred_top{int(top * 100)}_final_top30",
                lambda x, cutoff=cutoff: (x["pred_rank_pct"] >= cutoff) & (x["final_rank_pct"] >= 0.70),
            )
        )
    for pred_min in (0.45, 0.47, 0.50, 0.52):
        specs.append((f"pred_min_{pred_min:.2f}", lambda x, pred_min=pred_min: x["pred_up_prob"] >= pred_min))
        specs.append(
            (
                f"pred_min_{pred_min:.2f}_non_abstain",
                lambda x, pred_min=pred_min: (x["pred_up_prob"] >= pred_min) & (~x["abstain_bool"]),
            )
        )
    for er_min in (0.0, -0.0025, -0.005):
        label = str(er_min).replace("-", "m").replace(".", "p")
        specs.append(
            (
                f"expected_return_ge_{label}_non_abstain",
                lambda x, er_min=er_min: (x["expected_return"] >= er_min) & (~x["abstain_bool"]),
            )
        )
    specs.append(("final_top30", lambda x: x["final_rank_pct"] >= 0.70))
    specs.append(("all_realized", lambda x: pd.Series(True, index=x.index)))
    return specs


def _grade(row: Dict[str, Any], baseline: Dict[str, Any], final_top30: Dict[str, Any]) -> str:
    rows = int(row.get("rows") or 0)
    unique_d = int(row.get("unique_D") or 0)
    mean_return = row.get("mean_return")
    q10_return = row.get("q10_return")
    base_mean = baseline.get("mean_return")
    final_mean = final_top30.get("mean_return")
    if rows < 20 or unique_d < 5:
        return "INSUFFICIENT_SAMPLE"
    if mean_return is None or q10_return is None or base_mean is None:
        return "INSUFFICIENT_METRIC"
    if mean_return > 0 and mean_return > float(base_mean) and q10_return > -0.12:
        return "SHADOW_PROMISING"
    if final_mean is not None and mean_return > float(final_mean) and mean_return > float(base_mean):
        return "SHADOW_IMPROVES_BUT_RISKY"
    if mean_return > float(base_mean):
        return "SHADOW_WEAK_IMPROVEMENT"
    return "DISCARD_OR_WAIT"


def build_overlay_search(cross_d_csv: Path) -> Dict[str, Any]:
    work = _prepare(cross_d_csv)
    rows: List[Dict[str, Any]] = []
    specs = _condition_specs()
    horizons = sorted(work["horizon_type_norm"].dropna().unique().tolist())
    for horizon in horizons + ["ALL_REALIZED_HORIZONS"]:
        sub = work if horizon == "ALL_REALIZED_HORIZONS" else work[work["horizon_type_norm"] == horizon]
        baseline = _metrics(sub)
        final_top30 = _metrics(sub[sub["final_rank_pct"] >= 0.70])
        for name, fn in specs:
            mask = fn(sub).fillna(False)
            met = _metrics(sub[mask])
            met.update(
                {
                    "horizon": horizon,
                    "condition": name,
                    "baseline_rows": baseline["rows"],
                    "baseline_mean_return": baseline["mean_return"],
                    "final_top30_mean_return": final_top30["mean_return"],
                    "delta_mean_vs_baseline": None
                    if met["mean_return"] is None or baseline["mean_return"] is None
                    else round(float(met["mean_return"]) - float(baseline["mean_return"]), 6),
                    "delta_mean_vs_final_top30": None
                    if met["mean_return"] is None or final_top30["mean_return"] is None
                    else round(float(met["mean_return"]) - float(final_top30["mean_return"]), 6),
                }
            )
            met["grade"] = _grade(met, baseline, final_top30)
            rows.append(met)

    report = pd.DataFrame(rows)
    _write_csv(LATEST_CSV, report)
    eligible = report[report["grade"].isin(["SHADOW_PROMISING", "SHADOW_IMPROVES_BUT_RISKY", "SHADOW_WEAK_IMPROVEMENT"])].copy()
    if len(eligible):
        eligible = eligible.sort_values(["grade", "mean_return", "rows"], ascending=[True, False, False])
    best = eligible.head(20).to_dict("records")
    payload = {
        "status": "PASS",
        "reason": "ok",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": str(cross_d_csv),
        "rows_realized": int(len(work)),
        "unique_D": int(work["D"].nunique()),
        "horizons": horizons,
        "best_conditions": best,
        "policy": {
            "shadow_only": True,
            "used_for_trading": False,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
            "score_modified": False,
            "threshold_applied": False,
        },
        "method": {
            "baseline": "all realized rows by horizon",
            "rank_scope": "pred_up_prob percentile by D+horizon; final_score percentile by D",
            "costs": "not included",
            "activation": "not approved; read-only search only",
        },
        "outputs": {"json": str(LATEST_JSON), "csv": str(LATEST_CSV)},
    }
    _write_json(LATEST_JSON, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Search read-only future-signal overlay conditions against realized shadow outcomes.")
    ap.add_argument("--cross-d-csv", default=str(DEFAULT_CROSS_D))
    args = ap.parse_args()
    payload = build_overlay_search(Path(args.cross_d_csv))
    print(
        "[FUTURE_OVERLAY_SEARCH] "
        f"status={payload.get('status')} reason={payload.get('reason')} "
        f"rows_realized={payload.get('rows_realized', 0)} best={len(payload.get('best_conditions') or [])}"
    )
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
