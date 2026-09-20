from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
OUT_LATEST_JSON = LOG_DIR / "intraday_covariance_preavg_latest.json"
OUT_LATEST_CSV = LOG_DIR / "intraday_covariance_preavg_latest.csv"
CANDIDATES_FINAL = LOG_DIR / "candidates_latest_data.with_final_score.csv"
SECTOR_HISTORY = LOG_DIR / "sector_score_history.csv"


def _now() -> datetime:
    return datetime.now()


def _norm_code(v: Any) -> str:
    digits = "".join(ch for ch in str(v or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _read_csv(path: Path, **kwargs: Any) -> pd.DataFrame:
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:
        return pd.DataFrame()


def _load_sector_map() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for path in (CANDIDATES_FINAL, SECTOR_HISTORY):
        if not path.exists():
            continue
        df = _read_csv(path, dtype={"code": str})
        if df.empty or "code" not in df.columns:
            continue
        sector_col = "krx_sector" if "krx_sector" in df.columns else ("sector" if "sector" in df.columns else "")
        if not sector_col:
            continue
        if "date8" in df.columns:
            df = df.sort_values(["code", "date8"])
        for _, row in df.iterrows():
            code = _norm_code(row.get("code"))
            sec = str(row.get(sector_col) or "").strip()
            if code and sec and sec.lower() != "nan":
                out[code] = sec
    return out


def _load_ticks(days_back: int) -> Tuple[pd.DataFrame, List[str]]:
    files = sorted(LOG_DIR.glob("kis_quote_ticks_*.csv"))
    if days_back > 0:
        files = files[-days_back:]
    frames: List[pd.DataFrame] = []
    used: List[str] = []
    for path in files:
        df = _read_csv(path, dtype={"code": str})
        if df.empty or not {"ts", "code", "price"}.issubset(df.columns):
            continue
        df = df.copy()
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        df["code"] = df["code"].map(_norm_code)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = df[(df["ts"].notna()) & (df["code"] != "") & (df["price"] > 0)]
        if df.empty:
            continue
        frames.append(df[["ts", "code", "price"]])
        used.append(str(path))
    if not frames:
        return pd.DataFrame(columns=["ts", "code", "price"]), used
    return pd.concat(frames, ignore_index=True).sort_values(["ts", "code"]), used


def _kernel(k: int) -> np.ndarray:
    if k <= 1:
        return np.array([1.0], dtype="float64")
    x = np.arange(1, k + 1, dtype="float64") / float(k + 1)
    g = np.minimum(x, 1.0 - x)
    s = float(g.sum())
    return g / s if s > 0 else np.ones(k, dtype="float64") / float(k)


def _preaverage_returns(price_df: pd.DataFrame, k: int) -> pd.DataFrame:
    logp = np.log(price_df.replace(0, np.nan))
    ret = logp.diff()
    weights = _kernel(k)
    out = ret.rolling(window=max(1, k), min_periods=max(1, k)).apply(
        lambda x: float(np.dot(x, weights)), raw=True
    )
    return out.dropna(how="all")


def _cov_for_scale(ticks: pd.DataFrame, scale_seconds: int, k: int, min_obs: int) -> Tuple[pd.DataFrame, int]:
    if ticks.empty:
        return pd.DataFrame(), 0
    scale = max(1, int(scale_seconds))
    tmp = ticks.copy()
    tmp["bucket"] = tmp["ts"].dt.floor(f"{scale}s")
    px = tmp.sort_values("ts").groupby(["bucket", "code"], as_index=False)["price"].last()
    mat = px.pivot(index="bucket", columns="code", values="price").sort_index()
    pre = _preaverage_returns(mat, k)
    pre = pre.dropna(axis=1, thresh=max(2, min_obs))
    if pre.shape[0] < min_obs or pre.shape[1] < 2:
        return pd.DataFrame(), int(pre.shape[0])
    return pre.cov().fillna(0.0), int(pre.shape[0])


def _combine_covariances(covs: List[pd.DataFrame]) -> pd.DataFrame:
    valid = [c for c in covs if isinstance(c, pd.DataFrame) and not c.empty]
    if not valid:
        return pd.DataFrame()
    labels = sorted(set().union(*[set(c.columns.astype(str)) for c in valid]))
    acc = pd.DataFrame(0.0, index=labels, columns=labels)
    n = 0
    for cov in valid:
        c = cov.copy()
        c.columns = [str(x) for x in c.columns]
        c.index = [str(x) for x in c.index]
        acc = acc.add(c.reindex(index=labels, columns=labels).fillna(0.0), fill_value=0.0)
        n += 1
    return (acc / max(n, 1)).fillna(0.0)


def _psd_clip(cov: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if cov.empty:
        return cov, {"applied": False, "min_eigen_before": None, "min_eigen_after": None}
    labels = list(cov.columns)
    arr = cov.reindex(index=labels, columns=labels).fillna(0.0).to_numpy(dtype="float64")
    arr = (arr + arr.T) / 2.0
    vals, vecs = np.linalg.eigh(arr)
    min_before = float(vals.min()) if len(vals) else 0.0
    vals2 = np.clip(vals, 0.0, None)
    arr2 = (vecs @ np.diag(vals2) @ vecs.T)
    arr2 = (arr2 + arr2.T) / 2.0
    min_after = float(np.linalg.eigvalsh(arr2).min()) if arr2.size else 0.0
    return pd.DataFrame(arr2, index=labels, columns=labels), {
        "applied": bool(min_before < -1e-12),
        "min_eigen_before": min_before,
        "min_eigen_after": min_after,
    }


def _to_sector_cov(code_cov: pd.DataFrame, sector_map: Dict[str, str]) -> Tuple[pd.DataFrame, str, int]:
    if code_cov.empty:
        return pd.DataFrame(), "none", 0
    code_to_group = {c: sector_map.get(_norm_code(c), "") for c in code_cov.columns}
    mapped = {c: s for c, s in code_to_group.items() if s}
    if len(set(mapped.values())) >= 2:
        groups = sorted(set(mapped.values()))
        sec_cov = pd.DataFrame(0.0, index=groups, columns=groups)
        for a in groups:
            ac = [c for c, s in mapped.items() if s == a]
            for b in groups:
                bc = [c for c, s in mapped.items() if s == b]
                sec_cov.loc[a, b] = float(code_cov.reindex(index=ac, columns=bc).mean().mean())
        return sec_cov.fillna(0.0), "sector", len(mapped)
    return code_cov.copy(), "code_proxy", len(mapped)


def _write_matrix_csv(path: Path, cov: pd.DataFrame, level: str) -> None:
    rows: List[Dict[str, Any]] = []
    if not cov.empty:
        for a in cov.index:
            for b in cov.columns:
                rows.append({"level": level, "asset_a": str(a), "asset_b": str(b), "covariance": float(cov.loc[a, b])})
    pd.DataFrame(rows, columns=["level", "asset_a", "asset_b", "covariance"]).to_csv(path, index=False, encoding="utf-8-sig")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build pre-averaged/two-scale intraday covariance diagnostics.")
    ap.add_argument("--scales", default="1,5,20")
    ap.add_argument("--preavg-k", type=int, default=3)
    ap.add_argument("--min-obs", type=int, default=4)
    ap.add_argument("--days-back", type=int, default=5)
    args = ap.parse_args()

    ts = _now()
    ymd = ts.strftime("%Y%m%d")
    out_json = LOG_DIR / f"intraday_covariance_preavg_{ymd}.json"
    out_csv = LOG_DIR / f"intraday_covariance_preavg_{ymd}.csv"
    scales = [max(1, int(x)) for x in str(args.scales).split(",") if str(x).strip().isdigit()]
    if not scales:
        scales = [1, 5, 20]

    ticks, used_files = _load_ticks(max(0, int(args.days_back)))
    scale_meta: Dict[str, Any] = {}
    covs: List[pd.DataFrame] = []
    for scale in scales:
        cov, obs = _cov_for_scale(ticks, scale, max(1, int(args.preavg_k)), max(2, int(args.min_obs)))
        scale_meta[str(scale)] = {"rows_used": int(obs), "asset_count": int(cov.shape[0]) if not cov.empty else 0}
        if not cov.empty:
            covs.append(cov)

    code_cov = _combine_covariances(covs)
    code_cov, psd_meta = _psd_clip(code_cov)
    sector_map = _load_sector_map()
    cov, level, mapped_count = _to_sector_cov(code_cov, sector_map)
    cov, sector_psd_meta = _psd_clip(cov)

    status = "OK" if not cov.empty and cov.shape[0] >= 2 else "INSUFFICIENT_DATA"
    reason = "ok" if status == "OK" else "need_intraday_tick_history_with_at_least_two_assets"
    _write_matrix_csv(out_csv, cov, level)
    _write_matrix_csv(OUT_LATEST_CSV, cov, level)

    payload = {
        "generated_at": ts.isoformat(timespec="seconds"),
        "status": status,
        "reason": reason,
        "method": "PREAVG_TWO_SCALE_REALIZED_COV",
        "level": level,
        "scales_seconds": scales,
        "preavg_k": int(args.preavg_k),
        "min_obs": int(args.min_obs),
        "days_back": int(args.days_back),
        "input_files": used_files,
        "input_rows": int(len(ticks)),
        "input_codes": int(ticks["code"].nunique()) if not ticks.empty else 0,
        "mapped_code_count": int(mapped_count),
        "matrix_size": int(cov.shape[0]) if not cov.empty else 0,
        "scale_diagnostics": scale_meta,
        "psd_code": psd_meta,
        "psd_output": sector_psd_meta,
        "output_csv": str(out_csv),
        "output_latest_csv": str(OUT_LATEST_CSV),
        "operational_use": "diagnostic_only_no_order_block",
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
