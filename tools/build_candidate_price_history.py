# -*- coding: utf-8 -*-
r"""Build candidate daily price history from the official paper OHLCV parquet.

This keeps candidate snapshot files narrow and writes a separate history artifact:
- E:\1_Data\2_Logs\candidate_price_history_latest.csv
- E:\1_Data\2_Logs\candidate_price_history_YYYYMMDD_HHMMSS.csv
- E:\1_Data\2_Logs\candidate_price_history_status_latest.json
- E:\1_Data\2_Logs\candidate_price_history_status_YYYYMMDD_HHMMSS.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
PRICE_PARQUET = ROOT / "paper" / "prices" / "ohlcv_paper.parquet"
DEFAULT_CANDIDATES = LOGS / "candidates_latest_data.with_sector_score.csv"

OUT_LATEST_CSV = LOGS / "candidate_price_history_latest.csv"
STATUS_LATEST = LOGS / "candidate_price_history_status_latest.json"


def _now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _mtime_iso(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime).replace(microsecond=0).isoformat()


def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(text, encoding=encoding)
    tmp_path.replace(path)


def _atomic_write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    tmp_path.replace(path)


def _norm_code(value: Any) -> str:
    text = str(value or "").strip()
    digits = re.sub(r"\D", "", text)
    if digits:
        return digits.zfill(6)
    return text.upper()


def _read_candidates(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"candidate source missing: {path}")
    df = pd.read_csv(path, dtype=str).fillna("")
    if "code" not in df.columns:
        raise ValueError(f"candidate source has no code column: {path}")
    df["code"] = df["code"].map(_norm_code)
    df = df[df["code"].astype(str).str.len() > 0].copy()
    if df.empty:
        raise ValueError(f"candidate source has no candidate codes: {path}")
    return df


def _candidate_meta(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in ["code", "name", "date", "date_yyyymmdd", "final_score", "score", "sector_score", "candidate_origin"] if c in df.columns]
    meta = df[cols].drop_duplicates(subset=["code"], keep="first").copy()
    rename_map = {
        "name": "candidate_name",
        "date": "candidate_date",
        "date_yyyymmdd": "candidate_date_yyyymmdd",
    }
    return meta.rename(columns=rename_map)


def _read_price_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"price parquet missing: {path}")
    df = pd.read_parquet(path)
    required = {"date", "code", "open", "high", "low", "close", "volume"}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"price parquet missing required columns: {missing}")
    out = df.copy()
    out["code"] = out["code"].map(_norm_code)
    out["date"] = out["date"].astype(str).str.replace(r"\D", "", regex=True).str[:8]
    out = out[(out["code"].astype(str).str.len() > 0) & (out["date"].astype(str).str.len() == 8)].copy()
    return out


def _build_history(candidates: pd.DataFrame, prices: pd.DataFrame, lookback: int) -> pd.DataFrame:
    codes = candidates["code"].dropna().astype(str).unique().tolist()
    hist = prices[prices["code"].isin(codes)].copy()
    if hist.empty:
        return hist
    hist = hist.sort_values(["code", "date"]).groupby("code", as_index=False, group_keys=False).tail(int(lookback))
    meta = _candidate_meta(candidates)
    hist = hist.merge(meta, on="code", how="left")
    if "name" in hist.columns and "candidate_name" in hist.columns:
        hist["name"] = hist["name"].fillna("").astype(str).str.strip()
        hist["candidate_name"] = hist["candidate_name"].fillna("").astype(str).str.strip()
        hist["name"] = hist.apply(lambda r: r.get("name") or r.get("candidate_name") or "", axis=1)
    ordered = [
        "candidate_date",
        "candidate_date_yyyymmdd",
        "code",
        "candidate_name",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "final_score",
        "score",
        "sector_score",
        "candidate_origin",
    ]
    cols = [c for c in ordered if c in hist.columns] + [c for c in hist.columns if c not in ordered]
    return hist[cols].sort_values(["code", "date"]).reset_index(drop=True)


def _write_status(path: Path, obj: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build candidate price history artifacts.")
    parser.add_argument("--candidate-source", default=str(DEFAULT_CANDIDATES))
    parser.add_argument("--price-parquet", default=str(PRICE_PARQUET))
    parser.add_argument("--lookback", type=int, default=60)
    args = parser.parse_args()

    stamp = _now_stamp()
    dated_csv = LOGS / f"candidate_price_history_{stamp}.csv"
    dated_status = LOGS / f"candidate_price_history_status_{stamp}.json"
    candidate_source = Path(args.candidate_source)
    price_parquet = Path(args.price_parquet)
    lookback = max(1, int(args.lookback))

    status: dict[str, Any] = {
        "generated_at": _now_iso(),
        "status": "UNKNOWN",
        "candidate_source": str(candidate_source),
        "price_parquet": str(price_parquet),
        "lookback": lookback,
        "latest_csv": str(OUT_LATEST_CSV),
        "dated_csv": str(dated_csv),
        "latest_status": str(STATUS_LATEST),
        "dated_status": str(dated_status),
        "candidate_rows": 0,
        "candidate_codes": 0,
        "history_rows": 0,
        "history_codes": 0,
        "missing_price_codes": [],
        "date_min": "",
        "date_max": "",
        "issues": [],
        "raw_evidence": {
            "candidate_source": str(candidate_source),
            "price_parquet": str(price_parquet),
            "lookback": lookback,
        },
        "provenance": {
            "source_file": "tools/build_candidate_price_history.py",
            "source_mtime": _mtime_iso(Path(__file__).resolve()),
            "candidate_source": str(candidate_source),
            "candidate_source_mtime": _mtime_iso(candidate_source),
            "price_source": str(price_parquet),
            "price_source_mtime": _mtime_iso(price_parquet),
            "latest_csv": str(OUT_LATEST_CSV),
            "latest_status": str(STATUS_LATEST),
            "generator": "build_candidate_price_history.py",
        },
        "policy_effect": False,
        "trading_effect": False,
        "orders_fills_ledger_changed": False,
    }

    try:
        candidates = _read_candidates(candidate_source)
        prices = _read_price_history(price_parquet)
        history = _build_history(candidates, prices, lookback)

        candidate_codes = sorted(candidates["code"].dropna().astype(str).unique().tolist())
        history_codes = sorted(history["code"].dropna().astype(str).unique().tolist()) if not history.empty else []
        missing_codes = sorted(set(candidate_codes).difference(history_codes))

        status.update(
            {
                "candidate_rows": int(len(candidates)),
                "candidate_codes": int(len(candidate_codes)),
                "history_rows": int(len(history)),
                "history_codes": int(len(history_codes)),
                "missing_price_codes": missing_codes,
                "date_min": str(history["date"].min()) if not history.empty else "",
                "date_max": str(history["date"].max()) if not history.empty else "",
                "status": "PASS" if len(history_codes) == len(candidate_codes) and len(history) > 0 else "WARN",
            }
        )
        if missing_codes:
            status["issues"].append("missing_price_codes")
        if history.empty:
            status["issues"].append("history_empty")

        _atomic_write_csv(dated_csv, history)
        _atomic_write_csv(OUT_LATEST_CSV, history)
    except Exception as exc:
        status["status"] = "FAIL"
        status["issues"].append(f"{type(exc).__name__}: {exc}")

    _write_status(dated_status, status)
    _write_status(STATUS_LATEST, status)
    print(json.dumps(status, ensure_ascii=False, indent=2))
    return 0 if status["status"] in {"PASS", "WARN"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
