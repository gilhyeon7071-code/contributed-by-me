#!/usr/bin/env python
"""Diagnose paper trading PnL loss drivers without changing trading policy."""

from __future__ import annotations

import json
import math
import re
import importlib.util
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path("E:/1_Data")
VIBE = Path("E:/vibe/buffett")
LOG_DIR = ROOT / "2_Logs"
TRADES_CALC = ROOT / "paper" / "trades_calc.csv"
SECTOR_SSOT = ROOT / "_cache" / "sector_ssot.csv"
PNL_SUMMARY = LOG_DIR / "paper_pnl_summary_last.json"
PNL_REPORT = ROOT / "paper_pnl_report.py"
ORDERS_DIR = ROOT / "paper"
OUT_JSON_LATEST = LOG_DIR / "pnl_loss_driver_diagnosis_latest.json"
OUT_CSV_LATEST = LOG_DIR / "pnl_loss_driver_segments_latest.csv"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_pnl_module() -> Any:
    spec = importlib.util.spec_from_file_location("paper_pnl_report_runtime", PNL_REPORT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed_to_load:{PNL_REPORT}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_official_scope() -> tuple[pd.DataFrame, str, dict[str, Any]]:
    pnl = _load_pnl_module()
    raw = pnl._ensure_exit_date(pnl._load_trades(TRADES_CALC))
    df, op_scope = pnl._apply_operational_scope(raw)
    df, blocked = pnl._filter_blocked_entry_trades(df, ORDERS_DIR)
    df, unauditable = pnl._filter_unauditable_surge_trades(df)
    ret_col = pnl._pick_ret_col(df)
    if not ret_col:
        raise RuntimeError("return_column_missing")
    meta = {
        "rows_raw": int(len(raw)),
        "operational_scope": op_scope,
        "blocked_entry_filter": blocked,
        "unauditable_surge_filter": unauditable,
        "ret_col": ret_col,
    }
    return df.copy(), ret_col, meta


def _num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return default


def _note_value(note: str, key: str) -> str:
    m = re.search(rf"(?:^|[;| ]){re.escape(key)}=([^;| ]+)", note or "")
    return m.group(1).strip() if m else ""


def _note_flag(note: str, key: str) -> bool:
    value = _note_value(note, key)
    return value in {"1", "true", "True", "TRUE", "Y", "YES"}


def _bucket_hour(ts: pd.Series) -> pd.Series:
    hour = ts.dt.hour.fillna(-1).astype(int)
    minute = ts.dt.minute.fillna(0).astype(int)
    hhmm = hour * 100 + minute
    labels = []
    for v in hhmm:
        if v < 0:
            labels.append("unknown")
        elif v < 1000:
            labels.append("09:00-09:59")
        elif v < 1130:
            labels.append("10:00-11:29")
        elif v < 1330:
            labels.append("11:30-13:29")
        elif v < 1430:
            labels.append("13:30-14:29")
        else:
            labels.append("14:30-15:30")
    return pd.Series(labels, index=ts.index)


def _bucket_hold(days: pd.Series) -> pd.Series:
    labels = []
    for d in days.fillna(-1):
        if d < 0:
            labels.append("unknown")
        elif d < 1:
            labels.append("0D")
        elif d < 2:
            labels.append("1D")
        elif d < 4:
            labels.append("2-3D")
        elif d < 8:
            labels.append("4-7D")
        else:
            labels.append("8D+")
    return pd.Series(labels, index=days.index)


def _segment(df: pd.DataFrame, col: str, label: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    total_loss = abs(float(df.loc[df["pnl_krw"] < 0, "pnl_krw"].sum()))
    for key, g in df.groupby(col, dropna=False):
        n = int(len(g))
        pnl = float(g["pnl_krw"].sum())
        loss = abs(float(g.loc[g["pnl_krw"] < 0, "pnl_krw"].sum()))
        gross_win = float(g.loc[g["pnl_krw"] > 0, "pnl_krw"].sum())
        gross_loss = abs(float(g.loc[g["pnl_krw"] < 0, "pnl_krw"].sum()))
        rows.append(
            {
                "segment_type": label,
                "segment": str(key) if str(key) else "unknown",
                "trades": n,
                "win_rate": round(float((g["pnl_krw"] > 0).mean()), 4) if n else 0.0,
                "avg_net_ret": round(float(g["net_ret"].mean()), 6) if n else 0.0,
                "sum_net_ret": round(float(g["net_ret"].sum()), 6) if n else 0.0,
                "avg_notional_krw": round(float(g["notional"].mean()), 2) if n else 0.0,
                "pnl_krw": round(pnl, 2),
                "gross_profit_krw": round(gross_win, 2),
                "gross_loss_krw": round(gross_loss, 2),
                "profit_factor": round(gross_win / gross_loss, 4) if gross_loss > 0 else None,
                "loss_share": round(loss / total_loss, 4) if total_loss > 0 else 0.0,
            }
        )
    rows.sort(key=lambda x: (x["pnl_krw"], -x["trades"]))
    return rows


def _daily_curve(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    daily = (
        df.groupby("exit_date", dropna=False)
        .agg(pnl_krw=("pnl_krw", "sum"), notional=("notional", "sum"), avg_ret=("net_ret", "mean"))
        .reset_index()
    )
    daily["day_ret"] = daily.apply(
        lambda r: float(r["pnl_krw"] / r["notional"]) if _num(r["notional"]) > 0 else float(r["avg_ret"]),
        axis=1,
    )
    daily["equity"] = (1.0 + daily["day_ret"]).cumprod()
    daily["peak"] = daily["equity"].cummax().clip(lower=1.0)
    daily["drawdown"] = daily["equity"] / daily["peak"] - 1.0
    worst = daily.sort_values("day_ret").head(5).copy()
    return daily, {
        "days": int(len(daily)),
        "worst_days": [
            {
                "date": str(r["exit_date"]),
                "day_ret": round(float(r["day_ret"]), 6),
                "drawdown": round(float(r["drawdown"]), 6),
            }
            for _, r in worst.iterrows()
        ],
        "max_drawdown": round(float(daily["drawdown"].min()), 6) if len(daily) else 0.0,
        "last_drawdown": round(float(daily["drawdown"].iloc[-1]), 6) if len(daily) else 0.0,
    }


def _build_findings(segment_rows: list[dict[str, Any]], daily_summary: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    by_type: dict[str, list[dict[str, Any]]] = {}
    for row in segment_rows:
        by_type.setdefault(row["segment_type"], []).append(row)

    for seg_type in ["code", "sector", "entry_time_bucket", "hold_bucket", "surge_immediate", "split_entry"]:
        worst = [r for r in by_type.get(seg_type, []) if r["trades"] >= 2 or seg_type in {"code", "sector"}]
        worst = sorted(worst, key=lambda r: r["pnl_krw"])[:3]
        if worst:
            findings.append(
                {
                    "type": seg_type,
                    "message": f"worst loss segments by {seg_type}",
                    "evidence": worst,
                }
            )

    worst_days = daily_summary.get("worst_days") or []
    if worst_days:
        findings.append(
            {
                "type": "daily_drawdown",
                "message": "worst daily drawdown contributors",
                "evidence": worst_days[:5],
            }
        )
    return findings


def main() -> int:
    if not TRADES_CALC.exists():
        raise FileNotFoundError(str(TRADES_CALC))

    pnl = _read_json(PNL_SUMMARY)
    start_ymd = str(((pnl.get("meta") or {}).get("operational_scope") or {}).get("start_ymd") or "20260301")
    as_of = str(pnl.get("as_of") or pnl.get("as_of_ymd") or datetime.now().strftime("%Y%m%d"))

    df, ret_col, official_scope_meta = _load_official_scope()
    df["entry_dt"] = pd.to_datetime(df.get("entry_ts"), errors="coerce")
    df["exit_dt"] = pd.to_datetime(df.get("exit_ts"), errors="coerce")
    df["entry_date"] = df["entry_dt"].dt.strftime("%Y%m%d")
    df["exit_date"] = df["exit_dt"].dt.strftime("%Y%m%d")
    df["code"] = df["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)
    df["net_ret"] = pd.to_numeric(df.get(ret_col), errors="coerce").fillna(0.0)
    df["gross_ret"] = pd.to_numeric(df.get("gross_ret"), errors="coerce").fillna(0.0)
    df["qty"] = pd.to_numeric(df.get("qty"), errors="coerce").fillna(0.0)
    df["entry_price"] = pd.to_numeric(df.get("entry_price"), errors="coerce").fillna(0.0)
    df["notional"] = df["qty"] * df["entry_price"]
    df["pnl_krw"] = df["notional"] * df["net_ret"]
    df["note"] = df.get("note", "").fillna("").astype(str)
    df["signal_date"] = df["note"].map(lambda x: _note_value(x, "signal_date"))
    df["horizon"] = df["note"].map(lambda x: _note_value(x, "horizon") or "unknown")
    df["fallback_stage"] = df["note"].map(lambda x: _note_value(x, "fallback_stage") or "unknown")
    df["surge_immediate"] = df["note"].map(lambda x: "YES" if _note_flag(x, "surge_immediate") else "NO")
    df["split_entry"] = df["note"].map(lambda x: "YES" if "split_entry=" in x else "NO")
    df["sector_hrp_reduce"] = df["note"].map(lambda x: "YES" if _note_flag(x, "sector_hrp_reduce") else "NO")
    df["entry_time_bucket"] = _bucket_hour(df["entry_dt"])
    hold_days = (df["exit_dt"] - df["entry_dt"]).dt.total_seconds() / 86400.0
    df["hold_days"] = hold_days
    df["hold_bucket"] = _bucket_hold(hold_days)

    df = df[(df["exit_date"] >= start_ymd) & (df["exit_date"] <= as_of)].copy()

    if SECTOR_SSOT.exists():
        sec = pd.read_csv(SECTOR_SSOT, dtype=str)
        sec["code"] = sec["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)
        sec = sec.drop_duplicates("code")
        df = df.merge(sec[["code", "name", "krx_sector", "industry"]], on="code", how="left", suffixes=("", "_sector"))
    else:
        df["krx_sector"] = ""
        df["industry"] = ""

    df["krx_sector"] = df.get("krx_sector", "").fillna("unknown").replace("", "unknown")
    df["name_resolved"] = df.get("name_sector", df.get("name", "")).fillna(df.get("name", "")).fillna("").astype(str)

    segment_cols = [
        ("code", "code"),
        ("krx_sector", "sector"),
        ("entry_date", "entry_date"),
        ("exit_date", "exit_date"),
        ("entry_time_bucket", "entry_time_bucket"),
        ("hold_bucket", "hold_bucket"),
        ("horizon", "horizon"),
        ("fallback_stage", "fallback_stage"),
        ("surge_immediate", "surge_immediate"),
        ("split_entry", "split_entry"),
        ("sector_hrp_reduce", "sector_hrp_reduce"),
    ]
    segments: list[dict[str, Any]] = []
    for col, label in segment_cols:
        segments.extend(_segment(df, col, label))

    daily, daily_summary = _daily_curve(df)
    pnl_equity = pnl.get("equity") if isinstance(pnl.get("equity"), dict) else {}
    total_pnl = float(df["pnl_krw"].sum())
    gross_profit = float(df.loc[df["pnl_krw"] > 0, "pnl_krw"].sum())
    gross_loss = abs(float(df.loc[df["pnl_krw"] < 0, "pnl_krw"].sum()))
    summary = {
        "rows": int(len(df)),
        "as_of": as_of,
        "scope_start_ymd": start_ymd,
        "total_pnl_krw": round(total_pnl, 2),
        "avg_net_ret": round(float(df["net_ret"].mean()), 6) if len(df) else 0.0,
        "win_rate": round(float((df["pnl_krw"] > 0).mean()), 4) if len(df) else 0.0,
        "gross_profit_krw": round(gross_profit, 2),
        "gross_loss_krw": round(gross_loss, 2),
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "avg_win_notional_krw": round(float(df.loc[df["pnl_krw"] > 0, "notional"].mean()), 2)
        if (df["pnl_krw"] > 0).any()
        else 0.0,
        "avg_loss_notional_krw": round(float(df.loc[df["pnl_krw"] < 0, "notional"].mean()), 2)
        if (df["pnl_krw"] < 0).any()
        else 0.0,
        "ret_pnl_divergence": bool(float(df["net_ret"].mean()) > 0 and total_pnl < 0) if len(df) else False,
        "engine_max_drawdown": round(_num(pnl_equity.get("max_drawdown_pct")), 6),
        "engine_last_drawdown": round(_num(pnl_equity.get("dd_end_pct")), 6),
        "diagnostic_weighted_max_drawdown": daily_summary.get("max_drawdown"),
        "diagnostic_weighted_last_drawdown": daily_summary.get("last_drawdown"),
    }
    findings = _build_findings(segments, daily_summary)
    if summary["ret_pnl_divergence"]:
        findings.insert(
            0,
            {
                "type": "position_sizing",
                "message": "average return is positive but realized KRW PnL is negative",
                "evidence": {
                    "avg_net_ret": summary["avg_net_ret"],
                    "total_pnl_krw": summary["total_pnl_krw"],
                    "avg_win_notional_krw": summary["avg_win_notional_krw"],
                    "avg_loss_notional_krw": summary["avg_loss_notional_krw"],
                    "interpretation": "loss trades used larger average notional than win trades, so a few large losses dominate total PnL",
                },
            },
        )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / f"pnl_loss_driver_diagnosis_{stamp}.json"
    out_csv = LOG_DIR / f"pnl_loss_driver_segments_{stamp}.csv"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "paper trading realized PnL loss-driver diagnosis; no policy/config change",
        "inputs": {
            "trades_calc": str(TRADES_CALC),
            "sector_ssot": str(SECTOR_SSOT),
            "pnl_summary": str(PNL_SUMMARY),
            "paper_pnl_report": str(PNL_REPORT),
        },
        "official_scope_meta": official_scope_meta,
        "summary": summary,
        "daily_summary": daily_summary,
        "findings": findings,
        "recommended_next_checks": [
            "join worst loss codes with final_score, sector_score, and news_score at entry time",
            "separate clustered drawdown days from ordinary single-name losses",
            "compare split_entry and non_split_entry sizing contribution",
            "check whether loss concentration requires a sizing cap rather than a signal-score change",
        ],
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False)
    out_json.write_text(text, encoding="utf-8")
    OUT_JSON_LATEST.write_text(text, encoding="utf-8")
    seg_df = pd.DataFrame(segments)
    seg_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    seg_df.to_csv(OUT_CSV_LATEST, index=False, encoding="utf-8-sig")
    json.loads(OUT_JSON_LATEST.read_text(encoding="utf-8"))
    print(f"[OK] wrote {out_json}")
    print(f"[OK] wrote {OUT_JSON_LATEST}")
    print(f"[OK] wrote {out_csv}")
    print(f"[OK] wrote {OUT_CSV_LATEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
