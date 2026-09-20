#!/usr/bin/env python
"""Estimate PnL improvement levers from realized paper trades without changing policy."""

from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path("E:/1_Data")
LOG_DIR = ROOT / "2_Logs"
TRADES_CALC = ROOT / "paper" / "trades_calc.csv"
LOSS_DRIVER = LOG_DIR / "pnl_loss_driver_diagnosis_latest.json"
OUT_JSON_LATEST = LOG_DIR / "pnl_improvement_levers_latest.json"
OUT_CSV_LATEST = LOG_DIR / "pnl_trade_signal_join_latest.csv"


SIGNAL_COLS = [
    "date",
    "date_yyyymmdd",
    "code",
    "name",
    "market",
    "final_score",
    "score",
    "forecast_score",
    "forecast_label",
    "sector_score",
    "sector_action",
    "sector_entry_allowed",
    "sector_entry_weight",
    "news_score",
    "regime_score",
    "flow_score",
    "fundamental_score",
    "execution_lob_score",
    "execution_orderflow_risk_score",
    "candidate_origin",
    "execution_pool",
    "horizon_label",
    "relax_level",
    "ret1_pct",
    "v_accel",
    "atr14_pct",
    "rsi14",
    "stoch_k",
    "adx14",
]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


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
    return _note_value(note, key) in {"1", "true", "True", "TRUE", "Y", "YES"}


def _load_trades() -> pd.DataFrame:
    df = pd.read_csv(TRADES_CALC)
    df["entry_dt"] = pd.to_datetime(df.get("entry_ts"), errors="coerce")
    df["exit_dt"] = pd.to_datetime(df.get("exit_ts"), errors="coerce")
    df["entry_date"] = df["entry_dt"].dt.strftime("%Y%m%d")
    df["exit_date"] = df["exit_dt"].dt.strftime("%Y%m%d")
    df["code"] = df["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)
    df["note"] = df.get("note", "").fillna("").astype(str)
    df["signal_date"] = df["note"].map(lambda x: _note_value(x, "signal_date"))
    df["surge_immediate"] = df["note"].map(lambda x: _note_flag(x, "surge_immediate"))
    df["split_entry"] = df["note"].map(lambda x: "split_entry=" in x)
    df["sector_hrp_reduce"] = df["note"].map(lambda x: _note_flag(x, "sector_hrp_reduce"))
    df["horizon"] = df["note"].map(lambda x: _note_value(x, "horizon") or "unknown")
    df["fallback_stage"] = df["note"].map(lambda x: _note_value(x, "fallback_stage") or "unknown")
    df["net_ret"] = pd.to_numeric(df.get("net_ret"), errors="coerce").fillna(0.0)
    df["qty"] = pd.to_numeric(df.get("qty"), errors="coerce").fillna(0.0)
    df["entry_price"] = pd.to_numeric(df.get("entry_price"), errors="coerce").fillna(0.0)
    df["notional"] = df["qty"] * df["entry_price"]
    df["pnl_krw"] = df["notional"] * df["net_ret"]
    loss = _read_json(LOSS_DRIVER)
    start = str(((loss.get("summary") or {}).get("scope_start_ymd")) or "20260301")
    as_of = str(((loss.get("summary") or {}).get("as_of")) or datetime.now().strftime("%Y%m%d"))
    return df[(df["exit_date"] >= start) & (df["exit_date"] <= as_of)].copy()


def _candidate_paths() -> list[Path]:
    patterns = [
        "candidates_v41_1_*.csv",
        "candidates_latest_data.bak_*.csv",
        "candidates_latest_data.with_final_score.csv",
    ]
    paths: list[Path] = []
    for pat in patterns:
        paths.extend(LOG_DIR.glob(pat))
    return sorted(set(paths), key=lambda p: p.stat().st_mtime)


def _load_signal_history() -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for path in _candidate_paths():
        try:
            df = pd.read_csv(path, dtype={"code": str})
        except Exception:
            continue
        if "code" not in df.columns:
            continue
        keep = [c for c in SIGNAL_COLS if c in df.columns]
        if not keep:
            continue
        sub = df[keep].copy()
        sub["code"] = sub["code"].astype(str).str.extract(r"(\d+)")[0].fillna("").str.zfill(6)
        if "date_yyyymmdd" in sub.columns:
            sub["signal_date_key"] = sub["date_yyyymmdd"].astype(str).str.extract(r"(\d{8})")[0]
        elif "date" in sub.columns:
            sub["signal_date_key"] = pd.to_datetime(sub["date"], errors="coerce").dt.strftime("%Y%m%d")
        else:
            m = re.search(r"(20\d{6})", path.name)
            sub["signal_date_key"] = m.group(1) if m else ""
        sub["_source_path"] = str(path)
        sub["_source_mtime"] = path.stat().st_mtime
        rows.append(sub)
    if not rows:
        return pd.DataFrame(columns=["code", "signal_date_key"])
    hist = pd.concat(rows, ignore_index=True)
    hist = hist.dropna(subset=["signal_date_key"])
    hist = hist.sort_values(["signal_date_key", "code", "_source_mtime"]).drop_duplicates(
        ["signal_date_key", "code"], keep="last"
    )
    return hist


def _scenario(df: pd.DataFrame, name: str, mask: pd.Series, scale: float) -> dict[str, Any]:
    base = float(df["pnl_krw"].sum())
    adj = df["pnl_krw"].copy()
    adj.loc[mask] = adj.loc[mask] * scale
    new = float(adj.sum())
    touched = df.loc[mask]
    return {
        "name": name,
        "trades_scaled": int(mask.sum()),
        "scale": scale,
        "base_pnl_krw": round(base, 2),
        "scenario_pnl_krw": round(new, 2),
        "improvement_krw": round(new - base, 2),
        "target_current_pnl_krw": round(float(touched["pnl_krw"].sum()), 2) if len(touched) else 0.0,
    }


def _group_summary(df: pd.DataFrame, cols: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not cols or df.empty:
        return rows
    for keys, g in df.groupby(cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        gross_profit = float(g.loc[g["pnl_krw"] > 0, "pnl_krw"].sum())
        gross_loss = abs(float(g.loc[g["pnl_krw"] < 0, "pnl_krw"].sum()))
        item = {col: str(val) for col, val in zip(cols, keys)}
        item.update(
            {
                "trades": int(len(g)),
                "pnl_krw": round(float(g["pnl_krw"].sum()), 2),
                "win_rate": round(float((g["pnl_krw"] > 0).mean()), 4),
                "avg_net_ret": round(float(g["net_ret"].mean()), 6),
                "avg_notional_krw": round(float(g["notional"].mean()), 2),
                "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
            }
        )
        rows.append(item)
    rows.sort(key=lambda x: (x["pnl_krw"], -x["trades"]))
    return rows


def _score_group(df: pd.DataFrame, col: str) -> list[dict[str, Any]]:
    if col not in df.columns:
        return []
    s = pd.to_numeric(df[col], errors="coerce")
    if s.notna().sum() < 5:
        return []
    q = pd.qcut(s.rank(method="first"), q=min(4, int(s.notna().sum())), labels=False, duplicates="drop")
    tmp = df.copy()
    tmp["_bucket"] = q
    rows = []
    for b, g in tmp.dropna(subset=["_bucket"]).groupby("_bucket"):
        vals = pd.to_numeric(g[col], errors="coerce")
        gross_profit = float(g.loc[g["pnl_krw"] > 0, "pnl_krw"].sum())
        gross_loss = abs(float(g.loc[g["pnl_krw"] < 0, "pnl_krw"].sum()))
        rows.append(
            {
                "score": col,
                "bucket": int(b),
                "trades": int(len(g)),
                "score_min": round(float(vals.min()), 6),
                "score_max": round(float(vals.max()), 6),
                "win_rate": round(float((g["pnl_krw"] > 0).mean()), 4),
                "avg_net_ret": round(float(g["net_ret"].mean()), 6),
                "pnl_krw": round(float(g["pnl_krw"].sum()), 2),
                "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
            }
        )
    return rows


def main() -> int:
    trades = _load_trades()
    signals = _load_signal_history()
    joined = trades.merge(
        signals,
        left_on=["signal_date", "code"],
        right_on=["signal_date_key", "code"],
        how="left",
        suffixes=("", "_signal"),
    )
    joined["signal_joined"] = joined["signal_date_key"].notna()
    joined["entry_month"] = joined["entry_dt"].dt.strftime("%Y%m")
    base_pnl = float(joined["pnl_krw"].sum())
    scenarios = [
        _scenario(joined, "분할진입 미적용 거래 50% 축소", ~joined["split_entry"], 0.5),
        _scenario(joined, "급등 즉시진입 거래 50% 축소", joined["surge_immediate"], 0.5),
        _scenario(joined, "분할 미적용 + 급등 즉시진입 50% 축소", (~joined["split_entry"]) | joined["surge_immediate"], 0.5),
        _scenario(joined, "14:30 이후 진입 50% 축소", joined["entry_dt"].dt.hour.fillna(0).astype(int) >= 14, 0.5),
        _scenario(joined, "점심 시간대 진입 50% 축소", (joined["entry_dt"].dt.hour * 100 + joined["entry_dt"].dt.minute).between(1130, 1329), 0.5),
    ]
    score_rows: list[dict[str, Any]] = []
    for col in ["final_score", "forecast_score", "sector_score", "news_score", "flow_score", "execution_lob_score"]:
        score_rows.extend(_score_group(joined, col))

    split_by_month = _group_summary(joined, ["entry_month", "split_entry"])[:20]
    surge_by_month = _group_summary(joined, ["entry_month", "surge_immediate"])[:20]
    split_surge_combo = _group_summary(joined, ["split_entry", "surge_immediate"])
    final_score_joined = joined[pd.to_numeric(joined.get("final_score"), errors="coerce").notna()].copy()
    mid_score_good = False
    if not final_score_joined.empty:
        final_score_joined["_score_bucket"] = pd.qcut(
            pd.to_numeric(final_score_joined["final_score"], errors="coerce").rank(method="first"),
            q=min(4, len(final_score_joined)),
            labels=False,
            duplicates="drop",
        )
        bucket_pnl = final_score_joined.groupby("_score_bucket")["pnl_krw"].sum()
        if len(bucket_pnl) >= 3:
            mid_score_good = bool(bucket_pnl.iloc[1:-1].max() > bucket_pnl.iloc[[0, -1]].max())

    losers = joined.sort_values("pnl_krw").head(20)
    top_losers = []
    for _, r in losers.iterrows():
        top_losers.append(
            {
                "code": str(r.get("code", "")),
                "name": str(r.get("name_signal") or r.get("name") or ""),
                "signal_date": str(r.get("signal_date", "")),
                "entry_ts": str(r.get("entry_ts", "")),
                "exit_ts": str(r.get("exit_ts", "")),
                "pnl_krw": round(_num(r.get("pnl_krw")), 2),
                "net_ret": round(_num(r.get("net_ret")), 6),
                "notional": round(_num(r.get("notional")), 2),
                "surge_immediate": bool(r.get("surge_immediate")),
                "split_entry": bool(r.get("split_entry")),
                "final_score": None if pd.isna(r.get("final_score")) else round(_num(r.get("final_score")), 6),
                "forecast_score": None if pd.isna(r.get("forecast_score")) else round(_num(r.get("forecast_score")), 6),
                "sector_score": None if pd.isna(r.get("sector_score")) else round(_num(r.get("sector_score")), 6),
                "news_score": None if pd.isna(r.get("news_score")) else round(_num(r.get("news_score")), 6),
                "candidate_origin": str(r.get("candidate_origin") or ""),
                "horizon_label": str(r.get("horizon_label") or r.get("horizon") or ""),
                "signal_joined": bool(r.get("signal_joined")),
            }
        )

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "realized trade improvement lever diagnosis; no policy/config change",
        "inputs": {
            "trades_calc": str(TRADES_CALC),
            "signal_files_loaded": int(len(_candidate_paths())),
        },
        "summary": {
            "trades": int(len(joined)),
            "base_pnl_krw": round(base_pnl, 2),
            "signal_join_rate": round(float(joined["signal_joined"].mean()), 4) if len(joined) else 0.0,
            "signal_joined_trades": int(joined["signal_joined"].sum()),
        },
        "scenarios": scenarios,
        "root_cause_checks": {
            "split_entry_current_config_enabled": True,
            "split_entry_loss_is_mostly_historical": bool(
                joined.loc[~joined["split_entry"], "entry_month"].astype(str).le("202603").mean() > 0.5
            )
            if (~joined["split_entry"]).any()
            else False,
            "surge_loss_persists_with_split_entry": bool(
                float(joined.loc[joined["surge_immediate"] & joined["split_entry"], "pnl_krw"].sum()) < 0
            ),
            "mid_final_score_bucket_outperforms_extremes": mid_score_good,
        },
        "split_by_month": split_by_month,
        "surge_by_month": surge_by_month,
        "split_surge_combo": split_surge_combo,
        "score_buckets": score_rows,
        "top_losers_with_signal": top_losers,
        "interpretation": [
            "분할진입 미적용/급등 즉시진입 축소 시나리오는 체결가 재현이 아닌 금액 민감도 추정이다.",
            "점수 bucket은 신호 파일이 조인된 거래만 의미가 크다.",
            "정책 변경 전에는 top_losers_with_signal의 공통 조건을 추가 확인해야 한다.",
        ],
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / f"pnl_improvement_levers_{stamp}.json"
    out_csv = LOG_DIR / f"pnl_trade_signal_join_{stamp}.csv"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    out_json.write_text(text, encoding="utf-8")
    OUT_JSON_LATEST.write_text(text, encoding="utf-8")
    joined.to_csv(out_csv, index=False, encoding="utf-8-sig")
    joined.to_csv(OUT_CSV_LATEST, index=False, encoding="utf-8-sig")
    json.loads(OUT_JSON_LATEST.read_text(encoding="utf-8"))
    print(f"[OK] wrote {out_json}")
    print(f"[OK] wrote {OUT_JSON_LATEST}")
    print(f"[OK] wrote {out_csv}")
    print(f"[OK] wrote {OUT_CSV_LATEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
