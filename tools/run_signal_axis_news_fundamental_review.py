"""Guard V2 pre-registered standalone-effect review for two axis-4 (신호) candidates:
news_score (signals_naver_daily) and fundamental ROE (dart_fundamental snapshots).

Read-only research tool. Does not touch Gate/order/fill/ledger/config/score/threshold.

Pre-registration is locked in PRE_REG below BEFORE any result was inspected. Per
STRATEGY_VALIDATION_GUARD.md 11조, thresholds/combinations are not to be changed after
seeing output; any change requires a new tagged exploration round.
"""
import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
NEWS_DB = ROOT / "news_trading" / "data" / "trading.db"
CACHE_DIR = ROOT / "_cache"
LATEST_JSON = LOGS / "signal_axis_news_fundamental_review_latest.json"
LATEST_CSV = LOGS / "signal_axis_news_fundamental_review_latest.csv"

PRE_REG: Dict[str, Any] = {
    "guard": "STRATEGY_VALIDATION_GUARD.md V2",
    "round_type": "탐색 라운드 (exploration, not confirmatory)",
    "fixed_target": "신호축(4): news_score, fundamental_ROE 단독 효과 확인 (2026-07-24 사전등록)",
    "universe": "6자리 숫자 code, 신호일 다음 거래일 진입 시점 기준 당일 거래대금(value) > 1,000,000,000 KRW",
    "universe_known_limitation": "min_listing_days/exclude_administrative/investment_warning/investment_risk 미적용 (1차 패스 범위 제한, 후속 라운드 과제)",
    "primary_metric": "신호일별 동일가중 TOP20% 바스켓의 비용차감 평균수익률(mean_return)",
    "primary_horizon": "h5",
    "secondary_horizons_diagnostic_only": ["h1", "h2"],
    "execution_assumption": "신호일 종가 관측 -> 다음 실제 거래일 시가 진입 -> 진입일로부터 h거래일 후 시가 청산 (open-to-open)",
    # [2026-09-10] 0.00358 -> 0.00400 (브로커 실측 fee 0 / tax 0.19723%. C1)
    "cost_model": {"round_trip_pct": 0.00400, "source": "2026-08-24 브로커 실측 (fee 0.0*2 + slippage 0.001*2 + sell_tax 0.002). 종전 0.00358 은 2026-07-23 모델"},
    "signals_tested": ["news_score", "fundamental_ROE"],
    "bucket_primary": "TOP_20PCT",
    "bucket_diagnostic": ["TOP_10PCT", "TOP_30PCT"],
    "split": "signal-date 기준 시간순 60/20/20 = Train/Validation/OOS",
    "min_unique_signal_days_per_cell": 20,
    "pass_rule_oos_h5_top20": (
        "mean_return > 0 AND hit_rate >= 0.50 AND mean_return >= cost_model.round_trip_pct "
        "(비용 1회분을 추가로 넘는 마진 요구) -- 결과 확인 전 고정"
    ),
    "insufficient_sample_rule": "해당 셀 unique_signal_days < min_unique_signal_days_per_cell -> DEFERRED_INSUFFICIENT_SAMPLE",
    "known_limitations": [
        "CPCV/PBO/purge-embargo 미구현 -- 시간순 3분할 + 신호일 바스켓 시계열만 사용",
        "h>=2 보유기간은 연속 신호일 간 포지션이 겹쳐 바스켓 수익률 시계열에 직렬상관 존재, 이번 패스는 보정하지 않음(한계로 기록)",
        "fundamental_ROE는 DART 분기 보고 기준이라 신호일 시점에 이미 공시된 값인지 별도 lookahead 재검증 없음(스냅샷 도구의 as_of_ymd/dart_updated_at을 point-in-time 경계로 신뢰)",
    ],
    "operational_reflection": "diagnostic_only=true, used_for_trading=false, score/threshold/Gate/order/fill/ledger 미변경",
}

HORIZONS = [1, 2, 5]


def _read_price_data() -> pd.DataFrame:
    dirs_priority = [(ROOT / "_krx_manual", 2), (ROOT / "krx_daily_archive", 1), (ROOT, 0)]
    parts: List[pd.DataFrame] = []
    for directory, prio in dirs_priority:
        if not directory.exists():
            continue
        pattern = "krx_daily_*_clean.parquet" if directory.name != str(ROOT.name) else "krx_daily_*_clean.parquet"
        for f in sorted(directory.glob("krx_daily_*_clean.parquet")):
            if directory == ROOT and f.parent != ROOT:
                continue
            name = f.name
            if "quarantine" in name or f.suffix != ".parquet":
                continue
            try:
                df = pd.read_parquet(f, columns=["date", "code", "open", "close", "value"])
            except Exception:
                continue
            df["_prio"] = prio
            try:
                df["_mtime"] = f.stat().st_mtime
            except OSError:
                df["_mtime"] = 0.0
            parts.append(df)
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["code"] = df["code"].astype(str).str.zfill(6)
    for c in ("open", "close", "value"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values(["_prio", "_mtime"]).drop_duplicates(subset=["code", "date"], keep="last")
    return df.drop(columns=["_prio", "_mtime"])


def _trading_days(price: pd.DataFrame) -> List[pd.Timestamp]:
    return sorted(price["date"].unique())


def _load_news_signal() -> pd.DataFrame:
    if not NEWS_DB.exists():
        return pd.DataFrame()
    con = sqlite3.connect(str(NEWS_DB))
    try:
        df = pd.read_sql_query("SELECT date8, code, news_score FROM signals_naver_daily", con)
    finally:
        con.close()
    df["D"] = pd.to_datetime(df["date8"], format="%Y%m%d", errors="coerce")
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["signal_value"] = pd.to_numeric(df["news_score"], errors="coerce")
    return df.dropna(subset=["D", "signal_value"])[["D", "code", "signal_value"]]


def _load_fundamental_signal() -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for f in sorted(CACHE_DIR.glob("dart_fundamental_2*.csv")):
        stem = f.stem.split("_")[-1]
        if len(stem) != 8 or not stem.isdigit():
            continue
        try:
            df = pd.read_csv(f, dtype=str)
        except Exception:
            continue
        if "code" not in df.columns or "ROE" not in df.columns:
            continue
        df = df[["code", "ROE"]].copy()
        df["D"] = pd.to_datetime(stem, format="%Y%m%d", errors="coerce")
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["signal_value"] = pd.to_numeric(df["ROE"], errors="coerce")
        rows.append(df.dropna(subset=["D", "signal_value"])[["D", "code", "signal_value"]])
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    return out.drop_duplicates(subset=["D", "code"], keep="last")


def _forward_return(
    signal_df: pd.DataFrame, price: pd.DataFrame, trading_days: List[pd.Timestamp], horizon: int
) -> pd.DataFrame:
    td_index = {d: i for i, d in enumerate(trading_days)}
    day_open = price.set_index(["date", "code"])["open"]
    day_value = price.set_index(["date", "code"])["value"]

    out_rows = []
    for d, grp in signal_df.groupby("D"):
        after = [t for t in trading_days if t > d]
        if not after:
            continue
        entry_day = after[0]
        entry_idx = td_index[entry_day]
        exit_idx = entry_idx + horizon
        if exit_idx >= len(trading_days):
            continue
        exit_day = trading_days[exit_idx]
        for _, r in grp.iterrows():
            code = r["code"]
            try:
                v = day_value.loc[(entry_day, code)]
                if pd.isna(v) or float(v) <= 1_000_000_000.0:
                    continue
                p_in = day_open.loc[(entry_day, code)]
                p_out = day_open.loc[(exit_day, code)]
            except KeyError:
                continue
            if pd.isna(p_in) or pd.isna(p_out) or p_in <= 0:
                continue
            raw_ret = float(p_out) / float(p_in) - 1.0
            out_rows.append(
                {
                    "D": d,
                    "code": code,
                    "signal_value": float(r["signal_value"]),
                    "entry_day": entry_day,
                    "exit_day": exit_day,
                    "raw_return": raw_ret,
                    "net_return": raw_ret - PRE_REG["cost_model"]["round_trip_pct"],
                }
            )
    return pd.DataFrame(out_rows)


def _split_cells(unique_days: List[pd.Timestamp]) -> Dict[str, List[pd.Timestamp]]:
    n = len(unique_days)
    n_train = int(round(n * 0.6))
    n_val = int(round(n * 0.2))
    return {
        "TRAIN": unique_days[:n_train],
        "VALIDATION": unique_days[n_train : n_train + n_val],
        "OOS": unique_days[n_train + n_val :],
    }


def _bucket_metrics(ret_df: pd.DataFrame, days: List[pd.Timestamp], top_pct: float) -> Dict[str, Any]:
    sub = ret_df[ret_df["D"].isin(days)]
    unique_days = sorted(sub["D"].unique())
    day_means = []
    for d in unique_days:
        dd = sub[sub["D"] == d].sort_values("signal_value", ascending=False)
        n = max(1, int(round(len(dd) * top_pct)))
        top = dd.head(n)
        if top.empty:
            continue
        day_means.append(float(top["net_return"].mean()))
    unique_signal_days = len(unique_days)
    if not day_means:
        return {"unique_signal_days": unique_signal_days, "rows": int(len(sub)), "mean_return": None, "hit_rate": None}
    arr = np.array(day_means)
    return {
        "unique_signal_days": unique_signal_days,
        "rows": int(len(sub)),
        "mean_return": round(float(arr.mean()), 6),
        "hit_rate": round(float((arr > 0).mean()), 6),
        "median_return": round(float(np.median(arr)), 6),
    }


def _grade_oos_primary(m: Dict[str, Any]) -> str:
    min_days = PRE_REG["min_unique_signal_days_per_cell"]
    if m["unique_signal_days"] < min_days:
        return "DEFERRED_INSUFFICIENT_SAMPLE"
    if m["mean_return"] is None or m["hit_rate"] is None:
        return "INSUFFICIENT_METRIC"
    cost = PRE_REG["cost_model"]["round_trip_pct"]
    if m["mean_return"] > 0 and m["hit_rate"] >= 0.50 and m["mean_return"] >= cost:
        return "PASS_EXPLORATION_ROUND"
    return "FAIL_EXPLORATION_ROUND"


def build_review() -> Dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    price = _read_price_data()
    if price.empty:
        payload = {"generated_at": generated_at, "status": "FAIL", "reason": "PRICE_DATA_EMPTY", "pre_registration": PRE_REG}
        LOGS.mkdir(parents=True, exist_ok=True)
        LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        return payload
    trading_days = _trading_days(price)

    signals = {"news_score": _load_news_signal(), "fundamental_ROE": _load_fundamental_signal()}
    summary_rows: List[Dict[str, Any]] = []
    for sig_name, sig_df in signals.items():
        if sig_df.empty:
            summary_rows.append({"signal": sig_name, "status": "NO_DATA"})
            continue
        unique_signal_days_all = sorted(sig_df["D"].unique())
        cells = _split_cells(unique_signal_days_all)
        for horizon in HORIZONS:
            ret_df = _forward_return(sig_df, price, trading_days, horizon)
            if ret_df.empty:
                continue
            for cell_name, cell_days in cells.items():
                for top_pct, bucket_name in ((0.20, "TOP_20PCT"), (0.10, "TOP_10PCT"), (0.30, "TOP_30PCT")):
                    m = _bucket_metrics(ret_df, cell_days, top_pct)
                    row = {
                        "signal": sig_name,
                        "horizon": f"h{horizon}",
                        "cell": cell_name,
                        "bucket": bucket_name,
                        **m,
                    }
                    if horizon == 5 and bucket_name == "TOP_20PCT" and cell_name == "OOS":
                        row["grade"] = _grade_oos_primary(m)
                    summary_rows.append(row)

    summary = pd.DataFrame(summary_rows)
    LOGS.mkdir(parents=True, exist_ok=True)
    summary.to_csv(LATEST_CSV, index=False, encoding="utf-8-sig")

    primary_rows = [r for r in summary_rows if r.get("horizon") == "h5" and r.get("bucket") == "TOP_20PCT" and r.get("cell") == "OOS"]
    payload = {
        "generated_at": generated_at,
        "status": "PASS",
        "pre_registration": PRE_REG,
        "price_rows": int(len(price)),
        "trading_days": len(trading_days),
        "trading_day_range": [str(trading_days[0].date()), str(trading_days[-1].date())] if trading_days else None,
        "signal_day_counts": {k: int(v["D"].nunique()) if not v.empty else 0 for k, v in signals.items()},
        "primary_result_oos_h5_top20": primary_rows,
        "full_breakdown_csv": str(LATEST_CSV),
    }
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=True, indent=2, default=str), encoding="utf-8")
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only pre-registered axis-4 signal standalone effect review (news_score, fundamental_ROE).")
    ap.parse_args()
    payload = build_review()
    print(f"[SIGNAL_AXIS_REVIEW] status={payload.get('status')} trading_days={payload.get('trading_days')}")
    for row in payload.get("primary_result_oos_h5_top20", []):
        print(f"  PRIMARY(h5,TOP20,OOS) signal={row.get('signal')} days={row.get('unique_signal_days')} "
              f"mean_return={row.get('mean_return')} hit_rate={row.get('hit_rate')} grade={row.get('grade')}")
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
