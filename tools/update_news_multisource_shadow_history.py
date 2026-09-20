from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"

SIM_CSV = LOGS / "news_multisource_shadow_score_impact_sim_latest.csv"
CANDIDATES_CSV = LOGS / "candidates_latest_data.with_final_score.csv"
MARKET_CSV = LOGS / "backtest_market_ohlc_latest.csv"
HISTORY_CSV = LOGS / "news_multisource_shadow_score_history.csv"
STATUS_JSON = LOGS / "news_multisource_shadow_score_history_latest.json"

KST = timezone(timedelta(hours=9))
HISTORY_VERSION = "multisource_shadow_history_v1"


def _now_kst() -> datetime:
    return datetime.now(KST)


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc, dtype={"code": str})
        except Exception:
            continue
    return pd.read_csv(path, dtype={"code": str})


def _norm_code(value: object) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _date8(value: object) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _num(value: object, default: float = 0.0) -> float:
    try:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(parsed):
            return float(default)
        return float(parsed)
    except Exception:
        return float(default)


def _load_candidate_dates() -> pd.DataFrame:
    if not CANDIDATES_CSV.exists():
        return pd.DataFrame(columns=["code", "signal_date8", "candidate_name"])
    df = _read_csv(CANDIDATES_CSV)
    if df.empty or "code" not in df.columns:
        return pd.DataFrame(columns=["code", "signal_date8", "candidate_name"])
    out = pd.DataFrame()
    out["code"] = df["code"].map(_norm_code)
    out["candidate_name"] = df["name"] if "name" in df.columns else ""
    dates = []
    for _, row in df.iterrows():
        d = ""
        for col in ("date_yyyymmdd", "date", "signal_date", "ymd"):
            if col in df.columns:
                d = _date8(row.get(col))
                if d:
                    break
        dates.append(d)
    out["signal_date8"] = dates
    return out[out["code"] != ""].drop_duplicates("code", keep="first")


def _load_forward_returns() -> dict[tuple[str, str], dict[str, object]]:
    if not MARKET_CSV.exists():
        return {}
    df = _read_csv(MARKET_CSV)
    if df.empty or "code" not in df.columns:
        return {}
    date_col = "date"
    if date_col not in df.columns:
        return {}
    close_col = "close" if "close" in df.columns else ("Close" if "Close" in df.columns else "")
    if not close_col:
        return {}
    work = df[["code", date_col, close_col]].copy()
    work["code"] = work["code"].map(_norm_code)
    work["date8"] = work[date_col].map(_date8)
    work["close"] = pd.to_numeric(work[close_col], errors="coerce")
    work = work[(work["code"] != "") & (work["date8"] != "") & work["close"].notna()]
    out: dict[tuple[str, str], dict[str, object]] = {}
    for code, grp in work.sort_values(["date8"]).groupby("code"):
        rows = grp.reset_index(drop=True)
        by_date = {str(r["date8"]): i for i, r in rows.iterrows()}
        for date8, idx in by_date.items():
            base_close = _num(rows.loc[idx, "close"], 0.0)
            if base_close <= 0:
                continue
            payload: dict[str, object] = {"base_close": base_close, "outcome_status": "PENDING_FWD_RETURN"}
            for horizon in (1, 3, 5):
                next_idx = idx + horizon
                if next_idx < len(rows):
                    fwd_close = _num(rows.loc[next_idx, "close"], 0.0)
                    fwd_date8 = str(rows.loc[next_idx, "date8"])
                    if fwd_close > 0:
                        payload[f"fwd_{horizon}d_ret"] = round((fwd_close / base_close) - 1.0, 6)
                        payload[f"fwd_{horizon}d_date8"] = fwd_date8
                else:
                    payload[f"fwd_{horizon}d_ret"] = ""
                    payload[f"fwd_{horizon}d_date8"] = ""
            if payload.get("fwd_1d_ret") != "":
                payload["outcome_status"] = "HAS_FWD_1D"
            out[(code, date8)] = payload
    return out


def main() -> int:
    status = {
        "generated_at": _now_kst().isoformat(timespec="seconds"),
        "mode": "read_only_multisource_shadow_history_update",
        "history_version": HISTORY_VERSION,
        "input": str(SIM_CSV),
        "history_csv": str(HISTORY_CSV),
        "score_effect": False,
        "trading_effect": False,
        "quality": "FAIL",
        "reason": "",
    }
    if not SIM_CSV.exists():
        status["reason"] = "sim_input_missing"
        STATUS_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 1

    sim = _read_csv(SIM_CSV)
    if sim.empty or "code" not in sim.columns:
        status["reason"] = "sim_input_empty_or_missing_code"
        STATUS_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 1

    sim = sim.copy()
    sim["code"] = sim["code"].map(_norm_code)
    sim = sim[pd.to_numeric(sim.get("final_score_delta_sim"), errors="coerce").fillna(0.0) > 0].copy()
    if sim.empty:
        status.update({"quality": "PASS", "reason": "no_shadow_delta_rows", "rows_added_or_updated": 0})
        STATUS_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return 0

    candidate_dates = _load_candidate_dates()
    fwd = _load_forward_returns()
    sim = sim.merge(candidate_dates, on="code", how="left")
    sim["signal_date8"] = sim["signal_date8"].fillna("").astype(str)
    sim["history_version"] = HISTORY_VERSION
    sim["observed_at"] = status["generated_at"]
    sim["score_effect"] = False
    sim["trading_effect"] = False
    sim["outcome_status"] = "PENDING_FWD_RETURN"

    for idx, row in sim.iterrows():
        payload = fwd.get((str(row["code"]), str(row.get("signal_date8") or "")), {})
        for key, value in payload.items():
            sim.at[idx, key] = value

    keep_cols = [
        "history_version",
        "observed_at",
        "signal_date8",
        "code",
        "name",
        "current_news_score",
        "shadow_news_score",
        "current_final_score",
        "final_score_delta_sim",
        "final_score_shadow_sim",
        "rank_before",
        "rank_after",
        "rank_delta",
        "score_only_entry_effect",
        "sector_entry_allowed",
        "execution_pool",
        "outcome_status",
        "base_close",
        "fwd_1d_ret",
        "fwd_1d_date8",
        "fwd_3d_ret",
        "fwd_3d_date8",
        "fwd_5d_ret",
        "fwd_5d_date8",
        "score_effect",
        "trading_effect",
    ]
    for col in keep_cols:
        if col not in sim.columns:
            sim[col] = ""
    new_rows = sim[keep_cols].copy()

    if HISTORY_CSV.exists():
        hist = _read_csv(HISTORY_CSV)
    else:
        hist = pd.DataFrame(columns=keep_cols)
    if not hist.empty:
        for col in keep_cols:
            if col not in hist.columns:
                hist[col] = ""
        hist = hist[keep_cols].copy()
        hist["code"] = hist["code"].map(_norm_code)
        hist["signal_date8"] = hist["signal_date8"].map(_date8)

    if hist.empty:
        combined = new_rows.copy()
    else:
        combined = pd.concat([hist, new_rows], ignore_index=True)
    combined["code"] = combined["code"].map(_norm_code)
    combined["signal_date8"] = combined["signal_date8"].map(_date8)
    before = len(combined)
    combined = combined.sort_values(["observed_at"]).drop_duplicates(
        ["history_version", "signal_date8", "code"],
        keep="last",
    )
    updated_or_added = len(new_rows)
    deduped = before - len(combined)
    combined.to_csv(HISTORY_CSV, index=False, encoding="utf-8-sig")

    status.update(
        {
            "quality": "PASS",
            "reason": "ok",
            "history_rows": int(len(combined)),
            "rows_added_or_updated": int(updated_or_added),
            "deduped_rows": int(deduped),
            "pending_rows": int((combined["outcome_status"].astype(str) == "PENDING_FWD_RETURN").sum()),
            "has_fwd_1d_rows": int((combined["outcome_status"].astype(str) == "HAS_FWD_1D").sum()),
            "latest_codes": new_rows[["signal_date8", "code", "name", "outcome_status"]].to_dict(orient="records"),
            "score_effect": False,
            "trading_effect": False,
        }
    )
    STATUS_JSON.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        "[NEWS_MULTI_SHADOW_HISTORY] "
        f"quality=PASS rows={len(combined)} updated={updated_or_added} pending={status['pending_rows']} latest={STATUS_JSON}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
