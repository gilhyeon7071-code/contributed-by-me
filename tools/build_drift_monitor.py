from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "paper"
LOGS = ROOT / "2_Logs"
ROOTB = ROOT.parent / "vibe" / "buffett"
ROOTB_LEDGER = ROOTB / "data" / "ledger" / "paper_fills_ledger.csv"
KIS_ACCOUNT_SNAPSHOT = LOGS / "kis_account_snapshot_latest.json"

PASS = "PASS"
HIT = "HIT"
NOT_EVALUABLE = "NOT_EVALUABLE"
FAIL = "FAIL"


def _now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _date8(v: Any) -> str:
    m = re.search(r"(\d{8})", str(v or ""))
    return m.group(1) if m else ""


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    # [2026-08-24] 빈 파일(헤더조차 없음)이면 빈 DataFrame 으로 넘어간다.
    #   계좌 포지션이 0 이면 kis_account_snapshot.py 가 5바이트 CSV 를 만들고,
    #   pandas 가 EmptyDataError 로 죽어 배치 전체가 rc=1 이 됐다(19:01 실패).
    #   포지션 0 은 정상 상태다 - 그때마다 배치가 죽으면 안 된다.
    try:
        if path.stat().st_size == 0:
            return pd.DataFrame()
    except OSError:
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
        except Exception:
            continue
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _read_xlsx(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_excel(path, dtype=str).fillna("")


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _latest_file(pattern: str) -> Optional[Path]:
    files = sorted(LOGS.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _to_num(s: Any) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _float(v: Any, default: float = 0.0) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return float(default)


def _norm_code(v: Any) -> str:
    m = re.search(r"(\d+)", str(v or ""))
    return str(int(m.group(1))).zfill(6) if m else ""


def _parse_hms(v: Any) -> Optional[int]:
    raw = re.sub(r"\D", "", str(v or ""))
    if len(raw) < 6:
        return None
    raw = raw[-6:]
    try:
        h, m, s = int(raw[:2]), int(raw[2:4]), int(raw[4:6])
    except Exception:
        return None
    if not (0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59):
        return None
    return h * 3600 + m * 60 + s


def _accepted_submit_time_map(ymd: str) -> Dict[str, int]:
    path = PAPER / f"orders_{ymd}_broker_submit_mock.csv"
    df = _read_csv(path)
    if df.empty or "ord_no" not in df.columns or "dispatch_ts" not in df.columns:
        return {}
    if "dispatch_status" in df.columns:
        df = df[df["dispatch_status"].astype(str).str.upper().eq("ACCEPTED")].copy()
    out: Dict[str, int] = {}
    for _, row in df.iterrows():
        order_no = str(row.get("ord_no", "")).strip()
        if not order_no:
            continue
        raw = str(row.get("dispatch_ts", "")).strip()
        try:
            parsed = dt.datetime.fromisoformat(raw)
        except Exception:
            continue
        out.setdefault(order_no, parsed.hour * 3600 + parsed.minute * 60 + parsed.second)
    return out


def _latency_rows_for_date(ymd: str) -> pd.DataFrame:
    path = PAPER / f"kis_fills_api_{ymd}.csv"
    df = _read_csv(path)
    if df.empty:
        return pd.DataFrame()
    order_col = "ord_tmd" if "ord_tmd" in df.columns else ""
    fill_col = "ccld_tmd" if "ccld_tmd" in df.columns else ""
    submit_time_map = _accepted_submit_time_map(ymd)
    if not order_col:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    venue = "KIS"
    if "venue" in df.columns:
        venue_s = df["venue"].astype(str)
    elif "source" in df.columns:
        venue_s = df["source"].astype(str)
    else:
        venue_s = pd.Series([venue] * len(df), index=df.index)
    for idx, row in df.iterrows():
        end = _parse_hms(row.get(fill_col)) if fill_col else None
        start = _parse_hms(row.get(order_col))
        if end is None:
            end = start
            start = submit_time_map.get(str(row.get("order_no", "")).strip())
        if start is None or end is None:
            continue
        latency = end - start
        if latency < 0:
            continue
        rows.append(
            {
                "date": ymd,
                "venue": str(venue_s.loc[idx] or venue).strip() or venue,
                "hour": int(start // 3600),
                "latency_ms": float(latency * 1000),
            }
        )
    return pd.DataFrame(rows)


def _build_latency(ymd: str) -> Dict[str, Any]:
    dates = [(dt.datetime.strptime(ymd, "%Y%m%d") - dt.timedelta(days=i)).strftime("%Y%m%d") for i in range(0, 8)]
    frames = [_latency_rows_for_date(d) for d in dates]
    cur = frames[0] if frames else pd.DataFrame()
    hist = pd.concat([x for x in frames[1:] if not x.empty], ignore_index=True) if any(not x.empty for x in frames[1:]) else pd.DataFrame()
    if cur.empty:
        return {
            "status": NOT_EVALUABLE,
            "reason": "missing_latency_source_columns_or_rows",
            "rule": "p95 latency > 7d mean + 3 sigma OR p95 > 1500ms for 2 consecutive buckets",
            "rows": [],
        }

    grouped = cur.groupby(["venue", "hour"], dropna=False)["latency_ms"].quantile(0.95).reset_index(name="p95_latency_ms")
    hist_stats = pd.DataFrame()
    if not hist.empty:
        hist_bucket = hist.groupby(["date", "venue", "hour"], dropna=False)["latency_ms"].quantile(0.95).reset_index(name="p95_latency_ms")
        hist_stats = hist_bucket.groupby(["venue", "hour"], dropna=False)["p95_latency_ms"].agg(["mean", "std"]).reset_index()

    rows: List[Dict[str, Any]] = []
    for _, r in grouped.sort_values(["venue", "hour"]).iterrows():
        venue = str(r["venue"])
        hour = int(r["hour"])
        p95 = float(r["p95_latency_ms"])
        mean_7d = None
        std_7d = None
        sigma_hit = False
        if not hist_stats.empty:
            h = hist_stats[(hist_stats["venue"].astype(str) == venue) & (hist_stats["hour"].astype(int) == hour)]
            if len(h):
                mean_7d = float(h.iloc[0]["mean"])
                std_raw = h.iloc[0]["std"]
                std_7d = None if pd.isna(std_raw) else float(std_raw)
                sigma_hit = std_7d is not None and p95 > mean_7d + 3.0 * std_7d
        abs_hit = p95 > 1500.0
        rows.append(
            {
                "venue": venue,
                "hour": hour,
                "p95_latency_ms": round(p95, 4),
                "mean_7d": None if mean_7d is None else round(mean_7d, 4),
                "std_7d": None if std_7d is None else round(std_7d, 4),
                "hit": bool(sigma_hit or abs_hit),
                "hit_basis": "sigma" if sigma_hit else ("absolute" if abs_hit else ""),
            }
        )

    consecutive = False
    by_venue: Dict[str, List[int]] = {}
    for row in rows:
        if row["hit"]:
            by_venue.setdefault(str(row["venue"]), []).append(int(row["hour"]))
    for hours in by_venue.values():
        hs = sorted(set(hours))
        if any((h + 1) in hs for h in hs):
            consecutive = True
            break

    return {
        "status": HIT if consecutive else PASS,
        "recommended_action": "REDUCE venue new-entry size 50%; retry wait 120 seconds" if consecutive else "NONE",
        "rule": "p95 latency > 7d mean + 3 sigma OR p95 > 1500ms for 2 consecutive buckets",
        "rows": rows,
    }


def _match_rate_for_date(ymd: str) -> Optional[float]:
    paths = [
        LOGS / f"fills_match_report_{ymd}.json",
        LOGS / f"kis_fills_sync_{ymd}.json",
    ]
    for p in paths:
        obj = _read_json(p)
        if not obj:
            continue
        val = (((obj.get("bridge") or {}).get("match_summary") or {}).get("order_match_rate"))
        if val is None:
            val = ((obj.get("rates") or {}).get("order_match_rate"))
        try:
            return float(val)
        except Exception:
            continue
    return None


def _build_slippage(ymd: str) -> Dict[str, Any]:
    ledger = _read_csv(ROOTB_LEDGER)
    if ledger.empty:
        return {"status": NOT_EVALUABLE, "reason": "missing_rootb_ledger", "rows": []}
    date_col = "date" if "date" in ledger.columns else "as_of"
    if date_col not in ledger.columns:
        return {"status": NOT_EVALUABLE, "reason": "ledger_missing_date_col", "rows": []}
    work = ledger[ledger[date_col].astype(str).str.slice(0, 8) == ymd].copy()
    if work.empty:
        return {"status": NOT_EVALUABLE, "reason": "no_ledger_rows_for_date", "rows": []}
    venue_col = "source" if "source" in work.columns else ""
    work["_venue"] = work[venue_col].astype(str).str.strip().replace("", "UNKNOWN") if venue_col else "UNKNOWN"
    if "slippage_actual_bps" in work.columns:
        slip = _to_num(work["slippage_actual_bps"]).abs()
    elif {"fill_price", "ref_close"}.issubset(work.columns):
        fp = _to_num(work["fill_price"])
        ref = _to_num(work["ref_close"])
        slip = ((fp - ref).abs() / ref) * 10000.0
    elif "slippage_bps_model" in work.columns:
        slip = _to_num(work["slippage_bps_model"]).abs()
    else:
        return {"status": NOT_EVALUABLE, "reason": "missing_slippage_columns", "rows": []}
    work["_slip_bps"] = slip
    work = work[work["_slip_bps"].notna()].copy()
    if work.empty:
        return {"status": NOT_EVALUABLE, "reason": "no_numeric_slippage_rows", "rows": []}
    match_rate = _match_rate_for_date(ymd)
    status = PASS
    reason = ""
    if match_rate is None:
        status = NOT_EVALUABLE
        reason = "missing_match_rate_evidence"
    rows: List[Dict[str, Any]] = []
    any_hit = False
    for venue, g in work.groupby("_venue", dropna=False):
        p90_bps = float(g["_slip_bps"].quantile(0.90))
        hit = bool(p90_bps > 35.0 and match_rate is not None and match_rate < 0.92)
        any_hit = any_hit or hit
        rows.append(
            {
                "venue": str(venue),
                "p90_slippage_bps": round(p90_bps, 4),
                "p90_slippage_pct": round(p90_bps / 10000.0, 6),
                "match_rate": match_rate,
                "rows": int(len(g)),
                "hit": hit,
            }
        )
    return {
        "status": HIT if any_hit else status,
        "reason": reason,
        "recommended_action": "Increase allowed spread multiplier 1.5x; switch IOC to FOK for affected venue" if any_hit else "NONE",
        "rule": "venue p90 slippage > 0.35% AND match_rate < 92%",
        "rows": rows,
    }


def _amount(df: pd.DataFrame, price_cols: List[str], qty_cols: List[str]) -> float:
    if df.empty:
        return 0.0
    price_col = next((c for c in price_cols if c in df.columns), "")
    qty_col = next((c for c in qty_cols if c in df.columns), "")
    if not price_col or not qty_col:
        return 0.0
    return float((_to_num(df[price_col]).fillna(0.0) * _to_num(df[qty_col]).fillna(0.0)).sum())


def _id_set(df: pd.DataFrame, cols: List[str]) -> set[str]:
    out: set[str] = set()
    if df.empty:
        return out
    for c in cols:
        if c not in df.columns:
            continue
        out.update(x for x in df[c].astype(str).str.strip().tolist() if x)
    return out


def _business_key_set(df: pd.DataFrame) -> set[str]:
    out: set[str] = set()
    if df.empty or not {"code", "side"}.issubset(df.columns):
        return out
    for _, row in df.iterrows():
        code = _norm_code(row.get("code", ""))
        side = str(row.get("side", "")).upper().strip()
        if code and side:
            out.add(f"{code}|{side}")
    return out


def _key_series(df: pd.DataFrame, date_col: str, price_col: str, qty_col: str) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=str)
    return (
        df[date_col].map(_date8)
        + "|"
        + df["code"].map(_norm_code)
        + "|"
        + df["side"].astype(str).str.upper().str.strip()
        + "|"
        + _to_num(df[qty_col]).fillna(0.0).map(lambda x: f"{float(x):.6f}")
        + "|"
        + _to_num(df[price_col]).fillna(0.0).map(lambda x: f"{float(x):.6f}")
    )


def _effective_fills_for_recon(ymd: str, fills_d: pd.DataFrame, ledger_d: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    kis = _read_csv(PAPER / f"kis_fills_api_{ymd}.csv")
    if kis.empty or ledger_d.empty:
        return fills_d, "paper_fills"
    required = {"date", "code", "side", "fill_qty", "fill_price"}
    if not required.issubset(kis.columns) or not required.issubset(ledger_d.columns):
        return fills_d, "paper_fills"
    kis_d = kis[kis["date"].astype(str).str.slice(0, 8) == ymd].copy()
    if kis_d.empty:
        return fills_d, "paper_fills"
    kis_d["_key"] = _key_series(kis_d, "date", "fill_price", "fill_qty")
    ledger_d = ledger_d.copy()
    ledger_d["_source"] = ledger_d.get("source", pd.Series([""] * len(ledger_d), index=ledger_d.index)).astype(str).str.upper()
    bridge = ledger_d[ledger_d["_source"].eq("KIS_API_BRIDGE")].copy()
    if bridge.empty:
        return fills_d, "paper_fills"
    bridge["_key"] = _key_series(bridge, "date", "fill_price", "fill_qty")
    matched = kis_d[kis_d["_key"].isin(set(bridge["_key"].astype(str)))].copy()
    if matched.empty:
        return fills_d, "paper_fills"

    effective = fills_d.copy()
    if not effective.empty and {"code", "side", "qty", "price"}.issubset(effective.columns):
        paper_keys = _key_series(effective, "datetime", "price", "qty")
        bridge_order_ids = _id_set(bridge, ["order_id", "source_order_id", "entry_order_id"])
        paper_order_ids = effective.get("order_id", pd.Series([""] * len(effective), index=effective.index)).astype(str).str.strip()
        effective = effective[~paper_order_ids.isin(bridge_order_ids) & ~paper_keys.isin(set(bridge["_key"].astype(str)))].copy()

    out = matched.rename(columns={"fill_qty": "qty", "fill_price": "price"}).copy()
    if "datetime" not in out.columns:
        out["datetime"] = out["date"].astype(str)
    if "order_id" not in out.columns:
        out["order_id"] = out.get("order_no", pd.Series([""] * len(out), index=out.index)).astype(str)
    keep = [c for c in effective.columns if c in out.columns]
    if keep:
        out = out.reindex(columns=effective.columns, fill_value="")
    combined = pd.concat([effective, out], ignore_index=True, sort=False)
    return combined.fillna(""), "paper_fills_plus_kis_api_bridge"


def _build_recon(ymd: str) -> Dict[str, Any]:
    orders_path = PAPER / f"orders_{ymd}_exec.xlsx"
    orders = _read_xlsx(orders_path)
    fills = _read_csv(PAPER / "fills.csv")
    ledger = _read_csv(ROOTB_LEDGER)
    if orders.empty:
        return {
            "status": FAIL,
            "recommended_action": "BLOCK new orders; isolate run_id; create forensic snapshot",
            "reason": "missing_orders_exec",
            "paths": {"orders_exec": str(orders_path)},
        }
    if fills.empty or ledger.empty:
        return {
            "status": NOT_EVALUABLE,
            "recommended_action": "BLOCK until fills and ledger evidence exist",
            "reason": "missing_fills_or_ledger",
            "paths": {"fills": str(PAPER / "fills.csv"), "ledger": str(ROOTB_LEDGER)},
        }

    fills_d = fills[fills["datetime"].astype(str).str.slice(0, 8) == ymd].copy() if "datetime" in fills.columns else pd.DataFrame()
    ledger_date_col = "date" if "date" in ledger.columns else "as_of"
    ledger_d = ledger[ledger[ledger_date_col].astype(str).str.slice(0, 8) == ymd].copy() if ledger_date_col in ledger.columns else pd.DataFrame()

    fills_basis = "paper_fills"
    fills_amount_df = fills_d
    fills_amount_df, fills_basis = _effective_fills_for_recon(ymd, fills_d, ledger_d)

    orders_ids = _id_set(orders, ["order_id", "entry_order_id", "source_order_id"])
    fills_ids = _id_set(fills_amount_df, ["order_id", "entry_order_id", "source_order_id"])
    ledger_ids = _id_set(ledger_d, ["order_id", "entry_order_id", "source_order_id"])
    base_ids = orders_ids or fills_ids
    raw_missing_in_fills = sorted(base_ids - fills_ids)
    raw_missing_in_ledger = sorted(base_ids - ledger_ids)
    raw_denom = max(len(base_ids), 1)
    raw_missing_rate = float(len(raw_missing_in_fills + raw_missing_in_ledger) / (raw_denom * 2.0))

    orders_keys = _business_key_set(orders)
    fills_keys = _business_key_set(fills_amount_df)
    ledger_keys = _business_key_set(ledger_d)
    base_keys = orders_keys or fills_keys
    missing_in_fills = sorted(base_keys - fills_keys)
    missing_in_ledger = sorted(base_keys - ledger_keys)
    denom = max(len(base_keys), 1)
    missing_rate = float(len(missing_in_fills + missing_in_ledger) / (denom * 2.0))

    fills_amount = _amount(fills_amount_df, ["price", "fill_price"], ["qty", "fill_qty"])
    ledger_amount = _amount(ledger_d, ["fill_price", "price"], ["fill_qty", "qty"])
    amount_diff_rate = 0.0
    if ledger_amount > 0:
        amount_diff_rate = abs(fills_amount - ledger_amount) / ledger_amount
    elif fills_amount > 0:
        amount_diff_rate = math.inf

    hit = bool(missing_rate > 0.001 or amount_diff_rate > 0.0002)
    return {
        "status": HIT if hit else PASS,
        "recommended_action": "Isolate run_id; create forensic snapshot; BLOCK new orders; resync" if hit else "NONE",
        "rule": "missing_rate > 0.1% OR abs(sum(fills)-sum(ledger))/sum(ledger) > 0.02%",
        "metrics": {
            "orders_id_count": int(len(orders_ids)),
            "fills_id_count": int(len(fills_ids)),
            "ledger_id_count": int(len(ledger_ids)),
            "orders_business_key_count": int(len(orders_keys)),
            "fills_business_key_count": int(len(fills_keys)),
            "ledger_business_key_count": int(len(ledger_keys)),
            "missing_rate": round(missing_rate, 8),
            "raw_id_missing_rate": round(raw_missing_rate, 8),
            "fills_amount": round(fills_amount, 4),
            "ledger_amount": round(ledger_amount, 4),
            "amount_diff_rate": None if math.isinf(amount_diff_rate) else round(amount_diff_rate, 8),
            "fills_amount_basis": fills_basis,
            "recon_key_basis": "business_key:code|side",
        },
        "issues": {
            "missing_in_fills": missing_in_fills[:200],
            "missing_in_ledger": missing_in_ledger[:200],
            "raw_id_missing_in_fills": raw_missing_in_fills[:200],
            "raw_id_missing_in_ledger": raw_missing_in_ledger[:200],
        },
        "paths": {
            "orders_exec": str(orders_path),
            "fills": str(PAPER / "fills.csv"),
            "ledger": str(ROOTB_LEDGER),
        },
    }


def _load_kis_account_positions() -> Tuple[pd.DataFrame, Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "snapshot_json": str(KIS_ACCOUNT_SNAPSHOT),
        "positions_csv": "",
        "source": "",
        "json_parse_ok": False,
    }
    snap = _read_json(KIS_ACCOUNT_SNAPSHOT)
    positions = snap.get("positions") if isinstance(snap.get("positions"), list) else []
    if positions:
        meta["json_parse_ok"] = True
        meta["source"] = "kis_account_snapshot_latest.json"
        meta["generated_at"] = ((snap.get("summary") or {}).get("generated_at") if isinstance(snap.get("summary"), dict) else snap.get("generated_at"))
        meta["mode"] = ((snap.get("summary") or {}).get("mode") if isinstance(snap.get("summary"), dict) else snap.get("mode"))
        return pd.DataFrame(positions).fillna(""), meta

    latest_csv = _latest_file("kis_account_positions_*.csv")
    if latest_csv is None:
        return pd.DataFrame(), meta
    meta["positions_csv"] = str(latest_csv)
    meta["source"] = "kis_account_positions_latest_csv"
    meta["generated_at"] = dt.datetime.fromtimestamp(latest_csv.stat().st_mtime).isoformat(timespec="seconds")
    return _read_csv(latest_csv), meta


def _latest_ledger_positions(ledger: pd.DataFrame, ymd: str) -> pd.DataFrame:
    if ledger.empty:
        return pd.DataFrame()
    required = {"code", "position_qty_after", "avg_cost_after_krw"}
    if not required.issubset(ledger.columns):
        return pd.DataFrame()
    date_col = "date" if "date" in ledger.columns else "as_of"
    if date_col not in ledger.columns:
        return pd.DataFrame()
    work = ledger[ledger[date_col].astype(str).map(_date8) <= str(ymd)].copy()
    if work.empty:
        return pd.DataFrame()
    if "datetime" in work.columns:
        work["_sort_ts"] = work["datetime"].astype(str)
    else:
        work["_sort_ts"] = work[date_col].astype(str)
    work["_input_seq"] = _to_num(work.get("input_seq", pd.Series([0] * len(work), index=work.index))).fillna(0.0)
    work = work.sort_values(["code", "_sort_ts", "_input_seq"]).copy()
    latest = work.groupby("code", as_index=False).tail(1).copy()
    latest["code"] = latest["code"].map(_norm_code)
    latest["ledger_qty"] = _to_num(latest["position_qty_after"]).fillna(0.0)
    latest["ledger_avg_cost"] = _to_num(latest["avg_cost_after_krw"]).fillna(0.0)
    latest = latest[(latest["code"] != "") & (latest["ledger_qty"] > 0)].copy()
    return latest[["code", "ledger_qty", "ledger_avg_cost"]]


def _build_account_pnl_drift(ymd: str) -> Dict[str, Any]:
    ledger = _read_csv(ROOTB_LEDGER)
    ledger_pos = _latest_ledger_positions(ledger, ymd)
    kis_pos, kis_meta = _load_kis_account_positions()
    if ledger_pos.empty or kis_pos.empty:
        return {
            "status": NOT_EVALUABLE,
            "reason": "missing_ledger_positions_or_kis_account_positions",
            "rule": "abs(local_open_unrealized_pnl - kis_open_unrealized_pnl) / max(abs(kis), abs(local), 1) > threshold",
            "metrics": {
                "ledger_open_positions": int(len(ledger_pos)),
                "kis_open_positions": int(len(kis_pos)),
            },
            "paths": {"ledger": str(ROOTB_LEDGER), **kis_meta},
        }

    kis = kis_pos.copy()
    kis["code"] = kis["code"].map(_norm_code) if "code" in kis.columns else ""
    kis["kis_qty"] = _to_num(kis.get("qty", pd.Series([0] * len(kis), index=kis.index))).fillna(0.0)
    kis["kis_avg_price"] = _to_num(kis.get("avg_price", pd.Series([0] * len(kis), index=kis.index))).fillna(0.0)
    kis["kis_last_price"] = _to_num(kis.get("last_price", pd.Series([0] * len(kis), index=kis.index))).fillna(0.0)
    if "unrealized_krw" in kis.columns:
        kis["kis_unrealized_krw"] = _to_num(kis["unrealized_krw"]).fillna(0.0)
    else:
        kis["kis_unrealized_krw"] = (kis["kis_last_price"] - kis["kis_avg_price"]) * kis["kis_qty"]
    kis = kis[(kis["code"] != "") & (kis["kis_qty"] > 0)].copy()
    merged = ledger_pos.merge(kis[["code", "kis_qty", "kis_last_price", "kis_unrealized_krw"]], on="code", how="inner")
    if merged.empty:
        return {
            "status": NOT_EVALUABLE,
            "reason": "no_common_open_position_codes",
            "rule": "compare common open-position codes only",
            "metrics": {
                "ledger_open_positions": int(len(ledger_pos)),
                "kis_open_positions": int(len(kis)),
                "matched_positions": 0,
            },
            "paths": {"ledger": str(ROOTB_LEDGER), **kis_meta},
        }

    merged["local_unrealized_krw"] = (merged["kis_last_price"] - merged["ledger_avg_cost"]) * merged["ledger_qty"]
    qty_mismatch = merged[(merged["ledger_qty"] - merged["kis_qty"]).abs() > 1e-9].copy()
    local_pnl = float(merged["local_unrealized_krw"].sum())
    kis_pnl = float(merged["kis_unrealized_krw"].sum())
    diff = local_pnl - kis_pnl
    denom = max(abs(local_pnl), abs(kis_pnl), 1.0)
    diff_rate = abs(diff) / denom
    threshold = max(0.0, _float(os.environ.get("DRIFT_ACCOUNT_PNL_MAX_DIFF_RATE", "0.005"), 0.005))
    min_abs = max(0.0, _float(os.environ.get("DRIFT_ACCOUNT_PNL_MIN_ABS_KRW", "50000"), 50000.0))
    hit = bool(abs(diff) > min_abs and diff_rate > threshold)
    status = HIT if hit else PASS
    return {
        "status": status,
        "reason": "" if status == PASS else "account_open_unrealized_pnl_drift",
        "recommended_action": "Verify KIS account snapshot, ledger open positions, fills bridge, and average cost basis" if hit else "NONE",
        "basis": "open_unrealized_pnl_common_codes_only",
        "rule": "abs(diff_krw) > min_abs_krw AND diff_rate > max_diff_rate",
        "metrics": {
            "ledger_open_positions": int(len(ledger_pos)),
            "kis_open_positions": int(len(kis)),
            "matched_positions": int(len(merged)),
            "qty_mismatch_count": int(len(qty_mismatch)),
            "local_open_unrealized_pnl_krw": round(local_pnl, 4),
            "kis_open_unrealized_pnl_krw": round(kis_pnl, 4),
            "diff_krw": round(diff, 4),
            "diff_rate": round(diff_rate, 8),
            "max_diff_rate": round(threshold, 8),
            "min_abs_krw": round(min_abs, 4),
        },
        "issues": {
            "qty_mismatch_codes": [
                {
                    "code": str(r.get("code")),
                    "ledger_qty": float(r.get("ledger_qty", 0.0)),
                    "kis_qty": float(r.get("kis_qty", 0.0)),
                }
                for _, r in qty_mismatch.head(200).iterrows()
            ],
        },
        "paths": {"ledger": str(ROOTB_LEDGER), **kis_meta},
    }


def build_payload(ymd: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    latency = _build_latency(ymd)
    slippage = _build_slippage(ymd)
    recon = _build_recon(ymd)
    account_pnl = _build_account_pnl_drift(ymd)
    checks_list = [
        ("latency_heatmap", latency),
        ("slippage_by_venue", slippage),
        ("recon_diff", recon),
        ("account_pnl_drift", account_pnl),
    ]
    statuses = [check.get("status") for _, check in checks_list]
    not_evaluable_checks = [name for name, check in checks_list if check.get("status") == NOT_EVALUABLE]
    evaluated_statuses = [s for s in statuses if s != NOT_EVALUABLE]
    if HIT in statuses or FAIL in statuses:
        overall = HIT
    elif evaluated_statuses and all(s == PASS for s in evaluated_statuses):
        overall = PASS
    elif not evaluated_statuses:
        overall = NOT_EVALUABLE
    else:
        overall = NOT_EVALUABLE
    payload = {
        "schema_version": "drift_monitor_v1",
        "generated_at": _now(),
        "as_of_ymd": ymd,
        "overall_status": overall,
        "not_evaluable_checks": not_evaluable_checks,
        "policy": "observer_report_only; fail_closed_recommendations_are_structured_not_applied",
        "checks": {
            "latency_heatmap": latency,
            "slippage_by_venue": slippage,
            "recon_diff": recon,
            "account_pnl_drift": account_pnl,
        },
    }
    return payload, latency, slippage, recon, account_pnl


def main() -> int:
    ap = argparse.ArgumentParser(description="Build SSOT drift monitor artifacts")
    ap.add_argument("--date", default="", help="YYYYMMDD")
    ap.add_argument("--fail-on-hit", action="store_true")
    args = ap.parse_args()
    ymd = _date8(args.date) or dt.datetime.now().strftime("%Y%m%d")
    if not re.fullmatch(r"\d{8}", ymd):
        print(f"[DRIFT] FAIL invalid date: {args.date}")
        return 2

    payload, latency, slippage, recon, account_pnl = build_payload(ymd)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = ROOT / "runs" / f"SSOT_D{ymd}_DRIFT_{stamp}"

    outputs = {
        LOGS / f"latency_heatmap_{ymd}.json": latency,
        LOGS / f"slippage_by_venue_{ymd}.json": slippage,
        LOGS / f"recon_diff_{ymd}.json": recon,
        LOGS / f"account_pnl_drift_{ymd}.json": account_pnl,
        LOGS / f"drift_monitor_{ymd}.json": payload,
        LOGS / "drift_monitor_latest.json": payload,
        run_dir / "latency_heatmap.json": latency,
        run_dir / "slippage_by_venue.json": slippage,
        run_dir / "recon_diff.json": recon,
        run_dir / "account_pnl_drift.json": account_pnl,
        run_dir / "drift_monitor.json": payload,
    }
    for path, data in outputs.items():
        _write_json(path, data)

    print(f"[DRIFT] wrote {LOGS / 'drift_monitor_latest.json'}")
    print(f"[DRIFT] run_dir={run_dir}")
    print(f"[DRIFT] overall_status={payload['overall_status']}")
    if args.fail_on_hit and payload["overall_status"] in {HIT, FAIL}:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
