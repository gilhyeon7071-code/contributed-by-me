import csv
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import surge_lob_ingest as lob_ingest  # noqa: E402

LOG_DIR = ROOT / "2_Logs"
WAIT_LOB_CSV = LOG_DIR / "surge_wait_lob_classification_latest.csv"
WAIT_LOB_JSON = LOG_DIR / "surge_wait_lob_classification_latest.json"
SURGE_REALTIME_CSV = LOG_DIR / "surge_realtime_latest.csv"
SURGE_REALTIME_JSON = LOG_DIR / "surge_realtime_latest.json"
INTRADAY_PRICES = LOG_DIR / "intraday_prices_latest.csv"
OUT_JSON = LOG_DIR / "surge_wait_lob_hoga_observe_latest.json"
OUT_CSV = LOG_DIR / "surge_wait_lob_hoga_observe_latest.csv"
RECHECK_JSON = LOG_DIR / "surge_no_lob_recheck_queue_latest.json"
RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
SCORE_RVOL_RECHECK_JSON = LOG_DIR / "surge_score_rvol_conditional_recheck_queue_latest.json"
SCORE_RVOL_RECHECK_CSV = LOG_DIR / "surge_score_rvol_conditional_recheck_queue_latest.csv"
HOGA_LEVELS = 10
MAX_RECHECK_SPREAD_BPS = 40.0
SCORE_RVOL_CONDITIONAL_SCORE_MIN = 90.0
SCORE_RVOL_CONDITIONAL_RVOL_MIN = 3.0
SCORE_RVOL_CONDITIONAL_RVOL_MAX = 5.0
SCORE_RVOL_REMAINING_HARD_BLOCKERS = {
    "ENTRY_CHANGE_BLOCK",
    "ENTRY_ATR_CAP",
    "HIGH_REJECTION_ENTRY_BLOCK",
    "ORDER_IMBALANCE_EXTREME",
    "MARKOUT_NEGATIVE_BLOCK",
    "KRX_ADMIN",
    "KRX_WARNING",
    "KRX_CAUTION",
    "TRADING_VALUE_FLOOR",
    "RVOL_OVERHEAT_BLOCK",
}
NO_LOB_REMAINING_HARD_BLOCKERS = SCORE_RVOL_REMAINING_HARD_BLOCKERS | {
    "SCORE_RVOL_OVERHEAT_BLOCK",
    "SPREAD_BLOCK",
    "ORDERFLOW_RISK_BLOCK",
    "ORDERFLOW_PAUSE",
    "KYLE_LAMBDA_Z_BLOCK",
    "OFI_NORM_EXTREME",
    "KRX_RISK",
    "TRADING_VALUE_BLOCK",
    "LOW_TRADING_VALUE",
    "NEWS_NEGATIVE",
    "NEWS_IMPLICATION_BLOCK",
    "FEATURE_BLOCK",
    "FEATURE_MISSING_BLOCK",
}


def _history_paths(ts: str) -> Dict[str, Path]:
    try:
        stamp = datetime.fromisoformat(str(ts or "")).strftime("%Y%m%d_%H%M%S")
    except Exception:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "json": LOG_DIR / f"surge_wait_lob_hoga_observe_{stamp}.json",
        "csv": LOG_DIR / f"surge_wait_lob_hoga_observe_{stamp}.csv",
    }


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        s = str(v or "").replace(",", "").strip()
        return float(s) if s else float(default)
    except Exception:
        return float(default)


def _delay_sec(start: Any, end: Any) -> float | str:
    try:
        start_ts = datetime.fromisoformat(str(start or "").strip())
        end_ts = datetime.fromisoformat(str(end or "").strip())
        return round((end_ts - start_ts).total_seconds(), 3)
    except Exception:
        return ""


def _truthy(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y"}


def _reason_keys(v: Any) -> List[str]:
    out: List[str] = []
    for part in str(v or "").split("|"):
        part = part.strip()
        if part:
            out.append(part.split(":", 1)[0].strip().upper())
    return out


def _score_rvol_rule_score(v: Any) -> float:
    match = re.search(r"SCORE_RVOL_OVERHEAT_BLOCK:score=([0-9.]+)", str(v or ""))
    if not match:
        return 0.0
    return _to_float(match.group(1), 0.0)


def _is_wait_lob_realtime_row(row: pd.Series) -> bool:
    if not _truthy(row.get("detected_surge_flag")):
        return False
    if _truthy(row.get("entry_allowed")):
        return False
    return "NO_LOB_BLOCK" in set(_reason_keys(row.get("exclude_reasons")))


def _load_wait_lob_rows() -> pd.DataFrame:
    if SURGE_REALTIME_CSV.exists():
        try:
            rt = pd.read_csv(SURGE_REALTIME_CSV, dtype={"code": str})
            if not rt.empty and "code" in rt.columns:
                rt["code"] = rt["code"].astype(str).str.zfill(6)
                rt = rt[rt.apply(_is_wait_lob_realtime_row, axis=1)].copy()
                if not rt.empty:
                    rt["wait_lob_class"] = "DATA_COVERAGE_GAP_NO_LOB"
                    rt["wait_lob_note"] = "Latest realtime detector row contains NO_LOB_BLOCK"
                    if "detected_surge_type" not in rt.columns and "surge_type" in rt.columns:
                        rt["detected_surge_type"] = rt["surge_type"]
                    return rt.drop_duplicates(subset=["code"], keep="first")
        except Exception:
            pass
    if WAIT_LOB_CSV.exists():
        df = pd.read_csv(WAIT_LOB_CSV, dtype={"code": str})
        if df.empty or "code" not in df.columns:
            return pd.DataFrame()
        df["code"] = df["code"].astype(str).str.zfill(6)
        if "wait_lob_class" in df.columns:
            df = df[df["wait_lob_class"].astype(str).str.strip() != ""].copy()
        return df.drop_duplicates(subset=["code"], keep="first")
    return pd.DataFrame()


def _load_intraday_context(codes: List[str]) -> pd.DataFrame:
    base = pd.DataFrame({"code": [str(c).zfill(6) for c in codes]})
    if INTRADAY_PRICES.exists():
        try:
            px = pd.read_csv(INTRADAY_PRICES, dtype={"code": str})
            if not px.empty and "code" in px.columns:
                px["code"] = px["code"].astype(str).str.zfill(6)
                keep = [c for c in ["code", "current_price", "open", "high", "low", "volume"] if c in px.columns]
                base = base.merge(px[keep], on="code", how="left")
        except Exception:
            pass
    for col in ["current_price", "open", "high", "low", "volume"]:
        if col not in base.columns:
            base[col] = 0.0
        base[col] = pd.to_numeric(base[col], errors="coerce").fillna(0.0)
    return base


def _classify_probe(row: pd.Series) -> str:
    status = str(row.get("hoga_fetch_status") or "").strip().upper()
    lob_available = _truthy(row.get("lob_available"))
    orderflow_tag = str(row.get("orderflow_tag") or "").strip().upper()
    orderflow_risk = _to_float(row.get("orderflow_risk_score"), 0.0)
    spread_bps = _to_float(row.get("spread_bps"), 0.0)
    if status == "OK" and lob_available:
        if spread_bps > MAX_RECHECK_SPREAD_BPS or orderflow_tag in {"PAUSE", "CAUTION"} or orderflow_risk >= 0.60:
            return "LOB_COLLECTED_RISKY"
        return "LOB_COLLECTED_RECHECKABLE"
    if status == "ERROR":
        return "PROBE_ERROR"
    if status == "SKIP_MAX_FETCH":
        return "NOT_PROBED_MAX_FETCH"
    if status == "SKIP_SOFT_TIMEOUT":
        return "NOT_PROBED_SOFT_TIMEOUT"
    if status == "WS_HOGA":
        return "WS_HOGA_EXISTING"
    if status == "EMPTY":
        return "PROBE_EMPTY"
    if status == "CLIENT_ERROR":
        return "PROBE_CLIENT_ERROR"
    return f"PROBE_STATUS_{status or 'EMPTY'}"


def _build_lob_metrics(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["current_price"] + lob_ingest._hoga_numeric_columns():
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    spread = (df["ask1"] - df["bid1"]).clip(lower=0.0)
    mid = ((df["ask1"] + df["bid1"]) / 2.0).where((df["ask1"] > 0) & (df["bid1"] > 0), df["current_price"].clip(lower=0.0))
    df["mid_price"] = mid.fillna(0.0)
    df["spread"] = spread
    df["spread_bps"] = (spread / mid.replace(0, float("nan")) * 10000.0).fillna(0.0)
    denom = (df["bidq1"] + df["askq1"]).replace(0, float("nan"))
    df["order_imbalance_l1"] = ((df["bidq1"] - df["askq1"]) / denom).fillna(0.0)
    ask_depth = pd.Series(0, index=df.index, dtype="int64")
    for level in range(1, HOGA_LEVELS + 1):
        ask_depth += ((df[f"ask{level}"] > 0) & (df[f"askq{level}"] > 0)).astype(int)
    df["ask_depth_levels"] = ask_depth
    lob_mask = (df["ask1"] > 0) & (df["bid1"] > 0)
    bid_only_limit_mask = (df["ask1"] <= 0) & (df["bid1"] > 0) & (df["current_price"] > 0) & (df["bid1"] >= df["current_price"])
    df["lob_available"] = lob_mask | bid_only_limit_mask
    df["lob_status"] = "NO_LOB"
    df.loc[lob_mask, "lob_status"] = "OK"
    df.loc[bid_only_limit_mask, "lob_status"] = "BID_ONLY_LIMIT"
    if "orderflow_tag" not in df.columns:
        df["orderflow_tag"] = "NO_HISTORY"
    if "orderflow_risk_score" not in df.columns:
        df["orderflow_risk_score"] = 0.0
    df["probe_class"] = df.apply(_classify_probe, axis=1)
    return df


def _build_recheck_queue(out: pd.DataFrame) -> pd.DataFrame:
    if out.empty or "probe_class" not in out.columns:
        return pd.DataFrame()
    q = out[out["probe_class"].astype(str).eq("LOB_COLLECTED_RECHECKABLE")].copy()
    if q.empty:
        return pd.DataFrame()
    q["recheck_action"] = "RECHECK_ONLY"
    q["recheck_reason"] = "NO_LOB_BLOCK_LOB_COLLECTED"
    q["entry_approval_changed"] = False
    q["policy_change"] = False
    q["queue_scope"] = "observe_only_no_lob_recheck"
    keep = [
        "ts",
        "first_no_lob_ts",
        "lob_confirm_ts",
        "lob_confirm_delay_sec",
        "date",
        "code",
        "detected_surge_type",
        "surge_score_final",
        "change_pct",
        "day_range_pct",
        "intraday_high_drawdown_pct",
        "intraday_low_rebound_pct",
        "intraday_range_position_pct",
        "rvol20",
        "trading_value",
        "entry_decision",
        "entry_reason",
        "exclude_reasons",
        "lob_available",
        "lob_status",
        "spread_bps",
        "order_imbalance_l1",
        "ask_depth_levels",
        "orderflow_tag",
        "orderflow_risk_score",
        "probe_class",
        "recheck_action",
        "recheck_reason",
        "entry_approval_changed",
        "policy_change",
        "queue_scope",
    ]
    for col in keep:
        if col not in q.columns:
            q[col] = ""
    q["_score"] = pd.to_numeric(q["surge_score_final"], errors="coerce").fillna(0.0)
    q = q.sort_values(["_score", "code"], ascending=[False, True]).drop(columns=["_score"], errors="ignore")
    return q[keep].copy()


def _build_score_rvol_conditional_queue(out: pd.DataFrame) -> pd.DataFrame:
    if out.empty or "probe_class" not in out.columns:
        return pd.DataFrame()
    q = out[out["probe_class"].astype(str).eq("LOB_COLLECTED_RECHECKABLE")].copy()
    if q.empty:
        return pd.DataFrame()

    q["_reason_keys"] = q["exclude_reasons"].apply(lambda v: set(_reason_keys(v)))
    q["_remaining_hard_blockers"] = q["_reason_keys"].apply(
        lambda keys: sorted(k for k in keys if k in SCORE_RVOL_REMAINING_HARD_BLOCKERS)
    )
    q["_score"] = q["exclude_reasons"].apply(_score_rvol_rule_score)
    q["score_rvol_rule_score"] = q["_score"]
    q["_rvol20"] = pd.to_numeric(q.get("rvol20", 0.0), errors="coerce").fillna(0.0)
    mask = (
        q["_reason_keys"].apply(lambda keys: "SCORE_RVOL_OVERHEAT_BLOCK" in keys)
        & q["_remaining_hard_blockers"].apply(lambda xs: len(xs) == 0)
        & q["lob_status"].astype(str).str.upper().eq("OK")
        & (q["_score"] >= SCORE_RVOL_CONDITIONAL_SCORE_MIN)
        & (q["_rvol20"] >= SCORE_RVOL_CONDITIONAL_RVOL_MIN)
        & (q["_rvol20"] < SCORE_RVOL_CONDITIONAL_RVOL_MAX)
    )
    q = q[mask].copy()
    if q.empty:
        return pd.DataFrame()

    q["recheck_action"] = "CONDITIONAL_RECHECK_ONLY"
    q["recheck_reason"] = "SCORE_RVOL_LOB_CONFIRMED_NO_REMAINING_HARD_BLOCK"
    q["resolved_blockers"] = q["_reason_keys"].apply(lambda keys: "NO_LOB_BLOCK" if "NO_LOB_BLOCK" in keys else "")
    q["remaining_hard_blockers"] = q["_remaining_hard_blockers"].apply(lambda xs: "|".join(xs))
    q["entry_approval_changed"] = False
    q["policy_change"] = False
    q["queue_scope"] = "observe_only_score_rvol_conditional_recheck"
    keep = [
        "ts",
        "first_no_lob_ts",
        "lob_confirm_ts",
        "lob_confirm_delay_sec",
        "date",
        "code",
        "detected_surge_type",
        "surge_score_final",
        "score_rvol_rule_score",
        "change_pct",
        "day_range_pct",
        "intraday_high_drawdown_pct",
        "intraday_low_rebound_pct",
        "intraday_range_position_pct",
        "rvol20",
        "trading_value",
        "entry_decision",
        "entry_reason",
        "exclude_reasons",
        "resolved_blockers",
        "remaining_hard_blockers",
        "lob_available",
        "lob_status",
        "spread_bps",
        "order_imbalance_l1",
        "ask_depth_levels",
        "orderflow_tag",
        "orderflow_risk_score",
        "probe_class",
        "recheck_action",
        "recheck_reason",
        "entry_approval_changed",
        "policy_change",
        "queue_scope",
    ]
    for col in keep:
        if col not in q.columns:
            q[col] = ""
    q = q.sort_values(["_score", "code"], ascending=[False, True]).drop(
        columns=["_reason_keys", "_remaining_hard_blockers", "_score", "_rvol20"],
        errors="ignore",
    )
    return q[keep].copy()


def _score_rvol_conditional_diagnostics(out: pd.DataFrame, eligible_rows: int) -> Dict[str, Any]:
    if out.empty or "probe_class" not in out.columns:
        return {
            "lob_recheckable_rows": 0,
            "score_rvol_rows": 0,
            "eligible_rows": int(eligible_rows),
            "reject_reason_counts": [],
            "remaining_hard_blocker_counts": [],
        }
    q = out[out["probe_class"].astype(str).eq("LOB_COLLECTED_RECHECKABLE")].copy()
    if q.empty:
        return {
            "lob_recheckable_rows": 0,
            "score_rvol_rows": 0,
            "eligible_rows": int(eligible_rows),
            "reject_reason_counts": [],
            "remaining_hard_blocker_counts": [],
        }
    q["_reason_keys"] = q["exclude_reasons"].apply(lambda v: set(_reason_keys(v)))
    q = q[q["_reason_keys"].apply(lambda keys: "SCORE_RVOL_OVERHEAT_BLOCK" in keys)].copy()
    reject_counts: Counter[str] = Counter()
    blocker_counts: Counter[str] = Counter()
    for _, row in q.iterrows():
        keys = row["_reason_keys"]
        hard = sorted(k for k in keys if k in SCORE_RVOL_REMAINING_HARD_BLOCKERS)
        for token in hard:
            blocker_counts[token] += 1
        rvol20 = _to_float(row.get("rvol20"), 0.0)
        rule_score = _score_rvol_rule_score(row.get("exclude_reasons"))
        if hard:
            reject_counts["remaining_hard_blocker"] += 1
        if str(row.get("lob_status") or "").upper() != "OK":
            reject_counts["lob_not_normal_ok"] += 1
        if rule_score < SCORE_RVOL_CONDITIONAL_SCORE_MIN:
            reject_counts["score_below_min"] += 1
        if rvol20 < SCORE_RVOL_CONDITIONAL_RVOL_MIN:
            reject_counts["rvol_below_min"] += 1
        if rvol20 >= SCORE_RVOL_CONDITIONAL_RVOL_MAX:
            reject_counts["rvol_over_max"] += 1
    return {
        "lob_recheckable_rows": int((out["probe_class"].astype(str) == "LOB_COLLECTED_RECHECKABLE").sum()),
        "score_rvol_rows": int(len(q)),
        "eligible_rows": int(eligible_rows),
        "reject_reason_counts": [{"reason": k, "count": int(v)} for k, v in reject_counts.most_common()],
        "remaining_hard_blocker_counts": [{"blocker": k, "count": int(v)} for k, v in blocker_counts.most_common()],
    }


def _no_lob_recheck_diagnostics(out: pd.DataFrame) -> Dict[str, Any]:
    if out.empty or "probe_class" not in out.columns:
        return {
            "lob_recheckable_rows": 0,
            "clean_after_no_lob_removed_rows": 0,
            "remaining_hard_blocker_counts": [],
            "lob_status_counts": [],
            "policy_change": False,
            "entry_approval_changed": False,
        }
    q = out[out["probe_class"].astype(str).eq("LOB_COLLECTED_RECHECKABLE")].copy()
    if q.empty:
        return {
            "lob_recheckable_rows": 0,
            "clean_after_no_lob_removed_rows": 0,
            "remaining_hard_blocker_counts": [],
            "lob_status_counts": [],
            "policy_change": False,
            "entry_approval_changed": False,
        }
    q["_reason_keys"] = q["exclude_reasons"].apply(lambda v: set(_reason_keys(v)))
    blocker_counts: Counter[str] = Counter()
    clean_rows = 0
    for _, row in q.iterrows():
        remaining = sorted(
            k
            for k in row["_reason_keys"]
            if k != "NO_LOB_BLOCK" and k in NO_LOB_REMAINING_HARD_BLOCKERS
        )
        if remaining:
            for token in remaining:
                blocker_counts[token] += 1
        elif str(row.get("lob_status") or "").upper() == "OK":
            clean_rows += 1
    lob_status_counts = Counter(q["lob_status"].astype(str).fillna("MISSING"))
    return {
        "lob_recheckable_rows": int(len(q)),
        "clean_after_no_lob_removed_rows": int(clean_rows),
        "remaining_hard_blocker_counts": [{"blocker": k, "count": int(v)} for k, v in blocker_counts.most_common()],
        "lob_status_counts": [{"status": k, "count": int(v)} for k, v in lob_status_counts.most_common()],
        "policy_change": False,
        "entry_approval_changed": False,
    }


def main() -> int:
    ts = datetime.now().isoformat(timespec="seconds")
    os.environ.setdefault("SURGE_LOB_HOGA_MAX_FETCH", "20")
    os.environ.setdefault("SURGE_LOB_HOGA_SOFT_TIMEOUT_SEC", "40")
    os.environ.setdefault("SURGE_LOB_KIS_SLEEP_SEC", "0.35")

    wait_df = _load_wait_lob_rows()
    if wait_df.empty:
        payload = {
            "ts": ts,
            "status": "NO_WAIT_LOB_ROWS",
            "source_csv": str(WAIT_LOB_CSV),
            "policy_change": False,
            "entry_approval_changed": False,
            "rows": 0,
        }
        OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        pd.DataFrame().to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        return 0

    rt = _load_intraday_context(wait_df["code"].astype(str).tolist())
    for col in lob_ingest._hoga_numeric_columns():
        rt[col] = 0.0
    rt["hoga_fetch_status"] = ""
    rt["hoga_fetch_error"] = ""
    rt = lob_ingest._fill_hoga_from_ws(rt)
    rt = lob_ingest._fill_hoga_from_kis(rt)
    rt = _build_lob_metrics(rt)

    overlap_cols = {
        "current_price",
        "hoga_fetch_status",
        "hoga_fetch_error",
        "lob_available",
        "lob_status",
        "spread_bps",
        "ask_depth_levels",
        "orderflow_tag",
        "orderflow_risk_score",
        "probe_class",
    }
    wait_base = wait_df.drop(columns=[c for c in overlap_cols if c in wait_df.columns])
    out = wait_base.merge(
        rt[
            [
                "code",
                "current_price",
                "hoga_fetch_status",
                "hoga_fetch_error",
                "lob_available",
                "lob_status",
                "spread_bps",
                "order_imbalance_l1",
                "ask_depth_levels",
                "orderflow_tag",
                "orderflow_risk_score",
                "probe_class",
            ]
        ],
        on="code",
        how="left",
    )
    out["first_no_lob_ts"] = out.get("ts", "")
    out["lob_confirm_ts"] = ts
    out["lob_confirm_delay_sec"] = out["first_no_lob_ts"].apply(lambda v: _delay_sec(v, ts))
    out = out.sort_values(["probe_class", "detected_surge_type", "code"], ascending=[True, True, True])
    recheck = _build_recheck_queue(out)
    score_rvol_recheck = _build_score_rvol_conditional_queue(out)
    score_rvol_diag = _score_rvol_conditional_diagnostics(out, len(score_rvol_recheck))
    no_lob_diag = _no_lob_recheck_diagnostics(out)

    class_counts = Counter(out["probe_class"].astype(str).fillna("MISSING"))
    hoga_counts = Counter(out["hoga_fetch_status"].astype(str).fillna("MISSING"))
    history = _history_paths(ts)
    recheck_history_json = LOG_DIR / f"surge_no_lob_recheck_queue_{history['json'].stem.rsplit('_', 2)[-2]}_{history['json'].stem.rsplit('_', 1)[-1]}.json"
    recheck_history_csv = LOG_DIR / f"surge_no_lob_recheck_queue_{history['csv'].stem.rsplit('_', 2)[-2]}_{history['csv'].stem.rsplit('_', 1)[-1]}.csv"
    score_rvol_history_json = LOG_DIR / f"surge_score_rvol_conditional_recheck_queue_{history['json'].stem.rsplit('_', 2)[-2]}_{history['json'].stem.rsplit('_', 1)[-1]}.json"
    score_rvol_history_csv = LOG_DIR / f"surge_score_rvol_conditional_recheck_queue_{history['csv'].stem.rsplit('_', 2)[-2]}_{history['csv'].stem.rsplit('_', 1)[-1]}.csv"
    payload = {
        "ts": ts,
        "status": "OK",
        "scope": "observe_only_wait_lob_hoga_probe",
        "source_surge_realtime_json": str(SURGE_REALTIME_JSON),
        "source_surge_realtime_csv": str(SURGE_REALTIME_CSV),
        "source_wait_lob_json": str(WAIT_LOB_JSON),
        "source_wait_lob_csv": str(WAIT_LOB_CSV),
        "max_fetch": int(os.getenv("SURGE_LOB_HOGA_MAX_FETCH", "20") or "20"),
        "soft_timeout_sec": float(os.getenv("SURGE_LOB_HOGA_SOFT_TIMEOUT_SEC", "40") or "40"),
        "sleep_sec": float(os.getenv("SURGE_LOB_KIS_SLEEP_SEC", "0.35") or "0.35"),
        "rows": int(len(out)),
        "class_counts": [{"class": k, "count": int(v)} for k, v in class_counts.most_common()],
        "hoga_fetch_status_counts": [{"status": k, "count": int(v)} for k, v in hoga_counts.most_common()],
        "recheck_queue_rows": int(len(recheck)),
        "no_lob_recheck_diagnostics": no_lob_diag,
        "recheck_queue_json": str(RECHECK_JSON),
        "recheck_queue_csv": str(RECHECK_CSV),
        "score_rvol_conditional_recheck_rows": int(len(score_rvol_recheck)),
        "score_rvol_conditional_diagnostics": score_rvol_diag,
        "score_rvol_conditional_recheck_json": str(SCORE_RVOL_RECHECK_JSON),
        "score_rvol_conditional_recheck_csv": str(SCORE_RVOL_RECHECK_CSV),
        "policy_change": False,
        "entry_approval_changed": False,
        "out_csv": str(OUT_CSV),
        "history_json": str(history["json"]),
        "history_csv": str(history["csv"]),
    }
    recheck_payload = {
        "ts": ts,
        "status": "OK",
        "scope": "observe_only_no_lob_recheck_queue",
        "rows": int(len(recheck)),
        "source_observe_json": str(OUT_JSON),
        "source_observe_csv": str(OUT_CSV),
        "policy_change": False,
        "entry_approval_changed": False,
        "no_lob_recheck_diagnostics": no_lob_diag,
        "queue_rule": "probe_class == LOB_COLLECTED_RECHECKABLE",
        "next_action": "review/recheck only; do not treat as entry approval",
        "out_csv": str(RECHECK_CSV),
        "history_json": str(recheck_history_json),
        "history_csv": str(recheck_history_csv),
    }
    score_rvol_payload = {
        "ts": ts,
        "status": "OK",
        "scope": "observe_only_score_rvol_conditional_recheck_queue",
        "rows": int(len(score_rvol_recheck)),
        "items": score_rvol_recheck.to_dict(orient="records"),
        "source_observe_json": str(OUT_JSON),
        "source_observe_csv": str(OUT_CSV),
        "policy_change": False,
        "entry_approval_changed": False,
        "queue_rule": (
            "probe_class == LOB_COLLECTED_RECHECKABLE and "
            "SCORE_RVOL_OVERHEAT_BLOCK and no remaining hard blockers and "
            "lob_status==OK and score>=90 and 3<=rvol20<5"
        ),
        "diagnostics": score_rvol_diag,
        "resolved_blocker_rule": "NO_LOB_BLOCK is treated as resolved only after normal two-sided LOB collection",
        "next_action": "conditional review/recheck only; do not treat as entry approval",
        "out_csv": str(SCORE_RVOL_RECHECK_CSV),
        "history_json": str(score_rvol_history_json),
        "history_csv": str(score_rvol_history_csv),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    history["json"].write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    out.to_csv(history["csv"], index=False, encoding="utf-8-sig")
    RECHECK_JSON.write_text(json.dumps(recheck_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    recheck_history_json.write_text(json.dumps(recheck_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    recheck.to_csv(RECHECK_CSV, index=False, encoding="utf-8-sig")
    recheck.to_csv(recheck_history_csv, index=False, encoding="utf-8-sig")
    SCORE_RVOL_RECHECK_JSON.write_text(json.dumps(score_rvol_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    score_rvol_history_json.write_text(json.dumps(score_rvol_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    score_rvol_recheck.to_csv(SCORE_RVOL_RECHECK_CSV, index=False, encoding="utf-8-sig")
    score_rvol_recheck.to_csv(score_rvol_history_csv, index=False, encoding="utf-8-sig")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
