"""Explain why recheckable LOB codes are missed by latest LOB ingest.

This report mirrors the current surge_lob_ingest priority formula in a
read-only way and overlays the NO_LOB recheck queue. It does not change fetch
limits, target selection, or entry policy.
"""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

INTRADAY_PRICES = LOG_DIR / "intraday_prices_latest.csv"
MARKET_RISING = LOG_DIR / "market_rising_latest.csv"
SURGE_REALTIME_JSON = LOG_DIR / "surge_realtime_latest.json"
RECHECK_CSV = LOG_DIR / "surge_no_lob_recheck_queue_latest.csv"
LATEST_LOB_CSV = LOG_DIR / "surge_lob_latest.csv"
NORMAL_CANDIDATES_CSV = LOG_DIR / "candidates_latest_data.with_final_score.csv"
NORMAL_ACTION_PLAN_CSV = LOG_DIR / "candidate_action_plan_latest.csv"
NORMAL_ENTRY_DECISION_CSV = LOG_DIR / "entry_decision_layers_runtime_latest.csv"

OUT_JSON = LOG_DIR / "lob_ingest_priority_diagnostic_latest.json"
OUT_CSV = LOG_DIR / "lob_ingest_priority_diagnostic_latest.csv"

KST = timezone(timedelta(hours=9))


def _now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _load_surge_priority_codes() -> Set[str]:
    if not SURGE_REALTIME_JSON.exists():
        return set()
    try:
        payload = json.loads(SURGE_REALTIME_JSON.read_text(encoding="utf-8"))
    except Exception:
        return set()
    alerts = payload.get("alerts") if isinstance(payload, dict) else None
    if not isinstance(alerts, list):
        return set()
    out: Set[str] = set()
    for item in alerts:
        if not isinstance(item, dict):
            continue
        code = _code(item.get("code"))
        if len(code) == 6:
            out.add(code)
    return out


def _load_normal_candidate_code_sources() -> Dict[str, Set[str]]:
    sources: Dict[str, Set[str]] = {
        "execution_pool": set(),
        "action_plan_buy_candidate": set(),
        "entry_decision_normal_recheck": set(),
    }
    candidate_rows = _read_csv(NORMAL_CANDIDATES_CSV)
    for row in candidate_rows:
        code = _code(row.get("code"))
        if len(code) == 6 and _truthy(row.get("execution_pool")):
            sources["execution_pool"].add(code)
    for row in _read_csv(NORMAL_ACTION_PLAN_CSV):
        code = _code(row.get("code"))
        if len(code) != 6:
            continue
        if str(row.get("source") or "").strip().lower() != "daily_candidate":
            continue
        if str(row.get("next_action") or "").strip().upper() != "BUY_CANDIDATE":
            continue
        if not _truthy(row.get("trading_allowed")):
            continue
        sources["action_plan_buy_candidate"].add(code)
    for row in _read_csv(NORMAL_ENTRY_DECISION_CSV):
        code = _code(row.get("code"))
        if len(code) != 6:
            continue
        if not str(row.get("execution_reason") or "").strip().upper().startswith("NORMAL_"):
            continue
        if "positive_entry_ok" in row and not _truthy(row.get("positive_entry_ok")):
            continue
        sources["entry_decision_normal_recheck"].add(code)
    return sources


def _normal_candidate_supplement() -> pd.DataFrame:
    source_codes = _load_normal_candidate_code_sources()
    target_codes = set().union(*source_codes.values()) if source_codes else set()
    if not target_codes:
        return pd.DataFrame()
    rows = _read_csv(NORMAL_CANDIDATES_CSV)
    selected = [row for row in rows if _code(row.get("code")) in target_codes]
    if not selected:
        return pd.DataFrame()
    df = pd.DataFrame(selected)
    out = pd.DataFrame()
    out["code"] = df["code"].astype(str).str.zfill(6)
    close = pd.to_numeric(df.get("close", 0), errors="coerce").fillna(0.0)
    value = pd.to_numeric(df.get("value", 0), errors="coerce").fillna(0.0)
    volume = (value / close.replace(0, float("nan"))).fillna(0.0)
    out["current_price"] = close
    out["open"] = close
    out["high"] = close
    out["low"] = close
    out["volume"] = volume
    return out[out["current_price"] > 0].copy()


def _priority(
    row: Dict[str, Any],
    surge_codes: Set[str],
    recheck_codes: Set[str],
    normal_candidate_codes: Set[str],
    recheck_bonus: float,
    normal_candidate_bonus: float,
) -> float:
    code = _code(row.get("code"))
    score = 1000.0 if code in surge_codes else 0.0
    if code in recheck_codes:
        score += max(0.0, recheck_bonus)
    if code in normal_candidate_codes:
        score += max(0.0, normal_candidate_bonus)
    current = _f(row.get("current_price"))
    open_px = _f(row.get("open"))
    high = _f(row.get("high"))
    low = _f(row.get("low"))
    volume = _f(row.get("volume"))
    if current > 0 and open_px > 0:
        score += abs(current / open_px - 1.0) * 100.0
    if current > 0 and high > 0 and low > 0:
        score += max(0.0, (high - low) / current) * 50.0
    score += min(volume / 1_000_000.0, 20.0)
    return float(score)


def _load_universe() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    if INTRADAY_PRICES.exists():
        try:
            frames.append(pd.read_csv(INTRADAY_PRICES, dtype={"code": str}))
        except Exception:
            pass
    if MARKET_RISING.exists():
        try:
            mdf = pd.read_csv(MARKET_RISING, dtype={"code": str})
            if not mdf.empty:
                out = pd.DataFrame()
                out["code"] = mdf["code"].astype(str).str.zfill(6)
                out["current_price"] = pd.to_numeric(mdf.get("current_price", mdf.get("price", 0)), errors="coerce").fillna(0.0)
                out["open"] = pd.to_numeric(mdf.get("open", out["current_price"]), errors="coerce").fillna(out["current_price"])
                out["high"] = pd.to_numeric(mdf.get("high", out["current_price"]), errors="coerce").fillna(out["current_price"])
                out["low"] = pd.to_numeric(mdf.get("low", out["current_price"]), errors="coerce").fillna(out["current_price"])
                out["volume"] = pd.to_numeric(mdf.get("volume", 0), errors="coerce").fillna(0.0)
                frames.append(out)
        except Exception:
            pass
    normal = _normal_candidate_supplement()
    if not normal.empty:
        frames.append(normal)
    if not frames:
        return pd.DataFrame(columns=["code", "current_price", "open", "high", "low", "volume"])
    df = pd.concat(frames, ignore_index=True, sort=False)
    df["code"] = df["code"].astype(str).str.zfill(6)
    for col in ["current_price", "open", "high", "low", "volume"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df.drop_duplicates(subset=["code"], keep="first")


def _by_code(rows: Iterable[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _code(row.get("code"))
        if code:
            out[code] = row
    return out


def main() -> int:
    generated_at = _now()
    max_fetch_env = os.getenv("SURGE_LOB_HOGA_MAX_FETCH", "")
    configured_max_fetch = int(float(max_fetch_env)) if str(max_fetch_env).strip() else 14
    recheck_bonus = float(str(os.getenv("SURGE_LOB_RECHECK_PRIORITY_BONUS", "950")).strip() or "950")
    normal_candidate_bonus = float(str(os.getenv("SURGE_LOB_NORMAL_CANDIDATE_PRIORITY_BONUS", "1100")).strip() or "1100")
    surge_codes = _load_surge_priority_codes()
    normal_code_sources = _load_normal_candidate_code_sources()
    normal_candidate_codes = set().union(*normal_code_sources.values()) if normal_code_sources else set()
    universe = _load_universe()
    recheck_by_code = _by_code(_read_csv(RECHECK_CSV))
    latest_by_code = _by_code(_read_csv(LATEST_LOB_CSV))

    if universe.empty:
        ranked = pd.DataFrame(columns=["code", "current_priority", "current_rank"])
    else:
        records: List[Dict[str, Any]] = []
        for row in universe.to_dict(orient="records"):
            records.append({
                "code": _code(row.get("code")),
                "current_priority": _priority(
                    row,
                    surge_codes,
                    set(recheck_by_code),
                    normal_candidate_codes,
                    recheck_bonus,
                    normal_candidate_bonus,
                ),
                "current_price": _f(row.get("current_price")),
                "open": _f(row.get("open")),
                "high": _f(row.get("high")),
                "low": _f(row.get("low")),
                "volume": _f(row.get("volume")),
                "in_surge_priority_codes": _code(row.get("code")) in surge_codes,
                "in_normal_candidate_codes": _code(row.get("code")) in normal_candidate_codes,
            })
        ranked = pd.DataFrame(records).sort_values("current_priority", ascending=False).reset_index(drop=True)
        ranked["current_rank"] = ranked.index + 1

    rank_by_code = {str(row["code"]): row for row in ranked.to_dict(orient="records")}
    rows: List[Dict[str, Any]] = []
    target_codes = sorted(set(recheck_by_code) | normal_candidate_codes)
    for code in target_codes:
        recheck = recheck_by_code.get(code, {})
        rank = rank_by_code.get(code, {})
        latest = latest_by_code.get(code, {})
        current_rank = int(rank.get("current_rank", 0) or 0)
        would_be_fetched = bool(current_rank and current_rank <= configured_max_fetch)
        latest_status = str(latest.get("hoga_fetch_status") or "").strip()
        normal_sources = [
            name
            for name, codes in normal_code_sources.items()
            if code in codes
        ]
        target_parts = []
        if code in recheck_by_code:
            target_parts.append("NO_LOB_RECHECK")
        if normal_sources:
            target_parts.append("NORMAL_CANDIDATE")
        rows.append({
            "code": code,
            "diagnostic_target": "+".join(target_parts),
            "diagnostic_class": "WITHIN_CURRENT_FETCH_BUDGET" if would_be_fetched else "OUTSIDE_CURRENT_FETCH_BUDGET",
            "current_rank": current_rank,
            "configured_max_fetch": configured_max_fetch,
            "rank_gap_to_fetch": max(0, current_rank - configured_max_fetch) if current_rank else None,
            "current_priority": round(_f(rank.get("current_priority")), 6),
            "in_surge_priority_codes": bool(rank.get("in_surge_priority_codes")),
            "in_normal_candidate_codes": bool(rank.get("in_normal_candidate_codes")),
            "normal_candidate_sources": ",".join(normal_sources),
            "recheck_probe_class": recheck.get("probe_class", ""),
            "recheck_lob_status": recheck.get("lob_status", ""),
            "recheck_spread_bps": round(_f(recheck.get("spread_bps")), 6),
            "latest_hoga_fetch_status": latest_status,
            "latest_lob_available": _truthy(latest.get("lob_available")),
            "latest_lob_status": latest.get("lob_status", ""),
            "latest_spread_bps": round(_f(latest.get("spread_bps")), 6),
            "trading_allowed": False,
            "policy_effect": False,
            "policy_change_applied": False,
        })
    rows.sort(key=lambda row: (row.get("current_rank") or 999999, row.get("code") or ""))
    payload = {
        "generated_at": generated_at,
        "scope": "read_only_lob_ingest_priority_diagnostic",
        "policy_note": "Diagnostic only. No live entry, order route, threshold, or gate behavior is changed.",
        "source_files": {
            "intraday_prices": str(INTRADAY_PRICES),
            "market_rising": str(MARKET_RISING),
            "surge_realtime_json": str(SURGE_REALTIME_JSON),
            "recheck_csv": str(RECHECK_CSV),
            "latest_lob_csv": str(LATEST_LOB_CSV),
        },
        "summary": {
            "universe_rows": int(len(ranked)),
            "recheck_rows": int(len(rows)),
            "configured_max_fetch_assumed": configured_max_fetch,
            "recheck_priority_bonus": recheck_bonus,
            "normal_candidate_priority_bonus": normal_candidate_bonus,
            "within_fetch_budget_rows": sum(1 for row in rows if row.get("diagnostic_class") == "WITHIN_CURRENT_FETCH_BUDGET"),
            "outside_fetch_budget_rows": sum(1 for row in rows if row.get("diagnostic_class") == "OUTSIDE_CURRENT_FETCH_BUDGET"),
            "recheck_codes_in_surge_priority_codes": sum(1 for row in rows if row.get("in_surge_priority_codes")),
            "normal_candidate_rows": sum(1 for row in rows if row.get("in_normal_candidate_codes")),
            "normal_candidate_within_fetch_budget_rows": sum(
                1
                for row in rows
                if row.get("in_normal_candidate_codes")
                and row.get("diagnostic_class") == "WITHIN_CURRENT_FETCH_BUDGET"
            ),
            "trading_effect": False,
            "policy_effect": False,
            "policy_change_applied": False,
        },
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows)
    print(json.dumps({"out_json": str(OUT_JSON), "out_csv": str(OUT_CSV), "summary": payload["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
