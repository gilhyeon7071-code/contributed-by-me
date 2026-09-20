# -*- coding: utf-8 -*-
"""Validation-only realtime follow-through signal builder.

This script does not place orders and is not consumed by paper_engine yet.
It joins the latest normal candidates with the latest intraday price snapshot
and writes follow-through alerts for review.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


_MARKET_OPEN_H = 9.0
_MARKET_CLOSE_H = 15.5
_MARKET_HOURS = _MARKET_CLOSE_H - _MARKET_OPEN_H  # 6.5h
PSEUDO_INTRADAY_RULE_VERSION = "close_gt_open__high_ge_3pct__low_ge_minus4pct__stretch_le_1_15"
_ELAPSED_FRAC_MIN = 0.05  # 최소값: 0으로 나누기 방지

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
DEFAULT_CANDIDATES = LOG_DIR / "candidates_latest_data.with_final_score.csv"
FALLBACK_CANDIDATES = LOG_DIR / "candidates_latest_data.csv"
DEFAULT_INTRADAY = LOG_DIR / "intraday_prices_latest.csv"
OUT_CSV = LOG_DIR / "followthrough_realtime_latest.csv"
OUT_JSON = LOG_DIR / "followthrough_realtime_latest.json"
OUT_HISTORY_CSV = LOG_DIR / "followthrough_realtime_history.csv"
OUT_HISTORY_JSONL = LOG_DIR / "followthrough_realtime_history.jsonl"
OUT_MID_TRACK_CSV = LOG_DIR / "followthrough_mid_tracker.csv"
OUT_MID_TRACK_JSONL = LOG_DIR / "followthrough_mid_tracker.jsonl"
OUT_MID_FIRST_SEEN_JSON = LOG_DIR / "followthrough_mid_first_seen_cache.json"
OUT_MID_STATUS_CSV = LOG_DIR / "followthrough_mid_status.csv"
OUT_MID_STATUS_JSONL = LOG_DIR / "followthrough_mid_status.jsonl"
OUT_OBS_TRACK_CSV = LOG_DIR / "followthrough_observation_tracker.csv"
OUT_OBS_TRACK_JSONL = LOG_DIR / "followthrough_observation_tracker.jsonl"
OUT_OBS_FIRST_SEEN_JSON = LOG_DIR / "followthrough_observation_first_seen_cache.json"

ALERT_OUTPUT_COLUMNS = [
    "code",
    "name",
    "followthrough_score",
    "current_price",
    "prev_close",
    "open",
    "current_vs_open_pct",
    "current_vs_prev_close_pct",
    "low_from_open_pct",
    "high_from_open_pct",
    "atr14_pct",
    "stretch",
    "value",
    "score",
    "final_score",
    "entry_timing",
    "blocked_by_risk",
    "block_reasons",
    "entry_allowed_validation",
    "pseudo_intraday_core",
    "pseudo_intraday_stretch_ok",
    "pseudo_intraday_candidate",
    "pseudo_intraday_rule",
]

HISTORY_COLUMNS = [
    "ts",
    "status",
    "candidates_rows",
    "intraday_rows",
    "joined_rows",
    "alerts_count",
    "buy_signal_price_count",
    "buy_signal_liquidity_count",
    "buy_signal_count",
    "alpha_pool_count",
    "alpha_pool_blocked_by_risk_count",
    "strict_entry_allowed_count",
    "pseudo_intraday_candidate_count",
    "entry_allowed_validation_count",
    "observation_track_count",
    "mid_track_count",
    "mid_status_count",
    "out_csv",
    "out_json",
]

OBS_TRACK_COLUMNS = [
    "ts",
    "observation_group",
    "code",
    "name",
    "current_price",
    "first_seen_ts",
    "first_seen_price",
    "ret_from_first_pct",
    "current_vs_open_pct",
    "current_vs_prev_close_pct",
    "low_from_open_pct",
    "high_from_open_pct",
    "atr14_pct",
    "stretch",
    "trading_value",
    "trading_value_projected",
    "daily_value",
    "liquidity_gap_krw",
    "buy_signal_price",
    "buy_signal_liquidity",
    "buy_signal",
    "entry_allowed_validation",
    "block_reasons",
    "pseudo_intraday_core",
    "pseudo_intraday_stretch_ok",
    "pseudo_intraday_candidate",
    "pseudo_intraday_rule",
]


def _elapsed_market_frac(now: datetime | None = None) -> float:
    t = now or datetime.now()
    hour = t.hour + t.minute / 60.0 + t.second / 3600.0
    return max(_ELAPSED_FRAC_MIN, min(1.0, (hour - _MARKET_OPEN_H) / _MARKET_HOURS))


def _num(v: Any, default: float = 0.0) -> float:
    try:
        out = float(v)
    except Exception:
        return float(default)
    if pd.isna(out):
        return float(default)
    return float(out)


def _norm_code(v: Any) -> str:
    digits = "".join(ch for ch in str(v or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype={"code": str}, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path, dtype={"code": str})


def _pick_candidates_path(raw: str | None) -> Path:
    if raw:
        return Path(raw)
    if DEFAULT_CANDIDATES.exists():
        return DEFAULT_CANDIDATES
    return FALLBACK_CANDIDATES


def _status_payload(status: str, **extra: Any) -> dict[str, Any]:
    payload = {"ts": datetime.now().isoformat(timespec="seconds"), "status": status}
    payload.update(extra)
    return payload


def _load_json_dict(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): dict(v) for k, v in data.items() if isinstance(v, dict)}


def _write_json_dict(path: Path, data: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    tmp.replace(path)


def _csv_header_matches(path: Path, columns: list[str]) -> bool:
    if not path.exists():
        return True
    try:
        first_line = path.open("r", encoding="utf-8-sig").readline().strip()
    except Exception:
        return False
    return first_line.split(",") == columns


def _rebuild_observation_csv_from_jsonl(obs_csv: Path, obs_jsonl: Path) -> None:
    if not obs_jsonl.exists():
        pd.DataFrame(columns=OBS_TRACK_COLUMNS).to_csv(obs_csv, index=False, encoding="utf-8-sig")
        return
    rows = []
    with obs_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    pd.DataFrame(rows).reindex(columns=OBS_TRACK_COLUMNS).to_csv(obs_csv, index=False, encoding="utf-8-sig")


def _seed_mid_first_seen_cache(track_csv: Path) -> dict[str, dict[str, Any]]:
    first_seen: dict[str, dict[str, Any]] = {}
    if not track_csv.exists():
        return first_seen
    try:
        prev = pd.read_csv(track_csv, dtype={"code": str}, encoding="utf-8-sig")
    except Exception:
        return first_seen
    for _, row in prev.iterrows():
        code = _norm_code(row.get("code"))
        if code and code not in first_seen:
            first_seen[code] = {
                "first_seen_ts": str(row.get("first_seen_ts") or row.get("ts") or ""),
                "first_seen_price": _num(row.get("first_seen_price"), _num(row.get("current_price"))),
                "name": str(row.get("name", "")),
            }
    return first_seen


def _seed_observation_first_seen_cache(obs_csv: Path) -> dict[str, dict[str, Any]]:
    first_seen: dict[str, dict[str, Any]] = {}
    if not obs_csv.exists():
        return first_seen
    try:
        prev = pd.read_csv(obs_csv, dtype={"code": str, "observation_group": str}, encoding="utf-8-sig")
    except Exception:
        return first_seen
    for _, row in prev.iterrows():
        code = _norm_code(row.get("code"))
        group = str(row.get("observation_group") or "")
        key = f"{code}:{group}" if code and group else ""
        if key and key not in first_seen:
            first_seen[key] = {
                "first_seen_ts": str(row.get("first_seen_ts") or row.get("ts") or ""),
                "first_seen_price": _num(row.get("first_seen_price"), _num(row.get("current_price"))),
            }
    return first_seen


def _append_history(payload: dict[str, Any], history_csv: Path, history_jsonl: Path) -> None:
    history_csv.parent.mkdir(parents=True, exist_ok=True)
    history_jsonl.parent.mkdir(parents=True, exist_ok=True)
    if history_csv.exists() and not _csv_header_matches(history_csv, HISTORY_COLUMNS):
        try:
            prev = pd.read_csv(history_csv)
            for col in HISTORY_COLUMNS:
                if col not in prev.columns:
                    prev[col] = ""
            prev.reindex(columns=HISTORY_COLUMNS).to_csv(history_csv, index=False, encoding="utf-8-sig")
        except Exception:
            history_csv.rename(history_csv.with_suffix(history_csv.suffix + ".schema_mismatch.bak"))
    diagnostics = payload.get("diagnostics") or {}
    row = {
        "ts": payload.get("ts", ""),
        "status": payload.get("status", ""),
        "candidates_rows": payload.get("candidates_rows", 0),
        "intraday_rows": payload.get("intraday_rows", 0),
        "joined_rows": payload.get("joined_rows", 0),
        "alerts_count": payload.get("alerts_count", 0),
        "buy_signal_price_count": diagnostics.get("buy_signal_price_count", 0),
        "buy_signal_liquidity_count": diagnostics.get("buy_signal_liquidity_count", 0),
        "buy_signal_count": diagnostics.get("buy_signal_count", 0),
        "alpha_pool_count": diagnostics.get("alpha_pool_count", 0),
        "alpha_pool_blocked_by_risk_count": diagnostics.get("alpha_pool_blocked_by_risk_count", 0),
        "strict_entry_allowed_count": diagnostics.get("strict_entry_allowed_count", 0),
        "pseudo_intraday_candidate_count": diagnostics.get("pseudo_intraday_candidate_count", 0),
        "entry_allowed_validation_count": diagnostics.get("entry_allowed_validation_count", 0),
        "observation_track_count": payload.get("observation_track_count", 0),
        "mid_track_count": payload.get("mid_track_count", 0),
        "mid_status_count": payload.get("mid_status_count", 0),
        "out_csv": payload.get("out_csv", ""),
        "out_json": payload.get("out_json", ""),
    }
    pd.DataFrame([row]).reindex(columns=HISTORY_COLUMNS).to_csv(
        history_csv,
        mode="a",
        header=not history_csv.exists(),
        index=False,
        encoding="utf-8-sig",
    )
    with history_jsonl.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _append_mid_tracker(
    payload: dict[str, Any],
    track_csv: Path,
    track_jsonl: Path,
    first_seen_json: Path,
) -> dict[str, Any]:
    risk = ((payload.get("diagnostics") or {}).get("price_risk_grade_compare") or {})
    rows = [r for r in (risk.get("rows") or []) if str(r.get("price_risk_grade", "")).upper() == "MID"]
    track_csv.parent.mkdir(parents=True, exist_ok=True)
    track_jsonl.parent.mkdir(parents=True, exist_ok=True)

    first_seen = _load_json_dict(first_seen_json)
    if not first_seen:
        first_seen = _seed_mid_first_seen_cache(track_csv)
        if first_seen:
            _write_json_dict(first_seen_json, first_seen)

    out_rows = []
    ts = str(payload.get("ts", ""))
    cache_changed = False
    for row in rows:
        code = _norm_code(row.get("code"))
        current_price = _num(row.get("current_price"))
        prior = first_seen.get(code) or {"first_seen_ts": ts, "first_seen_price": current_price}
        if code and code not in first_seen:
            first_seen[code] = {
                "first_seen_ts": str(prior.get("first_seen_ts", ts)),
                "first_seen_price": _num(prior.get("first_seen_price"), current_price),
                "name": str(row.get("name", "")),
            }
            cache_changed = True
        first_price = _num(prior.get("first_seen_price"))
        ret_from_first = (current_price / first_price - 1.0) if first_price > 0 else 0.0
        out_rows.append(
            {
                "ts": ts,
                "code": code,
                "name": str(row.get("name", "")),
                "price_risk_grade": "MID",
                "current_price": current_price,
                "first_seen_ts": str(prior.get("first_seen_ts", ts)),
                "first_seen_price": first_price,
                "ret_from_first_pct": ret_from_first,
                "current_vs_open_pct": _num(row.get("current_vs_open_pct")),
                "current_vs_prev_close_pct": _num(row.get("current_vs_prev_close_pct")),
                "low_from_open_pct": _num(row.get("low_from_open_pct")),
                "high_from_open_pct": _num(row.get("high_from_open_pct")),
                "atr14_pct": _num(row.get("atr14_pct")),
                "stretch": _num(row.get("stretch")),
                "trading_value": _num(row.get("trading_value")),
                "daily_value": _num(row.get("daily_value")),
                "original_block_reasons": str(row.get("original_block_reasons", "")),
            }
        )

    if out_rows:
        pd.DataFrame(out_rows).to_csv(
            track_csv,
            mode="a",
            header=not track_csv.exists(),
            index=False,
            encoding="utf-8-sig",
        )
        with track_jsonl.open("a", encoding="utf-8") as f:
            for row in out_rows:
                f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    if cache_changed:
        _write_json_dict(first_seen_json, first_seen)
    return {
        "mid_track_count": int(len(out_rows)),
        "mid_track_csv": str(track_csv),
        "mid_track_jsonl": str(track_jsonl),
        "mid_first_seen_json": str(first_seen_json),
    }


def _append_mid_status(
    payload: dict[str, Any],
    current_diag: pd.DataFrame,
    first_seen_json: Path,
    status_csv: Path,
    status_jsonl: Path,
) -> dict[str, Any]:
    status_csv.parent.mkdir(parents=True, exist_ok=True)
    status_jsonl.parent.mkdir(parents=True, exist_ok=True)
    first_seen = _load_json_dict(first_seen_json)
    tracked_codes = set(first_seen.keys())

    risk = ((payload.get("diagnostics") or {}).get("price_risk_grade_compare") or {})
    current_rows = {_norm_code(r.get("code")): r for r in (risk.get("rows") or []) if _norm_code(r.get("code"))}
    diag_rows: dict[str, dict[str, Any]] = {}
    if isinstance(current_diag, pd.DataFrame) and not current_diag.empty and "code" in current_diag.columns:
        for _, row in current_diag.iterrows():
            code = _norm_code(row.get("code"))
            if code:
                diag_rows[code] = row.to_dict()
    ts = str(payload.get("ts", ""))
    out_rows = []
    for code in sorted(tracked_codes):
        row = current_rows.get(code, {})
        diag = diag_rows.get(code, {})
        prior = first_seen.get(code, {})
        grade = str(row.get("price_risk_grade", "") or "")
        source = row if row else diag
        current_price = _num(source.get("current_price"))
        first_price = _num(prior.get("first_seen_price"))
        ret_from_first = (current_price / first_price - 1.0) if current_price > 0 and first_price > 0 else 0.0
        if not row:
            buy_signal_price = str(diag.get("buy_signal_price", "")).strip().upper() == "TRUE"
            buy_signal_liquidity = str(diag.get("buy_signal_liquidity", "")).strip().upper() == "TRUE"
            buy_signal = str(diag.get("buy_signal", "")).strip().upper() == "TRUE"
            if diag and not buy_signal_price:
                status = "DROPPED_PRICE_SIGNAL"
                reason = "buy_signal_price_false"
            elif diag and not buy_signal_liquidity:
                status = "DROPPED_LIQUIDITY_SIGNAL"
                reason = "buy_signal_liquidity_false"
            elif diag and not buy_signal:
                status = "DROPPED_BUY_SIGNAL"
                reason = "buy_signal_false"
            else:
                status = "DROPPED_NO_CURRENT_ROW"
                reason = "not_present_in_current_diagnostics"
        elif grade == "MID":
            status = "MID_ACTIVE"
            reason = "price_risk_grade_mid"
        elif grade == "HIGH":
            status = "DROPPED_HIGH_RISK"
            reason = "price_risk_grade_high"
        elif grade == "LOW":
            status = "UPGRADED_LOW_RISK"
            reason = "price_risk_grade_low"
        else:
            status = "DROPPED_UNKNOWN"
            reason = "price_risk_grade_missing"
        out_rows.append(
            {
                "ts": ts,
                "code": code,
                "name": str(row.get("name") or prior.get("name") or ""),
                "status": status,
                "reason": reason,
                "price_risk_grade": grade,
                "current_price": current_price,
                "first_seen_ts": str(prior.get("first_seen_ts", "")),
                "first_seen_price": first_price,
                "ret_from_first_pct": ret_from_first,
                "current_vs_open_pct": _num(source.get("current_vs_open_pct")),
                "current_vs_prev_close_pct": _num(source.get("current_vs_prev_close_pct")),
                "low_from_open_pct": _num(source.get("low_from_open_pct")),
                "atr14_pct": _num(source.get("atr14_pct")),
                "stretch": _num(source.get("stretch")),
                "trading_value": _num(source.get("trading_value")),
                "daily_value": _num(source.get("daily_value"), _num(source.get("value"))),
                "original_block_reasons": str(source.get("original_block_reasons") or source.get("block_reasons") or ""),
            }
        )

    if out_rows:
        pd.DataFrame(out_rows).to_csv(
            status_csv,
            mode="a",
            header=not status_csv.exists(),
            index=False,
            encoding="utf-8-sig",
        )
        with status_jsonl.open("a", encoding="utf-8") as f:
            for row in out_rows:
                f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    counts: dict[str, int] = {}
    for row in out_rows:
        key = str(row.get("status", ""))
        counts[key] = counts.get(key, 0) + 1
    return {
        "mid_status_count": int(len(out_rows)),
        "mid_status_counts": counts,
        "mid_status_csv": str(status_csv),
        "mid_status_jsonl": str(status_jsonl),
    }


def _observation_group(row: dict[str, Any]) -> str:
    entry_allowed = str(row.get("entry_allowed_validation", "")).strip().upper() == "TRUE"
    buy_signal = str(row.get("buy_signal", "")).strip().upper() == "TRUE"
    buy_signal_price = str(row.get("buy_signal_price", "")).strip().upper() == "TRUE"
    buy_signal_liquidity = str(row.get("buy_signal_liquidity", "")).strip().upper() == "TRUE"
    high_stretch = "high_stretch" in str(row.get("block_reasons", ""))
    high_score = "high_score" in str(row.get("block_reasons", ""))
    if entry_allowed:
        return "STRICT"
    if buy_signal and not high_stretch and not high_score:
        return "SOFT"
    if buy_signal_price and not high_stretch and not high_score:
        return "PRICE_ONLY"
    near_open = _num(row.get("current_vs_open_pct")) >= 0.0
    near_close = _num(row.get("current_vs_prev_close_pct")) >= 0.0
    if buy_signal_liquidity and (buy_signal_price or (near_open and near_close)):
        return "NEAR"
    return ""


def _append_observation_tracker(
    current_diag: pd.DataFrame,
    obs_csv: Path,
    obs_jsonl: Path,
    first_seen_json: Path,
) -> dict[str, Any]:
    obs_csv.parent.mkdir(parents=True, exist_ok=True)
    obs_jsonl.parent.mkdir(parents=True, exist_ok=True)
    first_seen = _load_json_dict(first_seen_json)
    if not first_seen:
        first_seen = _seed_observation_first_seen_cache(obs_csv)
        if first_seen:
            _write_json_dict(first_seen_json, first_seen)

    out_rows = []
    if not isinstance(current_diag, pd.DataFrame) or current_diag.empty:
        return {
            "observation_track_count": 0,
            "observation_group_counts": {},
            "observation_track_csv": str(obs_csv),
            "observation_track_jsonl": str(obs_jsonl),
        }
    ts = datetime.now().isoformat(timespec="seconds")
    cache_changed = False
    for _, series in current_diag.iterrows():
        row = series.to_dict()
        group = _observation_group(row)
        if not group:
            continue
        code = _norm_code(row.get("code"))
        if not code:
            continue
        current_price = _num(row.get("current_price"))
        key = f"{code}:{group}"
        prior = first_seen.get(key) or {"first_seen_ts": ts, "first_seen_price": current_price}
        if key not in first_seen:
            first_seen[key] = {
                "first_seen_ts": str(prior.get("first_seen_ts", ts)),
                "first_seen_price": _num(prior.get("first_seen_price"), current_price),
            }
            cache_changed = True
        first_price = _num(prior.get("first_seen_price"))
        ret_from_first = (current_price / first_price - 1.0) if current_price > 0 and first_price > 0 else 0.0
        out_rows.append(
            {
                "ts": ts,
                "observation_group": group,
                "code": code,
                "name": str(row.get("name", "")),
                "current_price": current_price,
                "first_seen_ts": str(prior.get("first_seen_ts", ts)),
                "first_seen_price": first_price,
                "ret_from_first_pct": ret_from_first,
                "current_vs_open_pct": _num(row.get("current_vs_open_pct")),
                "current_vs_prev_close_pct": _num(row.get("current_vs_prev_close_pct")),
                "low_from_open_pct": _num(row.get("low_from_open_pct")),
                "atr14_pct": _num(row.get("atr14_pct")),
                "stretch": _num(row.get("stretch")),
                "trading_value": _num(row.get("trading_value")),
                "trading_value_projected": _num(row.get("trading_value_projected", row.get("trading_value"))),
                "daily_value": _num(row.get("daily_value"), _num(row.get("value"))),
                "liquidity_gap_krw": max(0.0, _num(row.get("min_intraday_trading_value_krw")) - _num(row.get("trading_value_projected", row.get("trading_value")))),
                "buy_signal_price": bool(str(row.get("buy_signal_price", "")).strip().upper() == "TRUE"),
                "buy_signal_liquidity": bool(str(row.get("buy_signal_liquidity", "")).strip().upper() == "TRUE"),
                "buy_signal": bool(str(row.get("buy_signal", "")).strip().upper() == "TRUE"),
                "entry_allowed_validation": bool(str(row.get("entry_allowed_validation", "")).strip().upper() == "TRUE"),
                "block_reasons": str(row.get("block_reasons", "")),
                "pseudo_intraday_core": bool(str(row.get("pseudo_intraday_core", "")).strip().upper() == "TRUE"),
                "pseudo_intraday_stretch_ok": bool(str(row.get("pseudo_intraday_stretch_ok", "")).strip().upper() == "TRUE"),
                "pseudo_intraday_candidate": bool(str(row.get("pseudo_intraday_candidate", "")).strip().upper() == "TRUE"),
                "pseudo_intraday_rule": str(row.get("pseudo_intraday_rule", "")),
            }
        )
    if out_rows:
        if not _csv_header_matches(obs_csv, OBS_TRACK_COLUMNS):
            _rebuild_observation_csv_from_jsonl(obs_csv, obs_jsonl)
        pd.DataFrame(out_rows).reindex(columns=OBS_TRACK_COLUMNS).to_csv(
            obs_csv,
            mode="a",
            header=not obs_csv.exists(),
            index=False,
            encoding="utf-8-sig",
        )
        with obs_jsonl.open("a", encoding="utf-8") as f:
            for row in out_rows:
                f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    if cache_changed:
        _write_json_dict(first_seen_json, first_seen)
    counts: dict[str, int] = {}
    for row in out_rows:
        group = str(row.get("observation_group", ""))
        counts[group] = counts.get(group, 0) + 1
    return {
        "observation_track_count": int(len(out_rows)),
        "observation_group_counts": counts,
        "observation_track_csv": str(obs_csv),
        "observation_track_jsonl": str(obs_jsonl),
        "observation_first_seen_json": str(first_seen_json),
    }


def _block_ablation(work: pd.DataFrame, block_cols: list[str]) -> list[dict[str, Any]]:
    base = work["buy_signal"].fillna(False)
    out: list[dict[str, Any]] = []
    scenarios: list[tuple[str, list[str]]] = [("strict_all_blocks", [])]
    scenarios.extend((f"relax_only_{c.replace('block_', '')}", [c]) for c in block_cols)
    scenarios.extend(
        [
            (
                "relax_daily_value_and_low_from_open",
                ["block_low_daily_value", "block_low_from_open"],
            ),
            (
                "relax_daily_value_and_atr",
                ["block_low_daily_value", "block_high_atr"],
            ),
            (
                "relax_daily_value_low_from_open_atr",
                ["block_low_daily_value", "block_low_from_open", "block_high_atr"],
            ),
            ("relax_all_blocks", list(block_cols)),
        ]
    )
    for name, relaxed in scenarios:
        active_blocks = [c for c in block_cols if c not in relaxed]
        if active_blocks:
            allowed = base & ~work[active_blocks].any(axis=1)
        else:
            allowed = base.copy()
        rows = []
        for _, row in work.loc[allowed].iterrows():
            rows.append(
                {
                    "code": str(row.get("code", "")),
                    "name": str(row.get("name", "")),
                    "current_vs_open_pct": round(_num(row.get("current_vs_open_pct")), 6),
                    "current_vs_prev_close_pct": round(_num(row.get("current_vs_prev_close_pct")), 6),
                    "trading_value": _num(row.get("trading_value")),
                    "value": _num(row.get("value")),
                    "atr14_pct": round(_num(row.get("atr14_pct")), 6),
                    "stretch": round(_num(row.get("stretch")), 6),
                    "score": round(_num(row.get("score")), 6),
                    "original_block_reasons": str(row.get("block_reasons", "")),
                }
            )
        out.append(
            {
                "scenario": name,
                "relaxed_blocks": relaxed,
                "active_blocks": active_blocks,
                "allowed_count": int(allowed.sum()),
                "rows": rows,
            }
        )
    return out


def _value_replacement_compare(work: pd.DataFrame) -> list[dict[str, Any]]:
    scenarios = [
        (
            "strict_daily_value_block",
            [
                "block_low_from_open",
                "block_high_atr",
                "block_high_stretch",
                "block_low_daily_value",
                "block_high_score",
            ],
        ),
        (
            "replace_daily_value_with_intraday_liquidity",
            [
                "block_low_from_open",
                "block_high_atr",
                "block_high_stretch",
                "block_high_score",
            ],
        ),
        (
            "intraday_liquidity_with_soft_price_risk",
            [
                "block_high_stretch",
                "block_high_score",
            ],
        ),
    ]
    out: list[dict[str, Any]] = []
    base = work["buy_signal"].fillna(False)
    for name, active_blocks in scenarios:
        allowed = base & ~work[active_blocks].any(axis=1)
        rows = []
        for _, row in work.loc[allowed].iterrows():
            rows.append(
                {
                    "code": str(row.get("code", "")),
                    "name": str(row.get("name", "")),
                    "current_vs_open_pct": round(_num(row.get("current_vs_open_pct")), 6),
                    "current_vs_prev_close_pct": round(_num(row.get("current_vs_prev_close_pct")), 6),
                    "trading_value": _num(row.get("trading_value")),
                    "daily_value": _num(row.get("value")),
                    "low_from_open_pct": round(_num(row.get("low_from_open_pct")), 6),
                    "atr14_pct": round(_num(row.get("atr14_pct")), 6),
                    "stretch": round(_num(row.get("stretch")), 6),
                    "score": round(_num(row.get("score")), 6),
                    "original_block_reasons": str(row.get("block_reasons", "")),
                }
            )
        out.append(
            {
                "scenario": name,
                "active_blocks": active_blocks,
                "allowed_count": int(allowed.sum()),
                "rows": rows,
            }
        )
    return out


def _price_risk_grade(low_from_open_pct: float, atr14_pct: float, max_low_from_open_pct: float, max_atr_pct: float) -> str:
    low_limit = -abs(float(max_low_from_open_pct))
    soft_low_limit = low_limit * 2.0
    atr_limit = float(max_atr_pct)
    soft_atr_limit = atr_limit * 1.35
    if low_from_open_pct < soft_low_limit or atr14_pct > soft_atr_limit:
        return "HIGH"
    if low_from_open_pct < low_limit or atr14_pct > atr_limit:
        return "MID"
    return "LOW"


def _risk_grade_compare(
    work: pd.DataFrame,
    *,
    max_low_from_open_pct: float,
    max_atr_pct: float,
) -> dict[str, Any]:
    rows = []
    counts = {"LOW": 0, "MID": 0, "HIGH": 0}
    for _, row in work.loc[work["buy_signal"]].iterrows():
        grade = _price_risk_grade(
            _num(row.get("low_from_open_pct")),
            _num(row.get("atr14_pct")),
            max_low_from_open_pct,
            max_atr_pct,
        )
        counts[grade] += 1
        rows.append(
            {
                "code": str(row.get("code", "")),
                "name": str(row.get("name", "")),
                "price_risk_grade": grade,
                "current_price": _num(row.get("current_price")),
                "current_vs_open_pct": round(_num(row.get("current_vs_open_pct")), 6),
                "current_vs_prev_close_pct": round(_num(row.get("current_vs_prev_close_pct")), 6),
                "low_from_open_pct": round(_num(row.get("low_from_open_pct")), 6),
                "atr14_pct": round(_num(row.get("atr14_pct")), 6),
                "stretch": round(_num(row.get("stretch")), 6),
                "trading_value": _num(row.get("trading_value")),
                "daily_value": _num(row.get("value")),
                "original_block_reasons": str(row.get("block_reasons", "")),
            }
        )
    return {
        "counts": counts,
        "rows": rows,
        "grade_rule": {
            "LOW": "low_from_open and ATR both within strict thresholds",
            "MID": "low_from_open or ATR breaches strict threshold but not soft threshold",
            "HIGH": "low_from_open or ATR breaches soft threshold",
            "strict_low_from_open_limit": -abs(float(max_low_from_open_pct)),
            "soft_low_from_open_limit": -abs(float(max_low_from_open_pct)) * 2.0,
            "strict_atr_limit": float(max_atr_pct),
            "soft_atr_limit": float(max_atr_pct) * 1.35,
        },
    }


def build_signals(
    candidates_path: Path,
    intraday_path: Path,
    *,
    min_current_vs_open_pct: float,
    min_current_vs_prev_close_pct: float,
    max_low_from_open_pct: float,
    max_atr_pct: float,
    max_stretch: float,
    min_value_krw: float,
    min_intraday_trading_value_krw: float,
    max_score: float,
    top_n: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not candidates_path.exists():
        return pd.DataFrame(), _status_payload("MISSING_CANDIDATES", path=str(candidates_path))
    if not intraday_path.exists():
        return pd.DataFrame(), _status_payload("MISSING_INTRADAY", path=str(intraday_path))

    cand = _read_csv(candidates_path)
    rt = _read_csv(intraday_path)
    if "code" not in cand.columns or "code" not in rt.columns:
        return pd.DataFrame(), _status_payload(
            "MISSING_CODE_COLUMN",
            candidates=str(candidates_path),
            intraday=str(intraday_path),
        )

    cand = cand.copy()
    rt = rt.copy()
    cand["code"] = cand["code"].map(_norm_code)
    rt["code"] = rt["code"].map(_norm_code)
    cand = cand[cand["code"].str.len().eq(6)].copy()
    rt = rt[rt["code"].str.len().eq(6)].copy()

    for c in ("close", "value", "score", "final_score", "stretch", "atr14_pct", "high_52w_gap"):
        if c not in cand.columns:
            cand[c] = 0.0
        cand[c] = pd.to_numeric(cand[c], errors="coerce").fillna(0.0)
    for c in ("current_price", "open", "high", "low", "volume", "trading_value", "ask1", "bid1"):
        if c not in rt.columns:
            rt[c] = 0.0
        rt[c] = pd.to_numeric(rt[c], errors="coerce").fillna(0.0)

    keep_cand = [
        "code",
        "name",
        "market",
        "close",
        "value",
        "score",
        "final_score",
        "stretch",
        "atr14_pct",
        "high_52w_gap",
        "relax_level",
        "candidate_origin",
    ]
    for c in keep_cand:
        if c not in cand.columns:
            cand[c] = ""
    work = cand[keep_cand].merge(rt, on="code", how="inner", suffixes=("", "_rt"))
    if work.empty:
        return work, _status_payload(
            "NO_JOINED_ROWS",
            candidates_rows=int(len(cand)),
            intraday_rows=int(len(rt)),
        )

    open_denom = work["open"].where(work["open"] > 0)
    close_denom = work["close"].where(work["close"] > 0)
    current_denom = work["current_price"].where(work["current_price"] > 0)
    work["current_vs_open_pct"] = pd.to_numeric(work["current_price"] / open_denom - 1.0, errors="coerce")
    work["current_vs_prev_close_pct"] = pd.to_numeric(work["current_price"] / close_denom - 1.0, errors="coerce")
    work["low_from_open_pct"] = pd.to_numeric(work["low"] / open_denom - 1.0, errors="coerce")
    work["high_from_open_pct"] = pd.to_numeric(work["high"] / open_denom - 1.0, errors="coerce")
    work["day_range_pct"] = pd.to_numeric((work["high"] - work["low"]) / current_denom, errors="coerce")
    work = work.replace([float("inf"), float("-inf")], pd.NA).fillna(0.0)

    work["buy_signal_price"] = (
        (work["current_price"] > 0)
        & (work["open"] > 0)
        & (work["close"] > 0)
        & (work["current_vs_open_pct"] >= float(min_current_vs_open_pct))
        & (work["current_vs_prev_close_pct"] >= float(min_current_vs_prev_close_pct))
    )
    elapsed_frac = _elapsed_market_frac()
    work["trading_value_projected"] = work["trading_value"] / elapsed_frac
    work["buy_signal_liquidity"] = work["trading_value_projected"] >= float(min_intraday_trading_value_krw)
    work["buy_signal"] = work["buy_signal_price"] & work["buy_signal_liquidity"]
    work["min_intraday_trading_value_krw"] = float(min_intraday_trading_value_krw)
    work["elapsed_market_frac"] = elapsed_frac
    work["block_low_from_open"] = work["low_from_open_pct"] < -abs(float(max_low_from_open_pct))
    work["block_high_atr"] = work["atr14_pct"] > float(max_atr_pct)
    work["block_high_stretch"] = work["stretch"] > float(max_stretch)
    work["block_low_daily_value"] = work["value"] < float(min_value_krw)
    work["block_high_score"] = work["score"] > float(max_score)
    block_cols = [
        "block_low_from_open",
        "block_high_atr",
        "block_high_stretch",
        "block_low_daily_value",
        "block_high_score",
    ]
    work["blocked_by_risk"] = work[block_cols].any(axis=1)
    work["entry_allowed_validation"] = work["buy_signal"] & ~work["blocked_by_risk"]
    work["block_reasons"] = work[block_cols].apply(
        lambda row: ",".join(name.replace("block_", "") for name, is_blocked in row.items() if bool(is_blocked)),
        axis=1,
    )
    work["pseudo_intraday_core"] = (
        (work["current_vs_open_pct"] > 0.0)
        & (work["high_from_open_pct"] >= 0.03)
        & (work["low_from_open_pct"] >= -0.04)
    )
    work["pseudo_intraday_stretch_ok"] = work["stretch"] <= 1.15
    work["pseudo_intraday_candidate"] = work["pseudo_intraday_core"] & work["pseudo_intraday_stretch_ok"]
    work["pseudo_intraday_rule"] = PSEUDO_INTRADAY_RULE_VERSION

    diagnostic_cols = [
        "code",
        "name",
        "current_price",
        "open",
        "close",
        "current_vs_open_pct",
        "current_vs_prev_close_pct",
        "low_from_open_pct",
        "high_from_open_pct",
        "trading_value",
        "trading_value_projected",
        "elapsed_market_frac",
        "min_intraday_trading_value_krw",
        "value",
        "atr14_pct",
        "stretch",
        "score",
        "final_score",
        "buy_signal_price",
        "buy_signal_liquidity",
        "buy_signal",
        "blocked_by_risk",
        "entry_allowed_validation",
        "block_reasons",
        "pseudo_intraday_core",
        "pseudo_intraday_stretch_ok",
        "pseudo_intraday_candidate",
        "pseudo_intraday_rule",
    ]
    diagnostics_df = work[diagnostic_cols].copy()
    alpha_pool_mask = work["buy_signal"].fillna(False)
    alpha_pool = work.loc[alpha_pool_mask].copy()
    diagnostics = {
        "elapsed_market_frac": round(float(elapsed_frac), 4),
        "buy_signal_price_count": int(work["buy_signal_price"].sum()),
        "buy_signal_liquidity_count": int(work["buy_signal_liquidity"].sum()),
        "buy_signal_count": int(work["buy_signal"].sum()),
        "alpha_pool_count": int(alpha_pool_mask.sum()),
        "alpha_pool_blocked_by_risk_count": int((alpha_pool_mask & work["blocked_by_risk"]).sum()),
        "strict_entry_allowed_count": int(work["entry_allowed_validation"].sum()),
        "pseudo_intraday_candidate_count": int((alpha_pool_mask & work["pseudo_intraday_candidate"]).sum()),
        "blocked_after_buy_signal_count": int((work["buy_signal"] & work["blocked_by_risk"]).sum()),
        "entry_allowed_validation_count": int(work["entry_allowed_validation"].sum()),
        "block_counts": {c: int(work.loc[work["buy_signal"], c].sum()) for c in block_cols},
        "block_ablation": _block_ablation(work, block_cols),
        "value_replacement_compare": _value_replacement_compare(work),
        "price_risk_grade_compare": _risk_grade_compare(
            work,
            max_low_from_open_pct=max_low_from_open_pct,
            max_atr_pct=max_atr_pct,
        ),
    }
    if alpha_pool.empty:
        payload = _status_payload(
            "NO_ALERTS",
            candidates_rows=int(len(cand)),
            intraday_rows=int(len(rt)),
            joined_rows=int(len(work)),
            alerts_count=0,
            diagnostics=diagnostics,
            rule={
                "min_current_vs_open_pct": min_current_vs_open_pct,
                "min_current_vs_prev_close_pct": min_current_vs_prev_close_pct,
                "max_low_from_open_pct": max_low_from_open_pct,
                "max_atr_pct": max_atr_pct,
                "max_stretch": max_stretch,
                "min_value_krw": min_value_krw,
                "min_intraday_trading_value_krw": min_intraday_trading_value_krw,
                "max_score": max_score,
            },
        )
        payload["_diagnostics_df"] = diagnostics_df
        return pd.DataFrame(columns=ALERT_OUTPUT_COLUMNS), payload

    alpha_pool["followthrough_score"] = (
        alpha_pool["current_vs_open_pct"].clip(lower=0.0) * 35.0
        + alpha_pool["current_vs_prev_close_pct"].clip(lower=0.0) * 35.0
        + alpha_pool["high_from_open_pct"].clip(lower=0.0) * 15.0
        + (1.0 - alpha_pool["atr14_pct"].clip(lower=0.0, upper=0.15) / 0.15) * 10.0
        + (1.0 - alpha_pool["stretch"].clip(lower=0.95, upper=1.15).sub(0.95) / 0.20) * 5.0
    ).clip(lower=0.0, upper=100.0)
    alpha_pool["entry_timing"] = "intraday_realtime_validation"
    alpha_pool["is_followthrough_realtime"] = True
    selected = alpha_pool.sort_values(
        ["followthrough_score", "current_vs_prev_close_pct", "trading_value"],
        ascending=[False, False, False],
        kind="mergesort",
    ).head(max(1, int(top_n)))

    alerts = []
    for _, row in selected.iterrows():
        alerts.append(
            {
                "code": str(row["code"]),
                "name": str(row.get("name", "")),
                "followthrough_score": round(float(row["followthrough_score"]), 6),
                "current_price": float(row["current_price"]),
                "prev_close": float(row["close"]),
                "open": float(row["open"]),
                "current_vs_open_pct": round(float(row["current_vs_open_pct"]), 6),
                "current_vs_prev_close_pct": round(float(row["current_vs_prev_close_pct"]), 6),
                "low_from_open_pct": round(float(row["low_from_open_pct"]), 6),
                "high_from_open_pct": round(float(row["high_from_open_pct"]), 6),
                "atr14_pct": round(float(row["atr14_pct"]), 6),
                "stretch": round(float(row["stretch"]), 6),
                "value": float(row["value"]),
                "score": round(float(row["score"]), 6),
                "final_score": round(float(row["final_score"]), 6),
                "entry_timing": "intraday_realtime_validation",
                "blocked_by_risk": bool(row.get("blocked_by_risk", False)),
                "block_reasons": str(row.get("block_reasons", "")),
                "entry_allowed_validation": bool(row.get("entry_allowed_validation", False)),
                "pseudo_intraday_core": bool(row.get("pseudo_intraday_core", False)),
                "pseudo_intraday_stretch_ok": bool(row.get("pseudo_intraday_stretch_ok", False)),
                "pseudo_intraday_candidate": bool(row.get("pseudo_intraday_candidate", False)),
                "pseudo_intraday_rule": str(row.get("pseudo_intraday_rule", "")),
            }
        )

    payload = _status_payload(
        "OK",
        candidates=str(candidates_path),
        intraday=str(intraday_path),
        candidates_rows=int(len(cand)),
        intraday_rows=int(len(rt)),
        joined_rows=int(len(work)),
        alerts_count=int(len(alerts)),
        alerts=alerts,
        diagnostics=diagnostics,
        rule={
            "min_current_vs_open_pct": min_current_vs_open_pct,
            "min_current_vs_prev_close_pct": min_current_vs_prev_close_pct,
            "max_low_from_open_pct": max_low_from_open_pct,
            "max_atr_pct": max_atr_pct,
            "max_stretch": max_stretch,
            "min_value_krw": min_value_krw,
            "min_intraday_trading_value_krw": min_intraday_trading_value_krw,
            "max_score": max_score,
            "top_n": top_n,
        },
    )
    payload["_diagnostics_df"] = diagnostics_df
    return pd.DataFrame(alerts).reindex(columns=ALERT_OUTPUT_COLUMNS), payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build validation-only realtime follow-through alerts.")
    ap.add_argument("--candidates", default="", help="candidate CSV path")
    ap.add_argument("--intraday", default=str(DEFAULT_INTRADAY), help="intraday price CSV path")
    ap.add_argument("--out-csv", default=str(OUT_CSV), help="output CSV path")
    ap.add_argument("--out-json", default=str(OUT_JSON), help="output JSON path")
    ap.add_argument("--history-csv", default=str(OUT_HISTORY_CSV), help="append-only run summary CSV path")
    ap.add_argument("--history-jsonl", default=str(OUT_HISTORY_JSONL), help="append-only run payload JSONL path")
    ap.add_argument("--mid-track-csv", default=str(OUT_MID_TRACK_CSV), help="append-only MID risk candidate tracker CSV path")
    ap.add_argument("--mid-track-jsonl", default=str(OUT_MID_TRACK_JSONL), help="append-only MID risk candidate tracker JSONL path")
    ap.add_argument("--mid-first-seen-json", default=str(OUT_MID_FIRST_SEEN_JSON), help="MID first-seen cache JSON path")
    ap.add_argument("--mid-status-csv", default=str(OUT_MID_STATUS_CSV), help="append-only MID candidate status CSV path")
    ap.add_argument("--mid-status-jsonl", default=str(OUT_MID_STATUS_JSONL), help="append-only MID candidate status JSONL path")
    ap.add_argument("--observation-track-csv", default=str(OUT_OBS_TRACK_CSV), help="append-only STRICT/SOFT/NEAR observation tracker CSV path")
    ap.add_argument("--observation-track-jsonl", default=str(OUT_OBS_TRACK_JSONL), help="append-only STRICT/SOFT/NEAR observation tracker JSONL path")
    ap.add_argument("--observation-first-seen-json", default=str(OUT_OBS_FIRST_SEEN_JSON), help="observation first-seen cache JSON path")
    ap.add_argument("--min-current-vs-open-pct", type=float, default=0.003)
    ap.add_argument("--min-current-vs-prev-close-pct", type=float, default=0.003)
    ap.add_argument("--max-low-from-open-pct", type=float, default=0.04)
    ap.add_argument("--max-atr-pct", type=float, default=0.05192369520951628)
    ap.add_argument("--max-stretch", type=float, default=1.0719612229679145)
    ap.add_argument("--min-value-krw", type=float, default=15_000_000_000.0)
    ap.add_argument("--min-intraday-trading-value-krw", type=float, default=5_000_000_000.0)
    ap.add_argument("--max-score", type=float, default=0.8862005532171512)
    ap.add_argument("--top-n", type=int, default=5)
    args = ap.parse_args()

    selected, payload = build_signals(
        _pick_candidates_path(args.candidates),
        Path(args.intraday),
        min_current_vs_open_pct=args.min_current_vs_open_pct,
        min_current_vs_prev_close_pct=args.min_current_vs_prev_close_pct,
        max_low_from_open_pct=args.max_low_from_open_pct,
        max_atr_pct=args.max_atr_pct,
        max_stretch=args.max_stretch,
        min_value_krw=args.min_value_krw,
        min_intraday_trading_value_krw=args.min_intraday_trading_value_krw,
        max_score=args.max_score,
        top_n=args.top_n,
    )
    out_csv = Path(args.out_csv)
    out_json = Path(args.out_json)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    selected.to_csv(out_csv, index=False, encoding="utf-8-sig")
    payload["out_csv"] = str(out_csv)
    payload["out_json"] = str(out_json)
    payload["history_csv"] = str(Path(args.history_csv))
    payload["history_jsonl"] = str(Path(args.history_jsonl))
    mid_track = _append_mid_tracker(
        payload,
        Path(args.mid_track_csv),
        Path(args.mid_track_jsonl),
        Path(args.mid_first_seen_json),
    )
    payload.update(mid_track)
    diagnostics_df = payload.pop("_diagnostics_df", selected)
    mid_status = _append_mid_status(
        payload,
        diagnostics_df,
        Path(args.mid_first_seen_json),
        Path(args.mid_status_csv),
        Path(args.mid_status_jsonl),
    )
    payload.update(mid_status)
    observation_track = _append_observation_tracker(
        diagnostics_df,
        Path(args.observation_track_csv),
        Path(args.observation_track_jsonl),
        Path(args.observation_first_seen_json),
    )
    payload.update(observation_track)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _append_history(payload, Path(args.history_csv), Path(args.history_jsonl))
    print(f"[FOLLOWTHROUGH_RT] status={payload.get('status')} alerts={payload.get('alerts_count', 0)}")
    print(f"[FOLLOWTHROUGH_RT] csv={out_csv}")
    print(f"[FOLLOWTHROUGH_RT] json={out_json}")
    print(f"[FOLLOWTHROUGH_RT] history_csv={Path(args.history_csv)}")
    print(f"[FOLLOWTHROUGH_RT] history_jsonl={Path(args.history_jsonl)}")
    print(f"[FOLLOWTHROUGH_RT] mid_track_count={mid_track.get('mid_track_count', 0)}")
    print(f"[FOLLOWTHROUGH_RT] mid_track_csv={Path(args.mid_track_csv)}")
    print(f"[FOLLOWTHROUGH_RT] mid_track_jsonl={Path(args.mid_track_jsonl)}")
    print(f"[FOLLOWTHROUGH_RT] mid_status_count={mid_status.get('mid_status_count', 0)}")
    print(f"[FOLLOWTHROUGH_RT] mid_status_csv={Path(args.mid_status_csv)}")
    print(f"[FOLLOWTHROUGH_RT] mid_status_jsonl={Path(args.mid_status_jsonl)}")
    print(f"[FOLLOWTHROUGH_RT] observation_track_count={observation_track.get('observation_track_count', 0)}")
    print(f"[FOLLOWTHROUGH_RT] observation_track_csv={Path(args.observation_track_csv)}")
    print(f"[FOLLOWTHROUGH_RT] observation_track_jsonl={Path(args.observation_track_jsonl)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
