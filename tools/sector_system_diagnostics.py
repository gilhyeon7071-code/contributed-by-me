import json
import math
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "2_Logs"
PAPER = ROOT / "paper"
CACHE = ROOT / "_cache"

SECTOR_HISTORY = LOGS / "sector_score_history.csv"
SECTOR_STATUS_GLOB = "sector_score_status_*.json"
CANDIDATES = LOGS / "candidates_latest_data.with_sector_score.csv"
CORRELATION = LOGS / "sector_correlation_latest.json"
CONFIG = PAPER / "paper_engine_config.json"
STATE = PAPER / "paper_state.json"
PAPER_ENGINE = ROOT / "paper_engine.py"
RUN_PAPER_DAILY = ROOT / "run_paper_daily.bat"
OUT_JSON = LOGS / "sector_system_diagnostics_latest.json"
OUT_CSV = LOGS / "sector_system_diagnostics_latest.csv"
ALLOWED_SECTOR_CODES = {"005", "008", "009", "011", "012", "013", "015", "016", "017", "018", "019", "020", "022", "024", "025", "026"}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _date8() -> str:
    return datetime.now().strftime("%Y%m%d")


def _read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig")), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8-sig")
    except Exception:
        return ""


def _extract_number(text: str, key: str, default: float | None = None) -> float | None:
    m = re.search(rf'"{re.escape(key)}"\s*:\s*(-?\d+(?:\.\d+)?)', text)
    if not m:
        return default
    try:
        return float(m.group(1))
    except Exception:
        return default


def _extract_bool(text: str, key: str, default: bool | None = None) -> bool | None:
    m = re.search(rf'"{re.escape(key)}"\s*:\s*(true|false)', text, flags=re.I)
    if not m:
        return default
    return m.group(1).lower() == "true"


def _clean(v: object) -> str:
    s = str(v if v is not None else "").strip()
    if s.lower() in {"", "nan", "none", "<na>"}:
        return ""
    return s


def _norm_code(v: object) -> str:
    s = _clean(v)
    if not s:
        return ""
    if s.endswith(".0"):
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits.zfill(3) if digits else ""


def _status(ok: bool, warn: bool = False) -> str:
    if ok:
        return "PASS"
    return "WARN" if warn else "FAIL"


def _business_lag(a: str, b: str) -> int | None:
    try:
        adt = pd.to_datetime(str(a), format="%Y%m%d", errors="raise")
        bdt = pd.to_datetime(str(b), format="%Y%m%d", errors="raise")
    except Exception:
        return None
    if bdt > adt:
        return 0
    return max(0, len(pd.bdate_range(bdt, adt)) - 1)


def _load_sector_targets() -> pd.DataFrame:
    frames = []
    if SECTOR_HISTORY.exists():
        frames.append(pd.read_csv(SECTOR_HISTORY, dtype={"code": str, "sector_code": str, "date8": str}))
    if CANDIDATES.exists():
        frames.append(pd.read_csv(CANDIDATES, dtype={"code": str, "sector_code": str}))
    if not frames:
        return pd.DataFrame(columns=["sector_code", "krx_sector"])
    df = pd.concat(frames, ignore_index=True, sort=False)
    for col in ["sector_code", "krx_sector"]:
        if col not in df.columns:
            df[col] = ""
    df["sector_code"] = df["sector_code"].map(_norm_code)
    df["krx_sector"] = df["krx_sector"].map(_clean)
    df = df[(df["sector_code"] != "") & (df["krx_sector"] != "")]
    df = df[df["sector_code"].isin(ALLOWED_SECTOR_CODES)]
    return df[["sector_code", "krx_sector"]].drop_duplicates(["sector_code"], keep="last")


def _load_krx_client(force_mock: bool):
    sys.path.insert(0, str(ROOT / "_dev" / "kospi_sector"))
    from data.krx_api import KRXClient

    key_path = CACHE / "krx_api_key.txt"
    key = os.getenv("KRX_API_KEY", "").strip()
    if not key and key_path.exists():
        key = key_path.read_text(encoding="utf-8").strip()
    use_mock = force_mock or not key
    return KRXClient(auth_key=(None if use_mock else key), mock=use_mock), use_mock


def check_return_history() -> dict[str, Any]:
    targets = _load_sector_targets()
    rows = []
    if targets.empty:
        return {"status": "FAIL", "reason": "no_sector_targets", "rows": rows}

    end = datetime.now()
    start = end - timedelta(days=550)
    client, use_mock = _load_krx_client(force_mock=False)
    min_return_obs = 120
    max_lag_bdays = 3

    for _, row in targets.sort_values("sector_code").iterrows():
        code = str(row["sector_code"])
        name = str(row["krx_sector"])
        item = {
            "sector_code": code,
            "krx_sector": name,
            "source": "KRX_MOCK" if use_mock else "KRX",
            "price_rows": 0,
            "return_obs": 0,
            "missing_close": None,
            "latest_date": "",
            "lag_business_days": None,
            "status": "FAIL",
            "reason": "",
        }
        try:
            px = client.get_sector_index(code, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
            if px is None or len(px) == 0 or "close" not in px.columns:
                item["reason"] = "no_price_rows"
            else:
                close = pd.to_numeric(px["close"], errors="coerce")
                item["price_rows"] = int(len(px))
                item["missing_close"] = int(close.isna().sum())
                returns = close.pct_change().dropna()
                item["return_obs"] = int(len(returns))
                latest = pd.Timestamp(px.index[-1]).strftime("%Y%m%d")
                item["latest_date"] = latest
                item["lag_business_days"] = _business_lag(_date8(), latest)
                if item["return_obs"] >= min_return_obs and (item["lag_business_days"] is not None and item["lag_business_days"] <= max_lag_bdays):
                    item["status"] = "PASS"
                    item["reason"] = "enough_return_history"
                else:
                    item["status"] = "WARN"
                    item["reason"] = "short_or_stale_history"
        except Exception as exc:
            item["reason"] = f"{type(exc).__name__}: {exc}"
        rows.append(item)

    status = "PASS" if all(r["status"] == "PASS" for r in rows) else "WARN"
    return {
        "status": status,
        "sector_count": int(len(rows)),
        "pass_count": int(sum(1 for r in rows if r["status"] == "PASS")),
        "warn_count": int(sum(1 for r in rows if r["status"] == "WARN")),
        "fail_count": int(sum(1 for r in rows if r["status"] == "FAIL")),
        "min_return_obs_required": min_return_obs,
        "max_lag_business_days": max_lag_bdays,
        "rows": rows,
    }


def check_score_stability() -> dict[str, Any]:
    records = []
    for path in sorted(LOGS.glob(SECTOR_STATUS_GLOB)):
        if path.name.endswith("_latest.json"):
            continue
        obj, err = _read_json(path)
        date8 = path.stem.replace("sector_score_status_", "")
        if obj is None:
            records.append({"date8": date8, "status": "FAIL", "freshness": "JSON_ERROR", "reason": err or "json_error"})
            continue
        records.append({
            "date8": str(obj.get("candidate_date8") or date8),
            "status": str(obj.get("status") or ""),
            "freshness": str(obj.get("freshness") or ""),
            "lag_business_days": obj.get("lag_business_days"),
            "rows": obj.get("rows"),
            "sector_latest_date8": obj.get("sector_latest_date8"),
        })
    if not records:
        return {"status": "FAIL", "reason": "no_status_files", "rows": records}

    bad = [r for r in records if r.get("status") not in {"PASS"}]
    stale = [r for r in records if r.get("freshness") in {"STALE", "NO_DATA", "EMPTY", "NO_SECTOR_CODES", "JSON_ERROR"}]
    latest = records[-1]
    status = "PASS" if not bad and not stale else "WARN"
    return {
        "status": status,
        "status_files": int(len(records)),
        "pass_count": int(sum(1 for r in records if r.get("status") == "PASS")),
        "warn_or_fail_count": int(len(bad)),
        "stale_or_empty_count": int(len(stale)),
        "latest": latest,
        "rows": records,
    }


def check_candidate_concentration() -> dict[str, Any]:
    if not CANDIDATES.exists():
        return {"status": "FAIL", "reason": "missing_candidates", "rows": []}
    df = pd.read_csv(CANDIDATES, dtype={"code": str, "sector_code": str})
    if df.empty or "sector_code" not in df.columns:
        return {"status": "FAIL", "reason": "empty_or_missing_sector_code", "rows": []}
    df["sector_code"] = df["sector_code"].map(_norm_code)
    df["krx_sector"] = df.get("krx_sector", pd.Series([""] * len(df))).map(_clean)
    total = int(len(df))
    grp = (
        df.groupby(["sector_code", "krx_sector"], dropna=False)
        .size()
        .reset_index(name="candidate_count")
        .sort_values("candidate_count", ascending=False)
    )
    rows = []
    for _, r in grp.iterrows():
        share = float(r["candidate_count"]) / max(total, 1)
        rows.append({
            "sector_code": str(r["sector_code"]),
            "krx_sector": str(r["krx_sector"]),
            "candidate_count": int(r["candidate_count"]),
            "candidate_share": round(share, 6),
            "status": "WARN" if share > 0.40 else "PASS",
        })
    hhi = sum(float(r["candidate_share"]) ** 2 for r in rows)
    top_share = max([float(r["candidate_share"]) for r in rows], default=0.0)
    status = "PASS" if top_share <= 0.40 and hhi <= 0.25 else "WARN"
    return {
        "status": status,
        "candidate_rows": total,
        "sector_count": int(len(rows)),
        "top_sector_share": round(top_share, 6),
        "hhi": round(hhi, 6),
        "top_share_warn_threshold": 0.40,
        "hhi_warn_threshold": 0.25,
        "rows": rows,
    }


def _open_notional_from_state() -> float:
    obj, err = _read_json(STATE)
    if obj is None:
        return 0.0
    positions = obj.get("open_positions")
    if positions is None:
        positions = obj.get("positions")
    if isinstance(positions, dict):
        iterable = positions.values()
    elif isinstance(positions, list):
        iterable = positions
    else:
        iterable = []
    total = 0.0
    for pos in iterable:
        if not isinstance(pos, dict):
            continue
        qty = float(pos.get("qty") or pos.get("quantity") or 0.0)
        px = float(pos.get("last_price") or pos.get("price") or pos.get("avg_price") or pos.get("entry_price") or 0.0)
        if qty > 0 and px > 0:
            total += qty * px
    return total


def _load_state_positions() -> list[dict[str, Any]]:
    obj, err = _read_json(STATE)
    if obj is None:
        return []
    positions = obj.get("open_positions")
    if positions is None:
        positions = obj.get("positions")
    if isinstance(positions, dict):
        return [p for p in positions.values() if isinstance(p, dict)]
    if isinstance(positions, list):
        return [p for p in positions if isinstance(p, dict)]
    return []


def check_risk_budget_policy() -> dict[str, Any]:
    text = _read_text(CONFIG)
    cfg_obj, cfg_err = _read_json(CONFIG)
    capital_total = _extract_number(text, "capital_total", 0.0) or 0.0
    max_positions = int(_extract_number(text, "max_positions", 0.0) or 0)
    gross_pct = _extract_number(text, "gross_exposure_pct", 0.55)
    basic_pct = _extract_number(text, "basic_alloc_pct", 0.40)
    surge_pct = _extract_number(text, "surge_alloc_pct", 0.12)
    split_pct = _extract_number(text, "split_alloc_pct", 0.18)
    recovery_pct = _extract_number(text, "recovery_alloc_pct", 0.10)
    reserve_pct = _extract_number(text, "reserve_alloc_pct", 0.20)
    defensive_floor = _extract_number(text, "defensive_floor_exposure_pct", 0.45)
    budget_enabled = _extract_bool(text, "enabled", None)
    corr_obj, corr_err = _read_json(CORRELATION)
    open_notional = _open_notional_from_state()

    budget_sum = sum(x or 0.0 for x in [basic_pct, surge_pct, split_pct, recovery_pct, reserve_pct])
    active_sum = sum(x or 0.0 for x in [basic_pct, surge_pct])
    planned_total = sum(x or 0.0 for x in [basic_pct, surge_pct, split_pct, recovery_pct, reserve_pct])
    per_symbol_basic = ((capital_total * (basic_pct or 0.0)) / max(max_positions, 1)) if capital_total and max_positions else 0.0
    gross_cap_krw = capital_total * (gross_pct or 0.0)
    open_exposure_pct = (open_notional / capital_total) if capital_total else 0.0

    issues = []
    if cfg_obj is None:
        issues.append("paper_engine_config_json_invalid")
    if abs(budget_sum - 1.0) > 0.001:
        issues.append("capital_budget_pct_sum_not_100pct")
    if active_sum > (gross_pct or 0.0) + 0.001:
        issues.append("basic_plus_surge_exceeds_gross_cap")
    if defensive_floor and gross_pct and defensive_floor > gross_pct:
        issues.append("defensive_floor_above_gross_cap")
    if corr_obj is None:
        issues.append("sector_correlation_json_missing_or_invalid")
    else:
        exposure_keys = list((corr_obj.get("exposure_by_sector") or {}).keys())
        if any(_clean(k) == "" for k in exposure_keys):
            issues.append("sector_correlation_has_empty_sector_key")
        if any("nan" == str(k).strip().lower() for k in exposure_keys):
            issues.append("sector_correlation_has_nan_sector_key")
        if any("?" in str(k) or "\ufffd" in str(k) for k in exposure_keys):
            issues.append("sector_correlation_has_mojibake_sector_key")

    status = "PASS" if not issues else "WARN"
    return {
        "status": status,
        "config_json_valid": bool(cfg_obj is not None),
        "config_json_error": cfg_err,
        "capital_total": capital_total,
        "max_positions": max_positions,
        "gross_exposure_pct": gross_pct,
        "gross_cap_krw": gross_cap_krw,
        "open_notional_krw": open_notional,
        "open_exposure_pct": round(open_exposure_pct, 6),
        "capital_budget_policy": {
            "enabled_first_key_seen": budget_enabled,
            "basic_alloc_pct": basic_pct,
            "surge_alloc_pct": surge_pct,
            "split_alloc_pct": split_pct,
            "recovery_alloc_pct": recovery_pct,
            "reserve_alloc_pct": reserve_pct,
            "budget_sum": round(budget_sum, 6),
            "active_basic_plus_surge": round(active_sum, 6),
            "defensive_floor_exposure_pct": defensive_floor,
            "basic_per_symbol_krw": round(per_symbol_basic, 2),
        },
        "sector_correlation_artifact": {
            "path": str(CORRELATION),
            "json_valid": bool(corr_obj is not None),
            "json_error": corr_err,
            "generated_at": (corr_obj or {}).get("generated_at") if corr_obj else None,
            "sector_count": (corr_obj or {}).get("sector_count") if corr_obj else None,
        },
        "issues": issues,
        "rows": [
            {"metric": "gross_cap_krw", "value": gross_cap_krw, "status": "PASS"},
            {"metric": "basic_per_symbol_krw", "value": round(per_symbol_basic, 2), "status": "PASS" if per_symbol_basic > 0 else "WARN"},
            {"metric": "budget_sum", "value": round(budget_sum, 6), "status": "PASS" if abs(budget_sum - 1.0) <= 0.001 else "WARN"},
            {"metric": "active_basic_plus_surge", "value": round(active_sum, 6), "status": "PASS" if active_sum <= (gross_pct or 0.0) + 0.001 else "WARN"},
        ],
    }


def check_auto_regeneration_path() -> dict[str, Any]:
    batch_text = _read_text(RUN_PAPER_DAILY)
    engine_text = _read_text(PAPER_ENGINE)
    sector_idx = batch_text.find("tools\\sector_score_daily.py")
    engine_idx = batch_text.find("paper_engine.py")
    has_order = sector_idx >= 0 and engine_idx >= 0 and sector_idx < engine_idx
    engine_writes = "SECTOR_CORRELATION_STATUS_PATH.write_text" in engine_text
    engine_builds = "_build_sector_correlation_guard(" in engine_text
    status = "PASS" if RUN_PAPER_DAILY.exists() and PAPER_ENGINE.exists() and has_order and engine_writes and engine_builds else "WARN"
    issues = []
    if not RUN_PAPER_DAILY.exists():
        issues.append("run_paper_daily_missing")
    if not PAPER_ENGINE.exists():
        issues.append("paper_engine_missing")
    if not has_order:
        issues.append("sector_score_not_before_paper_engine")
    if not engine_builds:
        issues.append("paper_engine_missing_sector_corr_builder")
    if not engine_writes:
        issues.append("paper_engine_missing_sector_corr_write")
    return {
        "status": status,
        "run_paper_daily": str(RUN_PAPER_DAILY),
        "paper_engine": str(PAPER_ENGINE),
        "sector_score_before_paper_engine": bool(has_order),
        "paper_engine_builds_sector_correlation": bool(engine_builds),
        "paper_engine_writes_sector_correlation": bool(engine_writes),
        "issues": issues,
        "rows": [
            {"metric": "sector_score_before_paper_engine", "value": bool(has_order), "status": "PASS" if has_order else "WARN"},
            {"metric": "paper_engine_builds_sector_correlation", "value": bool(engine_builds), "status": "PASS" if engine_builds else "WARN"},
            {"metric": "paper_engine_writes_sector_correlation", "value": bool(engine_writes), "status": "PASS" if engine_writes else "WARN"},
        ],
    }


def check_order_reflection() -> dict[str, Any]:
    corr_obj, corr_err = _read_json(CORRELATION)
    engine_text = _read_text(PAPER_ENGINE)
    if corr_obj is None:
        return {"status": "WARN", "reason": corr_err or "missing_correlation", "rows": []}

    multipliers = corr_obj.get("sector_hrp_qty_multiplier") if isinstance(corr_obj.get("sector_hrp_qty_multiplier"), dict) else {}
    corr_scores = corr_obj.get("sector_corr_score") if isinstance(corr_obj.get("sector_corr_score"), dict) else {}
    thresholds = corr_obj.get("thresholds") if isinstance(corr_obj.get("thresholds"), dict) else {}
    reduce_thr = float(thresholds.get("reduce_threshold_abs_corr") or 0.85)
    allow_block = bool(thresholds.get("allow_block", False))
    reduce_candidates = [k for k, v in corr_scores.items() if float(v or 0.0) >= reduce_thr]
    hrp_reduce_candidates = [k for k, v in multipliers.items() if 0.0 < float(v or 0.0) < 1.0]
    code_checks = {
        "sector_corr_score_lookup": "sector_corr_score" in engine_text and "row_sector_for_corr" in engine_text,
        "corr_reduce_counter": "sector_corr_reduce_count += 1" in engine_text,
        "hrp_multiplier_lookup": "sector_hrp_qty_multiplier" in engine_text,
        "hrp_reduce_log": "SECTOR_HRP_REDUCE" in engine_text,
    }
    status = "PASS" if all(code_checks.values()) else "WARN"
    rows = [
        {"metric": k, "value": bool(v), "status": "PASS" if v else "WARN"}
        for k, v in code_checks.items()
    ]
    rows.extend([
        {"metric": "corr_reduce_candidate_sectors", "value": len(reduce_candidates), "status": "PASS"},
        {"metric": "hrp_reduce_candidate_sectors", "value": len(hrp_reduce_candidates), "status": "PASS"},
    ])
    return {
        "status": status,
        "code_checks": code_checks,
        "reduce_threshold_abs_corr": reduce_thr,
        "allow_block": allow_block,
        "corr_reduce_candidate_sectors": reduce_candidates,
        "hrp_reduce_candidate_sectors": hrp_reduce_candidates,
        "rows": rows,
    }


def check_position_sector_classification() -> dict[str, Any]:
    positions = _load_state_positions()
    targets = _load_sector_targets()
    sector_map = {str(r["sector_code"]): str(r["krx_sector"]) for _, r in targets.iterrows()}
    candidate_map = {}
    if CANDIDATES.exists():
        try:
            cand = pd.read_csv(CANDIDATES, dtype={"code": str, "sector_code": str})
            for _, row in cand.iterrows():
                code = str(row.get("code") or "").zfill(6)
                sector = _clean(row.get("krx_sector", ""))
                scode = _norm_code(row.get("sector_code", ""))
                if code and sector:
                    candidate_map[code] = {"krx_sector": sector, "sector_code": scode}
        except Exception:
            pass

    rows = []
    for pos in positions:
        code = str(pos.get("code") or "").zfill(6)
        raw_sector = _clean(pos.get("sector", ""))
        cand_info = candidate_map.get(code, {})
        expected_sector = _clean(cand_info.get("krx_sector", ""))
        expected_code = _norm_code(cand_info.get("sector_code", ""))
        status = "PASS"
        reason = "state_sector_present"
        if not raw_sector:
            if expected_sector:
                status = "WARN"
                reason = "state_sector_empty_candidate_available"
            else:
                status = "WARN"
                reason = "state_sector_empty_no_candidate_mapping"
        elif "?" in raw_sector or "\ufffd" in raw_sector or raw_sector.lower() == "nan":
            status = "WARN"
            reason = "state_sector_invalid_text"
        rows.append({
            "code": code,
            "state_sector": raw_sector,
            "candidate_sector": expected_sector,
            "sector_code": expected_code,
            "status": status,
            "reason": reason,
        })

    warn_count = sum(1 for r in rows if r["status"] != "PASS")
    return {
        "status": "PASS" if warn_count == 0 else "WARN",
        "open_positions": int(len(rows)),
        "warn_count": int(warn_count),
        "state_path": str(STATE),
        "state_modified": False,
        "rows": rows,
    }


def flatten_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for section in [
        "return_history",
        "score_stability",
        "candidate_concentration",
        "risk_budget_policy",
        "auto_regeneration_path",
        "order_reflection",
        "position_sector_classification",
    ]:
        data = result.get(section) or {}
        for item in data.get("rows", []) or []:
            row = {"section": section}
            row.update(item)
            rows.append(row)
    return rows


def main() -> int:
    result = {
        "generated_at": _now(),
        "date8": _date8(),
        "return_history": check_return_history(),
        "score_stability": check_score_stability(),
        "candidate_concentration": check_candidate_concentration(),
        "risk_budget_policy": check_risk_budget_policy(),
        "auto_regeneration_path": check_auto_regeneration_path(),
        "order_reflection": check_order_reflection(),
        "position_sector_classification": check_position_sector_classification(),
    }
    section_statuses = [
        result["return_history"]["status"],
        result["score_stability"]["status"],
        result["candidate_concentration"]["status"],
        result["risk_budget_policy"]["status"],
        result["auto_regeneration_path"]["status"],
        result["order_reflection"]["status"],
        result["position_sector_classification"]["status"],
    ]
    result["status"] = "PASS" if all(s == "PASS" for s in section_statuses) else "WARN"

    dated_json = LOGS / f"sector_system_diagnostics_{result['date8']}.json"
    dated_csv = LOGS / f"sector_system_diagnostics_{result['date8']}.csv"
    text = json.dumps(result, ensure_ascii=False, indent=2)
    OUT_JSON.write_text(text + "\n", encoding="utf-8")
    dated_json.write_text(text + "\n", encoding="utf-8")

    rows = flatten_rows(result)
    pd.DataFrame(rows).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    pd.DataFrame(rows).to_csv(dated_csv, index=False, encoding="utf-8-sig")
    print(f"WROTE {OUT_JSON}")
    print(f"WROTE {dated_json}")
    print(f"WROTE {OUT_CSV} rows={len(rows)}")
    print(f"status={result['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
