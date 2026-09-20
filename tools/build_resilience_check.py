from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from holiday_manager import HolidayManager

LOG_DIR = ROOT / "2_Logs"
CFG_PATH = ROOT / "config" / "resilience_check.json"
KST = dt.timezone(dt.timedelta(hours=9))

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"


def _now_kst() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).astimezone(KST)


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _read_cfg(path: Path) -> Dict[str, Any]:
    cfg = _read_json(path)
    return cfg if isinstance(cfg, dict) else {}


def _cfg_get(cfg: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = cfg
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return float("nan")


def _is_finite(v: float) -> bool:
    return v == v and v not in (float("inf"), float("-inf"))


def _fmt(v: Any, digits: int = 4) -> str:
    f = _safe_float(v)
    if not _is_finite(f):
        return "-"
    return f"{f:.{digits}g}"


def _norm_ymd(v: Any) -> str:
    t = "".join(ch for ch in str(v or "") if ch.isdigit())
    if len(t) < 8:
        return ""
    y = t[:8]
    try:
        dt.datetime.strptime(y, "%Y%m%d")
        return y
    except Exception:
        return ""


def _calendar() -> HolidayManager:
    return HolidayManager()


def _prev_trading_ymd(ymd: str) -> str:
    return _calendar().previous_trading_day(ymd)


def _calendar_prev_ymd(ymd: str) -> str:
    y = _norm_ymd(ymd)
    if not y:
        return ""
    return (dt.datetime.strptime(y, "%Y%m%d").date() - dt.timedelta(days=1)).strftime("%Y%m%d")


def _latest_by_glob(pattern: str) -> Optional[Path]:
    xs = list(LOG_DIR.glob(pattern))
    if not xs:
        return None
    xs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return xs[0]


def _latest_p0_result() -> Optional[Path]:
    xs = []
    pat = re.compile(r"^p0_daily_check_\d{8}_\d{6}\.json$")
    for p in LOG_DIR.glob("p0_daily_check_*.json"):
        if pat.match(p.name):
            xs.append(p)
    if not xs:
        return None
    xs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return xs[0]


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _set_readonly(path: Path) -> None:
    mode = path.stat().st_mode
    path.chmod(mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH)


def _clear_readonly(path: Path) -> None:
    if not path.exists():
        return
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IWUSR)


def _check_gateway(cfg: Dict[str, Any]) -> Dict[str, Any]:
    section = _cfg_get(cfg, "gateway_health", {})
    paths = section.get("paths", {}) if isinstance(section, dict) else {}
    max_age_min = _safe_float(section.get("max_age_minutes", 2.0))
    lat_thr = _safe_float(section.get("latency_threshold_ms", 80.0))

    out_rows: List[Dict[str, Any]] = []
    status = PASS
    failover_target = None

    for key in ("primary", "secondary", "tertiary"):
        p_raw = paths.get(key)
        if not p_raw:
            continue
        p = Path(str(p_raw))
        row = {"name": key, "path": str(p), "exists": p.exists(), "latency_ms": None, "age_minutes": None, "ok": False, "ok_basis": None}
        if not p.exists():
            status = FAIL
            out_rows.append(row)
            continue
        obj = _read_json(p)
        lat = _safe_float(
            obj.get("latency_ms")
            or _cfg_get(obj, "metric.latency_ms")
            or _cfg_get(obj, "metrics.latency_ms")
            or _cfg_get(obj, "network.latency_ms")
        )
        age_min = (_now_kst() - dt.datetime.fromtimestamp(p.stat().st_mtime, tz=dt.timezone.utc).astimezone(KST)).total_seconds() / 60.0
        sig = obj.get("signals") if isinstance(obj.get("signals"), dict) else {}
        sig_any = bool(sig.get("e2e_ok")) or bool(sig.get("rt_ok")) or bool(sig.get("hc_ok"))
        row["latency_ms"] = lat if _is_finite(lat) else None
        row["age_minutes"] = age_min
        row["ok"] = bool((_is_finite(lat) and age_min <= max_age_min) or (sig_any and age_min <= max_age_min))
        row["ok_basis"] = "latency" if bool(_is_finite(lat) and age_min <= max_age_min) else ("signals_fallback" if row["ok"] else "none")
        if not row["ok"]:
            status = FAIL
        out_rows.append(row)

    primary = next((x for x in out_rows if x["name"] == "primary"), None)
    secondary = next((x for x in out_rows if x["name"] == "secondary"), None)
    if primary and primary.get("ok") and _is_finite(_safe_float(primary.get("latency_ms"))):
        if _safe_float(primary.get("latency_ms")) > lat_thr:
            if secondary and secondary.get("ok"):
                failover_target = "secondary"
            else:
                status = FAIL

    if not out_rows:
        status = FAIL

    return {
        "status": status,
        "threshold": {"latency_threshold_ms": lat_thr, "max_age_minutes": max_age_min},
        "rows": out_rows,
        "failover_target": failover_target,
    }


def _check_routes(cfg: Dict[str, Any]) -> Dict[str, Any]:
    section = _cfg_get(cfg, "route_table", {})
    routes = section.get("paths", {}) if isinstance(section, dict) else {}
    expected = ("order", "quote", "auth")
    out_rows: List[Dict[str, Any]] = []
    status = PASS
    for k in expected:
        p_raw = routes.get(k)
        p = Path(str(p_raw)) if p_raw else None
        exists = bool(p and p.exists())
        if not exists:
            status = FAIL
            out_rows.append({"name": k, "path": str(p) if p else "", "exists": False, "sha256": None})
            continue
        out_rows.append(
            {
                "name": k,
                "path": str(p),
                "exists": True,
                "mtime": dt.datetime.fromtimestamp(p.stat().st_mtime, tz=dt.timezone.utc).astimezone(KST).isoformat(),
                "sha256": _sha256_file(p),
            }
        )
    return {"status": status, "rows": out_rows}


def _csv_date_max(path: Path, cols: List[str]) -> str:
    if not path.exists():
        return ""
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rd = csv.DictReader(f)
        mx = ""
        for row in rd:
            for c in cols:
                y = _norm_ymd(row.get(c))
                if y and y > mx:
                    mx = y
        return mx


def _csv_dup_count(path: Path, keys: List[str]) -> int:
    if not path.exists() or not keys:
        return 0
    seen = set()
    dup = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rd = csv.DictReader(f)
        for row in rd:
            key = tuple((row.get(k) or "").strip() for k in keys)
            if any(not x for x in key):
                continue
            if key in seen:
                dup += 1
            else:
                seen.add(key)
    return dup


def _check_data(cfg: Dict[str, Any], as_of: str) -> Dict[str, Any]:
    section = _cfg_get(cfg, "data_checks", {})
    candidates_path = Path(str(section.get("candidates_csv", LOG_DIR / "candidates_latest_data.with_final_score.csv")))
    prices_path = Path(str(section.get("prices_csv", LOG_DIR / "backtest_market_ohlc_latest.csv")))
    candidates_meta_path = LOG_DIR / "candidates_latest_meta.json"
    p0_path = _latest_p0_result()
    p0 = _read_json(p0_path) if p0_path else {}
    candidates_meta = _read_json(candidates_meta_path)
    krx_max = _norm_ymd(
        _cfg_get(p0, "krx_clean.effective_date_max")
        or _cfg_get(p0, "krx_clean.date_max")
        or _cfg_get(p0, "prices.date_max")
        or _cfg_get(p0, "prices.max_date")
    )
    p0_prev_bday = _norm_ymd(_cfg_get(p0, "prices.prev_weekday"))
    krx_effective_ymd = _norm_ymd(_cfg_get(p0, "krx_clean.effective_date_max"))

    cand_csv_max = _csv_date_max(candidates_path, ["as_of_ymd", "signal_date", "date"])
    cand_meta_max = _norm_ymd(_cfg_get(candidates_meta, "latest_date") or _cfg_get(candidates_meta, "as_of_ymd"))
    cand_max = max(cand_csv_max, cand_meta_max)
    prices_max = _csv_date_max(prices_path, ["date", "as_of_ymd", "ymd"])
    cand_dup = _csv_dup_count(candidates_path, ["signal_date", "code"])
    price_dup = _csv_dup_count(prices_path, ["date", "code"])

    ref_ymd = as_of or krx_max or p0_prev_bday
    if not prices_max and krx_max:
        prices_max = krx_max
    elif prices_max and krx_max and prices_max < krx_max:
        # 운영 가격 기준은 p0(krx_clean) 최신일을 우선한다.
        prices_max = krx_max

    calendar_prev_ymd = _calendar_prev_ymd(ref_ymd)
    expected_prev_trading_ymd = p0_prev_bday or _prev_trading_ymd(ref_ymd)
    cand_ref_ymd = expected_prev_trading_ymd
    stale_flags = []
    for name, y in (("candidates", cand_max), ("prices", prices_max), ("krx_clean", krx_max)):
        threshold = cand_ref_ymd if name == "candidates" else (krx_effective_ymd or ref_ymd)
        stale = bool(threshold and y and y < threshold)
        stale_flags.append({"name": name, "max_ymd": y or None, "threshold_ymd": threshold or None, "stale": stale})

    any_stale = any(x["stale"] for x in stale_flags)
    any_dup = (cand_dup > 0) or (price_dup > 0)
    status = FAIL if (any_stale or any_dup) else PASS
    return {
        "status": status,
        "as_of_ymd": ref_ymd,
        "new_orders": "NO" if status == FAIL else "YES",
        "stale": stale_flags,
        "duplicates": {"candidates_dup": cand_dup, "prices_dup": price_dup},
        "sources": {
            "candidates_csv": str(candidates_path),
            "candidates_meta_json": str(candidates_meta_path) if candidates_meta else None,
            "candidates_csv_max_ymd": cand_csv_max or None,
            "candidates_meta_max_ymd": cand_meta_max or None,
            "prices_csv": str(prices_path),
            "p0_daily_check": str(p0_path) if p0_path else None,
            "p0_prev_weekday": p0_prev_bday or None,
            "calendar_prev_day": calendar_prev_ymd or None,
            "expected_prev_trading_day": expected_prev_trading_ymd or None,
            "source_calendar": "holiday_manager",
            "krx_effective_ymd": krx_effective_ymd or None,
        },
    }


def _check_risk() -> Dict[str, Any]:
    kill = _read_json(LOG_DIR / "kill_switch_validation_report_latest.json")
    gate = _read_json(_latest_by_glob("gate_daily_*.json") or Path(""))
    p0_path = _latest_p0_result()
    p0 = _read_json(p0_path) if p0_path else {}
    daily = kill.get("daily", []) if isinstance(kill.get("daily"), list) else []
    last = daily[-1] if daily else {}

    gate_action = _cfg_get(gate, "engine_action.action") or "UNKNOWN"
    p0_risk = p0.get("risk_off") if isinstance(p0.get("risk_off"), dict) else {}
    p0_kill = p0.get("kill_switch") if isinstance(p0.get("kill_switch"), dict) else {}
    p0_risk_enabled = bool(p0_risk.get("enabled"))
    p0_kill_triggered = bool(p0_kill.get("triggered"))
    p0_kill_mode = str(_cfg_get(p0_kill, "limits.mode") or "BLOCK").upper()

    validation_expected = "ALLOW"
    if bool(last.get("expected_trigger")):
        validation_expected = "BLOCK"
    elif bool(last.get("expect_day_loss")) or bool(last.get("expect_dd")):
        validation_expected = "REDUCE"

    # Runtime resilience must follow the P0 hard-trigger contract. The
    # kill_switch_validation report also contains strategy-curve shadow signals,
    # but those are diagnostic unless P0 has actually triggered risk_off/kill_switch.
    exp = "ALLOW"
    if p0_risk_enabled or p0_kill_triggered:
        exp = "REDUCE" if p0_kill_mode == "REDUCE" else "BLOCK"
    elif not p0:
        exp = validation_expected

    if p0_risk_enabled or p0_kill_triggered:
        actual = "BLOCK"
        actual_source = "p0_daily_check"
    else:
        actual = gate_action
        actual_source = "gate_daily"
    conservative_block = exp == "ALLOW" and actual in {"REDUCE", "BLOCK"}
    status = PASS if (exp == "ALLOW" and actual == "ALLOW") or conservative_block or (exp in {"REDUCE", "BLOCK"} and actual in {"REDUCE", "BLOCK"}) else FAIL
    return {
        "status": status,
        "expected_mode": exp,
        "actual_mode": actual,
        "actual_source": actual_source,
        "gate_action": gate_action,
        "p0_daily_check": str(p0_path) if p0_path else None,
        "p0_risk_off_enabled": p0_risk_enabled,
        "p0_kill_switch_triggered": p0_kill_triggered,
        "p0_risk_off_reasons": p0_risk.get("reasons") if isinstance(p0_risk.get("reasons"), list) else [],
        "validation_expected_mode": validation_expected,
        "p0_hard_trigger_basis": _cfg_get(p0_kill, "metrics.hard_trigger_basis"),
        "p0_hard_trigger_metrics": _cfg_get(p0_kill, "metrics.hard_trigger_metrics", {}),
        "conservative_block": conservative_block,
        "metrics": {
            "max_drawdown": last.get("max_dd"),
            "max_drawdown_limit": last.get("max_dd_limit"),
            "last_day_ret": last.get("last_day_ret"),
            "daily_loss_limit": last.get("day_loss_limit"),
            "kill_triggered": last.get("kill_triggered"),
        },
    }


def _check_sla(cfg: Dict[str, Any]) -> Dict[str, Any]:
    section = _cfg_get(cfg, "sla_guard", {})
    path = Path(str(section.get("execution_health_json", LOG_DIR / "execution_health_observed_latest.json")))
    obj = _read_json(path)
    p95 = _safe_float(_cfg_get(obj, "execution_health.metric.slippage_bps_p95"))
    caution = _safe_float(_cfg_get(obj, "execution_health.threshold.slippage_bps_caution") or section.get("p95_caution", 10.0))
    block = _safe_float(_cfg_get(obj, "execution_health.threshold.slippage_bps_block") or section.get("p95_block", 20.0))

    mode = "NORMAL"
    order_rate_limit = 1.0
    max_exposure = 1.0
    if not _is_finite(p95):
        return {
            "status": FAIL,
            "metric": {"p95": None},
            "threshold": {"caution": caution, "block": block},
            "safe_mode": {"mode": "SAFE_BLOCK", "order_rate_limit": 0.0, "max_exposure": 0.0},
        }
    if p95 >= block:
        mode = "SAFE_BLOCK"
        order_rate_limit = 0.0
        max_exposure = 0.0
    elif p95 >= caution:
        mode = "SAFE_REDUCE"
        order_rate_limit = 0.5
        max_exposure = 0.6

    status = PASS
    return {
        "status": status,
        "metric": {"p95": p95},
        "threshold": {"caution": caution, "block": block},
        "safe_mode": {"mode": mode, "order_rate_limit": order_rate_limit, "max_exposure": max_exposure},
    }


def build_report(cfg: Dict[str, Any]) -> Dict[str, Any]:
    p0 = _read_json(_latest_p0_result() or Path(""))
    p0_prev_bday = _norm_ymd(_cfg_get(p0, "prices.prev_weekday"))
    p0_asof = _norm_ymd(p0.get("as_of_ymd") if isinstance(p0, dict) else "")
    as_of_cfg = _norm_ymd(_cfg_get(cfg, "as_of_ymd", ""))
    as_of = p0_asof or as_of_cfg or _now_kst().strftime("%Y%m%d")
    gateway = _check_gateway(cfg)
    routes = _check_routes(cfg)
    data = _check_data(cfg, as_of=as_of)
    risk = _check_risk()
    sla = _check_sla(cfg)

    checks = {
        "gateway_health": gateway,
        "route_table": routes,
        "data_freshness_integrity": data,
        "risk_fail_closed": risk,
        "sla_auto_degrade": sla,
    }
    overall = PASS if all(v.get("status") == PASS for v in checks.values()) else FAIL
    new_orders = "YES"
    if data.get("new_orders") == "NO" or risk.get("actual_mode") in {"BLOCK"} or overall == FAIL:
        new_orders = "NO"

    return {
        "generated_at": _now_kst().isoformat(),
        "as_of_ymd": as_of,
        "overall_status": overall,
        "new_orders": new_orders,
        "checks": checks,
        "policy": "FAIL_CLOSED",
    }


def write_bundle(report: Dict[str, Any], out_dir: Path) -> Tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = _now_kst().strftime("%Y%m%d_%H%M%S")
    out_json = out_dir / f"resilience_check_{stamp}.json"
    out_latest = out_dir / "resilience_check_latest.json"
    out_sha = out_dir / f"resilience_check_{stamp}.sha256"
    out_sha_latest = out_dir / "resilience_check_latest.sha256"

    raw = json.dumps(report, ensure_ascii=False, indent=2)
    _clear_readonly(out_json)
    _clear_readonly(out_latest)
    _clear_readonly(out_sha)
    _clear_readonly(out_sha_latest)

    out_json.write_text(raw, encoding="utf-8-sig")
    out_latest.write_text(raw, encoding="utf-8-sig")
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    sha_line = f"{digest}  {out_json.name}\n"
    out_sha.write_text(sha_line, encoding="utf-8")
    out_sha_latest.write_text(sha_line, encoding="utf-8")

    _set_readonly(out_json)
    _set_readonly(out_latest)
    _set_readonly(out_sha)
    _set_readonly(out_sha_latest)
    return out_json, out_sha


def main() -> int:
    ap = argparse.ArgumentParser(description="3-minute resilience fail-closed checklist")
    ap.add_argument("--config", default=str(CFG_PATH))
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    args = ap.parse_args()

    cfg = _read_cfg(Path(args.config))
    report = build_report(cfg)
    out_json, out_sha = write_bundle(report, Path(args.out_dir))
    print(f"[RESILIENCE] status={report.get('overall_status')} new_orders={report.get('new_orders')}")
    print(f"[RESILIENCE] json={out_json}")
    print(f"[RESILIENCE] sha256={out_sha}")
    return 0 if report.get("overall_status") == PASS else 2


if __name__ == "__main__":
    raise SystemExit(main())
