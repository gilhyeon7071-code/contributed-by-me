from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
INPUT_CSV = LOG_DIR / "entry_trace_performance_latest.csv"
P1_HISTORY = LOG_DIR / "p1_entry_gate_status_history.csv"
OUT_JSON = LOG_DIR / "normal_intraday_entry_context_latest.json"
OUT_CSV = LOG_DIR / "normal_intraday_entry_context_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "normal_intraday_entry_context_summary_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str)


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
        except Exception:
            return {}
    return {}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return default
        return val
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def _parse_entry_ts(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    for fmt in ("%Y%m%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    digits = re.sub(r"[^0-9]", "", text)
    if len(digits) >= 14:
        try:
            return datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
        except ValueError:
            return None
    return None


def _filename_dt(path: Path, prefix: str) -> Optional[datetime]:
    m = re.match(rf"{re.escape(prefix)}_(\d{{8}})_(\d{{6}})\.json$", path.name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
    except ValueError:
        return None


def _nearest_file_before(paths: Iterable[Path], target: datetime, prefix: str) -> Tuple[Optional[Path], Optional[datetime]]:
    best_path: Optional[Path] = None
    best_dt: Optional[datetime] = None
    for path in paths:
        dt = _filename_dt(path, prefix)
        if dt is None or dt > target:
            continue
        if best_dt is None or dt > best_dt:
            best_dt = dt
            best_path = path
    return best_path, best_dt


def _nearest_p1_before(p1: pd.DataFrame, target: datetime, ymd: str) -> Dict[str, Any]:
    if p1.empty or "ts" not in p1.columns:
        return {}
    work = p1.copy()
    if "as_of_ymd" in work.columns:
        work = work[work["as_of_ymd"].astype(str) == str(ymd)].copy()
    if work.empty:
        return {}
    work["_ts_dt"] = pd.to_datetime(work["ts"], errors="coerce")
    work = work[work["_ts_dt"].notna() & (work["_ts_dt"] <= target)].copy()
    if work.empty:
        return {}
    row = work.sort_values("_ts_dt").iloc[-1]
    return {
        "p1_ts": str(row.get("ts", "") or ""),
        "p1_market_regime": str(row.get("market_regime", "") or ""),
        "p1_entry_gate_decision_before_p1": str(row.get("entry_gate_decision_before_p1", "") or ""),
        "p1_max_new_before": _to_int(row.get("max_new_before"), 0),
        "p1_max_new_after": _to_int(row.get("max_new_after"), 0),
        # [2026-08-24] max_new_after 는 최종이 아니다(PAPER_EXIT_ONLY 로 0 이 될 수 있다).
        "p1_max_new_final": _to_int(row.get("max_new_final_expected"), -1),
        "p1_exit_only_mode": bool(row.get("exit_only_mode")),
        "p1_entry_candidates_before": _to_int(row.get("entry_candidates_before"), 0),
        "p1_entry_candidates_after": _to_int(row.get("entry_candidates_after"), 0),
        "p1_actions_count": _to_int(row.get("actions_count"), 0),
        "p1_missing_inputs_count": _to_int(row.get("missing_inputs_count"), 0),
    }


def _extract_p0_context(path: Optional[Path]) -> Dict[str, Any]:
    obj = _read_json(path) if path else {}
    risk_off = obj.get("risk_off") if isinstance(obj.get("risk_off"), dict) else {}
    kill = obj.get("kill_switch") if isinstance(obj.get("kill_switch"), dict) else {}
    kill_metrics = kill.get("metrics") if isinstance(kill.get("metrics"), dict) else {}
    crash = obj.get("crash_risk_off") if isinstance(obj.get("crash_risk_off"), dict) else {}
    crash_metrics = crash.get("metrics") if isinstance(crash.get("metrics"), dict) else {}
    meta = obj.get("meta") if isinstance(obj.get("meta"), dict) else {}
    return {
        "p0_path": str(path or ""),
        "p0_generated_at": str(obj.get("generated_at", "") or ""),
        "p0_as_of_ymd": str(obj.get("as_of_ymd", "") or ""),
        "p0_market_regime": str(meta.get("market_regime", "") or ""),
        "p0_risk_off_enabled": bool(risk_off.get("enabled", False)),
        "p0_risk_off_reasons": "|".join([str(x) for x in risk_off.get("reasons", [])]) if isinstance(risk_off.get("reasons"), list) else "",
        "p0_kill_switch_triggered": bool(kill.get("triggered", False)),
        "p0_daily_loss_active": bool(kill_metrics.get("daily_loss_active", False)),
        "p0_last_day_ret": _to_float(kill_metrics.get("last_day_ret"), 0.0),
        "p0_max_drawdown_pct": _to_float(kill_metrics.get("max_drawdown_pct"), 0.0),
        "p0_dd_end_pct": _to_float(kill_metrics.get("dd_end_pct"), 0.0),
        "p0_crash_triggered": bool(crash.get("triggered", False)),
        "p0_crash_max_dd": _to_float(crash_metrics.get("max_dd"), 0.0),
        "p0_crash_day_ret": _to_float(crash_metrics.get("day_ret"), 0.0),
    }


def _extract_gate_daily(ymd: str) -> Dict[str, Any]:
    path = LOG_DIR / f"gate_daily_{ymd}.json"
    obj = _read_json(path)
    engine = obj.get("engine_action") if isinstance(obj.get("engine_action"), dict) else {}
    gate_macro = obj.get("gate_macro") if isinstance(obj.get("gate_macro"), dict) else {}
    snap = obj.get("snapshot") if isinstance(obj.get("snapshot"), dict) else {}
    snap_ro = snap.get("risk_off") if isinstance(snap.get("risk_off"), dict) else {}
    macro = obj.get("macro") if isinstance(obj.get("macro"), dict) else {}
    return {
        "gate_daily_path": str(path if path.exists() else ""),
        "gate_daily_generated_at": str(obj.get("generated_at", "") or ""),
        "gate_daily_regime": str(obj.get("regime", "") or macro.get("regime", "") or ""),
        "gate_daily_as_of_ymd": str(obj.get("as_of_ymd", "") or ""),
        "gate_daily_action": str(engine.get("action", "") or ""),
        "gate_daily_gate_macro_status": str(gate_macro.get("status", "") or ""),
        "gate_daily_gate_macro_msg": str(gate_macro.get("msg", "") or ""),
        "gate_daily_risk_off_enabled": bool(snap_ro.get("enabled", False)),
        "gate_daily_macro_risk_on": bool(macro.get("risk_on", False)),
    }


def _extract_pnl_context(path: Optional[Path]) -> Dict[str, Any]:
    obj = _read_json(path) if path else {}
    return {
        "pnl_path": str(path or ""),
        "pnl_generated_at": str(obj.get("generated_at", "") or ""),
        "pnl_as_of": str(obj.get("as_of", "") or obj.get("as_of_ymd", "") or ""),
        "pnl_current_mdd_abs": _to_float(obj.get("current_mdd_abs"), 0.0),
        "pnl_daily_ret": _to_float(obj.get("daily_ret"), 0.0),
        "pnl_open_positions": _to_int(obj.get("open_positions"), 0),
    }


def _risk_flags(row: Dict[str, Any]) -> List[str]:
    flags: List[str] = []
    if bool(row.get("p0_risk_off_enabled")):
        flags.append("p0_risk_off")
    if bool(row.get("p0_daily_loss_active")) and _to_float(row.get("p0_last_day_ret"), 0.0) < 0:
        flags.append("p0_daily_loss_active_negative")
    if _to_float(row.get("p0_max_drawdown_pct"), 0.0) <= -0.10:
        flags.append("p0_rolling_dd_ge_10pct")
    if _to_float(row.get("p0_crash_max_dd"), 0.0) <= -0.12:
        flags.append("p0_crash_proxy_dd_ge_12pct")
    if str(row.get("gate_daily_action") or "").upper() not in {"", "ALLOW"}:
        flags.append("gate_daily_not_allow")
    if str(row.get("gate_daily_gate_macro_status") or "").upper() not in {"", "PASS"}:
        flags.append("gate_macro_not_pass")
    if str(row.get("p1_entry_gate_decision_before_p1") or "").upper() in {"BLOCK", "REDUCE", "CAUTION"}:
        flags.append("p1_entry_gate_not_allow")
    # [2026-08-24] 최종값이 기록돼 있으면 그것을 쓴다. 없으면(-1) 예전대로 max_new_after.
    _mn_final = _to_int(row.get("p1_max_new_final"), -1)
    _mn = _mn_final if _mn_final >= 0 else _to_int(row.get("p1_max_new_after"), 1)
    if _mn <= 0:
        flags.append("p1_max_new_zero")
    if _to_float(row.get("pnl_current_mdd_abs"), 0.0) >= 0.15:
        flags.append("pnl_mdd_ge_15pct")
    return flags


def _group_summary(df: pd.DataFrame, key: str) -> List[Dict[str, Any]]:
    if df.empty or key not in df.columns:
        return []
    rows: List[Dict[str, Any]] = []
    for val, grp in df.groupby(key, dropna=False):
        net = pd.to_numeric(grp["net_ret_sum"], errors="coerce").dropna()
        rows.append(
            {
                "key": str(val),
                "entries": int(len(grp)),
                "mean_net": float(net.mean()) if len(net) else 0.0,
                "loss_sum": float(net[net < 0].sum()) if len(net) else 0.0,
                "win_rate": float((net > 0).mean()) if len(net) else 0.0,
            }
        )
    rows.sort(key=lambda x: (float(x["mean_net"]), -int(x["entries"])))
    return rows


def run() -> Dict[str, Any]:
    entries = _read_csv(INPUT_CSV)
    if entries.empty:
        payload = {"generated_at": _now_ts(), "status": "NO_INPUT", "input": str(INPUT_CSV)}
        OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    p1 = _read_csv(P1_HISTORY)
    target = entries[
        (entries["entry_class"].astype(str) == "normal_intraday_realtime")
        & (~entries["entry_trace_id"].astype(str).str.startswith("MISSING_TRACE_", na=False))
    ].copy()
    target["closed_rows_num"] = pd.to_numeric(target["closed_rows"], errors="coerce").fillna(0).astype(int)
    target = target[target["closed_rows_num"] > 0].copy()

    p0_files = list(LOG_DIR.glob("p0_daily_check_*.json"))
    pnl_files = list(LOG_DIR.glob("paper_pnl_summary_*.json"))

    rows: List[Dict[str, Any]] = []
    for _, src in target.iterrows():
        entry_dt = _parse_entry_ts(src.get("entry_ts"))
        ymd = str(src.get("entry_ymd") or "")
        if entry_dt is None or not ymd:
            continue
        p0_path, p0_dt = _nearest_file_before([p for p in p0_files if f"_{ymd}_" in p.name], entry_dt, "p0_daily_check")
        pnl_path, pnl_dt = _nearest_file_before([p for p in pnl_files if f"_{ymd}_" in p.name], entry_dt, "paper_pnl_summary")
        row: Dict[str, Any] = {
            "entry_trace_id": str(src.get("entry_trace_id", "") or ""),
            "code": str(src.get("code", "") or "").zfill(6),
            "entry_ts": str(src.get("entry_ts", "") or ""),
            "entry_ymd": ymd,
            "entry_qty": _to_int(src.get("entry_qty"), 0),
            "entry_price": _to_float(src.get("entry_price"), 0.0),
            "entry_notional": _to_float(src.get("entry_notional"), 0.0),
            "horizon": str(src.get("horizon", "") or ""),
            "net_ret_sum": _to_float(src.get("net_ret_sum"), 0.0),
            "exit_reasons": str(src.get("exit_reasons", "") or ""),
            "p0_nearest_ts": p0_dt.isoformat() if p0_dt else "",
            "pnl_nearest_ts": pnl_dt.isoformat() if pnl_dt else "",
        }
        row.update(_extract_gate_daily(ymd))
        row.update(_extract_p0_context(p0_path))
        row.update(_extract_pnl_context(pnl_path))
        row.update(_nearest_p1_before(p1, entry_dt, ymd))
        flags = _risk_flags(row)
        row["risk_flags"] = "|".join(flags)
        row["risk_flag_count"] = len(flags)
        row["weak_context"] = len(flags) > 0
        rows.append(row)

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["net_ret_sum", "entry_ts"], ascending=[True, True])
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    weak = out[out["weak_context"] == True].copy() if not out.empty and "weak_context" in out.columns else pd.DataFrame()
    clean = out[out["weak_context"] != True].copy() if not out.empty and "weak_context" in out.columns else pd.DataFrame()
    summary_rows = [
        {
            "bucket": "weak_context",
            "entries": int(len(weak)),
            "mean_net": float(pd.to_numeric(weak["net_ret_sum"], errors="coerce").mean()) if len(weak) else 0.0,
            "loss_sum": float(pd.to_numeric(weak["net_ret_sum"], errors="coerce").clip(upper=0).sum()) if len(weak) else 0.0,
        },
        {
            "bucket": "no_weak_context_detected",
            "entries": int(len(clean)),
            "mean_net": float(pd.to_numeric(clean["net_ret_sum"], errors="coerce").mean()) if len(clean) else 0.0,
            "loss_sum": float(pd.to_numeric(clean["net_ret_sum"], errors="coerce").clip(upper=0).sum()) if len(clean) else 0.0,
        },
    ]
    pd.DataFrame(summary_rows).to_csv(OUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    all_flags: Dict[str, int] = {}
    if not out.empty:
        for text in out["risk_flags"].astype(str):
            for flag in [x for x in text.split("|") if x]:
                all_flags[flag] = all_flags.get(flag, 0) + 1

    payload = {
        "generated_at": _now_ts(),
        "status": "OK",
        "input": str(INPUT_CSV),
        "outputs": {"detail_csv": str(OUT_CSV), "summary_csv": str(OUT_SUMMARY_CSV)},
        "counts": {
            "target_realized_entries": int(len(target)),
            "context_rows": int(len(out)),
            "weak_context_rows": int(len(weak)),
            "no_weak_context_rows": int(len(clean)),
        },
        "metrics": {
            "weak_context_mean_net": summary_rows[0]["mean_net"],
            "weak_context_loss_sum": summary_rows[0]["loss_sum"],
            "no_weak_context_mean_net": summary_rows[1]["mean_net"],
            "no_weak_context_loss_sum": summary_rows[1]["loss_sum"],
        },
        "flag_counts": dict(sorted(all_flags.items(), key=lambda kv: (-kv[1], kv[0]))),
        "by_entry_ymd": _group_summary(out, "entry_ymd"),
        "by_exit_reasons": _group_summary(out, "exit_reasons"),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    print(json.dumps(run(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
