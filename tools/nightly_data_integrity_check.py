from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import re
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple

from holiday_manager import HolidayManager

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
STATE_PATH = LOG_DIR / "nightly_data_integrity_state.json"

DEFAULT_SCHEMA_FILES = [
    ROOT / "paper" / "fills.csv",
    ROOT / "paper" / "trades.csv",
]

DEFAULT_INGEST_SERIES = [
    {
        "name": "kis_fills_api_daily",
        "glob": str(ROOT / "paper" / "kis_fills_api_*.csv"),
        "threshold_pct": 0.25,
    },
    {
        "name": "candidates_latest_data_daily",
        "glob": str(ROOT / "2_Logs" / "candidates_latest_data.bak_*.csv"),
        "threshold_pct": 0.25,
    },
]

DEFAULT_CONFIG_FILES = [
    ROOT / "paper" / "paper_engine_config.json",
    ROOT / "paper" / "paper_engine_config.lock.json",
    ROOT / "run_paper_daily.bat",
]


def _now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _safe_load_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return fallback


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _is_null_like(v: str) -> bool:
    s = str(v or "").strip()
    return s == "" or s.lower() in {"none", "null", "nan", "na", "n/a"}


def _infer_scalar_type(v: str) -> Optional[str]:
    s = str(v or "").strip()
    if _is_null_like(s):
        return None
    lo = s.lower()
    if lo in {"true", "false"}:
        return "bool"
    try:
        int(s)
        return "int"
    except Exception:
        pass
    try:
        float(s)
        return "float"
    except Exception:
        return "string"


def _promote_type(cur: Optional[str], new_t: Optional[str]) -> Optional[str]:
    if new_t is None:
        return cur
    if cur is None:
        return new_t
    if cur == new_t:
        return cur
    if {cur, new_t} <= {"int", "float"}:
        return "float"
    return "string"


def _profile_csv_schema(path: Path, sample_limit: int = 200000) -> Dict[str, Any]:
    columns: Dict[str, Dict[str, Any]] = {}
    rows = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        for col in fieldnames:
            columns[col] = {"non_null": 0, "null": 0, "inferred_type": None}
        for row in reader:
            rows += 1
            for col in fieldnames:
                val = row.get(col, "")
                if _is_null_like(val):
                    columns[col]["null"] += 1
                    continue
                columns[col]["non_null"] += 1
                t = _infer_scalar_type(str(val))
                columns[col]["inferred_type"] = _promote_type(columns[col]["inferred_type"], t)
            if rows >= sample_limit:
                break

    for col in columns:
        c = columns[col]
        total = c["non_null"] + c["null"]
        c["null_ratio"] = (float(c["null"]) / float(total)) if total > 0 else 0.0
        if c["inferred_type"] is None:
            c["inferred_type"] = "unknown"

    return {
        "path": str(path),
        "rows_profiled": rows,
        "columns": columns,
    }


def _mean_std(values: List[float]) -> Tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean_v = sum(values) / float(len(values))
    if len(values) < 2:
        return mean_v, 0.0
    var = sum((x - mean_v) ** 2 for x in values) / float(len(values))
    return mean_v, math.sqrt(var)


def _schema_drift_check(state: Dict[str, Any]) -> Dict[str, Any]:
    issues: List[Dict[str, Any]] = []
    snapshots = state.setdefault("schema_snapshots", {})
    null_hist = state.setdefault("null_ratio_history", {})
    current_profiles: List[Dict[str, Any]] = []

    for path in DEFAULT_SCHEMA_FILES:
        if not path.exists():
            issues.append({"type": "missing_file", "path": str(path), "severity": "WARN"})
            continue
        profile = _profile_csv_schema(path)
        current_profiles.append(profile)

        prev = snapshots.get(str(path), {})
        prev_cols = (prev.get("columns") or {}) if isinstance(prev, dict) else {}
        cur_cols = profile.get("columns", {})

        for col, cur_meta in cur_cols.items():
            prev_meta = prev_cols.get(col, {})
            prev_type = str(prev_meta.get("inferred_type", ""))
            cur_type = str(cur_meta.get("inferred_type", ""))
            if prev_type and prev_type != cur_type:
                issues.append(
                    {
                        "type": "type_change",
                        "path": str(path),
                        "column": col,
                        "previous": prev_type,
                        "current": cur_type,
                        "severity": "WARN",
                    }
                )

            hist_key = f"{path}|{col}"
            hist = list(null_hist.get(hist_key, []))
            cur_null = float(cur_meta.get("null_ratio", 0.0))
            if len(hist) >= 5:
                mean_v, std_v = _mean_std(hist)
                threshold = mean_v + 3.0 * std_v
                if std_v > 0.0 and cur_null > threshold:
                    issues.append(
                        {
                            "type": "null_ratio_spike_3sigma",
                            "path": str(path),
                            "column": col,
                            "current_null_ratio": cur_null,
                            "history_mean": mean_v,
                            "history_std": std_v,
                            "threshold": threshold,
                            "severity": "WARN",
                        }
                    )
                elif std_v == 0.0 and cur_null > (mean_v + 0.03):
                    issues.append(
                        {
                            "type": "null_ratio_spike_flat_history",
                            "path": str(path),
                            "column": col,
                            "current_null_ratio": cur_null,
                            "history_mean": mean_v,
                            "threshold": mean_v + 0.03,
                            "severity": "WARN",
                        }
                    )

            hist.append(cur_null)
            if len(hist) > 60:
                hist = hist[-60:]
            null_hist[hist_key] = hist

        snapshots[str(path)] = profile

    return {
        "name": "schema_drift",
        "ok": len(issues) == 0,
        "issues": issues,
        "profiles": current_profiles,
    }


def _extract_ymd_from_name(name: str) -> Optional[str]:
    m = re.search(r"(20\d{6})", name)
    return m.group(1) if m else None


def _count_csv_rows(path: Path) -> int:
    with path.open("rb") as f:
        line_count = 0
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            line_count += chunk.count(b"\n")
    return max(0, line_count - 1)


def _iter_glob_paths(glob_expr: str) -> Iterable[Path]:
    parent = Path(glob_expr).parent
    pattern = Path(glob_expr).name
    if not parent.exists():
        return []
    return parent.glob(pattern)


def _market_status_for_ymd(ymd: str) -> Dict[str, Any]:
    status = HolidayManager().explain(ymd)
    return {
        "ymd": ymd,
        "is_open": bool(status.is_open),
        "reason": str(status.reason or ""),
        "source_calendar": "holiday_manager",
    }


def _load_final_score_merge_policy_context() -> Dict[str, Any]:
    latest_path = LOG_DIR / "final_score_merge_status_latest.json"
    latest = _safe_load_json(latest_path, {})
    dated_paths = sorted(LOG_DIR.glob("final_score_merge_status_20*.json"))
    prev = _safe_load_json(dated_paths[-2], {}) if len(dated_paths) >= 2 else {}
    return {
        "latest_rows": int(latest.get("rows", 0) or 0),
        "latest_score_regime": str(latest.get("score_regime", "") or ""),
        "latest_policy": str(latest.get("policy", "") or ""),
        "latest_asof_ymd": str(latest.get("asof_ymd", "") or ""),
        "prev_rows": int(prev.get("rows", 0) or 0),
        "prev_score_regime": str(prev.get("score_regime", "") or ""),
        "prev_policy": str(prev.get("policy", "") or ""),
        "prev_asof_ymd": str(prev.get("asof_ymd", "") or ""),
    }


def _ingest_count_check() -> Dict[str, Any]:
    series_results: List[Dict[str, Any]] = []
    issues: List[Dict[str, Any]] = []
    today_ymd = dt.datetime.now().strftime("%Y%m%d")
    today_market_status = _market_status_for_ymd(today_ymd)
    policy_ctx = _load_final_score_merge_policy_context()

    for s in DEFAULT_INGEST_SERIES:
        name = str(s.get("name"))
        glob_expr = str(s.get("glob"))
        threshold = float(s.get("threshold_pct", 0.25))

        per_day: Dict[str, Path] = {}
        for p in sorted(_iter_glob_paths(glob_expr)):
            ymd = _extract_ymd_from_name(p.name)
            if not ymd:
                continue
            prev = per_day.get(ymd)
            if prev is None or p.name > prev.name:
                per_day[ymd] = p

        if not per_day:
            issues.append({"type": "no_series_data", "series": name, "glob": glob_expr, "severity": "WARN"})
            series_results.append({"name": name, "ok": False, "reason": "no_series_data"})
            continue

        ordered = sorted(per_day.items(), key=lambda x: x[0])
        rows_by_day: List[Tuple[str, int]] = []
        skipped_non_session: List[Dict[str, Any]] = []
        for ymd, p in ordered:
            market_status = _market_status_for_ymd(ymd)
            if not market_status["is_open"]:
                skipped_non_session.append(
                    {
                        "ymd": ymd,
                        "reason": market_status["reason"],
                        "path": str(p),
                    }
                )
                continue
            try:
                rows_by_day.append((ymd, _count_csv_rows(p)))
            except Exception:
                continue

        if not rows_by_day:
            issues.append(
                {
                    "type": "no_trading_session_series_data",
                    "series": name,
                    "glob": glob_expr,
                    "severity": "WARN",
                    "source_calendar": "holiday_manager",
                    "skipped_non_session": skipped_non_session,
                }
            )
            series_results.append(
                {
                    "name": name,
                    "ok": False,
                    "reason": "no_trading_session_series_data",
                    "source_calendar": "holiday_manager",
                    "skipped_non_session_count": len(skipped_non_session),
                }
            )
            continue

        target_idx = -1
        for i, (ymd, _) in enumerate(rows_by_day):
            if ymd == today_ymd:
                target_idx = i
        if target_idx < 0:
            target_idx = len(rows_by_day) - 1

        target_ymd, target_rows = rows_by_day[target_idx]
        baseline_vals = [r for _, r in rows_by_day[max(0, target_idx - 30):target_idx]]
        med = float(median(baseline_vals)) if baseline_vals else 0.0
        delta_pct = ((float(target_rows) - med) / med) if med > 0 else 0.0
        exceed = abs(delta_pct) >= threshold if med > 0 else False
        waived_by_policy_shift = False
        waiver_reason = ""

        if exceed and name == "candidates_latest_data_daily":
            regime_changed = (
                bool(policy_ctx.get("latest_score_regime"))
                and bool(policy_ctx.get("prev_score_regime"))
                and policy_ctx.get("latest_score_regime") != policy_ctx.get("prev_score_regime")
            )
            policy_changed = (
                bool(policy_ctx.get("latest_policy"))
                and bool(policy_ctx.get("prev_policy"))
                and policy_ctx.get("latest_policy") != policy_ctx.get("prev_policy")
            )
            asof_changed = (
                bool(policy_ctx.get("latest_asof_ymd"))
                and bool(policy_ctx.get("prev_asof_ymd"))
                and policy_ctx.get("latest_asof_ymd") != policy_ctx.get("prev_asof_ymd")
            )
            rows_aligned = int(policy_ctx.get("latest_rows", 0)) == int(target_rows)
            if rows_aligned and (regime_changed or policy_changed or asof_changed):
                waived_by_policy_shift = True
                waiver_reason = "score_policy_or_regime_shift"

        if exceed and not waived_by_policy_shift:
            issues.append(
                {
                    "type": "ingest_count_shift",
                    "series": name,
                    "asof_ymd": target_ymd,
                    "today_count": target_rows,
                    "median_30d": med,
                    "delta_pct": delta_pct,
                    "threshold_pct": threshold,
                    "severity": "WARN",
                }
            )
        elif exceed and waived_by_policy_shift:
            issues.append(
                {
                    "type": "ingest_count_shift_waived",
                    "series": name,
                    "asof_ymd": target_ymd,
                    "today_count": target_rows,
                    "median_30d": med,
                    "delta_pct": delta_pct,
                    "threshold_pct": threshold,
                    "severity": "INFO",
                    "reason": waiver_reason,
                    "policy_context": policy_ctx,
                }
            )

        series_results.append(
            {
                "name": name,
                "ok": (not exceed) or waived_by_policy_shift,
                "asof_ymd": target_ymd,
                "today_count": target_rows,
                "median_30d": med,
                "delta_pct": delta_pct,
                "threshold_pct": threshold,
                "baseline_n": len(baseline_vals),
                "waived_by_policy_shift": waived_by_policy_shift,
                "waiver_reason": waiver_reason,
                "source_calendar": "holiday_manager",
                "today_market_status": today_market_status,
                "skipped_non_session_count": len(skipped_non_session),
                "skipped_non_session": skipped_non_session[-10:],
            }
        )

    effective_issues = [x for x in issues if str(x.get("severity", "")).upper() == "WARN"]
    return {
        "name": "ingest_count_change",
        "ok": len(effective_issues) == 0,
        "issues": issues,
        "series": series_results,
    }


def _config_hash_check(state: Dict[str, Any]) -> Dict[str, Any]:
    issues: List[Dict[str, Any]] = []
    current: List[Dict[str, Any]] = []
    prev = state.setdefault("config_hashes", {})

    for path in DEFAULT_CONFIG_FILES:
        if not path.exists():
            issues.append({"type": "missing_config_file", "path": str(path), "severity": "WARN"})
            continue
        sha = _sha256_file(path)
        old = str(prev.get(str(path), ""))
        changed = bool(old and old != sha)
        if changed:
            issues.append(
                {
                    "type": "config_hash_changed",
                    "path": str(path),
                    "previous_sha256": old,
                    "current_sha256": sha,
                    "severity": "WARN",
                }
            )
        prev[str(path)] = sha
        current.append({"path": str(path), "cfg_sha256": sha, "changed": changed})

    return {
        "name": "config_hash_mismatch",
        "ok": len(issues) == 0,
        "issues": issues,
        "files": current,
    }


def run_checks() -> Dict[str, Any]:
    state = _safe_load_json(STATE_PATH, {})
    schema = _schema_drift_check(state)
    ingest = _ingest_count_check()
    cfg = _config_hash_check(state)

    checks = [schema, ingest, cfg]
    all_issues = []
    for c in checks:
        for it in c.get("issues", []):
            if str(it.get("severity", "")).upper() != "WARN":
                continue
            all_issues.append(it)

    payload = {
        "generated_at": _now_iso(),
        "ok": len(all_issues) == 0,
        "checks": checks,
        "issue_count": len(all_issues),
        "issues": all_issues,
    }
    _save_json(STATE_PATH, state)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Nightly data integrity checks")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--warn-only", action="store_true")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"nightly_data_integrity_{stamp}.json")
    out_latest = LOG_DIR / "nightly_data_integrity_latest.json"

    result = run_checks()
    _save_json(out_json, result)
    _save_json(out_latest, result)

    print(f"[NIGHTLY_INTEGRITY] ok={result['ok']} issues={result['issue_count']}")
    print(f"[NIGHTLY_INTEGRITY] json={out_json}")
    print(f"[NIGHTLY_INTEGRITY] latest={out_latest}")

    if args.warn_only:
        return 0
    return 0 if result.get("ok", False) else 2


if __name__ == "__main__":
    raise SystemExit(main())
