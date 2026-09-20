from __future__ import annotations

import argparse
import glob
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml


ROOT = Path(r"E:\1_Data")
DEFAULT_POLICY = ROOT / "config" / "sensitivity_matrix_policy_roota.yaml"
DEFAULT_STATE = ROOT / "2_Logs" / "sensitivity_guard_state.json"
DEFAULT_OUT = ROOT / "2_Logs" / "sensitivity_guard_latest.json"
DEFAULT_OUT_STAMPED = ROOT / "2_Logs"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_float(v: Any) -> float:
    try:
        f = float(v)
        if math.isfinite(f):
            return f
    except Exception:
        pass
    return math.nan


def _json_path_get(obj: Any, path: str) -> Any:
    cur = obj
    token = ""
    i = 0
    parts: List[Any] = []
    while i < len(path):
        ch = path[i]
        if ch == ".":
            if token:
                parts.append(token)
                token = ""
            i += 1
            continue
        if ch == "[":
            if token:
                parts.append(token)
                token = ""
            j = path.find("]", i)
            if j < 0:
                raise KeyError(f"invalid json path: {path}")
            idx = int(path[i + 1 : j])
            parts.append(idx)
            i = j + 1
            continue
        token += ch
        i += 1
    if token:
        parts.append(token)
    for p in parts:
        cur = cur[p]
    return cur


def _eval_transform(value: Any, expr: str) -> float:
    local_vars = {"value": _safe_float(value)}
    out = eval(expr, {"__builtins__": {}}, local_vars)
    return _safe_float(out)


def _resolve_source_file(src: Dict[str, Any]) -> Tuple[Path | None, List[Path]]:
    f = src.get("file")
    if isinstance(f, str) and f.strip():
        fp = Path(f)
        if fp.exists():
            return fp, [fp]
    fg = src.get("file_glob")
    candidates: List[Path] = []
    if isinstance(fg, str) and fg.strip():
        candidates = [Path(x) for x in glob.glob(fg)]
        candidates = [p for p in candidates if p.exists()]
        candidates.sort(key=lambda p: p.stat().st_mtime)
        if candidates:
            return candidates[-1], candidates
    ff = src.get("fallback_file")
    if isinstance(ff, str) and ff.strip():
        fp = Path(ff)
        if fp.exists():
            return fp, [fp]
    return None, candidates


def _history_values(src: Dict[str, Any], json_path: str, transform: str, windows: int) -> List[float]:
    _, candidates = _resolve_source_file(src)
    if not candidates:
        return []
    if len(candidates) > windows:
        candidates = candidates[-windows:]
    vals: List[float] = []
    for fp in candidates:
        try:
            obj = _read_json(fp)
            raw = _json_path_get(obj, json_path)
            cur = _eval_transform(raw, transform)
            if math.isfinite(cur):
                vals.append(cur)
        except Exception:
            continue
    return vals


def _window_seconds(policy: Dict[str, Any], label: str) -> int:
    sec = ((policy.get("evaluation_windows") or {}).get(label))
    try:
        return int(sec)
    except Exception:
        return 0


def _check_threshold(current: float, baseline: float, th: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    ok = True

    def add(flag: bool, txt: str) -> None:
        nonlocal ok
        if not flag:
            ok = False
        reasons.append(txt)

    if "relative_change_ge" in th:
        x = _safe_float(th["relative_change_ge"])
        got = 0.0 if baseline == 0 else (current - baseline) / abs(baseline)
        add(got >= x, f"relative_change={got:.6f}>= {x}")
    if "relative_multiple_ge" in th:
        x = _safe_float(th["relative_multiple_ge"])
        got = math.inf if baseline == 0 else current / baseline
        add(got >= x, f"relative_multiple={got:.6f}>= {x}")
    if "absolute_ge" in th:
        x = _safe_float(th["absolute_ge"])
        add(current >= x, f"absolute={current:.6f}>= {x}")
    if "absolute_increase_ge" in th:
        x = _safe_float(th["absolute_increase_ge"])
        got = current - baseline
        add(got >= x, f"absolute_increase={got:.6f}>= {x}")
    if "absolute_increase_ms_ge" in th:
        x = _safe_float(th["absolute_increase_ms_ge"])
        got = current - baseline
        add(got >= x, f"absolute_increase_ms={got:.6f}>= {x}")
    if "absolute_increase_bps_ge" in th:
        x = _safe_float(th["absolute_increase_bps_ge"])
        got = current - baseline
        add(got >= x, f"absolute_increase_bps={got:.6f}>= {x}")
    if "relative_drop_ge" in th:
        x = _safe_float(th["relative_drop_ge"])
        got = 0.0 if baseline == 0 else (baseline - current) / abs(baseline)
        add(got >= x, f"relative_drop={got:.6f}>= {x}")
    if "absolute_drop_pp_ge" in th:
        x = _safe_float(th["absolute_drop_pp_ge"])
        got = (baseline - current) * 100.0
        add(got >= x, f"absolute_drop_pp={got:.6f}>= {x}")

    return ok, reasons


def _counter_update(
    state: Dict[str, Any],
    metric_id: str,
    level: str,
    cond: bool,
    now_ts: float,
    window_sec: int,
    need: int,
) -> Tuple[int, bool]:
    counters = state.setdefault("counters", {})
    key = f"{metric_id}::{level}"
    entry = counters.get(key, {"count": 0, "last_ts": 0.0})
    prev_count = int(entry.get("count", 0))
    prev_ts = _safe_float(entry.get("last_ts", 0.0))
    if not math.isfinite(prev_ts):
        prev_ts = 0.0

    if cond:
        if window_sec <= 0:
            count = 1
        else:
            if prev_ts > 0 and (now_ts - prev_ts) <= (window_sec * 1.5 + 1.0):
                count = prev_count + 1
            else:
                count = 1
        entry["last_ts"] = now_ts
    else:
        count = 0
        entry["last_ts"] = now_ts
    entry["count"] = count
    counters[key] = entry
    return count, cond and (count >= max(1, need))


def _action_priority(action: str) -> int:
    pri = {
        "emergency_stop_signed": 100,
        "rollback": 90,
        "degrade_to_paper": 80,
        "broker_failover": 70,
        "data_failover": 60,
        "soft_pause": 50,
        "rate_limit_new_orders": 40,
        "price_band_tighten": 30,
        "none": 0,
    }
    return pri.get(action, 0)


def main() -> int:
    ap = argparse.ArgumentParser(description="Evaluate sensitivity matrix policy and emit verdict/action.")
    ap.add_argument("--policy", default=str(DEFAULT_POLICY))
    ap.add_argument("--state", default=str(DEFAULT_STATE))
    ap.add_argument("--out-latest", default=str(DEFAULT_OUT))
    ap.add_argument("--out-stamped-dir", default=str(DEFAULT_OUT_STAMPED))
    ap.add_argument("--run-id", default="")
    args = ap.parse_args()

    policy_path = Path(args.policy)
    state_path = Path(args.state)
    out_latest = Path(args.out_latest)
    out_stamped_dir = Path(args.out_stamped_dir)
    run_id = args.run_id.strip() or datetime.now().strftime("%Y%m%d_%H%M%S")
    now = datetime.now(timezone.utc)
    now_ts = now.timestamp()

    policy = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    state = {}
    if state_path.exists():
        try:
            state = _read_json(state_path)
        except Exception:
            state = {}
    if not isinstance(state, dict):
        state = {}

    metrics_out: List[Dict[str, Any]] = []
    highest_action = "none"
    triggered_total = 0
    hard_or_critical_total = 0

    level_order = ["monitor", "soft_precheck", "soft", "hard", "critical"]
    level_action_prefer = ["critical", "hard", "soft", "soft_precheck", "monitor"]

    for m in policy.get("metrics", []):
        metric_id = str(m.get("metric_id") or "")
        source = m.get("source") or {}
        json_path = str(source.get("json_path") or "")
        transform = str(source.get("transform") or "value")
        src_file, _ = _resolve_source_file(source)

        current = math.nan
        load_err = ""
        if src_file is None:
            load_err = "source file not found"
        else:
            try:
                obj = _read_json(src_file)
                raw = _json_path_get(obj, json_path)
                current = _eval_transform(raw, transform)
            except Exception as e:
                load_err = f"{type(e).__name__}: {e}"

        base_mode = ((m.get("baseline") or {}).get("mode") or "").strip().lower()
        base_windows = int((m.get("baseline") or {}).get("windows") or 20)
        baseline = current
        history_vals: List[float] = []
        if math.isfinite(current) and base_mode == "rolling_median":
            history_vals = _history_values(source, json_path, transform, max(2, base_windows))
            if history_vals:
                baseline = float(statistics.median(history_vals))

        threshold_results: Dict[str, Any] = {}
        triggered_levels: List[str] = []
        selected_action = "none"

        thresholds = m.get("thresholds") or {}
        for level in level_order:
            th = thresholds.get(level)
            if not isinstance(th, dict):
                continue

            if not math.isfinite(current) or not math.isfinite(baseline):
                cond = False
                reasons = ["non-finite current or baseline"]
            else:
                cond, reasons = _check_threshold(current, baseline, th)

            window_label = str(th.get("window") or "immediate_sec")
            window_sec = _window_seconds(policy, window_label)
            need = int(th.get("consecutive_windows") or 1)
            count, fired = _counter_update(state, metric_id, level, cond, now_ts, window_sec, need)
            threshold_results[level] = {
                "condition": bool(cond),
                "fired": bool(fired),
                "counter": int(count),
                "need": int(need),
                "window": window_label,
                "window_sec": int(window_sec),
                "reasons": reasons,
            }
            if fired:
                triggered_levels.append(level)
                triggered_total += 1
                if level in {"hard", "critical"}:
                    hard_or_critical_total += 1

        action_map = m.get("action_map") or {}
        for lvl in level_action_prefer:
            if lvl in triggered_levels and lvl in action_map:
                selected_action = str(action_map[lvl])
                break

        if _action_priority(selected_action) > _action_priority(highest_action):
            highest_action = selected_action

        metrics_out.append(
            {
                "metric_id": metric_id,
                "proxy_for": m.get("proxy_for"),
                "source_file": str(src_file) if src_file else "",
                "json_path": json_path,
                "current_value": current if math.isfinite(current) else None,
                "baseline_value": baseline if math.isfinite(baseline) else None,
                "history_n": len(history_vals),
                "load_error": load_err,
                "triggered_levels": triggered_levels,
                "selected_action": selected_action,
                "thresholds": threshold_results,
            }
        )

    block_actions = {"emergency_stop_signed", "rollback", "degrade_to_paper", "broker_failover", "data_failover"}
    out = {
        "generated_at_utc": now.isoformat(),
        "run_id": run_id,
        "policy_path": str(policy_path),
        "state_path": str(state_path),
        "summary": {
            "metrics_total": len(metrics_out),
            "triggered_total": triggered_total,
            "hard_or_critical_total": hard_or_critical_total,
            "verdict_action": highest_action,
            "apply_blocked": highest_action in block_actions,
        },
        "metrics": metrics_out,
    }

    _write_json(out_latest, out)
    stamped = out_stamped_dir / f"sensitivity_guard_{run_id}.json"
    _write_json(stamped, out)
    _write_json(state_path, state)

    print(f"[SENSITIVITY] wrote_latest={out_latest}")
    print(f"[SENSITIVITY] wrote_stamped={stamped}")
    print(f"[SENSITIVITY] verdict_action={highest_action}")
    print(f"[SENSITIVITY] apply_blocked={str(highest_action in block_actions).lower()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

