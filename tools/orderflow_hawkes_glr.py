from __future__ import annotations

import argparse
import csv
import json
import math
import os
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
OUT_JSON = LOG_DIR / "orderflow_hawkes_glr_latest.json"
OUT_CSV = LOG_DIR / "orderflow_hawkes_glr_latest.csv"
OBSERVER_JSON = LOG_DIR / "orderflow_observer_state_latest.json"

TRADE_TR_ID = "H0STCNT0"
EPS = 1e-9
DEFAULT_MAX_INPUT_AGE_SEC = 300


@dataclass(frozen=True)
class Event:
    code: str
    ts: datetime


@dataclass(frozen=True)
class FitResult:
    loglik: float
    mu: float
    beta: float
    eta: float
    half_life_sec: float


def _now_ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _latest_tick_jsonl() -> Optional[Path]:
    files = []
    for path in LOG_DIR.glob("kis_ws_ticks_*.jsonl"):
        stem = path.stem
        # 생산 파일명은 kis_ws_ticks{worker_suffix}_{ymd} 이다(kis_realtime_ws.py).
        # worker_suffix 가 붙으면 접미부가 8자리가 아니므로 뒤 8자리로 판정한다.
        if "hoga" in stem:
            continue  # 호가 전용 파일에는 체결(H0STCNT0) 이벤트가 없다
        suffix = stem.removeprefix("kis_ws_ticks_")
        if len(suffix) >= 8 and suffix[-8:].isdigit():
            files.append(path)
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _file_age_sec(path: Path) -> float:
    try:
        return max(0.0, datetime.now().timestamp() - path.stat().st_mtime)
    except Exception:
        return 0.0


def _parse_iso(s: Any) -> Optional[datetime]:
    raw = str(s or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _parse_tick_time(base: datetime, hhmmss: Any) -> datetime:
    raw = "".join(ch for ch in str(hhmmss or "") if ch.isdigit())
    if len(raw) < 6:
        return base
    try:
        h, m, sec = int(raw[:2]), int(raw[2:4]), int(raw[4:6])
        return base.replace(hour=h, minute=m, second=sec, microsecond=0)
    except Exception:
        return base


def _load_trade_events(path: Path) -> Tuple[List[Event], int, int]:
    events: List[Event] = []
    total_rows = 0
    malformed_rows = 0
    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total_rows += 1
            try:
                rec = json.loads(line)
            except Exception:
                malformed_rows += 1
                continue
            normalized = rec.get("normalized") if isinstance(rec.get("normalized"), dict) else {}
            tr_id = str(normalized.get("tr_id") or rec.get("tr_id") or "").strip()
            event_type = str(normalized.get("event_type") or "").strip().lower()
            if tr_id != TRADE_TR_ID and event_type != "trade":
                continue
            base_ts = _parse_iso(rec.get("ts"))
            if base_ts is None:
                malformed_rows += 1
                continue
            fields = normalized.get("fields") if isinstance(normalized.get("fields"), list) else rec.get("fields")
            fields = fields if isinstance(fields, list) else []
            code = str(normalized.get("code") or (fields[0] if fields else "") or "").strip().zfill(6)
            if not code.isdigit() or len(code) != 6:
                malformed_rows += 1
                continue
            event_ts = _parse_tick_time(base_ts, fields[1] if len(fields) > 1 else "")
            events.append(Event(code=code, ts=event_ts))
    events.sort(key=lambda x: (x.code, x.ts))
    return events, total_rows, malformed_rows


def _median(vals: List[float]) -> float:
    if not vals:
        return 0.0
    xs = sorted(float(v) for v in vals)
    n = len(xs)
    mid = n // 2
    if n % 2:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2.0


def _estimate_lambda0(events: List[datetime], quiet_minutes: int) -> Tuple[float, Dict[str, Any]]:
    if not events:
        return 0.0, {"method": "no_events", "median_count_per_min": 0.0}
    start = min(events)
    observed_end = max(events) + timedelta(seconds=1)
    quiet_end = min(start + timedelta(minutes=max(1, quiet_minutes)), observed_end)
    quiet = [t for t in events if start <= t < quiet_end]
    if not quiet:
        quiet = list(events)
        start = min(events)
        quiet_end = max(events) + timedelta(seconds=1)
    buckets: Dict[datetime, int] = defaultdict(int)
    for t in quiet:
        key = t.replace(second=0, microsecond=0)
        buckets[key] += 1
    span_min = max(1, int(math.ceil((quiet_end - start).total_seconds() / 60.0)))
    counts = [0] * span_min
    base_min = start.replace(second=0, microsecond=0)
    for key, count in buckets.items():
        idx = int((key - base_min).total_seconds() // 60)
        if 0 <= idx < span_min:
            counts[idx] = int(count)
    med_per_min = _median([float(x) for x in counts])
    return med_per_min / 60.0, {
        "method": "quiet_window_minute_median",
        "quiet_minutes": int(quiet_minutes),
        "quiet_event_count": int(len(quiet)),
        "median_count_per_min": float(med_per_min),
    }


def _window_size_sec(lambda0: float) -> float:
    if lambda0 <= EPS:
        return 300.0
    return min(300.0, max(1.0, 30.0 / lambda0))


def _poisson_loglik(n: int, duration_sec: float, lambda0: float) -> float:
    lam = max(lambda0, EPS)
    return float(n) * math.log(lam) - lam * max(duration_sec, EPS)


def _hawkes_loglik(times: List[float], duration_sec: float, eta: float, beta: float) -> FitResult:
    n = len(times)
    if n == 0:
        return FitResult(loglik=0.0, mu=EPS, beta=beta, eta=eta, half_life_sec=math.log(2.0) / beta)

    tail_sum = sum(1.0 - math.exp(-beta * max(duration_sec - t, 0.0)) for t in times)
    mu = max((n - eta * tail_sum) / max(duration_sec, EPS), EPS)
    alpha = eta * beta
    r = 0.0
    prev = times[0]
    log_sum = 0.0
    for idx, t in enumerate(times):
        if idx == 0:
            r = 0.0
        else:
            r = math.exp(-beta * (t - prev)) * (1.0 + r)
        intensity = max(mu + alpha * r, EPS)
        log_sum += math.log(intensity)
        prev = t
    integral = mu * duration_sec + eta * tail_sum
    return FitResult(
        loglik=log_sum - integral,
        mu=mu,
        beta=beta,
        eta=eta,
        half_life_sec=math.log(2.0) / beta,
    )


def _fit_hawkes_alt(times: List[float], duration_sec: float, half_life_grid: Iterable[float], eta_grid: Iterable[float]) -> FitResult:
    best: Optional[FitResult] = None
    for half_life in half_life_grid:
        hl = max(float(half_life), 0.01)
        beta = math.log(2.0) / hl
        for eta in eta_grid:
            e = min(max(float(eta), 0.0), 0.99)
            fit = _hawkes_loglik(times, duration_sec, e, beta)
            if best is None or fit.loglik > best.loglik:
                best = fit
    assert best is not None
    return best


def _sliding_windows(events: List[datetime], width_sec: float, step_sec: float) -> Iterable[Tuple[datetime, datetime, List[datetime]]]:
    if not events:
        return
    ordered = sorted(events)
    start = ordered[0]
    end_all = ordered[-1]
    cur = start
    width = timedelta(seconds=width_sec)
    step = timedelta(seconds=max(step_sec, 1.0))
    left = 0
    right = 0
    n = len(ordered)
    while cur <= end_all:
        end = cur + width
        while left < n and ordered[left] < cur:
            left += 1
        if right < left:
            right = left
        while right < n and ordered[right] < end:
            right += 1
        part = ordered[left:right]
        yield cur, end, part
        cur += step


def _evaluate_code(
    code: str,
    events: List[datetime],
    quiet_minutes: int,
    tau: float,
    consecutive_required: int,
    eta_delta_threshold: float,
    eta_high_threshold: float,
    min_baseline_events: int,
    max_windows: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    lambda0, baseline_meta = _estimate_lambda0(events, quiet_minutes)
    width_sec = _window_size_sec(lambda0)
    step_sec = max(1.0, width_sec / 4.0)
    if len(events) < max(1, int(min_baseline_events)) or lambda0 <= EPS:
        return [], {
            "code": code,
            "event_count": int(len(events)),
            "lambda0_per_sec": float(lambda0),
            "lambda0_per_min": float(lambda0 * 60.0),
            "window_sec": float(width_sec),
            "step_sec": float(step_sec),
            "baseline": baseline_meta,
            "max_glr": 0.0,
            "max_eta": 0.0,
            "auto_action_candidate": False,
            "ops_triage_candidate": False,
            "window_count": 0,
            "insufficient_baseline": True,
            "min_baseline_events": int(min_baseline_events),
            "insufficient_baseline_reason": "lambda0_zero" if lambda0 <= EPS else "too_few_events",
        }
    half_life_grid = [0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0]
    eta_grid = [0.0, 0.2, 0.4, 0.6, 0.75, 0.85, 0.9, 0.95]
    rows: List[Dict[str, Any]] = []
    prev_eta: Optional[float] = None
    trigger_run = 0
    window_iter = _sliding_windows(events, width_sec, step_sec)
    total_window_count = 0
    if int(max_windows or 0) > 0:
        recent_windows = deque(maxlen=max(1, int(max_windows)))
        for window in window_iter:
            total_window_count += 1
            recent_windows.append(window)
        windows = list(recent_windows)
    else:
        windows = list(window_iter)
        total_window_count = len(windows)

    for win_start, win_end, part in windows:
        rel = [(t - win_start).total_seconds() for t in part]
        rel = sorted(t for t in rel if 0.0 <= t < width_sec)
        if not rel:
            trigger_run = 0
            rows.append(
                {
                    "code": code,
                    "window_start": win_start.isoformat(timespec="seconds"),
                    "window_end": win_end.isoformat(timespec="seconds"),
                    "event_count": 0,
                    "lambda0_per_sec": lambda0,
                    "window_sec": width_sec,
                    "step_sec": step_sec,
                    "glr": 0.0,
                    "tau": tau,
                    "over_tau": False,
                    "consecutive_over_tau": 0,
                    "eta": 0.0,
                    "eta_delta": 0.0 - (prev_eta if prev_eta is not None else 0.0),
                    "beta": 0.0,
                    "half_life_sec": 0.0,
                    "mu": 0.0,
                    "structure_jump": False,
                    "auto_action_candidate": False,
                    "severity": "OK",
                }
            )
            prev_eta = 0.0
            continue
        null_ll = _poisson_loglik(len(rel), width_sec, lambda0)
        alt = _fit_hawkes_alt(rel, width_sec, half_life_grid, eta_grid)
        glr = max(0.0, alt.loglik - null_ll)
        eta_delta = 0.0 if prev_eta is None else alt.eta - prev_eta
        high_eta = bool(alt.eta >= eta_high_threshold)
        structure_jump = bool(eta_delta >= eta_delta_threshold or high_eta)
        over_tau = bool(glr > tau)
        trigger_run = trigger_run + 1 if over_tau else 0
        auto_action_candidate = bool(trigger_run >= consecutive_required or structure_jump)
        severity = "AUTO_THROTTLE_CANDIDATE" if auto_action_candidate else ("OPS_TRIAGE" if over_tau else "OK")
        rows.append(
            {
                "code": code,
                "window_start": win_start.isoformat(timespec="seconds"),
                "window_end": win_end.isoformat(timespec="seconds"),
                "event_count": int(len(rel)),
                "lambda0_per_sec": round(lambda0, 9),
                "window_sec": round(width_sec, 3),
                "step_sec": round(step_sec, 3),
                "glr": round(glr, 6),
                "tau": round(float(tau), 6),
                "over_tau": over_tau,
                "consecutive_over_tau": int(trigger_run),
                "eta": round(alt.eta, 6),
                "eta_delta": round(eta_delta, 6),
                "beta": round(alt.beta, 6),
                "half_life_sec": round(alt.half_life_sec, 6),
                "mu": round(alt.mu, 9),
                "structure_jump": structure_jump,
                "auto_action_candidate": auto_action_candidate,
                "severity": severity,
            }
        )
        prev_eta = alt.eta

    summary = {
        "code": code,
        "event_count": int(len(events)),
        "lambda0_per_sec": float(lambda0),
        "lambda0_per_min": float(lambda0 * 60.0),
        "window_sec": float(width_sec),
        "step_sec": float(step_sec),
        "baseline": baseline_meta,
        "max_glr": max((float(r["glr"]) for r in rows), default=0.0),
        "max_eta": max((float(r["eta"]) for r in rows), default=0.0),
        "auto_action_candidate": any(bool(r["auto_action_candidate"]) for r in rows),
        "ops_triage_candidate": any(str(r["severity"]) == "OPS_TRIAGE" for r in rows),
        "window_count": int(len(rows)),
        "window_count_total": int(total_window_count),
        "window_cap_applied": bool(int(max_windows or 0) > 0 and total_window_count > len(rows)),
        "max_windows": int(max_windows or 0),
    }
    return rows, summary


def _evaluate_code_task(args: Tuple[str, List[datetime], int, float, int, float, float, int, int]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _evaluate_code(*args)


def _default_workers(code_count: int) -> int:
    raw = str(os.environ.get("ORDERFLOW_GLR_WORKERS", "")).strip()
    if raw:
        try:
            return max(1, min(int(raw), max(1, code_count)))
        except ValueError:
            pass
    cpu = os.cpu_count() or 1
    return max(1, min(4, cpu, max(1, code_count)))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    fields = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _orderflow_evidence_fields(
    *,
    status: str,
    source_fresh: bool,
    input_age_sec: float,
    max_input_age_sec: int,
    trade_event_count: int,
    code_count: int,
    insufficient_baseline_count: int,
) -> Dict[str, Any]:
    unusable_reasons: List[str] = []
    if status == "NO_INPUT":
        unusable_reasons.append("missing kis_ws_ticks jsonl")
    if not source_fresh:
        unusable_reasons.append(f"input_stale:{round(float(input_age_sec), 3)}>{int(max_input_age_sec)}")
    if trade_event_count <= 0:
        unusable_reasons.append("trade_event_count=0")
    if code_count > 0 and insufficient_baseline_count >= code_count:
        unusable_reasons.append("insufficient_baseline_all_codes")
    elif insufficient_baseline_count > 0:
        unusable_reasons.append(f"insufficient_baseline_partial={insufficient_baseline_count}/{code_count}")
    usable = bool(source_fresh and trade_event_count > 0 and not (code_count > 0 and insufficient_baseline_count >= code_count))
    return {
        "orderflow_evidence_usable": usable,
        "orderflow_evidence_reason": "orderflow_evidence_usable" if usable else ";".join(unusable_reasons),
        "evidence_contract": {
            "orderflow_evidence_usable": usable,
            "must_not_use_orderflow_for_entry": not usable,
            "unusable_reasons": unusable_reasons,
        },
    }


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Order-flow Hawkes/Poisson GLR sliding-window observer")
    ap.add_argument("--input-jsonl", default="", help="KIS websocket tick jsonl. Default: latest daily 2_Logs/kis_ws_ticks_YYYYMMDD.jsonl")
    ap.add_argument("--quiet-minutes", type=int, default=60)
    ap.add_argument("--tau", type=float, default=12.0)
    ap.add_argument("--consecutive", type=int, default=3)
    ap.add_argument("--eta-delta-threshold", type=float, default=0.15)
    ap.add_argument("--eta-high-threshold", type=float, default=0.90)
    ap.add_argument("--max-input-age-sec", type=int, default=DEFAULT_MAX_INPUT_AGE_SEC)
    ap.add_argument("--min-baseline-events", type=int, default=30)
    ap.add_argument("--workers", type=int, default=0, help="Parallel code-level workers. Default: env ORDERFLOW_GLR_WORKERS or up to 4.")
    ap.add_argument(
        "--max-windows-per-code",
        type=int,
        default=int(str(os.environ.get("ORDERFLOW_GLR_MAX_WINDOWS_PER_CODE", "720") or "720")),
        help="Evaluate only the latest N windows per code. 0 disables the cap.",
    )
    args = ap.parse_args(argv)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = _now_ts()
    input_path = Path(args.input_jsonl) if str(args.input_jsonl or "").strip() else _latest_tick_jsonl()
    if input_path is None or not input_path.exists():
        evidence = _orderflow_evidence_fields(
            status="NO_INPUT",
            source_fresh=False,
            input_age_sec=0.0,
            max_input_age_sec=int(args.max_input_age_sec),
            trade_event_count=0,
            code_count=0,
            insufficient_baseline_count=0,
        )
        payload = {
            "ts": ts,
            "status": "NO_INPUT",
            "reason": "missing kis_ws_ticks jsonl",
            **evidence,
            "alerts": [],
            "out_csv": str(OUT_CSV),
            "observer_state": str(OBSERVER_JSON),
        }
        OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        OBSERVER_JSON.write_text(json.dumps({**payload, "fail_closed_candidate": False}, ensure_ascii=False, indent=2), encoding="utf-8")
        _write_csv(OUT_CSV, [])
        return 0

    input_age_sec = _file_age_sec(input_path)
    source_fresh = bool(input_age_sec <= max(1, int(args.max_input_age_sec)))
    events, total_rows, malformed_rows = _load_trade_events(input_path)
    by_code: Dict[str, List[datetime]] = defaultdict(list)
    for e in events:
        by_code[e.code].append(e.ts)

    tasks = [
        (
            code,
            sorted(code_events),
            max(1, int(args.quiet_minutes)),
            float(args.tau),
            max(1, int(args.consecutive)),
            float(args.eta_delta_threshold),
            float(args.eta_high_threshold),
            max(1, int(args.min_baseline_events)),
            max(0, int(args.max_windows_per_code)),
        )
        for code, code_events in sorted(by_code.items())
    ]
    worker_count = int(args.workers) if int(args.workers or 0) > 0 else _default_workers(len(tasks))
    all_rows: List[Dict[str, Any]] = []
    summaries: List[Dict[str, Any]] = []
    if worker_count <= 1 or len(tasks) <= 1:
        results = [_evaluate_code_task(task) for task in tasks]
    else:
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            results = list(executor.map(_evaluate_code_task, tasks))
    for rows, summary in results:
        all_rows.extend(rows)
        summaries.append(summary)

    alerts = [
        s for s in summaries if bool(s.get("auto_action_candidate")) or bool(s.get("ops_triage_candidate"))
    ]
    fail_closed_candidate = any(bool(s.get("auto_action_candidate")) for s in summaries)
    insufficient_baseline_count = sum(1 for s in summaries if bool(s.get("insufficient_baseline")))
    if not source_fresh:
        status = "STALE_INPUT"
    elif insufficient_baseline_count > 0 and not alerts:
        status = "INSUFFICIENT_BASELINE"
    elif total_rows > 0 and not events:
        status = "NO_TRADE_FRAMES"
    elif not events:
        status = "NO_TRADE_EVENTS"
    else:
        status = "OK"
    evidence = _orderflow_evidence_fields(
        status=status,
        source_fresh=bool(source_fresh),
        input_age_sec=float(input_age_sec),
        max_input_age_sec=int(args.max_input_age_sec),
        trade_event_count=int(len(events)),
        code_count=int(len(by_code)),
        insufficient_baseline_count=int(insufficient_baseline_count),
    )
    payload = {
        "ts": ts,
        "status": status,
        "model": "hawkes_glr_exponential_kernel_grid_mle",
        "input_jsonl": str(input_path),
        "input_age_sec": round(float(input_age_sec), 3),
        "max_input_age_sec": int(args.max_input_age_sec),
        "source_fresh": bool(source_fresh),
        "input_rows": int(total_rows),
        "malformed_rows": int(malformed_rows),
        "trade_event_count": int(len(events)),
        "code_count": int(len(by_code)),
        "insufficient_baseline_count": int(insufficient_baseline_count),
        "thresholds": {
            "tau": float(args.tau),
            "consecutive": int(args.consecutive),
            "eta_delta_threshold": float(args.eta_delta_threshold),
            "eta_high_threshold": float(args.eta_high_threshold),
            "min_baseline_events": int(args.min_baseline_events),
            "workers": int(worker_count),
            "max_windows_per_code": max(0, int(args.max_windows_per_code)),
        },
        "summaries": summaries,
        "alerts": alerts,
        "fail_closed_candidate": bool(fail_closed_candidate),
        "ops_triage_candidate": bool(alerts and not fail_closed_candidate),
        **evidence,
        "out_csv": str(OUT_CSV),
        "observer_state": str(OBSERVER_JSON),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    observer = {
        "ts": ts,
        "status": status,
        "source": str(OUT_JSON),
        "fail_closed_candidate": bool(fail_closed_candidate),
        "ops_triage_candidate": bool(alerts and not fail_closed_candidate),
        "reason": (
            "input_stale"
            if status == "STALE_INPUT"
            else "insufficient_baseline"
            if status == "INSUFFICIENT_BASELINE"
            else "no_trade_frames"
            if status == "NO_TRADE_FRAMES"
            else "persistent_glr_or_eta_structure_jump"
            if fail_closed_candidate
            else ("glr_triage_only" if alerts else "no_orderflow_anomaly")
        ),
        "input_jsonl": str(input_path),
        "input_age_sec": round(float(input_age_sec), 3),
        "source_fresh": bool(source_fresh),
        "trade_event_count": int(len(events)),
        "alert_count": int(len(alerts)),
        "alerts": alerts[:20],
        **evidence,
    }
    OBSERVER_JSON.write_text(json.dumps(observer, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, all_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
