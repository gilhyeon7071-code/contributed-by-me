"""Unified gate audit logger.

Append-only JSONL for cross-layer gate decisions (stable, candidate,
active_response, dispatch, broker).  Multiple processes are synchronized with
filelock.  Existing artifacts are not modified; this module only adds new log
files under E:/1_Data/2_Logs.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union

try:
    from filelock import FileLock
except Exception:  # pragma: no cover
    FileLock = None  # type: ignore

DEFAULT_BASE_DIR = Path("E:/1_Data/2_Logs")


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")


def _resolve_path(path: Union[str, Path, None], date: Optional[str] = None) -> Path:
    if path is None:
        d = date or datetime.now().strftime("%Y%m%d")
        path = DEFAULT_BASE_DIR / f"gate_audit_{d}.jsonl"
    else:
        path = Path(path)
        if date:
            path = Path(str(path).replace("{date}", date).replace("{{date}}", date))
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _safe_json_value(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (list, tuple)):
        return [_safe_json_value(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _safe_json_value(vv) for k, vv in v.items()}
    return str(v)


def log_gate_event(
    path: Union[str, Path, None] = None,
    *,
    code: Optional[str] = None,
    gate_layer: str,
    gate_name: str,
    decision: str,
    reason_code: str = "",
    reason_detail: str = "",
    source_file: str = "",
    source_key: str = "",
    threshold: Optional[float] = None,
    actual_value: Optional[float] = None,
    recoverable: Optional[bool] = None,
    recovery_hint: str = "",
    date: Optional[str] = None,
    ts: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Path:
    """Append one gate audit record to a JSONL file.

    Parameters
    ----------
    path: target JSONL path.  None -> E:/1_Data/2_Logs/gate_audit_YYYYMMDD.jsonl
    gate_layer: 'stable' | 'candidate' | 'active_response' | 'dispatch' | 'broker'
    gate_name: specific gate/check name
    decision: 'PASS' | 'BLOCK' | 'SKIP' | 'RELAX' | 'DRY_RUN' | 'FAIL' | ...
    reason_code: stable short code, e.g. 'oos_pf_low'
    reason_detail: human-readable detail with values
    source_file: originating source file path/identifier
    source_key: config key or function name
    threshold: gate threshold if numeric
    actual_value: measured value that was compared to threshold
    recoverable: whether the block can be cleared by parameter/config change
    recovery_hint: short actionable hint
    date: trading date (YYYYMMDD).  Used for filename fallback and stored as 'date'
    ts: event timestamp ISO.  Defaults to now.
    extra: arbitrary additional fields
    """
    record: Dict[str, Any] = {
        "ts": ts or _now_iso(),
        "date": date or datetime.now().strftime("%Y%m%d"),
        "gate_layer": gate_layer,
        "gate_name": gate_name,
        "decision": decision,
    }
    if code:
        record["code"] = str(code).zfill(6)
    if reason_code:
        record["reason_code"] = reason_code
    if reason_detail:
        record["reason_detail"] = reason_detail
    if source_file:
        record["source_file"] = source_file
    if source_key:
        record["source_key"] = source_key
    if threshold is not None:
        record["threshold"] = float(threshold)
    if actual_value is not None:
        record["actual_value"] = float(actual_value)
    if recoverable is not None:
        record["recoverable"] = bool(recoverable)
    if recovery_hint:
        record["recovery_hint"] = recovery_hint
    if extra:
        record["extra"] = _safe_json_value(extra)

    out_path = _resolve_path(path, date=record["date"])
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"

    lock_path = out_path.with_suffix(out_path.suffix + ".lock")
    if FileLock is not None:
        with FileLock(str(lock_path), timeout=5):
            with open(out_path, "a", encoding="utf-8") as f:
                f.write(line)
    else:
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(line)
    return out_path


def log_candidate_filter_event(
    path: Union[str, Path, None] = None,
    *,
    code: str,
    filter_name: str,
    decision: str,
    reason_detail: str = "",
    threshold: Optional[float] = None,
    actual_value: Optional[float] = None,
    date: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Path:
    """Convenience wrapper for candidate-generation hard-filter events."""
    return log_gate_event(
        path=path,
        code=code,
        gate_layer="candidate",
        gate_name=filter_name,
        decision=decision,
        reason_detail=reason_detail,
        threshold=threshold,
        actual_value=actual_value,
        source_file="generate_candidates_v41_1.py",
        recoverable=True,
        recovery_hint="파라미터 완화 또는 필터 정의 재검토",
        date=date,
        extra=extra,
    )


def log_active_response_event(
    path: Union[str, Path, None] = None,
    *,
    code: str,
    gate_name: str,
    decision: str,
    reason_code: str = "",
    reason_detail: str = "",
    threshold: Optional[float] = None,
    actual_value: Optional[float] = None,
    date: Optional[str] = None,
    ts: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Path:
    """Convenience wrapper for surge_detector_realtime events."""
    return log_gate_event(
        path=path,
        code=code,
        gate_layer="active_response",
        gate_name=gate_name,
        decision=decision,
        reason_code=reason_code,
        reason_detail=reason_detail,
        source_file="surge_detector_realtime.py",
        threshold=threshold,
        actual_value=actual_value,
        recoverable=True,
        recovery_hint="실시간 entry policy 파라미터 조정",
        date=date,
        ts=ts,
        extra=extra,
    )
