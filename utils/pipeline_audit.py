"""Pipeline audit logger.

Append-only JSONL for core pipeline stage start/end events.  Multiple processes
are synchronized with filelock.  Existing artifacts are not modified; this
module only adds new log files under E:/1_Data/2_Logs.
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
    FileLock = None

DEFAULT_BASE_DIR = Path("E:/1_Data/2_Logs")


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")


def _resolve_path(path: Union[str, Path, None], date: Optional[str] = None) -> Path:
    if path is None:
        d = date or datetime.now().strftime("%Y%m%d")
        path = DEFAULT_BASE_DIR / f"pipeline_audit_{d}.jsonl"
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


def count_rows(path: Union[str, Path]) -> Optional[int]:
    """Return row count for .csv/.jsonl/.xlsx, or None if file missing.

    - .csv / .jsonl : number of lines (excluding header for csv).
    - .xlsx         : openpyxl read_only max_row.
    """
    p = Path(path)
    if not p.exists():
        return None
    suffix = p.suffix.lower()
    if suffix == ".csv":
        try:
            with open(p, "r", encoding="utf-8-sig", errors="replace") as f:
                total = sum(1 for _ in f)
            return max(0, total - 1)
        except Exception:
            return None
    if suffix == ".jsonl":
        try:
            with open(p, "r", encoding="utf-8", errors="replace") as f:
                return sum(1 for line in f if line.strip())
        except Exception:
            return None
    if suffix in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        try:
            from openpyxl import load_workbook

            wb = load_workbook(p, read_only=True, data_only=True)
            ws = wb.active
            max_row = ws.max_row
            wb.close()
            return max(0, max_row - 1)
        except Exception:
            return None
    return None


def log_pipeline_event(
    path: Union[str, Path, None] = None,
    *,
    stage: str,
    batch_label: str,
    event: str,
    date: Optional[str] = None,
    ts: Optional[str] = None,
    input_files: Optional[Dict[str, Any]] = None,
    output_files: Optional[Dict[str, Any]] = None,
    metrics: Optional[Dict[str, Any]] = None,
    status: str = "",
) -> Path:
    """Append one pipeline stage event record to a JSONL file.

    Parameters
    ----------
    path: target JSONL path.  None -> E:/1_Data/2_Logs/pipeline_audit_YYYYMMDD.jsonl
    stage: pipeline stage name, e.g. 'generate_candidates'
    batch_label: human-readable pipeline position, e.g. '[6.25/9]'
    event: 'START' | 'END' | other milestone
    date: trading date (YYYYMMDD).  Used for filename fallback and stored as 'date'
    ts: event timestamp ISO.  Defaults to now.
    input_files: mapping of logical name -> path or path+row_count dict
    output_files: mapping of logical name -> path or path+row_count dict
    metrics: arbitrary stage metrics dict
    status: optional status string, e.g. 'PASS' / 'FAIL' / 'SOFT_FAIL'
    """
    record: Dict[str, Any] = {
        "ts": ts or _now_iso(),
        "date": date or datetime.now().strftime("%Y%m%d"),
        "stage": stage,
        "batch_label": batch_label,
        "event": event,
    }
    if input_files is not None:
        record["input_files"] = _safe_json_value(input_files)
    if output_files is not None:
        record["output_files"] = _safe_json_value(output_files)
    if metrics is not None:
        record["metrics"] = _safe_json_value(metrics)
    if status:
        record["status"] = status

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


if __name__ == "__main__":
    # quick smoke test
    print("pipeline_audit smoke test")
    out = log_pipeline_event(
        stage="test",
        batch_label="[test/0]",
        event="START",
        metrics={"ok": True},
    )
    print(f"wrote to {out}")
