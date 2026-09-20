from __future__ import annotations

import csv
import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Tuple


ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = Path(os.getenv("FILLS_IDEMPOTENCY_STATE", str(ROOT / "state" / "fills_idempotency.json")))
LOCK_PATH = STATE_PATH.with_suffix(STATE_PATH.suffix + ".lock")
REPORT_PATH = ROOT / "2_Logs" / "fills_idempotency_latest.json"


class IdempotencyConflict(RuntimeError):
    pass


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _text(v: Any) -> str:
    return str(v if v is not None else "").strip()


def _norm_code(v: Any) -> str:
    s = _text(v).replace(".0", "")
    return s.zfill(6) if s else ""


def _norm_num(v: Any) -> str:
    s = _text(v).replace(",", "")
    try:
        f = float(s)
    except Exception:
        return s
    if f.is_integer():
        return str(int(f))
    return f"{f:.8f}".rstrip("0").rstrip(".")


def _norm_dt(v: Any) -> str:
    return _text(v).replace(" ", "T")


def _row_key(row: Mapping[str, Any]) -> str:
    fill_id = _text(row.get("fill_id"))
    seq = _text(row.get("seq") or row.get("event_seq") or row.get("fill_seq") or row.get("order_no"))
    if fill_id:
        return f"fill_id:{fill_id}:seq:{seq or '0'}"

    order_id = _text(row.get("order_id") or row.get("order_no"))
    if order_id:
        return f"order:{order_id}"

    parts = [
        _norm_dt(row.get("datetime") or row.get("ts") or row.get("date")),
        _norm_code(row.get("code")),
        _text(row.get("side")).upper(),
        _norm_num(row.get("qty") or row.get("fill_qty")),
        _norm_num(row.get("price") or row.get("fill_price")),
    ]
    return "fallback:" + "|".join(parts)


def _row_fingerprint(row: Mapping[str, Any]) -> str:
    comparable = {
        "datetime": _norm_dt(row.get("datetime") or row.get("ts") or row.get("date")),
        "date": _text(row.get("date")),
        "code": _norm_code(row.get("code")),
        "side": _text(row.get("side")).upper(),
        "qty": _norm_num(row.get("qty") or row.get("fill_qty")),
        "price": _norm_num(row.get("price") or row.get("fill_price")),
        "order_id": _text(row.get("order_id") or row.get("order_no")),
        "fill_id": _text(row.get("fill_id")),
        "seq": _text(row.get("seq") or row.get("event_seq") or row.get("fill_seq")),
    }
    raw = json.dumps(comparable, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _default_state() -> Dict[str, Any]:
    return {
        "version": 1,
        "processed_fill_ids": [],
        "fill_fingerprints": {},
        "last_applied_seq": 0,
        "dup_attempts": 0,
        "updated_at": "",
    }


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return _default_state()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"idempotency_state_load_failed:{path}:{type(exc).__name__}:{exc}") from exc
    state = _default_state()
    if isinstance(data, dict):
        state.update(data)
    if not isinstance(state.get("processed_fill_ids"), list):
        state["processed_fill_ids"] = []
    if not isinstance(state.get("fill_fingerprints"), dict):
        state["fill_fingerprints"] = {}
    state["last_applied_seq"] = int(float(state.get("last_applied_seq") or 0))
    state["dup_attempts"] = int(float(state.get("dup_attempts") or 0))
    return state


def _write_state_atomic(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    payload = json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True)
    tmp.write_text(payload, encoding="utf-8")
    os.replace(str(tmp), str(path))


def _write_report(report: Mapping[str, Any]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = REPORT_PATH.with_suffix(REPORT_PATH.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(str(tmp), str(REPORT_PATH))


class _StateLock:
    def __init__(self, path: Path, timeout_sec: float = 10.0) -> None:
        self.path = path
        self.timeout_sec = timeout_sec
        self.fd: int | None = None

    def __enter__(self) -> "_StateLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.time() + self.timeout_sec
        payload = f"pid={os.getpid()} created_at={_now()}\n".encode("utf-8")
        while True:
            try:
                self.fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self.fd, payload)
                return self
            except FileExistsError:
                if time.time() >= deadline:
                    raise RuntimeError(f"idempotency_state_lock_timeout:{self.path}")
                time.sleep(0.1)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.fd is not None:
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = None
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


def _mapping_from_row(header: List[str], row: Iterable[Any]) -> Dict[str, Any]:
    return {str(col): value for col, value in zip(header, row)}


def filter_new_fill_rows(
    *,
    header: List[str],
    existing_rows: Iterable[Mapping[str, Any]],
    new_rows: List[List[Any]],
    source: str,
    state_path: Path = STATE_PATH,
) -> Tuple[List[List[Any]], Dict[str, Any]]:
    with _StateLock(state_path.with_suffix(state_path.suffix + ".lock")):
        state = _load_state(state_path)
        processed = set(str(x) for x in state.get("processed_fill_ids", []))
        fingerprints: Dict[str, str] = {str(k): str(v) for k, v in dict(state.get("fill_fingerprints", {})).items()}

        seeded = 0
        for row in existing_rows:
            key = _row_key(row)
            if not key or key == "fallback:||||":
                continue
            fp = _row_fingerprint(row)
            old_fp = fingerprints.get(key)
            if old_fp and old_fp != fp:
                raise IdempotencyConflict(f"idempotency_existing_conflict key={key}")
            if key not in processed:
                processed.add(key)
                seeded += 1
            fingerprints[key] = fp

        accepted: List[List[Any]] = []
        duplicate_keys: List[str] = []
        accepted_keys: List[str] = []
        for raw in new_rows:
            row = _mapping_from_row(header, raw)
            key = _row_key(row)
            fp = _row_fingerprint(row)
            old_fp = fingerprints.get(key)
            if key in processed:
                if old_fp and old_fp != fp:
                    raise IdempotencyConflict(f"idempotency_new_conflict key={key}")
                duplicate_keys.append(key)
                continue
            processed.add(key)
            fingerprints[key] = fp
            accepted.append(raw)
            accepted_keys.append(key)

        state["processed_fill_ids"] = sorted(processed)
        state["fill_fingerprints"] = {k: fingerprints[k] for k in sorted(fingerprints)}
        state["last_applied_seq"] = int(state.get("last_applied_seq") or 0) + len(accepted)
        state["dup_attempts"] = int(state.get("dup_attempts") or 0) + len(duplicate_keys)
        state["updated_at"] = _now()
        _write_state_atomic(state_path, state)

        report = {
            "generated_at": _now(),
            "source": str(source),
            "state_path": str(state_path),
            "processed_count": len(state["processed_fill_ids"]),
            "seeded_from_existing": int(seeded),
            "input_new_rows": int(len(new_rows)),
            "accepted_rows": int(len(accepted)),
            "duplicate_rows": int(len(duplicate_keys)),
            "duplicate_keys": duplicate_keys[:50],
            "accepted_keys": accepted_keys[:50],
            "last_applied_seq": int(state["last_applied_seq"]),
            "dup_attempts": int(state["dup_attempts"]),
        }
        _write_report(report)
        return accepted, report


def rebuild_state_from_existing_rows(
    *,
    existing_rows: Iterable[Mapping[str, Any]],
    source: str,
    state_path: Path = STATE_PATH,
) -> Dict[str, Any]:
    with _StateLock(state_path.with_suffix(state_path.suffix + ".lock")):
        processed: set[str] = set()
        fingerprints: Dict[str, str] = {}
        conflicts: List[str] = []
        for row in existing_rows:
            key = _row_key(row)
            if not key or key == "fallback:||||":
                continue
            fp = _row_fingerprint(row)
            old_fp = fingerprints.get(key)
            if old_fp and old_fp != fp:
                conflicts.append(key)
                continue
            processed.add(key)
            fingerprints[key] = fp
        if conflicts:
            raise IdempotencyConflict(f"idempotency_rebuild_conflicts keys={conflicts[:10]}")

        state = _default_state()
        state["processed_fill_ids"] = sorted(processed)
        state["fill_fingerprints"] = {k: fingerprints[k] for k in sorted(fingerprints)}
        state["last_applied_seq"] = len(processed)
        state["updated_at"] = _now()
        _write_state_atomic(state_path, state)

        report = {
            "generated_at": _now(),
            "source": str(source),
            "state_path": str(state_path),
            "processed_count": len(state["processed_fill_ids"]),
            "rebuild_from_existing": True,
            "conflicts": [],
            "last_applied_seq": int(state["last_applied_seq"]),
            "dup_attempts": int(state["dup_attempts"]),
        }
        _write_report(report)
        return report


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))
