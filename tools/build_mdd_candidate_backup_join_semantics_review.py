from __future__ import annotations

import csv
import glob
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

MISSING_CSV = LOG_DIR / "paper_mdd_candidate_missing_split_latest.csv"
OUT_JSON = LOG_DIR / "mdd_candidate_backup_join_semantics_latest.json"
OUT_CSV = LOG_DIR / "mdd_candidate_backup_join_semantics_latest.csv"

TARGET_ROOT_CAUSE = "LATEST_BACKUP_SELECTION_GAP"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(str(value).replace(",", ""))
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _file_stamp(path: Path) -> str:
    m = re.search(r"bak_(\d{8})_(\d{6})\.csv$", path.name)
    return "" if not m else m.group(1) + m.group(2)


def _entry_stamp(value: Any, signal_date: str) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) >= 14:
        return digits[:14]
    if len(digits) >= 6:
        return signal_date + digits[:6]
    return signal_date + "000000"


def _quality_bucket(cand: dict[str, str]) -> str:
    if not cand:
        return "NO_CANDIDATE_ROW"
    origin = str(cand.get("candidate_origin") or "").strip().upper()
    execution_pool = _truthy(cand.get("execution_pool"))
    natural_pass = _truthy(cand.get("natural_pass"))
    relax_level = str(cand.get("relax_level") or "").strip().upper()
    junk_grade = str(cand.get("junk_risk_grade") or "").strip().upper()
    junk_flags = str(cand.get("junk_flags") or "").strip()
    final_score = _f(cand.get("final_score"), -1.0)
    if origin == "SECTOR_PREFILTER_UNION" and not execution_pool:
        return "SECTOR_UNION_NOT_EXECUTION_POOL"
    if relax_level == "L3" and junk_flags:
        return "L3_WITH_RISK_FLAGS"
    if relax_level == "L3":
        return "L3_RELAXED"
    if not natural_pass:
        return "NON_NATURAL_PASS"
    if junk_grade in {"MID", "WARN", "HIGH"} or junk_flags:
        return "RISK_FLAGGED_NATURAL_PASS"
    if 0 <= final_score < 70:
        return "LOW_FINAL_SCORE"
    return "CLEAN_NATURAL_PASS"


def _candidate_paths(signal_date: str) -> list[Path]:
    return sorted(Path(p) for p in glob.glob(str(LOG_DIR / f"candidates_latest_data.bak_{signal_date}_*.csv")))


def _hits(signal_date: str, code: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in _candidate_paths(signal_date):
        for row in _read_csv(path):
            if str(row.get("code") or "").zfill(6) != code:
                continue
            out.append({"path": path, "stamp": _file_stamp(path), "row": row})
            break
    return out


def _pick_closest(hits: list[dict[str, Any]], entry_stamp: str, before_only: bool) -> dict[str, Any] | None:
    candidates = [h for h in hits if h.get("stamp")]
    if before_only:
        candidates = [h for h in candidates if str(h["stamp"]) <= entry_stamp]
    if not candidates:
        return None
    return min(candidates, key=lambda h: abs(int(str(h["stamp"])) - int(entry_stamp)))


def _row_payload(base: dict[str, str], pick: dict[str, Any] | None, prefix: str) -> dict[str, Any]:
    row = pick.get("row", {}) if pick else {}
    path = pick.get("path") if pick else None
    return {
        f"{prefix}_file": path.name if isinstance(path, Path) else "",
        f"{prefix}_stamp": pick.get("stamp", "") if pick else "",
        f"{prefix}_quality_bucket": _quality_bucket(row),
        f"{prefix}_candidate_origin": row.get("candidate_origin", ""),
        f"{prefix}_execution_pool": row.get("execution_pool", ""),
        f"{prefix}_natural_pass": row.get("natural_pass", ""),
        f"{prefix}_relax_level": row.get("relax_level", ""),
        f"{prefix}_final_score": row.get("final_score", ""),
        f"{prefix}_junk_risk_grade": row.get("junk_risk_grade", ""),
        f"{prefix}_junk_flags": row.get("junk_flags", ""),
    }


def build() -> dict[str, Any]:
    source_rows = [row for row in _read_csv(MISSING_CSV) if row.get("missing_root_cause") == TARGET_ROOT_CAUSE]
    out_rows: list[dict[str, Any]] = []
    for row in source_rows:
        signal_date = str(row.get("signal_date") or "")
        code = str(row.get("code") or "").zfill(6)
        entry_stamp = _entry_stamp(row.get("entry_ts"), signal_date)
        hit_rows = _hits(signal_date, code)
        earliest = hit_rows[0] if hit_rows else None
        latest_hit = hit_rows[-1] if hit_rows else None
        closest_any = _pick_closest(hit_rows, entry_stamp, before_only=False)
        closest_before = _pick_closest(hit_rows, entry_stamp, before_only=True)
        file_count = len(_candidate_paths(signal_date))
        latest_file = _candidate_paths(signal_date)[-1].name if file_count else ""
        selected = closest_before or closest_any or latest_hit or earliest
        selected_mode = "closest_before" if closest_before else ("closest_any" if closest_any else ("latest_hit" if latest_hit else ("earliest" if earliest else "none")))
        selected_quality = _quality_bucket(selected.get("row", {}) if selected else {})
        out = {
            "trade_id": row.get("trade_id", ""),
            "code": code,
            "signal_date": signal_date,
            "entry_ts": row.get("entry_ts", ""),
            "entry_stamp": entry_stamp,
            "exit_reason": row.get("exit_reason", ""),
            "net_ret": row.get("net_ret", ""),
            "same_date_file_count": file_count,
            "hit_file_count": len(hit_rows),
            "latest_file": latest_file,
            "latest_file_has_code": bool(hit_rows and latest_file == hit_rows[-1]["path"].name),
            "selected_join_mode": selected_mode,
            "selected_quality_bucket": selected_quality,
        }
        for prefix, pick in [
            ("earliest_hit", earliest),
            ("latest_hit", latest_hit),
            ("closest_any", closest_any),
            ("closest_before", closest_before),
        ]:
            out.update(_row_payload(row, pick, prefix))
        out_rows.append(out)

    selected_counts = Counter(row.get("selected_quality_bucket", "") for row in out_rows)
    mode_counts = Counter(row.get("selected_join_mode", "") for row in out_rows)
    code_counts = Counter(row.get("code", "") for row in out_rows)
    date_counts = Counter(row.get("signal_date", "") for row in out_rows)
    net_sum = sum(_f(row.get("net_ret")) for row in out_rows)
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL" if out_rows else "PASS",
        "policy_change": False,
        "trading_effect": False,
        "source": {"missing_split_csv": str(MISSING_CSV)},
        "scope": {
            "target_root_cause": TARGET_ROOT_CAUSE,
            "rows": len(out_rows),
            "sum_net_ret": round(net_sum, 6),
            "codes": dict(sorted(code_counts.items())),
            "signal_dates": dict(sorted(date_counts.items())),
        },
        "breakdown": {
            "selected_join_mode_counts": dict(sorted(mode_counts.items())),
            "selected_quality_bucket_counts": dict(sorted(selected_counts.items())),
        },
        "artifacts": {"csv": str(OUT_CSV)},
        "interpretation": [
            "This is a read-only review of latest-only candidate backup join semantics for MDD diagnostics.",
            "closest_before means the same-date backup at or before entry_ts that still contained the code.",
            "If closest_before recovers rows, the diagnostic should not treat latest-only absence as proof the entry lacked candidate context.",
            "No trading policy value is changed by this diagnostic.",
        ],
    }
    fields = [
        "trade_id", "code", "signal_date", "entry_ts", "entry_stamp", "exit_reason", "net_ret",
        "same_date_file_count", "hit_file_count", "latest_file", "latest_file_has_code",
        "selected_join_mode", "selected_quality_bucket",
        "earliest_hit_file", "earliest_hit_stamp", "earliest_hit_quality_bucket", "earliest_hit_candidate_origin", "earliest_hit_execution_pool", "earliest_hit_natural_pass", "earliest_hit_relax_level", "earliest_hit_final_score", "earliest_hit_junk_risk_grade", "earliest_hit_junk_flags",
        "latest_hit_file", "latest_hit_stamp", "latest_hit_quality_bucket", "latest_hit_candidate_origin", "latest_hit_execution_pool", "latest_hit_natural_pass", "latest_hit_relax_level", "latest_hit_final_score", "latest_hit_junk_risk_grade", "latest_hit_junk_flags",
        "closest_any_file", "closest_any_stamp", "closest_any_quality_bucket", "closest_any_candidate_origin", "closest_any_execution_pool", "closest_any_natural_pass", "closest_any_relax_level", "closest_any_final_score", "closest_any_junk_risk_grade", "closest_any_junk_flags",
        "closest_before_file", "closest_before_stamp", "closest_before_quality_bucket", "closest_before_candidate_origin", "closest_before_execution_pool", "closest_before_natural_pass", "closest_before_relax_level", "closest_before_final_score", "closest_before_junk_risk_grade", "closest_before_junk_flags",
    ]
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, out_rows, fields)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps({
        "status": payload["status"],
        "scope": payload["scope"],
        "breakdown": payload["breakdown"],
        "artifacts": payload["artifacts"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
