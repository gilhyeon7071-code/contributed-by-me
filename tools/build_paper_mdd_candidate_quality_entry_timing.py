from __future__ import annotations

import csv
import glob
import json
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"

MDD_JSON = LOG_DIR / "paper_mdd_cluster_axis_diagnosis_latest.json"
TRADES_CALC = PAPER_DIR / "trades_calc.csv"
FILLS_NORM = PAPER_DIR / "fills_norm.csv"
ORDERS_EXEC_DIR = PAPER_DIR

OUT_JSON = LOG_DIR / "paper_mdd_candidate_quality_entry_timing_latest.json"
OUT_CSV = LOG_DIR / "paper_mdd_candidate_quality_entry_timing_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "paper_mdd_candidate_quality_entry_timing_summary_latest.csv"
OUT_MISSING_CSV = LOG_DIR / "paper_mdd_candidate_missing_split_latest.csv"
ENTRY_SOURCE_ARCHIVE = LOG_DIR / "entry_source_archive_history.csv"

FOCUS_EXITS = {"STOP", "STOP_GAP", "DDM_LIQUIDATE_L3", "DDM_LIQUIDATE_L4"}


def _read_json(path: Path) -> dict[str, Any]:
    for enc in ("utf-8", "utf-8-sig", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except UnicodeDecodeError:
            continue
    return {}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    with path.open("r", newline="") as f:
        return list(csv.DictReader(f))


def _official_filtered_trades() -> tuple[list[dict[str, str]], dict[str, Any]]:
    # Reuse the same official scope chain as paper_pnl_report diagnostics so
    # this read-only split stays aligned with paper_pnl_summary_last.
    sys.path.insert(0, str(ROOT))
    import pandas as pd  # type: ignore
    import paper_pnl_report as pnl  # type: ignore

    raw = pd.read_csv(TRADES_CALC, dtype=str).fillna("")
    scoped, op_scope = pnl._apply_operational_scope(raw)
    blocked_filtered, blocked_meta = pnl._filter_blocked_entry_trades(scoped, ORDERS_EXEC_DIR)
    final, unauditable_meta = pnl._filter_unauditable_surge_trades(blocked_filtered)
    final = pnl._ensure_exit_date(final)
    meta = {
        "raw": int(len(raw)),
        "operational_scope": int(len(scoped)),
        "after_blocked_entry_filter": int(len(blocked_filtered)),
        "final_used": int(len(final)),
        "blocked_removed": int(blocked_meta.get("removed_rows", 0) or 0),
        "unauditable_surge_removed": int(unauditable_meta.get("removed_rows", 0) or 0),
        "operational_scope_meta": op_scope,
        "blocked_entry_filter": blocked_meta,
        "unauditable_surge_filter": unauditable_meta,
    }
    return final.astype(str).to_dict(orient="records"), meta


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _note_value(note: Any, key: str) -> str:
    text = str(note or "").replace(" | ", ";")
    m = re.search(rf"(?:^|[;|])\s*{re.escape(key)}=([^;|]*)", text)
    return m.group(1).strip() if m else ""


def _ymd(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def _float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if not text:
            return default
        val = float(text)
        if math.isnan(val) or math.isinf(val):
            return default
        return val
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        text = str(value or "").strip()
        if not text:
            return default
        return int(float(text))
    except Exception:
        return default


def _bool_text(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _latest_candidate_backup(ymd: str) -> Path | None:
    paths = sorted(glob.glob(str(LOG_DIR / f"candidates_latest_data.bak_{ymd}_*.csv")))
    return Path(paths[-1]) if paths else None


def _candidate_backup_stamp(path: Path) -> str:
    m = re.search(r"bak_(\d{8})_(\d{6})\.csv$", path.name)
    return "" if not m else m.group(1) + m.group(2)


def _entry_stamp(entry_ts: Any, signal_date: str) -> str:
    digits = re.sub(r"\D", "", str(entry_ts or ""))
    if len(digits) >= 14:
        return digits[:14]
    if len(digits) >= 6:
        return signal_date + digits[:6]
    return signal_date + "000000"


def _candidate_cache(dates: set[str]) -> dict[str, list[tuple[Path, str, dict[str, dict[str, str]]]]]:
    out: dict[str, list[tuple[Path, str, dict[str, dict[str, str]]]]] = {}
    for ymd in sorted(d for d in dates if d):
        paths = sorted(Path(p) for p in glob.glob(str(LOG_DIR / f"candidates_latest_data.bak_{ymd}_*.csv")))
        out[ymd] = []
        for path in paths:
            rows = _read_csv(path)
            out[ymd].append((path, _candidate_backup_stamp(path), {str(row.get("code") or "").zfill(6): row for row in rows}))
    return out


def _select_candidate_for_entry(
    cached: list[tuple[Path, str, dict[str, dict[str, str]]]],
    code: str,
    entry_ts: Any,
    signal_date: str,
) -> tuple[Path | None, dict[str, str], str]:
    hits = [(path, stamp, rows[code]) for path, stamp, rows in cached if code in rows]
    if not hits:
        return None, {}, "no_same_date_hit"
    target = _entry_stamp(entry_ts, signal_date)
    before = [item for item in hits if item[1] and item[1] <= target]
    pool = before if before else hits
    selected = min(pool, key=lambda item: abs(int(item[1] or "0") - int(target)))
    mode = "closest_before" if before else "closest_any"
    return selected[0], selected[2], mode


def _candidate_presence_by_date(dates: set[str], codes: set[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for ymd in sorted(d for d in dates if d):
        paths = sorted(Path(p) for p in glob.glob(str(LOG_DIR / f"candidates_latest_data.bak_{ymd}_*.csv")))
        file_hits: dict[str, list[str]] = {}
        for path in paths:
            hits: list[str] = []
            for row in _read_csv(path):
                code = str(row.get("code") or "").zfill(6)
                if code in codes:
                    hits.append(code)
            if hits:
                file_hits[path.name] = sorted(set(hits))
        latest = paths[-1].name if paths else ""
        out[ymd] = {
            "file_count": len(paths),
            "latest_file": latest,
            "file_hits": file_hits,
        }
    return out


def _candidate_gap_class(row: dict[str, Any], presence: dict[str, dict[str, Any]]) -> str:
    ymd = str(row.get("signal_date") or "")
    code = str(row.get("code") or "").zfill(6)
    info = presence.get(ymd, {})
    file_hits = info.get("file_hits") if isinstance(info.get("file_hits"), dict) else {}
    latest = str(info.get("latest_file") or "")
    if not info or int(info.get("file_count") or 0) <= 0:
        return "NO_CANDIDATE_FILE_FOR_SIGNAL_DATE"
    if latest and code in set(file_hits.get(latest, [])):
        return "LATEST_MATCH_SHOULD_HAVE_HIT"
    if any(code in set(codes) for codes in file_hits.values()):
        return "CODE_IN_OLDER_SAME_DATE_BACKUP_NOT_LATEST"
    return "CODE_NOT_FOUND_IN_SAME_DATE_BACKUPS"


def _archive_by_entry_order_id() -> dict[str, dict[str, str]]:
    return {
        str(row.get("entry_order_id") or ""): row
        for row in _read_csv(ENTRY_SOURCE_ARCHIVE)
        if str(row.get("entry_order_id") or "")
    }


def _entry_source_bucket(row: dict[str, Any]) -> str:
    archive_kind = str(row.get("archive_entry_source_kind") or "").strip().upper()
    if archive_kind:
        return f"ARCHIVED_{archive_kind}"
    if str(row.get("surge_flag_bucket") or "").startswith("surge_immediate_1"):
        return "SURGE_IMMEDIATE_METADATA"
    if str(row.get("entry_timing") or "") == "intraday_realtime":
        return "INTRADAY_REALTIME_NO_SURGE_TYPE"
    return "UNKNOWN_ENTRY_SOURCE"


def _missing_root_cause(row: dict[str, Any]) -> str:
    gap = str(row.get("candidate_gap_class") or "")
    source = str(row.get("entry_source_bucket") or "")
    if source.startswith("ARCHIVED_"):
        return "ENTRY_SOURCE_ARCHIVE_AVAILABLE_NO_CANDIDATE_MATCH"
    if gap == "CODE_IN_OLDER_SAME_DATE_BACKUP_NOT_LATEST":
        return "LATEST_BACKUP_SELECTION_GAP"
    if gap == "CODE_NOT_FOUND_IN_SAME_DATE_BACKUPS" and source == "SURGE_IMMEDIATE_METADATA":
        return "SURGE_RUNTIME_ONLY_NO_CANDIDATE_ARCHIVE"
    if gap == "CODE_NOT_FOUND_IN_SAME_DATE_BACKUPS" and source == "INTRADAY_REALTIME_NO_SURGE_TYPE":
        return "INTRADAY_REALTIME_FALLBACK_OR_LINEAGE_GAP"
    if gap == "NO_CANDIDATE_FILE_FOR_SIGNAL_DATE":
        return "MISSING_SIGNAL_DATE_CANDIDATE_ARCHIVE"
    return "UNCLASSIFIED_CANDIDATE_LINEAGE_GAP"


def _quality_bucket(cand: dict[str, str]) -> str:
    if not cand:
        return "NO_CANDIDATE_MATCH"
    origin = str(cand.get("candidate_origin") or "").strip().upper()
    execution_pool = _bool_text(cand.get("execution_pool"))
    natural_pass = _bool_text(cand.get("natural_pass"))
    relax_level = str(cand.get("relax_level") or "").strip().upper()
    junk_grade = str(cand.get("junk_risk_grade") or "").strip().upper()
    junk_flags = str(cand.get("junk_flags") or "").strip()
    final_score = _float(cand.get("final_score"), default=-1.0)
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


def _stats(values: list[float]) -> dict[str, Any]:
    xs = [float(v) for v in values if not math.isnan(float(v))]
    if not xs:
        return {"n": 0, "sum": 0.0, "avg": None, "median": None, "min": None, "max": None, "loss_rate": None}
    return {
        "n": len(xs),
        "sum": round(sum(xs), 6),
        "avg": round(sum(xs) / len(xs), 6),
        "median": round(median(xs), 6),
        "min": round(min(xs), 6),
        "max": round(max(xs), 6),
        "loss_rate": round(sum(1 for x in xs if x < 0) / len(xs), 6),
    }


def _sell_meta_rows(fills: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in fills:
        if str(row.get("side") or "").upper() != "SELL":
            continue
        note = row.get("note", "")
        out.append(
            {
                "code": str(row.get("code") or "").zfill(6),
                "ts": str(row.get("ts") or row.get("datetime") or ""),
                "ymd": _ymd(row.get("ts") or row.get("datetime") or row.get("date")),
                "qty": _int(row.get("qty")),
                "price": _float(row.get("price")),
                "order_id": str(row.get("order_id") or ""),
                "entry_order_id": _note_value(note, "entry_order_id") or _note_value(note, "source_order_id"),
                "exit_reason": _note_value(note, "exit_reason"),
                "sell_ratio_pct": _float(_note_value(note, "sell_ratio_pct")),
                "partial_exit": _int(_note_value(note, "partial_exit")),
                "prior_stop_count": _int(_note_value(note, "prior_stop_count")),
            }
        )
    return out


def _match_sell_meta(trade: dict[str, str], sells: list[dict[str, Any]]) -> dict[str, Any]:
    code = str(trade.get("code") or "").zfill(6)
    note = trade.get("note", "")
    entry_oid = _note_value(note, "entry_order_id") or _note_value(note, "order_id")
    exit_ymd = _ymd(trade.get("exit_ts"))
    qty = _int(trade.get("qty"))
    candidates = [
        s for s in sells
        if s["code"] == code and s["entry_order_id"] == entry_oid and s["ymd"] == exit_ymd
    ]
    for item in candidates:
        if _int(item.get("qty")) == qty:
            return item
    # Keep attribution aligned with build_mdd_exit_quality_diagnostic.py:
    # exact qty first, then first SELL for the same entry_order_id/date.
    return candidates[0] if candidates else {}


def _summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        net = _float(row.get("net_ret"))
        for kind in [
            "exit_reason",
            "entry_timing",
            "quality_bucket",
            "candidate_origin",
            "relax_level",
            "execution_pool",
            "natural_pass",
            "junk_risk_grade",
            "surge_flag_bucket",
            "horizon",
            "entry_hour",
        ]:
            buckets[(kind, str(row.get(kind) or "UNKNOWN"))].append(net)
    out = []
    for (kind, bucket), vals in sorted(buckets.items()):
        out.append({"kind": kind, "bucket": bucket, **_stats(vals)})
    return out


def main() -> int:
    mdd = _read_json(MDD_JSON)
    mdd_window = mdd.get("mdd", {}) if isinstance(mdd.get("mdd"), dict) else {}
    start = str(mdd_window.get("peak_date") or "")
    end = str(mdd_window.get("trough_date") or "")
    if not start or not end:
        raise RuntimeError("Missing MDD window in latest diagnosis")

    trades, official_filter = _official_filtered_trades()
    fills = _read_csv(FILLS_NORM)
    sells = _sell_meta_rows(fills)
    archive_by_oid = _archive_by_entry_order_id()

    window_trades = [
        row for row in trades
        if start <= _ymd(row.get("exit_ts")) <= end
    ]
    candidate_dates = {_note_value(row.get("note", ""), "signal_date") or _ymd(row.get("entry_ts")) for row in window_trades}
    candidates = _candidate_cache(candidate_dates)

    rows: list[dict[str, Any]] = []
    for trade in window_trades:
        code = str(trade.get("code") or "").zfill(6)
        note = trade.get("note", "")
        signal_date = _note_value(note, "signal_date") or _ymd(trade.get("entry_ts"))
        entry_oid = _note_value(note, "entry_order_id") or _note_value(note, "order_id")
        archive = archive_by_oid.get(entry_oid, {})
        entry_timing = _note_value(note, "entry_timing") or "UNKNOWN"
        sell_meta = _match_sell_meta(trade, sells)
        exit_reason = str(sell_meta.get("exit_reason") or "")
        if exit_reason and exit_reason not in FOCUS_EXITS:
            continue
        if not exit_reason:
            continue
        cand_path, cand, candidate_join_mode = _select_candidate_for_entry(
            candidates.get(signal_date, []),
            code,
            trade.get("entry_ts"),
            signal_date,
        )
        surge_type = _note_value(note, "surge_type")
        surge_immediate = _note_value(note, "surge_immediate")
        if surge_type or _bool_text(surge_immediate):
            surge_flag_bucket = "surge_immediate_1_with_type" if surge_type else "surge_immediate_1_missing_type"
        else:
            surge_flag_bucket = "missing_surge_type"
        entry_ts = str(trade.get("entry_ts") or "")
        entry_hour = entry_ts[11:13] if len(entry_ts) >= 13 else "UNKNOWN"
        rows.append(
            {
                "trade_id": str(trade.get("trade_id") or ""),
                "code": code,
                "entry_ts": entry_ts,
                "exit_ts": str(trade.get("exit_ts") or ""),
                "entry_hour": entry_hour,
                "signal_date": signal_date,
                "entry_order_id": entry_oid,
                "exit_reason": exit_reason,
                "qty": _int(trade.get("qty")),
                "entry_price": _float(trade.get("entry_price")),
                "exit_price": _float(trade.get("exit_price")),
                "net_ret": _float(trade.get("net_ret")),
                "entry_notional": round(_int(trade.get("qty")) * _float(trade.get("entry_price")), 6),
                "entry_timing": entry_timing,
                "horizon": _note_value(note, "horizon") or "UNKNOWN",
                "surge_flag_bucket": surge_flag_bucket,
                "candidate_file": "" if cand_path is None else cand_path.name,
                "candidate_join_mode": candidate_join_mode,
                "candidate_matched": bool(cand),
                "entry_archive_matched": bool(archive),
                "archive_entry_source_kind": archive.get("entry_source_kind", ""),
                "archive_candidate_snapshot_id": archive.get("candidate_snapshot_id", ""),
                "archive_entry_trace_id": archive.get("entry_trace_id", ""),
                "archive_entry_timing": archive.get("entry_timing", ""),
                "archive_fallback_stage": archive.get("fallback_stage", ""),
                "candidate_origin": cand.get("candidate_origin", ""),
                "execution_pool": str(_bool_text(cand.get("execution_pool"))).lower() if cand else "",
                "natural_pass": str(_bool_text(cand.get("natural_pass"))).lower() if cand else "",
                "relax_level": cand.get("relax_level", ""),
                "final_score": cand.get("final_score", ""),
                "score": cand.get("score", ""),
                "ret1_pct": cand.get("ret1_pct", ""),
                "rs": cand.get("rs", ""),
                "rs_slope": cand.get("rs_slope", ""),
                "stretch": cand.get("stretch", ""),
                "v_accel": cand.get("v_accel", ""),
                "atr14_pct": cand.get("atr14_pct", ""),
                "rsi14": cand.get("rsi14", ""),
                "junk_risk_grade": cand.get("junk_risk_grade", ""),
                "junk_flags": cand.get("junk_flags", ""),
                "quality_bucket": _quality_bucket(cand),
                "sell_ratio_pct": sell_meta.get("sell_ratio_pct", ""),
                "partial_exit": sell_meta.get("partial_exit", ""),
                "prior_stop_count": sell_meta.get("prior_stop_count", ""),
            }
        )

    quality_counts = Counter(str(row.get("quality_bucket") or "") for row in rows)
    missing_rows = [row for row in rows if str(row.get("quality_bucket") or "") == "NO_CANDIDATE_MATCH"]
    missing_dates = {str(row.get("signal_date") or "") for row in missing_rows}
    missing_codes = {str(row.get("code") or "").zfill(6) for row in missing_rows}
    missing_presence = _candidate_presence_by_date(missing_dates, missing_codes)
    for row in missing_rows:
        ymd = str(row.get("signal_date") or "")
        code = str(row.get("code") or "").zfill(6)
        info = missing_presence.get(ymd, {})
        file_hits = info.get("file_hits") if isinstance(info.get("file_hits"), dict) else {}
        hit_files = [name for name, codes in sorted(file_hits.items()) if code in set(codes)]
        row["candidate_gap_class"] = _candidate_gap_class(row, missing_presence)
        row["entry_source_bucket"] = _entry_source_bucket(row)
        row["missing_root_cause"] = _missing_root_cause(row)
        row["same_date_candidate_file_count"] = int(info.get("file_count") or 0)
        row["latest_candidate_file_for_signal_date"] = str(info.get("latest_file") or "")
        row["same_date_candidate_hit_files"] = ";".join(hit_files)
    for row in rows:
        row.setdefault("candidate_gap_class", "")
        row.setdefault("candidate_join_mode", "")
        row.setdefault("entry_source_bucket", "")
        row.setdefault("missing_root_cause", "")
        row.setdefault("entry_archive_matched", False)
        row.setdefault("archive_entry_source_kind", "")
        row.setdefault("archive_candidate_snapshot_id", "")
        row.setdefault("archive_entry_trace_id", "")
        row.setdefault("archive_entry_timing", "")
        row.setdefault("archive_fallback_stage", "")
        row.setdefault("same_date_candidate_file_count", "")
        row.setdefault("latest_candidate_file_for_signal_date", "")
        row.setdefault("same_date_candidate_hit_files", "")
    timing_counts = Counter(str(row.get("entry_timing") or "") for row in rows)
    exit_counts = Counter(str(row.get("exit_reason") or "") for row in rows)
    missing_gap_counts = Counter(str(row.get("candidate_gap_class") or "") for row in missing_rows)
    missing_source_counts = Counter(str(row.get("entry_source_bucket") or "") for row in missing_rows)
    missing_root_counts = Counter(str(row.get("missing_root_cause") or "") for row in missing_rows)
    missing_archive_counts = Counter(str(bool(row.get("entry_archive_matched"))) for row in missing_rows)
    matched = sum(1 for row in rows if row.get("candidate_matched"))
    no_match = int(quality_counts.get("NO_CANDIDATE_MATCH", 0))
    weak_quality = sum(
        int(quality_counts.get(k, 0))
        for k in ["SECTOR_UNION_NOT_EXECUTION_POOL", "L3_WITH_RISK_FLAGS", "L3_RELAXED", "NON_NATURAL_PASS", "RISK_FLAGGED_NATURAL_PASS", "LOW_FINAL_SCORE"]
    )
    intraday_rows = [row for row in rows if str(row.get("entry_timing") or "") == "intraday_realtime"]
    summary_rows = _summary(rows)
    out = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "status": "FAIL",
        "policy_change": False,
        "trading_effect": False,
        "source": {
            "mdd_diagnosis": str(MDD_JSON),
            "trades_calc": str(TRADES_CALC),
            "fills_norm": str(FILLS_NORM),
        },
        "mdd": mdd_window,
        "scope": {
            "window_start": start,
            "window_end": end,
            "window_trade_rows": len(window_trades),
            "focus_exit_reasons": sorted(FOCUS_EXITS),
            "focus_rows": len(rows),
        },
        "official_filter": official_filter,
        "candidate_join": {
            "candidate_dates": sorted(candidate_dates),
            "candidate_files_found": sum(len(items) for items in candidates.values()),
            "candidate_join_mode": "closest_before_by_entry_ts",
            "candidate_matched_rows": matched,
            "candidate_missing_rows": no_match,
        },
        "candidate_missing_diagnosis": {
            "gap_class_counts": dict(sorted(missing_gap_counts.items())),
            "entry_source_counts": dict(sorted(missing_source_counts.items())),
            "root_cause_counts": dict(sorted(missing_root_counts.items())),
            "entry_archive_match_counts": dict(sorted(missing_archive_counts.items())),
            "entry_source_archive": str(ENTRY_SOURCE_ARCHIVE),
            "candidate_missing_csv": str(OUT_MISSING_CSV),
        },
        "breakdown": {
            "exit_reason_counts": dict(sorted(exit_counts.items())),
            "entry_timing_counts": dict(sorted(timing_counts.items())),
            "quality_bucket_counts": dict(sorted(quality_counts.items())),
            "focus_net_stats": _stats([_float(row.get("net_ret")) for row in rows]),
            "intraday_realtime_net_stats": _stats([_float(row.get("net_ret")) for row in intraday_rows]),
            "weak_quality_rows": weak_quality,
        },
        "interpretation": [
            "This is read-only evidence for candidate quality and entry timing before STOP/DDM exits.",
            "It does not relax STOP/DDM thresholds and does not change order, fill, ledger, or broker routes.",
            "NO_CANDIDATE_MATCH means the selected trade could not be matched to the latest candidate backup for its signal_date/code; it is metadata coverage, not proof of non-candidate entry.",
            "When entry_source_archive_history.csv has the entry_order_id, missing rows are classified from the captured runtime source instead of a generic lineage gap.",
        ],
    }

    fields = [
        "trade_id", "code", "entry_ts", "exit_ts", "entry_hour", "signal_date", "entry_order_id", "exit_reason", "qty",
        "entry_price", "exit_price", "net_ret", "entry_notional", "entry_timing", "horizon",
        "surge_flag_bucket", "candidate_file", "candidate_join_mode", "candidate_matched",
        "entry_archive_matched", "archive_entry_source_kind", "archive_candidate_snapshot_id",
        "archive_entry_trace_id", "archive_entry_timing", "archive_fallback_stage", "candidate_origin",
        "execution_pool", "natural_pass", "relax_level", "final_score", "score", "ret1_pct", "rs",
        "rs_slope", "stretch", "v_accel", "atr14_pct", "rsi14", "junk_risk_grade", "junk_flags",
        "quality_bucket", "sell_ratio_pct", "partial_exit", "prior_stop_count",
        "candidate_gap_class", "entry_source_bucket", "missing_root_cause", "same_date_candidate_file_count",
        "latest_candidate_file_for_signal_date", "same_date_candidate_hit_files",
    ]
    missing_fields = fields
    summary_fields = ["kind", "bucket", "n", "sum", "avg", "median", "min", "max", "loss_rate"]
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows, fields)
    _write_csv(OUT_MISSING_CSV, missing_rows, missing_fields)
    _write_csv(OUT_SUMMARY_CSV, summary_rows, summary_fields)
    print(json.dumps({"status": out["status"], "scope": out["scope"], "candidate_join": out["candidate_join"], "breakdown": out["breakdown"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
