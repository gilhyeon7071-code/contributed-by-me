from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"

FILLS_PATH = PAPER_DIR / "fills.csv"
TRADES_CALC_PATH = PAPER_DIR / "trades_calc.csv"

OUT_JSON = LOG_DIR / "normal_entry_baseline_label_report_latest.json"
OUT_CSV = LOG_DIR / "normal_entry_baseline_label_report_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "normal_entry_baseline_label_report_summary_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> List[Dict[str, str]]:
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


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _note_fields(note: Any) -> Dict[str, str]:
    text = str(note or "").replace(" | ", ";")
    out: Dict[str, str] = {}
    for part in re.split(r"[;|]", text):
        part = part.strip()
        if "=" not in part:
            continue
        key, val = part.split("=", 1)
        key = key.strip()
        if key:
            out[key] = val.strip()
    return out


def _ymd(value: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def _float(value: Any) -> float | None:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def _classify(note: Dict[str, str], raw_note: str, order_id: str) -> Tuple[str, str, str]:
    text = str(raw_note or "").lower()
    timing = str(note.get("entry_timing") or "").strip().lower()
    stage = str(note.get("fallback_stage") or "").strip()
    split = str(note.get("split_entry") or "").strip().lower()
    surge_type = str(note.get("surge_type") or "").strip()
    is_surge = _truthy(note.get("surge_immediate")) or bool(surge_type)
    is_validation_reduce = "validation_reduce" in text
    is_paper_probe = "paper_probe" in text
    is_shadow_or_observe = any(
        token in text
        for token in (
            "shadow_",
            "observe_only",
            "policy_review_only_no_trading_effect",
            "wait_lob",
        )
    )
    is_reduced = ("overheat_reduce" in text) or ("sector_hrp_reduce" in text)
    is_split_followup = bool(split and split not in {"1st", "first"})
    is_replay_unclear = bool("_R" in str(order_id or "") or re.search(r"_N\d+", str(order_id or "")))

    if is_shadow_or_observe:
        return "EXCLUDE_SHADOW_OR_OBSERVE", "shadow/observe/review-only evidence", "exclude"
    if is_validation_reduce:
        return "EXCLUDE_VALIDATION_REDUCE", "validation_reduce is sample/exception evidence", "exclude"
    if is_paper_probe:
        return "EXCLUDE_PAPER_PROBE", "paper_probe is sample/probe evidence", "exclude"
    if is_split_followup:
        return "EXCLUDE_SPLIT_FOLLOWUP", "split follow-up is not new-entry quality", "exclude"
    if is_surge:
        return "ISOLATE_SURGE_IMMEDIATE", "surge immediate/realtime policy group", "isolate"
    if timing == "intraday_realtime" or stage == "0(intraday_realtime)":
        if is_reduced:
            return "SEPARATE_INTRADAY_REDUCED", "intraday entry with active size reduction", "separate"
        return "SEPARATE_INTRADAY_NON_SURGE", "intraday realtime is separate from normal baseline", "separate"
    if timing == "same_close" or stage in {"0(close_auction)", "1(next_open_limit)", "2(intraday_limit)"}:
        if is_reduced:
            return "SEPARATE_DELAYED_REDUCED", "delayed/fallback with active size reduction", "separate"
        return "INCLUDE_NORMAL_BASELINE", "same_close/fallback baseline candidate", "include"
    if is_replay_unclear:
        return "EXCLUDE_LEGACY_REPLAY_UNCLEAR", "legacy/replay metadata is not clean baseline proof", "exclude"
    return "EXCLUDE_UNKNOWN_OR_LEGACY", "missing timing metadata", "exclude"


def _stats(values: Iterable[float]) -> Dict[str, Any]:
    xs = [float(v) for v in values if v is not None and not math.isnan(float(v))]
    if not xs:
        return {"n": 0, "avg": None, "median": None, "win_rate": None, "min": None, "max": None}
    return {
        "n": len(xs),
        "avg": sum(xs) / len(xs),
        "median": median(xs),
        "win_rate": sum(1 for x in xs if x > 0) / len(xs),
        "min": min(xs),
        "max": max(xs),
    }


def _build_trade_rows(trades: List[Dict[str, str]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    duplicate_counter: Counter[Tuple[str, str]] = Counter()

    for row in trades:
        note_text = str(row.get("note") or "")
        note = _note_fields(note_text)
        order_id = str(note.get("order_id") or row.get("entry_order_id") or "").strip()
        code = str(row.get("code") or "").zfill(6)
        label, reason, bucket = _classify(note, note_text, order_id)
        net_ret = _float(row.get("net_ret"))
        duplicate_counter[(code, order_id)] += 1
        rows.append(
            {
                "source": "trades_calc",
                "trade_id": row.get("trade_id", ""),
                "entry_ts": row.get("entry_ts", ""),
                "entry_ymd": _ymd(row.get("entry_ts", "")),
                "exit_ts": row.get("exit_ts", ""),
                "code": code,
                "order_id": order_id,
                "entry_timing": note.get("entry_timing", ""),
                "fallback_stage": note.get("fallback_stage", ""),
                "horizon": note.get("horizon", ""),
                "split_entry": note.get("split_entry", ""),
                "surge_immediate": note.get("surge_immediate", ""),
                "surge_type": note.get("surge_type", ""),
                "run_label": note.get("run_label", ""),
                "entry_trace_id": note.get("entry_trace_id", ""),
                "net_ret": "" if net_ret is None else net_ret,
                "baseline_label": label,
                "baseline_bucket": bucket,
                "label_reason": reason,
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )

    duplicate_groups = sum(1 for v in duplicate_counter.values() if v > 1)
    duplicate_rows = sum(v for v in duplicate_counter.values() if v > 1)
    return rows, {"duplicate_trade_groups": duplicate_groups, "duplicate_trade_rows": duplicate_rows}


def _build_fill_summary(fills: List[Dict[str, str]]) -> Dict[str, Any]:
    buy_rows = [r for r in fills if str(r.get("side") or "").strip().upper() == "BUY"]
    labels: Counter[str] = Counter()
    by_ymd: Dict[str, Counter[str]] = defaultdict(Counter)
    for row in buy_rows:
        note_text = str(row.get("note") or "")
        note = _note_fields(note_text)
        label, _, _ = _classify(note, note_text, str(row.get("order_id") or ""))
        labels[label] += 1
        by_ymd[_ymd(row.get("datetime", ""))][label] += 1
    return {
        "buy_rows": len(buy_rows),
        "label_counts": dict(labels),
        "recent_ymd_label_counts": {
            ymd: dict(counts)
            for ymd, counts in sorted(by_ymd.items())[-12:]
        },
    }


def main() -> int:
    fills = _read_csv(FILLS_PATH)
    trades = _read_csv(TRADES_CALC_PATH)
    trade_rows, duplicate_summary = _build_trade_rows(trades)
    fill_summary = _build_fill_summary(fills)

    by_label: Dict[str, List[float]] = defaultdict(list)
    by_bucket: Dict[str, List[float]] = defaultdict(list)
    for row in trade_rows:
        val = _float(row.get("net_ret"))
        if val is None:
            continue
        by_label[str(row["baseline_label"])].append(val)
        by_bucket[str(row["baseline_bucket"])].append(val)

    label_summary = [
        {"kind": "label", "name": k, **_stats(v)}
        for k, v in sorted(by_label.items())
    ]
    bucket_summary = [
        {"kind": "bucket", "name": k, **_stats(v)}
        for k, v in sorted(by_bucket.items())
    ]

    out = {
        "generated_at": _now_ts(),
        "status": "PASS",
        "source_files": {
            "fills": str(FILLS_PATH),
            "trades_calc": str(TRADES_CALC_PATH),
        },
        "trade_rows": len(trade_rows),
        "fill_summary": fill_summary,
        "label_summary": label_summary,
        "bucket_summary": bucket_summary,
        "duplicate_summary": duplicate_summary,
        "normal_baseline_definition": [
            "entry_timing=same_close",
            "fallback_stage=0(close_auction)",
            "fallback_stage=1(next_open_limit)",
            "fallback_stage=2(intraday_limit) as small-sample sub-bucket",
        ],
        "excluded_from_normal_policy_proof": [
            "surge_immediate",
            "intraday_realtime",
            "validation_reduce",
            "paper_probe",
            "shadow/observe/review-only",
            "split follow-up",
            "legacy/replay/unknown metadata",
            "active reduced-size rows when evaluating original entry quality",
        ],
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    row_fields = [
        "source",
        "trade_id",
        "entry_ts",
        "entry_ymd",
        "exit_ts",
        "code",
        "order_id",
        "entry_timing",
        "fallback_stage",
        "horizon",
        "split_entry",
        "surge_immediate",
        "surge_type",
        "run_label",
        "entry_trace_id",
        "net_ret",
        "baseline_label",
        "baseline_bucket",
        "label_reason",
        "policy_effect",
        "policy_change_applied",
    ]
    _write_csv(OUT_CSV, trade_rows, row_fields)

    summary_rows = bucket_summary + label_summary
    _write_csv(OUT_SUMMARY_CSV, summary_rows, ["kind", "name", "n", "avg", "median", "win_rate", "min", "max"])

    print(
        f"[FINAL] normal entry baseline labels -> {OUT_JSON} "
        f"trade_rows={len(trade_rows)} labels={len(label_summary)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
