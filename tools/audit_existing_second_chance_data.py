"""Audit whether existing logs can validate second-chance entry logic.

Read-only. This script does not approve trades, alter gates, write candidate
inputs, or change paper-engine policy. It only inventories historical artifacts
and classifies which questions can be answered from existing data.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

CANDIDATE_REVIEW_CSV = LOG_DIR / "candidate_action_review_latest.csv"
CANDIDATE_FOLLOWUP_CSV = LOG_DIR / "candidate_action_followup_latest.csv"
SHADOW_HISTORY_CSV = LOG_DIR / "shadow_promotion_report_history.csv"
SURGE_LATEST_CSV = LOG_DIR / "surge_realtime_latest.csv"
P1_HISTORY_CSV = LOG_DIR / "p1_entry_gate_status_history.csv"
FOLLOWTHROUGH_REALTIME_CSV = LOG_DIR / "followthrough_realtime_history.csv"
FOLLOWTHROUGH_MID_CSV = LOG_DIR / "followthrough_mid_status.csv"

OUT_JSON = LOG_DIR / "existing_second_chance_data_audit_latest.json"
OUT_CSV = LOG_DIR / "existing_second_chance_data_audit_latest.csv"


def _now_kst() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec="seconds")


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): str(v) for k, v in row.items()} for row in csv.DictReader(f)]


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").strip()
        if not text:
            return float(default)
        return float(text)
    except Exception:
        return float(default)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _code(value: Any) -> str:
    raw = "".join(ch for ch in str(value or "") if ch.isdigit())
    return raw.zfill(6)[-6:] if raw else ""


def _tokenize_reasons(value: Any) -> List[str]:
    tokens: List[str] = []
    for raw in str(value or "").replace(",", "|").split("|"):
        token = raw.strip()
        if not token:
            continue
        tokens.append(token.split(":", 1)[0])
    return tokens


def _csv_file_rows(files: Iterable[Path]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for path in files:
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                out[path.name] = max(sum(1 for _ in f) - 1, 0)
        except Exception:
            out[path.name] = -1
    return out


def _latest_mtime(files: Iterable[Path]) -> str:
    paths = [p for p in files if p.exists()]
    if not paths:
        return ""
    latest = max(paths, key=lambda p: p.stat().st_mtime)
    return dt.datetime.fromtimestamp(latest.stat().st_mtime).isoformat(timespec="seconds")


def _target_codes(shadow_rows: List[Dict[str, str]]) -> List[str]:
    codes = sorted({_code(row.get("code")) for row in shadow_rows if _code(row.get("code"))})
    return codes


def _surge_snapshot_files() -> List[Path]:
    return sorted(
        p
        for p in LOG_DIR.glob("surge_realtime_20260521_*.csv")
        if p.name != "surge_realtime_latest.csv"
    )


def _history_price_files() -> List[Path]:
    return sorted(LOG_DIR.glob("intraday_prices_history_*.csv"))


def _read_surge_snapshots(files: Iterable[Path]) -> Tuple[List[Dict[str, str]], Counter[str], Counter[str]]:
    rows: List[Dict[str, str]] = []
    reason_counts: Counter[str] = Counter()
    code_counts: Counter[str] = Counter()
    for path in files:
        for row in _read_csv(path):
            row["_snapshot_file"] = path.name
            code = _code(row.get("code"))
            if code:
                code_counts[code] += 1
            for token in _tokenize_reasons(row.get("exclude_reasons")):
                reason_counts[token] += 1
            rows.append(row)
    return rows, reason_counts, code_counts


def _series_by_target(rows: Iterable[Dict[str, str]], target_codes: Iterable[str]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    targets = set(target_codes)
    for row in rows:
        code = _code(row.get("code"))
        if code in targets:
            grouped[code].append(row)

    out: List[Dict[str, Any]] = []
    for code in sorted(targets):
        series = grouped.get(code, [])
        changes = [_f(r.get("change_pct")) for r in series]
        drawdowns = [_f(r.get("intraday_high_drawdown_pct")) for r in series]
        rvols = [_f(r.get("rvol20")) for r in series]
        lob_count = sum(1 for r in series if _truthy(r.get("lob_available")))
        reason_counter: Counter[str] = Counter()
        for row in series:
            reason_counter.update(_tokenize_reasons(row.get("exclude_reasons")))
        out.append(
            {
                "code": code,
                "surge_snapshot_rows": len(series),
                "first_ts": str(series[0].get("ts") or "") if series else "",
                "last_ts": str(series[-1].get("ts") or "") if series else "",
                "first_change_pct": changes[0] if changes else "",
                "latest_change_pct": changes[-1] if changes else "",
                "max_change_pct": max(changes) if changes else "",
                "min_change_pct": min(changes) if changes else "",
                "min_intraday_high_drawdown_pct": min(drawdowns) if drawdowns else "",
                "max_rvol20": max(rvols) if rvols else "",
                "lob_available_rows": lob_count,
                "no_lob_rows": reason_counter.get("NO_LOB_BLOCK", 0),
                "entry_change_block_rows": reason_counter.get("ENTRY_CHANGE_BLOCK", 0),
                "atr_cap_rows": reason_counter.get("ENTRY_ATR_CAP", 0),
                "rvol_overheat_rows": reason_counter.get("SCORE_RVOL_OVERHEAT_BLOCK", 0),
                "krx_caution_rows": sum(v for k, v in reason_counter.items() if k.startswith("KRX_")),
            }
        )
    return out


def _capability_rows(
    candidate_review_rows: List[Dict[str, str]],
    candidate_followup_rows: List[Dict[str, str]],
    shadow_rows: List[Dict[str, str]],
    surge_files: List[Path],
    surge_rows: List[Dict[str, str]],
    target_series: List[Dict[str, Any]],
    price_files: List[Path],
    p1_rows: List[Dict[str, str]],
    followthrough_realtime_rows: List[Dict[str, str]],
    followthrough_mid_rows: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    target_with_series = sum(1 for r in target_series if int(r.get("surge_snapshot_rows") or 0) > 0)
    rows = [
        {
            "check": "missed_move_return_snapshot",
            "status": "PASS" if candidate_review_rows or candidate_followup_rows else "FAIL",
            "evidence_count": len(candidate_review_rows) + len(candidate_followup_rows),
            "meaning": "existing latest candidate files can show observed return since first seen",
            "gap": "latest snapshot only; it does not prove executable fill quality",
        },
        {
            "check": "shadow_candidate_block_reason",
            "status": "PASS" if shadow_rows else "FAIL",
            "evidence_count": len(shadow_rows),
            "meaning": "shadow promotion history can explain why current review rows were blocked",
            "gap": "history currently has too few rows for policy calibration",
        },
        {
            "check": "surge_overheat_time_series",
            "status": "PARTIAL" if surge_files and target_with_series else "FAIL",
            "evidence_count": sum(int(r.get("surge_snapshot_rows") or 0) for r in target_series),
            "meaning": "surge snapshots can replay change, RVOL, ATR, LOB flags for covered target codes",
            "gap": "coverage is only as complete as saved snapshots and does not include all prior candidates",
        },
        {
            "check": "p1_ddm_gate_history",
            "status": "PASS" if p1_rows else "FAIL",
            "evidence_count": len(p1_rows),
            "meaning": "P1/DDM gate state has historical rows for gate-open or gate-closed context",
            "gap": "gate history is batch-level, not a complete per-candidate counterfactual entry result",
        },
        {
            "check": "intraday_price_history",
            "status": "PARTIAL" if price_files else "FAIL",
            "evidence_count": len(price_files),
            "meaning": "intraday price history can support post-signal price movement checks",
            "gap": "it still needs candidate-time alignment before it can validate a fill rule",
        },
        {
            "check": "followthrough_history",
            "status": "PARTIAL" if followthrough_realtime_rows or followthrough_mid_rows else "FAIL",
            "evidence_count": len(followthrough_realtime_rows) + len(followthrough_mid_rows),
            "meaning": "followthrough history can help compare observation and later movement",
            "gap": "it is not an approval log and does not replace order/fill evidence",
        },
        {
            "check": "lob_availability_history",
            "status": "PARTIAL" if surge_rows and "lob_available" in (surge_rows[0].keys() if surge_rows else []) else "FAIL",
            "evidence_count": sum(int(r.get("surge_snapshot_rows") or 0) for r in target_series),
            "meaning": "saved surge rows include LOB availability flags for covered target snapshots",
            "gap": "depth and slippage at the exact hypothetical entry moment are not fully recoverable",
        },
        {
            "check": "real_order_fill_e2e",
            "status": "NA",
            "evidence_count": 0,
            "meaning": "existing shadow data cannot prove real order to fill to ledger behavior",
            "gap": "requires actual order/fill/ledger chain under approved policy",
        },
    ]
    return rows


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "check",
        "status",
        "evidence_count",
        "meaning",
        "gap",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    candidate_review_rows = _read_csv(CANDIDATE_REVIEW_CSV)
    candidate_followup_rows = _read_csv(CANDIDATE_FOLLOWUP_CSV)
    shadow_rows = _read_csv(SHADOW_HISTORY_CSV)
    p1_rows = _read_csv(P1_HISTORY_CSV)
    followthrough_realtime_rows = _read_csv(FOLLOWTHROUGH_REALTIME_CSV)
    followthrough_mid_rows = _read_csv(FOLLOWTHROUGH_MID_CSV)
    surge_files = _surge_snapshot_files()
    price_files = _history_price_files()
    surge_rows, reason_counts, code_counts = _read_surge_snapshots(surge_files)
    targets = _target_codes(shadow_rows)
    target_series = _series_by_target(surge_rows, targets)

    capability = _capability_rows(
        candidate_review_rows,
        candidate_followup_rows,
        shadow_rows,
        surge_files,
        surge_rows,
        target_series,
        price_files,
        p1_rows,
        followthrough_realtime_rows,
        followthrough_mid_rows,
    )
    status_counts = Counter(str(row.get("status") or "") for row in capability)

    review_candidates = [r for r in candidate_review_rows if _truthy(r.get("policy_review_candidate"))]
    payload = {
        "generated_at": _now_kst(),
        "schema_version": "existing_second_chance_data_audit_v1",
        "generated_from": {
            "candidate_review_csv": str(CANDIDATE_REVIEW_CSV),
            "candidate_followup_csv": str(CANDIDATE_FOLLOWUP_CSV),
            "shadow_history_csv": str(SHADOW_HISTORY_CSV),
            "surge_snapshot_glob": str(LOG_DIR / "surge_realtime_20260521_*.csv"),
            "p1_history_csv": str(P1_HISTORY_CSV),
            "followthrough_realtime_csv": str(FOLLOWTHROUGH_REALTIME_CSV),
            "followthrough_mid_csv": str(FOLLOWTHROUGH_MID_CSV),
            "intraday_price_history_glob": str(LOG_DIR / "intraday_prices_history_*.csv"),
        },
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "summary": {
            "candidate_review_rows": len(candidate_review_rows),
            "candidate_review_policy_rows": len(review_candidates),
            "candidate_followup_rows": len(candidate_followup_rows),
            "shadow_history_rows": len(shadow_rows),
            "surge_snapshot_files": len(surge_files),
            "surge_snapshot_rows": len(surge_rows),
            "surge_unique_codes": len(code_counts),
            "target_codes_from_shadow": targets,
            "target_codes_with_surge_series": sum(1 for r in target_series if int(r.get("surge_snapshot_rows") or 0) > 0),
            "intraday_price_history_files": len(price_files),
            "p1_history_rows": len(p1_rows),
            "followthrough_realtime_rows": len(followthrough_realtime_rows),
            "followthrough_mid_rows": len(followthrough_mid_rows),
            "capability_status_counts": dict(status_counts),
        },
        "interpretation": {
            "existing_data_verdict": "PARTIAL_VALIDATION_ONLY",
            "can_validate": [
                "missed-move observation and blocked reason frequency",
                "whether today target candidates were also surge or overheat risk rows",
                "limited time-series behavior for target codes covered by saved surge snapshots",
            ],
            "cannot_validate": [
                "normal-policy alpha profitability with unbiased live entries",
                "exact hypothetical fill quality, depth, and slippage at missed entry time",
                "real order to fill to ledger to stats E2E under a changed policy",
            ],
            "policy_value_implication": (
                "existing data is useful for rejecting unsafe relaxations, but insufficient for "
                "calibrating final policy thresholds without a separated approved sample path"
            ),
        },
        "top_block_reason_counts": dict(reason_counts.most_common(20)),
        "target_series": target_series,
        "price_history_files": _csv_file_rows(price_files),
        "latest_input_mtime": _latest_mtime(
            [
                CANDIDATE_REVIEW_CSV,
                CANDIDATE_FOLLOWUP_CSV,
                SHADOW_HISTORY_CSV,
                P1_HISTORY_CSV,
                FOLLOWTHROUGH_REALTIME_CSV,
                FOLLOWTHROUGH_MID_CSV,
                *surge_files,
                *price_files,
            ]
        ),
        "artifacts": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "capability_rows": capability,
    }
    _write_json(OUT_JSON, payload)
    _write_csv(OUT_CSV, capability)
    print(
        json.dumps(
            {
                "status": "OK",
                **payload["summary"],
                "out_json": str(OUT_JSON),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
