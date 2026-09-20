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
ENTRY_LAYERS_PATH = LOG_DIR / "entry_decision_layers_runtime_latest.csv"
FALLBACK_WHATIF_PATH = LOG_DIR / "entry_fallback_stage_whatif_latest.csv"

OUT_JSON = LOG_DIR / "entry_policy_group_report_latest.json"
OUT_CSV = LOG_DIR / "entry_policy_group_report_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "entry_policy_group_report_summary_latest.csv"

POLICY_GROUPS = {
    "normal_entry",
    "intraday_realtime",
    "surge_immediate",
    "validation_reduce",
    "shadow_probe_observe",
    "legacy_unknown",
}


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


def _contains_any(text: str, tokens: Iterable[str]) -> bool:
    return any(token in text for token in tokens)


def _classify_policy_group(fields: Dict[str, str], raw_text: str, order_id: str = "") -> Tuple[str, str, str]:
    text = str(raw_text or "").lower()
    timing = str(fields.get("entry_timing") or "").strip().lower()
    stage = str(fields.get("fallback_stage") or "").strip()
    split = str(fields.get("split_entry") or "").strip().lower()
    surge_type = str(fields.get("surge_type") or "").strip()
    surge_immediate = _truthy(fields.get("surge_immediate")) or bool(surge_type)
    shadow_or_probe = _contains_any(
        text,
        (
            "shadow_",
            "observe_only",
            "policy_review_only_no_trading_effect",
            "wait_lob",
            "paper_probe",
            "probe",
        ),
    )
    validation_reduce = "validation_reduce" in text
    reduced = ("overheat_reduce" in text) or ("sector_hrp_reduce" in text)
    split_followup = bool(split and split not in {"1st", "first"})
    replay_unclear = bool("_R" in str(order_id or "") or re.search(r"_N\d+", str(order_id or "")))

    if shadow_or_probe:
        return "shadow_probe_observe", "observe/probe/shadow row", "exclude_from_policy_proof"
    if validation_reduce or reduced:
        return "validation_reduce", "reduced-size validation/risk path", "validation_only"
    if surge_immediate:
        return "surge_immediate", "surge immediate or surge type row", "separate_policy_group"
    if timing == "intraday_realtime" or stage == "0(intraday_realtime)":
        return "intraday_realtime", "intraday realtime row", "separate_policy_group"
    if split_followup:
        return "legacy_unknown", "split follow-up is not a clean new-entry sample", "exclude_from_policy_proof"
    if timing == "same_close" or stage in {"0(close_auction)", "1(next_open_limit)", "2(intraday_limit)"}:
        return "normal_entry", "clean normal timing/fallback row", "normal_policy_candidate"
    if replay_unclear:
        return "legacy_unknown", "legacy/replay metadata is not clean policy proof", "exclude_from_policy_proof"
    return "legacy_unknown", "missing or unknown policy metadata", "exclude_from_policy_proof"


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


def _append_trade_rows(rows: List[Dict[str, Any]], trades: List[Dict[str, str]]) -> None:
    for row in trades:
        note_text = str(row.get("note") or "")
        note = _note_fields(note_text)
        order_id = str(note.get("order_id") or row.get("entry_order_id") or "").strip()
        group, reason, eligibility = _classify_policy_group(note, note_text, order_id)
        rows.append(
            {
                "source": "trades_calc",
                "row_id": row.get("trade_id", ""),
                "ymd": _ymd(row.get("entry_ts", "")),
                "code": str(row.get("code") or "").zfill(6),
                "order_id": order_id,
                "entry_timing": note.get("entry_timing", ""),
                "fallback_stage": note.get("fallback_stage", ""),
                "surge_immediate": note.get("surge_immediate", ""),
                "surge_type": note.get("surge_type", ""),
                "risk_mode": "validation_reduce" if group == "validation_reduce" else "",
                "entry_policy_group": group,
                "entry_policy_path": note.get("entry_timing", "") or note.get("fallback_stage", ""),
                "entry_decision_source": "trade_note",
                "sample_eligibility": eligibility,
                "promotion_candidate": "false",
                "label_reason": reason,
                "net_ret": row.get("net_ret", ""),
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )


def _append_fill_rows(rows: List[Dict[str, Any]], fills: List[Dict[str, str]]) -> None:
    for idx, row in enumerate(fills, start=1):
        if str(row.get("side") or "").strip().upper() != "BUY":
            continue
        note_text = str(row.get("note") or "")
        note = _note_fields(note_text)
        order_id = str(row.get("order_id") or "").strip()
        group, reason, eligibility = _classify_policy_group(note, note_text, order_id)
        rows.append(
            {
                "source": "fills_buy",
                "row_id": idx,
                "ymd": _ymd(row.get("datetime", "")),
                "code": str(row.get("code") or "").zfill(6),
                "order_id": order_id,
                "entry_timing": note.get("entry_timing", ""),
                "fallback_stage": note.get("fallback_stage", ""),
                "surge_immediate": note.get("surge_immediate", ""),
                "surge_type": note.get("surge_type", ""),
                "risk_mode": "validation_reduce" if group == "validation_reduce" else "",
                "entry_policy_group": group,
                "entry_policy_path": note.get("entry_timing", "") or note.get("fallback_stage", ""),
                "entry_decision_source": "fill_note",
                "sample_eligibility": eligibility,
                "promotion_candidate": "false",
                "label_reason": reason,
                "net_ret": "",
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )


def _append_entry_layer_rows(rows: List[Dict[str, Any]], layer_rows: List[Dict[str, str]]) -> None:
    for idx, row in enumerate(layer_rows, start=1):
        text = ";".join(str(row.get(k) or "") for k in row.keys()).lower()
        fields = {
            "surge_type": row.get("surge_shadow_condition", ""),
        }
        group, reason, eligibility = _classify_policy_group(fields, text, str(row.get("order_id") or ""))
        if group == "legacy_unknown" and str(row.get("alpha_layer") or ""):
            group = "normal_entry"
            reason = "entry decision layer alpha candidate without surge/intraday marker"
            eligibility = "runtime_candidate_only"
        rows.append(
            {
                "source": "entry_decision_layers_runtime",
                "row_id": idx,
                "ymd": row.get("d_ref", "") or row.get("entry_day", ""),
                "code": str(row.get("code") or "").zfill(6) if row.get("code") else "",
                "order_id": row.get("order_id", ""),
                "entry_timing": "",
                "fallback_stage": "",
                "surge_immediate": "",
                "surge_type": row.get("surge_shadow_condition", ""),
                "risk_mode": row.get("risk_layer", ""),
                "entry_policy_group": group,
                "entry_policy_path": row.get("execution_layer", ""),
                "entry_decision_source": "entry_decision_layer",
                "sample_eligibility": eligibility,
                "promotion_candidate": "false",
                "label_reason": reason,
                "net_ret": "",
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )


def _append_fallback_whatif_rows(rows: List[Dict[str, Any]], whatif_rows: List[Dict[str, str]]) -> None:
    for idx, row in enumerate(whatif_rows, start=1):
        group = "normal_entry" if str(row.get("positive_like_ok") or "").lower() == "true" else "legacy_unknown"
        eligibility = "whatif_normal_candidate" if group == "normal_entry" else "whatif_not_policy_proof"
        rows.append(
            {
                "source": "entry_fallback_stage_whatif",
                "row_id": idx,
                "ymd": row.get("d_ref", ""),
                "code": str(row.get("code") or "").zfill(6) if row.get("code") else "",
                "order_id": "",
                "entry_timing": "fallback_whatif",
                "fallback_stage": row.get("shadow_state", ""),
                "surge_immediate": "",
                "surge_type": "",
                "risk_mode": "",
                "entry_policy_group": group,
                "entry_policy_path": "fallback_whatif",
                "entry_decision_source": "whatif_report",
                "sample_eligibility": eligibility,
                "promotion_candidate": "false",
                "label_reason": row.get("whatif_block_reason", "") or row.get("positive_like_reason", ""),
                "net_ret": "",
                "trading_effect": "false",
                "policy_effect": "false",
                "policy_change_applied": "false",
            }
        )


def _summary_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_source_group: Counter[Tuple[str, str]] = Counter()
    by_group_returns: Dict[str, List[float]] = defaultdict(list)
    for row in rows:
        source = str(row.get("source") or "")
        group = str(row.get("entry_policy_group") or "")
        by_source_group[(source, group)] += 1
        val = _float(row.get("net_ret"))
        if val is not None:
            by_group_returns[group].append(val)

    out: List[Dict[str, Any]] = []
    for (source, group), count in sorted(by_source_group.items()):
        out.append(
            {
                "kind": "source_group_count",
                "source": source,
                "entry_policy_group": group,
                "n": count,
                "avg": "",
                "median": "",
                "win_rate": "",
                "min": "",
                "max": "",
            }
        )
    for group, values in sorted(by_group_returns.items()):
        out.append(
            {
                "kind": "return_stats",
                "source": "trades_calc",
                "entry_policy_group": group,
                **_stats(values),
            }
        )
    return out


def main() -> int:
    rows: List[Dict[str, Any]] = []
    sources = {
        "fills": str(FILLS_PATH),
        "trades_calc": str(TRADES_CALC_PATH),
        "entry_decision_layers_runtime": str(ENTRY_LAYERS_PATH),
        "entry_fallback_stage_whatif": str(FALLBACK_WHATIF_PATH),
    }

    _append_trade_rows(rows, _read_csv(TRADES_CALC_PATH))
    _append_fill_rows(rows, _read_csv(FILLS_PATH))
    _append_entry_layer_rows(rows, _read_csv(ENTRY_LAYERS_PATH))
    _append_fallback_whatif_rows(rows, _read_csv(FALLBACK_WHATIF_PATH))

    group_counts = Counter(str(row.get("entry_policy_group") or "") for row in rows)
    source_counts = Counter(str(row.get("source") or "") for row in rows)
    unknown_groups = sorted(set(group_counts) - POLICY_GROUPS)
    out = {
        "generated_at": _now_ts(),
        "status": "PASS" if not unknown_groups else "WARN",
        "schema_version": "entry_policy_group_report_v1",
        "source_files": sources,
        "rows": len(rows),
        "source_counts": dict(sorted(source_counts.items())),
        "entry_policy_group_counts": dict(sorted(group_counts.items())),
        "unknown_groups": unknown_groups,
        "policy_groups": sorted(POLICY_GROUPS),
        "trading_effect": False,
        "policy_effect": False,
        "policy_change_applied": False,
        "note": "Read-only normalized policy group labels; does not change entry filtering, risk gates, sizing, orders, fills, ledger, or broker dispatch.",
    }

    fields = [
        "source",
        "row_id",
        "ymd",
        "code",
        "order_id",
        "entry_timing",
        "fallback_stage",
        "surge_immediate",
        "surge_type",
        "risk_mode",
        "entry_policy_group",
        "entry_policy_path",
        "entry_decision_source",
        "sample_eligibility",
        "promotion_candidate",
        "label_reason",
        "net_ret",
        "trading_effect",
        "policy_effect",
        "policy_change_applied",
    ]
    summary_fields = ["kind", "source", "entry_policy_group", "n", "avg", "median", "win_rate", "min", "max"]

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, rows, fields)
    _write_csv(OUT_SUMMARY_CSV, _summary_rows(rows), summary_fields)

    print(f"[FINAL] entry policy group report -> {OUT_JSON} rows={len(rows)} groups={dict(group_counts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
