from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CONFIG_JSON = ROOT / "paper" / "paper_engine_config.json"

SOURCE_CSV = LOG_DIR / "surge_realtime_latest.csv"
SOURCE_JSON = LOG_DIR / "surge_realtime_latest.json"
OUT_JSON = LOG_DIR / "surge_active_response_layer_latest.json"
OUT_CSV = LOG_DIR / "surge_active_response_layer_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 5:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        return list(csv.DictReader(fp))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size <= 0:
        return {}
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except UnicodeDecodeError:
            continue
        except json.JSONDecodeError:
            return {}
    return {}


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").replace(",", "").strip()
        return float(text) if text else float(default)
    except Exception:
        return float(default)


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _detected(row: dict[str, Any]) -> bool:
    return str(row.get("detected_surge_type") or "NONE").strip() != "NONE"


def _has_reason(row: dict[str, Any], key: str) -> bool:
    text = str(row.get("exclude_reasons") or "")
    return key in text


def _microstructure(row: dict[str, Any]) -> tuple[bool, bool]:
    spread = _float(row.get("spread_bps"))
    markout = _float(row.get("markout_1step_bps"))
    spread_bad = spread > 40.0 if spread else False
    spread_severe = spread > 80.0 if spread else False
    markout_bad = markout <= -5.0 if markout else False
    markout_severe = markout <= -150.0 if markout else False
    return spread_bad or markout_bad, spread_severe or markout_severe


def _wait_reclaim_probe_cfg() -> dict[str, Any]:
    cfg = _read_json(CONFIG_JSON)
    policy = cfg.get("surge_entry_policy", {}) if isinstance(cfg.get("surge_entry_policy"), dict) else {}
    probe = policy.get("wait_reclaim_paper_probe", {}) if isinstance(policy.get("wait_reclaim_paper_probe"), dict) else {}
    return probe


def _wait_reclaim_probe_evidence(row: dict[str, Any], cfg: dict[str, Any]) -> tuple[bool, str]:
    if not bool(cfg.get("enabled", False)):
        return False, "wait_reclaim_probe_disabled"
    entry_decision = str(row.get("entry_decision") or "").strip().upper()
    entry_allowed = _bool(row.get("entry_allowed")) or entry_decision == "ENTRY_ALLOWED"
    entry_blocked = _bool(row.get("entry_blocked")) or _bool(row.get("excluded_by_policy")) or entry_decision == "ENTRY_BLOCKED"
    if not entry_allowed or entry_blocked:
        return False, f"entry_not_allowed:{entry_decision or 'unknown'}"
    exclude = str(row.get("exclude_reasons") or "").strip()
    if exclude:
        return False, f"exclude_reasons:{exclude}"

    change = _float(row.get("change_pct"))
    rvol20 = _float(row.get("rvol20"))
    trading_value = _float(row.get("trading_value"))
    spread = _float(row.get("spread_bps"))
    markout = _float(row.get("markout_1step_bps"))
    score = _float(row.get("surge_score_final"), _float(row.get("surge_score")))
    range_pos = _float(row.get("intraday_range_position_pct"), -1.0)
    high_drawdown = _float(row.get("intraday_high_drawdown_pct"), 1.0)
    lob_ok = str(row.get("lob_status") or "").strip().upper() == "OK" or _bool(row.get("lob_available"))
    orderflow_tag = str(row.get("orderflow_tag") or "").strip().upper()

    min_change = _float(cfg.get("min_change_pct"), 0.05)
    max_change = _float(cfg.get("max_gap_pct"), 0.30)
    min_value = _float(cfg.get("min_trading_value_krw"), 1_000_000_000.0)
    max_rvol = _float(cfg.get("max_rvol20"), 30.0)
    max_spread = _float(cfg.get("max_spread_bps"), 25.0)
    min_markout = _float(cfg.get("min_markout_1step_bps"), 0.0)
    min_score = _float(cfg.get("min_score_final"), 78.0)
    min_range_pos = _float(cfg.get("min_intraday_range_position_pct"), 0.95)
    max_high_dd = _float(cfg.get("max_intraday_high_drawdown_pct"), 0.005)
    require_lob = bool(cfg.get("require_lob_ok", True))
    require_orderflow = bool(cfg.get("require_orderflow_ok", False))
    allow_no_history = bool(cfg.get("allow_orderflow_no_history", True))

    if not (min_change <= change <= max_change):
        return False, f"change={change:.4f}_outside_{min_change:.4f}_{max_change:.4f}"
    if trading_value < min_value:
        return False, f"trading_value={trading_value:.0f}<{min_value:.0f}"
    if rvol20 > max_rvol:
        return False, f"rvol20={rvol20:.4f}>{max_rvol:.4f}"
    if require_lob and not lob_ok:
        return False, "lob_not_ok"
    if max_spread > 0 and spread > max_spread:
        return False, f"spread={spread:.2f}>{max_spread:.2f}"
    if markout < min_markout:
        return False, f"markout={markout:.2f}<{min_markout:.2f}"
    if score < min_score:
        return False, f"score={score:.2f}<{min_score:.2f}"
    orderflow_ok = orderflow_tag == "OK" or (allow_no_history and orderflow_tag == "NO_HISTORY")
    if require_orderflow and not orderflow_ok:
        return False, f"orderflow={orderflow_tag or '-'}"
    reclaim_ok = range_pos >= min_range_pos or high_drawdown <= max_high_dd
    if not reclaim_ok:
        return False, f"no_reclaim:range_pos={range_pos:.4f},high_dd={high_drawdown:.4f}"
    return True, (
        f"range_pos={range_pos:.4f};high_dd={high_drawdown:.4f};"
        f"lob=OK;spread={spread:.2f};markout={markout:.2f};orderflow={orderflow_tag or '-'}"
    )


def _cap_wait_reclaim_probe_rows(rows: list[dict[str, Any]], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = [row for row in rows if row.get("reclaim_probe_transition") == "WAIT_RECLAIM_TO_PROBE_READY"]
    if not candidates:
        return rows
    max_selected = max(0, int(_float(cfg.get("max_selected"), 1)))
    ranked = sorted(
        candidates,
        key=lambda row: (
            -_float(row.get("surge_score_final"), _float(row.get("surge_score"))),
            -_float(row.get("trading_value")),
            _float(row.get("change_pct")),
            str(row.get("code") or ""),
        ),
    )
    selected_ids = {id(row) for row in ranked[:max_selected]}
    for row in candidates:
        if id(row) in selected_ids:
            continue
        row["active_response_label"] = "WAIT_RECLAIM"
        row["active_response_reason"] = "RECLAIM_CONFIRMED_CAP_WAIT"
        row["active_response_next_check"] = "paper_probe_slot_wait"
        row["reclaim_probe_transition"] = ""
        row["reclaim_check_status"] = "RECLAIM_CONFIRMED_CAP_WAIT"
        row["paper_probe_allowed"] = False
    return rows


def _active_response(row: dict[str, Any], wait_reclaim_cfg: dict[str, Any]) -> tuple[str, str, str]:
    change = _float(row.get("change_pct"))
    rvol20 = _float(row.get("rvol20"))
    trading_value = _float(row.get("trading_value"))
    lob_ok = str(row.get("lob_status") or "") == "OK" or _bool(row.get("lob_available"))
    orderflow_ok = str(row.get("orderflow_tag") or "").strip().upper() == "OK"
    entry_allowed = _bool(row.get("entry_allowed"))
    entry_blocked = _bool(row.get("entry_blocked")) or _bool(row.get("excluded_by_policy"))
    value_ok = trading_value >= 1_000_000_000.0
    high_rejection = _has_reason(row, "HIGH_REJECTION_ENTRY_BLOCK")
    entry_change = _has_reason(row, "ENTRY_CHANGE_BLOCK")
    atr_cap = _has_reason(row, "ENTRY_ATR_CAP")
    krx_warning = _has_reason(row, "KRX_WARNING")
    rvol_overheat = _has_reason(row, "RVOL_OVERHEAT_BLOCK") or _has_reason(row, "SCORE_RVOL_OVERHEAT_BLOCK")
    paper_probe_allowed = _bool(row.get("paper_probe_allowed"))
    paper_probe_relax = "PAPER_PROBE" in str(row.get("paper_policy_relaxations") or "")
    micro_bad, micro_severe = _microstructure(row)

    if krx_warning:
        return "HARD_EXCLUDE", "KRX_WARNING", "risk_warning"
    if not value_ok:
        return "HARD_EXCLUDE", "TRADING_VALUE_LT_1B", "insufficient_value"
    if micro_severe:
        return "HARD_EXCLUDE", "SEVERE_MICROSTRUCTURE_RISK", "spread_or_markout_severe"

    if entry_blocked and not paper_probe_allowed:
        if high_rejection or entry_change or atr_cap or rvol_overheat or change > 0.16:
            return "WAIT_RECLAIM", "ENTRY_BLOCKED_RECLAIM_REQUIRED", "vwap_or_high_reclaim_required"
        return "HARD_EXCLUDE", "ENTRY_BLOCKED", "entry_policy_blocked"

    if (
        entry_allowed
        and lob_ok
        and orderflow_ok
        and 0.05 <= change <= 0.16
        and 2.0 <= rvol20 <= 5.0
        and not high_rejection
        and not micro_bad
    ):
        return "ACTIVE_ENTRY_READY", "ACTIVE_CONDITIONS_MET", "immediate_active_review"

    if paper_probe_allowed and paper_probe_relax and lob_ok and value_ok and not micro_severe:
        return "PROBE_READY", "PAPER_PROBE_ALLOWED", "paper_probe_order_review"

    if (
        lob_ok
        and 0.05 <= change <= 0.16
        and 1.2 <= rvol20 <= 8.0
        and not high_rejection
        and not micro_severe
    ):
        return "PROBE_READY", "PROBE_CONDITIONS_MET", "small_probe_review"

    reclaim_ok, reclaim_evidence = _wait_reclaim_probe_evidence(row, wait_reclaim_cfg)
    if reclaim_ok:
        return "PROBE_READY", "RECLAIM_CONFIRMED_PAPER_PROBE", reclaim_evidence

    if high_rejection or entry_change or atr_cap or change > 0.16:
        return "WAIT_RECLAIM", "RECLAIM_REQUIRED", "vwap_or_high_reclaim_required"

    if not lob_ok and value_ok and rvol20 <= 5.0 and not micro_severe:
        return "WAIT_LOB", "LOB_MISSING_RECHECK", "lob_then_recheck"

    return "HARD_EXCLUDE", "ACTIVE_EDGE_NOT_PRESENT", "no_active_response_edge"


def _fields(rows: list[dict[str, Any]]) -> list[str]:
    fields: list[str] = []
    preferred = [
        "ts",
        "date",
        "code",
        "detected_surge_type",
        "active_response_label",
        "active_response_reason",
        "active_response_next_check",
        "entry_decision",
        "entry_reason",
        "entry_allowed",
        "entry_blocked",
        "change_pct",
        "rvol20",
        "trading_value",
        "lob_status",
        "lob_available",
        "spread_bps",
        "markout_1step_bps",
        "exclude_reasons",
    ]
    for key in preferred:
        if any(key in row for row in rows):
            fields.append(key)
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    return fields


def build() -> dict[str, Any]:
    source_rows = _read_csv(SOURCE_CSV)
    source_meta = _read_json(SOURCE_JSON)
    if not source_rows:
        payload = {
            "status": "FAIL",
            "reason": "SOURCE_MISSING_OR_EMPTY",
            "generated_at": _now_ts(),
            "source": str(SOURCE_CSV),
            "research_only": True,
            "policy_change": False,
            "entry_approval_changed": False,
            "paper_order_route": False,
            "broker_order_route": False,
            "trading_route": False,
        }
        _write_json(OUT_JSON, payload)
        return payload

    wait_reclaim_cfg = _wait_reclaim_probe_cfg()
    out_rows: list[dict[str, Any]] = []
    for row in source_rows:
        if not _detected(row):
            continue
        out = dict(row)
        label, reason, next_check = _active_response(row, wait_reclaim_cfg)
        out["active_response_label"] = label
        out["active_response_reason"] = reason
        out["active_response_next_check"] = next_check
        if reason == "RECLAIM_CONFIRMED_PAPER_PROBE":
            out["active_response_original_label"] = "WAIT_RECLAIM"
            out["reclaim_probe_transition"] = "WAIT_RECLAIM_TO_PROBE_READY"
            out["reclaim_check_status"] = "RECLAIM_CONFIRMED"
            out["reclaim_evidence"] = next_check
            out["paper_probe_allowed"] = True
        out["research_only"] = True
        out["policy_change"] = False
        out["entry_approval_changed"] = False
        out["paper_order_route"] = False
        out["broker_order_route"] = False
        out["trading_route"] = False
        out_rows.append(out)

    out_rows = _cap_wait_reclaim_probe_rows(out_rows, wait_reclaim_cfg)
    fields = _fields(out_rows)
    if not fields:
        fields = ["ts", "date", "code", "active_response_label"]
    _write_csv(OUT_CSV, out_rows, fields)

    payload = {
        "status": "PASS",
        "reason": "ok",
        "generated_at": _now_ts(),
        "source_ts": source_meta.get("ts") or (source_rows[0].get("ts") if source_rows else ""),
        "scope": "read_only_surge_active_response_layer",
        "research_only": True,
        "policy_change": False,
        "entry_approval_changed": False,
        "paper_order_route": False,
        "broker_order_route": False,
        "trading_route": False,
        "source_files": {
            "surge_realtime_csv": str(SOURCE_CSV),
            "surge_realtime_json": str(SOURCE_JSON),
        },
        "outputs": {"json": str(OUT_JSON), "csv": str(OUT_CSV)},
        "source_counts": {
            "source_rows": len(source_rows),
            "detected_rows": len(out_rows),
            "source_detected_count": source_meta.get("detected_count", ""),
        },
        "active_response_label_counts": dict(Counter(row.get("active_response_label") for row in out_rows)),
        "reclaim_probe_transition_counts": dict(Counter(row.get("reclaim_probe_transition") for row in out_rows if row.get("reclaim_probe_transition"))),
        "production_entry_decision_counts": dict(Counter(row.get("entry_decision") for row in out_rows)),
        "production_entry_reason_counts": dict(Counter(row.get("entry_reason") for row in out_rows)),
        "rows": out_rows[:50],
        "access_issues": [
            "read-only active response layer only; does not approve orders",
            "WAIT_LOB requires later LOB availability before any entry review",
            "WAIT_RECLAIM requires VWAP or high reclaim evidence before any entry review",
            "production Gate, order, fill, ledger, score, and threshold behavior is unchanged",
        ],
    }
    _write_json(OUT_JSON, payload)
    return payload


def main() -> int:
    payload = build()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())




