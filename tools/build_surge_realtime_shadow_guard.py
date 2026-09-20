from __future__ import annotations

import csv
import importlib.util
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path("E:/1_Data")
LOG_DIR = ROOT / "2_Logs"
TRADES_CALC = ROOT / "paper" / "trades_calc.csv"
WHATIF_LATEST = LOG_DIR / "surge_realtime_entry_whatif_latest.json"
OUT_JSON_LATEST = LOG_DIR / "surge_realtime_shadow_guard_latest.json"
OUT_CSV_LATEST = LOG_DIR / "surge_realtime_shadow_guard_latest.csv"
WHATIF_JSON_LATEST = LOG_DIR / "surge_realtime_entry_whatif_latest.json"
WHATIF_CSV_LATEST = LOG_DIR / "surge_realtime_entry_whatif_latest.csv"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _note_value(note: str, key: str) -> str:
    m = re.search(rf"(?:^|[;| ]){re.escape(key)}=([^;| ]+)", str(note or ""))
    return m.group(1).strip() if m else ""


def _note_flag(note: str, key: str) -> bool:
    return _note_value(note, key).lower() in {"1", "true", "yes", "y"}


def _ymd(value: str) -> str:
    return str(value or "")[:10].replace("-", "")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _norm_code(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = re.sub(r"\D", "", text)
    return digits.zfill(6) if digits else text


def _load_order_meta() -> dict[str, dict[str, Any]]:
    meta: dict[str, dict[str, Any]] = {}
    for path in sorted((ROOT / "paper").glob("orders_2026*_exec.xlsx")):
        try:
            df = pd.read_excel(path, dtype=str)
        except Exception:
            continue
        if df.empty:
            continue
        if "side" not in df.columns or "note" not in df.columns:
            continue
        buys = df[df["side"].astype(str).str.upper().str.strip().eq("BUY")].copy()
        for _, row in buys.iterrows():
            note = str(row.get("note") or "")
            keys = {
                str(row.get("order_id") or "").strip(),
                _note_value(note, "order_id"),
                _note_value(note, "entry_order_id"),
                _note_value(note, "source_order_id"),
            }
            keys = {k for k in keys if k}
            if not keys:
                continue
            item = {
                "source_path": str(path),
                "order_note": note,
                "code": _norm_code(row.get("code")),
                "signal_date": _ymd(str(row.get("signal_date") or _note_value(note, "signal_date"))),
                "surge_immediate": _note_flag(note, "surge_immediate"),
                "surge_type": _note_value(note, "surge_type"),
                "entry_timing": _note_value(note, "entry_timing"),
                "fallback_stage": _note_value(note, "fallback_stage"),
            }
            for key in keys:
                meta.setdefault(key, item)
    return meta


def _merge_meta(note: str, order_id: str, order_meta: dict[str, dict[str, Any]]) -> dict[str, Any]:
    linked = order_meta.get(str(order_id or "").strip()) or {}
    out: dict[str, Any] = {
        "metadata_source": "note",
        "orders_exec_source_path": "",
        "metadata_missing_fields": [],
    }
    fields = {
        "surge_immediate": _note_flag(note, "surge_immediate"),
        "surge_type": _note_value(note, "surge_type"),
        "entry_timing": _note_value(note, "entry_timing"),
        "fallback_stage": _note_value(note, "fallback_stage"),
    }
    for key, value in list(fields.items()):
        missing = (value is False) if key == "surge_immediate" else (str(value or "").strip() == "")
        linked_value = linked.get(key)
        if missing and linked_value not in (None, "", False):
            fields[key] = linked_value
            out["metadata_source"] = "orders_exec"
            out["orders_exec_source_path"] = str(linked.get("source_path") or "")
    for key, value in fields.items():
        missing = (value is False) if key == "surge_immediate" else (str(value or "").strip() == "")
        if missing:
            out["metadata_missing_fields"].append(key)
    if out["metadata_missing_fields"] and not linked:
        out["metadata_source"] = "missing"
    out.update(fields)
    return out


def _load_rows() -> list[dict[str, Any]]:
    spec = importlib.util.spec_from_file_location("paper_pnl_report", str(ROOT / "paper_pnl_report.py"))
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load paper_pnl_report.py")
    pnl = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pnl)

    df_raw = pnl._ensure_exit_date(pnl._load_trades(TRADES_CALC))
    df_scope, _op_scope = pnl._apply_operational_scope(df_raw)
    df_blocked, _blocked_filter = pnl._filter_blocked_entry_trades(df_scope, ROOT / "paper")
    order_meta = _load_order_meta()

    rows: list[dict[str, Any]] = []
    for _, row in df_blocked.iterrows():
        exit_date = _ymd(str(row.get("exit_date") or row.get("exit_ts") or ""))
        if not ("20260301" <= exit_date <= "20260526"):
            continue
        note = str(row.get("note") or "")
        order_id = _note_value(note, "order_id")
        merged = _merge_meta(note, order_id, order_meta)
        net_ret = _to_float(row.get("net_ret", row.get("pnl_pct", 0.0)))
        qty = _to_float(row.get("qty"))
        entry_price = _to_float(row.get("entry_price"))
        rows.append(
            {
                "exit_date": exit_date,
                "code": str(row.get("code") or "").zfill(6),
                "net_ret": net_ret,
                "weight": qty * entry_price,
                "signal_date": _note_value(note, "signal_date"),
                "order_id": order_id,
                "surge_immediate": bool(merged["surge_immediate"]),
                "surge_type": str(merged["surge_type"] or ""),
                "entry_timing": str(merged["entry_timing"] or ""),
                "fallback_stage": str(merged["fallback_stage"] or ""),
                "metadata_source": str(merged["metadata_source"]),
                "orders_exec_source_path": str(merged["orders_exec_source_path"]),
                "metadata_missing_fields": list(merged["metadata_missing_fields"]),
                "note": note,
            }
            )
    return rows


def _metadata_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    source_counts: dict[str, int] = {}
    missing_counts: dict[str, int] = {}
    for row in rows:
        source = str(row.get("metadata_source") or "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1
        for key in row.get("metadata_missing_fields") or []:
            missing_counts[str(key)] = missing_counts.get(str(key), 0) + 1
    return {
        "rows": int(len(rows)),
        "metadata_source_counts": source_counts,
        "metadata_missing_field_counts": missing_counts,
    }


def _day_returns(rows: list[dict[str, Any]]) -> list[tuple[str, float]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["exit_date"]), []).append(row)
    out: list[tuple[str, float]] = []
    for day in sorted(grouped):
        part = grouped[day]
        wsum = sum(float(x.get("weight") or 0.0) for x in part)
        if wsum > 0:
            ret = sum(float(x["net_ret"]) * float(x.get("weight") or 0.0) for x in part) / wsum
        else:
            ret = sum(float(x["net_ret"]) for x in part) / max(len(part), 1)
        out.append((day, float(ret)))
    return out


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"rows": 0, "days": 0, "comp_ret": 0.0, "max_mdd": 0.0, "end_dd": 0.0}
    equity = 1.0
    peak = 1.0
    max_mdd = 0.0
    end_dd = 0.0
    for _day, ret in _day_returns(rows):
        equity *= 1.0 + ret
        peak = max(peak, equity)
        end_dd = equity / peak - 1.0
        max_mdd = min(max_mdd, end_dd)
    return {
        "rows": int(len(rows)),
        "days": int(len(_day_returns(rows))),
        "comp_ret": float(equity - 1.0),
        "max_mdd": float(max_mdd),
        "end_dd": float(end_dd),
    }


def _candidate_rows(rows: list[dict[str, Any]], condition: str) -> list[dict[str, Any]]:
    if condition == "surge_immediate":
        return [r for r in rows if bool(r["surge_immediate"])]
    if condition == "intraday_realtime":
        return [r for r in rows if str(r["entry_timing"]) == "intraday_realtime"]
    if condition == "fallback_stage_0_intraday":
        return [r for r in rows if str(r["fallback_stage"]) == "0(intraday_realtime)"]
    if condition == "price_vol_breakout":
        return [r for r in rows if str(r["surge_type"]) == "PRICE_VOL_BREAKOUT"]
    if condition == "price_range_breakout":
        return [r for r in rows if str(r["surge_type"]) == "PRICE_RANGE_BREAKOUT"]
    return []


def _filter_scenario(rows: list[dict[str, Any]], scenario: str) -> list[dict[str, Any]]:
    if scenario == "baseline_blocked_filter_only":
        return list(rows)
    if scenario == "exclude_surge_immediate":
        return [r for r in rows if not bool(r["surge_immediate"])]
    if scenario == "exclude_intraday_realtime":
        return [r for r in rows if str(r["entry_timing"]) != "intraday_realtime"]
    if scenario == "exclude_price_vol_break":
        return [r for r in rows if str(r["surge_type"]) != "PRICE_VOL_BREAKOUT"]
    if scenario == "exclude_price_range_break":
        return [r for r in rows if str(r["surge_type"]) != "PRICE_RANGE_BREAKOUT"]
    if scenario == "exclude_all_surge_type":
        return [r for r in rows if not str(r["surge_type"]).strip()]
    if scenario == "dedupe_order_id_keep_first":
        seen: set[tuple[str, str]] = set()
        out: list[dict[str, Any]] = []
        for row in sorted(rows, key=lambda r: (str(r["exit_date"]), str(r["order_id"]))):
            key = (str(row["exit_date"]), str(row["order_id"]))
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
        return out
    if scenario == "exclude_loss_cluster_0513_0515":
        return [r for r in rows if str(r["signal_date"]) not in {"20260513", "20260514", "20260515"}]
    if scenario == "exclude_loss_cluster_0519_0521":
        return [r for r in rows if str(r["signal_date"]) not in {"20260519", "20260520", "20260521"}]
    if scenario == "exclude_both_loss_clusters":
        return [
            r
            for r in rows
            if str(r["signal_date"]) not in {"20260513", "20260514", "20260515", "20260519", "20260520", "20260521"}
        ]
    return list(rows)


def _between(rows: list[dict[str, Any]], start: str, end: str) -> list[dict[str, Any]]:
    return [r for r in rows if start <= str(r["exit_date"]) <= end]


def _scenario_table(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    names = [
        "baseline_blocked_filter_only",
        "exclude_surge_immediate",
        "exclude_intraday_realtime",
        "exclude_price_vol_break",
        "exclude_price_range_break",
        "exclude_all_surge_type",
        "dedupe_order_id_keep_first",
        "exclude_loss_cluster_0513_0515",
        "exclude_loss_cluster_0519_0521",
        "exclude_both_loss_clusters",
    ]
    baseline_rows = _filter_scenario(rows, "baseline_blocked_filter_only")
    baseline = _metrics(baseline_rows)
    out: list[dict[str, Any]] = []
    for name in names:
        kept = _filter_scenario(rows, name)
        m = _metrics(kept)
        may = _metrics(_between(kept, "20260501", "20260526"))
        c1 = _metrics(_between(kept, "20260513", "20260515"))
        c2 = _metrics(_between(kept, "20260519", "20260521"))
        rec = {
            "scenario": name,
            "removed_rows": int(len(baseline_rows) - len(kept)),
            "rows": int(m["rows"]),
            "days": int(m["days"]),
            "global_comp_ret": float(m["comp_ret"]),
            "global_max_mdd": float(m["max_mdd"]),
            "global_end_dd": float(m["end_dd"]),
            "may_comp_ret": float(may["comp_ret"]),
            "may_max_mdd": float(may["max_mdd"]),
            "cluster_0513_0515_ret": float(c1["comp_ret"]),
            "cluster_0513_0515_mdd": float(c1["max_mdd"]),
            "cluster_0519_0521_ret": float(c2["comp_ret"]),
            "cluster_0519_0521_mdd": float(c2["max_mdd"]),
        }
        rec["max_mdd_improvement_vs_base"] = float(rec["global_max_mdd"] - baseline["max_mdd"])
        rec["may_ret_delta_vs_base"] = float(rec["may_comp_ret"] - _metrics(_between(baseline_rows, "20260501", "20260526"))["comp_ret"])
        out.append(rec)
    return out


def _group(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(key) or ""), []).append(row)
    out: list[dict[str, Any]] = []
    for value, part in grouped.items():
        rets = [float(r["net_ret"]) for r in part]
        out.append(
            {
                "key": value,
                "rows": int(len(part)),
                "avg_ret": float(sum(rets) / max(len(rets), 1)),
                "sum_ret": float(sum(rets)),
                "neg_rows": int(sum(1 for x in rets if x < 0.0)),
                "worst_ret": float(min(rets) if rets else 0.0),
            }
        )
    return sorted(out, key=lambda r: float(r["sum_ret"]))


def _write_whatif(rows: list[dict[str, Any]]) -> dict[str, Any]:
    loss_cluster = [
        r
        for r in rows
        if ("20260513" <= str(r["exit_date"]) <= "20260515") or ("20260519" <= str(r["exit_date"]) <= "20260521")
    ]
    worst = sorted(loss_cluster, key=lambda r: float(r["net_ret"]))[:30]
    payload = {
        "generated_at": _now(),
        "scope": "read_only_what_if_no_policy_change",
        "sources": {
            "trades_calc": str(TRADES_CALC),
            "orders_exec_join": "enabled",
            "metadata_resolution": "note first, orders_exec by order_id/entry_order_id fallback, missing counted",
        },
        "metadata_summary": _metadata_summary(rows),
        "scenario_rows": _scenario_table(rows),
        "loss_cluster_groups": {
            "by_signal_date": _group(loss_cluster, "signal_date"),
            "by_surge_type": _group(loss_cluster, "surge_type"),
            "by_entry_timing": _group(loss_cluster, "entry_timing"),
            "by_fallback_stage": _group(loss_cluster, "fallback_stage"),
            "by_metadata_source": _group(loss_cluster, "metadata_source"),
        },
        "worst_loss_rows": [
            {
                "exit_date": str(r["exit_date"]),
                "code": str(r["code"]),
                "net_ret": float(r["net_ret"]),
                "signal_date": str(r["signal_date"]),
                "surge_type": str(r["surge_type"]),
                "entry_timing": str(r["entry_timing"]),
                "order_id": str(r["order_id"]),
                "metadata_source": str(r["metadata_source"]),
                "metadata_missing_fields": list(r.get("metadata_missing_fields") or []),
                "note": str(r["note"])[:220],
            }
            for r in worst
        ],
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / f"surge_realtime_entry_whatif_{stamp}.json"
    out_csv = LOG_DIR / f"surge_realtime_entry_whatif_{stamp}.csv"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    WHATIF_JSON_LATEST.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    scenario_rows = payload["scenario_rows"]
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(scenario_rows[0].keys()))
        writer.writeheader()
        writer.writerows(scenario_rows)
    with WHATIF_CSV_LATEST.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(scenario_rows[0].keys()))
        writer.writeheader()
        writer.writerows(scenario_rows)
    payload["written_paths"] = {"json": str(out_json), "csv": str(out_csv)}
    return payload


def _evaluate_guard(rows: list[dict[str, Any]], condition: str, mode: str) -> dict[str, Any]:
    target_ids = {id(r) for r in _candidate_rows(rows, condition)}
    if mode == "shadow_block":
        kept = [r for r in rows if id(r) not in target_ids]
    elif mode == "shadow_reduce_50":
        kept = []
        for row in rows:
            row2 = dict(row)
            if id(row) in target_ids:
                row2["weight"] = float(row2.get("weight") or 0.0) * 0.5
            kept.append(row2)
    else:
        kept = list(rows)
    m = _metrics(kept)
    affected = [r for r in rows if id(r) in target_ids]
    avg_ret = sum(float(r["net_ret"]) for r in affected) / max(len(affected), 1)
    neg_rows = sum(1 for r in affected if float(r["net_ret"]) < 0.0)
    return {
        "condition": condition,
        "mode": mode,
        "affected_rows": int(len(affected)),
        "affected_avg_ret": float(avg_ret),
        "affected_negative_rows": int(neg_rows),
        "rows_after": m["rows"],
        "global_comp_ret_after": m["comp_ret"],
        "global_max_mdd_after": m["max_mdd"],
        "global_end_dd_after": m["end_dd"],
    }


def main() -> int:
    rows = _load_rows()
    generated_whatif = _write_whatif(rows)
    baseline = _metrics(rows)
    whatif = _load_json(WHATIF_LATEST) or generated_whatif
    scenario_by_name = {
        str(x.get("scenario")): x
        for x in (whatif.get("scenario_rows") or [])
        if isinstance(x, dict)
    }
    baseline_src = scenario_by_name.get("baseline_blocked_filter_only")
    if isinstance(baseline_src, dict):
        baseline = {
            "rows": int(baseline_src.get("rows") or 0),
            "days": int(baseline_src.get("days") or 0),
            "comp_ret": float(baseline_src.get("global_comp_ret") or 0.0),
            "max_mdd": float(baseline_src.get("global_max_mdd") or 0.0),
            "end_dd": float(baseline_src.get("global_end_dd") or 0.0),
        }

    scenario_map = {
        "surge_immediate": "exclude_surge_immediate",
        "intraday_realtime": "exclude_intraday_realtime",
        "fallback_stage_0_intraday": "exclude_intraday_realtime",
        "price_vol_breakout": None,
        "price_range_breakout": None,
    }
    conditions = list(scenario_map.keys())
    modes = ["shadow_block", "shadow_reduce_50"]
    guard_rows: list[dict[str, Any]] = []
    for condition in conditions:
        for mode in modes:
            rec = _evaluate_guard(rows, condition, mode)
            scenario_name = scenario_map.get(condition) if mode == "shadow_block" else None
            scenario = scenario_by_name.get(str(scenario_name)) if scenario_name else None
            if isinstance(scenario, dict):
                rec["affected_rows"] = int(scenario.get("removed_rows") or rec["affected_rows"])
                rec["rows_after"] = int(scenario.get("rows") or rec["rows_after"])
                rec["global_comp_ret_after"] = float(scenario.get("global_comp_ret") or rec["global_comp_ret_after"])
                rec["global_max_mdd_after"] = float(scenario.get("global_max_mdd") or rec["global_max_mdd_after"])
                rec["global_end_dd_after"] = float(scenario.get("global_end_dd") or rec["global_end_dd_after"])
                rec["may_comp_ret_after"] = float(scenario.get("may_comp_ret") or 0.0)
                rec["may_max_mdd_after"] = float(scenario.get("may_max_mdd") or 0.0)
                rec["source_scenario"] = scenario_name
            else:
                rec["source_scenario"] = "direct_shadow_recalc"
            rec["global_max_mdd_improvement"] = float(rec["global_max_mdd_after"] - baseline["max_mdd"])
            rec["global_comp_ret_delta"] = float(rec["global_comp_ret_after"] - baseline["comp_ret"])
            rec["deployable_now"] = False
            rec["recommended_status"] = "SHADOW_ONLY"
            guard_rows.append(rec)

    guard_rows.sort(key=lambda r: (r["global_max_mdd_improvement"], r["global_comp_ret_delta"]), reverse=True)
    best = guard_rows[0] if guard_rows else {}
    payload = {
        "generated_at": _now(),
        "scope": "shadow_only_guard_candidate; no policy/config/runtime behavior change",
        "inputs": {
            "trades_calc": str(TRADES_CALC),
            "whatif_latest": str(WHATIF_LATEST),
        },
        "baseline": baseline,
        "basis": "DDM/P0 blocked-entry-filter basis from surge_realtime_entry_whatif_latest.json",
        "whatif_generated_at": whatif.get("generated_at"),
        "metadata_summary": _metadata_summary(rows),
        "guard_candidates": guard_rows,
        "recommendation": {
            "status": "SHADOW_ONLY",
            "primary_condition": best.get("condition"),
            "primary_mode": best.get("mode"),
            "reason": (
                "The best candidate improves MDD materially in the realized May loss window, "
                "but it must be logged shadow-only before operating policy is changed."
            ),
            "do_not_change": [
                "DDM stage thresholds",
                "risk_orchestration dd_stop",
                "production risk HARD action",
                "entry score or rank score",
            ],
            "next_runtime_check": [
                "count current candidates matching the primary condition",
                "record whether they would be held, reduced, or allowed under shadow mode",
                "compare later realized return before any operating policy apply",
            ],
        },
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = LOG_DIR / f"surge_realtime_shadow_guard_{stamp}.json"
    out_csv = LOG_DIR / f"surge_realtime_shadow_guard_{stamp}.csv"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_JSON_LATEST.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(guard_rows[0].keys()))
        writer.writeheader()
        writer.writerows(guard_rows)
    with OUT_CSV_LATEST.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(guard_rows[0].keys()))
        writer.writeheader()
        writer.writerows(guard_rows)
    print(json.dumps({"status": "PASS", "json": str(out_json), "csv": str(out_csv), "primary": payload["recommendation"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
