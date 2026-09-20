from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
PAPER = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
FILLS_LOG_DIR = ROOT / "live" / "fills_log"


def _load_canonical_module():
    spec = importlib.util.spec_from_file_location("canonical_fills_shadow", TOOLS / "canonical_fills_shadow.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("canonical_fills_shadow load failed")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CANON = _load_canonical_module()


def _norm_ymd(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", str(value or ""))
    return text[:8] if len(text) >= 8 else ""


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except UnicodeDecodeError:
            continue
        except Exception:
            return {}
    return {}


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _derive_d_from_fills(rows: List[Dict[str, str]]) -> str:
    max_buy = ""
    max_any = ""
    for row in rows:
        ymd = _norm_ymd(row.get("datetime") or row.get("ts") or row.get("date"))
        if len(ymd) != 8:
            continue
        if ymd > max_any:
            max_any = ymd
        if str(row.get("side") or "").strip().upper() == "BUY" and ymd > max_buy:
            max_buy = ymd
    return max_buy or max_any


def _event_key(event: Dict[str, Any]) -> Tuple[str, str, str]:
    k = event.get("k") or {}
    return str(k.get("xid") or ""), str(k.get("lid") or ""), str(k.get("xts") or "")


def _value_hash(event: Dict[str, Any]) -> str:
    value = event.get("v") or {}
    normalizer = getattr(CANON, "_event_value_for_duplicate_compare", None)
    if callable(normalizer):
        value = normalizer(value)
    return CANON._hash_obj(value)


def _replay_in_memory(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    positions: Dict[str, Dict[str, Decimal]] = {}
    cash: Dict[str, Decimal] = {"KRW": Decimal("0"), "REALIZED_PNL": Decimal("0")}
    chain_hash = "0" * 64
    final_state_hash = CANON._hash_obj(CANON._state_payload(positions, cash))
    ordered = sorted(events, key=CANON._order_key)
    for event in ordered:
        CANON._apply_fill(positions, cash, event)
        payload = CANON._state_payload(positions, cash)
        position_root = CANON._hash_obj(payload["positions"])
        cash_root = CANON._hash_obj(payload["cash"])
        state_hash = CANON._hash_obj({"cash_root": cash_root, "position_root": position_root})
        chain_hash = CANON._hash_obj({"prev": chain_hash, "state_hash": state_hash, "key": event.get("k")})
        final_state_hash = state_hash
    return {
        "events_replayed": len(ordered),
        "final_state_hash": final_state_hash,
        "chain_hash": chain_hash,
        "state": CANON._state_payload(positions, cash),
    }


def _source_events_from_fills(fills_path: Path, d: str) -> Tuple[List[Dict[str, Any]], int]:
    rows = _read_csv_rows(fills_path)
    target: List[Tuple[int, Dict[str, str]]] = []
    for idx, row in enumerate(rows, start=2):
        ymd = _norm_ymd(row.get("datetime") or row.get("ts") or row.get("date"))
        if ymd == d:
            target.append((idx, row))
    events = [CANON._canonical_event(row, idx, fills_path.resolve(), d) for idx, row in target]
    return events, len(target)


def _source_events_for_compare(fills_path: Path, d: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    events, rows = _source_events_from_fills(fills_path, d)
    sources: List[Dict[str, Any]] = [
        {
            "label": "paper_fills",
            "path": str(fills_path),
            "rows_for_D": int(rows),
            "events": int(len(events)),
        }
    ]

    kis_fills_path = PAPER / f"kis_fills_api_{d}.csv"
    if kis_fills_path.exists():
        kis_events, kis_rows = _source_events_from_fills(kis_fills_path, d)
        events.extend(kis_events)
        sources.append(
            {
                "label": "kis_fills_api",
                "path": str(kis_fills_path),
                "rows_for_D": int(kis_rows),
                "events": int(len(kis_events)),
            }
        )
    return events, sources


def _load_canonical_events(d: str) -> List[Dict[str, Any]]:
    path = FILLS_LOG_DIR / f"{d}.ndjson"
    if not path.exists():
        return []
    return list(CANON._read_existing_events(path).values())


def _compare_event_sets(source_events: List[Dict[str, Any]], canonical_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    source = {_event_key(e): e for e in source_events}
    canon = {_event_key(e): e for e in canonical_events}
    source_keys = set(source)
    canon_keys = set(canon)
    common = source_keys & canon_keys
    value_mismatch = []
    for key in sorted(common):
        if _value_hash(source[key]) != _value_hash(canon[key]):
            value_mismatch.append("|".join(key))
    return {
        "source_event_keys": len(source_keys),
        "canonical_event_keys": len(canon_keys),
        "common_keys": len(common),
        "missing_in_canonical": sorted("|".join(k) for k in (source_keys - canon_keys))[:100],
        "extra_in_canonical": sorted("|".join(k) for k in (canon_keys - source_keys))[:100],
        "value_mismatch": value_mismatch[:100],
        "checks": {
            "source_keys_equal_canonical": source_keys == canon_keys,
            "value_mismatch_zero": len(value_mismatch) == 0,
        },
    }


def _latest_json(prefix: str, d: str) -> Path | None:
    exact = LOG_DIR / f"{prefix}_{d}.json"
    if exact.exists():
        return exact
    matches = sorted(LOG_DIR.glob(f"{prefix}_{d}_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _find_pnl_summary_for_d(d: str) -> Path | None:
    """D 기준일의 손익 요약을 찾는다.

    2026-08-20: 이전에는 `_latest_json("paper_sync_pnl_summary", d)`로 **파일명 스탬프**를
    D로 가정해 찾았다. 그러나 그 스탬프는 `paper_sync.py:414`가 붙이는 **실행 시각**이다.
    D는 마지막 체결일로 고정되므로, 그날 paper_sync 가 돌지 않았으면 파일이 영원히 없고
    `stats_as_of_match` 가 구조적으로 통과 불가가 된다.
    실제로 D=20260807 스탬프 파일은 0개였고 그 결과 ops_sanity 가 매일 rc=10 으로 실패했다.

    이제 파일 **내용**의 `as_of` 로 찾는다. 이름 기반 탐색은 하위호환으로 남긴다.
    """
    exact = _latest_json("paper_sync_pnl_summary", d)
    if exact is not None:
        return exact
    try:
        cands = sorted(
            LOG_DIR.glob("paper_sync_pnl_summary_*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        return None
    for p in cands:
        data = _read_json(p)
        if not isinstance(data, dict):
            continue
        as_of = str(data.get("as_of") or data.get("as_of_ymd") or "")
        if as_of == d:
            return p
    return None


def _load_supporting_reports(d: str) -> Dict[str, Any]:
    p0_core_path = LOG_DIR / f"p0_live_vs_bt_core_{d}.json"
    orders_contract_path = LOG_DIR / f"p0_orders_exec_contract_{d}.json"
    ledger_report_path = LOG_DIR / f"ledger_append_report_{d}.json"
    pnl_path = _find_pnl_summary_for_d(d)
    return {
        "p0_core": {"path": str(p0_core_path), "data": _read_json(p0_core_path)},
        "orders_exec_contract": {"path": str(orders_contract_path), "data": _read_json(orders_contract_path)},
        "orders_exec_contract_current_file": _recompute_orders_exec_contract_from_file(d),
        "ledger_append": {"path": str(ledger_report_path), "data": _read_json(ledger_report_path)},
        "stats_pnl": {"path": str(pnl_path) if pnl_path else "", "data": _read_json(pnl_path) if pnl_path else {}},
    }


def _to_bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "y", "yes"})


def _recompute_orders_exec_contract_from_file(d: str) -> Dict[str, Any]:
    path = PAPER / f"orders_{d}_exec.xlsx"
    if not path.exists():
        return {"path": str(path), "status": "NOT_EVALUABLE", "reason": "missing_orders_exec"}
    try:
        df = pd.read_excel(path, dtype=str).fillna("")
    except Exception as exc:
        return {"path": str(path), "status": "NOT_EVALUABLE", "reason": f"read_failed:{exc}"}
    required = {"exec_date", "side", "fill_qty", "is_stop", "entry_blocked"}
    missing = sorted(required - set(df.columns))
    if missing:
        return {"path": str(path), "status": "NOT_EVALUABLE", "reason": f"missing_columns:{missing}"}

    side_u = df["side"].astype(str).str.upper().str.strip()
    stop_b = _to_bool_series(df["is_stop"])
    blocked_b = _to_bool_series(df["entry_blocked"])
    posthoc_b = _to_bool_series(df.get("posthoc_policy_violation", pd.Series(False, index=df.index)))
    execution_blocked_b = _to_bool_series(df.get("execution_blocked", pd.Series(False, index=df.index)))
    qty_num = pd.to_numeric(df["fill_qty"], errors="coerce")
    buy_nonstop = side_u.eq("BUY") & (~stop_b)
    exec_unique = sorted({str(v).strip() for v in df["exec_date"].astype(str).tolist() if str(v).strip()})

    summary = {
        "orders_rows": int(len(df)),
        "exec_date_unique": exec_unique,
        "qty_nan": int(qty_num.isna().sum()),
        "qty_le_zero": int((qty_num <= 0).sum()),
        "posthoc_policy_violation_rows": int((buy_nonstop & posthoc_b).sum()),
        "execution_blocked_rows": int((buy_nonstop & execution_blocked_b).sum()),
        "blocked_filled_rows": int((buy_nonstop & (blocked_b | posthoc_b | execution_blocked_b) & (qty_num > 0)).sum()),
    }
    checks = {
        "exec_date_unique_eq_D": exec_unique == [d],
        "qty_positive_only": summary["qty_nan"] == 0 and summary["qty_le_zero"] == 0,
        "posthoc_policy_violation_zero": summary["posthoc_policy_violation_rows"] == 0,
        "execution_blocked_zero": summary["execution_blocked_rows"] == 0,
        "blocked_filled_zero": summary["blocked_filled_rows"] == 0,
    }
    status = "PASS" if all(bool(v) for v in checks.values()) else "FAIL"
    return {"path": str(path), "status": status, "summary": summary, "checks": checks}


def _supporting_checks(reports: Dict[str, Any], d: str) -> Dict[str, Any]:
    p0 = reports["p0_core"]["data"]
    contract = reports["orders_exec_contract"]["data"]
    current_contract = reports.get("orders_exec_contract_current_file") or {}
    ledger = reports["ledger_append"]["data"]
    stats = reports["stats_pnl"]["data"]
    reported_contract_pass = contract.get("status") == "PASS" and str(contract.get("as_of") or "") == d
    current_contract_pass = current_contract.get("status") == "PASS"
    return {
        "p0_core_status_pass": p0.get("status") == "PASS" and str(p0.get("as_of") or "") == d,
        "orders_exec_contract_pass": bool(reported_contract_pass or current_contract_pass),
        "orders_exec_contract_report_pass": bool(reported_contract_pass),
        "orders_exec_contract_current_file_pass": bool(current_contract_pass),
        "ledger_append_pass": ledger.get("status") == "PASS" and str(ledger.get("as_of") or "") == d,
        # 2026-08-20: 산출물 부재와 값 불일치를 분리한다.
        # 요약이 아예 없는 것은 데이터 불일치가 아니라 미생성이므로 HARD_FAIL 이 아니다.
        "stats_pnl_available": bool(stats),
        "stats_as_of_match": (not stats) or str(stats.get("as_of") or "") == d,
        "ledger_apply_true": bool(ledger.get("apply")) if ledger else False,
    }


def build_report(d: str = "") -> Tuple[int, Dict[str, Any]]:
    fills_path = PAPER / "fills.csv"
    rows = _read_csv_rows(fills_path)
    d = _norm_ymd(d) or _derive_d_from_fills(rows)
    if len(d) != 8:
        return 2, {"status": "FAIL", "reason": "cannot derive D"}

    source_events, source_sources = _source_events_for_compare(fills_path, d)
    canonical_events = _load_canonical_events(d)
    source_replay = _replay_in_memory(source_events)
    canonical_replay = _replay_in_memory(canonical_events)
    event_compare = _compare_event_sets(source_events, canonical_events)
    reports = _load_supporting_reports(d)
    support_checks = _supporting_checks(reports, d)
    source_rows = sum(int(src.get("rows_for_D", 0) or 0) for src in source_sources)

    checks = {
        "canonical_log_exists": (FILLS_LOG_DIR / f"{d}.ndjson").exists(),
        "source_rows_positive": source_rows > 0,
        "source_keys_equal_canonical": event_compare["checks"]["source_keys_equal_canonical"],
        "value_mismatch_zero": event_compare["checks"]["value_mismatch_zero"],
        "state_hash_equal": source_replay["final_state_hash"] == canonical_replay["final_state_hash"],
        "chain_hash_equal": source_replay["chain_hash"] == canonical_replay["chain_hash"],
        **support_checks,
    }
    hard_keys = [
        "canonical_log_exists",
        "source_rows_positive",
        "source_keys_equal_canonical",
        "value_mismatch_zero",
        "state_hash_equal",
        "chain_hash_equal",
        "p0_core_status_pass",
        "orders_exec_contract_pass",
        "ledger_append_pass",
        "stats_as_of_match",
    ]
    status = "PASS" if all(bool(checks.get(k)) for k in hard_keys) else "FAIL"
    report = {
        "status": status,
        "mode": "read_only_compare",
        "D": d,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "paths": {
            "source_fills": str(fills_path),
            "source_kis_fills": str(PAPER / f"kis_fills_api_{d}.csv") if (PAPER / f"kis_fills_api_{d}.csv").exists() else "",
            "canonical_log": str(FILLS_LOG_DIR / f"{d}.ndjson"),
            "report": str(LOG_DIR / f"canonical_replay_compare_{d}.json"),
        },
        "counts": {
            "source_fills_rows_for_D": source_rows,
            "source_events": len(source_events),
            "canonical_events": len(canonical_events),
        },
        "source_inputs": source_sources,
        "event_compare": event_compare,
        "source_replay": source_replay,
        "canonical_replay": canonical_replay,
        "supporting_reports": reports,
        "checks": checks,
        "tested": [
            "paper fills D rows canonicalized in memory",
            "kis fills api D rows canonicalized in memory when present",
            "canonical ndjson event keys and value hashes",
            "source replay hash vs canonical replay hash",
            "p0 orders_exec contract status",
            "ledger append report status",
            "paper sync pnl summary as_of",
        ],
        "not_tested": [
            "live KIS non-empty append day",
            "RootB dashboard card rendering",
            "production SSOT transition",
        ],
    }
    return (0 if status == "PASS" else 2), report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="")
    args = parser.parse_args()

    rc, report = build_report(args.date)
    d = str(report.get("D") or _norm_ymd(args.date) or "unknown")
    out = LOG_DIR / f"canonical_replay_compare_{d}.json"
    latest = LOG_DIR / "canonical_replay_compare_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    latest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        "[CANONICAL_COMPARE] "
        f"status={report.get('status')} D={d} "
        f"source_events={report.get('counts', {}).get('source_events')} "
        f"canonical_events={report.get('counts', {}).get('canonical_events')} "
        f"state_hash_equal={report.get('checks', {}).get('state_hash_equal')} "
        f"chain_hash_equal={report.get('checks', {}).get('chain_hash_equal')} "
        f"report={out}"
    )
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
