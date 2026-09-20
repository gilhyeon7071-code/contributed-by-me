from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import stat
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
FILLS_LOG_DIR = ROOT / "live" / "fills_log"


CANONICAL_ORDER = ("xts", "xid", "lid")


def _norm_ymd(value: Any) -> str:
    text = re.sub(r"[^0-9]", "", str(value or ""))
    return text[:8] if len(text) >= 8 else ""


def _norm_ts(value: Any, fallback_ymd: str) -> str:
    text = str(value or "").strip()
    digits = re.sub(r"[^0-9]", "", text)
    if len(digits) >= 14:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}T{digits[8:10]}:{digits[10:12]}:{digits[12:14]}Z"
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}T00:00:00Z"
    if fallback_ymd:
        return f"{fallback_ymd[:4]}-{fallback_ymd[4:6]}-{fallback_ymd[6:8]}T00:00:00Z"
    return "1970-01-01T00:00:00Z"


def _norm_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"BUY", "B", "02", "2"}:
        return "BUY"
    if side in {"SELL", "S", "01", "1"}:
        return "SELL"
    return side


def _norm_code(value: Any) -> str:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return digits.zfill(6)[-6:] if digits else ""


def _decimal_text(value: Any, default: str = "0") -> str:
    try:
        text = str(value or "").replace(",", "").strip()
        dec = Decimal(text if text else default)
        if not dec.is_finite():
            return default
        return format(dec.normalize(), "f")
    except (InvalidOperation, ValueError):
        return default


def _int_text(value: Any, default: str = "0") -> str:
    dec = Decimal(_decimal_text(value, default))
    return str(int(dec))


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _event_value_for_duplicate_compare(value: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(value or {})
    # raw_ptr can move when the source CSV is regenerated in a different row order.
    # The canonical key plus economic fields define duplicate identity.
    out.pop("raw_ptr", None)
    return out


def _is_kis_cumulative_snapshot_supersede(previous: Dict[str, Any], incoming: Dict[str, Any]) -> bool:
    prev_v = previous.get("v") or {}
    inc_v = incoming.get("v") or {}
    if "kis_fills_api_" not in str(prev_v.get("raw_ptr") or ""):
        return False
    if "kis_fills_api_" not in str(inc_v.get("raw_ptr") or ""):
        return False
    for field in ("sym", "side", "venue", "xts"):
        if str(prev_v.get(field) or "") != str(inc_v.get(field) or ""):
            return False
    try:
        prev_qty = Decimal(str(prev_v.get("qty") or "0"))
        inc_qty = Decimal(str(inc_v.get("qty") or "0"))
        inc_px = Decimal(str(inc_v.get("px") or "0"))
    except InvalidOperation:
        return False
    return inc_qty > 0 and inc_px > 0 and inc_qty >= prev_qty


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _derive_d(rows: Iterable[Dict[str, str]]) -> str:
    max_buy = ""
    max_any = ""
    for row in rows:
        ymd = _norm_ymd(row.get("datetime") or row.get("ts") or row.get("date"))
        if len(ymd) != 8:
            continue
        if ymd > max_any:
            max_any = ymd
        if _norm_side(row.get("side")) == "BUY" and ymd > max_buy:
            max_buy = ymd
    return max_buy or max_any


def _canonical_event(row: Dict[str, str], row_no: int, fills_path: Path, d: str) -> Dict[str, Any]:
    raw_ts = row.get("datetime") or row.get("ts") or ""
    if not raw_ts and (row.get("date") or row.get("ccld_tmd") or row.get("ord_tmd")):
        raw_ts = f"{row.get('date') or d}{row.get('ccld_tmd') or row.get('ord_tmd') or ''}"
    if not raw_ts:
        raw_ts = row.get("date") or ""
    xts = _norm_ts(raw_ts, d)
    lid = str(row.get("local_order_id") or row.get("order_id") or row.get("order_no") or "").strip()
    raw_key_material = _json_dumps({"row_no": row_no, "row": row})
    xid = str(row.get("exchange_exec_id") or row.get("exec_id") or row.get("xid") or "").strip()
    if not xid and (row.get("order_branch_no") or row.get("order_no") or row.get("ccld_tmd")):
        xid = "|".join(
            [
                str(row.get("order_branch_no") or "").strip(),
                str(row.get("order_no") or "").strip(),
                str(row.get("ccld_tmd") or row.get("ord_tmd") or "").strip(),
            ]
        ).strip("|")
    if not xid:
        xid = lid
    if not xid:
        xid = "sha256:" + _sha256_text(raw_key_material)[:24]
    if not lid:
        lid = "unknown:" + _sha256_text(raw_key_material)[:24]

    qty = _int_text(row.get("qty") or row.get("fill_qty") or row.get("exec_qty") or "0")
    px = _decimal_text(row.get("price") or row.get("fill_price") or row.get("exec_price") or "0")
    fee = _decimal_text(row.get("fee") or row.get("commission") or "0")
    sym = _norm_code(row.get("symbol") or row.get("code"))
    side = _norm_side(row.get("side"))

    key = {"xid": xid, "lid": lid, "xts": xts}
    value = {
        "sym": sym,
        "side": side,
        "qty": qty,
        "px": px,
        "fee": fee,
        "liq": str(row.get("liquidity_flag") or row.get("liq") or "UNKNOWN").strip() or "UNKNOWN",
        "venue": str(row.get("venue") or ("KRX" if row.get("order_no") else "PAPER")).strip() or "PAPER",
        "xts": xts,
        "rts": _norm_ts(row.get("recv_ts") or row.get("rts") or raw_ts, d),
        "raw_ptr": f"file://{fills_path}#row={row_no}",
    }
    return {"k": key, "v": value}


def _event_key(event: Dict[str, Any]) -> Tuple[str, str, str]:
    k = event.get("k") or {}
    return str(k.get("xid") or ""), str(k.get("lid") or ""), str(k.get("xts") or "")


def _order_key(event: Dict[str, Any]) -> Tuple[str, str, str]:
    k = event.get("k") or {}
    return str(k.get("xts") or ""), str(k.get("xid") or ""), str(k.get("lid") or "")


def _read_existing_events(path: Path) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", newline="") as f:
        for line_no, line in enumerate(f, start=1):
            text = line.strip()
            if not text:
                continue
            event = json.loads(text)
            key = _event_key(event)
            if key in out and _json_dumps(out[key]) != _json_dumps(event):
                raise ValueError(f"existing_log_conflict line={line_no} key={key}")
            out[key] = event
    return out


def _set_readonly(path: Path, readonly: bool) -> None:
    if not path.exists():
        return
    mode = path.stat().st_mode
    if readonly:
        path.chmod(mode & ~stat.S_IWRITE)
    else:
        path.chmod(mode | stat.S_IWRITE)


def _write_events(path: Path, events: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _set_readonly(path, False)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for event in sorted(events, key=_order_key):
            f.write(_json_dumps(event) + "\n")
    _set_readonly(path, True)


def _dedupe_append(path: Path, events: List[Dict[str, Any]], evidence_path: Path) -> Dict[str, Any]:
    existing = _read_existing_events(path)
    appended: List[Dict[str, Any]] = []
    duplicates = 0
    superseded = 0
    conflicts: List[Dict[str, Any]] = []

    for event in events:
        key = _event_key(event)
        previous = existing.get(key)
        if previous is None:
            existing[key] = event
            appended.append(event)
            continue
        if _json_dumps(_event_value_for_duplicate_compare(previous.get("v") or {})) == _json_dumps(
            _event_value_for_duplicate_compare(event.get("v") or {})
        ):
            duplicates += 1
            continue
        if _is_kis_cumulative_snapshot_supersede(previous, event):
            existing[key] = event
            superseded += 1
            continue
        conflicts.append({"key": key, "existing": previous, "incoming": event})

    if conflicts:
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(
            json.dumps(
                {
                    "status": "HARD_FAIL",
                    "reason": "canonical_fill_key_conflict",
                    "conflict_count": len(conflicts),
                    "conflicts": conflicts,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return {"status": "HARD_FAIL", "duplicates": duplicates, "appended": 0, "superseded": superseded, "conflicts": len(conflicts)}
    if evidence_path.exists():
        evidence_path.write_text(
            json.dumps(
                {
                    "status": "PASS",
                    "reason": "previous_conflict_resolved",
                    "conflict_count": 0,
                    "events_superseded": superseded,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    if appended or superseded:
        _write_events(path, existing.values())
    elif path.exists():
        _set_readonly(path, True)

    return {"status": "PASS", "duplicates": duplicates, "appended": len(appended), "superseded": superseded, "conflicts": 0}


def _state_payload(positions: Dict[str, Dict[str, Decimal]], cash: Dict[str, Decimal]) -> Dict[str, Any]:
    pos_out = []
    for sym in sorted(positions):
        pos = positions[sym]
        qty = pos["qty"]
        cost = pos["cost"]
        if qty == 0 and cost == 0:
            continue
        pos_out.append({"sym": sym, "qty": format(qty.normalize(), "f"), "cost": format(cost.normalize(), "f")})
    cash_out = {k: format(v.normalize(), "f") for k, v in sorted(cash.items())}
    return {"positions": pos_out, "cash": cash_out}


def _hash_obj(obj: Any) -> str:
    return _sha256_text(_json_dumps(obj))


def _apply_fill(
    positions: Dict[str, Dict[str, Decimal]],
    cash: Dict[str, Decimal],
    event: Dict[str, Any],
) -> None:
    v = event.get("v") or {}
    sym = str(v.get("sym") or "")
    side = str(v.get("side") or "").upper()
    qty = Decimal(str(v.get("qty") or "0"))
    px = Decimal(str(v.get("px") or "0"))
    fee = Decimal(str(v.get("fee") or "0"))
    if not sym or qty <= 0 or px <= 0:
        return
    pos = positions.setdefault(sym, {"qty": Decimal("0"), "cost": Decimal("0")})
    notional = qty * px
    if side == "BUY":
        pos["qty"] += qty
        pos["cost"] += notional
        cash["KRW"] = cash.get("KRW", Decimal("0")) - notional - fee
    elif side == "SELL":
        old_qty = pos["qty"]
        avg_cost = (pos["cost"] / old_qty) if old_qty > 0 else Decimal("0")
        reduce_qty = min(qty, old_qty) if old_qty > 0 else Decimal("0")
        pos["qty"] -= reduce_qty
        pos["cost"] -= avg_cost * reduce_qty
        cash["KRW"] = cash.get("KRW", Decimal("0")) + notional - fee
        cash["REALIZED_PNL"] = cash.get("REALIZED_PNL", Decimal("0")) + ((px - avg_cost) * reduce_qty) - fee


def _replay(events: List[Dict[str, Any]], state_hash_path: Path) -> Dict[str, Any]:
    positions: Dict[str, Dict[str, Decimal]] = {}
    cash: Dict[str, Decimal] = {"KRW": Decimal("0"), "REALIZED_PNL": Decimal("0")}
    lines: List[str] = []
    chain_hash = "0" * 64
    final_state_hash = _hash_obj(_state_payload(positions, cash))

    for idx, event in enumerate(sorted(events, key=_order_key), start=1):
        _apply_fill(positions, cash, event)
        payload = _state_payload(positions, cash)
        position_root = _hash_obj(payload["positions"])
        cash_root = _hash_obj(payload["cash"])
        state_hash = _hash_obj({"cash_root": cash_root, "position_root": position_root})
        chain_hash = _hash_obj({"prev": chain_hash, "state_hash": state_hash, "key": event.get("k")})
        final_state_hash = state_hash
        lines.append(
            _json_dumps(
                {
                    "idx": idx,
                    "k": event.get("k"),
                    "position_root": position_root,
                    "cash_root": cash_root,
                    "state_hash": state_hash,
                    "chain_hash": chain_hash,
                }
            )
        )

    state_hash_path.parent.mkdir(parents=True, exist_ok=True)
    state_hash_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return {
        "events_replayed": len(events),
        "final_state_hash": final_state_hash,
        "chain_hash": chain_hash,
        "state_hash_log": str(state_hash_path),
        "state": _state_payload(positions, cash),
    }


def run_shadow(
    fills_path: Path,
    date: str = "",
    fills_log_dir: Path = FILLS_LOG_DIR,
    summary_path: Path | None = None,
) -> Tuple[int, Dict[str, Any]]:
    if not fills_path.exists():
        return 2, {"status": "FAIL", "reason": f"missing fills: {fills_path}"}

    rows = _read_csv_rows(fills_path)
    d = _norm_ymd(date) or _derive_d(rows)
    if len(d) != 8:
        return 2, {"status": "FAIL", "reason": "cannot derive D"}

    target_rows = []
    for idx, row in enumerate(rows, start=2):
        ymd = _norm_ymd(row.get("datetime") or row.get("ts") or row.get("date"))
        if ymd == d:
            target_rows.append((idx, row))

    events = [_canonical_event(row, idx, fills_path.resolve(), d) for idx, row in target_rows]
    fills_log_path = fills_log_dir / f"{d}.ndjson"
    evidence_path = LOG_DIR / f"canonical_fills_conflict_{d}.json"
    append_result = _dedupe_append(fills_log_path, events, evidence_path)

    all_events = list(_read_existing_events(fills_log_path).values()) if fills_log_path.exists() else []
    replay_result = _replay(all_events, LOG_DIR / f"state_hash_{d}.log")
    summary = {
        "status": append_result["status"],
        "mode": "shadow_read_only",
        "D": d,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_fills": str(fills_path.resolve()),
        "fills_log": str(fills_log_path),
        "input_rows_for_D": len(target_rows),
        "events_input": len(events),
        "events_appended": append_result["appended"],
        "events_superseded": append_result.get("superseded", 0),
        "duplicates_ignored": append_result["duplicates"],
        "conflicts": append_result["conflicts"],
        "canonical_order": list(CANONICAL_ORDER),
        "replay": replay_result,
        "conflict_evidence": str(evidence_path) if append_result["status"] == "HARD_FAIL" else "",
        "operational_contract": "shadow only; positions/cash production path is unchanged",
    }

    summary_path = summary_path if summary_path is not None else LOG_DIR / f"canonical_fills_shadow_{d}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path = LOG_DIR / "canonical_fills_shadow_latest.json"
    latest_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return (0 if append_result["status"] == "PASS" else 2), summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="", help="YYYYMMDD. Defaults to fills D rule.")
    parser.add_argument("--fills", default=str(PAPER / "fills.csv"))
    parser.add_argument("--fills-log-dir", default=str(FILLS_LOG_DIR))
    parser.add_argument("--summary", default="")
    ns = parser.parse_args()

    rc, summary = run_shadow(
        fills_path=Path(ns.fills),
        date=ns.date,
        fills_log_dir=Path(ns.fills_log_dir),
        summary_path=Path(ns.summary) if ns.summary else None,
    )
    if rc != 0 and summary.get("status") == "FAIL":
        print(f"[CANONICAL_FILLS] FAIL {summary.get('reason')}")
        return rc
    replay_result = summary.get("replay") or {}
    fills_log_path = summary.get("fills_log")
    summary_path = Path(ns.summary) if ns.summary else LOG_DIR / f"canonical_fills_shadow_{summary.get('D')}.json"

    print(
        "[CANONICAL_FILLS] "
        f"status={summary['status']} D={summary.get('D')} input={summary['events_input']} "
        f"appended={summary['events_appended']} duplicates={summary['duplicates_ignored']} "
        f"superseded={summary.get('events_superseded', 0)} "
        f"conflicts={summary['conflicts']} replay={replay_result['events_replayed']} "
        f"state_hash={replay_result.get('final_state_hash')} chain_hash={replay_result.get('chain_hash')}"
    )
    print(f"[CANONICAL_FILLS] log={fills_log_path}")
    print(f"[CANONICAL_FILLS] state_hash_log={replay_result.get('state_hash_log')}")
    print(f"[CANONICAL_FILLS] summary={summary_path}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
