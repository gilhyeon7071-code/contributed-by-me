from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_execution_safety_contract import build_contract  # noqa: E402


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _norm_int(value: Any, default: int = 0) -> int:
    try:
        text = str(value or "").replace(",", "").strip()
        if not text:
            return int(default)
        return int(float(text))
    except Exception:
        return int(default)


def _norm_float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value or "").replace(",", "").strip()
        if not text:
            return float(default)
        out = float(text)
        if out != out:
            return float(default)
        return out
    except Exception:
        return float(default)


def _slice_quantities(qty: int, slice_count: int) -> List[int]:
    if qty <= 0:
        return []
    count = max(1, min(int(slice_count), int(qty)))
    base = qty // count
    rem = qty % count
    return [base + (1 if idx < rem else 0) for idx in range(count)]


def _run_self_test() -> int:
    cases = [
        (0, 3, []),
        (1, 5, [1]),
        (7, 3, [3, 2, 2]),
        (10, 4, [3, 3, 2, 2]),
        (5, 0, [5]),
    ]
    for qty, slice_count, expected in cases:
        got = _slice_quantities(qty, slice_count)
        assert got == expected, f"slice mismatch qty={qty} count={slice_count}: got={got} expected={expected}"
        assert sum(got) == max(0, qty), f"slice sum mismatch qty={qty} got={got}"
        assert all(int(x) > 0 for x in got), f"slice contains non-positive qty: {got}"
    print("[EXEC_SLICING_PREVIEW] self-test PASS cases=5")
    return 0


def build_preview(date: str = "", orders_exec: Path | None = None) -> Dict[str, Any]:
    contract = build_contract(date=date, orders_path=orders_exec)
    d = str(contract.get("D") or "")
    context = contract.get("context") if isinstance(contract.get("context"), dict) else {}
    orders_path = Path(str(context.get("orders_exec_path") or ""))
    if not orders_path.exists():
        raise FileNotFoundError(f"orders_exec not found from contract: {orders_path}")

    orders = pd.read_excel(orders_path, dtype=str).fillna("")
    decisions = contract.get("decisions") if isinstance(contract.get("decisions"), list) else []
    if len(decisions) != len(orders):
        raise ValueError(f"decision/order row mismatch: decisions={len(decisions)} orders={len(orders)}")

    stage_notional = _norm_float(context.get("stage_notional_krw"), 0.0)
    rows: List[Dict[str, Any]] = []
    blocked = 0
    staged_orders = 0

    for idx, decision in enumerate(decisions):
        order_row = orders.iloc[idx].to_dict()
        action = str(decision.get("action") or "").upper()
        qty = _norm_int(order_row.get("fill_qty"), 0)
        price = _norm_float(order_row.get("fill_price"), 0.0)
        notional = qty * price
        if action == "BLOCK":
            blocked += 1
            continue
        if qty <= 0 or price <= 0:
            blocked += 1
            continue

        if stage_notional > 0 and notional > stage_notional:
            slice_count = int(math.ceil(notional / stage_notional))
        else:
            slice_count = 1
        slice_count = max(1, min(slice_count, qty))
        quantities = _slice_quantities(qty, slice_count)
        if len(quantities) > 1:
            staged_orders += 1

        for slice_idx, slice_qty in enumerate(quantities, start=1):
            slice_notional = float(slice_qty) * price
            rows.append(
                {
                    "D": d,
                    "mode": "DRY_RUN_PREVIEW",
                    "source_orders_exec": str(orders_path),
                    "source_row": int(idx),
                    "decision_id": str(decision.get("decision_id") or ""),
                    "decision_action": action,
                    "code": str(decision.get("code") or "").zfill(6),
                    "side": str(decision.get("side") or "").upper(),
                    "order_qty": int(qty),
                    "order_price": price,
                    "order_notional_krw": round(float(notional), 2),
                    "slice_index": int(slice_idx),
                    "slice_count": int(len(quantities)),
                    "slice_qty": int(slice_qty),
                    "slice_notional_krw": round(slice_notional, 2),
                    "slice_limit_krw": round(float(stage_notional), 2),
                    "execution_algo": "NOT_SELECTED_PREVIEW_ONLY",
                    "broker_submit_allowed": False,
                    "kis_api_call": False,
                    "reasons": ";".join(str(x) for x in list(decision.get("reasons") or [])),
                }
            )

    df = pd.DataFrame(rows)
    status = "PASS"
    if contract.get("status") == "NOT_EVALUABLE":
        status = "NOT_EVALUABLE"
    elif not rows:
        status = "NOT_EVALUABLE"

    generated_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    out_csv = LOG_DIR / f"execution_slicing_preview_{d}.csv"
    out_json = LOG_DIR / f"execution_slicing_preview_{d}.json"
    latest_json = LOG_DIR / "execution_slicing_preview_latest.json"
    latest_csv = LOG_DIR / "execution_slicing_preview_latest.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    df.to_csv(latest_csv, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": generated_at,
        "schema_version": "execution_slicing_preview_v1",
        "status": status,
        "mode": "DRY_RUN_PREVIEW",
        "D": d,
        "summary": {
            "source_orders": int(len(orders)),
            "contract_status": str(contract.get("status") or ""),
            "blocked_orders": int(blocked),
            "preview_orders": int(len(decisions) - blocked),
            "staged_orders": int(staged_orders),
            "slice_rows": int(len(rows)),
            "broker_submit_allowed": False,
            "kis_api_calls": 0,
        },
        "paths": {
            "source_orders_exec": str(orders_path),
            "csv": str(out_csv),
            "json": str(out_json),
            "latest_csv": str(latest_csv),
            "latest_json": str(latest_json),
        },
        "context": {
            "stage_notional_krw": stage_notional,
            "fills_D_rule": str(context.get("fills_D_rule") or ""),
            "policy_note": "dry-run preview only; no broker dispatch, fills, ledger, stats, Gate, or LOCK mutation",
        },
    }
    _write_json(out_json, payload)
    _write_json(latest_json, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build dry-run execution slicing preview artifact.")
    parser.add_argument("--date", default="", help="D in YYYYMMDD; defaults to fills D rule")
    parser.add_argument("--orders-exec", default="", help="optional orders_exec xlsx path")
    parser.add_argument("--self-test", action="store_true", help="run offline slicing math self-test")
    args = parser.parse_args()

    if args.self_test:
        return _run_self_test()

    orders_path = Path(args.orders_exec) if str(args.orders_exec or "").strip() else None
    try:
        payload = build_preview(date=args.date, orders_exec=orders_path)
    except Exception as exc:
        payload = {
            "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "schema_version": "execution_slicing_preview_v1",
            "status": "NOT_EVALUABLE",
            "mode": "DRY_RUN_PREVIEW",
            "error": f"{type(exc).__name__}: {exc}",
            "policy_note": "fail-closed: preview inputs were missing or inconsistent",
        }
        _write_json(LOG_DIR / "execution_slicing_preview_latest.json", payload)
        print(f"[EXEC_SLICING_PREVIEW] status=NOT_EVALUABLE error={payload['error']}")
        return 2

    print(
        f"[EXEC_SLICING_PREVIEW] status={payload['status']} D={payload['D']} "
        f"orders={payload['summary']['source_orders']} slices={payload['summary']['slice_rows']}"
    )
    print(f"[EXEC_SLICING_PREVIEW] json={payload['paths']['json']}")
    print(f"[EXEC_SLICING_PREVIEW] csv={payload['paths']['csv']}")
    return 0 if str(payload.get("status")) == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
