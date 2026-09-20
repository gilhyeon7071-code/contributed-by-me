from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"


REQUIRED_ORDER_COLS = {
    "exec_date",
    "side",
    "code",
    "fill_qty",
    "fill_price",
    "signal_date",
}


def _read_json(path: Path) -> Dict[str, Any]:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return json.loads(path.read_text(encoding=enc))
        except Exception:
            continue
    raise ValueError(f"unreadable json: {path}")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _norm_date8(value: Any) -> str:
    return re.sub(r"[^0-9]", "", str(value or ""))[:8]


def _norm_code(value: Any) -> str:
    text = re.sub(r"\D", "", str(value or ""))
    return text.zfill(6)[-6:] if text else ""


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).replace(",", "").strip()
        if not text:
            return default
        out = float(text)
        if out != out:
            return default
        return out
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "1.0", "true", "yes", "y"}


def _pct01(value: Any, default: float) -> float:
    v = _to_float(value, default)
    if v > 1.0:
        v = v / 100.0
    return max(0.0, min(1.0, v))


def _latest_orders_exec() -> Path | None:
    files = sorted(PAPER.glob("orders_*_exec.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _derive_d_from_fills(fills: pd.DataFrame) -> str:
    if fills.empty or "datetime" not in fills.columns:
        return ""
    work = fills.copy()
    work["_ymd"] = work["datetime"].map(_norm_date8)
    if "side" in work.columns:
        buys = work.loc[work["side"].astype(str).str.upper().eq("BUY")]
        if not buys.empty:
            return str(buys["_ymd"].max() or "")
    return str(work["_ymd"].max() or "")


def _load_orders(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str).fillna("")
    missing = sorted(REQUIRED_ORDER_COLS - set(df.columns))
    if missing:
        raise ValueError(f"orders_exec missing columns: {missing}")
    return df


def _open_notional(state: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    total = 0.0
    by_code: Dict[str, float] = {}
    for pos in state.get("open_positions") or []:
        if not isinstance(pos, dict):
            continue
        code = _norm_code(pos.get("code"))
        qty = _to_float(pos.get("qty"))
        price = _to_float(pos.get("last_price"), 0.0) or _to_float(pos.get("entry_price"), 0.0)
        notional = max(0.0, qty * price)
        if code:
            by_code[code] = by_code.get(code, 0.0) + notional
        total += notional
    return total, by_code


def _daily_buy_notional(fills: pd.DataFrame, d: str) -> float:
    if fills.empty or not d:
        return 0.0
    work = fills.copy()
    work["_ymd"] = work.get("datetime", "").map(_norm_date8)
    side = work.get("side", pd.Series("", index=work.index)).astype(str).str.upper()
    today = work.loc[(work["_ymd"] == d) & side.eq("BUY")].copy()
    if today.empty:
        return 0.0
    qty = pd.to_numeric(today.get("qty", 0), errors="coerce").fillna(0.0)
    price = pd.to_numeric(today.get("price", 0), errors="coerce").fillna(0.0)
    return float((qty * price).sum())


def _decision_id(d: str, row: Dict[str, Any], reasons: List[str]) -> str:
    raw = "|".join(
        [
            d,
            str(row.get("exec_date", "")),
            str(row.get("side", "")),
            _norm_code(row.get("code")),
            str(row.get("fill_qty", "")),
            str(row.get("fill_price", "")),
            str(row.get("signal_date", "")),
            ";".join(reasons),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _evaluate_row(
    row: Dict[str, Any],
    context: Dict[str, Any],
    symbol_notional: Dict[str, float],
) -> Dict[str, Any]:
    side = str(row.get("side", "") or "").strip().upper()
    code = _norm_code(row.get("code"))
    qty = max(0.0, _to_float(row.get("fill_qty")))
    price = max(0.0, _to_float(row.get("fill_price")))
    notional = float(qty * price)
    reasons: List[str] = []
    action = "ALLOW"
    reduce_to_qty = qty
    slice_count = 1

    if side not in {"BUY", "SELL"}:
        reasons.append("invalid_side")
    if not code:
        reasons.append("invalid_code")
    if qty <= 0:
        reasons.append("invalid_qty")
    if price <= 0:
        reasons.append("invalid_price")
    if _to_bool(row.get("entry_blocked")):
        reasons.append(f"upstream_entry_blocked:{row.get('entry_block_reason', '')}")

    capital_total = float(context.get("capital_total") or 0.0)
    gross_cap = float(context.get("gross_cap_krw") or 0.0)
    daily_new_cap = float(context.get("daily_new_cap_krw") or 0.0)
    per_symbol_cap = float(context.get("per_symbol_cap_krw") or 0.0)
    stage_notional = float(context.get("stage_notional_krw") or 0.0)
    current_open = float(context.get("open_notional_krw") or 0.0)
    daily_buy = float(context.get("daily_buy_notional_krw") or 0.0)

    if side == "BUY" and capital_total > 0:
        projected_gross = current_open + notional
        projected_daily = daily_buy + notional
        projected_symbol = float(symbol_notional.get(code, 0.0) or 0.0) + notional
        if gross_cap > 0 and projected_gross > gross_cap:
            reasons.append(f"gross_exposure_budget_exceeded:{projected_gross:.2f}>{gross_cap:.2f}")
        if daily_new_cap > 0 and projected_daily > daily_new_cap:
            reasons.append(f"daily_new_budget_exceeded:{projected_daily:.2f}>{daily_new_cap:.2f}")
        if per_symbol_cap > 0 and projected_symbol > per_symbol_cap:
            reasons.append(f"symbol_budget_exceeded:{projected_symbol:.2f}>{per_symbol_cap:.2f}")
        if stage_notional > 0 and notional > stage_notional:
            slice_count = max(2, int((notional + stage_notional - 1) // stage_notional))
            reasons.append(f"staged_execution_recommended:slices={slice_count}")

    hard_reasons = [
        r
        for r in reasons
        if r.startswith(("invalid_", "upstream_entry_blocked", "gross_exposure_budget_exceeded", "daily_new_budget_exceeded", "symbol_budget_exceeded"))
    ]
    if hard_reasons:
        action = "BLOCK"
        reduce_to_qty = 0.0
    elif slice_count > 1:
        action = "STAGE"
        reduce_to_qty = max(1.0, float(int(qty / slice_count)))

    return {
        "decision_id": _decision_id(str(context.get("D") or ""), row, reasons),
        "action": action,
        "side": side,
        "code": code,
        "qty": qty,
        "price": price,
        "notional_krw": round(notional, 2),
        "reduce_to_qty": int(reduce_to_qty) if float(reduce_to_qty).is_integer() else reduce_to_qty,
        "slice_count": int(slice_count),
        "reasons": reasons or ["ok"],
    }


def build_contract(date: str = "", orders_path: Path | None = None) -> Dict[str, Any]:
    cfg_path = PAPER / "paper_engine_config.json"
    state_path = PAPER / "paper_state_shadow.json"
    fills_path = PAPER / "fills.csv"
    trades_path = PAPER / "trades.csv"

    if not cfg_path.exists():
        raise FileNotFoundError(f"missing config: {cfg_path}")
    if not state_path.exists():
        raise FileNotFoundError(f"missing state: {state_path}")
    if not fills_path.exists():
        raise FileNotFoundError(f"missing fills: {fills_path}")
    if not trades_path.exists():
        raise FileNotFoundError(f"missing trades: {trades_path}")

    cfg = _read_json(cfg_path)
    state = _read_json(state_path)
    fills = pd.read_csv(fills_path, dtype=str, encoding="utf-8-sig").fillna("")

    d_rule = _derive_d_from_fills(fills)
    d = _norm_date8(date) or d_rule
    if not d:
        raise ValueError("cannot derive D")

    orders_path = orders_path or (PAPER / f"orders_{d}_exec.xlsx")
    if not orders_path.exists():
        fallback = _latest_orders_exec()
        if fallback and not date:
            orders_path = fallback
        else:
            raise FileNotFoundError(f"missing orders_exec: {orders_path}")

    orders = _load_orders(orders_path)
    exec_dates = sorted({_norm_date8(v) for v in orders["exec_date"].tolist() if _norm_date8(v)})
    if exec_dates != [d]:
        raise ValueError(f"exec_date mismatch: D={d} exec_dates={exec_dates}")
    if d_rule and d_rule != d:
        raise ValueError(f"fills D rule mismatch: D={d} fills_D={d_rule}")

    capital_total = _to_float(cfg.get("capital_total"), 0.0)
    top_gross = _pct01(cfg.get("max_gross_exposure_pct"), 1.0)
    budget_policy = cfg.get("capital_budget_policy") if isinstance(cfg.get("capital_budget_policy"), dict) else {}
    budget_gross = _pct01(budget_policy.get("gross_exposure_pct"), top_gross) if budget_policy else top_gross
    orch = cfg.get("risk_orchestration") if isinstance(cfg.get("risk_orchestration"), dict) else {}
    orch_gross = _pct01(orch.get("gross_exposure_pct"), budget_gross) if orch else budget_gross
    gross_pct = min(top_gross, budget_gross, orch_gross)
    daily_pct = _pct01(cfg.get("max_daily_new_exposure_pct"), 1.0)
    max_positions = max(1, int(_to_float(cfg.get("max_positions"), 1.0)))
    per_symbol_pct = gross_pct / float(max_positions)
    if budget_policy:
        basic_alloc_pct = _pct01(budget_policy.get("basic_alloc_pct"), gross_pct)
        basic_target_positions = max(1, int(_to_float(budget_policy.get("basic_target_positions"), max_positions)))
        per_symbol_pct = min(per_symbol_pct, basic_alloc_pct / float(basic_target_positions))
    stage_pct = _pct01(((cfg.get("execution_safety_contract") or {}) if isinstance(cfg.get("execution_safety_contract"), dict) else {}).get("stage_notional_pct"), per_symbol_pct)

    open_notional, symbol_notional = _open_notional(state)
    daily_notional = _daily_buy_notional(fills, d)
    context = {
        "D": d,
        "fills_D_rule": d_rule,
        "orders_exec_path": str(orders_path),
        "fills_path": str(fills_path),
        "trades_path": str(trades_path),
        "state_path": str(state_path),
        "config_path": str(cfg_path),
        "capital_total": capital_total,
        "gross_cap_pct": gross_pct,
        "gross_cap_krw": round(capital_total * gross_pct, 2),
        "daily_new_cap_pct": daily_pct,
        "daily_new_cap_krw": round(capital_total * daily_pct, 2),
        "per_symbol_cap_pct": round(per_symbol_pct, 8),
        "per_symbol_cap_krw": round(capital_total * per_symbol_pct, 2),
        "stage_notional_pct": round(stage_pct, 8),
        "stage_notional_krw": round(capital_total * stage_pct, 2),
        "open_notional_krw": round(open_notional, 2),
        "daily_buy_notional_krw": round(daily_notional, 2),
        "mode": "SHADOW_READ_ONLY",
        "allowed_execution_modes": ["PAPER", "DRY", "MOCK"],
    }

    decisions = [_evaluate_row(dict(row), context, symbol_notional) for _, row in orders.iterrows()]
    counts: Dict[str, int] = {}
    for decision in decisions:
        counts[decision["action"]] = counts.get(decision["action"], 0) + 1
    upstream_block_count = sum(
        1
        for decision in decisions
        if decision.get("action") == "BLOCK"
        and all(str(reason).startswith("upstream_entry_blocked") for reason in (decision.get("reasons") or []))
    )
    safety_block_count = int(counts.get("BLOCK", 0)) - int(upstream_block_count)

    status = "PASS"
    if not decisions:
        status = "NOT_EVALUABLE"
    elif safety_block_count > 0:
        status = "WARN"

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "schema_version": "execution_safety_contract_v1",
        "status": status,
        "mode": "SHADOW_READ_ONLY",
        "D": d,
        "summary": {
            "orders": int(len(orders)),
            "decisions_by_action": counts,
            "blocked": int(counts.get("BLOCK", 0)),
            "upstream_blocked": int(upstream_block_count),
            "safety_blocked": int(safety_block_count),
            "staged": int(counts.get("STAGE", 0)),
            "reduced": int(counts.get("REDUCE", 0)),
            "allowed": int(counts.get("ALLOW", 0)),
        },
        "context": context,
        "decisions": decisions,
        "policy_note": "shadow/read-only contract; does not mutate orders, fills, ledger, stats, gates, or broker dispatch",
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build read-only execution safety contract artifact.")
    ap.add_argument("--date", default="", help="D in YYYYMMDD; defaults to fills D rule")
    ap.add_argument("--orders-exec", default="", help="optional orders_exec xlsx path")
    args = ap.parse_args()

    orders_path = Path(args.orders_exec) if str(args.orders_exec or "").strip() else None
    try:
        payload = build_contract(date=args.date, orders_path=orders_path)
    except Exception as exc:
        payload = {
            "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "schema_version": "execution_safety_contract_v1",
            "status": "NOT_EVALUABLE",
            "mode": "SHADOW_READ_ONLY",
            "error": f"{type(exc).__name__}: {exc}",
            "policy_note": "fail-closed: contract inputs were missing or inconsistent",
        }
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        _write_json(LOG_DIR / f"execution_safety_contract_{ts}.json", payload)
        _write_json(LOG_DIR / "execution_safety_contract_latest.json", payload)
        print(f"[EXEC_SAFETY] status=NOT_EVALUABLE error={payload['error']}")
        return 2

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dated = LOG_DIR / f"execution_safety_contract_{payload['D']}_{ts}.json"
    latest = LOG_DIR / "execution_safety_contract_latest.json"
    _write_json(dated, payload)
    _write_json(latest, payload)
    print(
        f"[EXEC_SAFETY] status={payload['status']} D={payload['D']} "
        f"orders={payload['summary']['orders']} actions={payload['summary']['decisions_by_action']}"
    )
    print(f"[EXEC_SAFETY] json={dated}")
    print(f"[EXEC_SAFETY] latest={latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
