"""Read-only retroactive recalculation of paper/trades.csv PnL under the corrected
cost model (2026-07-23: fee_pct 0.5%->0.004%, sell_tax_pct 0.2%->0.15%).

This does NOT modify paper/trades.csv or any SSOT ledger file. It produces a
comparison-only artifact so the old (as-recorded) and new (corrected-cost)
PnL can be viewed side by side. slippage_pct/tiered_slippage are intentionally
left untouched (2026-07-23 decision: no empirical basis to re-derive, see PLANS.md).

Formula (exact copy of pricing_engine.py:compute_costs):
    net = gross - ((entry+exit)/entry)*(fee_pct+slippage_pct) - sell_tax_pct

Because slippage_pct is unchanged between old and new, it cancels out of the
delta, so the recalculation only needs entry/exit price and the originally
recorded pnl_pct - no need to know which slippage value (flat or tiered by
market cap, which this project has no historical market-cap data to resolve)
was actually applied to each historical trade:

    delta_pct   = ((entry+exit)/entry) * (old_fee_pct - new_fee_pct) + (old_tax - new_tax)
    new_pnl_pct = old_pnl_pct + delta_pct
    new_pnl_krw = new_pnl_pct * (old_pnl_krw / old_pnl_pct)   # implied notional

sell_tax_pct history (verified against dated config backups, not assumed):
  - 2026-04-02 and 2026-04-24 backups: sell_tax_pct=0.0
  - 2026-04-27 "before_lock_init_sell_tax" backup: sell_tax_pct=0.002 (already
    changed by then via undocumented config drift, later formally re-approved)
  - exact change day within 2026-04-24..04-27 is not recoverable (no backups
    in that 3-day window) - this script uses exit_date>=20260427 as the cutover,
    which is a stated approximation, not a verified exact date.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TRADES_PATH = ROOT / "paper" / "trades.csv"
LOG_DIR = ROOT / "2_Logs"
OUT_PREFIX = "trades_pnl_recalc_20260723_cost_fix"

OLD_FEE_PCT = 0.005
NEW_FEE_PCT = 0.00004
TAX_CUTOVER_YMD = "20260427"  # approximate, see module docstring
OLD_TAX_BEFORE_CUTOVER = 0.0
OLD_TAX_AFTER_CUTOVER = 0.002
NEW_TAX_PCT = 0.0015


def old_tax_for(exit_date: str) -> float:
    return OLD_TAX_AFTER_CUTOVER if exit_date >= TAX_CUTOVER_YMD else OLD_TAX_BEFORE_CUTOVER


def main() -> int:
    with TRADES_PATH.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    out_rows = []
    for r in rows:
        try:
            entry_price = float(r["entry_price"])
            exit_price = float(r["exit_price"])
            old_pnl_pct = float(r["pnl_pct"])
            old_pnl_krw = float(r["pnl_krw"])
        except (ValueError, KeyError):
            continue
        if entry_price <= 0 or old_pnl_pct == 0:
            continue

        exit_date = str(r.get("exit_date") or "")
        old_tax = old_tax_for(exit_date)
        price_ratio = (entry_price + exit_price) / entry_price
        delta_pct = price_ratio * (OLD_FEE_PCT - NEW_FEE_PCT) + (old_tax - NEW_TAX_PCT)
        new_pnl_pct = old_pnl_pct + delta_pct
        implied_notional = old_pnl_krw / old_pnl_pct
        new_pnl_krw = new_pnl_pct * implied_notional

        out_rows.append({
            "trade_id": r.get("trade_id", ""),
            "code": r.get("code", ""),
            "entry_date": r.get("entry_date", ""),
            "exit_date": exit_date,
            "old_tax_used": old_tax,
            "old_pnl_pct": old_pnl_pct,
            "new_pnl_pct": round(new_pnl_pct, 8),
            "delta_pct": round(delta_pct, 8),
            "old_pnl_krw": round(old_pnl_krw, 2),
            "new_pnl_krw": round(new_pnl_krw, 2),
            "is_surge": r.get("is_surge", ""),
        })

    out_csv = LOG_DIR / f"{OUT_PREFIX}.csv"
    with out_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)

    n = len(out_rows)
    old_wins = sum(1 for x in out_rows if x["old_pnl_pct"] > 0)
    new_wins = sum(1 for x in out_rows if x["new_pnl_pct"] > 0)
    old_total_krw = sum(x["old_pnl_krw"] for x in out_rows)
    new_total_krw = sum(x["new_pnl_krw"] for x in out_rows)
    old_avg_pct = sum(x["old_pnl_pct"] for x in out_rows) / n
    new_avg_pct = sum(x["new_pnl_pct"] for x in out_rows) / n

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_retroactive_recalc_no_ledger_change",
        "trades_path": str(TRADES_PATH),
        "n_trades": n,
        "entry_date_range": [min(x["entry_date"] for x in out_rows), max(x["entry_date"] for x in out_rows)],
        "cost_model_old": {"fee_pct": OLD_FEE_PCT, "sell_tax_pct": f"0.0 before {TAX_CUTOVER_YMD}, 0.002 after (approx cutover)"},
        "cost_model_new": {"fee_pct": NEW_FEE_PCT, "sell_tax_pct": NEW_TAX_PCT},
        "slippage_pct": "unchanged in both old and new (cancels out of delta, see docstring)",
        "old_win_rate": round(old_wins / n, 4),
        "new_win_rate": round(new_wins / n, 4),
        "old_avg_pnl_pct": round(old_avg_pct, 6),
        "new_avg_pnl_pct": round(new_avg_pct, 6),
        "old_total_pnl_krw": round(old_total_krw, 2),
        "new_total_pnl_krw": round(new_total_krw, 2),
        "note": "Ledger/trades.csv NOT modified. This is a comparison-only artifact.",
    }
    out_json = LOG_DIR / f"{OUT_PREFIX}_summary.json"
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
