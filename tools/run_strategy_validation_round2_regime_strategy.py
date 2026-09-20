"""Read-only Strategy Validation Round 2: pre-registered situation x strategy grammar."""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROUND1_PATH = Path(__file__).resolve().with_name("run_strategy_validation_round1.py")
_spec = importlib.util.spec_from_file_location("strategy_validation_round1_shared", ROUND1_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"cannot load Round 1 shared module: {ROUND1_PATH}")
_round1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_round1)
FEE = _round1.FEE
LOG = _round1.LOG
MIN_CALENDAR_DAYS = _round1.MIN_CALENDAR_DAYS
MIN_EPISODES = _round1.MIN_EPISODES
MIN_SIGNAL_DATES = _round1.MIN_SIGNAL_DATES
POPULATION_MEMBERSHIP = _round1.POPULATION_MEMBERSHIP
ROOT = _round1.ROOT
SELL_TAX = _round1.SELL_TAX
SIGNAL_MIN_PRICE = _round1.SIGNAL_MIN_PRICE
SIGNAL_MIN_VALUE = _round1.SIGNAL_MIN_VALUE
SLIPPAGE = _round1.SLIPPAGE
TRAIN_END = _round1.TRAIN_END
TRAIN_START = _round1.TRAIN_START
add_research_axes = _round1.add_research_axes
build_execution_panel = _round1.build_execution_panel
load_report = _round1.load_report
regime_replay_check = _round1.regime_replay_check
safe_records = _round1.safe_records
sha256 = _round1.sha256
summarize = _round1.summarize

OUT_PREFIX = "strategy_validation_round2_train_regime_strategy_20260720"
REGIMES = ("BULL", "BEAR", "SIDEWAYS", "STRESS", "TRANSITION")
STRATEGIES = ("MR_BOLLINGER", "MA_CROSS_UP", "BREAKOUT_252D")


def main() -> int:
    report = load_report()
    print("[1/5] loading integrity-filtered price history")
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    print("[2/5] computing as-of factors and ex-ante regime")
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    full_regime = report._assign_report_research_regime(factors)
    factors["research_regime"] = factors["date"].map(full_regime)
    replay = regime_replay_check(report, factors, full_regime)
    if any(item["status"] != "PASS" for item in replay):
        raise RuntimeError(f"regime truncate-replay failed: {replay}")
    print("[3/5] building actual-session h5 execution panel")
    panel = build_execution_panel(factors)
    membership = pd.read_parquet(POPULATION_MEMBERSHIP, columns=["as_of_date", "code", "market", "security_type"])
    membership["date"] = pd.to_datetime(membership["as_of_date"].astype(str), format="%Y%m%d", errors="coerce")
    membership["code"] = membership["code"].astype(str).str.zfill(6)
    membership["market"] = membership["market"].fillna("").astype(str).str.upper().str.strip()
    membership = membership.loc[
        membership["date"].between(TRAIN_START, TRAIN_END)
        & membership["market"].isin(["KOSPI", "KOSDAQ"])
        & membership["security_type"].fillna("").astype(str).str.upper().eq("COMMON"),
        ["date", "code", "market"],
    ].drop_duplicates(["date", "code"])
    if membership.duplicated(["date", "code"]).any():
        raise RuntimeError("duplicate static population membership key")
    panel = panel.merge(membership, on=["date", "code"], how="inner", validate="many_to_one", suffixes=("", "_membership"))
    eligible = (
        panel["date"].between(TRAIN_START, TRAIN_END)
        & pd.to_numeric(panel["open"], errors="coerce").gt(0)
        & pd.to_numeric(panel["close"], errors="coerce").ge(SIGNAL_MIN_PRICE)
        & pd.to_numeric(panel["value"], errors="coerce").ge(SIGNAL_MIN_VALUE)
    )
    panel = panel.loc[eligible].copy()
    if panel.empty:
        raise RuntimeError("empty registered research universe")
    print("[4/5] evaluating exactly 15 registered situation x strategy combinations")
    panel = add_research_axes(panel)
    rows: list[dict[str, object]] = []
    for regime in REGIMES:
        for strategy in STRATEGIES:
            item = summarize(
                panel,
                panel["research_regime"].eq(regime) & panel[strategy].fillna(False),
                "SITUATION_X_STRATEGY",
                f"{regime} × {strategy}",
            )
            item["research_regime"] = regime
            item["strategy_grammar"] = strategy
            rows.append(item)
    result = pd.DataFrame(rows).sort_values(["research_regime", "strategy_grammar"], kind="mergesort").reset_index(drop=True)
    csv_path = LOG / f"{OUT_PREFIX}.csv"
    json_path = LOG / f"{OUT_PREFIX}.json"
    md_path = LOG / f"{OUT_PREFIX}.md"
    result.to_csv(csv_path, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "OK",
        "scope": "read_only_strategy_validation_round2_train_situation_x_strategy",
        "plan": str(ROOT / "docs" / "exec-plans" / "active" / "20260720_strategy_validation_round2_regime_strategy.md"),
        "price_history_contract": integrity,
        "contract": {
            "signal_window": [TRAIN_START.strftime("%Y-%m-%d"), TRAIN_END.strftime("%Y-%m-%d")],
            "combination_count": 15,
            "situations": list(REGIMES),
            "strategy_grammars": list(STRATEGIES),
            "entry": "next actual global trading-session open",
            "exit": "h5 actual global trading-session close",
            "costs": {"fee_pct_each_side": FEE, "slippage_pct_each_side": SLIPPAGE, "sell_tax_pct": SELL_TAX},
            "primary_metric": "signal-date equal-weight candidate basket net h5 return",
            "minimum_cell": {"unique_signal_dates": MIN_SIGNAL_DATES, "calendar_span_days": MIN_CALENDAR_DAYS, "coverage_half_year_blocks": MIN_EPISODES},
            "no_signal_or_parameter_addition": True,
        },
        "input": {
            "eligible_rows": int(len(panel)),
            "eligible_signal_dates": int(panel["date"].nunique()),
            "population_membership_rows": int(len(membership)),
            "population_membership_hash": sha256(POPULATION_MEMBERSHIP),
            "runner_hash": sha256(Path(__file__)),
        },
        "regime_truncate_replay": replay,
        "results": safe_records(result),
        "operational_change": False,
        "selection_or_promotion": "FORBIDDEN_IN_ROUND_2",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Strategy Validation Round 2 — Train Situation x Strategy Results",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- eligible_rows: {len(panel)}",
        "- selection_or_promotion: forbidden",
        "",
        "## Results",
        "",
        "```csv",
        result.to_csv(index=False),
        "```",
        "",
        "## Regime truncate-replay",
        "",
        "```csv",
        pd.DataFrame(replay).to_csv(index=False),
        "```",
        "",
        "## Interpretation limit",
        "",
        "- These are pre-registered two-axis exploratory results only. No Validation/OOS selection, signal addition, candidate-generator change, or operating application is permitted from this file.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "csv": str(csv_path), "json": str(json_path), "md": str(md_path), "eligible_rows": int(len(panel))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())