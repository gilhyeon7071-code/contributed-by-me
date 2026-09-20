"""Read-only reality audit of selected paper-engine mechanics.

The input signal is deliberately independent of legacy final_score and gates:
the price-integrity-controlled residual-momentum h5 grammar.  This measures the
effect of paper policies on one fixed eligible population; it does not claim
that the legacy candidate policy itself has alpha.
"""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
BASE = ROOT / "tools" / "build_new_method_price_structure_validation.py"
OUT_PREFIX = LOG / "existing_paper_policy_reality_audit"
START = pd.Timestamp("2025-06-01")
END = pd.Timestamp("2026-06-30")
MAX_NEW = 3
# [2026-09-10] 라이브 설정에 맞춰 교정. 이 상수들은 **편도**이고
#   왕복 = 2*FEE + 2*SLIPPAGE + SELL_TAX.
#   **이 파일에는 SELL_TAX 항이 아예 없었다** - 오늘 산식에 추가했다.
#   종전 0.005 는 왕복 1.0~1.4% 로 실제의 2.5~3.5배였다(BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2 + 0.001*2 + 0.002 = 0.00400 (optimize_params_v41_1.DEFAULT_FEE 와 동일).
#   **이 파일의 과거 산출물은 더 비싼 세계의 값이라 새 실행과 직접 비교할 수 없다.**
FEE_PCT = 0.0
SLIPPAGE_PCT = 0.001
SELL_TAX_PCT = 0.002   # 브로커 실측 0.19723% (수수료는 0)


def _load_base():
    spec = importlib.util.spec_from_file_location("policy_audit_base", BASE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _period(date: pd.Timestamp) -> str:
    if date <= pd.Timestamp("2025-09-30"):
        return "P1_202506_202509"
    if date <= pd.Timestamp("2025-12-31"):
        return "P2_202510_202512"
    if date <= pd.Timestamp("2026-03-31"):
        return "P3_202601_202603"
    return "P4_202604_202606"


def _net_return(gross_return: pd.Series) -> pd.Series:
    """Same percentage convention as current paper defaults: buy/sell fee+slippage."""
    gross = pd.to_numeric(gross_return, errors="coerce")
    entry_cash = (1.0 + SLIPPAGE_PCT) * (1.0 + FEE_PCT)
    # [2026-09-10] 매도 거래세 항이 없었다. 청산 쪽에만 한 번 적용한다.
    exit_cash = (1.0 + gross) * (1.0 - SLIPPAGE_PCT) * (1.0 - FEE_PCT) * (1.0 - SELL_TAX_PCT)
    return exit_cash / entry_cash - 1.0


def _select(
    signals: pd.DataFrame,
    *,
    top_n: int | None,
    prevent_overlap: bool,
    hold_to_session_offset: int,
    return_col: str,
) -> pd.DataFrame:
    selected: list[pd.DataFrame] = []
    held_until: dict[str, int] = {}
    for date, day in signals.groupby("date", sort=True):
        day = day.sort_values(["strategy_rank", "code"], kind="mergesort")
        if prevent_overlap:
            day = day[day.apply(lambda row: held_until.get(str(row.code), -1) < int(row.session_index), axis=1)]
        if top_n is not None:
            day = day.head(top_n)
        if day.empty:
            continue
        use = day.copy()
        use["gross_return"] = pd.to_numeric(use[return_col], errors="coerce")
        use = use[use["gross_return"].notna()].copy()
        if use.empty:
            continue
        selected.append(use)
        if prevent_overlap:
            for row in use.itertuples(index=False):
                held_until[str(row.code)] = int(row.session_index) + hold_to_session_offset
    return pd.concat(selected, ignore_index=True) if selected else signals.iloc[0:0].copy()


def _summarize(name: str, trades: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if trades.empty:
        return rows
    data = trades.copy()
    data["net_return"] = _net_return(data["gross_return"])
    data["period"] = data["date"].map(_period)
    for period, frame in list(data.groupby("period", sort=True)) + [("ALL_AVAILABLE_PRICE_COVERAGE", data)]:
        daily = frame.groupby("date", sort=True)[["gross_return", "net_return"]].mean()
        rows.append({
            "variant": name,
            "period": period,
            "signals_selected": int(len(frame)),
            "signal_days": int(frame["date"].nunique()),
            "avg_gross_return": float(frame["gross_return"].mean()),
            "median_gross_return": float(frame["gross_return"].median()),
            "gross_win_rate": float((frame["gross_return"] > 0).mean()),
            "avg_net_return": float(frame["net_return"].mean()),
            "median_net_return": float(frame["net_return"].median()),
            "net_win_rate": float((frame["net_return"] > 0).mean()),
            "avg_daily_equal_weight_net_return": float(daily["net_return"].mean()),
            "cost_drag": float(frame["gross_return"].mean() - frame["net_return"].mean()),
        })
    return rows


def main() -> int:
    base = _load_base()
    axis = base.load_base()
    report = axis._load_report_module()
    raw = report.load_data()
    integrity = raw.attrs.get("price_history_integrity", {})
    factors = report.compute_factors(raw)
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce").dt.normalize()
    factors = axis._exact_returns(factors)
    factors = factors.sort_values(["price_history_key", "price_session_index"], kind="mergesort").copy()
    group = factors.groupby("price_history_key", sort=False)
    factors["next_open"] = group["open"].shift(-1)
    factors["next_session_index"] = group["price_session_index"].shift(-1)
    factors["exit_close_after_next_open_h5"] = group["close"].shift(-6)
    factors["exit_session_index_after_next_open_h5"] = group["price_session_index"].shift(-6)
    factors["next_open_h5_return"] = (
        factors["exit_close_after_next_open_h5"] / factors["next_open"] - 1.0
    )
    exact_next_open = (
        (factors["next_session_index"] - factors["price_session_index"]).eq(1)
        & (factors["exit_session_index_after_next_open_h5"] - factors["price_session_index"]).eq(6)
    )
    factors.loc[~exact_next_open, "next_open_h5_return"] = np.nan
    eligible = factors[
        factors["date"].between(START, END)
        & factors["market"].astype(str).str.upper().isin(["KOSPI", "KOSDAQ"])
        & pd.to_numeric(factors["close"], errors="coerce").gt(0)
        & pd.to_numeric(factors["value"], errors="coerce").gt(0)
    ].copy()
    residual = pd.to_numeric(eligible["ret_20"], errors="coerce") - pd.to_numeric(eligible["m_ret_20"], errors="coerce")
    eligible["residual_ret20"] = residual
    eligible["residual_rank"] = eligible.groupby("date", sort=False)["residual_ret20"].rank(method="first", pct=True)
    signals = eligible[
        eligible["residual_rank"].ge(0.8)
        & pd.to_numeric(eligible["path_return_h5"], errors="coerce").notna()
    ].copy()
    signals = signals.rename(columns={"price_session_index": "session_index"})
    signals["strategy_rank"] = signals.groupby("date", sort=False)["residual_ret20"].rank(ascending=False, method="first")
    signals["code"] = signals["code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    if signals.duplicated(["date", "code"]).any():
        raise RuntimeError("duplicate signal key after the price integrity contract")
    variants = {
        "CLOSE_H5_ALL_SIGNALS_GROSS": dict(top_n=None, prevent_overlap=False, hold=5, ret="path_return_h5"),
        "CLOSE_H5_TOP3_GROSS": dict(top_n=MAX_NEW, prevent_overlap=False, hold=5, ret="path_return_h5"),
        "CLOSE_H5_TOP3_NO_REENTRY_WHILE_OPEN": dict(top_n=MAX_NEW, prevent_overlap=True, hold=5, ret="path_return_h5"),
        "NEXT_OPEN_H5_TOP3_NO_REENTRY_WHILE_OPEN": dict(top_n=MAX_NEW, prevent_overlap=True, hold=6, ret="next_open_h5_return"),
    }
    selected: dict[str, pd.DataFrame] = {}
    summary_rows: list[dict[str, object]] = []
    for name, cfg in variants.items():
        chosen = _select(signals, top_n=cfg["top_n"], prevent_overlap=cfg["prevent_overlap"], hold_to_session_offset=cfg["hold"], return_col=cfg["ret"])
        selected[name] = chosen
        summary_rows.extend(_summarize(name, chosen))
    summary = pd.DataFrame(summary_rows)
    summary_path = Path(f"{OUT_PREFIX}_summary_latest.csv")
    selected_path = Path(f"{OUT_PREFIX}_selected_trades_latest.csv")
    policy_path = Path(f"{OUT_PREFIX}_policy_verdict_latest.csv")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    selected_out = pd.concat([
        frame.assign(policy_variant=name, net_return=_net_return(frame["gross_return"]))
        for name, frame in selected.items()
    ], ignore_index=True)
    selected_out[[
        "policy_variant", "date", "code", "market", "strategy_rank", "session_index",
        "close", "gross_return", "net_return", "residual_ret20",
    ]].to_csv(selected_path, index=False, encoding="utf-8-sig")
    all_row = summary[(summary["variant"] == "CLOSE_H5_ALL_SIGNALS_GROSS") & (summary["period"] == "ALL_AVAILABLE_PRICE_COVERAGE")].iloc[0]
    top3_row = summary[(summary["variant"] == "CLOSE_H5_TOP3_GROSS") & (summary["period"] == "ALL_AVAILABLE_PRICE_COVERAGE")].iloc[0]
    no_reentry_row = summary[(summary["variant"] == "CLOSE_H5_TOP3_NO_REENTRY_WHILE_OPEN") & (summary["period"] == "ALL_AVAILABLE_PRICE_COVERAGE")].iloc[0]
    next_open_row = summary[(summary["variant"] == "NEXT_OPEN_H5_TOP3_NO_REENTRY_WHILE_OPEN") & (summary["period"] == "ALL_AVAILABLE_PRICE_COVERAGE")].iloc[0]
    verdict = pd.DataFrame([
        ["daily_max_new_3", "CLOSE_H5_ALL_SIGNALS_GROSS -> CLOSE_H5_TOP3_GROSS", "MEASURED", float(top3_row.avg_net_return - all_row.avg_net_return), "daily-cap effect on the same signal grammar"],
        ["no_reentry_while_open", "CLOSE_H5_TOP3_GROSS -> CLOSE_H5_TOP3_NO_REENTRY_WHILE_OPEN", "MEASURED", float(no_reentry_row.avg_net_return - top3_row.avg_net_return), "same-code exclusion only while an h5 position remains open"],
        ["current_fee_slippage", "gross -> net inside each variant", "MEASURED", float(no_reentry_row.cost_drag), "fee=0.005 and slippage=0.001 on both entry and exit"],
        ["next_open", "CLOSE_H5_TOP3_NO_REENTRY_WHILE_OPEN -> NEXT_OPEN_H5_TOP3_NO_REENTRY_WHILE_OPEN", "MEASURED", float(next_open_row.avg_net_return - no_reentry_row.avg_net_return), "next-open entry with five global sessions held after entry"],
        ["fixed_qty_1", "return calculation", "NOT_TESTABLE", np.nan, "one share changes KRW PnL and capital use, not percentage return"],
        ["t2_settled_cash", "capital/settlement feasibility", "NOT_TESTABLE", np.nan, "requires an explicit research capital and notional allocation policy"],
        ["legacy_final_score_and_entry_gate", "signal eligibility", "REJECTED_FOR_METHOD_TEST", np.nan, "would change the tested population to the legacy logic"],
        ["generic_stop_loss", "intraday exit ordering", "NOT_TESTABLE", np.nan, "requires a separately fixed stop threshold and intraday fill-order contract"],
    ], columns=["policy_component", "comparison", "verdict", "avg_net_return_difference", "interpretation"])
    verdict.to_csv(policy_path, index=False, encoding="utf-8-sig")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_existing_paper_policy_reality_audit",
        "input_signal": "price-integrity-controlled RESIDUAL_MOMENTUM top-quintile signal",
        "period": {"start": str(START.date()), "end": str(END.date())},
        "price_history_contract": integrity,
        "actual_signal_coverage": {"start": str(signals["date"].min().date()), "end": str(signals["date"].max().date()), "note": "P1 is partial because h5-complete signals begin after the requested window start."},
        "signal_rows": int(len(signals)),
        "signal_days": int(signals["date"].nunique()),
        "policy_rows": verdict.to_dict(orient="records"),
        "operational_change": False,
        "broker_order": False,
    }
    Path(f"{OUT_PREFIX}_latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Existing Paper Policy Reality Audit", "", "- scope: read_only", f"- period: {START.date()} to {END.date()}", f"- signal rows: {len(signals)}", f"- actual h5-complete signal coverage: {signals['date'].min().date()} to {signals['date'].max().date()} (P1 partial)", "", "## Policy Verdict", "", verdict.to_csv(index=False)]
    Path(f"{OUT_PREFIX}_latest.md").write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps({"status": "OK", "summary": str(summary_path), "signals": int(len(signals))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
