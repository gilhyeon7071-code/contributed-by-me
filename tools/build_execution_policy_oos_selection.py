"""Time-ordered execution-policy selection on one fixed research signal population."""
from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
BASE = ROOT / "tools" / "build_new_method_price_structure_validation.py"
PREFIX = LOG / "execution_policy_oos_selection"
# [2026-09-10] 라이브 설정에 맞춰 교정 (BROKEN_WINDOW_REGISTER C6).
#   브로커 실측(tr_id TTTC8715R): 매도대금 88,730 / **수수료 0** / 제세금 175 = 0.19723%.
#   라이브 왕복 = 0.0*2 + 0.001*2 + 0.002 = 0.00400 (optimize_params_v41_1.DEFAULT_FEE).
#   **이 파일에는 SELL_TAX 항이 아예 없었다** - 오늘 산식에 추가했다.
FEE, SLIP, TOP_N = 0.0, 0.001, 3
SELL_TAX = 0.002   # 브로커 실측 0.19723%


def _load_base():
    spec = importlib.util.spec_from_file_location("execution_policy_base", BASE)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _period(date: pd.Timestamp) -> str:
    if date <= pd.Timestamp("2025-12-31"):
        return "P2_TRAIN_202510_202512"
    if date <= pd.Timestamp("2026-03-31"):
        return "P3_TRAIN_202601_202603"
    return "P4_OOS_202604_202606"


def _net(ret: pd.Series) -> pd.Series:
    # [2026-09-10] 매도 거래세 항이 없었다. 청산 쪽에 (1 - SELL_TAX) 를 곱한다.
    return ((1 + ret) * (1 - SLIP) * (1 - FEE) * (1 - SELL_TAX)) / ((1 + SLIP) * (1 + FEE)) - 1


def _select(frame: pd.DataFrame, rank_col: str) -> pd.DataFrame:
    held_until: dict[str, int] = {}
    chosen: list[pd.DataFrame] = []
    for _, day in frame.groupby("date", sort=True):
        day = day.sort_values([rank_col, "code"], kind="mergesort")
        day = day[day.apply(lambda r: held_until.get(str(r.code), -1) < int(r.session_index), axis=1)].head(TOP_N).copy()
        if day.empty:
            continue
        for row in day.itertuples(index=False):
            held_until[str(row.code)] = int(row.session_index) + 5
        chosen.append(day)
    return pd.concat(chosen, ignore_index=True) if chosen else frame.iloc[0:0].copy()


def _summary(name: str, trades: pd.DataFrame) -> list[dict[str, object]]:
    trades = trades.copy()
    trades["net_return"] = _net(trades["gross_return"])
    trades["period"] = trades["date"].map(_period)
    rows = []
    for period, part in trades.groupby("period", sort=True):
        daily = part.groupby("date", sort=True).net_return.mean()
        rows.append({
            "selection_policy": name, "period": period, "trades": len(part), "days": part.date.nunique(),
            "avg_trade_net_return": part.net_return.mean(), "median_trade_net_return": part.net_return.median(),
            "net_win_rate": (part.net_return > 0).mean(), "avg_daily_equal_weight_net_return": daily.mean(),
        })
    return rows


def main() -> int:
    base = _load_base(); axis = base.load_base(); report = axis._load_report_module()
    raw = report.load_data(); integrity = raw.attrs.get("price_history_integrity", {})
    f = report.compute_factors(raw); f["date"] = pd.to_datetime(f.date, errors="coerce").dt.normalize(); f = axis._exact_returns(f)
    f = f[(f.date >= pd.Timestamp("2025-10-01")) & (f.date <= pd.Timestamp("2026-06-30")) & f.market.astype(str).str.upper().isin(["KOSPI", "KOSDAQ"])].copy()
    f = f[pd.to_numeric(f.close, errors="coerce").gt(0) & pd.to_numeric(f.value, errors="coerce").gt(0)].copy()
    f["residual_ret20"] = pd.to_numeric(f.ret_20, errors="coerce") - pd.to_numeric(f.m_ret_20, errors="coerce")
    f["residual_pct"] = f.groupby("date", sort=False).residual_ret20.rank(method="first", pct=True)
    sig = f[f.residual_pct.ge(0.8) & pd.to_numeric(f.path_return_h5, errors="coerce").notna()].copy()
    sig = sig.rename(columns={"price_session_index": "session_index", "path_return_h5": "gross_return"})
    sig["code"] = sig.code.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    if sig.duplicated(["date", "code"]).any():
        raise RuntimeError("duplicate signal key")
    sig["rank_residual"] = sig.groupby("date", sort=False).residual_ret20.rank(ascending=False, method="first")
    sig["rank_liquidity"] = sig.groupby("date", sort=False).value.rank(ascending=False, method="first")
    sig["composite"] = sig.groupby("date", sort=False).residual_ret20.rank(pct=True) + sig.groupby("date", sort=False).value.rank(pct=True)
    sig["rank_composite"] = sig.groupby("date", sort=False).composite.rank(ascending=False, method="first")
    rules = {
        "RESIDUAL_INTENSITY_TOP3": "rank_residual",
        "LIQUIDITY_FIRST_TOP3": "rank_liquidity",
        "RESIDUAL_LIQUIDITY_COMPOSITE_TOP3": "rank_composite",
    }
    pieces, summaries = [], []
    for name, rank_col in rules.items():
        pick = _select(sig, rank_col); pick["selection_policy"] = name; pick["net_return"] = _net(pick.gross_return)
        pieces.append(pick); summaries.extend(_summary(name, pick))
    summary = pd.DataFrame(summaries)
    all_trades = pd.concat(pieces, ignore_index=True)
    train = summary[summary.period.isin(["P2_TRAIN_202510_202512", "P3_TRAIN_202601_202603"])].copy()
    train_pivot = train.pivot(index="selection_policy", columns="period", values="avg_daily_equal_weight_net_return")
    train_counts = train.pivot(index="selection_policy", columns="period", values="trades")
    decision_rows = []
    for policy in rules:
        p2, p3 = float(train_pivot.loc[policy, "P2_TRAIN_202510_202512"]), float(train_pivot.loc[policy, "P3_TRAIN_202601_202603"])
        c2, c3 = int(train_counts.loc[policy, "P2_TRAIN_202510_202512"]), int(train_counts.loc[policy, "P3_TRAIN_202601_202603"])
        eligible = p2 > 0 and p3 > 0 and c2 >= 100 and c3 >= 100
        decision_rows.append({"selection_policy": policy, "p2_daily_net": p2, "p3_daily_net": p3, "p2_trades": c2, "p3_trades": c3, "train_mean_daily_net": (p2+p3)/2, "eligible_for_oos_selection": eligible})
    decision = pd.DataFrame(decision_rows).sort_values("train_mean_daily_net", ascending=False)
    eligible = decision[decision.eligible_for_oos_selection]
    chosen = str(eligible.iloc[0].selection_policy) if not eligible.empty else ""
    p4 = summary[summary.period.eq("P4_OOS_202604_202606")].copy()
    decision = decision.merge(p4[["selection_policy", "trades", "avg_daily_equal_weight_net_return", "avg_trade_net_return"]], on="selection_policy", how="left").rename(columns={"trades": "p4_oos_trades", "avg_daily_equal_weight_net_return": "p4_oos_daily_net", "avg_trade_net_return": "p4_oos_trade_net"})
    decision["selected_by_train_only"] = decision.selection_policy.eq(chosen)
    decision["oos_result"] = decision.apply(lambda r: "NOT_SELECTED" if not r.selected_by_train_only else ("OOS_POSITIVE" if r.p4_oos_daily_net > 0 else "OOS_NEGATIVE"), axis=1)
    summary.to_csv(f"{PREFIX}_summary_latest.csv", index=False, encoding="utf-8-sig")
    decision.to_csv(f"{PREFIX}_decision_latest.csv", index=False, encoding="utf-8-sig")
    all_trades[["selection_policy", "date", "code", "market", "session_index", "residual_ret20", "value", "gross_return", "net_return"]].to_csv(f"{PREFIX}_trades_latest.csv", index=False, encoding="utf-8-sig")
    payload = {"generated_at": datetime.now().isoformat(timespec="seconds"), "scope": "read_only_time_ordered_execution_policy_selection", "population": "residual-momentum top quintile; same-close; h5; daily top3; no reentry while open", "train": ["P2_202510_202512", "P3_202601_202603"], "oos": "P4_202604_202606", "selection_rule": "P2 and P3 daily net return must both be positive with >=100 trades", "selected_policy": chosen or None, "decision": decision.to_dict(orient="records"), "price_history_contract": integrity, "operational_change": False, "broker_order": False}
    Path(f"{PREFIX}_latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(f"{PREFIX}_latest.md").write_text("# Execution Policy OOS Selection\n\n" + summary.to_csv(index=False) + "\n## Decision\n\n" + decision.to_csv(index=False), encoding="utf-8")
    print(json.dumps({"status":"OK", "signals":len(sig), "selected_policy":chosen or None}, ensure_ascii=False))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
