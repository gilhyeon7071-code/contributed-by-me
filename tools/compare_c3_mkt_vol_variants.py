from __future__ import annotations

import csv
import importlib.util
import json
import os
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT = Path(r"E:\1_Data")
LOG_DIR = ROOT / "2_Logs"
REPORT_BACKTEST = ROOT / "report_backtest_v41_1.py"
SELECTION_CONTRACT = LOG_DIR / "c3_normal_official_replay_selection_contract_latest.json"
RESEARCH_PARAMS = LOG_DIR / "c3_strategy_replay_params_latest.json"

LATEST_JSON = LOG_DIR / "c3_mkt_vol_variant_compare_latest.json"
LATEST_SUMMARY_CSV = LOG_DIR / "c3_mkt_vol_variant_compare_summary_latest.csv"
LATEST_TRADES_CSV = LOG_DIR / "c3_mkt_vol_variant_compare_trades_latest.csv"
LATEST_MD = LOG_DIR / "c3_mkt_vol_variant_compare_latest.md"
RUN_DIR = LOG_DIR / "c3_mkt_vol_variant_compare_runtime"

STATUS = "READ_ONLY_C3_MKT_VOL_VARIANT_COMPARE_NOT_OPERATIONAL"


def _load_report_module() -> Any:
    spec = importlib.util.spec_from_file_location("report_backtest_v41_1_for_c3_mkt_vol_compare", REPORT_BACKTEST)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {REPORT_BACKTEST}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _profit_factor(ret: Iterable[float]) -> float | str:
    gains = 0.0
    losses = 0.0
    for value in ret:
        if value > 0:
            gains += float(value)
        elif value < 0:
            losses += abs(float(value))
    if losses > 0:
        return round(gains / losses, 6)
    if gains > 0:
        return "inf"
    return "NA"


def _summarize(variant: str, segment: str, df: pd.DataFrame, note: str = "") -> dict[str, Any]:
    ret = pd.to_numeric(df.get("ret", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    if df.empty:
        return {
            "variant": variant,
            "segment": segment,
            "n": 0,
            "win_n": 0,
            "loss_n": 0,
            "win_rate": "NA",
            "ret_sum": 0.0,
            "ret_mean": "NA",
            "profit_factor": "NA",
            "first_entry_date": "",
            "last_entry_date": "",
            "unique_codes": 0,
            "note": note,
        }
    return {
        "variant": variant,
        "segment": segment,
        "n": int(len(df)),
        "win_n": int((ret > 0).sum()),
        "loss_n": int((ret < 0).sum()),
        "win_rate": round(float((ret > 0).mean()), 6),
        "ret_sum": round(float(ret.sum()), 6),
        "ret_mean": round(float(ret.mean()), 6),
        "profit_factor": _profit_factor(ret.tolist()),
        "first_entry_date": str(df["entry_date_norm"].min()),
        "last_entry_date": str(df["entry_date_norm"].max()),
        "unique_codes": int(df["code"].nunique()) if "code" in df.columns else 0,
        "note": note,
    }


def _segment(df: pd.DataFrame, start: str = "", end: str = "") -> pd.DataFrame:
    if df.empty:
        return df.copy()
    mask = pd.Series(True, index=df.index)
    if start:
        mask &= df["entry_date_norm"] >= start
    if end:
        mask &= df["entry_date_norm"] <= end
    return df[mask].copy()


def _norm_trade_dates(trades: pd.DataFrame) -> pd.DataFrame:
    out = trades.copy()
    if out.empty:
        out["entry_date_norm"] = pd.Series(dtype=str)
        return out
    out["entry_date_norm"] = pd.to_datetime(out["entry_date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    return out


def _field_name(raw_field: str) -> str:
    return {
        "signal_rs": "rs",
        "signal_v_accel": "v_accel",
        "signal_stretch": "stretch",
        "signal_atr_pct": "atr_pct",
        "signal_rsi14": "rsi14",
        "signal_vol_close_corr20": "vol_close_corr20",
        "signal_high_52w_gap": "high_52w_gap",
        "signal_listing_days": "listing_days",
        "signal_ret1_pct": "ret1_pct",
        "signal_value": "value",
    }.get(raw_field, raw_field)


def _apply_filter(sig: pd.DataFrame, item: dict[str, Any]) -> pd.DataFrame:
    raw_field = str(item.get("field", "")).strip()
    op = str(item.get("op", "")).strip()
    value = float(item.get("value"))
    if raw_field == "entry_gap_pct":
        series = (pd.to_numeric(sig["n_open"], errors="coerce") - pd.to_numeric(sig["close"], errors="coerce")) / (
            pd.to_numeric(sig["close"], errors="coerce") + 1e-9
        )
    else:
        series = pd.to_numeric(sig[_field_name(raw_field)], errors="coerce")
    if op == "<=":
        mask = series <= value
    elif op == ">=":
        mask = series >= value
    elif op == "<":
        mask = series < value
    elif op == ">":
        mask = series > value
    elif op == "==":
        mask = series == value
    elif op == "!=":
        mask = series != value
    else:
        raise ValueError(f"unsupported op: {op}")
    return sig[mask.fillna(False)].copy()


def _pre_mkt_vol_pool(sig_all: pd.DataFrame, contract: dict[str, Any], start: str, end: str) -> pd.DataFrame:
    sig = sig_all[(sig_all["date"] >= pd.Timestamp(start)) & (sig_all["date"] <= pd.Timestamp(end))].copy()
    allowed_regimes = {str(x).upper() for x in contract.get("regimes", []) if str(x).strip()}
    if allowed_regimes:
        sig = sig[sig["market_regime"].astype(str).str.upper().isin(allowed_regimes)].copy()
    for item in list(contract.get("numeric_filters") or []):
        if str(item.get("field", "")).strip() == "mkt_vol20":
            break
        sig = _apply_filter(sig, item)
        if sig.empty:
            break
    return sig


def _replace_mkt_vol_filter(contract: dict[str, Any], value: float | None) -> dict[str, Any]:
    out = deepcopy(contract)
    filters = []
    for item in list(out.get("numeric_filters") or []):
        if str(item.get("field", "")).strip() == "mkt_vol20":
            if value is None:
                continue
            new_item = dict(item)
            new_item["value"] = float(value)
            filters.append(new_item)
        else:
            filters.append(dict(item))
    out["numeric_filters"] = filters
    return out


def _write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "variant",
        "segment",
        "n",
        "win_n",
        "loss_n",
        "win_rate",
        "ret_sum",
        "ret_mean",
        "profit_factor",
        "first_entry_date",
        "last_entry_date",
        "unique_codes",
        "note",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_md(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# C3 mkt_vol20 Variant Compare",
        "",
        f"- status: `{payload['status']}`",
        f"- created_at: `{payload['created_at']}`",
        f"- conclusion: `{payload['conclusion']}`",
        f"- full_logic_application: `{payload['operation_effect']['full_logic_application']}`",
        "",
        "## Variant Thresholds",
        "",
        "| variant | mkt_vol20 rule | note |",
        "|---|---|---|",
    ]
    for row in payload["variant_definitions"]:
        lines.append(f"| {row['variant']} | {row['mkt_vol20_rule']} | {row['note']} |")
    lines.extend(
        [
            "",
            "## Segment Summary",
            "",
            "| variant | segment | n | win_rate | ret_sum | profit_factor | first | last |",
            "|---|---|---:|---:|---:|---:|---|---|",
        ]
    )
    for row in payload["summary_rows"]:
        lines.append(
            "| {variant} | {segment} | {n} | {win_rate} | {ret_sum} | {profit_factor} | {first_entry_date} | {last_entry_date} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Read-only comparison only.",
            "- No candidate generation, official backtest source, HPO, paper/live order, gate, stable parameter, or policy file was changed.",
            "- The relaxed threshold variants are diagnostic sensitivity checks, not approved operating rules.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    os.environ["REPORT_RESEARCH_MODE"] = "1"
    os.environ["REPORT_RESEARCH_PARAMS_PATH"] = str(RESEARCH_PARAMS)
    os.environ["REPORT_RESEARCH_SELECTION_CONTRACT_PATH"] = str(SELECTION_CONTRACT)
    os.environ["REPORT_RESEARCH_OUTPUT_DIR"] = str(RUN_DIR)
    os.environ["REPORT_ALLOW_UNAPPROVED_FALLBACK"] = "1"

    report = _load_report_module()
    params = report.load_params()
    contract = report._load_research_selection_contract()
    if not isinstance(contract, dict):
        raise RuntimeError("selection contract was not loaded")

    data = report.load_data()
    factors = report.compute_factors(data)
    context = report.prepare_simulation_context(factors)
    sig_all = context["sig"].copy()

    pre_2026 = _pre_mkt_vol_pool(sig_all, contract, "2026-01-01", "2026-06-30")
    pre_vol = pd.to_numeric(pre_2026.get("mkt_vol20", pd.Series(dtype=float)), errors="coerce").dropna()
    if pre_vol.empty:
        raise RuntimeError("no pre-mkt_vol20 pool for 2026H1")

    current_value = next(float(x["value"]) for x in contract.get("numeric_filters", []) if x.get("field") == "mkt_vol20")
    threshold_min = float(pre_vol.min())
    threshold_q25 = float(pre_vol.quantile(0.25))
    threshold_median = float(pre_vol.quantile(0.50))

    variant_specs = [
        {
            "variant": "hard_current_mkt_vol20",
            "contract": deepcopy(contract),
            "mkt_vol20_rule": f"mkt_vol20 <= {current_value}",
            "note": "current C3 proxy condition",
        },
        {
            "variant": "remove_mkt_vol20_only",
            "contract": _replace_mkt_vol_filter(contract, None),
            "mkt_vol20_rule": "removed",
            "note": "sensitivity check; keeps all other C3 filters",
        },
        {
            "variant": "relax_mkt_vol20_min_2026_pool",
            "contract": _replace_mkt_vol_filter(contract, threshold_min),
            "mkt_vol20_rule": f"mkt_vol20 <= {threshold_min:.10f}",
            "note": "minimum threshold that can begin allowing 2026H1 pre-vol pool rows; diagnostic only",
        },
        {
            "variant": "relax_mkt_vol20_q25_2026_pool",
            "contract": _replace_mkt_vol_filter(contract, threshold_q25),
            "mkt_vol20_rule": f"mkt_vol20 <= {threshold_q25:.10f}",
            "note": "2026H1 pre-vol pool first quartile threshold; diagnostic only",
        },
        {
            "variant": "relax_mkt_vol20_median_2026_pool",
            "contract": _replace_mkt_vol_filter(contract, threshold_median),
            "mkt_vol20_rule": f"mkt_vol20 <= {threshold_median:.10f}",
            "note": "2026H1 pre-vol pool median threshold; diagnostic only",
        },
    ]

    all_trades: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for spec in variant_specs:
        trades = report.simulate_trades(factors, params, selection_contract=spec["contract"], simulation_context=context)
        trades = _norm_trade_dates(trades)
        trades["variant"] = spec["variant"]
        trades["mkt_vol20_rule"] = spec["mkt_vol20_rule"]
        all_trades.append(trades)
        summary_rows.extend(
            [
                _summarize(spec["variant"], "all_contract_window", trades, spec["note"]),
                _summarize(spec["variant"], "reference_2025H2", _segment(trades, "2025-07-01", "2025-12-31"), spec["note"]),
                _summarize(spec["variant"], "target_2026H1", _segment(trades, "2026-01-01", "2026-06-30"), spec["note"]),
            ]
        )

    combined = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    combined.to_csv(LATEST_TRADES_CSV, index=False, encoding="utf-8-sig")

    target_rows = [r for r in summary_rows if r["segment"] == "target_2026H1"]
    remove_target = next(r for r in target_rows if r["variant"] == "remove_mkt_vol20_only")
    hard_target = next(r for r in target_rows if r["variant"] == "hard_current_mkt_vol20")
    if int(hard_target["n"]) == 0 and int(remove_target["n"]) > 0:
        conclusion = (
            "Removing mkt_vol20 alone revives 2026H1 trades; compare returns before considering any adaptive threshold."
        )
    else:
        conclusion = "mkt_vol20 is not the only blocker after full simulation."

    payload = {
        "status": STATUS,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_artifacts": {
            "report_backtest": str(REPORT_BACKTEST),
            "selection_contract": str(SELECTION_CONTRACT),
            "research_params": str(RESEARCH_PARAMS),
        },
        "data_rows": int(len(data)),
        "factor_rows": int(len(factors)),
        "signal_rows": int(len(sig_all)),
        "pre_mkt_vol_2026h1_pool": {
            "rows": int(len(pre_2026)),
            "dates": int(pre_2026["date"].nunique()) if not pre_2026.empty else 0,
            "codes": int(pre_2026["code"].nunique()) if not pre_2026.empty else 0,
            "current_threshold": current_value,
            "min": threshold_min,
            "q25": threshold_q25,
            "median": threshold_median,
            "max": float(pre_vol.max()),
        },
        "variant_definitions": [
            {"variant": spec["variant"], "mkt_vol20_rule": spec["mkt_vol20_rule"], "note": spec["note"]}
            for spec in variant_specs
        ],
        "summary_rows": summary_rows,
        "conclusion": conclusion,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_live_order_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "next_required": [
            "If a relaxed threshold shows positive 2026H1 trades, test it on additional windows before any policy change.",
            "If removing mkt_vol20 hurts reference 2025H2 badly, keep it as a regime-specific guard or search a different regime split.",
        ],
    }

    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_summary_csv(LATEST_SUMMARY_CSV, summary_rows)
    _write_md(LATEST_MD, payload)
    print(
        json.dumps(
            {
                "status": STATUS,
                "json": str(LATEST_JSON),
                "summary_csv": str(LATEST_SUMMARY_CSV),
                "trades_csv": str(LATEST_TRADES_CSV),
                "md": str(LATEST_MD),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
