from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
GRID_JSON = LOG_DIR / "c3_highvol_entry_grid_hold7_latest.json"
GRID_ROWS = LOG_DIR / "c3_highvol_entry_grid_hold7_rows_latest.csv"

OUT_JSON = LOG_DIR / "c3_highvol_best_grid_row_review_latest.json"
OUT_ROWS = LOG_DIR / "c3_highvol_best_grid_row_review_rows_latest.csv"
OUT_GROUPS = LOG_DIR / "c3_highvol_best_grid_row_review_groups_latest.csv"
OUT_FEATURES = LOG_DIR / "c3_highvol_best_grid_row_review_features_latest.csv"
OUT_MD = LOG_DIR / "c3_highvol_best_grid_row_review_latest.md"


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _profit_factor(ret: pd.Series) -> float | None:
    gains = float(ret[ret > 0].sum())
    losses = float(-ret[ret < 0].sum())
    if losses == 0:
        return None if gains > 0 else 0.0
    return gains / losses


def _summarize(frame: pd.DataFrame, group_type: str, group_value: str) -> dict[str, Any]:
    ret = _num(frame["ret"]) if len(frame) else pd.Series(dtype=float)
    return {
        "group_type": group_type,
        "group_value": group_value,
        "n": int(len(frame)),
        "win_n": int((ret > 0).sum()) if len(ret) else 0,
        "loss_n": int((ret < 0).sum()) if len(ret) else 0,
        "stop_n": int((frame["exit_reason"].astype(str).str.upper() == "STOP").sum()) if len(frame) else 0,
        "ret_sum": float(ret.sum()) if len(ret) else 0.0,
        "ret_mean": float(ret.mean()) if len(ret) else None,
        "profit_factor": _profit_factor(ret),
        "first_entry_date": str(frame["entry_date_norm"].min()) if len(frame) else None,
        "last_entry_date": str(frame["entry_date_norm"].max()) if len(frame) else None,
        "unique_codes": int(frame["code"].nunique()) if len(frame) and "code" in frame.columns else 0,
    }


def _add_time_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    dt = pd.to_datetime(out["entry_date_norm"], errors="coerce")
    out["entry_half"] = dt.dt.year.astype("Int64").astype(str) + "H" + (((dt.dt.month - 1) // 6) + 1).astype("Int64").astype(str)
    out["entry_month"] = dt.dt.strftime("%Y-%m")
    return out


def _feature_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    fields = [
        "entry_gap_pct",
        "signal_v_accel",
        "signal_atr_pct",
        "signal_value",
        "signal_rs",
        "signal_stretch",
        "mfe_1d",
        "mae_1d",
        "entry_day_open_to_close_ret",
    ]
    masks = {
        "all": pd.Series(True, index=df.index),
        "win": _num(df["ret"]) > 0,
        "loss": _num(df["ret"]) < 0,
        "top5_win": df["ret_rank_desc"].le(5),
        "non_top5": df["ret_rank_desc"].gt(5),
        "stop": df["exit_reason"].astype(str).str.upper().eq("STOP"),
    }
    for bucket, mask in masks.items():
        part = df[mask.fillna(False)]
        for field in fields:
            values = _num(part[field]).dropna() if field in part.columns else pd.Series(dtype=float)
            rows.append(
                {
                    "bucket": bucket,
                    "field": field,
                    "n": int(len(values)),
                    "mean": float(values.mean()) if len(values) else None,
                    "median": float(values.median()) if len(values) else None,
                    "q25": float(values.quantile(0.25)) if len(values) else None,
                    "q75": float(values.quantile(0.75)) if len(values) else None,
                    "min": float(values.min()) if len(values) else None,
                    "max": float(values.max()) if len(values) else None,
                }
            )
    return rows


def _concentration(df: pd.DataFrame) -> dict[str, Any]:
    ret = _num(df["ret"]).dropna().sort_values(ascending=False)
    total = float(_num(df["ret"]).sum())
    gross_profit = float(ret[ret > 0].sum())
    out = {
        "ret_sum": total,
        "gross_profit": gross_profit,
        "top1_ret": float(ret.iloc[0]) if len(ret) else 0.0,
        "top3_ret": float(ret.iloc[:3].sum()) if len(ret) else 0.0,
        "top5_ret": float(ret.iloc[:5].sum()) if len(ret) else 0.0,
        "top1_share_gross_profit": float(ret.iloc[0] / gross_profit) if gross_profit > 0 and len(ret) else None,
        "top3_share_gross_profit": float(ret.iloc[:3].sum() / gross_profit) if gross_profit > 0 and len(ret) else None,
        "top5_share_gross_profit": float(ret.iloc[:5].sum() / gross_profit) if gross_profit > 0 and len(ret) else None,
        "ret_without_top1": float(total - ret.iloc[0]) if len(ret) else total,
        "ret_without_top3": float(total - ret.iloc[:3].sum()) if len(ret) else total,
        "ret_without_top5": float(total - ret.iloc[:5].sum()) if len(ret) else total,
    }
    return out


def _classify(conc: dict[str, Any], groups: pd.DataFrame) -> dict[str, Any]:
    reasons: list[str] = []
    if float(conc.get("ret_without_top5") or 0.0) < 0:
        reasons.append("ret_negative_without_top5")
    if float(conc.get("top5_share_gross_profit") or 0.0) > 0.6:
        reasons.append("top5_profit_concentration_high")
    half = groups[groups["group_type"].eq("entry_half")]
    negative_halves = half[half["ret_sum"] < 0]["group_value"].astype(str).tolist()
    if negative_halves:
        reasons.append("negative_halves:" + ",".join(negative_halves))
    if len(half[half["ret_sum"] > 0]) >= 3:
        reasons.append("multiple_positive_halves")
    return {
        "operational_decision": "NOT_APPROVED",
        "research_classification": "C3_HIGHVOL_BEST_GRID_OUTLIER_DEPENDENT",
        "confidence": "MEDIUM_RESEARCH_ONLY",
        "full_logic_application": "NOT_APPLIED",
        "reason": reasons or ["insufficient_evidence"],
    }


def _clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def main() -> int:
    if not GRID_JSON.exists() or not GRID_ROWS.exists():
        raise FileNotFoundError("missing grid outputs")
    meta = json.loads(GRID_JSON.read_text(encoding="utf-8"))
    best_rule = meta["classification"]["best_rule_id"]
    rows = pd.read_csv(GRID_ROWS, dtype={"code": str})
    best = rows[rows["rule_id"].astype(str).eq(best_rule)].copy()
    if best.empty:
        raise RuntimeError(f"no rows for best_rule={best_rule}")
    for col in ["ret", "entry_gap_pct", "signal_v_accel", "signal_atr_pct", "signal_value"]:
        best[col] = _num(best[col])
    best = _add_time_fields(best)
    best = best.sort_values("ret", ascending=False).reset_index(drop=True)
    best["ret_rank_desc"] = range(1, len(best) + 1)
    best["row_class"] = "middle"
    best.loc[best["ret_rank_desc"].le(5), "row_class"] = "top5_win"
    best.loc[best["ret"] < 0, "row_class"] = "loss"
    best.loc[best["exit_reason"].astype(str).str.upper().eq("STOP"), "row_class"] = "stop_loss"

    group_rows: list[dict[str, Any]] = [_summarize(best, "all", "all")]
    for col in ["entry_half", "entry_month", "market_regime", "exit_reason", "row_class", "code"]:
        for value, part in best.groupby(col, dropna=False, sort=True):
            group_rows.append(_summarize(part.copy(), col, str(value)))
    groups = pd.DataFrame(group_rows)
    features = pd.DataFrame(_feature_rows(best))
    conc = _concentration(best)
    classification = _classify(conc, groups)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_best_grid_row_level_review",
        "input": {
            "grid_json": str(GRID_JSON),
            "grid_rows": str(GRID_ROWS),
        },
        "best_rule_id": best_rule,
        "row_count": int(len(best)),
        "classification": classification,
        "concentration": conc,
        "operation_effect": {
            "candidate_generation_changed": False,
            "backtest_changed": False,
            "hpo_changed": False,
            "paper_or_broker_changed": False,
            "gate_or_threshold_changed": False,
            "policy_changed": False,
            "full_logic_application": "NOT_APPLIED",
        },
        "top_rows": _clean_records(best.head(10)),
        "bottom_rows": _clean_records(best.sort_values("ret").head(10)),
        "groups": _clean_records(groups),
        "features": _clean_records(features),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    best.to_csv(OUT_ROWS, index=False, encoding="utf-8-sig")
    groups.to_csv(OUT_GROUPS, index=False, encoding="utf-8-sig")
    features.to_csv(OUT_FEATURES, index=False, encoding="utf-8-sig")

    focus_groups = groups[
        groups["group_type"].isin(["all", "entry_half", "market_regime", "exit_reason", "row_class"])
    ].copy()
    lines = [
        "# C3 high-volatility best grid row review",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Best rule id: `{best_rule}`",
        f"- Row count: {len(best)}",
        "- Scope: read-only row-level review; no operating rule or gate changed.",
        "",
        "## Concentration",
        "",
        f"- ret_sum: {conc['ret_sum']:.6f}",
        f"- top1_ret: {conc['top1_ret']:.6f}",
        f"- top3_ret: {conc['top3_ret']:.6f}",
        f"- top5_ret: {conc['top5_ret']:.6f}",
        f"- top5_share_gross_profit: {conc['top5_share_gross_profit']:.6f}",
        f"- ret_without_top5: {conc['ret_without_top5']:.6f}",
        "",
        "## Focus groups",
        "",
        "| group_type | group_value | n | win | loss | stop | ret_sum | PF |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in focus_groups.to_dict(orient="records"):
        pf = row["profit_factor"]
        pf_text = "inf" if pd.isna(pf) and row["ret_sum"] > 0 else f"{pf:.6f}" if not pd.isna(pf) else ""
        lines.append(
            f"| {row['group_type']} | {row['group_value']} | {row['n']} | {row['win_n']} | {row['loss_n']} | "
            f"{row['stop_n']} | {row['ret_sum']:.6f} | {pf_text} |"
        )
    lines.extend(
        [
            "",
            "## Classification",
            "",
            f"- Research classification: `{classification['research_classification']}`",
            f"- Operational decision: `{classification['operational_decision']}`",
            f"- Full logic application: `{classification['full_logic_application']}`",
            f"- Confidence: `{classification['confidence']}`",
            f"- Reasons: {', '.join(classification['reason'])}",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "json": str(OUT_JSON),
                "rows": str(OUT_ROWS),
                "groups": str(OUT_GROUPS),
                "features": str(OUT_FEATURES),
                "md": str(OUT_MD),
                "classification": classification,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
