from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

OUT_JSON = LOG_DIR / "current_regime_methodology_map_latest.json"
OUT_REGIME_CSV = LOG_DIR / "current_regime_methodology_map_regimes_latest.csv"
OUT_BRANCH_CSV = LOG_DIR / "current_regime_methodology_map_branches_latest.csv"
OUT_MD = LOG_DIR / "current_regime_methodology_map_latest.md"

CURRENT_SAMPLE = "current_2026_07_01_to_data_max"
REGIMES = ("STRESS", "BEAR", "TRANSITION")


INPUTS = {
    "recent_regime_distribution": LOG_DIR / "recent_regime_distribution_202606_202607_latest.json",
    "stress_summary": LOG_DIR / "stress_bear_signal_direction_scan_summary_latest.csv",
    "stress_rows": LOG_DIR / "stress_bear_signal_direction_scan_rows_latest.csv",
    "stress_replay": LOG_DIR / "stress_tradability_h2_replay_latest.json",
    "stress_pending": LOG_DIR / "stress_h2_pending_resolution_latest.json",
    "bear_summary": LOG_DIR / "bear_signal_direction_scan_summary_latest.csv",
    "bear_rows": LOG_DIR / "bear_signal_direction_scan_rows_latest.csv",
    "transition_summary": LOG_DIR / "transition_signal_direction_scan_summary_latest.csv",
    "transition_rows": LOG_DIR / "transition_signal_direction_scan_rows_latest.csv",
}


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def clean_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def load_summary(regime: str) -> pd.DataFrame:
    if regime == "STRESS":
        df = pd.read_csv(INPUTS["stress_summary"])
        return df[df["market_regime"].astype(str).eq("STRESS")].copy()
    if regime == "BEAR":
        return pd.read_csv(INPUTS["bear_summary"])
    if regime == "TRANSITION":
        return pd.read_csv(INPUTS["transition_summary"])
    raise ValueError(regime)


def load_rows(regime: str) -> pd.DataFrame:
    if regime == "STRESS":
        df = pd.read_csv(INPUTS["stress_rows"])
        return df[df["market_regime"].astype(str).eq("STRESS")].copy()
    if regime == "BEAR":
        return pd.read_csv(INPUTS["bear_rows"])
    if regime == "TRANSITION":
        return pd.read_csv(INPUTS["transition_rows"])
    raise ValueError(regime)


def top_branch(summary: pd.DataFrame, sample: str) -> dict[str, Any] | None:
    positive = summary[
        summary["sample"].astype(str).eq(sample)
        & summary["decision_flag"].astype(str).eq("POSITIVE_BRANCH")
    ].copy()
    if positive.empty:
        return None
    positive = positive.sort_values(["profit_factor", "ret_mean", "n"], ascending=[False, False, False])
    return positive.iloc[0].astype(object).where(pd.notna(positive.iloc[0]), None).to_dict()


def count_branch_dates(rows: pd.DataFrame, branch: dict[str, Any] | None) -> int:
    if not branch or rows.empty:
        return 0
    horizon = str(branch["horizon"])
    ret_col = f"fwd_ret_{horizon}"
    if ret_col not in rows.columns:
        return 0
    mask = (
        rows["matched_signal"].astype(str).eq(str(branch["signal"]))
        & rows["matched_direction"].astype(str).eq(str(branch["direction"]))
        & pd.to_numeric(rows[ret_col], errors="coerce").notna()
    )
    return int(pd.to_datetime(rows.loc[mask, "date"], errors="coerce").dt.strftime("%Y-%m-%d").nunique())


def classify_regime(regime: str, current_positive_count: int, date_count: int, top: dict[str, Any] | None, stress_pending: dict[str, Any]) -> tuple[str, str, int]:
    if regime == "STRESS":
        pending_rows = int(stress_pending.get("row_counts", {}).get("h2_still_pending_rows", 0))
        if pending_rows > 0:
            return (
                "COMPOSITE_REPLAY_CANDIDATE_PRICE_PENDING",
                "Keep as first replay priority after h2 pending rows close; do not promote while latest h2 rows remain unresolved.",
                1,
            )
        return (
            "COMPOSITE_REPLAY_CANDIDATE_REFRESH_READY",
            "Rerun STRESS composite replay because pending h2 rows are resolved.",
            1,
        )
    if current_positive_count <= 0 or top is None:
        return ("NO_CURRENT_BRANCH", "Hold; no current positive single-signal branch found.", 9)
    if date_count >= 2:
        return (
            "OBSERVATION_TO_COMPOSITE_CANDIDATE",
            "Build a small composite replay around the current branch before any operational discussion.",
            2,
        )
    return (
        "OBSERVATION_CANDIDATE_SMALL_DATE_COUNT",
        "Keep as observation only until more signal dates appear.",
        3,
    )


def main() -> int:
    missing = [str(path) for path in INPUTS.values() if not path.exists()]
    if missing:
        raise SystemExit("missing inputs: " + "; ".join(missing))

    recent = read_json(INPUTS["recent_regime_distribution"])
    stress_replay = read_json(INPUTS["stress_replay"])
    stress_pending = read_json(INPUTS["stress_pending"])

    current_regime_counts = recent.get("current_regime_row_counts", {})
    regime_rows: list[dict[str, Any]] = []
    branch_rows: list[dict[str, Any]] = []

    for regime in REGIMES:
        summary = load_summary(regime)
        rows = load_rows(regime)
        current_positive = summary[
            summary["sample"].astype(str).eq(CURRENT_SAMPLE)
            & summary["decision_flag"].astype(str).eq("POSITIVE_BRANCH")
        ].copy()
        historical_positive = summary[
            summary["sample"].astype(str).eq("historical_2020_to_2026_06_30")
            & summary["decision_flag"].astype(str).eq("POSITIVE_BRANCH")
        ].copy()
        top = top_branch(summary, CURRENT_SAMPLE)
        branch_date_count = count_branch_dates(rows, top)
        status, next_action, priority = classify_regime(regime, len(current_positive), branch_date_count, top, stress_pending)

        top_payload = top or {}
        regime_rows.append({
            "regime": regime,
            "current_row_count": int(current_regime_counts.get(regime, 0)),
            "current_positive_count": int(len(current_positive)),
            "historical_positive_count": int(len(historical_positive)),
            "top_signal": top_payload.get("signal"),
            "top_direction": top_payload.get("direction"),
            "top_horizon": top_payload.get("horizon"),
            "top_family_hint": top_payload.get("family_hint"),
            "top_n": top_payload.get("n"),
            "top_win_rate": top_payload.get("win_rate"),
            "top_ret_mean": top_payload.get("ret_mean"),
            "top_profit_factor": top_payload.get("profit_factor"),
            "top_first_signal_date": top_payload.get("first_signal_date"),
            "top_last_signal_date": top_payload.get("last_signal_date"),
            "top_signal_date_count": branch_date_count,
            "methodology_status": status,
            "replay_priority": priority,
            "next_action": next_action,
        })

        if not current_positive.empty:
            current_positive = current_positive.sort_values(["profit_factor", "ret_mean", "n"], ascending=[False, False, False])
            current_positive.insert(0, "regime_priority", priority)
            branch_rows.extend(clean_records(current_positive))

    regime_df = pd.DataFrame(regime_rows).sort_values(["replay_priority", "regime"]).reset_index(drop=True)
    branch_df = pd.DataFrame(branch_rows)
    if not branch_df.empty:
        branch_df = branch_df.sort_values(["regime_priority", "market_regime", "profit_factor", "ret_mean"], ascending=[True, True, False, False]).reset_index(drop=True)

    stress_primary = stress_replay.get("primary_summary", {})
    payload = {
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
        "scope": "current_regime_methodology_map_read_only",
        "classification": "CURRENT_REGIME_METHODOLOGY_MAP_READ_ONLY",
        "operational_decision": "NOT_APPROVED",
        "full_logic_application": "NOT_APPLIED",
        "conclusion": "CURRENT_REGIME_MAP_BUILT_READ_ONLY",
        "data_window": recent.get("data_window", {}),
        "inputs": {key: str(path) for key, path in INPUTS.items()},
        "regime_map": clean_records(regime_df),
        "current_positive_branches": clean_records(branch_df),
        "stress_replay_context": {
            "conclusion": stress_replay.get("conclusion"),
            "primary_variant": stress_replay.get("primary_variant"),
            "primary_summary": stress_primary,
            "pending_conclusion": stress_pending.get("conclusion"),
            "pending_row_counts": stress_pending.get("row_counts"),
        },
        "operation_effect": {
            "candidate_generation": False,
            "official_backtest": False,
            "hpo": False,
            "diagnostics": False,
            "paper_or_live": False,
            "parameter_or_gate_change": False,
        },
        "validation": [
            "all_required_inputs_exist: PASS",
            "three_current_regimes_mapped: PASS" if set(regime_df["regime"]) == set(REGIMES) else "three_current_regimes_mapped: FAIL",
            "current_positive_branch_rows_loaded: PASS" if len(branch_df) > 0 else "current_positive_branch_rows_loaded: FAIL",
            "operation_effect_no_changes: PASS",
        ],
    }
    if any(item.endswith("FAIL") for item in payload["validation"]):
        raise SystemExit("validation failed: " + "; ".join(payload["validation"]))

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    regime_df.to_csv(OUT_REGIME_CSV, index=False, encoding="utf-8-sig")
    branch_df.to_csv(OUT_BRANCH_CSV, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    md = [
        "# Current Regime Methodology Map",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['classification']}",
        f"- conclusion: {payload['conclusion']}",
        f"- audit_window: {payload['data_window'].get('audit_start')} ~ {payload['data_window'].get('audit_end_effective')}",
        f"- current_start: {payload['data_window'].get('current_start')}",
        "",
        "## Regime Decisions",
        "",
    ]
    for row in payload["regime_map"]:
        md.append(
            f"- {row['regime']}: status={row['methodology_status']}, priority={row['replay_priority']}, "
            f"current_rows={row['current_row_count']}, current_positive={row['current_positive_count']}, "
            f"top={row['top_signal']} {row['top_direction']} {row['top_horizon']}, "
            f"pf={row['top_profit_factor']}, mean={row['top_ret_mean']}, dates={row['top_signal_date_count']}"
        )
        md.append(f"  - next: {row['next_action']}")
    md += [
        "",
        "## STRESS Replay Context",
        "",
        f"- replay_conclusion: {payload['stress_replay_context']['conclusion']}",
        f"- pending_conclusion: {payload['stress_replay_context']['pending_conclusion']}",
        f"- pending_row_counts: {payload['stress_replay_context']['pending_row_counts']}",
        "",
        "## Operation Effect",
        "",
        "- NOT_APPLIED to candidate generation, official backtest, HPO, diagnostics, paper/live, gates, thresholds, or parameters.",
    ]
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] wrote {OUT_JSON}")
    print(f"[OK] wrote {OUT_REGIME_CSV}")
    print(f"[OK] wrote {OUT_BRANCH_CSV}")
    print(f"[OK] wrote {OUT_MD}")
    print("[CONCLUSION] CURRENT_REGIME_MAP_BUILT_READ_ONLY")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
