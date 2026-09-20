"""Build a read-only candidate bridge policy simulation.

This classifies observe-only general candidates into proposed bridge buckets.
It does not change candidate eligibility, paper_engine, orders, fills, gates,
sizing, operational ledger, or stats.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype={"code": str})


def _num(value: Any, default: float = 0.0) -> float:
    try:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(parsed):
            return float(default)
        return float(parsed)
    except Exception:
        return float(default)


def _flag(row: pd.Series, col: str) -> bool:
    return str(row.get(col) or "").strip().lower() in {"true", "1", "yes", "y"}


def build_simulation(root: Path) -> dict[str, Any]:
    log_dir = root / "2_Logs"
    candidate_path = log_dir / "candidates_latest_data.csv"
    quality_path = log_dir / "bear_survivor_observe_quality_diagnostic_latest.csv"
    candidates = _read_csv(candidate_path)
    quality = _read_csv(quality_path)

    if not candidates.empty:
        candidates = candidates.copy()
        candidates["code"] = candidates["code"].astype(str).str.zfill(6)
    if not quality.empty:
        quality = quality.copy()
        quality["code"] = quality["code"].astype(str).str.zfill(6)
        cols = [
            col
            for col in [
                "code",
                "regime_bucket",
                "quality_bucket",
                "observe_quality",
                "observe_promotion_status",
                "promotion_status",
            ]
            if col in quality.columns
        ]
        candidates = candidates.merge(quality[cols], on="code", how="left", suffixes=("", "_q"))

    rows: list[dict[str, Any]] = []
    for _, row in candidates.iterrows():
        code = str(row.get("code") or "").zfill(6)
        final_score = _num(row.get("final_score"), 0.0)
        fundamental_score = _num(row.get("fundamental_score"), 0.0)
        rs = _num(row.get("rs"), 0.0)
        ret1_pct = _num(row.get("ret1_pct"), 0.0)
        trading_value = _num(row.get("trading_value"), 0.0)
        regime_bucket = str(row.get("regime_bucket") or "").strip()
        quality_bucket = str(row.get("quality_bucket") or row.get("observe_quality") or "").strip()

        reasons: list[str] = []
        hard_watch = any(_flag(row, col) for col in ["krx_admin", "krx_warning", "krx_risk", "krx_caution"])
        weak_fund = fundamental_score < 50.0
        if hard_watch:
            reasons.append("krx_watch_or_caution")
        if weak_fund:
            reasons.append("fundamental_score_below_50")
        if final_score < 0.50:
            reasons.append("final_score_below_0.50")
        if regime_bucket != "BEAR_SURVIVOR_OBSERVE":
            reasons.append("not_bear_survivor_bucket")
        if ret1_pct <= -11.0:
            reasons.append("ret1_deep_loss_watch")
        if trading_value < 100_000_000_000:
            reasons.append("trading_value_below_100b")

        if hard_watch or weak_fund or final_score < 0.50:
            decision = "BLOCK"
            entry_eligible = False
            size_multiplier = 0.0
        elif (
            regime_bucket == "BEAR_SURVIVOR_OBSERVE"
            and final_score >= 0.70
            and fundamental_score >= 55.0
            and rs >= 4.0
            and trading_value >= 100_000_000_000
            and ret1_pct > -11.0
        ):
            decision = "ALLOW_REDUCED"
            entry_eligible = True
            size_multiplier = 0.25
            reasons.append("conservative_bear_survivor_bridge")
        elif (
            regime_bucket == "BEAR_SURVIVOR_OBSERVE"
            and final_score >= 0.54
            and fundamental_score >= 53.0
            and trading_value >= 90_000_000_000
        ):
            decision = "REDUCE_RECHECK"
            entry_eligible = False
            size_multiplier = 0.0
            reasons.append("needs_recheck_before_entry")
        else:
            decision = "OBSERVE"
            entry_eligible = False
            size_multiplier = 0.0

        rows.append(
            {
                "code": code,
                "name": row.get("name", ""),
                "date": row.get("date", ""),
                "relax_level": row.get("relax_level", ""),
                "regime_bucket": regime_bucket,
                "quality_bucket": quality_bucket,
                "final_score": final_score,
                "fundamental_score": fundamental_score,
                "rs": rs,
                "ret1_pct": ret1_pct,
                "trading_value": trading_value,
                "proposed_decision_read_only": decision,
                "entry_eligible_read_only": entry_eligible,
                "proposed_size_multiplier": size_multiplier,
                "reasons": "|".join(reasons) if reasons else "passes_conservative_bridge",
            }
        )

    summary: dict[str, int] = {}
    for row in rows:
        decision = str(row["proposed_decision_read_only"])
        summary[decision] = summary.get(decision, 0) + 1

    generated_at = datetime.now().isoformat(timespec="seconds")
    payload = {
        "generated_at": generated_at,
        "scope": "read_only_candidate_bridge_policy_simulation",
        "policy_effect": "read_only_no_candidate_order_fill_ledger_change",
        "source_candidates": str(candidate_path),
        "source_quality": str(quality_path),
        "candidate_rows": len(rows),
        "summary": summary,
        "entry_eligible_read_only_count": sum(1 for row in rows if row["entry_eligible_read_only"]),
        "policy_note": "This simulation is not wired into generate_candidates_v41_1.py or paper_engine.py.",
        "rows": rows,
        "limitations": [
            "Thresholds are diagnostic and not approved trading policy.",
            "Live quote freshness and execution quality are not verified here.",
            "No orders, fills, gates, sizing, operational ledger, or stats are modified.",
        ],
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = log_dir / f"candidate_bridge_policy_simulation_{stamp}.json"
    latest_json = log_dir / "candidate_bridge_policy_simulation_latest.json"
    csv_path = log_dir / f"candidate_bridge_policy_simulation_{stamp}.csv"
    latest_csv = log_dir / "candidate_bridge_policy_simulation_latest.csv"
    for path in [json_path, latest_json]:
        path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    fields = list(rows[0].keys()) if rows else [
        "code",
        "name",
        "date",
        "proposed_decision_read_only",
    ]
    for path in [csv_path, latest_csv]:
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    payload["outputs"] = {
        "json": str(json_path),
        "latest_json": str(latest_json),
        "csv": str(csv_path),
        "latest_csv": str(latest_csv),
    }
    latest_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="RootA path")
    args = parser.parse_args()
    payload = build_simulation(Path(args.root).resolve())
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
