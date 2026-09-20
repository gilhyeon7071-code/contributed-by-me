from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

import paper_engine as pe
# [2026-09-13] These three live in submodules; the package __init__ never re-exported
#   them, so every pe.<name> below raised AttributeError and this step has been
#   failing rc=1 'advisory' - i.e. silently - since the package split.
from paper_engine.common import norm_code
from paper_engine.entry import (
    _apply_defense_signal_entry_policy,
    _load_defense_signal_entry_map,
)


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

CANDIDATE_PATHS = [
    LOG_DIR / "candidates_latest_data.with_final_score.csv",
    LOG_DIR / "candidates_latest_data.csv",
]
DEFENSE_CSV = LOG_DIR / "defense_signal_shadow_latest.csv"
OUT_JSON = LOG_DIR / "defense_signal_entry_policy_impact_latest.json"
OUT_CSV = LOG_DIR / "defense_signal_entry_policy_impact_latest.csv"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _read_candidates() -> tuple[pd.DataFrame, str]:
    for path in CANDIDATE_PATHS:
        if path.exists():
            return pd.read_csv(path, dtype={"code": str}), str(path)
    return pd.DataFrame(), ""


def _truthy(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _norm_code(value: Any) -> str:
    return norm_code(value)


def _row_is_surge(row: dict[str, Any]) -> bool:
    surge_type = str(row.get("surge_type") or "").strip().upper()
    if surge_type and surge_type != "NONE":
        return True
    for col in ("is_realtime_surge", "surge_flag", "_surge_immediate"):
        if _truthy(row.get(col)):
            return True
    return False


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "scope",
        "code",
        "name",
        "route_scope",
        "in_current_candidates",
        "row_is_surge",
        "would_block_general",
        "would_block_surge",
        "general_action_shadow",
        "surge_action_shadow",
        "defense_signal_score",
        "defense_reasons",
        "active_policy_result",
        "observe_only_result",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    cfg = dict(pe.load_config())  # not DEFAULT_CONFIG: the engine runs the merged
    #   effective config, and validating the defaults can pass while production differs
    policy = cfg.get("defense_signal_entry_policy") or {}

    candidates, candidate_source = _read_candidates()
    candidate_rows = candidates.to_dict("records") if not candidates.empty else []
    candidate_codes = {_norm_code(row.get("code")) for row in candidate_rows if _norm_code(row.get("code"))}

    defense_rows = []
    if DEFENSE_CSV.exists():
        defense_rows = pd.read_csv(DEFENSE_CSV, dtype={"code": str}).to_dict("records")

    defense_by_code = _load_defense_signal_entry_map(cfg)
    current_candidate_details: list[dict[str, Any]] = []
    for row in candidate_rows:
        code = _norm_code(row.get("code"))
        defense = defense_by_code.get(code, {})
        row_surge = _row_is_surge(row)
        general_block = bool(
            defense.get("would_block_general")
            and str(defense.get("general_action_shadow") or "").upper() in {"GENERAL_SHADOW_WAIT"}
        )
        surge_block = bool(
            row_surge
            and defense.get("would_block_surge")
            and str(defense.get("surge_action_shadow") or "").upper() in {"SURGE_SHADOW_BLOCK"}
        )
        active_result = "BLOCK_BY_DEFENSE_SIGNAL" if general_block or surge_block else "KEEP"
        current_candidate_details.append(
            {
                "scope": "current_candidate",
                "code": code,
                "name": row.get("name") or "",
                "route_scope": defense.get("route_scope") or "",
                "in_current_candidates": True,
                "row_is_surge": row_surge,
                "would_block_general": bool(defense.get("would_block_general", False)),
                "would_block_surge": bool(defense.get("would_block_surge", False)),
                "general_action_shadow": defense.get("general_action_shadow") or "",
                "surge_action_shadow": defense.get("surge_action_shadow") or "",
                "defense_signal_score": defense.get("defense_signal_score", 0.0),
                "defense_reasons": defense.get("defense_reasons") or "",
                "active_policy_result": active_result,
                "observe_only_result": "KEEP",
            }
        )

    potential_details: list[dict[str, Any]] = []
    for row in defense_rows:
        code = _norm_code(row.get("code"))
        would_block_surge = _truthy(row.get("would_block_surge"))
        would_block_general = _truthy(row.get("would_block_general"))
        if not (would_block_surge or would_block_general):
            continue
        potential_details.append(
            {
                "scope": "defense_shadow_potential",
                "code": code,
                "name": row.get("name") or "",
                "route_scope": row.get("route_scope") or "",
                "in_current_candidates": code in candidate_codes,
                "row_is_surge": str(row.get("route_scope") or "").upper() in {"SURGE_ONLY", "BOTH"},
                "would_block_general": would_block_general,
                "would_block_surge": would_block_surge,
                "general_action_shadow": row.get("general_action_shadow") or "",
                "surge_action_shadow": row.get("surge_action_shadow") or "",
                "defense_signal_score": row.get("defense_signal_score") or "",
                "defense_reasons": row.get("defense_reasons") or "",
                "active_policy_result": "POTENTIAL_BLOCK_IF_IN_ENTRY_POOL",
                "observe_only_result": "KEEP",
            }
        )

    output_rows = current_candidate_details + potential_details
    _write_csv(OUT_CSV, output_rows)

    current_counts = Counter(row["active_policy_result"] for row in current_candidate_details)
    potential_counts = Counter(row["route_scope"] for row in potential_details)
    payload = {
        "generated_at": _now(),
        "status": "PASS",
        "schema_version": "defense_signal_entry_policy_impact_v1",
        "policy_enabled": bool(policy.get("enabled", False)),
        "source_files": {
            "candidates": candidate_source,
            "defense_signal_shadow": str(DEFENSE_CSV),
        },
        "outputs": {
            "json": str(OUT_JSON),
            "csv": str(OUT_CSV),
        },
        "current_candidates": {
            "rows": len(current_candidate_details),
            "active_keep_rows": int(current_counts.get("KEEP", 0)),
            "active_block_rows": int(current_counts.get("BLOCK_BY_DEFENSE_SIGNAL", 0)),
            "observe_only_keep_rows": len(current_candidate_details),
        },
        "defense_shadow_potential": {
            "rows": len(potential_details),
            "route_scope_counts": dict(potential_counts),
            "would_block_general_rows": sum(1 for row in potential_details if row["would_block_general"]),
            "would_block_surge_rows": sum(1 for row in potential_details if row["would_block_surge"]),
        },
        "effect_scope": {
            "score_effect": False,
            "order_dispatch_effect": False,
            "entry_pool_filter_effect": True,
            "policy_change_applied": False,
        },
        "interpretation": {
            "current_candidate_delta": "active_block_rows is the current difference between active filter and observe-only",
            "potential_delta": "potential rows can be blocked only if they enter the paper_engine candidate pool with matching route flags",
        },
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "PASS", "out_json": str(OUT_JSON), "rows": len(output_rows)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
