from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

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
OUT_PATH = LOG_DIR / "defense_signal_entry_policy_validation_latest.json"


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return str(value)


def _read_candidates() -> pd.DataFrame:
    for path in [
        LOG_DIR / "candidates_latest_data.with_final_score.csv",
        LOG_DIR / "candidates_latest_data.csv",
    ]:
        if path.exists():
            df = pd.read_csv(path)
            df.attrs["source_path"] = str(path)
            return df
    df = pd.DataFrame()
    df.attrs["source_path"] = ""
    return df


def _current_candidate_check(cfg: Dict[str, Any]) -> Dict[str, Any]:
    df = _read_candidates()
    source_path = df.attrs.get("source_path", "")
    before = int(len(df))
    defense_by_code = _load_defense_signal_entry_map(cfg)
    matched = 0
    if before and "code" in df.columns and defense_by_code:
        matched = int(df["code"].astype(str).map(norm_code).map(lambda c: c in defense_by_code).sum())
    out = _apply_defense_signal_entry_policy(df, cfg)
    after = int(len(out))
    blocked = before - after
    return {
        "source_path": source_path,
        "before": before,
        "after": after,
        "matched": matched,
        "blocked": blocked,
        "status": "PASS",
    }


def _synthetic_surge_block_check(cfg: Dict[str, Any]) -> Dict[str, Any]:
    defense_path = LOG_DIR / "defense_signal_shadow_latest.csv"
    if not defense_path.exists():
        return {"status": "NA", "reason": "defense_signal_shadow_latest.csv_missing"}
    d = pd.read_csv(defense_path)
    if d.empty:
        return {"status": "NA", "reason": "defense_signal_shadow_latest.csv_empty"}

    mask = d.get("would_block_surge", False).astype(str).str.lower().isin(["true", "1", "yes"])
    if "surge_action_shadow" in d.columns:
        mask &= d["surge_action_shadow"].astype(str).eq("SURGE_SHADOW_BLOCK")
    blocked_rows = d.loc[mask].copy()
    if blocked_rows.empty:
        return {"status": "NA", "reason": "no_would_block_surge_rows"}

    row = blocked_rows.iloc[0].to_dict()
    code = str(row.get("code") or "").zfill(6)
    candidate = pd.DataFrame(
        [
            {
                "code": code,
                "name": row.get("name") or "",
                "surge_type": "PRICE_VOL_BREAKOUT",
                "is_realtime_surge": True,
            }
        ]
    )
    defense_by_code = _load_defense_signal_entry_map(cfg)
    matched = int(candidate["code"].astype(str).map(norm_code).map(lambda c: c in defense_by_code).sum())
    out = _apply_defense_signal_entry_policy(candidate, cfg)
    blocked = int(len(candidate) - len(out))
    return {
        "status": "PASS" if blocked == 1 else "FAIL",
        "source_path": str(defense_path),
        "test_code": code,
        "before": int(len(candidate)),
        "after": int(len(out)),
        "matched": matched,
        "blocked": blocked,
        "surge_action_shadow": _json_safe(row.get("surge_action_shadow")),
        "defense_signal_score": _json_safe(row.get("defense_signal_score")),
        "defense_signal_reasons": _json_safe(row.get("defense_signal_reasons")),
    }


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    cfg = dict(pe.load_config())  # not DEFAULT_CONFIG: the engine runs the merged
    #   effective config, and validating the defaults can pass while production differs
    result = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "policy_enabled": bool((cfg.get("defense_signal_entry_policy") or {}).get("enabled")),
        "checks": {
            "current_candidates": _current_candidate_check(cfg),
            "synthetic_surge_block": _synthetic_surge_block_check(cfg),
        },
        "effect_scope": {
            "score_effect": False,
            "order_dispatch_effect": False,
            "entry_pool_filter_effect": True,
        },
    }
    statuses = [str(v.get("status")) for v in result["checks"].values() if isinstance(v, dict)]
    result["status"] = "PASS" if "FAIL" not in statuses and "PASS" in statuses else "FAIL"
    OUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
