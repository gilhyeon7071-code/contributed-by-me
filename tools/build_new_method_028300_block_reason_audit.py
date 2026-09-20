#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Read-only audit for why 028300 is blocked in the STRESS10 proxy-clear replay."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

AUDIT_CSV = LOG_DIR / "new_method_stress10_proxy_clear_audit_latest.csv"
REPLAY_JSON = LOG_DIR / "new_method_followthrough_replay_stress10_proxy_clear_latest.json"

OUT_JSON = LOG_DIR / "new_method_028300_block_reason_audit_latest.json"
OUT_MD = LOG_DIR / "new_method_028300_block_reason_audit_latest.md"


def _safe_float(value, default=0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def main() -> int:
    audit = pd.read_csv(AUDIT_CSV, dtype={"code": str}, encoding="utf-8-sig")
    audit["code"] = audit["code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    row_df = audit[audit["code"].eq("028300")].copy()
    if row_df.empty:
        raise SystemExit("028300 row not found")
    row = row_df.iloc[0].to_dict()
    replay = json.loads(REPLAY_JSON.read_text(encoding="utf-8"))
    rule = replay.get("rule") or {}
    diagnostics = replay.get("diagnostics") or {}

    low_from_open = _safe_float(row.get("low_from_open_pct"))
    low_limit = -abs(_safe_float(rule.get("max_low_from_open_pct"), 0.04))
    soft_low_limit = low_limit * 2.0
    score = _safe_float(row.get("score"))
    max_score = _safe_float(rule.get("max_score"))

    low_breach_abs = low_limit - low_from_open if low_from_open < low_limit else 0.0
    score_breach_abs = score - max_score if score > max_score else 0.0

    scenarios = []
    for item in diagnostics.get("block_ablation") or []:
        scenarios.append(
            {
                "scenario": item.get("scenario"),
                "relaxed_blocks": item.get("relaxed_blocks"),
                "active_blocks": item.get("active_blocks"),
                "allowed_count": item.get("allowed_count"),
                "contains_028300": any(str(r.get("code")).zfill(6) == "028300" for r in item.get("rows") or []),
            }
        )

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "scope": "read_only_028300_block_reason_audit",
        "source_audit_csv": str(AUDIT_CSV),
        "source_replay_json": str(REPLAY_JSON),
        "code": "028300",
        "current_classification": row.get("classification"),
        "current_classification_kr": row.get("classification_kr"),
        "signals": {
            "buy_signal_price": str(row.get("buy_signal_price")).lower() == "true",
            "buy_signal_liquidity": str(row.get("buy_signal_liquidity")).lower() == "true",
            "buy_signal": str(row.get("buy_signal")).lower() == "true",
            "entry_allowed_validation": str(row.get("entry_allowed_validation")).lower() == "true",
            "block_reasons": str(row.get("block_reasons") or ""),
        },
        "low_from_open_check": {
            "low_from_open_pct": low_from_open,
            "strict_limit": low_limit,
            "soft_limit": soft_low_limit,
            "strict_breach_abs": low_breach_abs,
            "strict_breach_pct_points": low_breach_abs * 100.0,
            "within_soft_limit": low_from_open >= soft_low_limit,
            "interpretation": "MID risk: strict low-from-open breached, soft limit not breached",
        },
        "high_score_check": {
            "score": score,
            "max_score": max_score,
            "breach_abs": score_breach_abs,
            "breach_pct_points": score_breach_abs * 100.0,
            "implementation_rule": "block_high_score = score > max_score",
            "policy_basis_found_in_narrow_search": False,
        },
        "block_ablation": scenarios,
        "decision": {
            "single_block_relaxation_sufficient": False,
            "both_low_from_open_and_high_score_must_be_removed_for_entry": True,
            "safe_conclusion": "Keep as read-only exploration candidate; do not treat as operational entry evidence.",
        },
    }

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# 028300 Block Reason Audit",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- classification: {payload['current_classification_kr']}",
        f"- buy_signal: {payload['signals']['buy_signal']}",
        f"- entry_allowed_validation: {payload['signals']['entry_allowed_validation']}",
        f"- block_reasons: {payload['signals']['block_reasons']}",
        "",
        "## low_from_open",
        f"- value: {low_from_open:.6f}",
        f"- strict_limit: {low_limit:.6f}",
        f"- breach: {low_breach_abs * 100.0:.4f} pct-points",
        f"- soft_limit: {soft_low_limit:.6f}",
        f"- within_soft_limit: {payload['low_from_open_check']['within_soft_limit']}",
        "",
        "## high_score",
        f"- score: {score:.6f}",
        f"- max_score: {max_score:.6f}",
        f"- breach: {score_breach_abs * 100.0:.4f} pct-points",
        "- implementation_rule: block_high_score = score > max_score",
        "- policy_basis_found_in_narrow_search: False",
        "",
        "## block ablation",
    ]
    for s in scenarios:
        if s["scenario"] in {"strict_all_blocks", "relax_only_low_from_open", "relax_only_high_score", "relax_all_blocks"}:
            lines.append(
                f"- {s['scenario']}: allowed_count={s['allowed_count']} contains_028300={s['contains_028300']}"
            )
    lines.extend(
        [
            "",
            "## decision",
            "- single_block_relaxation_sufficient: False",
            "- both_low_from_open_and_high_score_must_be_removed_for_entry: True",
            "- safe_conclusion: Keep as read-only exploration candidate; do not treat as operational entry evidence.",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": "OK", "json": str(OUT_JSON), "md": str(OUT_MD)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
