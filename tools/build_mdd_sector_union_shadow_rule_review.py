from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

SOURCE_CSV = LOG_DIR / "mdd_sector_union_quality_review_latest.csv"
OUT_JSON = LOG_DIR / "mdd_sector_union_shadow_rule_review_latest.json"
OUT_CSV = LOG_DIR / "mdd_sector_union_shadow_rule_review_latest.csv"


def _now_ts() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        return float(value)
    except Exception:
        return default


def _text(row: dict[str, Any], key: str) -> str:
    return str(row.get(key) or "").strip()


def _has_flag(row: dict[str, Any], flag: str) -> bool:
    return flag.lower() in _text(row, "junk_flags").lower()


def _is_non_execution_union(row: dict[str, Any]) -> bool:
    return (
        _text(row, "candidate_origin").upper() == "SECTOR_PREFILTER_UNION"
        and _text(row, "execution_pool").lower() == "false"
        and _text(row, "natural_pass").lower() == "false"
    )


def _rule_rows(
    rows: list[dict[str, Any]],
    rule_id: str,
    description: str,
    predicate: Callable[[dict[str, Any]], bool],
) -> dict[str, Any]:
    excluded = [row for row in rows if predicate(row)]
    kept = [row for row in rows if not predicate(row)]
    excluded_sum = sum(_float(row.get("net_ret")) for row in excluded)
    kept_sum = sum(_float(row.get("net_ret")) for row in kept)
    positive_excluded = sum(1 for row in excluded if _float(row.get("net_ret")) > 0)
    return {
        "rule_id": rule_id,
        "description": description,
        "excluded_rows": len(excluded),
        "kept_rows": len(kept),
        "excluded_sum_net_ret": excluded_sum,
        "kept_sum_net_ret": kept_sum,
        "positive_excluded_rows": positive_excluded,
        "negative_excluded_rows": len(excluded) - positive_excluded,
        "policy_change_applied": False,
    }


def main() -> int:
    rows = _read_csv(SOURCE_CSV)
    total_sum = sum(_float(row.get("net_ret")) for row in rows)

    rules: list[tuple[str, str, Callable[[dict[str, Any]], bool]]] = [
        (
            "R1_LOW_LIQUIDITY_ONLY",
            "Exclude sector-union non-execution rows when junk_flags contains low_liquidity.",
            lambda row: _has_flag(row, "low_liquidity"),
        ),
        (
            "R2_MID_OR_WARN_RISK_ONLY",
            "Exclude sector-union non-execution rows when junk_risk_grade is MID or WARN.",
            lambda row: _text(row, "junk_risk_grade").upper() in {"MID", "WARN"},
        ),
        (
            "R3_LOW_LIQUIDITY_AND_MID_WARN",
            "Exclude low-liquidity sector-union rows only when junk_risk_grade is MID or WARN.",
            lambda row: _has_flag(row, "low_liquidity")
            and _text(row, "junk_risk_grade").upper() in {"MID", "WARN"},
        ),
        (
            "R4_RELAX_L4_L6_ONLY",
            "Exclude sector-union non-execution rows when relax_level is L4 or L6.",
            lambda row: _text(row, "relax_level").upper() in {"L4", "L6"},
        ),
        (
            "R5_STOP_FAMILY_AND_LOW_LIQUIDITY",
            "Exclude low-liquidity sector-union rows that later exited through STOP family.",
            lambda row: _has_flag(row, "low_liquidity")
            and _text(row, "exit_reason").upper().startswith("STOP"),
        ),
        (
            "R6_CORE_CONSERVATIVE_SHADOW",
            "Exclude sector-union non-execution rows with low_liquidity plus MID/WARN risk, or relax_level L4/L6.",
            lambda row: (
                _has_flag(row, "low_liquidity")
                and _text(row, "junk_risk_grade").upper() in {"MID", "WARN"}
            )
            or _text(row, "relax_level").upper() in {"L4", "L6"},
        ),
    ]

    review_rows = [_rule_rows(rows, *rule) for rule in rules]
    for review in review_rows:
        review["excluded_share_of_rows"] = (
            review["excluded_rows"] / len(rows) if rows else 0.0
        )
        review["excluded_share_of_abs_loss"] = (
            abs(review["excluded_sum_net_ret"]) / abs(total_sum)
            if total_sum < 0 and review["excluded_sum_net_ret"] < 0
            else 0.0
        )

    best_loss_capture = sorted(
        review_rows,
        key=lambda row: (
            -float(row["excluded_share_of_abs_loss"]),
            int(row["positive_excluded_rows"]),
            int(row["excluded_rows"]),
        ),
    )[:3]

    all_non_execution_union = all(_is_non_execution_union(row) for row in rows) if rows else False
    out = {
        "generated_at": _now_ts(),
        "status": "FAIL",
        "schema_version": "mdd_sector_union_shadow_rule_review_v1",
        "source_files": {
            "mdd_sector_union_quality_review": str(SOURCE_CSV),
        },
        "scope": "read_only_shadow_rules_no_policy_change",
        "rows": len(rows),
        "sum_net_ret": total_sum,
        "all_rows_non_execution_sector_union": all_non_execution_union,
        "rules": review_rows,
        "best_loss_capture_rules": best_loss_capture,
        "interpretation": (
            "This is a historical shadow-rule decomposition only. It does not prove "
            "that the same rule should be applied live until runtime lineage preserves "
            "the sector decision fields and a forward sample confirms behavior."
        ),
        "decision": "SHADOW_RULE_CANDIDATE_IDENTIFIED_BUT_POLICY_NOT_CHANGED",
        "policy_effect": False,
        "policy_change_applied": False,
    }

    fields = [
        "rule_id",
        "description",
        "excluded_rows",
        "kept_rows",
        "excluded_sum_net_ret",
        "kept_sum_net_ret",
        "positive_excluded_rows",
        "negative_excluded_rows",
        "excluded_share_of_rows",
        "excluded_share_of_abs_loss",
        "policy_change_applied",
    ]
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(OUT_CSV, review_rows, fields)
    print(f"[FINAL] mdd sector union shadow rule review -> {OUT_JSON} rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
