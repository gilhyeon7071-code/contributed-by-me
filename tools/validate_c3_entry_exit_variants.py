from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SRC_CSV = LOG_DIR / "c3_research_adapter_latest.csv"

OUT_ROWS_CSV = LOG_DIR / "c3_entry_exit_variants_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "c3_entry_exit_variants_summary_latest.csv"
OUT_JSON = LOG_DIR / "c3_entry_exit_variants_latest.json"
OUT_MD = LOG_DIR / "c3_entry_exit_variants_latest.md"


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def _i(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def _load_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _has_tag(row: Dict[str, Any], tag: str) -> bool:
    return tag in {x.strip() for x in str(row.get("risk_tags", "") or "").split("|") if x.strip()}


def _variant_return(row: Dict[str, Any], variant: str) -> Tuple[bool, float, str]:
    base_ret = _f(row.get("ret"), 0.0)
    max_high = _f(row.get("max_high_pct_to_exit"), -999.0)

    if variant == "baseline_current_contract":
        return True, base_ret, "baseline"

    if variant == "skip_entry_day_chase_watch":
        if _has_tag(row, "ENTRY_DAY_CHASE_WATCH"):
            return False, 0.0, "skipped_ENTRY_DAY_CHASE_WATCH"
        return True, base_ret, "kept"

    if variant == "skip_deep_adverse_watch":
        if _has_tag(row, "DEEP_ADVERSE_PATH_WATCH"):
            return False, 0.0, "skipped_DEEP_ADVERSE_PATH_WATCH"
        return True, base_ret, "kept"

    if variant == "partial_tp_3pct_50pct":
        if max_high >= 3.0:
            return True, 0.5 * 0.03 + 0.5 * base_ret, "partial_tp_3pct_hit_50pct"
        return True, base_ret, "tp_not_hit"

    if variant == "partial_tp_5pct_50pct":
        if max_high >= 5.0:
            return True, 0.5 * 0.05 + 0.5 * base_ret, "partial_tp_5pct_hit_50pct"
        return True, base_ret, "tp_not_hit"

    if variant == "profit_then_stop_repair_only":
        if _has_tag(row, "PROFIT_THEN_STOP_EXIT_REPAIR") and max_high >= 5.0:
            return True, 0.5 * 0.05 + 0.5 * base_ret, "repair_profit_then_stop_5pct_50pct"
        return True, base_ret, "not_repair_target"

    raise ValueError(f"unknown variant: {variant}")


def _pf(rows: Iterable[Dict[str, Any]], ret_key: str = "variant_ret") -> Optional[float]:
    gain = 0.0
    loss = 0.0
    for r in rows:
        ret = _f(r.get(ret_key), 0.0)
        if ret > 0:
            gain += ret
        elif ret < 0:
            loss += abs(ret)
    if loss <= 0:
        return None if gain <= 0 else float("inf")
    return round(gain / loss, 6)


def _outcome(ret: float, executed: bool) -> str:
    if not executed:
        return "SKIPPED"
    if ret > 0:
        return "WIN"
    if ret < 0:
        return "LOSS"
    return "FLAT"


def _build_variant_rows(src_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    variants = [
        "baseline_current_contract",
        "partial_tp_3pct_50pct",
        "partial_tp_5pct_50pct",
        "profit_then_stop_repair_only",
        "skip_entry_day_chase_watch",
        "skip_deep_adverse_watch",
    ]
    out: List[Dict[str, Any]] = []
    for row in src_rows:
        for variant in variants:
            executed, ret, reason = _variant_return(row, variant)
            out.append(
                {
                    "variant": variant,
                    "methodology_family": row.get("methodology_family", ""),
                    "route_class": row.get("route_class", ""),
                    "code": row.get("code", ""),
                    "entry_date": row.get("entry_date", ""),
                    "year": row.get("year", ""),
                    "baseline_ret": _f(row.get("ret"), 0.0),
                    "variant_ret": round(float(ret), 8),
                    "variant_ret_pct": round(float(ret) * 100.0, 4),
                    "executed": 1 if executed else 0,
                    "variant_outcome": _outcome(float(ret), executed),
                    "baseline_exit_reason": row.get("exit_reason", ""),
                    "baseline_outcome": row.get("outcome", ""),
                    "risk_tags": row.get("risk_tags", ""),
                    "variant_reason": reason,
                    "max_high_pct_to_exit": row.get("max_high_pct_to_exit", ""),
                    "min_low_pct_to_exit": row.get("min_low_pct_to_exit", ""),
                    "operation_effect": "none",
                    "research_only": 1,
                }
            )
    return out


def _summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_variant: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_variant[str(r.get("variant", ""))].append(r)

    summary: List[Dict[str, Any]] = []
    for variant, group in sorted(by_variant.items()):
        executed = [r for r in group if _i(r.get("executed"), 0) == 1]
        skipped = len(group) - len(executed)
        ret_sum = round(sum(_f(r.get("variant_ret"), 0.0) for r in executed), 6)
        n = len(executed)
        base_sum = round(sum(_f(r.get("baseline_ret"), 0.0) for r in executed), 6)
        by_year: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in executed:
            by_year[str(r.get("year", ""))].append(r)
        reasons = Counter(str(r.get("variant_reason", "")) for r in group)
        summary.append(
            {
                "variant": variant,
                "source_rows": len(group),
                "executed_n": n,
                "skipped_n": skipped,
                "win_n": sum(1 for r in executed if _f(r.get("variant_ret"), 0.0) > 0),
                "loss_n": sum(1 for r in executed if _f(r.get("variant_ret"), 0.0) < 0),
                "ret_sum": ret_sum,
                "ret_mean": round(ret_sum / n, 6) if n else 0.0,
                "pf": _pf(executed),
                "delta_vs_executed_baseline": round(ret_sum - base_sum, 6),
                "reason_counts": json.dumps(dict(reasons), ensure_ascii=False, sort_keys=True),
                "by_year": json.dumps(
                    {
                        y: {
                            "n": len(yr),
                            "ret_sum": round(sum(_f(r.get("variant_ret"), 0.0) for r in yr), 6),
                            "pf": _pf(yr),
                        }
                        for y, yr in sorted(by_year.items())
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "operation_effect": "none",
            }
        )
    return summary


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def _write_md(path: Path, payload: Dict[str, Any]) -> None:
    lines = [
        "# C3 진입/청산 변형 read-only 검증",
        "",
        "## 판정",
        "",
        "- 이 검증은 C3 연구 표본 11건만 대상으로 한 read-only 비교다.",
        "- `skip_deep_adverse_watch`는 002460 단일 사례 제거라 과최적 위험이 크다.",
        "- `partial_tp_5pct_50pct`와 `profit_then_stop_repair_only`는 청산 연구축으로 유지할 가치가 있다.",
        "- 운영 반영 또는 stable 승격 근거는 아니다.",
        "",
        "## 요약",
        "",
    ]
    for row in payload["summary_rows"]:
        lines.append(
            f"- `{row['variant']}`: executed `{row['executed_n']}`, skipped `{row['skipped_n']}`, "
            f"ret_sum `{row['ret_sum']}`, PF `{row['pf']}`, delta `{row['delta_vs_executed_baseline']}`"
        )
    lines.extend(
        [
            "",
            "## 공식/운영 영향",
            "",
            "- stable params 변경 없음",
            "- 후보 생성기 변경 없음",
            "- HPO 변경 없음",
            "- paper engine 변경 없음",
            "- order/fill/ledger 변경 없음",
            "- 전체로직 미적용",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    src_rows = _load_rows(SRC_CSV)
    variant_rows = _build_variant_rows(src_rows)
    summary_rows = _summary(variant_rows)

    row_fields = [
        "variant",
        "methodology_family",
        "route_class",
        "code",
        "entry_date",
        "year",
        "baseline_ret",
        "variant_ret",
        "variant_ret_pct",
        "executed",
        "variant_outcome",
        "baseline_exit_reason",
        "baseline_outcome",
        "risk_tags",
        "variant_reason",
        "max_high_pct_to_exit",
        "min_low_pct_to_exit",
        "operation_effect",
        "research_only",
    ]
    summary_fields = [
        "variant",
        "source_rows",
        "executed_n",
        "skipped_n",
        "win_n",
        "loss_n",
        "ret_sum",
        "ret_mean",
        "pf",
        "delta_vs_executed_baseline",
        "reason_counts",
        "by_year",
        "operation_effect",
    ]
    _write_csv(OUT_ROWS_CSV, variant_rows, row_fields)
    _write_csv(OUT_SUMMARY_CSV, summary_rows, summary_fields)

    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "READ_ONLY_C3_ENTRY_EXIT_VARIANTS_NOT_OPERATIONAL",
        "source_file": str(SRC_CSV),
        "source_rows": len(src_rows),
        "variant_rows": len(variant_rows),
        "summary_rows": summary_rows,
        "interpretation": {
            "best_non_skip_variant_by_ret_sum": max(
                [r for r in summary_rows if not str(r["variant"]).startswith("skip_")],
                key=lambda r: _f(r.get("ret_sum"), -999.0),
            )["variant"]
            if summary_rows
            else "",
            "overfit_warning": "skip_deep_adverse_watch removes the single 002460 false positive and must not be promoted as a rule",
            "recommended_next": "validate partial take-profit variants on a broader C3-like research sample",
        },
        "operation_effect": {
            "stable_params": False,
            "candidate_generator": False,
            "hpo": False,
            "paper_engine": False,
            "paper_config": False,
            "orders": False,
            "fills": False,
            "ledger": False,
        },
        "full_logic_application": "NOT_APPLIED",
        "output_files": {
            "rows_csv": str(OUT_ROWS_CSV),
            "summary_csv": str(OUT_SUMMARY_CSV),
            "json": str(OUT_JSON),
            "md": str(OUT_MD),
        },
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_md(OUT_MD, payload)
    print(json.dumps({k: payload[k] for k in ("status", "source_rows", "variant_rows", "interpretation", "output_files")}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
