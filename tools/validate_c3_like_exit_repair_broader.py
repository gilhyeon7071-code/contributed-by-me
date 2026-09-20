from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
SRC_JSON = LOG_DIR / "report_entry_trigger_v2_transition_value2b_ret1_1_2025_2026_axis_c3_outcome_latest.json"

OUT_ROWS_CSV = LOG_DIR / "c3_like_exit_repair_broader_latest.csv"
OUT_SUMMARY_CSV = LOG_DIR / "c3_like_exit_repair_broader_summary_latest.csv"
OUT_JSON = LOG_DIR / "c3_like_exit_repair_broader_latest.json"
OUT_MD = LOG_DIR / "c3_like_exit_repair_broader_latest.md"


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


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"json root is not object: {path}")
    return obj


def _market(row: Dict[str, Any]) -> str:
    return str(row.get("market_after_recheck") or row.get("market_raw") or "").strip()


def _tier(row: Dict[str, Any]) -> str:
    score = _f(row.get("score"), 0.0)
    follow = _i(row.get("followthrough_1d"), 0)
    v = _f(row.get("signal_v_accel"), 0.0)
    market_ok = bool(_market(row))
    if score >= 1.0 and follow == 1 and v >= 0.9 and market_ok:
        return "exact_c3"
    if score >= 0.8 and follow == 1 and v >= 0.8 and market_ok:
        return "c3_like_score080_follow1_v080"
    if score >= 0.5 and follow == 1 and v >= 0.8 and market_ok:
        return "c3_like_score050_follow1_v080"
    if score >= 0.5 and v >= 0.8 and market_ok:
        return "c3_adjacent_score050_v080"
    return "outside_c3_like"


def _pf(rows: Iterable[Dict[str, Any]], key: str = "variant_ret") -> Optional[float]:
    gain = 0.0
    loss = 0.0
    for row in rows:
        ret = _f(row.get(key), 0.0)
        if ret > 0:
            gain += ret
        elif ret < 0:
            loss += abs(ret)
    if loss <= 0:
        return None if gain <= 0 else float("inf")
    return round(gain / loss, 6)


def _variant_ret(row: Dict[str, Any], variant: str) -> tuple[float, str]:
    base = _f(row.get("ret"), 0.0)
    max_high = _f(row.get("max_high_pct_to_exit"), -999.0)
    exit_reason = str(row.get("exit_reason", "") or "").upper()
    if variant == "baseline":
        return base, "baseline"
    if variant == "partial_tp_5pct_50pct_all":
        if max_high >= 5.0:
            return 0.5 * 0.05 + 0.5 * base, "partial_tp_5pct_hit_50pct"
        return base, "tp_not_hit"
    if variant == "profit_then_stop_repair_5pct_50pct":
        if "STOP" in exit_reason and max_high >= 5.0:
            return 0.5 * 0.05 + 0.5 * base, "repair_stop_after_5pct_profit"
        return base, "not_repair_target"
    if variant == "partial_tp_3pct_50pct_all":
        if max_high >= 3.0:
            return 0.5 * 0.03 + 0.5 * base, "partial_tp_3pct_hit_50pct"
        return base, "tp_not_hit"
    raise ValueError(variant)


def _build_rows(src_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    variants = [
        "baseline",
        "partial_tp_3pct_50pct_all",
        "partial_tp_5pct_50pct_all",
        "profit_then_stop_repair_5pct_50pct",
    ]
    out: List[Dict[str, Any]] = []
    for row in src_rows:
        tier = _tier(row)
        for variant in variants:
            vret, reason = _variant_ret(row, variant)
            out.append(
                {
                    "tier": tier,
                    "variant": variant,
                    "code": str(row.get("code", "") or "").zfill(6),
                    "signal_date": row.get("signal_date", ""),
                    "entry_date": row.get("entry_date", ""),
                    "year": str(row.get("entry_date", ""))[:4],
                    "market": _market(row),
                    "score": _f(row.get("score"), 0.0),
                    "followthrough_1d": _i(row.get("followthrough_1d"), 0),
                    "signal_v_accel": _f(row.get("signal_v_accel"), 0.0),
                    "signal_ret1_pct": _f(row.get("signal_ret1_pct"), 0.0),
                    "entry_gap_pct": _f(row.get("entry_gap_pct"), 0.0),
                    "baseline_ret": _f(row.get("ret"), 0.0),
                    "variant_ret": round(vret, 8),
                    "variant_ret_pct": round(vret * 100.0, 4),
                    "exit_reason": row.get("exit_reason", ""),
                    "max_high_pct_to_exit": row.get("max_high_pct_to_exit", ""),
                    "min_low_pct_to_exit": row.get("min_low_pct_to_exit", ""),
                    "variant_reason": reason,
                    "operation_effect": "none",
                    "research_only": 1,
                }
            )
    return out


def _summarize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row.get("tier", "")), str(row.get("variant", "")))].append(row)
    out: List[Dict[str, Any]] = []
    for (tier, variant), group in sorted(grouped.items()):
        base_sum = sum(_f(r.get("baseline_ret"), 0.0) for r in group)
        ret_sum = sum(_f(r.get("variant_ret"), 0.0) for r in group)
        by_year: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in group:
            by_year[str(r.get("year", ""))].append(r)
        out.append(
            {
                "tier": tier,
                "variant": variant,
                "n": len(group),
                "win_n": sum(1 for r in group if _f(r.get("variant_ret"), 0.0) > 0),
                "loss_n": sum(1 for r in group if _f(r.get("variant_ret"), 0.0) < 0),
                "stop_source_n": sum(1 for r in group if "STOP" in str(r.get("exit_reason", "")).upper()),
                "ret_sum": round(ret_sum, 6),
                "ret_mean": round(ret_sum / len(group), 6) if group else 0.0,
                "pf": _pf(group),
                "delta_vs_baseline": round(ret_sum - base_sum, 6),
                "repair_hit_n": sum(1 for r in group if str(r.get("variant_reason")) == "repair_stop_after_5pct_profit"),
                "tp_hit_n": sum(1 for r in group if "partial_tp" in str(r.get("variant_reason"))),
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
    return out


def _write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def _write_md(path: Path, payload: Dict[str, Any]) -> None:
    exact = [r for r in payload["summary_rows"] if r["tier"] == "exact_c3"]
    broader = [r for r in payload["summary_rows"] if r["tier"] in {"c3_like_score080_follow1_v080", "c3_like_score050_follow1_v080"}]
    lines = [
        "# C3-like 확장 표본 청산 보정 검증",
        "",
        "## 판정",
        "",
        "- 53건 대표 표본에서 C3-like 단계별로 부분익절/청산 보정을 비교했다.",
        "- exact C3에서는 profit-then-stop 보정이 11건 결과와 같은 방향이다.",
        "- 더 넓은 C3-like에서는 전체 부분익절이 ret_sum을 낮추는 구간이 있어, 전면 적용은 아직 근거 부족이다.",
        "- 운영 반영 또는 stable 승격 근거는 아니다.",
        "",
        "## exact C3",
        "",
    ]
    for row in exact:
        lines.append(f"- `{row['variant']}` n `{row['n']}` ret_sum `{row['ret_sum']}` PF `{row['pf']}` delta `{row['delta_vs_baseline']}`")
    lines.extend(["", "## C3-like 확장", ""])
    for row in broader:
        lines.append(f"- `{row['tier']} / {row['variant']}` n `{row['n']}` ret_sum `{row['ret_sum']}` PF `{row['pf']}` delta `{row['delta_vs_baseline']}`")
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
    obj = _load_json(SRC_JSON)
    rows = obj.get("rows") if isinstance(obj.get("rows"), list) else []
    src_rows = [r for r in rows if isinstance(r, dict)]
    variant_rows = _build_rows(src_rows)
    summary_rows = _summarize(variant_rows)

    row_fields = [
        "tier",
        "variant",
        "code",
        "signal_date",
        "entry_date",
        "year",
        "market",
        "score",
        "followthrough_1d",
        "signal_v_accel",
        "signal_ret1_pct",
        "entry_gap_pct",
        "baseline_ret",
        "variant_ret",
        "variant_ret_pct",
        "exit_reason",
        "max_high_pct_to_exit",
        "min_low_pct_to_exit",
        "variant_reason",
        "operation_effect",
        "research_only",
    ]
    summary_fields = [
        "tier",
        "variant",
        "n",
        "win_n",
        "loss_n",
        "stop_source_n",
        "ret_sum",
        "ret_mean",
        "pf",
        "delta_vs_baseline",
        "repair_hit_n",
        "tp_hit_n",
        "by_year",
        "operation_effect",
    ]
    _write_csv(OUT_ROWS_CSV, variant_rows, row_fields)
    _write_csv(OUT_SUMMARY_CSV, summary_rows, summary_fields)

    exact_profit = next((r for r in summary_rows if r["tier"] == "exact_c3" and r["variant"] == "profit_then_stop_repair_5pct_50pct"), None)
    exact_base = next((r for r in summary_rows if r["tier"] == "exact_c3" and r["variant"] == "baseline"), None)
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "READ_ONLY_C3_LIKE_EXIT_REPAIR_BROADER_NOT_OPERATIONAL",
        "source_file": str(SRC_JSON),
        "source_rows": len(src_rows),
        "variant_rows": len(variant_rows),
        "summary_rows": summary_rows,
        "interpretation": {
            "exact_c3_baseline_ret_sum": None if exact_base is None else exact_base["ret_sum"],
            "exact_c3_profit_repair_ret_sum": None if exact_profit is None else exact_profit["ret_sum"],
            "exact_c3_profit_repair_delta": None if exact_profit is None else exact_profit["delta_vs_baseline"],
            "judgment": "profit-then-stop repair remains positive on exact C3, but broad partial take-profit should not be applied wholesale before larger-sample confirmation",
            "recommended_next": "expand C3-like candidates beyond the single representative variant or reconstruct path data for all cross variants",
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
