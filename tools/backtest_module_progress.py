from __future__ import annotations

from typing import Any, Dict, List


STATUS_READY = "READY"
STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_BLOCKED = "BLOCKED"


MODULE_PROGRESS_DEFS: List[Dict[str, Any]] = [
    {
        "id": "strategy_overview",
        "tab_no": 1,
        "title": "전략개요",
        "objective": "데이터/시간축/기본 무결성",
        "required_for_optimize": True,
        "names": [
            "data_close_integrity",
            "data_return_finite",
            "data_time_index_integrity",
        ],
    },
    {
        "id": "entry_logic",
        "tab_no": 2,
        "title": "진입",
        "objective": "신호 시점/체결 타이밍 일관성",
        "required_for_optimize": True,
        "names": [
            "look_ahead_proxy",
            "position_lag",
            "signal_quality_ic_ir",
        ],
    },
    {
        "id": "exit_logic",
        "tab_no": 3,
        "title": "청산",
        "objective": "청산 규칙의 시나리오/기간 일관성",
        "required_for_optimize": True,
        "names": [
            "historical_scenario_response",
            "temporal_consistency",
        ],
    },
    {
        "id": "sizing",
        "tab_no": 4,
        "title": "사이징",
        "objective": "파라미터/워크포워드 강건성",
        "required_for_optimize": True,
        "names": [
            "strategy_parameter_validation",
            "walk_forward",
        ],
    },
    {
        "id": "risk",
        "tab_no": 5,
        "title": "리스크",
        "objective": "낙폭/실질수익/허용손실 관리",
        "required_for_optimize": True,
        "names": [
            "monte_carlo",
            "inflation_real_return",
            "psychological_tolerance",
            "outlier_concentration",
            "acceptance_pnl_turnover",
        ],
    },
    {
        "id": "filters",
        "tab_no": 6,
        "title": "필터",
        "objective": "레짐 적응/과최적화 방지",
        "required_for_optimize": True,
        "names": [
            "market_regime_response",
            "cpcv_pbo",
            "deflated_sharpe_ratio",
        ],
    },
]


def build_module_progress(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    item_map = {
        str((item or {}).get("name") or "").strip(): item
        for item in items
        if isinstance(item, dict) and str((item or {}).get("name") or "").strip()
    }
    modules: List[Dict[str, Any]] = []

    for module_def in MODULE_PROGRESS_DEFS:
        names = list(module_def["names"])
        evidence: List[Dict[str, Any]] = []
        missing_names: List[str] = []
        pass_n = 0
        fail_n = 0
        not_eval_n = 0

        for name in names:
            hit = item_map.get(name)
            if hit is None:
                missing_names.append(name)
                evidence.append(
                    {
                        "name": name,
                        "item": name,
                        "status": "MISSING",
                        "metric": "-",
                        "threshold": "-",
                        "action": "검증 항목 매핑 추가 필요",
                    }
                )
                continue

            status = str(hit.get("status") or "").upper()
            if status == "PASS":
                pass_n += 1
            elif status == "FAIL":
                fail_n += 1
            else:
                not_eval_n += 1

            evidence.append(
                {
                    "name": name,
                    "item": str(hit.get("item") or name),
                    "status": status or "NOT_EVALUABLE",
                    "metric": str(hit.get("metric") or "-"),
                    "threshold": str(hit.get("threshold") or "-"),
                    "action": str(hit.get("action") or "-"),
                }
            )

        total = len(names)
        missing_n = len(missing_names)
        completion_pct = round((pass_n / total) * 100.0, 1) if total > 0 else 0.0

        if missing_n > 0 or fail_n > 0:
            status = STATUS_BLOCKED
        elif not_eval_n > 0:
            status = STATUS_IN_PROGRESS
        else:
            status = STATUS_READY

        blocker_labels = [
            row["item"]
            for row in evidence
            if str(row.get("status")) in {"FAIL", "NOT_EVALUABLE", "MISSING"}
        ]

        modules.append(
            {
                "id": module_def["id"],
                "tab_no": int(module_def["tab_no"]),
                "title": module_def["title"],
                "objective": module_def["objective"],
                "required_for_optimize": bool(module_def["required_for_optimize"]),
                "total_items": total,
                "pass_n": pass_n,
                "fail_n": fail_n,
                "not_evaluable_n": not_eval_n,
                "missing_n": missing_n,
                "completion_pct": completion_pct,
                "status": status,
                "ready_for_optimize": status == STATUS_READY,
                "blockers": blocker_labels,
                "missing_names": missing_names,
                "evidence": evidence,
            }
        )

    required_modules = [m for m in modules if m["required_for_optimize"]]
    optimizer_blockers = [m["title"] for m in required_modules if not m["ready_for_optimize"]]

    return {
        "schema_version": 1,
        "optimizer_ready": len(optimizer_blockers) == 0,
        "optimizer_blockers": optimizer_blockers,
        "modules_ready_n": sum(1 for m in modules if m["ready_for_optimize"]),
        "modules_total_n": len(modules),
        "modules": modules,
    }
