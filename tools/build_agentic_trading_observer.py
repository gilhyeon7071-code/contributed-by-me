from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LOG_ROOT = ROOT / "2_Logs"
WIKI_ROOT = ROOT / "docs" / "llm_wiki"
OUT_DOC = WIKI_ROOT / "03_Agentic_Review" / "agentic_trading_observer_latest.md"
OUT_JSON = LOG_ROOT / "agentic_trading_observer_latest.json"
FRESHNESS_PTR = LOG_ROOT / "freshness_source_last.json"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for encoding in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=encoding))
        except UnicodeDecodeError:
            continue
        except json.JSONDecodeError:
            return {"_read_error": "json_decode_error"}
    return {"_read_error": "unicode_decode_error"}


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for encoding in ("utf-8-sig", "cp949", "utf-8"):
        try:
            with path.open("r", encoding=encoding, newline="") as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


def _latest_buy_ymd(rows: list[dict[str, str]]) -> str:
    buys = [r for r in rows if str(r.get("side", "")).upper() == "BUY"]
    source = buys or rows
    ymds = [str(r.get("datetime", ""))[:8] for r in source if str(r.get("datetime", ""))[:8].isdigit()]
    return max(ymds) if ymds else "unknown"


def _latest_freshness_path() -> Path:
    ptr = _read_json(FRESHNESS_PTR)
    raw = str(ptr.get("last") or "").strip()
    return Path(raw) if raw else LOG_ROOT / "freshness_source_latest.json"


def _kv(value: Any) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _status_from_bool(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _agent(name: str, role: str, facts: dict[str, Any], interpretation: list[str], action: str) -> dict[str, Any]:
    return {
        "name": name,
        "role": role,
        "facts": facts,
        "interpretation": interpretation,
        "allowed_action": action,
    }


def main() -> int:
    generated_at = datetime.now().isoformat(timespec="seconds")

    fills_path = ROOT / "paper" / "fills.csv"
    fills_rows = _read_csv_rows(fills_path)
    d_ymd = _latest_buy_ymd(fills_rows)
    orders_exec_path = ROOT / "paper" / f"orders_{d_ymd}_exec.xlsx"

    final_score_path = LOG_ROOT / "final_score_merge_status_latest.json"
    news_score_path = LOG_ROOT / "news_score_status_latest.json"
    future_daily_path = LOG_ROOT / "future_signal_daily_status_latest.json"
    risk_path = LOG_ROOT / "risk_orchestration_latest.json"
    ops_path = LOG_ROOT / "ops_sanity_quick_latest.json"
    freshness_path = _latest_freshness_path()
    wiki_index_path = WIKI_ROOT / "wiki_index_latest.md"
    source_index_path = WIKI_ROOT / "02_Sources" / "source_index_latest.md"

    final_score = _read_json(final_score_path)
    news_score = _read_json(news_score_path)
    future_daily = _read_json(future_daily_path)
    risk = _read_json(risk_path)
    ops = _read_json(ops_path)
    freshness = _read_json(freshness_path)

    asof_ymd = final_score.get("asof_ymd") or final_score.get("score_asof_ymd") or "unknown"
    score_expected_ymd = freshness.get("expected_date") or "unknown"
    score_expected_mode = freshness.get("expected_date_mode") or "unknown"
    score_asof_matches_expected = asof_ymd == score_expected_ymd
    orders_exec_exists = orders_exec_path.exists()
    strict_asof_matches_d = asof_ymd == d_ymd
    risk_orch = risk.get("risk_orchestration") if isinstance(risk.get("risk_orchestration"), dict) else {}
    future_summary = future_daily.get("summary") if isinstance(future_daily.get("summary"), dict) else {}

    stop_concerns = []
    if d_ymd == "unknown":
        stop_concerns.append("D_UNKNOWN")
    if not orders_exec_exists:
        stop_concerns.append("ORDERS_EXEC_MISSING")
    if not score_asof_matches_expected:
        stop_concerns.append("SCORE_ASOF_EXPECTED_MISMATCH")

    strict_interpretation_concerns = []
    if not strict_asof_matches_d:
        strict_interpretation_concerns.append("STRICT_ASOF_D_MISMATCH")

    agents = [
        _agent(
            "Planner",
            "Select the next safe review focus from current artifacts.",
            {
                "D": d_ymd,
                "orders_exec_exists": orders_exec_exists,
                "asof_ymd": asof_ymd,
                "score_expected_ymd": score_expected_ymd,
                "score_expected_mode": score_expected_mode,
                "score_asof_matches_expected": score_asof_matches_expected,
                "strict_asof_matches_D": strict_asof_matches_d,
                "final_score_rows": final_score.get("rows"),
            },
            [
                "Use current artifacts before any trading interpretation.",
                "Candidate/score freshness uses the freshness expected date; strict D mismatch remains visible for order/fill/ledger/stat interpretation.",
            ],
            "read_only_review_only",
        ),
        _agent(
            "Alpha",
            "Summarize candidate and future-signal evidence without creating orders.",
            {
                "score_regime": final_score.get("score_regime"),
                "final_nonzero_rows": (final_score.get("nonzero_rows") or {}).get("final")
                if isinstance(final_score.get("nonzero_rows"), dict)
                else None,
                "future_status": future_daily.get("status"),
                "future_validation_state": future_summary.get("validation_state"),
                "future_trading_approved": future_summary.get("trading_approved"),
                "future_sample_collection_state": future_summary.get("sample_collection_state"),
            },
            [
                "Future signal remains shadow evidence while trading_approved is false.",
                "Alpha evidence may explain candidates but does not relax Gate, STOP, or score policy.",
            ],
            "summarize_candidate_context_only",
        ),
        _agent(
            "Risk",
            "Surface risk blocks and sizing constraints from current orchestration.",
            {
                "market_regime": risk.get("market_regime"),
                "position_size_multiplier": risk.get("position_size_multiplier"),
                "risk_scale": risk_orch.get("scale"),
                "dd_stop_triggered": risk_orch.get("dd_stop_triggered"),
                "es_triggered": risk_orch.get("es_triggered"),
                "scale_zero_causes": risk_orch.get("scale_zero_causes"),
            },
            [
                "Risk evidence is advisory here, but any zero-scale or stop condition must remain fail-closed.",
                "This observer does not change risk limits, locks, thresholds, or capital allocation.",
            ],
            "block_visibility_only",
        ),
        _agent(
            "News",
            "Summarize news coverage and implication status without direct candidate insertion.",
            {
                "news_quality": news_score.get("quality"),
                "news_nonzero_rows": news_score.get("nonzero_rows"),
                "candidate_article_overlap": (news_score.get("meta") or {}).get("candidate_article_overlap")
                if isinstance(news_score.get("meta"), dict)
                else None,
                "candidate_signal_overlap": (news_score.get("meta") or {}).get("candidate_signal_overlap")
                if isinstance(news_score.get("meta"), dict)
                else None,
            },
            [
                "News is interpretation and coverage evidence unless a separate policy change is approved.",
                "WARN quality remains a visible issue, not a reason to upgrade score by wording.",
            ],
            "explain_news_evidence_only",
        ),
        _agent(
            "Memory",
            "Expose current-state and source-note surfaces for future review.",
            {
                "wiki_index_exists": wiki_index_path.exists(),
                "source_index_exists": source_index_path.exists(),
                "wiki_index": str(wiki_index_path),
                "source_index": str(source_index_path),
            },
            [
                "Memory is implemented as files and indexes for retrieval, not as a trading state authority.",
                "Any imported note still needs policy and artifact validation before use.",
            ],
            "retrieve_context_only",
        ),
        _agent(
            "Audit",
            "Record evidence paths, policy boundary, and validation status.",
            {
                "ops_sanity_status": ops.get("status"),
                "ops_sanity_reason": ops.get("reason"),
                "stop_concerns": stop_concerns,
                "strict_interpretation_concerns": strict_interpretation_concerns,
                "read_only_for_trading": True,
                "trading_approved": False,
            },
            [
                "The observer is deterministic and writes only report artifacts.",
                "It does not call an LLM, broker API, order generator, ledger repair, or batch scheduler.",
            ],
            "audit_report_only",
        ),
    ]

    validation = {
        "functional": {"status": "PASS", "note": "observer built role summaries from current artifacts"},
        "consistency": {
            "status": _status_from_bool(orders_exec_exists and score_asof_matches_expected),
            "note": f"orders_exec_exists={orders_exec_exists}, score_asof_matches_expected={score_asof_matches_expected}, strict_asof_matches_D={strict_asof_matches_d}",
        },
        "operational_reflection": {"status": "NA", "note": "no runtime, scheduler, order, fill, ledger, or stats path changed"},
        "policy": {"status": "PASS", "note": "read-only report; Gate/STOP/LOCK/score/risk semantics unchanged"},
        "fail_closed": {"status": "PASS", "note": "strict D mismatch remains visible for order/fill/ledger/stat interpretation"},
        "regression": {"status": "NA", "note": "no trading code path changed by this observer"},
    }

    payload = {
        "generated_at": generated_at,
        "root": str(ROOT),
        "mode": "read_only_agentic_observer",
        "policy_boundary": {
            "read_only_for_trading": True,
            "trading_approved": False,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "lock_modified": False,
            "score_policy_modified": False,
            "risk_policy_modified": False,
        },
        "evidence_paths": {
            "fills": str(fills_path),
            "orders_exec": str(orders_exec_path),
            "final_score": str(final_score_path),
            "news_score": str(news_score_path),
            "future_signal_daily": str(future_daily_path),
            "risk_orchestration": str(risk_path),
            "ops_sanity": str(ops_path),
            "freshness": str(freshness_path),
            "wiki_index": str(wiki_index_path),
            "source_index": str(source_index_path),
        },
        "stop_concerns": stop_concerns,
        "strict_interpretation_concerns": strict_interpretation_concerns,
        "agents": agents,
        "validation": validation,
    }

    _write_json(OUT_JSON, payload)

    agent_rows = []
    for item in agents:
        fact_text = "; ".join(f"{k}={_kv(v)}" for k, v in item["facts"].items())
        interp_text = " / ".join(item["interpretation"])
        agent_rows.append(
            f"| {item['name']} | {item['role']} | {fact_text} | {interp_text} | {item['allowed_action']} |"
        )

    validation_rows = [
        f"| {name} | {value['status']} | {value['note']} |" for name, value in validation.items()
    ]

    stop_text = ", ".join(stop_concerns) if stop_concerns else "none"
    strict_text = ", ".join(strict_interpretation_concerns) if strict_interpretation_concerns else "none"
    md = f"""# Agentic Trading Observer Latest

generated_at: {generated_at}
root: `{ROOT}`
mode: read-only agentic observer

## Boundary

- This report adapts Planner, Alpha, Risk, News, Memory, and Audit agent roles to RootA artifacts.
- It does not call an LLM and does not approve trading.
- Gate, STOP, LOCK, score, risk, order, fill, ledger, and stats semantics remain unchanged.
- stop_concerns: `{stop_text}`
- strict_interpretation_concerns: `{strict_text}`

## Agent Review

| agent | role | facts | interpretation | allowed action |
|---|---|---|---|---|
{chr(10).join(agent_rows)}

## Evidence Paths

| artifact | path |
|---|---|
| fills | `{fills_path}` |
| orders_exec | `{orders_exec_path}` |
| final_score | `{final_score_path}` |
| news_score | `{news_score_path}` |
| future_signal_daily | `{future_daily_path}` |
| risk_orchestration | `{risk_path}` |
| ops_sanity | `{ops_path}` |
| freshness | `{freshness_path}` |
| wiki_index | `{wiki_index_path}` |
| source_index | `{source_index_path}` |

## Validation Matrix

| item | status | note |
|---|---|---|
{chr(10).join(validation_rows)}

"""
    _write_text(OUT_DOC, md)

    print(json.dumps({"status": "PASS", "json": str(OUT_JSON), "markdown": str(OUT_DOC)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
