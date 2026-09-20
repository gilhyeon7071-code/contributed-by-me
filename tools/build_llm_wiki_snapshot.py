from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ROOT_B = Path("E:/vibe/buffett")
OUT_ROOT = ROOT / "docs" / "llm_wiki"
WIKI_INDEX = OUT_ROOT / "wiki_index_latest.md"
CURRENT_STATE = OUT_ROOT / "00_Current_State" / "system_state_latest.md"
SOURCE_INDEX = OUT_ROOT / "02_Sources" / "source_index_latest.md"
ARTIFACT_INDEX = OUT_ROOT / "05_Logs" / "latest_artifact_index.md"
EXTERNAL_READINESS = OUT_ROOT / "05_Logs" / "external_readiness_latest.json"
SCHEDULER_REGISTRATION = OUT_ROOT / "05_Logs" / "scheduler_registration_latest.json"
OPERATIONAL_CLASSIFICATION_MD = OUT_ROOT / "05_Logs" / "current_operational_classification_latest.md"
OPERATIONAL_CLASSIFICATION_JSON = OUT_ROOT / "05_Logs" / "current_operational_classification_latest.json"
FRESHNESS_PTR = ROOT / "2_Logs" / "freshness_source_last.json"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    for encoding in ("utf-8-sig", "utf-8"):
        try:
            return json.loads(path.read_text(encoding=encoding))
        except UnicodeDecodeError:
            continue
    return {}


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


def _mtime(path: Path) -> str:
    if not path.exists():
        return "missing"
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def _size(path: Path) -> str:
    if not path.exists():
        return "missing"
    return str(path.stat().st_size)


def _relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _latest_buy_ymd(rows: list[dict[str, str]]) -> str:
    buys = [r for r in rows if str(r.get("side", "")).upper() == "BUY"]
    source = buys or rows
    ymds = [str(r.get("datetime", ""))[:8] for r in source if str(r.get("datetime", ""))[:8].isdigit()]
    return max(ymds) if ymds else "unknown"


def _latest_freshness_path() -> Path:
    ptr = _read_json(FRESHNESS_PTR)
    raw = str(ptr.get("last") or "").strip()
    return Path(raw) if raw else ROOT / "2_Logs" / "freshness_source_latest.json"


def _kv(value: Any) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def _date_asof_status(
    orders_exec_exists: bool,
    score_asof_matches_expected: bool,
    strict_asof_matches_d: bool,
) -> str:
    if not orders_exec_exists or not score_asof_matches_expected:
        return "BLOCKED"
    if not strict_asof_matches_d:
        return "REVIEW_STRICT_D_MISMATCH"
    return "READY"


def main() -> int:
    fills_path = ROOT / "paper" / "fills.csv"
    state_path = ROOT / "paper" / "paper_state.json"
    lock_path = ROOT / "paper" / "paper_engine_config.lock.json"
    intraday_path = ROOT / "2_Logs" / "intraday_loop_status_latest.json"
    final_score_path = ROOT / "2_Logs" / "final_score_merge_status_latest.json"
    forecast_path = ROOT / "2_Logs" / "forecast_score_validation_latest.json"
    news_score_path = ROOT / "2_Logs" / "news_score_status_latest.json"
    news_implication_path = ROOT / "2_Logs" / "news_implication_status_latest.json"
    freshness_path = _latest_freshness_path()

    fills = _read_csv_rows(fills_path)
    d_ymd = _latest_buy_ymd(fills)
    orders_exec_path = ROOT / "paper" / f"orders_{d_ymd}_exec.xlsx"

    paper_state = _read_json(state_path)
    lock = _read_json(lock_path)
    intraday = _read_json(intraday_path)
    final_score = _read_json(final_score_path)
    forecast = _read_json(forecast_path)
    news_score = _read_json(news_score_path)
    news_implication = _read_json(news_implication_path)
    freshness = _read_json(freshness_path)

    asof_ymd = final_score.get("asof_ymd") or final_score.get("score_asof_ymd") or "unknown"
    score_expected_ymd = freshness.get("expected_date") or "unknown"
    score_expected_mode = freshness.get("expected_date_mode") or "unknown"
    score_asof_matches_expected = asof_ymd == score_expected_ymd
    orders_exec_exists = orders_exec_path.exists()
    strict_asof_matches_d = asof_ymd == d_ymd
    strict_asof_d_stop = not strict_asof_matches_d
    date_asof_status = _date_asof_status(
        orders_exec_exists=orders_exec_exists,
        score_asof_matches_expected=score_asof_matches_expected,
        strict_asof_matches_d=strict_asof_matches_d,
    )

    generated_at = datetime.now().isoformat(timespec="seconds")
    macro_freshness = final_score.get("macro_freshness") if isinstance(final_score.get("macro_freshness"), dict) else {}
    news_gate = final_score.get("news_gate") if isinstance(final_score.get("news_gate"), dict) else {}
    forecast_validation = (
        final_score.get("forecast_validation")
        if isinstance(final_score.get("forecast_validation"), dict)
        else {}
    )

    status_md = f"""# System State Latest

generated_at: {generated_at}
root: `{ROOT}`
mode: read-only wiki snapshot

## Facts

| key | value | evidence |
|---|---:|---|
| D | `{d_ymd}` | `paper\\fills.csv` latest BUY ymd |
| orders_exec_exists | `{_kv(orders_exec_exists)}` | `{orders_exec_path}` |
| asof_ymd | `{asof_ymd}` | `2_Logs\\final_score_merge_status_latest.json` |
| score_expected_ymd | `{score_expected_ymd}` | `{freshness_path}` |
| score_expected_mode | `{score_expected_mode}` | `{freshness_path}` |
| score_asof_matches_expected | `{_kv(score_asof_matches_expected)}` | compare score_expected_ymd vs asof_ymd |
| strict_asof_matches_D | `{_kv(strict_asof_matches_d)}` | compare fills D vs asof_ymd |
| strict_asof_D_stop | `{_kv(strict_asof_d_stop)}` | strict order/fill/ledger/stat interpretation guard |
| date_asof_interpretation_status | `{date_asof_status}` | `docs\\llm_wiki\\01_Policies\\date_asof_interpretation.md` |
| paper_open_positions | `{len(paper_state.get("open_positions", []) or [])}` | `paper\\paper_state.json` |
| paper_next_trade_seq | `{_kv(paper_state.get("next_trade_seq"))}` | `paper\\paper_state.json` |
| config_lock_ts | `{_kv(lock.get("ts"))}` | `paper\\paper_engine_config.lock.json` |
| intraday_ts | `{_kv(intraday.get("ts"))}` | `2_Logs\\intraday_loop_status_latest.json` |
| intraday_steps | `{_kv(intraday.get("steps_ok"))}/{_kv(intraday.get("steps_total"))}` | `2_Logs\\intraday_loop_status_latest.json` |
| final_score_generated_at | `{_kv(final_score.get("generated_at"))}` | `2_Logs\\final_score_merge_status_latest.json` |
| final_score_rows | `{_kv(final_score.get("rows"))}` | `2_Logs\\final_score_merge_status_latest.json` |
| score_regime | `{_kv(final_score.get("score_regime"))}` | `2_Logs\\final_score_merge_status_latest.json` |
| macro_freshness_status | `{_kv(macro_freshness.get("status"))}` | `2_Logs\\final_score_merge_status_latest.json` |
| news_gate | `{_kv(news_gate.get("gate"))}` | `2_Logs\\final_score_merge_status_latest.json` |
| forecast_validation_status | `{_kv(forecast_validation.get("status"))}` | `2_Logs\\final_score_merge_status_latest.json` |

## Interpretation

- This snapshot is a context artifact for LLM reading.
- It does not approve trading and does not modify any policy or runtime state.
- Candidate/score freshness is checked against `score_expected_ymd`.
- `strict_asof_matches_D=false` remains a STOP concern for order/fill/ledger/stat interpretation.
- `date_asof_interpretation_status` is a reporting label only, not a trading approval.

## Validation Matrix

| item | status | note |
|---|---|---|
| function | PASS | generator wrote this snapshot from current artifacts |
| consistency | {"PASS" if orders_exec_exists and score_asof_matches_expected else "FAIL"} | orders_exec_exists={_kv(orders_exec_exists)}, score_asof_matches_expected={_kv(score_asof_matches_expected)}, strict_asof_matches_D={_kv(strict_asof_matches_d)} |
| operations reflected | NA | no scheduler or trading runtime was changed |
| policy | PASS | read-only docs only; Gate/STOP/LOCK semantics unchanged |
| FAIL-CLOSED | PASS | strict D mismatch remains visible for order/fill/ledger/stat interpretation |
| regression | NA | no trading code path changed |

"""

    artifacts = [
        WIKI_INDEX,
        OUT_ROOT / "README.md",
        OUT_ROOT / "OBSIDIAN_START.md",
        OUT_ROOT / "RUNBOOK.md",
        OUT_ROOT / "SOURCE_FORMAT_CONTRACT.md",
        OUT_ROOT / "AUTO_INGEST_PLAN.md",
        OUT_ROOT / "01_Policies" / "date_asof_interpretation.md",
        ROOT / "tools" / "collect_llm_wiki_exports.py",
        ROOT / "run_llm_wiki_collect.bat",
        ROOT / "run_llm_wiki_pipeline.bat",
        ROOT / "tools" / "check_llm_wiki_external_readiness.py",
        ROOT / "tools" / "build_llm_wiki_operational_classification.py",
        ROOT / "run_llm_wiki_readiness.bat",
        ROOT / "tools" / "register_llm_wiki_scheduler.ps1",
        ROOT / "run_llm_wiki_register_scheduler_dryrun.bat",
        ROOT / "run_llm_wiki_register_scheduler_apply.bat",
        ROOT / "tools" / "validate_llm_wiki_source_contract.py",
        OUT_ROOT / "00_Inbox" / "examples" / "slack_contract_valid.md",
        OUT_ROOT / "00_Inbox" / "examples" / "slack_export_sample.json",
        OUT_ROOT / "00_Inbox" / "import_exports" / "README.md",
        CURRENT_STATE,
        SOURCE_INDEX,
        ARTIFACT_INDEX,
        EXTERNAL_READINESS,
        SCHEDULER_REGISTRATION,
        OPERATIONAL_CLASSIFICATION_MD,
        OPERATIONAL_CLASSIFICATION_JSON,
        fills_path,
        state_path,
        lock_path,
        intraday_path,
        final_score_path,
        forecast_path,
        news_score_path,
        news_implication_path,
        freshness_path,
        orders_exec_path,
        ROOT / "AGENTS.md",
        ROOT / "docs" / "references" / "WORK_PROMPT_MANDATORY.md",
        ROOT / "docs" / "references" / "FINAL_REPORT_TEMPLATE.md",
        ROOT_B / "docs" / "llm_wiki" / "wiki_index_latest.md",
        ROOT_B / "run_llm_wiki_snapshot_rootb.bat",
        ROOT_B / "tools" / "build_llm_wiki_snapshot_rootb.py",
    ]
    artifact_lines = [
        "# Latest Artifact Index",
        "",
        f"generated_at: {generated_at}",
        "",
        "| path | exists | bytes | mtime |",
        "|---|---:|---:|---|",
    ]
    for path in artifacts:
        artifact_lines.append(
            f"| `{path}` | `{_kv(path.exists())}` | `{_size(path)}` | `{_mtime(path)}` |"
        )
    artifact_lines.append("")
    artifact_lines.append("This index is read-only evidence for LLM retrieval.")
    artifact_lines.append("")

    _write_text(CURRENT_STATE, status_md)
    _write_text(ARTIFACT_INDEX, "\n".join(artifact_lines))
    wiki_index_lines = [
        "# LLM Wiki Index",
        "",
        f"generated_at: {generated_at}",
        f"root: `{ROOT}`",
        "",
        "## Primary Entry Points",
        "",
        f"- Current state: `{_relative(CURRENT_STATE)}`",
        f"- Source notes index: `{_relative(SOURCE_INDEX)}`",
        f"- Runtime artifact index: `{_relative(ARTIFACT_INDEX)}`",
        f"- Operational classification: `{_relative(OPERATIONAL_CLASSIFICATION_MD)}`",
        f"- RootB dashboard Wiki index: `{ROOT_B / 'docs' / 'llm_wiki' / 'wiki_index_latest.md'}`",
        f"- Policy pointer: `{_relative(OUT_ROOT / '01_Policies' / 'roota_operating_contract.md')}`",
        f"- Date/as-of interpretation: `{_relative(OUT_ROOT / '01_Policies' / 'date_asof_interpretation.md')}`",
        f"- Inbox guide: `{_relative(OUT_ROOT / '00_Inbox' / 'README.md')}`",
        "",
        "## Current State Summary",
        "",
        f"- D: `{d_ymd}`",
        f"- orders_exec_exists: `{_kv(orders_exec_exists)}`",
        f"- asof_ymd: `{asof_ymd}`",
        f"- score_expected_ymd: `{score_expected_ymd}`",
        f"- score_asof_matches_expected: `{_kv(score_asof_matches_expected)}`",
        f"- strict_asof_matches_D: `{_kv(strict_asof_matches_d)}`",
        f"- date_asof_interpretation_status: `{date_asof_status}`",
        f"- intraday_steps: `{_kv(intraday.get('steps_ok'))}/{_kv(intraday.get('steps_total'))}`",
        "",
        "## Boundary",
        "",
        "- This index is for LLM retrieval only.",
        "- It does not approve trading or modify runtime behavior.",
        "- Gate, STOP, LOCK, risk, score, order, fill, ledger, and stats semantics remain unchanged.",
        "",
    ]
    _write_text(WIKI_INDEX, "\n".join(wiki_index_lines))
    print(str(WIKI_INDEX))
    print(str(CURRENT_STATE))
    print(str(ARTIFACT_INDEX))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
