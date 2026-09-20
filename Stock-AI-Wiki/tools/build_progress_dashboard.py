from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = ROOT.parent
LOGS = DATA_ROOT / "2_Logs"
TASK_NAME = "VIBE_Stock_AI_Wiki_Update"


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8-sig")


def count_contains(paths: list[Path], needle: str) -> int:
    return sum(1 for path in paths if needle in read_text(path))


def frontmatter_value(text: str, key: str) -> str:
    match = re.search(rf"(?m)^{re.escape(key)}:\s*(.+?)\s*$", text)
    return match.group(1).strip().strip('"') if match else ""


def article_archive_stats() -> dict[str, int]:
    archive_dir = ROOT / "01_Sources" / "article_archive"
    files = sorted(archive_dir.glob("*.json"))
    articles = []
    for path in files:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        for raw in data.get("articles", []):
            if isinstance(raw, dict):
                articles.append(raw)
    return {
        "archive_files": len(files),
        "archive_articles": len(articles),
        "body_available": sum(1 for item in articles if str(item.get("body") or "").strip()),
        "body_missing": sum(1 for item in articles if not str(item.get("body") or "").strip()),
    }


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def parse_dt(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def task_info() -> dict[str, object]:
    ps = (
        "$i=Get-ScheduledTaskInfo -TaskName VIBE_Stock_AI_Wiki_Update; "
        "[pscustomobject]@{"
        "LastRunTime=$i.LastRunTime.ToString('s');"
        "LastTaskResult=$i.LastTaskResult;"
        "NextRunTime=$i.NextRunTime.ToString('s');"
        "NumberOfMissedRuns=$i.NumberOfMissedRuns"
        "} | ConvertTo-Json -Compress"
    )
    try:
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", ps],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception as exc:
        return {"status": "unavailable", "error": type(exc).__name__}
    if result.returncode != 0:
        return {"status": "unavailable", "error": result.stderr.strip() or f"rc={result.returncode}"}
    try:
        data = json.loads(result.stdout)
    except Exception:
        return {"status": "unavailable", "error": "parse_failed"}
    data["status"] = "available"
    return data


def latest_probe_summary() -> dict[str, object]:
    data = load_json(LOGS / "google_news_rss_probe_latest.json")
    results = data.get("results") if isinstance(data.get("results"), list) else []
    return {
        "generated_at": data.get("generated_at") or "unknown",
        "quality": data.get("quality") or "unknown",
        "symbol_count": data.get("symbol_count") or len(results),
        "item_count_total": data.get("item_count_total") or 0,
        "db_write": data.get("db_write"),
        "db_rows_saved": data.get("db_rows_saved"),
        "codes": [str(item.get("code") or "").strip() for item in results if isinstance(item, dict) and item.get("code")],
    }


def latest_coverage_summary() -> dict[str, object]:
    data = load_json(LOGS / "google_news_rss_coverage_report_latest.json")
    coverage = data.get("coverage") if isinstance(data.get("coverage"), dict) else {}
    return {
        "generated_at": data.get("generated_at") or "unknown",
        "quality": data.get("quality") or "unknown",
        "rows": data.get("rows") or 0,
        "reference_ymd": data.get("reference_ymd") or "unknown",
        "naver_covered": coverage.get("naver_covered", 0),
        "google_rss_covered": coverage.get("google_rss_covered", 0),
        "kis_title_covered": coverage.get("kis_title_covered", 0),
        "any_covered": coverage.get("any_covered", 0),
    }


def status_text(value: object) -> str:
    text = str(value if value is not None else "unknown")
    return text.replace("|", "\\|")


def build_dashboard() -> str:
    now = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    now_dt = parse_dt(now)
    md_files = [path for path in ROOT.rglob("*.md") if ".obsidian" not in path.parts]
    real_md = [
        path
        for path in md_files
        if not path.name.startswith("SAMPLE")
        and "99_Prompts" not in path.parts
        and "SAMPLE" not in path.name
        and "EXAMPLE" not in path.name
    ]
    companies = sorted((ROOT / "10_Companies").glob("KRX_*.md"))
    concepts = sorted((ROOT / "20_Themes" / "concepts").glob("*.md"))
    source_notes = sorted((ROOT / "01_Sources").glob("*google-rss-coverage-source.md"))
    verification_notes = sorted((ROOT / "01_Sources").glob("*google-rss-coverage-verification.md"))
    news_notes = sorted((ROOT / "30_News").glob("*google-rss-coverage.md"))
    thesis_notes = sorted((ROOT / "20_Themes").glob("*google-rss-thesis-blocked.md"))
    auto_linked = count_contains(real_md, "STOCK_AI_AUTO_LINKS_BEGIN")
    verification_unknown = count_contains(verification_notes, "verification_status: unknown")
    verification_passed = count_contains(verification_notes, "verification_status: verified")
    original_text_available = count_contains(verification_notes, "original_text_available: true")
    trading_approved = count_contains(real_md, "trading_approved: true")
    execution_allowed = count_contains(real_md, "execution_allowed: true")
    stats = article_archive_stats()
    scheduler = task_info()
    probe = latest_probe_summary()
    coverage = latest_coverage_summary()
    latest_source_dt = max(
        [dt for dt in [parse_dt(probe["generated_at"]), parse_dt(coverage["generated_at"])] if dt is not None],
        default=None,
    )
    freshness_status = "CURRENT" if latest_source_dt is None or now_dt is None or now_dt >= latest_source_dt else "STALE"
    latest_codes = ", ".join(f"`{code}`" for code in probe["codes"]) or "not_available"

    stage_rows = [
        ("1. 문서 갱신", "DONE" if auto_linked else "TODO", f"자동 링크 블록 {auto_linked}개"),
        ("2. 새 개념 문서", "DONE" if concepts else "TODO", f"개념 후보 {len(concepts)}개"),
        ("3. 관련 링크 연결", "DONE" if auto_linked and companies and concepts else "TODO", "회사/개념/뉴스/검증/소스 연결"),
        ("4. 본문 기반 검증", "TODO" if stats["body_available"] == 0 else "PARTIAL", f"본문 있음 {stats['body_available']} / 없음 {stats['body_missing']}"),
        ("5. Shadow Review", "BLOCKED", "검증 완료 소스 필요"),
        ("6. Paper Review", "BLOCKED", "Shadow 결과 필요"),
    ]
    stage_table = "\n".join(f"| {name} | {status} | {meaning} |" for name, status, meaning in stage_rows)
    concept_lines = "\n".join(f"- [[{path.stem}]]" for path in concepts) or "- not_available"
    company_lines = "\n".join(f"- [[{path.stem}]]" for path in companies) or "- not_available"

    return f"""---
id: stock-ai-wiki-progress-dashboard
type: dashboard
title: Stock AI Wiki Progress Dashboard
created: {now[:10]}
updated: {now[:10]}
status: active
stage: dashboard

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: generated progress dashboard
---

# Stock AI Wiki Progress Dashboard

Generated at: `{now}`

## Stage Progress

| Stage | Status | Evidence |
|---|---|---|
{stage_table}

## Current Counts

| Item | Count |
|---|---:|
| Company notes | {len(companies)} |
| Concept candidate notes | {len(concepts)} |
| Source notes | {len(source_notes)} |
| Verification notes | {len(verification_notes)} |
| News notes | {len(news_notes)} |
| Blocked thesis notes | {len(thesis_notes)} |
| Auto-linked notes | {auto_linked} |
| Article archive files | {stats["archive_files"]} |
| Archived article metadata rows | {stats["archive_articles"]} |
| Article bodies available | {stats["body_available"]} |
| Article bodies missing | {stats["body_missing"]} |
| Verification unknown notes | {verification_unknown} |
| Verification passed notes | {verification_passed} |
| Trading approved notes | {trading_approved} |
| Execution allowed notes | {execution_allowed} |

## Automation Status

| Item | Value |
|---|---|
| Scheduler task | `{TASK_NAME}` |
| Scheduler info status | `{status_text(scheduler.get("status"))}` |
| Last run time | `{status_text(scheduler.get("LastRunTime"))}` |
| Last task result | `{status_text(scheduler.get("LastTaskResult"))}` |
| Next run time | `{status_text(scheduler.get("NextRunTime"))}` |
| Missed runs | `{status_text(scheduler.get("NumberOfMissedRuns"))}` |

## Data Freshness

| Item | Value |
|---|---|
| Dashboard generated at | `{now}` |
| Latest RSS probe generated at | `{status_text(probe["generated_at"])}` |
| Latest coverage generated at | `{status_text(coverage["generated_at"])}` |
| Freshness status | `{freshness_status}` |
| RSS probe quality | `{status_text(probe["quality"])}` |
| Coverage quality | `{status_text(coverage["quality"])}` |

## Latest Observation

| Item | Value |
|---|---|
| Latest probe codes | {latest_codes} |
| RSS symbol count | {status_text(probe["symbol_count"])} |
| RSS item count total | {status_text(probe["item_count_total"])} |
| Coverage reference ymd | `{status_text(coverage["reference_ymd"])}` |
| Coverage rows | {status_text(coverage["rows"])} |
| Google RSS covered | {status_text(coverage["google_rss_covered"])} |
| KIS title covered | {status_text(coverage["kis_title_covered"])} |
| Naver covered | {status_text(coverage["naver_covered"])} |

## Evidence Boundary

| Evidence type | Count | Meaning |
|---|---:|---|
| RSS metadata rows | {status_text(probe["item_count_total"])} | Headlines, links, timestamps, and sources only |
| Article archive metadata rows | {stats["archive_articles"]} | Local archive seed rows |
| Article bodies available | {stats["body_available"]} | Body-backed source verification input |
| Article bodies missing | {stats["body_missing"]} | Verification remains blocked |

## Main Blockers

- Article body text is not archived yet.
- Source verification remains `unknown`.
- Stage 5 and Stage 6 remain blocked.
- Trading approval and execution approval remain `false`.

## Stage 4 Waiting Reason

- `verification_status: verified` notes: `{verification_passed}`
- `original_text_available: true` notes: `{original_text_available}`
- Trading approved notes: `{trading_approved}`
- Execution allowed notes: `{execution_allowed}`
- Shadow Decision remains blocked until body-backed verification and review queue records exist.

## Company Hubs

{company_lines}

## Concept Candidates

{concept_lines}

## Visual Navigation

- Use Obsidian Graph View from this note to see company, concept, news, source, verification, and thesis links.
- Use the counts above to check whether the wiki is still metadata-only or has moved into body-backed verification.

## Safety Boundary

- This dashboard is a read-only progress view.
- It does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Stock-AI-Wiki progress dashboard.")
    parser.add_argument("--out", type=Path, default=ROOT / "WIKI_PROGRESS_DASHBOARD.md")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    text = build_dashboard()
    if args.apply:
        args.out.write_text(text, encoding="utf-8")
    print(json.dumps({"apply": args.apply, "out": str(args.out), "bytes": len(text.encode("utf-8"))}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
