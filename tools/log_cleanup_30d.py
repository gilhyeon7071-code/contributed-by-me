from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Date-stamped artifacts, e.g. *_YYYYMMDD.json or *_YYYYMMDD_HHMMSS.log
DATED_ARTIFACT_RE = re.compile(r".+_(\d{8})(?:_\d{6})?\.(json|csv|log|txt)$", re.IGNORECASE)
# Backup artifacts that carry date in backup suffix, e.g. *.bak_20260308_100911
BACKUP_ARTIFACT_RE = re.compile(r".+\.bak.*?(\d{8})(?:_\d{6})?.*$", re.IGNORECASE)

PRESERVE_EXACT = {
    "verification_runtime_evidence_latest.json",
    "design_evidence_latest.json",
    "pending_entry_status_latest.json",
}
ALLOWED_ROOTS = (
    Path(r"E:\1_Data").resolve(),
    Path(r"E:\vibe\buffett").resolve(),
)


def _now_local() -> datetime:
    return datetime.now()


def _safe_int(x, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _is_preserved_name(name: str) -> bool:
    n = name.lower()
    if n.startswith("log_cleanup_report_"):
        return True
    if "_latest" in n or "_last" in n:
        return True
    if n in PRESERVE_EXACT:
        return True
    return False


def _parse_ymd_from_name(name: str, bak_only: bool = False) -> datetime | None:
    if bak_only:
        m = BACKUP_ARTIFACT_RE.match(name)
    else:
        m = DATED_ARTIFACT_RE.match(name)
    if not m:
        return None
    ymd = m.group(1)
    try:
        return datetime.strptime(ymd, "%Y%m%d")
    except Exception:
        return None


def _is_under_allowed_root(target: Path) -> bool:
    for root in ALLOWED_ROOTS:
        if root in [target] + list(target.parents):
            return True
    return False


def _collect_candidates(
    target: Path,
    cutoff: datetime,
    include_ext: Set[str],
    max_list: int,
    bak_only: bool,
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    scanned = 0
    eligible = 0
    would_count = 0
    would_bytes = 0
    errors: List[str] = []

    samples: List[Dict[str, object]] = []
    delete_entries: List[Dict[str, object]] = []

    for p in target.rglob("*"):
        if not p.is_file():
            continue

        scanned += 1
        ext = p.suffix.lower()
        if include_ext and ext not in include_ext:
            continue

        if _is_preserved_name(p.name):
            continue

        if bak_only and ".bak" not in p.name.lower():
            continue

        file_day = _parse_ymd_from_name(p.name, bak_only=bak_only)
        if file_day is None:
            continue

        try:
            st = p.stat()
            size = _safe_int(st.st_size, 0)
        except Exception as e:
            errors.append(f"stat_fail:{p}:{type(e).__name__}")
            continue

        eligible += 1

        if file_day < cutoff:
            would_count += 1
            would_bytes += size
            entry = {
                "path": str(p),
                "size": size,
                "date_from_name": file_day.strftime("%Y-%m-%d"),
            }
            delete_entries.append(entry)
            if len(samples) < max_list:
                samples.append(entry)

    summary: Dict[str, object] = {
        "scanned_files": scanned,
        "eligible_files": eligible,
        "would_delete_count": would_count,
        "would_delete_bytes": would_bytes,
        "errors_count": len(errors),
        "errors_sample": errors[:20],
    }
    return delete_entries, {**summary, "samples": samples}


def _delete_candidates(entries: List[Dict[str, object]]) -> Tuple[int, int, List[str]]:
    deleted = 0
    deleted_bytes = 0
    errors: List[str] = []

    for e in entries:
        sp = str(e.get("path") or "").strip()
        if not sp:
            continue
        try:
            p = Path(sp)
            if not p.exists() or not p.is_file():
                continue
            size = _safe_int(p.stat().st_size, 0)
            os.remove(p)
            deleted += 1
            deleted_bytes += size
        except Exception as ex:
            errors.append(f"delete_fail:{sp}:{type(ex).__name__}")

    return deleted, deleted_bytes, errors


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default=r"E:\1_Data\2_Logs", help="cleanup target dir")
    ap.add_argument("--retention-days", type=int, default=30, help="keep recent N days")
    ap.add_argument("--enabled", action="store_true", help="when set, actually delete candidates")
    ap.add_argument("--bak-only", action="store_true", help="consider backup-like files only (.bak*)")
    ap.add_argument("--all-ext", action="store_true", help="ignore extension filter and scan all extensions")
    ap.add_argument(
        "--include-ext",
        default=".json,.csv,.txt,.log",
        help="comma-separated file extensions to consider (ignored when --all-ext)",
    )
    ap.add_argument("--max-list", type=int, default=50, help="max sample list size in report")
    args = ap.parse_args()

    target = Path(args.target)
    if not target.exists() or not target.is_dir():
        print(f"[CLEANUP] FAIL target_not_found: {target}")
        return 2

    try:
        tgt = target.resolve()
    except Exception:
        tgt = target.absolute()
    if not _is_under_allowed_root(tgt):
        print(f"[CLEANUP] FAIL target_outside_allowed_roots: {tgt}")
        return 3

    now = _now_local()
    retention_days = int(args.retention_days)
    cutoff = now - timedelta(days=retention_days)

    include_ext: Set[str] = set()
    if not args.all_ext and args.include_ext.strip():
        include_ext = {s.strip().lower() for s in args.include_ext.split(",") if s.strip()}
        include_ext = {"." + e.lstrip(".") for e in include_ext}

    candidates, collect = _collect_candidates(
        target=target,
        cutoff=cutoff,
        include_ext=include_ext,
        max_list=max(0, int(args.max_list)),
        bak_only=bool(args.bak_only),
    )

    report: Dict[str, object] = {
        "generated_at_local": now.strftime("%Y-%m-%d %H:%M:%S"),
        "target": str(target),
        "retention_days": retention_days,
        "cutoff_local": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "enabled_delete": bool(args.enabled),
        "bak_only": bool(args.bak_only),
        "all_ext": bool(args.all_ext),
        "include_ext": sorted(include_ext),
        "summary": {
            "scanned_files": collect["scanned_files"],
            "eligible_files": collect["eligible_files"],
            "would_delete_count": collect["would_delete_count"],
            "would_delete_bytes": collect["would_delete_bytes"],
            "errors_count": collect["errors_count"],
            "errors_sample": collect["errors_sample"],
        },
        "would_delete_samples": collect["samples"],
    }

    if args.enabled:
        deleted, deleted_bytes, del_errors = _delete_candidates(candidates)
        report["delete_result"] = {
            "deleted_count": deleted,
            "deleted_bytes": deleted_bytes,
            "errors_count": len(del_errors),
            "errors_sample": del_errors[:20],
        }

    stamp = now.strftime("%Y%m%d_%H%M%S")
    report_path = target / f"log_cleanup_report_{stamp}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[CLEANUP] WROTE_REPORT {report_path}")
    print(f"[CLEANUP] target={target} retention_days={retention_days} cutoff={report['cutoff_local']}")
    print(
        "[CLEANUP] scanned={scanned} eligible={eligible} would_delete={would} bytes={bytes}".format(
            scanned=collect["scanned_files"],
            eligible=collect["eligible_files"],
            would=collect["would_delete_count"],
            bytes=collect["would_delete_bytes"],
        )
    )
    print(f"[CLEANUP] enabled_delete={bool(args.enabled)} bak_only={bool(args.bak_only)} all_ext={bool(args.all_ext)}")
    if args.enabled and isinstance(report.get("delete_result"), dict):
        d = report["delete_result"]
        print(
            "[CLEANUP] deleted={deleted} deleted_bytes={deleted_bytes} delete_errors={errors}".format(
                deleted=d.get("deleted_count", 0),
                deleted_bytes=d.get("deleted_bytes", 0),
                errors=d.get("errors_count", 0),
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
