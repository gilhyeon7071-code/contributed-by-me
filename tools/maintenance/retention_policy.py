from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "2_Logs"
BACKUP_DIR = ROOT / "backup"
ROOTB_RUNS_DIR = Path("E:/vibe/buffett/runs")

BACKUP_RETENTION_DAYS = 90
RUNS_RETENTION_DAYS = 90
TMP_RETENTION_DAYS = 7
DIAG_TMP_RETENTION_DAYS = 30

CACHE_DIR_NAMES = {"__pycache__", ".pytest_cache", ".mypy_cache"}
TEXT_EXTENSIONS = {
    ".bat",
    ".cmd",
    ".ps1",
    ".py",
    ".json",
    ".txt",
    ".md",
    ".yaml",
    ".yml",
    ".ini",
    ".cfg",
    ".csv",
}
PROTECTED_ROOT_NAMES = {
    ".agent",
    ".git",
    "_diag",
    "_runtime",
    "0_Config",
    "1_Raw",
    "2_Logs",
    "3_Models",
    "4_Reports",
    "5_Exports",
    "12_Risk_Controlled",
    "backup",
    "docs",
    "paper",
    "tools",
    "utils",
}
CREDENTIAL_MARKERS = {
    ".env",
    "apikey",
    "api_key",
    "credential",
    "credentials",
    "kis_secret",
    "password",
    "secret",
    "token",
}


@dataclass(frozen=True)
class Decision:
    path: Path
    action: str
    category: str
    reason: str
    age_days: int
    kind: str
    size_bytes: int
    root_scope: str


def _now() -> datetime:
    return datetime.now()


def _age_days(path: Path, now: datetime) -> int:
    try:
        return (now - datetime.fromtimestamp(path.stat().st_mtime)).days
    except OSError:
        return -1


def _size_bytes(path: Path) -> int:
    try:
        if path.is_file():
            return path.stat().st_size
        return -1
    except OSError:
        return -1


def _kind(path: Path) -> str:
    if path.is_dir():
        return "dir"
    if path.is_file():
        return "file"
    return "other"


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _is_tmp_name(name: str) -> bool:
    return name.startswith("tmp_") or name.startswith("_tmp_")


def _is_date_prefix_backup_name(name: str) -> bool:
    return len(name) >= 9 and name[:8].isdigit() and name[8] == "_"


def _is_backup_candidate(path: Path, root: Path) -> bool:
    if path.name.startswith("backup_"):
        return True
    return path.parent == (root / "backup") and _is_date_prefix_backup_name(path.name)


def _is_inside_backup_tree(path: Path, root: Path) -> bool:
    try:
        rel = path.relative_to(root)
    except ValueError:
        return False
    parts = rel.parts
    if len(parts) >= 2 and parts[0] == "backup":
        return True
    return any(part.startswith("backup_") for part in parts[:-1])


def _is_legacy_backup_container_name(name: str) -> bool:
    return name in {"_backup", "_bak"} or name.startswith("_bulk_backup")


def _is_inside_legacy_backup_container(path: Path, root: Path) -> bool:
    try:
        rel = path.relative_to(root)
    except ValueError:
        return False
    parts = rel.parts
    return bool(parts) and _is_legacy_backup_container_name(parts[0])


def _is_credential_like(path: Path) -> bool:
    lowered = path.name.lower()
    return any(marker in lowered for marker in CREDENTIAL_MARKERS)


def _is_text_probe(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in TEXT_EXTENSIONS


def _contains_reference(probe: Path, target_name: str) -> bool:
    try:
        if not _is_text_probe(probe) or probe.stat().st_size > 2_000_000:
            return False
        return target_name in probe.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False


def _is_referenced_in_repo(root: Path, target: Path, probes: Sequence[Path]) -> bool:
    target_name = target.name
    for probe in probes:
        if probe == target:
            continue
        if _is_under(probe, BACKUP_DIR):
            continue
        if any(part in CACHE_DIR_NAMES for part in probe.parts):
            continue
        if _contains_reference(probe, target_name):
            return True
    return False


def _decision(
    path: Path,
    action: str,
    category: str,
    reason: str,
    now: datetime,
    root_scope: str,
) -> Decision:
    return Decision(
        path=path,
        action=action,
        category=category,
        reason=reason,
        age_days=_age_days(path, now),
        kind=_kind(path),
        size_bytes=_size_bytes(path),
        root_scope=root_scope,
    )


def _iter_root_paths(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        yield path


def _collect_root_decisions(root: Path, now: datetime, max_reference_probes: int) -> List[Decision]:
    decisions: List[Decision] = []
    paths = list(_iter_root_paths(root))
    probes = [path for path in paths if _is_text_probe(path) and not _is_under(path, BACKUP_DIR)]
    if max_reference_probes >= 0:
        probes = probes[:max_reference_probes]

    for path in paths:
        rel_parts = path.relative_to(root).parts
        top = rel_parts[0] if rel_parts else path.name

        if _is_inside_backup_tree(path, root) and not _is_backup_candidate(path, root):
            continue

        if _is_inside_legacy_backup_container(path, root):
            if len(rel_parts) == 1:
                decisions.append(_decision(path, "archive_candidate", "legacy_backup_container", "legacy_backup_container_review_first", now, "RootA"))
            continue

        if _is_credential_like(path):
            decisions.append(_decision(path, "review_required", "credential_like", "credential_name_marker", now, "RootA"))
            continue

        if path.name in CACHE_DIR_NAMES:
            decisions.append(_decision(path, "delete_candidate", "cache", "disposable_cache_dir", now, "RootA"))
            continue

        if _is_backup_candidate(path, root):
            age = _age_days(path, now)
            if age >= BACKUP_RETENTION_DAYS:
                decisions.append(_decision(path, "archive_candidate", "backup", "backup_retention_expired_review_first", now, "RootA"))
            else:
                decisions.append(_decision(path, "keep", "backup", "backup_within_retention", now, "RootA"))
            continue

        if path.is_file() and _is_tmp_name(path.name):
            age = _age_days(path, now)
            if top == "_diag" and age < DIAG_TMP_RETENTION_DAYS:
                decisions.append(_decision(path, "keep", "tmp", "diag_tmp_within_retention", now, "RootA"))
            elif age < TMP_RETENTION_DAYS:
                decisions.append(_decision(path, "keep", "tmp", "tmp_within_retention", now, "RootA"))
            elif _is_referenced_in_repo(root, path, probes):
                decisions.append(_decision(path, "review_required", "tmp", "tmp_reference_found", now, "RootA"))
            else:
                decisions.append(_decision(path, "delete_candidate", "tmp", "tmp_retention_expired_unreferenced", now, "RootA"))
            continue

        if len(rel_parts) == 1 and top not in PROTECTED_ROOT_NAMES:
            if path.is_file() and path.suffix.lower() in {".log", ".tmp", ".bak", ".old"}:
                decisions.append(_decision(path, "archive_candidate", "root_artifact", "root_file_archive_review", now, "RootA"))
            elif path.is_dir() and (path.name.startswith("run_") or path.name.startswith("tmp_")):
                decisions.append(_decision(path, "archive_candidate", "root_artifact", "root_dir_archive_review", now, "RootA"))

    return decisions


def _collect_rootb_runs_decisions(now: datetime) -> List[Decision]:
    if not ROOTB_RUNS_DIR.exists():
        return []
    decisions: List[Decision] = []
    for path in ROOTB_RUNS_DIR.iterdir():
        if path.name == "tasklocks":
            decisions.append(_decision(path, "keep", "runs", "tasklocks_protected", now, "RootB"))
            continue
        age = _age_days(path, now)
        if age >= RUNS_RETENTION_DAYS:
            decisions.append(_decision(path, "archive_candidate", "runs", "runs_retention_expired_review_first", now, "RootB"))
        else:
            decisions.append(_decision(path, "keep", "runs", "runs_within_retention", now, "RootB"))
    return decisions


def _decision_to_dict(item: Decision) -> Dict[str, object]:
    return {
        "path": str(item.path),
        "root_scope": item.root_scope,
        "action": item.action,
        "category": item.category,
        "reason": item.reason,
        "age_days": item.age_days,
        "kind": item.kind,
        "size_bytes": item.size_bytes,
    }


def _write_report(report: Dict[str, object], stamp: str) -> Dict[str, str]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    dated = LOG_DIR / f"retention_policy_status_{stamp}.json"
    latest = LOG_DIR / "retention_policy_status_latest.json"
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    dated.write_text(payload, encoding="utf-8")
    latest.write_text(payload, encoding="utf-8")
    return {"dated": str(dated), "latest": str(latest)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only unified retention/archive classifier.")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--max-list", type=int, default=300)
    parser.add_argument("--max-reference-probes", type=int, default=300)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    now = _now()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    limit = max(0, int(args.max_list))

    decisions = _collect_root_decisions(root, now, int(args.max_reference_probes))
    decisions.extend(_collect_rootb_runs_decisions(now))
    decisions = sorted(decisions, key=lambda item: (item.action, item.category, str(item.path)))

    counts: Dict[str, int] = {}
    category_counts: Dict[str, int] = {}
    for item in decisions:
        counts[item.action] = counts.get(item.action, 0) + 1
        key = f"{item.action}:{item.category}"
        category_counts[key] = category_counts.get(key, 0) + 1

    report: Dict[str, object] = {
        "generated_at_local": now.strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "DRY",
        "root": str(root),
        "policy": {
            "backup_retention_days": BACKUP_RETENTION_DAYS,
            "runs_retention_days": RUNS_RETENTION_DAYS,
            "tmp_retention_days": TMP_RETENTION_DAYS,
            "diag_tmp_retention_days": DIAG_TMP_RETENTION_DAYS,
            "actions": ["keep", "archive_candidate", "delete_candidate", "review_required"],
            "apply_supported": False,
            "rootb_runs_dir": str(ROOTB_RUNS_DIR),
        },
        "summary": {
            "decision_count": len(decisions),
            "action_counts": counts,
            "category_counts": category_counts,
            "listed_count": min(limit, len(decisions)),
        },
        "decisions": [_decision_to_dict(item) for item in decisions[:limit]],
    }
    paths = _write_report(report, stamp)

    print(f"[RETENTION_POLICY] mode=DRY")
    print(f"[RETENTION_POLICY] decisions={len(decisions)}")
    print(f"[RETENTION_POLICY] action_counts={json.dumps(counts, ensure_ascii=False, sort_keys=True)}")
    print(f"[RETENTION_POLICY] report={paths['dated']}")
    print(f"[RETENTION_POLICY] latest={paths['latest']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
