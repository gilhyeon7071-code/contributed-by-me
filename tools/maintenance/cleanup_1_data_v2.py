# -*- coding: utf-8 -*-
r"""
cleanup_1_data_v2.py

Purpose
- Move non-essential root/2_Logs artifacts out of E:\1_Data.
- Corrected replacement for cleanup_1_data.py after the following findings
  were confirmed against AGENTS.md and the original source (2026-07-10):
    1. destination was D:\1_Data_Archive, not the AGENTS.md-approved
       D:\1_Data_Offsite_Backup path
    2. DRY mode still wrote to D: (dest dir + report) before checking mode
    3. .bak file matching had no minimum-age floor, so backups as young
       as 7 days were swept up
    4. no SSOT/evidence keyword protection (latest/orders/fills/ledger/stats)
    5. no hash/size verification, no partial-failure exit code, no lock,
       no recovery manifest
    6. the _archive folder was planned as a single item instead of being
       walked file-by-file

Modes
- DRY      (default): plan only, zero writes outside E:\1_Data\2_Logs
- APPLY    : requires --apply AND --confirm-d-drive (mirrors
             offsite_backup_manifest.py's convention). Performs
             copy -> verify (sha256 + size) -> delete-original moves.
- ROLLBACK : --rollback <manifest_path> restores every entry in a prior
             APPLY manifest back to its original location.

Design goals
- No write to D:\ in DRY mode, ever.
- Destination root is validated to resolve exactly to
  D:\1_Data_Offsite_Backup (raises otherwise) and cleanup content is
  kept under a dedicated _cleanup_archive\<stamp>\ subpath so it never
  mixes with the narrow whitelisted offsite-backup targets defined in
  offsite_backup_manifest.py.
- Every move is copy2 -> sha256+size compare -> delete source only on
  verified match. Any failure leaves the source untouched.
- Partial failure -> process exit code 1 (never silently 0).
- Recovery manifest written to both the archive destination and
  E:\1_Data\2_Logs, and --rollback reads it back.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

BASE_DIR = Path(r"E:\1_Data")
DEST_ROOT = Path(r"D:\1_Data_Offsite_Backup")
ARCHIVE_SUBDIR = "_cleanup_archive"
LOG_DIR = BASE_DIR / "2_Logs"
DIAG_DIR = BASE_DIR / "_diag"
LOCK_PATH = DIAG_DIR / "cleanup_1_data_v2.lock"
LOCK_STALE_MINUTES = 60

BACKUP_RETENTION_DAYS = 90       # must match tools/maintenance/retention_policy.py
LOG_RETENTION_DAYS = 30          # unchanged from the original script's intent

PROTECT_DIRS = {
    "paper",
    "12_Risk_Controlled",
}

PROTECT_FILES = {
    str(Path("2_Logs") / "run_paper_daily_last.log"),
    str(Path("2_Logs") / "candidates_latest_data.csv"),
    str(Path("2_Logs") / "candidates_latest_meta.json"),
    str(Path("2_Logs") / "candidates_latest.csv"),
    str(Path("2_Logs") / "survivorship_daily_last.json"),
    str(Path("2_Logs") / "liquidity_filter_daily_last.json"),
}

# Any filename containing one of these (case-insensitive) is treated as
# possible SSOT/evidence content and is never auto-moved, regardless of
# age or pattern match. AGENTS.md SS2: orders(D) -> fills(D) -> ledger -> stats.
EVIDENCE_KEYWORDS = (
    "latest",
    "_last",
    "order",
    "fill",
    "ledger",
    "stats",
    "ssot",
)

# [2026-08-25] 코드가 실제로 읽는 .bak_ 계열. 이력 아카이브지 백업이 아니다.
#   candidates_latest_data.bak_<YMD>_*.csv 를 읽는 도구 5개를 확인했다:
#     build_cluster_candidate_quality_audit.py:80
#     build_mdd_candidate_backup_join_semantics_review.py:100
#     build_paper_mdd_candidate_quality_entry_timing.py:131/152/180
#     diagnose_pnl_improvement_levers.py:110
#     nightly_data_integrity_check.py:33
#   여기 없는 .bak_ 는 순수 롤백 사본으로 보고 BACKUP_RETENTION_DAYS 를 적용한다.
CONSUMED_BAK_PREFIXES = (
    "candidates_latest_data.bak_",
)


def is_consumed_bak(name: str) -> bool:
    low = name.lower()
    return any(low.startswith(x) for x in CONSUMED_BAK_PREFIXES)


ZIP_PAT = re.compile(r"^_fix_(inputs|outputs).+\.zip$", re.IGNORECASE)
# [2026-08-25] .bak 와 타임스탬프 사이에 라벨이 끼는 형태를 못 잡고 있었다.
#   실제: joined_trades_final_latest.csv.bak_signal_integ_20260819_135506
#   옛 패턴은 .bak_ 뒤에 바로 8자리 숫자를 요구해서 35,545개가 통째로 빠졌다.
BAK_PAT = re.compile(r".*\.bak_?[A-Za-z0-9_]*?\d{8}.*$", re.IGNORECASE)
# [2026-08-25] jsonl 추가. 없어서 kis_ws_ticks_*.jsonl 99개 40.0 GB 가 안 보였다.
LOG_DATED_PAT = re.compile(r".*_(\d{8})(?:_\d{6})?\.(json|jsonl|csv|log)$", re.IGNORECASE)


@dataclass
class MoveItem:
    src: Path
    dst_rel: str  # path relative to the run's archive root, forward-slash free
    reason: str
    size_bytes: int = 0
    age_days: int = -1


@dataclass
class MoveResult:
    item: MoveItem
    ok: bool
    error: Optional[str] = None
    src_sha256: str = ""
    dst_sha256: str = ""
    dst_path: str = ""


def _now() -> datetime:
    return datetime.now()


def _age_days(path: Path, now: datetime) -> int:
    try:
        return (now - datetime.fromtimestamp(path.stat().st_mtime)).days
    except OSError:
        return -1


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def _rel(p: Path) -> Path:
    return p.relative_to(BASE_DIR)


def is_under_protect_dir(p: Path) -> bool:
    try:
        parts = _rel(p).parts
    except ValueError:
        return False
    return len(parts) > 0 and parts[0] in PROTECT_DIRS


def is_protected_file(p: Path) -> bool:
    try:
        rel_s = str(_rel(p)).replace("/", "\\")
    except ValueError:
        return False
    return rel_s in PROTECT_FILES


def is_evidence_like(p: Path) -> bool:
    name_lower = p.name.lower()
    return any(kw in name_lower for kw in EVIDENCE_KEYWORDS)


def _iter_archive_folder(now: datetime) -> List[MoveItem]:
    """Walk _archive file-by-file instead of treating it as one opaque item."""
    root = BASE_DIR / "_archive"
    items: List[MoveItem] = []
    if not root.exists() or not root.is_dir():
        return items
    for p in root.rglob("*"):
        if p.is_dir():
            continue
        if is_evidence_like(p):
            continue
        rel = _rel(p)
        items.append(
            MoveItem(
                src=p,
                dst_rel=str(Path("_archive") / rel.relative_to("_archive")),
                reason="archive_folder_member",
                size_bytes=p.stat().st_size if p.exists() else 0,
                age_days=_age_days(p, now),
            )
        )
    return items


def plan_moves(now: datetime) -> List[MoveItem]:
    items: List[MoveItem] = []

    # 1) root-level fix zips + aged backup snapshots
    for p in BASE_DIR.iterdir():
        if p.is_dir():
            continue
        name = p.name
        if is_evidence_like(p):
            continue
        if ZIP_PAT.match(name):
            items.append(MoveItem(p, str(Path("packages") / name), "fix_zip", age_days=_age_days(p, now)))
        elif BAK_PAT.match(name):
            age = _age_days(p, now)
            if age < BACKUP_RETENTION_DAYS:
                continue  # fix: no more unconditional sweep of recent rollback copies
            items.append(MoveItem(p, str(Path("backups") / name), "backup_file", age_days=age))

    # 2) 2_Logs dated artifacts older than retention (keep *_last.* and evidence-like names)
    logs = BASE_DIR / "2_Logs"
    if logs.exists() and logs.is_dir():
        cutoff = now - timedelta(days=LOG_RETENTION_DAYS)
        for p in logs.iterdir():
            if p.is_dir():
                continue
            if is_protected_file(p) or is_evidence_like(p):
                continue
            if p.stem.endswith("_last"):
                continue
            m = LOG_DATED_PAT.match(p.name)
            if not m:
                continue
            try:
                dt = datetime.strptime(m.group(1), "%Y%m%d")
            except ValueError:
                continue
            if dt < cutoff:
                items.append(
                    MoveItem(
                        p,
                        str(Path("2_Logs") / p.name),
                        f"logs_older_than_{LOG_RETENTION_DAYS}d",
                        age_days=_age_days(p, now),
                    )
                )

    # 2b) 2_Logs 의 .bak_ 롤백 사본
    #   [2026-08-25] BAK_PAT 규칙이 1) 에서 BASE_DIR.iterdir() 즉 루트만 훑고 있었다.
    #   2_Logs 에 같은 형태가 35,547개 쌓여 있었는데 어떤 규칙에도 안 걸렸다.
    #   is_evidence_like 는 여기서 쓰지 않는다 - "latest" 파일의 백업본이
    #   원본의 보호를 물려받는 것이 정확히 이 누락의 원인이었다.
    #   대신 실제로 읽히는 계열만 CONSUMED_BAK_PREFIXES 로 뺀다.
    if logs.exists() and logs.is_dir():
        for p in logs.iterdir():
            if p.is_dir() or is_protected_file(p):
                continue
            if not BAK_PAT.match(p.name):
                continue
            if is_consumed_bak(p.name):
                continue
            age = _age_days(p, now)
            if age < BACKUP_RETENTION_DAYS:
                continue
            items.append(
                MoveItem(p, str(Path("2_Logs") / p.name), "logs_bak_older_than_%dd"
                         % BACKUP_RETENTION_DAYS, age_days=age)
            )

    # 3) _archive folder, expanded file-by-file (fix: was a single opaque item)
    items.extend(_iter_archive_folder(now))

    # 4) 8_MetaEvolution CSV snapshots older than retention
    meta_ev = BASE_DIR / "8_MetaEvolution"
    if meta_ev.exists() and meta_ev.is_dir():
        cutoff = now - timedelta(days=LOG_RETENTION_DAYS)
        for p in meta_ev.glob("*.csv"):
            if is_evidence_like(p):
                continue
            try:
                wtime = datetime.fromtimestamp(p.stat().st_mtime)
            except OSError:
                continue
            if wtime < cutoff:
                items.append(
                    MoveItem(
                        p,
                        str(Path("8_MetaEvolution") / p.name),
                        f"meta_csv_older_than_{LOG_RETENTION_DAYS}d",
                        age_days=_age_days(p, now),
                    )
                )

    # Safety filters
    safe: List[MoveItem] = []
    for it in items:
        if is_under_protect_dir(it.src):
            continue
        if is_protected_file(it.src):
            continue
        # [2026-08-25] .bak 사본은 원본의 evidence 보호를 물려받지 않는다.
        #   이 전역 필터가 규칙과 무관하게 한 번 더 걸러서, 규칙 2b 가 골라낸
        #   joined_trades_final_latest.csv.bak_signal_integ_* 18,012개가
        #   이름 속 "latest" 때문에 전부 여기서 탈락하고 있었다.
        #   원본(joined_trades_final_latest.csv)은 그대로 보호된다 - 사본만 나간다.
        #   실제로 읽히는 사본 계열은 is_consumed_bak 으로 계속 보호한다.
        _bak_copy = bool(BAK_PAT.match(it.src.name)) and not is_consumed_bak(it.src.name)
        if not _bak_copy and is_evidence_like(it.src):
            continue
        if it.size_bytes == 0 and it.src.exists() and it.src.is_file():
            try:
                it.size_bytes = it.src.stat().st_size
            except OSError:
                pass
        safe.append(it)

    seen = set()
    uniq: List[MoveItem] = []
    for it in safe:
        k = str(it.src).lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)

    return sorted(uniq, key=lambda i: str(i.src))


def _ensure_inside_dest(path: Path, dest_root: Path) -> None:
    resolved = path.resolve()
    root = dest_root.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError(f"destination outside allowed root: {path}")


def _write_lock() -> None:
    DIAG_DIR.mkdir(parents=True, exist_ok=True)
    LOCK_PATH.write_text(
        json.dumps({"pid": os.getpid(), "started_at": _now().isoformat()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _remove_lock() -> None:
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink()
    except OSError:
        pass


def _check_lock() -> Optional[str]:
    if not LOCK_PATH.exists():
        return None
    try:
        age_minutes = (time.time() - LOCK_PATH.stat().st_mtime) / 60.0
    except OSError:
        return None
    if age_minutes >= LOCK_STALE_MINUTES:
        try:
            LOCK_PATH.unlink()
        except OSError:
            pass
        return None
    return f"lock held (age={age_minutes:.1f}min): {LOCK_PATH}"


def safe_move(src: Path, dst: Path) -> MoveResult:
    """copy2 -> verify sha256+size -> delete source only on verified match."""
    item_stub = MoveItem(src=src, dst_rel=str(dst), reason="")
    try:
        pre_mtime = src.stat().st_mtime
        pre_size = src.stat().st_size
        src_hash = _sha256(src)

        dst.parent.mkdir(parents=True, exist_ok=True)
        final_dst = dst
        if final_dst.exists():
            stem, suf = final_dst.stem, final_dst.suffix
            i = 1
            while final_dst.exists():
                final_dst = final_dst.with_name(f"{stem}__{i}{suf}")
                i += 1

        tmp_dst = final_dst.with_name(final_dst.name + ".part")
        import shutil

        shutil.copy2(src, tmp_dst)
        dst_hash = _sha256(tmp_dst)
        dst_size = tmp_dst.stat().st_size

        if dst_hash != src_hash or dst_size != pre_size:
            tmp_dst.unlink(missing_ok=True)
            return MoveResult(item_stub, ok=False, error="hash_or_size_mismatch_after_copy")

        # guard against concurrent modification of the source during copy
        if src.stat().st_mtime != pre_mtime:
            tmp_dst.unlink(missing_ok=True)
            return MoveResult(item_stub, ok=False, error="source_changed_during_copy")

        tmp_dst.rename(final_dst)
        src.unlink()
        return MoveResult(
            item_stub, ok=True, src_sha256=src_hash, dst_sha256=dst_hash, dst_path=str(final_dst)
        )
    except Exception as exc:  # noqa: BLE001 - report, never crash the batch
        return MoveResult(item_stub, ok=False, error=f"{type(exc).__name__}: {exc}")


def _write_report(report: Dict[str, object], stamp: str, extra_path: Optional[Path]) -> List[Path]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    dated = LOG_DIR / f"cleanup_1_data_v2_status_{stamp}.json"
    latest = LOG_DIR / "cleanup_1_data_v2_status_latest.json"
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    dated.write_text(payload, encoding="utf-8")
    latest.write_text(payload, encoding="utf-8")
    written = [dated, latest]
    if extra_path is not None:
        extra_path.parent.mkdir(parents=True, exist_ok=True)
        extra_path.write_text(payload, encoding="utf-8")
        written.append(extra_path)
    return written


def cmd_run(apply_mode: bool, confirm_d_drive: bool, max_list: int) -> int:
    if apply_mode and not confirm_d_drive:
        print("[FAILED] --apply requires --confirm-d-drive")
        return 2

    dest_root_resolved = DEST_ROOT.resolve()
    expected = Path(r"D:\1_Data_Offsite_Backup").resolve()
    if dest_root_resolved != expected:
        print(f"[FAILED] dest root is not allowed: {DEST_ROOT}")
        return 3

    now = _now()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    plan = plan_moves(now)

    total_size = sum(i.size_bytes for i in plan)

    report: Dict[str, object] = {
        "generated_at_local": now.strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "APPLY" if apply_mode else "DRY",
        "base_dir": str(BASE_DIR),
        "dest_root": str(DEST_ROOT),
        "archive_run_dir": str(DEST_ROOT / ARCHIVE_SUBDIR / stamp),
        "policy": {
            "backup_retention_days": BACKUP_RETENTION_DAYS,
            "log_retention_days": LOG_RETENTION_DAYS,
            "evidence_keywords": list(EVIDENCE_KEYWORDS),
        },
        "planned_count": len(plan),
        "planned_size_bytes": total_size,
        "moved_count": 0,
        "failed_count": 0,
        "moves": [],
        "failures": [],
    }

    print(f"[CLEANUP_V2] base={BASE_DIR}")
    print(f"[CLEANUP_V2] mode={'APPLY' if apply_mode else 'DRY'}")
    print(f"[CLEANUP_V2] planned_count={len(plan)} planned_size_bytes={total_size}")

    for it in plan[: max(0, max_list)]:
        print(f" - {it.src} ({it.reason}, age={it.age_days}d, {it.size_bytes}B)")

    if not apply_mode:
        report["moves"] = [
            {"src": str(i.src), "dst_rel": i.dst_rel, "reason": i.reason, "age_days": i.age_days, "size_bytes": i.size_bytes}
            for i in plan[: max(0, max_list)]
        ]
        written = _write_report(report, stamp, extra_path=None)
        print(f"[CLEANUP_V2] DRY RUN. report={written[0]}")
        print("[CLEANUP_V2] no writes were made outside E:\\1_Data\\2_Logs")
        return 0

    lock_conflict = _check_lock()
    if lock_conflict:
        print(f"[FAILED] {lock_conflict}")
        return 4
    _write_lock()

    run_dir = DEST_ROOT / ARCHIVE_SUBDIR / stamp
    try:
        _ensure_inside_dest(run_dir, DEST_ROOT)
        run_dir.mkdir(parents=True, exist_ok=True)

        moved = 0
        failed = 0
        move_rows: List[Dict[str, object]] = []
        failure_rows: List[Dict[str, object]] = []

        for it in plan:
            dst = run_dir / it.dst_rel
            _ensure_inside_dest(dst, DEST_ROOT)
            result = safe_move(it.src, dst)
            if result.ok:
                moved += 1
                move_rows.append(
                    {
                        "src": str(it.src),
                        "dst": result.dst_path,
                        "reason": it.reason,
                        "sha256": result.src_sha256,
                        "size_bytes": it.size_bytes,
                    }
                )
            else:
                failed += 1
                failure_rows.append(
                    {"src": str(it.src), "reason": it.reason, "error": result.error}
                )
                print(f"[CLEANUP_V2] FAIL: {it.src} err={result.error}")

        report["moved_count"] = moved
        report["failed_count"] = failed
        report["moves"] = move_rows
        report["failures"] = failure_rows

        manifest_path = run_dir / "manifest.json"
        written = _write_report(report, stamp, extra_path=manifest_path)
        print(f"[CLEANUP_V2] DONE moved={moved}/{len(plan)} failed={failed}")
        print(f"[CLEANUP_V2] report={written[0]}")
        print(f"[CLEANUP_V2] recovery_manifest={manifest_path}")
        return 0 if failed == 0 else 1
    finally:
        _remove_lock()


def cmd_rollback(manifest_path: Path) -> int:
    if not manifest_path.exists():
        print(f"[FAILED] manifest not found: {manifest_path}")
        return 2

    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    moves = data.get("moves", [])
    if not moves:
        print("[ROLLBACK] nothing to restore (moves list empty)")
        return 0

    restored = 0
    failed = 0
    for row in moves:
        src = Path(str(row["dst"]))  # currently-archived location
        dst = Path(str(row["src"]))  # original location to restore to
        expected_hash = str(row.get("sha256", ""))
        try:
            if not src.exists():
                raise FileNotFoundError(f"archived file missing: {src}")
            current_hash = _sha256(src)
            if expected_hash and current_hash != expected_hash:
                raise ValueError("archived file hash no longer matches manifest")
            dst.parent.mkdir(parents=True, exist_ok=True)
            if dst.exists():
                raise FileExistsError(f"restore target already exists: {dst}")
            import shutil

            shutil.move(str(src), str(dst))
            restored += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"[ROLLBACK] FAIL: {src} -> {dst} err={exc}")

    print(f"[ROLLBACK] restored={restored}/{len(moves)} failed={failed}")
    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-d-drive", action="store_true")
    parser.add_argument("--max-list", type=int, default=500)
    parser.add_argument("--rollback", type=str, default="")
    args = parser.parse_args()

    if args.rollback:
        return cmd_rollback(Path(args.rollback))

    return cmd_run(apply_mode=bool(args.apply), confirm_d_drive=bool(args.confirm_d_drive), max_list=args.max_list)


if __name__ == "__main__":
    raise SystemExit(main())
