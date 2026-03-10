# -*- coding: utf-8 -*-
"""
cleanup_1_data.py

Purpose
- Clean up E:\1_Data by moving non-essential artifacts to D:\1_Data_Archive\<timestamp>\

Modes
- DRY  (default): print plan only
- DOIT          : execute moves

Design goals
- Safe by default: protect operational folders (paper/, 12_Risk_Controlled/)
- No PowerShell required; works from CMD
"""
from __future__ import annotations

import os
import sys
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parent  # E:\1_Data
DEST_ROOT = Path(r"D:\1_Data_Archive")      # will be auto-created
TS = datetime.now().strftime("%Y%m%d_%H%M%S")
DEST_DIR = DEST_ROOT / TS

PROTECT_DIRS = {
    "paper",
    "12_Risk_Controlled",
}

PROTECT_FILES = {
    # keep these “latest pointers”
    str(Path("2_Logs") / "run_paper_daily_last.log"),
    str(Path("2_Logs") / "candidates_latest_data.csv"),
    str(Path("2_Logs") / "candidates_latest_meta.json"),
    str(Path("2_Logs") / "candidates_latest.csv"),
    str(Path("2_Logs") / "survivorship_daily_last.json"),
    str(Path("2_Logs") / "liquidity_filter_daily_last.json"),
}

# Move rules
ZIP_PAT = re.compile(r"^_fix_(inputs|outputs).+\.zip$", re.IGNORECASE)
BAK_PAT = re.compile(r".*\.bak_?\d{8}.*$", re.IGNORECASE)

# In 2_Logs, move date-stamped artifacts older than N days (keep *_last.*)
LOG_DATED_PAT = re.compile(r".*_(\d{8})(?:_\d{6})?\.(json|csv|log)$", re.IGNORECASE)

DEFAULT_LOG_RETENTION_DAYS = 30

@dataclass
class MoveItem:
    src: Path
    dst: Path
    reason: str

def is_under_protect_dir(p: Path) -> bool:
    try:
        rel = p.relative_to(BASE_DIR)
    except Exception:
        return False
    parts = rel.parts
    return len(parts) > 0 and parts[0] in PROTECT_DIRS

def is_protected_file(p: Path) -> bool:
    try:
        rel = p.relative_to(BASE_DIR)
    except Exception:
        return False
    rel_s = str(rel).replace("/", "\\")
    return rel_s in PROTECT_FILES

def plan_moves() -> list[MoveItem]:
    items: list[MoveItem] = []

    # 1) root-level fix zips + backups
    for p in BASE_DIR.iterdir():
        if p.is_dir():
            continue
        name = p.name
        if ZIP_PAT.match(name):
            items.append(MoveItem(p, DEST_DIR / "packages" / name, "fix_zip"))
        elif BAK_PAT.match(name):
            items.append(MoveItem(p, DEST_DIR / "backups" / name, "backup_file"))

    # 2) 2_Logs cleanup (dated artifacts older than retention)
    logs = BASE_DIR / "2_Logs"
    if logs.exists() and logs.is_dir():
        cutoff = datetime.now() - timedelta(days=DEFAULT_LOG_RETENTION_DAYS)
        for p in logs.iterdir():
            if p.is_dir():
                continue
            if is_protected_file(p):
                continue
            # keep *_last.* always
            if p.stem.endswith("_last"):
                continue
            m = LOG_DATED_PAT.match(p.name)
            if not m:
                continue
            ymd = m.group(1)
            try:
                dt = datetime.strptime(ymd, "%Y%m%d")
            except Exception:
                continue
            if dt < cutoff:
                items.append(MoveItem(p, DEST_DIR / "2_Logs" / p.name, f"logs_older_than_{DEFAULT_LOG_RETENTION_DAYS}d"))

    # 3) _archive folder (if exists): move whole folder (can be large)
    arch = BASE_DIR / "_archive"
    if arch.exists() and arch.is_dir():
        items.append(MoveItem(arch, DEST_DIR / "_archive", "archive_folder"))

    # 4) 8_MetaEvolution large reports (optional): move only CSV snapshots older than retention
    meta_ev = BASE_DIR / "8_MetaEvolution"
    if meta_ev.exists() and meta_ev.is_dir():
        cutoff = datetime.now() - timedelta(days=DEFAULT_LOG_RETENTION_DAYS)
        for p in meta_ev.glob("*.csv"):
            try:
                wtime = datetime.fromtimestamp(p.stat().st_mtime)
            except Exception:
                continue
            if wtime < cutoff:
                items.append(MoveItem(p, DEST_DIR / "8_MetaEvolution" / p.name, f"meta_csv_older_than_{DEFAULT_LOG_RETENTION_DAYS}d"))

    # Safety filters: never move protected dirs/files
    safe_items: list[MoveItem] = []
    for it in items:
        if is_under_protect_dir(it.src):
            continue
        if is_protected_file(it.src):
            continue
        safe_items.append(it)

    # De-dup (src unique)
    seen = set()
    uniq: list[MoveItem] = []
    for it in safe_items:
        k = str(it.src).lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)

    return uniq

def ensure_parent(dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)

def move_one(src: Path, dst: Path) -> None:
    ensure_parent(dst)
    if dst.exists():
        # avoid overwrite by suffixing
        stem = dst.stem
        suf = dst.suffix
        i = 1
        while True:
            cand = dst.with_name(f"{stem}__{i}{suf}")
            if not cand.exists():
                dst = cand
                break
            i += 1
    shutil.move(str(src), str(dst))

def main() -> int:
    mode = "DRY"
    if len(sys.argv) >= 2:
        mode = str(sys.argv[1]).strip().upper()
    if mode not in ("DRY", "DOIT"):
        print("[CLEANUP] Usage: cleanup_1_data.py [DRY|DOIT]")
        return 2

    # auto-create dest root
    try:
        DEST_ROOT.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[CLEANUP] FATAL: cannot create dest root: {DEST_ROOT} err={e}")
        return 3

    plan = plan_moves()

    report = {
        "ts": TS,
        "base_dir": str(BASE_DIR),
        "dest_root": str(DEST_ROOT),
        "dest_dir": str(DEST_DIR),
        "mode": mode,
        "counts": {"planned": len(plan), "moved": 0, "skipped": 0},
        "moves": [],
    }

    print(f"[CLEANUP] base={BASE_DIR}")
    print(f"[CLEANUP] dest={DEST_DIR}")
    print(f"[CLEANUP] mode={mode}")
    print(f"[CLEANUP] planned_moves={len(plan)}")

    for it in plan:
        rel = str(it.src.relative_to(BASE_DIR))
        dst = it.dst
        report["moves"].append({"src": str(it.src), "dst": str(dst), "reason": it.reason})
        print(f" - {rel} -> {dst}  ({it.reason})")

    if mode == "DRY":
        # save report
        DEST_DIR.mkdir(parents=True, exist_ok=True)
        out = DEST_DIR / "cleanup_report.json"
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[CLEANUP] DRY RUN. report_saved={out}")
        return 0

    # DOIT
    DEST_DIR.mkdir(parents=True, exist_ok=True)
    moved = 0
    for it in plan:
        try:
            move_one(it.src, it.dst)
            moved += 1
        except Exception as e:
            report["counts"]["skipped"] += 1
            print(f"[CLEANUP] WARN: move failed: {it.src} -> {it.dst} err={e}")

    report["counts"]["moved"] = moved
    out = DEST_DIR / "cleanup_report.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[CLEANUP] DONE moved={moved}/{len(plan)} report_saved={out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
