# -*- coding: utf-8 -*-
import os
import shutil
from datetime import datetime


def _base_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _archive_root(base_dir: str) -> str:
    return os.path.join(base_dir, "_archive")


# Keep operational root files in place.
PROTECT_EXT = {".py", ".bat", ".ps1", ".json", ".csv", ".parquet"}

# Explicit move targets for noisy artifacts.
MOVE_EXT = {".zip", ".tmp", ".bak", ".log"}


def _should_move_file(name: str) -> bool:
    lower = name.lower()
    ext = os.path.splitext(lower)[1]

    # Explicit backup naming patterns such as *.bak_20260308_100000
    if lower.endswith(".bak") or ".bak_" in lower:
        return True

    if ext in PROTECT_EXT:
        return False

    if ext in MOVE_EXT:
        return True

    return False


def run_cleanup() -> int:
    base = _base_dir()
    arch_root = _archive_root(base)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    arch = os.path.join(arch_root, ts)
    os.makedirs(arch, exist_ok=True)

    moved = 0

    for name in os.listdir(base):
        full = os.path.join(base, name)

        if os.path.isdir(full):
            continue

        if not _should_move_file(name):
            continue

        try:
            shutil.move(full, os.path.join(arch, name))
            moved += 1
        except Exception as e:
            print(f"[CLEANUP] move failed: {name} ({e})")

    print("=" * 60)
    print(f"[CLEANUP] moved_files={moved}")
    print(f"[CLEANUP] archive={arch}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(run_cleanup())
