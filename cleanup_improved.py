# -*- coding: utf-8 -*-
"""
cleanup_improved.py  (v2.0 - 2026-02-24)

Additional cleanup rules compared with cleanup_1_data.py:
  1) Delete code-fragment files/directories, including zero-byte invalid names.
  2) Move non-standard backup-extension files such as .asofforce_, .BROKEN_, .broken_.
  3) Rename the legacy folder name to _krx_clean archive name.
  4) Windows 寃쎈줈紐??뚯씪 泥섎━
  5) Archive to D:/ when available, otherwise to local _bak/<ts>.
  6) .bak_before_restore, .bak_A_*, .bak_C*_, .bak_H*_ ?⑦꽩 ?ы븿

Usage:
  python cleanup_improved.py                  # DRY: print planned actions only
  python cleanup_improved.py DOIT             # Apply moves/deletes/renames
  python cleanup_improved.py DOIT --no-logs   # 2_Logs 泥섎━ ?쒖쇅
  python cleanup_improved.py DOIT --dest D:/1_Data_Archive
"""
from __future__ import annotations

import sys
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parent  
TS = datetime.now().strftime("%Y%m%d_%H%M%S")

# Archive destination: use D:\ if available, otherwise local _bak/.
_dest_arg = next((a for a in sys.argv if a.startswith("--dest")), None)
if _dest_arg and "=" in _dest_arg:
    DEST_ROOT = Path(_dest_arg.split("=", 1)[1])
elif _dest_arg:
    idx = sys.argv.index(_dest_arg)
    DEST_ROOT = Path(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else None
else:
    d_archive = Path(r"D:\1_Data_Archive")
    DEST_ROOT = d_archive if d_archive.drive and Path(d_archive.drive + "\\").exists() else BASE_DIR / "_bak" / "archive"

DEST_DIR = DEST_ROOT / TS

# ---------------------------------------------------------------------------
# Protected paths
# ---------------------------------------------------------------------------
PROTECT_DIRS = {
    "paper", "12_Risk_Controlled", "_bak", "_krx_clean", "krx_daily_archive",
    "data", "docs", "tools", "utils", "Raw",
    "_krx_seed_full", "_krx_manual", "2_Logs", "news_trading",
}

PROTECT_FILES = {
    "virtual_ledger.csv",
    "requirements.txt",
    "docker-compose.yml",
    "Dockerfile",
    ".gitignore",
    ".dockerignore",
    "holidays.json",
    str(Path("2_Logs") / "run_paper_daily_last.log"),
    str(Path("2_Logs") / "candidates_latest_data.csv"),
    str(Path("2_Logs") / "candidates_latest_meta.json"),
    str(Path("2_Logs") / "candidates_latest.csv"),
    str(Path("2_Logs") / "survivorship_daily_last.json"),
    str(Path("2_Logs") / "liquidity_filter_daily_last.json"),
}

# ---------------------------------------------------------------------------
# Pattern rules
# ---------------------------------------------------------------------------

# Existing .bak_YYYYMMDD pattern.
BAK_PAT = re.compile(r".*\.bak_?\d{8}.*$", re.IGNORECASE)

# Non-standard backup-extension pattern.
BAK_EXT_PAT = re.compile(
    r".*\.(bak_[A-Za-z]|bak_before|broken_|BROKEN_|asofforce_|cleandiag_|"
    r"dateparsefix_|FINALFIX_|indentfix_|loadfix_|nokfix_|parseddiag_|"
    r"pickfix_|rebak_|loadfix_|fixsyntax_)",
    re.IGNORECASE
)

# Code-fragment file-name patterns.
_CODE_FRAG_NAMES = {
    "'", "0", "0)", "0).mean()", "0).mean())", "0]", "0].sum()",
    "127", "8]", "bool", "int", "Dict", "Params", "type",
    "EUC-KR", "cd", "python", "REDUCE", "risk_off",
    "entry_date", "best)", "mx)", "None", "or", "REDUCE",
    "pd.DataFrame", "pd.Series", "Optional[pd.DataFrame]",
    "List[Path]", "tuple[str", "d].head(params.hold).copy()",
    "params.rs_lim", "params.value_min", "params.v_accel_lim",
    "_HIT_C_PATHS.txt",  # 0諛붿씠??
}

# Code-fragment directory-name patterns.
_CODE_FRAG_DIRS = {
    "(report['meta'].get('latest_date')",
    "'')",
    "'').replace('-'",
    "mx",
    "None",
    "or",
    "pq.ParquetFile(str(p)).metadata",
}

# Windows path-like file names.
_WIN_PATH_FILES = {
    "C:UsersjjtopAppDataLocalTempexcel_content.txt",
}

# Windows environment variable file names.
_ENV_VAR_FILES = {
    "%BAKCFG%",
}

# Empty file with Python-expression-like name.
_CODE_CHARS_PAT = re.compile(r"[\[\]()'=.]")


def _is_code_fragment_file(p: Path) -> bool:
    """Return True when the path looks like a code-fragment file."""
    if p.name in _CODE_FRAG_NAMES:
        return True
    if p.name in _WIN_PATH_FILES:
        return True
    if p.name in _ENV_VAR_FILES:
        return True
    # Empty files with Python-expression-like names are treated as fragments.
    try:
        if p.stat().st_size == 0 and _CODE_CHARS_PAT.search(p.name):
            return True
    except OSError:
        pass
    return False


def _is_code_fragment_dir(p: Path) -> bool:
    """Return True when the directory name looks like a code fragment."""
    return p.name in _CODE_FRAG_DIRS


# ---------------------------------------------------------------------------
# Action model
# ---------------------------------------------------------------------------
@dataclass
class Action:
    kind: str          # "move" | "delete" | "rename"
    src: Path
    dst: Path | None   # rename/move target; delete uses None
    reason: str


# ---------------------------------------------------------------------------
# Protection checks
# ---------------------------------------------------------------------------
def _is_protected(p: Path) -> bool:
    try:
        rel = p.relative_to(BASE_DIR)
    except Exception:
        return False
    parts = rel.parts
    if not parts:
        return True
    if parts[0] in PROTECT_DIRS:
        return True
    # Protect exact file paths and file names.
    rel_s = "/".join(parts)
    if rel_s in PROTECT_FILES or parts[-1] in PROTECT_FILES:
        return True
    return False


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------
def plan_actions() -> list[Action]:
    actions: list[Action] = []

    for p in sorted(BASE_DIR.iterdir()):
        # Skip hidden system directories.
        if p.name.startswith(".") and p.is_dir():
            continue
        if _is_protected(p):
            continue

        name = p.name

        # 1) Rename legacy Korean folder name
        if p.is_dir() and name == "새 폴더":
            actions.append(Action(
                kind="rename",
                src=p,
                dst=BASE_DIR / "krx_daily_archive",
                reason="legacy_folder_rename_to_krx_daily_archive"
            ))
            continue

        # 2) Delete or move code-fragment directories.
        if p.is_dir() and _is_code_fragment_dir(p):
            # Move non-empty fragment directories; delete empty ones.
            children = list(p.iterdir())
            if not children:
                actions.append(Action(kind="delete", src=p, dst=None,
                                      reason="code_fragment_empty_directory"))
            else:
                actions.append(Action(kind="move", src=p,
                                      dst=DEST_DIR / "garbage_dirs" / _safe_name(name),
                                      reason="code_fragment_non_empty_directory"))
            continue

        if p.is_file():
            # 3) Delete code-fragment files.
            if _is_code_fragment_file(p):
                actions.append(Action(kind="delete", src=p, dst=None,
                                      reason="code_fragment_file"))
                continue

            # 4) Move non-standard backup-extension files.
            if BAK_EXT_PAT.match(name):
                actions.append(Action(kind="move", src=p,
                                      dst=DEST_DIR / "backups_ext" / name,
                                      reason="non_standard_backup_extension"))
                continue

            # 5) Move existing .bak_YYYYMMDD backup files.
            if BAK_PAT.match(name):
                actions.append(Action(kind="move", src=p,
                                      dst=DEST_DIR / "backups" / name,
                                      reason="dated_bak_backup"))
                continue

    # 6) Move 2_Logs files older than 30 days unless --no-logs is set.
    no_logs = "--no-logs" in sys.argv
    if not no_logs:
        LOG_DATED_PAT = re.compile(
            r".*_(\d{8})(?:_\d{6})?\.(json|csv|log)$", re.IGNORECASE
        )
        cutoff = datetime.now() - timedelta(days=30)
        logs = BASE_DIR / "2_Logs"
        if logs.exists():
            for p in logs.iterdir():
                if p.is_dir():
                    continue
                if p.stem.endswith("_last"):
                    continue
                m = LOG_DATED_PAT.match(p.name)
                if not m:
                    continue
                try:
                    dt = datetime.strptime(m.group(1), "%Y%m%d")
                except Exception:
                    continue
                if dt < cutoff:
                    actions.append(Action(kind="move", src=p,
                                          dst=DEST_DIR / "2_Logs" / p.name,
                                          reason="logs_older_than_30_days"))

    # Deduplicate planned actions.
    seen: set[str] = set()
    deduped: list[Action] = []
    for a in actions:
        k = str(a.src).lower()
        if k not in seen:
            seen.add(k)
            deduped.append(a)
    return deduped


def _safe_name(name: str) -> str:
    """Replace filesystem-unsafe characters with underscores."""
    return re.sub(r'[<>:"/\\|?*\x00-\x1f\']', "_", name)[:80]


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
def _ensure(dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)


def _safe_move(src: Path, dst: Path) -> None:
    _ensure(dst)
    if dst.exists():
        stem, suf = dst.stem, dst.suffix
        for i in range(1, 999):
            cand = dst.with_name(f"{stem}__{i}{suf}")
            if not cand.exists():
                dst = cand
                break
    shutil.move(str(src), str(dst))


def _safe_delete(p: Path) -> None:
    if p.is_dir():
        shutil.rmtree(str(p), ignore_errors=True)
    else:
        p.unlink(missing_ok=True)


def _safe_rename(src: Path, dst: Path) -> None:
    if dst.exists():
        print(f"  [WARN] rename target already exists: {dst}")
        return
    src.rename(dst)


def execute(actions: list[Action]) -> dict:
    counts = {"move": 0, "delete": 0, "rename": 0, "error": 0}
    for a in actions:
        try:
            if a.kind == "delete":
                _safe_delete(a.src)
                counts["delete"] += 1
            elif a.kind == "move":
                _safe_move(a.src, a.dst)
                counts["move"] += 1
            elif a.kind == "rename":
                _safe_rename(a.src, a.dst)
                counts["rename"] += 1
        except Exception as e:
            counts["error"] += 1
            print(f"  [ERR] {a.kind} failed: {a.src}  err={e}")
    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    mode = "DRY"
    for arg in sys.argv[1:]:
        if arg.upper() in ("DRY", "DOIT"):
            mode = arg.upper()

    print(f"[CLEANUP v2] base={BASE_DIR}")
    print(f"[CLEANUP v2] dest={DEST_DIR}")
    print(f"[CLEANUP v2] mode={mode}")
    print()

    actions = plan_actions()

    # Print by action type.
    by_kind: dict[str, list[Action]] = {}
    for a in actions:
        by_kind.setdefault(a.kind, []).append(a)

    for kind in ("delete", "rename", "move"):
        items = by_kind.get(kind, [])
        if not items:
            continue
        label = {"delete": "delete", "rename": "rename", "move": "move"}[kind]
        print(f"-- {label} ({len(items)} items)")
        for a in items:
            rel = a.src.relative_to(BASE_DIR)
            arrow = f" ??{a.dst.name}" if a.dst else ""
            print(f"  {rel}{arrow}  [{a.reason}]")
        print()

    total = len(actions)
    print(f"[CLEANUP v2] total={total} planned (delete={len(by_kind.get('delete',[]))}, "
          f"rename={len(by_kind.get('rename',[]))}, "
          f"move={len(by_kind.get('move',[]))})")

    if mode == "DRY":
        print("[CLEANUP v2] DRY mode - no filesystem changes applied.")
        print("[CLEANUP v2] To apply: python cleanup_improved.py DOIT")
        return 0

    # DOIT mode.
    try:
        DEST_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[CLEANUP v2] FATAL: failed to create dest: {DEST_DIR}  err={e}")
        return 3

    counts = execute(actions)

    # Save result.
    report = {
        "ts": TS,
        "base_dir": str(BASE_DIR),
        "dest_dir": str(DEST_DIR),
        "mode": mode,
        "counts": counts,
        "actions": [
            {"kind": a.kind, "src": str(a.src),
             "dst": str(a.dst) if a.dst else None, "reason": a.reason}
            for a in actions
        ],
    }
    out = BASE_DIR / "_diag" / f"cleanup_result_{TS}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n[CLEANUP v2] finished: move={counts['move']}, delete={counts['delete']}, "
          f"rename={counts['rename']}, error={counts['error']}")
    print(f"[CLEANUP v2] result saved: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


