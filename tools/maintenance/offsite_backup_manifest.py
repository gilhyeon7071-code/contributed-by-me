from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List


ROOT = Path("E:/1_Data")
DEST_ROOT = Path("D:/1_Data_Offsite_Backup")
LOG_DIR = ROOT / "2_Logs"


@dataclass(frozen=True)
class BackupTarget:
    rel_path: str
    label: str
    patterns: tuple = ()      # 디렉터리일 때만 쓴다. 비면 그 아래 전부
    recursive: bool = True


# [2026-09-08] 정책상 절대 나가면 안 되는 것 (AGENTS.md §14):
#   "2차 백업 대상에는 API 키, 자격증명, 개인 메모 파일을 포함하지 않는다"
#   디렉터리 대상을 허용하는 순간 이 규칙을 사람 눈으로 지킬 수 없다. 코드가 지키게 한다.
# _bak / _archive 는 코드의 옛 사본이다. 2차 백업에 넣으면 "무엇이 현행인가" 가 흐려진다.
DENY_PARTS = (".secrets", "__pycache__", ".git", "backup", "_bak", "_archive", "scratch", "scratchpad")
DENY_NAME_TOKENS = ("app_key", "app_secret", "account_no", "bot_token", "chat_id",
                    "token", "secret", "credential", "password", "passwd")
DENY_SUFFIX = (".bak", ".broken", ".recovered", ".tmp", ".log")


def _denied(path: Path) -> bool:
    parts = {p.lower() for p in path.parts}
    if any(d in parts for d in DENY_PARTS):
        return True
    name = path.name.lower()
    if any(tok in name for tok in DENY_NAME_TOKENS):
        return True
    if any(name.endswith(sfx) for sfx in DENY_SUFFIX):
        return True
    if name.startswith("backup_") or name.startswith("_tmp"):
        return True
    return False


# [2026-09-08 갱신] 이전 목록 7개는 v41.1 페이퍼 엔진 세트였고 **지금의 핵심이 하나도 없었다**
#   (PLANS (251) 감사). 마지막 실제 복사가 2026-04-30 이라 지금 E: 가 죽으면 4월로 돌아간다.
#   정책 §14 의 네 범주(핵심 실행 코드 / 설정·락 / 체결·원장·상태 / 가격 parquet)에
#   **연구 계약**(사전등록·동결 매니페스트·작업기록)을 더한다 -
#   그것이 없으면 살아남은 데이터의 의미를 복원할 수 없다.
TARGETS = [
    # 핵심 실행 코드
    BackupTarget("tools", "code_tools", patterns=("*.py",)),
    BackupTarget(".", "code_root_py", patterns=("*.py",), recursive=False),
    BackupTarget(".", "code_root_cmd", patterns=("*.bat", "*.ps1", "*.vbs"), recursive=False),
    BackupTarget("utils", "code_utils", patterns=("*.py",)),
    BackupTarget("paper_engine.py", "engine"),
    # 설정 / 락 / 계약
    BackupTarget("config", "config"),
    BackupTarget("paper/paper_engine_config.json", "config_engine"),
    BackupTarget("paper/paper_engine_config.lock.json", "config_engine_lock"),
    BackupTarget("AGENTS.md", "contract_agents"),
    # 체결 / 원장 / 상태
    BackupTarget("paper/paper_state.json", "state"),
    BackupTarget("paper/fills.csv", "fills"),
    BackupTarget("paper/trades.csv", "trades"),
    BackupTarget("2_Logs/topn", "ledger_topn", patterns=("*.csv", "*.json", "*.xlsx")),
    BackupTarget("state", "state_runtime", patterns=("*.json", "*.txt")),
    BackupTarget("live", "live", patterns=("*.csv", "*.json", "*.ndjson")),  # 실체결 로그는 ndjson 이다
    # 연구 계약
    BackupTarget(".agent", "worklog", patterns=("*.md",)),
    BackupTarget("docs/references", "prereg_refs", patterns=("*.md", "*.csv")),
    BackupTarget("docs/research", "rounds", patterns=("*.md", "*.json")),
    BackupTarget("docs/exec-plans", "exec_plans", patterns=("*.md",)),
    # 가격 parquet
    BackupTarget("paper/prices/ohlcv_paper.parquet", "prices_paper"),
    BackupTarget("krx_daily_archive", "prices_krx", patterns=("*.parquet",)),
]


def _now() -> datetime:
    return datetime.now()


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def _file_info(path: Path) -> Dict[str, object]:
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes": int(stat.st_size),
        "mtime_local": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        "sha256": _sha256(path),
    }


def _file_info_light(path: Path) -> Dict[str, object]:
    """해시 없이 크기·시각만.

    [2026-09-08] 대상이 7개에서 수천 개(가격 parquet 4.7GB 포함)로 늘었다.
      매일 도는 DRY 에서 전부 해시하면 USB 외장 HDD 를 몇 분씩 읽는다.
      정책(§14)이 SHA256 을 요구하는 것은 **2차 백업 결과** 이므로 APPLY 에서만 해시한다.
    """
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes": int(stat.st_size),
        "mtime_local": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        "sha256": None,
    }


def _ensure_inside_dest(path: Path, dest_root: Path) -> None:
    resolved = path.resolve()
    root = dest_root.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError(f"destination outside allowed root: {path}")


def _expand(target: BackupTarget) -> List[Path]:
    """대상 하나를 실제 파일 목록으로 편다.

    [2026-09-08] 파일 단건만 되던 것을 디렉터리까지 넓혔다. 넓히는 순간 자격증명이
      섞일 위험이 생기므로 _denied() 를 **모든 경로에** 적용한다. 예외 없다.
    """
    src = ROOT / target.rel_path
    if src.is_file():
        return [] if _denied(src) else [src]
    if not src.is_dir():
        return []
    pats = target.patterns or ("*",)
    out: List[Path] = []
    for pat in pats:
        it = src.rglob(pat) if target.recursive else src.glob(pat)
        for p in it:
            if p.is_file() and not _denied(p):
                out.append(p)
    return sorted(set(out))


def _build_plan(dest_root: Path, stamp: str, hash_sources: bool = True) -> List[Dict[str, object]]:
    run_root = dest_root / stamp
    _ensure_inside_dest(run_root, dest_root)
    rows: List[Dict[str, object]] = []
    for target in TARGETS:
        src_root = ROOT / target.rel_path
        files = _expand(target)
        if not files:
            # 단건 대상이 없으면 MISSING 으로 남긴다(기존 동작). 디렉터리가 비어도 마찬가지.
            rows.append({
                "label": target.label, "rel_path": target.rel_path,
                "source": str(src_root), "destination": str(run_root / target.rel_path),
                "status": "MISSING",
            })
            continue
        for f in files:
            rel = f.relative_to(ROOT).as_posix()
            row: Dict[str, object] = {
                "label": target.label,
                "rel_path": rel,
                "source": str(f),
                "destination": str(run_root / rel),
                "status": "OK",
            }
            row["source_info"] = _file_info(f) if hash_sources else _file_info_light(f)
            rows.append(row)
    return rows


def _apply_plan(rows: List[Dict[str, object]], dest_root: Path) -> List[Dict[str, object]]:
    copied: List[Dict[str, object]] = []
    for row in rows:
        if row.get("status") != "OK":
            continue
        src = Path(str(row["source"]))
        dst = Path(str(row["destination"]))
        _ensure_inside_dest(dst, dest_root)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied_row = dict(row)
        copied_row["source_info"] = _file_info(src)  # APPLY 에서는 원본도 해시한다
        copied_row["backup_info"] = _file_info(dst)
        copied_row["hash_match"] = copied_row["source_info"]["sha256"] == copied_row["backup_info"]["sha256"]
        copied_row["size_match"] = copied_row["source_info"]["size_bytes"] == copied_row["backup_info"]["size_bytes"]
        copied.append(copied_row)
    return copied


def _write_report(report: Dict[str, object], stamp: str) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / f"offsite_backup_manifest_{stamp}.json"
    latest = LOG_DIR / "offsite_backup_manifest_latest.json"
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    path.write_text(payload, encoding="utf-8")
    latest.write_text(payload, encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dest-root", default=str(DEST_ROOT))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-d-drive", action="store_true")
    args = parser.parse_args()

    dest_root = Path(args.dest_root)
    if dest_root.resolve() != DEST_ROOT.resolve():
        raise SystemExit(f"[FAILED] dest root is not allowed: {dest_root}")

    apply_mode = bool(args.apply)
    if apply_mode and not args.confirm_d_drive:
        raise SystemExit("[FAILED] --apply requires --confirm-d-drive")

    stamp = _now().strftime("%Y%m%d_%H%M%S")
    # [2026-09-08] DRY 는 해시를 생략한다(수천 파일 x USB HDD). APPLY 는 양쪽 다 해시한다.
    rows = _build_plan(dest_root, stamp, hash_sources=apply_mode)
    copied = _apply_plan(rows, dest_root) if apply_mode else []
    missing = [row for row in rows if row.get("status") != "OK"]
    copy_failures = [
        row
        for row in copied
        if not bool(row.get("hash_match")) or not bool(row.get("size_match"))
    ]
    report = {
        "generated_at_local": _now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "APPLY" if apply_mode else "DRY",
        "root": str(ROOT),
        "dest_root": str(dest_root),
        "target_count": len(rows),
        "ok_count": len(rows) - len(missing),
        "missing_count": len(missing),
        "copied_count": len(copied),
        "copy_failure_count": len(copy_failures),
        "targets": rows,
        "copied": copied,
        "missing": missing,
    }
    report_path = _write_report(report, stamp)
    print(f"[OFFSITE_BACKUP] mode={report['mode']}")
    print(f"[OFFSITE_BACKUP] report={report_path}")
    print(
        "[OFFSITE_BACKUP] targets={target_count} ok={ok_count} missing={missing_count} copied={copied_count} failures={copy_failure_count}".format(
            **report
        )
    )
    if missing or copy_failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
