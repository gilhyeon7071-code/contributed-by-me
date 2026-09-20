from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
import logging


ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "2_Logs"
DIAG_DIR = ROOT / "_diag"
LOCK_PATH = DIAG_DIR / "temp_cleanup_policy.lock"
ROOTB_RUNS_DIR = Path("E:/vibe/buffett/runs")

BACKUP_RETENTION_DAYS = 90
RUNS_RETENTION_DAYS = 90
TMP_RETENTION_DAYS = 7
DIAG_TMP_RETENTION_DAYS = 30
LOCK_STALE_MINUTES = 60

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
}
CACHE_DIR_NAMES = {"__pycache__", ".pytest_cache", ".mypy_cache"}
LOCK_EXTENSIONS = {".lock", ".lck", ".pid"}
APPLY_ALLOWED_CATEGORIES = {"cache"}


@dataclass
class Candidate:
    path: Path
    category: str
    reason: str
    age_days: int
    kind: str




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _now() -> datetime:
    return datetime.now()


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _age_days(path: Path, now: datetime) -> int:
    return (now - datetime.fromtimestamp(path.stat().st_mtime)).days


def _is_backup_name(name: str) -> bool:
    return name.startswith("backup_")


def _is_date_prefix_backup_name(name: str) -> bool:
    return len(name) >= 9 and name[:8].isdigit() and name[8] == "_"


def _is_backup_retention_candidate(path: Path, root: Path) -> bool:
    if _is_backup_name(path.name):
        return True
    return path.parent == (root / "backup") and _is_date_prefix_backup_name(path.name)


def _is_tmp_name(name: str) -> bool:
    return name.startswith("tmp_") or name.startswith("_tmp_")


def _is_under_backup_tree(path: Path) -> bool:
    return any(part == "backup" or _is_backup_name(part) or _is_legacy_backup_container_name(part) for part in path.parts)


def _is_legacy_backup_container_name(name: str) -> bool:
    return name in {"_backup", "_bak"} or name.startswith("_bulk_backup")


def _is_diag_tmp_exception(path: Path) -> bool:
    try:
        rel = path.relative_to(DIAG_DIR)
    except ValueError:
        return False
    return len(rel.parts) == 1 and path.name.startswith("tmp_")


def _list_candidate_paths(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        yield path


def _has_same_stem_lock(path: Path) -> bool:
    stem = path.stem
    for sibling in path.parent.iterdir():
        if not sibling.is_file() or sibling == path:
            continue
        if sibling.suffix.lower() not in LOCK_EXTENSIONS:
            continue
        if sibling.stem == stem:
            return True
    return False


def _is_latest_tmp_job_log(path: Path, candidates: List[Path]) -> bool:
    if path.parent != LOG_DIR:
        return False
    if not path.name.startswith("tmp_"):
        return False
    if not (path.name.endswith(".out.log") or path.name.endswith(".err.log")):
        return False

    prefix = path.name[:-8]
    group = [p for p in candidates if p.parent == path.parent and p.name.startswith(prefix) and p.suffix.lower() == ".log"]
    if not group:
        return False
    latest = max(group, key=lambda item: item.stat().st_mtime)
    return latest == path


def _text_file_contains_reference(probe: Path, target_name: str) -> bool:
    try:
        if probe.suffix.lower() not in TEXT_EXTENSIONS:
            return False
        if probe.stat().st_size > 2_000_000:
            return False
        content = probe.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False
    return target_name in content


def _is_referenced_in_repo(root: Path, target: Path) -> bool:
    target_name = target.name
    for probe in root.rglob("*"):
        if not probe.is_file() or probe == target:
            continue
        if _is_under_backup_tree(probe):
            continue
        if any(part in CACHE_DIR_NAMES for part in probe.parts):
            continue
        if _is_tmp_name(probe.name):
            continue
        if _text_file_contains_reference(probe, target_name):
            return True
    return False


def _collect_backup_candidates(root: Path, now: datetime) -> List[Candidate]:
    results: List[Candidate] = []
    for path in _list_candidate_paths(root):
        if path == LOCK_PATH:
            continue
        if not _is_backup_retention_candidate(path, root):
            continue
        age_days = _age_days(path, now)
        if age_days >= BACKUP_RETENTION_DAYS:
            results.append(Candidate(path=path, category="backup", reason="backup_retention_expired", age_days=age_days, kind=("dir" if path.is_dir() else "file")))
    return sorted(results, key=lambda item: str(item.path))


def _collect_tmp_candidates(root: Path, now: datetime) -> Tuple[List[Candidate], List[Dict[str, object]]]:
    delete_list: List[Candidate] = []
    keep_list: List[Dict[str, object]] = []

    tmp_paths = [
        path
        for path in _list_candidate_paths(root)
        if path.is_file()
        and _is_tmp_name(path.name)
        and not _is_under_backup_tree(path)
        and not any(part in CACHE_DIR_NAMES for part in path.parts)
    ]

    for path in sorted(tmp_paths, key=lambda item: str(item)):
        age_days = _age_days(path, now)
        if datetime.fromtimestamp(path.stat().st_mtime).date() == now.date():
            keep_list.append({"path": str(path), "reason": "tmp_today_skip", "age_days": age_days})
            continue
        if _is_diag_tmp_exception(path) and age_days < DIAG_TMP_RETENTION_DAYS:
            keep_list.append({"path": str(path), "reason": "diag_tmp_retention", "age_days": age_days})
            continue
        if age_days >= TMP_RETENTION_DAYS:
            delete_list.append(Candidate(path=path, category="tmp", reason="tmp_7d_expired", age_days=age_days, kind="file"))
            continue
        if _has_same_stem_lock(path):
            keep_list.append({"path": str(path), "reason": "same_stem_lock", "age_days": age_days})
            continue
        if _is_latest_tmp_job_log(path, tmp_paths):
            keep_list.append({"path": str(path), "reason": "latest_tmp_job_log", "age_days": age_days})
            continue
        if _is_referenced_in_repo(root, path):
            keep_list.append({"path": str(path), "reason": "repo_reference_found", "age_days": age_days})
            continue
        delete_list.append(Candidate(path=path, category="tmp", reason="tmp_1to7_unreferenced", age_days=age_days, kind="file"))

    return delete_list, keep_list


def _collect_cache_candidates(root: Path, now: datetime) -> List[Candidate]:
    results: List[Candidate] = []
    for path in root.rglob("*"):
        if _is_under_backup_tree(path):
            continue
        if path.name not in CACHE_DIR_NAMES:
            continue
        results.append(Candidate(path=path, category="cache", reason="cache_cleanup", age_days=_age_days(path, now), kind="dir"))
    return sorted(results, key=lambda item: str(item.path))


def _collect_runs_candidates(now: datetime) -> List[Candidate]:
    results: List[Candidate] = []
    if not ROOTB_RUNS_DIR.exists():
        return results
    for path in ROOTB_RUNS_DIR.iterdir():
        if path.name == "tasklocks":
            continue
        age_days = _age_days(path, now)
        if age_days >= RUNS_RETENTION_DAYS:
            results.append(Candidate(path=path, category="runs", reason="runs_retention_expired", age_days=age_days, kind=("dir" if path.is_dir() else "file")))
    return sorted(results, key=lambda item: str(item.path))



# --- ws_ticks 보존 규칙 (2026-08-20) --------------------------------------
# 틱 로그(kis_ws_ticks*.jsonl)는 하루 1.5GB 씩 쌓이는데 어느 카테고리에도
# 걸리지 않아 후보로 잡힌 적이 없었다. 2026-08-20 실측: 100개 39.20 GB.
#
# 나이만으로 지우지 않는다. **대체본이 확인될 때만** 후보가 된다:
#   1) parquet 존재  (data/ws_ticks_parquet/ws_{trade,hoga}_<tag>.parquet)
#   2) D 사본 존재 + 바이트 일치 (D:/1_Data_Offsite_Backup/ws_ticks_raw/)
# 변환이나 복사가 멈추면 후보가 그냥 생기지 않는다 -> 잘못 지우는 일이 구조적으로 불가능하다.
#
# 보존 90일은 BACKUP/RUNS 와 동일 값이다. 새 임계를 만들지 않는다.
WS_TICKS_RETENTION_DAYS = 90
WS_TICKS_GLOB = "kis_ws_ticks*.jsonl"
WS_TICKS_PARQUET_DIR = ROOT / "data" / "ws_ticks_parquet"
WS_TICKS_OFFSITE_DIR = Path("D:/1_Data_Offsite_Backup/ws_ticks_raw")
# 알림 임계: 파일이 하루 1개 단위로 생기므로 파일 수 = 밀린 일수.
# 7 은 TMP_RETENTION_DAYS 와 같은 값을 쓴다(새 숫자를 만들지 않음).
WS_TICKS_PENDING_WARN = 3
WS_TICKS_PENDING_ALERT = TMP_RETENTION_DAYS


def _ws_ticks_tag(path: Path) -> str:
    stem = path.stem
    return stem.replace("kis_ws_ticks", "").strip("_")


def _ws_ticks_has_parquet(tag: str) -> bool:
    if not tag or not WS_TICKS_PARQUET_DIR.exists():
        return False
    for kind in ("trade", "hoga"):
        if (WS_TICKS_PARQUET_DIR / f"ws_{kind}_{tag}.parquet").exists():
            return True
    return False


def _ws_ticks_offsite_ok(path: Path) -> bool:
    try:
        dst = WS_TICKS_OFFSITE_DIR / path.name
        return dst.exists() and dst.stat().st_size == path.stat().st_size
    except OSError:
        return False


def _collect_ws_ticks(now: datetime) -> Tuple[List[Candidate], List[Dict[str, object]]]:
    """(삭제 후보, 아카이브 대기) 를 돌려준다.

    대기 = 보존기간을 넘겼는데 parquet 또는 D 사본이 아직 없는 것.
    이것이 쌓이면 알림 대상이다.
    """
    ready: List[Candidate] = []
    pending: List[Dict[str, object]] = []
    if not LOG_DIR.exists():
        return ready, pending
    for path in sorted(LOG_DIR.glob(WS_TICKS_GLOB)):
        if not path.is_file():
            continue
        age_days = _age_days(path, now)
        if age_days < WS_TICKS_RETENTION_DAYS:
            continue
        tag = _ws_ticks_tag(path)
        has_pq = _ws_ticks_has_parquet(tag)
        has_off = _ws_ticks_offsite_ok(path)
        if has_pq and has_off:
            ready.append(Candidate(path=path, category="ws_ticks",
                                   reason="ws_ticks_retention_expired_archived",
                                   age_days=age_days, kind="file"))
        else:
            miss = []
            if not has_pq:
                miss.append("parquet")
            if not has_off:
                miss.append("offsite")
            try:
                size = int(path.stat().st_size)
            except OSError:
                size = 0
            pending.append({"path": str(path), "age_days": age_days,
                            "size_bytes": size, "missing": miss})
    return ready, pending


def _ws_ticks_alert(pending: List[Dict[str, object]]) -> Dict[str, object]:
    n = len(pending)
    level = "OK"
    if n > WS_TICKS_PENDING_ALERT:
        level = "ALERT"
    elif n >= WS_TICKS_PENDING_WARN:
        level = "WARN"
    return {
        "level": level,
        "pending_count": n,
        "pending_bytes": int(sum(int(p.get("size_bytes") or 0) for p in pending)),
        "warn_at": WS_TICKS_PENDING_WARN,
        "alert_at_over": WS_TICKS_PENDING_ALERT,
        "retention_days": WS_TICKS_RETENTION_DAYS,
        "parquet_dir": str(WS_TICKS_PARQUET_DIR),
        "offsite_dir": str(WS_TICKS_OFFSITE_DIR),
        "note": ("아카이브 대기가 밀렸다. parquet 변환과 D 복사를 실행할 것."
                 if level != "OK" else ""),
    }
# --- ws_ticks 끝 -----------------------------------------------------------



def _ws_ticks_notify(alert: Dict[str, object], dry_run: bool) -> Dict[str, object]:
    """정비 알림을 직접 발송한다.

    2026-08-20: 배치 알림 경로(`notify_channels.send_alert`)는 텔레그램 API 를 직접
    호출하므로 대시보드의 수신 이벤트 체크박스를 거치지 않는다.
    이 도구는 `telegram_notifier.py --event maintenance` 를 써서
    사용자가 화면에서 켜고 끌 수 있는 채널로 보낸다.

    중간 단계를 두지 않는 이유: 이 도구가 안 돌면 알림도 안 온다.
    "정리 도구가 죽었는데 알림만 계속 오는" 상태를 만들지 않는다.

    같은 레벨을 하루에 여러 번 보내지 않도록 상태 파일로 중복을 막는다.
    """
    out = {"attempted": False, "sent": False, "reason": ""}
    level = str(alert.get("level") or "OK").upper()
    if level == "OK":
        out["reason"] = "level_ok"
        return out
    if dry_run is False:
        pass  # APPLY 여부와 무관하게 알림은 보낸다
    stamp_path = DIAG_DIR / "ws_ticks_notify_last.json"
    today = _now().strftime("%Y%m%d")
    try:
        if stamp_path.exists():
            prev = json.loads(stamp_path.read_text(encoding="utf-8"))
            if str(prev.get("ymd")) == today and str(prev.get("level")) == level:
                out["reason"] = "already_notified_today"
                return out
    except Exception:
        pass

    notifier = ROOT / "tools" / "telegram_notifier.py"
    python_exe = ROOT / "_runtime" / "python312-embed" / "python.exe"
    if not notifier.exists() or not python_exe.exists():
        out["reason"] = "notifier_or_python_missing"
        return out

    gb = float(alert.get("pending_bytes") or 0) / 1e9
    msg = (
        "[운영 정비] 틱 로그 아카이브 대기\n"
        f"레벨: {level}\n"
        f"대기: {alert.get('pending_count')}개 / {gb:.1f}GB\n"
        f"기준: 보존 {alert.get('retention_days')}일 초과 + parquet 또는 D사본 없음\n"
        f"임계: 경고 {alert.get('warn_at')}개 / 알림 {alert.get('alert_at_over')}개 초과\n"
        "조치: parquet 변환 후 D:/1_Data_Offsite_Backup/ws_ticks_raw 복사"
    )
    out["attempted"] = True
    try:
        r = subprocess.run(
            [str(python_exe), str(notifier), "--event", "maintenance", "--msg", msg],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30,
        )
        ok = (r.returncode == 0) and ("Sent successfully" in (r.stdout or ""))
        out["sent"] = bool(ok)
        out["reason"] = (r.stdout or r.stderr or "").strip()[-120:]
        if ok:
            _ensure_dir(DIAG_DIR)
            stamp_path.write_text(json.dumps({"ymd": today, "level": level},
                                             ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        out["reason"] = f"{type(exc).__name__}: {exc}"
    return out


def _candidate_to_dict(item: Candidate) -> Dict[str, object]:
    return {
        "path": str(item.path),
        "category": item.category,
        "reason": item.reason,
        "age_days": item.age_days,
        "kind": item.kind,
    }


def _apply_block_reason(item: Candidate) -> str | None:
    if item.category in APPLY_ALLOWED_CATEGORIES:
        return None
    if item.category == "tmp" and item.path.parent == LOG_DIR and item.path.name.startswith("_tmp"):
        return None
    return "apply_scope_guard"


def _delete_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def _write_status(report: Dict[str, object], stamp: str) -> Tuple[Path, Path]:
    _ensure_dir(LOG_DIR)
    dated = LOG_DIR / f"temp_cleanup_status_{stamp}.json"
    latest = LOG_DIR / "temp_cleanup_status_latest.json"
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    dated.write_text(payload, encoding="utf-8")
    latest.write_text(payload, encoding="utf-8")
    return dated, latest


def _write_lock() -> None:
    _ensure_dir(DIAG_DIR)
    LOCK_PATH.write_text(
        json.dumps({"pid": os.getpid(), "started_at": _now().isoformat()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _remove_lock() -> None:
    try:
        if LOCK_PATH.exists():
            LOCK_PATH.unlink()
    except Exception:
        pass


def _is_stale_lock(path: Path, now: datetime) -> bool:
    try:
        age_minutes = (now - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds() / 60.0
    except Exception:
        return False
    return age_minutes >= LOCK_STALE_MINUTES


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-list", type=int, default=200)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    dry_run = not args.apply or bool(args.dry_run)
    now = _now()
    stamp = now.strftime("%Y%m%d_%H%M%S")

    if LOCK_PATH.exists():
        if _is_stale_lock(LOCK_PATH, now):
            _log_print(f"[TEMP_CLEANUP] stale lock removed: {LOCK_PATH}")
            _remove_lock()
        else:
            _log_print(f"[TEMP_CLEANUP] skip lock_exists: {LOCK_PATH}")
            return 3

    _write_lock()
    try:
        backup_candidates = _collect_backup_candidates(root, now)
        tmp_candidates, tmp_keeps = _collect_tmp_candidates(root, now)
        cache_candidates = _collect_cache_candidates(root, now)
        runs_candidates = _collect_runs_candidates(now)
        ws_ticks_candidates, ws_ticks_pending = _collect_ws_ticks(now)
        ws_ticks_alert = _ws_ticks_alert(ws_ticks_pending)
        ws_ticks_notify = _ws_ticks_notify(ws_ticks_alert, dry_run)
        all_candidates = (backup_candidates + tmp_candidates + cache_candidates
                          + runs_candidates + ws_ticks_candidates)
        apply_candidates = [item for item in all_candidates if _apply_block_reason(item) is None]
        apply_blocked = [
            {**_candidate_to_dict(item), "block_reason": str(_apply_block_reason(item))}
            for item in all_candidates
            if _apply_block_reason(item) is not None
        ]

        delete_errors: List[Dict[str, str]] = []
        deleted: List[Dict[str, object]] = []

        _log_print(f"[TEMP_CLEANUP] mode={'DRY' if dry_run else 'APPLY'}")
        _log_print(f"[TEMP_CLEANUP] delete_candidates={len(all_candidates)}")
        for item in all_candidates[: max(0, int(args.max_list))]:
            _log_print(f"[TEMP_CLEANUP] delete {item.category} {item.reason} {item.path}")

        if not dry_run:
            for item in apply_candidates:
                try:
                    _delete_path(item.path)
                    deleted.append(_candidate_to_dict(item))
                except Exception as exc:
                    delete_errors.append({"path": str(item.path), "error": f"{type(exc).__name__}: {exc}"})

        report = {
            "generated_at_local": now.strftime("%Y-%m-%d %H:%M:%S"),
            "root": str(root),
            "mode": "DRY" if dry_run else "APPLY",
            "policy": {
                "backup_retention_days": BACKUP_RETENTION_DAYS,
                "runs_retention_days": RUNS_RETENTION_DAYS,
                "tmp_retention_days": TMP_RETENTION_DAYS,
                "diag_tmp_retention_days": DIAG_TMP_RETENTION_DAYS,
                "lock_stale_minutes": LOCK_STALE_MINUTES,
                "cache_dir_names": sorted(CACHE_DIR_NAMES),
                "apply_allowed_categories": sorted(APPLY_ALLOWED_CATEGORIES),
                "apply_allowed_tmp_scope": str(LOG_DIR / "_tmp*"),
                "runs_dir": str(ROOTB_RUNS_DIR),
            },
            "summary": {
                "backup_delete_count": len(backup_candidates),
                "tmp_delete_count": len(tmp_candidates),
                "cache_delete_count": len(cache_candidates),
                "runs_delete_count": len(runs_candidates),
                "ws_ticks_delete_count": len(ws_ticks_candidates),
                "ws_ticks_pending_count": len(ws_ticks_pending),
                "ws_ticks_alert_level": str(ws_ticks_alert.get("level")),
                "delete_total_count": len(all_candidates),
                "apply_candidate_count": len(apply_candidates),
                "apply_blocked_count": len(apply_blocked),
                "tmp_keep_count": len(tmp_keeps),
                "delete_error_count": len(delete_errors),
            },
            "ws_ticks": {**ws_ticks_alert, "notify": ws_ticks_notify},
            "ws_ticks_pending": ws_ticks_pending[: max(0, int(args.max_list))],
            "delete_candidates": [_candidate_to_dict(item) for item in all_candidates[: max(0, int(args.max_list))]],
            "apply_candidates": [_candidate_to_dict(item) for item in apply_candidates[: max(0, int(args.max_list))]],
            "apply_blocked": apply_blocked[: max(0, int(args.max_list))],
            "tmp_keep_reasons": tmp_keeps[: max(0, int(args.max_list))],
            "deleted": deleted[: max(0, int(args.max_list))],
            "delete_errors": delete_errors[: max(0, int(args.max_list))],
        }
        dated, latest = _write_status(report, stamp)

        _log_print(f"[TEMP_CLEANUP] report={dated}")
        _log_print(f"[TEMP_CLEANUP] latest={latest}")
        _log_print(
            "[TEMP_CLEANUP] backup={backup} tmp={tmp} cache={cache} runs={runs} keep={keep} errors={errors}".format(
                backup=len(backup_candidates),
                tmp=len(tmp_candidates),
                cache=len(cache_candidates),
                runs=len(runs_candidates),
                keep=len(tmp_keeps),
                errors=len(delete_errors),
            )
        )
        return 0 if not delete_errors else 1
    finally:
        _remove_lock()


if __name__ == "__main__":
    raise SystemExit(main())


