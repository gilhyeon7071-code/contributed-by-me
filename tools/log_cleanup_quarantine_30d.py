from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent))
from log_cleanup_30d import _collect_candidates, _now_local


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
DEST_ROOT = Path("D:/1_Data_Offsite_Backup")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def _inside(path: Path, root: Path) -> bool:
    try:
        resolved = path.resolve()
        root_resolved = root.resolve()
    except Exception:
        resolved = path.absolute()
        root_resolved = root.absolute()
    return resolved == root_resolved or root_resolved in resolved.parents


def _copy_then_remove(src: Path, dst: Path) -> Dict[str, object]:
    src_size = int(src.stat().st_size)
    src_hash = _sha256(src)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    dst_size = int(dst.stat().st_size)
    dst_hash = _sha256(dst)
    size_match = src_size == dst_size
    hash_match = src_hash == dst_hash
    if not size_match or not hash_match:
        return {
            "source": str(src),
            "destination": str(dst),
            "status": "COPY_VERIFY_FAIL",
            "source_size": src_size,
            "destination_size": dst_size,
            "source_sha256": src_hash,
            "destination_sha256": dst_hash,
            "size_match": size_match,
            "hash_match": hash_match,
        }
    src.unlink()
    return {
        "source": str(src),
        "destination": str(dst),
        "status": "MOVED",
        "source_size": src_size,
        "destination_size": dst_size,
        "source_sha256": src_hash,
        "destination_sha256": dst_hash,
        "size_match": True,
        "hash_match": True,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Quarantine log cleanup candidates to approved offsite backup root.")
    ap.add_argument("--target", default=str(LOG_DIR))
    ap.add_argument("--retention-days", type=int, default=30)
    ap.add_argument("--include-ext", default=".json,.csv,.txt,.log")
    ap.add_argument("--dest-root", default=str(DEST_ROOT))
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--confirm-d-drive", action="store_true")
    ap.add_argument("--max-apply", type=int, default=0, help="0 means no limit")
    ap.add_argument("--max-list", type=int, default=50)
    args = ap.parse_args()

    target = Path(args.target)
    dest_root = Path(args.dest_root)
    if dest_root.resolve() != DEST_ROOT.resolve():
        raise SystemExit(f"[FAILED] dest root is not allowed: {dest_root}")
    if args.apply and not args.confirm_d_drive:
        raise SystemExit("[FAILED] --apply requires --confirm-d-drive")
    if not _inside(target, LOG_DIR):
        raise SystemExit(f"[FAILED] target outside log dir: {target}")

    now = _now_local()
    cutoff = now - timedelta(days=int(args.retention_days))
    include_ext = {"." + x.strip().lower().lstrip(".") for x in str(args.include_ext).split(",") if x.strip()}
    candidates, collect = _collect_candidates(
        target=target,
        cutoff=cutoff,
        include_ext=include_ext,
        max_list=max(0, int(args.max_list)),
        bak_only=False,
    )

    stamp = now.strftime("%Y%m%d_%H%M%S")
    run_root = dest_root / "log_cleanup_quarantine" / stamp
    if not _inside(run_root, dest_root):
        raise SystemExit(f"[FAILED] destination outside allowed root: {run_root}")

    moved: List[Dict[str, object]] = []
    failures: List[Dict[str, object]] = []
    apply_candidates = candidates
    if int(args.max_apply) > 0:
        apply_candidates = candidates[: int(args.max_apply)]

    if args.apply:
        for row in apply_candidates:
            src = Path(str(row.get("path") or ""))
            if not _inside(src, target) or not src.is_file():
                failures.append({"source": str(src), "status": "SOURCE_INVALID"})
                continue
            rel = src.relative_to(target)
            dst = run_root / "2_Logs" / rel
            try:
                result = _copy_then_remove(src, dst)
                if result.get("status") == "MOVED":
                    moved.append(result)
                else:
                    failures.append(result)
            except Exception as exc:
                failures.append({"source": str(src), "destination": str(dst), "status": type(exc).__name__})

    report = {
        "generated_at_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "APPLY" if args.apply else "DRY",
        "target": str(target),
        "dest_root": str(dest_root),
        "run_root": str(run_root),
        "retention_days": int(args.retention_days),
        "cutoff_local": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "include_ext": sorted(include_ext),
        "summary": collect,
        "candidate_count": len(candidates),
        "apply_candidate_count": len(apply_candidates) if args.apply else 0,
        "moved_count": len(moved),
        "failure_count": len(failures),
        "candidate_samples": collect.get("samples", []),
        "moved_samples": moved[: int(args.max_list)],
        "failure_samples": failures[: int(args.max_list)],
    }
    report_path = LOG_DIR / f"log_cleanup_quarantine_{stamp}.json"
    latest_path = LOG_DIR / "log_cleanup_quarantine_latest.json"
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    report_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")

    print(f"[LOG_CLEANUP_QUARANTINE] mode={report['mode']}")
    print(f"[LOG_CLEANUP_QUARANTINE] report={report_path}")
    print(
        "[LOG_CLEANUP_QUARANTINE] candidates={candidate_count} moved={moved_count} failures={failure_count}".format(
            **report
        )
    )
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
