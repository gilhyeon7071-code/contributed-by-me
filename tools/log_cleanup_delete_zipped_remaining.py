from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent))
from log_cleanup_30d import _collect_candidates, _now_local


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
ROOTB_RUNS = ROOT.parent / "vibe" / "buffett" / "runs"
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


def _target_allowed(target: Path) -> bool:
    return _inside(target, LOG_DIR) or _inside(target, ROOTB_RUNS)


def _load_zip_manifest(zip_path: Path) -> Dict[str, Dict[str, object]]:
    if not _inside(zip_path, DEST_ROOT):
        raise SystemExit(f"[FAILED] zip outside allowed root: {zip_path}")
    with zipfile.ZipFile(zip_path, "r", allowZip64=True) as zf:
        if zf.testzip() is not None:
            raise SystemExit("[FAILED] zip integrity check failed")
        manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
    rows = {}
    for row in manifest.get("sources", []):
        rel = str(row.get("rel_path") or "").replace("\\", "/")
        if rel:
            rows[rel] = row
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Delete cleanup candidates already verified in a quarantine zip.")
    ap.add_argument("--target", default=str(LOG_DIR))
    ap.add_argument("--retention-days", type=int, default=30)
    ap.add_argument("--include-ext", default=".json,.csv,.txt,.log")
    ap.add_argument("--zip-path", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-list", type=int, default=50)
    args = ap.parse_args()

    target = Path(args.target)
    zip_path = Path(args.zip_path)
    if not _target_allowed(target):
        raise SystemExit(f"[FAILED] target outside allowed cleanup roots: {target}")
    manifest_by_rel = _load_zip_manifest(zip_path)

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

    verified: List[Dict[str, object]] = []
    skipped: List[Dict[str, object]] = []
    deleted: List[Dict[str, object]] = []
    errors: List[Dict[str, object]] = []

    for row in candidates:
        src = Path(str(row.get("path") or ""))
        if not _inside(src, target) or not src.is_file():
            skipped.append({"source": str(src), "status": "SOURCE_INVALID"})
            continue
        rel = str(src.relative_to(target)).replace("\\", "/")
        manifest_row = manifest_by_rel.get(rel)
        if not manifest_row:
            skipped.append({"source": str(src), "rel_path": rel, "status": "NOT_IN_ZIP_MANIFEST"})
            continue
        size = int(src.stat().st_size)
        digest = _sha256(src)
        if size != int(manifest_row.get("size_bytes", -1)) or digest != str(manifest_row.get("sha256", "")):
            skipped.append({"source": str(src), "rel_path": rel, "status": "SOURCE_MISMATCH"})
            continue
        verified.append({"source": str(src), "rel_path": rel, "size_bytes": size, "sha256": digest})

    if args.apply:
        for row in verified:
            src = Path(str(row["source"]))
            try:
                os.chmod(src, stat.S_IWRITE)
                src.unlink()
                deleted.append(row)
            except Exception as exc:
                err = dict(row)
                err["status"] = type(exc).__name__
                errors.append(err)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report = {
        "generated_at_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "APPLY" if args.apply else "DRY",
        "target": str(target),
        "zip_path": str(zip_path),
        "retention_days": int(args.retention_days),
        "cutoff_local": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "summary": collect,
        "candidate_count": len(candidates),
        "zip_manifest_count": len(manifest_by_rel),
        "verified_count": len(verified),
        "skipped_count": len(skipped),
        "deleted_count": len(deleted),
        "delete_error_count": len(errors),
        "verified_samples": verified[: int(args.max_list)],
        "skipped_samples": skipped[: int(args.max_list)],
        "delete_error_samples": errors[: int(args.max_list)],
    }
    report_path = LOG_DIR / f"log_cleanup_delete_zipped_remaining_{stamp}.json"
    latest_path = LOG_DIR / "log_cleanup_delete_zipped_remaining_latest.json"
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    report_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")

    print(f"[LOG_CLEANUP_DELETE_ZIPPED] mode={report['mode']}")
    print(f"[LOG_CLEANUP_DELETE_ZIPPED] report={report_path}")
    print(
        "[LOG_CLEANUP_DELETE_ZIPPED] candidates={candidate_count} verified={verified_count} deleted={deleted_count} skipped={skipped_count} errors={delete_error_count}".format(
            **report
        )
    )
    if skipped or errors:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
