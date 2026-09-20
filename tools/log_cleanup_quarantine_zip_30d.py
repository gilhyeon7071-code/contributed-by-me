from __future__ import annotations

import argparse
import hashlib
import json
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


def _target_label(target: Path) -> str:
    if _inside(target, LOG_DIR):
        return "rootA_2_Logs"
    if _inside(target, ROOTB_RUNS):
        return "rootB_runs"
    raise SystemExit(f"[FAILED] target outside allowed cleanup roots: {target}")


def _source_row(src: Path, target: Path) -> Dict[str, object]:
    st = src.stat()
    return {
        "source": str(src),
        "rel_path": str(src.relative_to(target)).replace("\\", "/"),
        "size_bytes": int(st.st_size),
        "mtime_local": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        "sha256": _sha256(src),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Zip remaining log cleanup candidates to approved offsite backup root.")
    ap.add_argument("--target", default=str(LOG_DIR))
    ap.add_argument("--retention-days", type=int, default=30)
    ap.add_argument("--include-ext", default=".json,.csv,.txt,.log")
    ap.add_argument("--dest-root", default=str(DEST_ROOT))
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--confirm-d-drive", action="store_true")
    ap.add_argument("--max-list", type=int, default=50)
    args = ap.parse_args()

    target = Path(args.target)
    dest_root = Path(args.dest_root)
    if dest_root.resolve() != DEST_ROOT.resolve():
        raise SystemExit(f"[FAILED] dest root is not allowed: {dest_root}")
    if args.apply and not args.confirm_d_drive:
        raise SystemExit("[FAILED] --apply requires --confirm-d-drive")
    target_label = _target_label(target)

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
    run_root = dest_root / "log_cleanup_quarantine_zip" / target_label / stamp
    if not _inside(run_root, dest_root):
        raise SystemExit(f"[FAILED] destination outside allowed root: {run_root}")

    manifest_rows: List[Dict[str, object]] = []
    skipped: List[Dict[str, object]] = []
    delete_errors: List[Dict[str, object]] = []
    zip_path = run_root / "2_Logs_cleanup_candidates.zip"
    zip_sha256 = None
    zip_size = 0
    zip_test_ok = False
    deleted_count = 0
    deleted_bytes = 0

    if args.apply:
        run_root.mkdir(parents=True, exist_ok=True)
        for row in candidates:
            src = Path(str(row.get("path") or ""))
            if not _inside(src, target) or not src.is_file():
                skipped.append({"source": str(src), "status": "SOURCE_INVALID"})
                continue
            manifest_rows.append(_source_row(src, target))

        manifest_payload = {
            "generated_at_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "target": str(target),
            "retention_days": int(args.retention_days),
            "cutoff_local": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
            "candidate_count": len(manifest_rows),
            "source_total_bytes": sum(int(r["size_bytes"]) for r in manifest_rows),
            "sources": manifest_rows,
        }
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as zf:
            zf.writestr("manifest.json", json.dumps(manifest_payload, ensure_ascii=False, indent=2))
            for row in manifest_rows:
                src = Path(str(row["source"]))
                zf.write(src, target_label + "/" + str(row["rel_path"]))

        with zipfile.ZipFile(zip_path, "r", allowZip64=True) as zf:
            bad_member = zf.testzip()
        zip_test_ok = bad_member is None
        zip_size = int(zip_path.stat().st_size)
        zip_sha256 = _sha256(zip_path)

        if zip_test_ok and len(skipped) == 0:
            for row in manifest_rows:
                src = Path(str(row["source"]))
                try:
                    size = int(src.stat().st_size)
                    src.unlink()
                    deleted_count += 1
                    deleted_bytes += size
                except Exception as exc:
                    delete_errors.append({"source": str(src), "status": type(exc).__name__})

    report = {
        "generated_at_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "APPLY" if args.apply else "DRY",
        "target": str(target),
        "target_label": target_label,
        "dest_root": str(dest_root),
        "run_root": str(run_root),
        "zip_path": str(zip_path),
        "zip_size_bytes": zip_size,
        "zip_sha256": zip_sha256,
        "zip_test_ok": zip_test_ok,
        "retention_days": int(args.retention_days),
        "cutoff_local": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "include_ext": sorted(include_ext),
        "summary": collect,
        "candidate_count": len(candidates),
        "manifest_count": len(manifest_rows),
        "skipped_count": len(skipped),
        "deleted_count": deleted_count,
        "deleted_bytes": deleted_bytes,
        "delete_error_count": len(delete_errors),
        "candidate_samples": collect.get("samples", []),
        "skipped_samples": skipped[: int(args.max_list)],
        "delete_error_samples": delete_errors[: int(args.max_list)],
    }
    report_path = LOG_DIR / f"log_cleanup_quarantine_zip_{stamp}.json"
    latest_path = LOG_DIR / "log_cleanup_quarantine_zip_latest.json"
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    report_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")

    print(f"[LOG_CLEANUP_QUARANTINE_ZIP] mode={report['mode']}")
    print(f"[LOG_CLEANUP_QUARANTINE_ZIP] report={report_path}")
    print(
        "[LOG_CLEANUP_QUARANTINE_ZIP] candidates={candidate_count} manifest={manifest_count} deleted={deleted_count} delete_errors={delete_error_count} zip_test_ok={zip_test_ok}".format(
            **report
        )
    )
    if skipped or delete_errors or (args.apply and not zip_test_ok):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
