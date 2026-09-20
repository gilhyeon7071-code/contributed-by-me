from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(r"E:\1_Data")
DEFAULT_EVAL = ROOT / "2_Logs" / "sensitivity_guard_latest.json"
DEFAULT_OUT_ROOT = ROOT / "runs" / "evidence"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _candidate_files(eval_json: Path) -> List[Path]:
    return [
        eval_json,
        ROOT / "2_Logs" / "orderflow_observer_state_latest.json",
        ROOT / "2_Logs" / "canonical_fills_shadow_latest.json",
        ROOT / "2_Logs" / "fix_reject_pattern_last.json",
        ROOT / "2_Logs" / "production_risk_playbook_latest.json",
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description="Build sensitivity evidence bundle zip + hash manifest.")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--eval-json", default=str(DEFAULT_EVAL))
    ap.add_argument("--out-root", default=str(DEFAULT_OUT_ROOT))
    args = ap.parse_args()

    run_id = args.run_id.strip()
    eval_json = Path(args.eval_json)
    out_root = Path(args.out_root)
    out_dir = out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    files = _candidate_files(eval_json)
    included: List[Dict[str, Any]] = []
    missing: List[str] = []
    for fp in files:
        if fp.exists():
            included.append(
                {
                    "path": str(fp),
                    "size": fp.stat().st_size,
                    "sha256": _sha256(fp),
                }
            )
        else:
            missing.append(str(fp))

    manifest = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "run_id": run_id,
        "bundle_type": "sensitivity_guard",
        "included_files": included,
        "missing_files": missing,
        "signed": False,
        "note": "hash manifest only (no cryptographic signature key configured)",
    }
    manifest_path = out_dir / "manifest.json"
    _write_json(manifest_path, manifest)

    bundle_path = out_dir / "bundle.zip"
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(manifest_path, arcname="manifest.json")
        for item in included:
            src = Path(item["path"])
            arc = f"inputs/{src.name}"
            zf.write(src, arcname=arc)

    out = {
        "run_id": run_id,
        "bundle_zip": str(bundle_path),
        "manifest": str(manifest_path),
        "included_n": len(included),
        "missing_n": len(missing),
    }
    latest_path = ROOT / "2_Logs" / "sensitivity_evidence_bundle_latest.json"
    stamped_path = ROOT / "2_Logs" / f"sensitivity_evidence_bundle_{run_id}.json"
    _write_json(latest_path, out)
    _write_json(stamped_path, out)

    print(f"[SENSITIVITY_BUNDLE] bundle_zip={bundle_path}")
    print(f"[SENSITIVITY_BUNDLE] manifest={manifest_path}")
    print(f"[SENSITIVITY_BUNDLE] included_n={len(included)} missing_n={len(missing)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

