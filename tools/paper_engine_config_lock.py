from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple


ROOT = Path(r"E:\1_Data")
PAPER_DIR = ROOT / "paper"
CFG_PATH = PAPER_DIR / "paper_engine_config.json"
LOCK_PATH = PAPER_DIR / "paper_engine_config.lock.json"
LOG_DIR = ROOT / "2_Logs"


def _ts() -> str:
    return _dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(p: Path) -> str:
    return _sha256_bytes(p.read_bytes())


def load_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def dump_json(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def atomic_write_text(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + f".tmp_{_ts()}")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, p)


def atomic_write_json(p: Path, obj: Any) -> None:
    text = json.dumps(obj, ensure_ascii=False, indent=2) + "\n"
    atomic_write_text(p, text)


def set_by_dotted_key(obj: Dict[str, Any], dotted: str, value: Any) -> None:
    keys = dotted.split(".")
    cur: Any = obj
    for k in keys[:-1]:
        if k not in cur or not isinstance(cur[k], dict):
            cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = value


def parse_kv(s: str) -> Tuple[str, Any]:
    # format: key=value ; value tries json parse else string
    if "=" not in s:
        raise ValueError(f"Invalid --set '{s}'. Expected key=value")
    k, vraw = s.split("=", 1)
    k = k.strip()
    vraw = vraw.strip()
    try:
        v = json.loads(vraw)
    except Exception:
        # treat as string (so set mode=REDUCE works without quotes)
        v = vraw
    return k, v


def write_change_log(action: str, before_sha: str, after_sha: str, patch: Dict[str, Any], before_cfg: Dict[str, Any], after_cfg: Dict[str, Any]) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    p = LOG_DIR / f"paper_engine_config.change_{_ts()}.json"
    rec = {
        "ts": _ts(),
        "action": action,
        "config_path": str(CFG_PATH),
        "lock_path": str(LOCK_PATH),
        "before_sha256": before_sha,
        "after_sha256": after_sha,
        "patch_applied": patch,
        # keep full configs for audit/repro (small file ~1KB)
        "before_config": before_cfg,
        "after_config": after_cfg,
    }
    atomic_write_json(p, rec)
    return p


def cmd_init(args: argparse.Namespace) -> int:
    if not CFG_PATH.exists():
        print(f"[FAIL] config missing: {CFG_PATH}")
        return 2

    cur_sha = sha256_file(CFG_PATH)
    lock = {
        "ts": _ts(),
        "approved_sha256": cur_sha,
        "approved_config_path": str(CFG_PATH),
        "last_change_log": None,
        "note": "Initialized approval hash. Subsequent runs will require approved_sha256 match.",
    }
    atomic_write_json(LOCK_PATH, lock)
    print(f"[OK] INIT lock written: {LOCK_PATH}")
    print(f"[OK] approved_sha256={cur_sha}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    if not CFG_PATH.exists():
        print(f"[FAIL] config missing: {CFG_PATH}")
        return 2
    cur_sha = sha256_file(CFG_PATH)
    print(f"[CFG] {CFG_PATH}")
    print(f"[CFG_SHA256] {cur_sha}")

    if not LOCK_PATH.exists():
        print(f"[LOCK] missing: {LOCK_PATH}")
        return 0

    lock = load_json(LOCK_PATH)
    ap = str(lock.get("approved_sha256") or "")
    print(f"[LOCK] {LOCK_PATH}")
    print(f"[APPROVED_SHA256] {ap}")
    print(f"[MATCH] {str(cur_sha == ap)}")
    print(f"[LAST_CHANGE_LOG] {lock.get('last_change_log')}")
    return 0


def cmd_set(args: argparse.Namespace) -> int:
    if not CFG_PATH.exists():
        print(f"[FAIL] config missing: {CFG_PATH}")
        return 2
    if not LOCK_PATH.exists():
        print(f"[FAIL] lock missing: {LOCK_PATH} (run: python tools\\paper_engine_config_lock.py init)")
        return 3

    before_cfg = load_json(CFG_PATH)
    before_sha = sha256_file(CFG_PATH)

    lock = load_json(LOCK_PATH)
    approved = str(lock.get("approved_sha256") or "")
    if approved and before_sha != approved:
        print("[FAIL] current config sha256 != approved_sha256 in lock.")
        print(f"  current={before_sha}")
        print(f"  approved={approved}")
        print("  -> This indicates out-of-band modification. Restore from backup or re-init intentionally.")
        return 4

    # build patch dict from --set and optional --patch-file
    patch: Dict[str, Any] = {}
    if args.patch_file:
        pf = Path(args.patch_file)
        if not pf.exists():
            print(f"[FAIL] patch file missing: {pf}")
            return 5
        patch_obj = load_json(pf)
        if not isinstance(patch_obj, dict):
            print("[FAIL] patch file must be a JSON object (dict).")
            return 6
        patch.update(patch_obj)

    sets = args.set or []
    dotted_applied: Dict[str, Any] = {}
    for s in sets:
        k, v = parse_kv(s)
        dotted_applied[k] = v

    after_cfg = dict(before_cfg)

    # apply dict patch shallow (top-level merge)
    for k, v in patch.items():
        after_cfg[k] = v

    # apply dotted keys (supports nested)
    for k, v in dotted_applied.items():
        set_by_dotted_key(after_cfg, k, v)

    # write backup
    bak = CFG_PATH.with_suffix(CFG_PATH.suffix + f".bak_{_ts()}")
    bak.write_bytes(CFG_PATH.read_bytes())
    print(f"[OK] BACKUP {bak}")

    # write new config atomically
    atomic_write_json(CFG_PATH, after_cfg)

    after_sha = sha256_file(CFG_PATH)

    # write change log
    patch_record = {}
    if patch:
        patch_record["patch_file_merge"] = patch
    if dotted_applied:
        patch_record["set_dotted_keys"] = dotted_applied

    logp = write_change_log("SET", before_sha, after_sha, patch_record, before_cfg, after_cfg)

    # update lock approved hash
    lock["ts"] = _ts()
    lock["approved_sha256"] = after_sha
    lock["last_change_log"] = str(logp)
    atomic_write_json(LOCK_PATH, lock)

    print(f"[OK] APPLIED sha256={after_sha}")
    print(f"[OK] CHANGE_LOG {logp}")
    print(f"[OK] LOCK_UPDATED {LOCK_PATH}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Initialize lock with current config hash")
    p_init.set_defaults(fn=cmd_init)

    p_status = sub.add_parser("status", help="Show current vs approved hash")
    p_status.set_defaults(fn=cmd_status)

    p_set = sub.add_parser("set", help="Apply patch via lock (backup + log + update approved hash)")
    p_set.add_argument("--patch-file", default=None, help="JSON file with top-level keys to merge")
    p_set.add_argument("--set", action="append", help="Set dotted key: a.b.c=value (value is JSON if possible)")
    p_set.set_defaults(fn=cmd_set)

    args = ap.parse_args()
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
