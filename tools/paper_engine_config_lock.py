from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Tuple


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
CFG_PATH = PAPER_DIR / "paper_engine_config.json"
LOCK_PATH = PAPER_DIR / "paper_engine_config.lock.json"
LOG_DIR = ROOT / "2_Logs"
logger = logging.getLogger("paper_engine_config_lock")


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
    """점 표기 경로에 값을 넣는다. **중간 경로를 절대 덮어쓰지 않는다.**

    [2026-09-08 수리] 이전 구현은 이랬다.
        if k not in cur or not isinstance(cur[k], dict): cur[k] = {}
    중간 키가 dict 가 아니면 **그 자리를 빈 dict 로 갈아치웠다.** 리스트가 거기 있으면 사라진다.

    실측(수리 전):
        set_by_dotted_key(cfg, "drawdown_manager.stages.0.mdd_threshold", 0.30)
        전  stages = [{"mdd":0.2,"mdd_threshold":0.25}, {"mdd":0.27}]
        후  stages = {"0": {"mdd_threshold":0.3}}      <- 단계 전부 소실, mdd 도 사라짐
    그러면 drawdown.py 의 stage 순회가 빈 목록이 되어 **낙폭 제어가 통째로 무력화**된다.
    게다가 이 도구는 그 결과로 승인 해시를 갱신하므로 **"승인된 변경"으로 기록**된다.
    조용한 파괴다. PLANS 2026-09-08 (256).

    고친 뒤 규칙
      - 리스트는 정수 인덱스로 **들어간다** (기존엔 파괴했다)
      - 중간에 dict 도 list 도 아닌 값이 있으면 **예외를 던진다** (덮어쓰지 않는다)
      - 리스트 인덱스가 범위를 벗어나면 예외 (자동 확장하지 않는다)
      - 없는 dict 키를 만드는 것만 허용한다 (기존 동작 유지)
    """
    keys = dotted.split(".")
    cur: Any = obj
    for i, k in enumerate(keys[:-1]):
        path = ".".join(keys[:i + 1])
        if isinstance(cur, list):
            if not (k.lstrip("-").isdigit()):
                raise ValueError(f"'{path}': 리스트에는 정수 인덱스만 쓸 수 있다 (받은 값 '{k}')")
            idx = int(k)
            if idx < 0 or idx >= len(cur):
                raise ValueError(f"'{path}': 인덱스 {idx} 가 범위를 벗어났다 (길이 {len(cur)})")
            cur = cur[idx]
            continue
        if not isinstance(cur, dict):
            raise ValueError(f"'{path}': 중간 경로가 dict/list 가 아니다 ({type(cur).__name__}). 덮어쓰지 않는다")
        if k not in cur:
            cur[k] = {}
        elif not isinstance(cur[k], (dict, list)):
            raise ValueError(
                f"'{path}': 이미 값({type(cur[k]).__name__})이 있어 하위 경로를 만들 수 없다. 덮어쓰지 않는다"
            )
        cur = cur[k]

    last = keys[-1]
    if isinstance(cur, list):
        if not (last.lstrip("-").isdigit()):
            raise ValueError(f"'{dotted}': 리스트에는 정수 인덱스만 쓸 수 있다 (받은 값 '{last}')")
        idx = int(last)
        if idx < 0 or idx >= len(cur):
            raise ValueError(f"'{dotted}': 인덱스 {idx} 가 범위를 벗어났다 (길이 {len(cur)})")
        cur[idx] = value
        return
    if not isinstance(cur, dict):
        raise ValueError(f"'{dotted}': 마지막 경로가 dict/list 가 아니다 ({type(cur).__name__})")
    cur[last] = value


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
        logger.error("[FAIL] config missing: %s", CFG_PATH)
        return 2

    # Re-approving an existing lock erases the audit trail: it blesses whatever
    # is on disk without recording what changed.  That is the documented path by
    # which config keys changed with no change log.  Require --force + --reason,
    # and always leave a change log behind.
    # docs/exec-plans/active/20260820_config_lock_enforcement.md
    prev_approved = ""
    if LOCK_PATH.exists():
        try:
            prev_approved = str(load_json(LOCK_PATH).get("approved_sha256") or "")
        except Exception:
            prev_approved = "<unreadable>"
        if not bool(getattr(args, "force", False)):
            logger.error("[FAIL] lock already exists: %s", LOCK_PATH)
            logger.error("       approved_sha256=%s", prev_approved)
            logger.error("       re-approving hides what changed. use 'set' to change config,")
            logger.error("       or 'init --force --reason \"...\"' if you really must re-bless.")
            return 3
        if not str(getattr(args, "reason", "") or "").strip():
            logger.error("[FAIL] --force requires --reason")
            return 3

    cur_sha = sha256_file(CFG_PATH)
    change_log_path = None
    if LOCK_PATH.exists():
        change_log_path = write_change_log(
            "INIT_FORCE",
            prev_approved,
            cur_sha,
            {"init_force": True, "reason": str(getattr(args, "reason", "") or "")},
            {"_note": "previously approved config content is not recoverable from the lock file"},
            load_json(CFG_PATH),
        )
        logger.info("[OK] change log: %s", change_log_path)

    lock = {
        "ts": _ts(),
        "approved_sha256": cur_sha,
        "approved_config_path": str(CFG_PATH),
        "last_change_log": None if change_log_path is None else str(change_log_path),
        "note": "Initialized approval hash. Subsequent runs will require approved_sha256 match.",
    }
    atomic_write_json(LOCK_PATH, lock)
    logger.info("[OK] INIT lock written: %s", LOCK_PATH)
    logger.info("[OK] approved_sha256=%s", cur_sha)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    if not CFG_PATH.exists():
        logger.error("[FAIL] config missing: %s", CFG_PATH)
        return 2
    cur_sha = sha256_file(CFG_PATH)
    logger.info("[CFG] %s", CFG_PATH)
    logger.info("[CFG_SHA256] %s", cur_sha)

    if not LOCK_PATH.exists():
        logger.warning("[LOCK] missing: %s", LOCK_PATH)
        return 0

    lock = load_json(LOCK_PATH)
    ap = str(lock.get("approved_sha256") or "")
    logger.info("[LOCK] %s", LOCK_PATH)
    logger.info("[APPROVED_SHA256] %s", ap)
    logger.info("[MATCH] %s", str(cur_sha == ap))
    logger.info("[LAST_CHANGE_LOG] %s", lock.get("last_change_log"))
    return 0


def unset_by_dotted_key(obj: Dict[str, Any], dotted: str) -> bool:
    """[2026-09-12] 점 표기 키를 **지운다.** 지웠으면 True.

    잠금 도구에 삭제가 없어서 죽은 설정을 처분할 방법이 없었다
    (crash_risk_off 의 rv20_floor / rv20_spike_ratio / day_drop_pct / gap_down_pct -
     어떤 .py 도 읽지 않는데 값이 거의 0이라 되살아나면 상시 발동한다).
    PLANS (163)(165)(370).
    """
    parts = [x for x in str(dotted).split(".") if x]
    if not parts:
        return False
    cur: Any = obj
    for k in parts[:-1]:
        if not isinstance(cur, dict) or k not in cur:
            return False
        cur = cur[k]
    if not isinstance(cur, dict) or parts[-1] not in cur:
        return False
    del cur[parts[-1]]
    return True


def cmd_set(args: argparse.Namespace) -> int:
    if not CFG_PATH.exists():
        logger.error("[FAIL] config missing: %s", CFG_PATH)
        return 2
    if not LOCK_PATH.exists():
        logger.error("[FAIL] lock missing: %s (run: python tools\\paper_engine_config_lock.py init)", LOCK_PATH)
        return 3

    before_cfg = load_json(CFG_PATH)
    before_sha = sha256_file(CFG_PATH)

    lock = load_json(LOCK_PATH)
    approved = str(lock.get("approved_sha256") or "")
    if approved and before_sha != approved:
        logger.error("[FAIL] current config sha256 != approved_sha256 in lock.")
        logger.error("current=%s", before_sha)
        logger.error("approved=%s", approved)
        logger.error("out-of-band modification detected. Restore from backup or re-init intentionally.")
        return 4

    # build patch dict from --set and optional --patch-file
    patch: Dict[str, Any] = {}
    if args.patch_file:
        pf = Path(args.patch_file)
        if not pf.exists():
            logger.error("[FAIL] patch file missing: %s", pf)
            return 5
        patch_obj = load_json(pf)
        if not isinstance(patch_obj, dict):
            logger.error("[FAIL] patch file must be a JSON object (dict).")
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

    # [2026-09-12] --unset: 점 표기 키 삭제. 없는 키는 조용히 넘기지 않고 보고한다
    unset_done, unset_missing = [], []
    for k in (args.unset or []):
        (unset_done if unset_by_dotted_key(after_cfg, k) else unset_missing).append(k)
    if unset_missing:
        logger.warning("[WARN] UNSET 대상이 없다: %s", ", ".join(unset_missing))
    if unset_done:
        logger.info("[OK] UNSET %s", ", ".join(unset_done))
        # [2026-09-12] **설정 파일에서 지워도 DEFAULT_CONFIG 가 다시 공급한다.**
        #   load_config() 가 DEFAULT_CONFIG 를 깊은 병합하므로, 파일에서만 지운 키는
        #   실효 설정에 그대로 남는다. 그것을 모르고 "처분했다" 고 하면 거짓이다.
        #   실측으로 확인해 경고한다 (PLANS 370 / 미결 대장 C15).
        try:
            import sys as _sys
            _sys.path.insert(0, str(ROOT))
            from paper_engine.config import load_config as _load_cfg  # type: ignore
            _eff = _load_cfg()
            still: List[str] = []
            for _k in unset_done:
                _cur: Any = _eff
                _ok = True
                for _part in [x for x in _k.split(".") if x]:
                    if isinstance(_cur, dict) and _part in _cur:
                        _cur = _cur[_part]
                    else:
                        _ok = False
                        break
                if _ok:
                    still.append(_k)
            if still:
                logger.warning("[WARN] 파일에서는 지웠지만 **실효 설정에 그대로 있다** "
                               "(DEFAULT_CONFIG 가 공급): %s", ", ".join(still))
                logger.warning("[WARN] 진짜로 없애려면 paper_engine/config.py 의 "
                               "DEFAULT_CONFIG 도 같이 고쳐야 한다")
            else:
                logger.info("[OK] 실효 설정에서도 사라졌다 (DEFAULT_CONFIG 에 없던 키)")
        except Exception as _exc:  # noqa: BLE001
            logger.warning("[WARN] 실효 설정 확인 실패(%s) - 직접 load_config() 로 확인할 것",
                           type(_exc).__name__)

    if not (patch or dotted_applied or unset_done):
        logger.error("[FAIL] 바꿀 것이 없다. --set / --patch-file / --unset 중 하나가 필요하다")
        return 7

    # write backup
    bak = CFG_PATH.with_suffix(CFG_PATH.suffix + f".bak_{_ts()}")
    bak.write_bytes(CFG_PATH.read_bytes())
    logger.info("[OK] BACKUP %s", bak)

    # write new config atomically
    atomic_write_json(CFG_PATH, after_cfg)

    after_sha = sha256_file(CFG_PATH)

    # write change log
    patch_record = {}
    if patch:
        patch_record["patch_file_merge"] = patch
    if dotted_applied:
        patch_record["set_dotted_keys"] = dotted_applied
    if unset_done:
        patch_record["unset_dotted_keys"] = unset_done

    logp = write_change_log("SET", before_sha, after_sha, patch_record, before_cfg, after_cfg)

    # update lock approved hash
    lock["ts"] = _ts()
    lock["approved_sha256"] = after_sha
    lock["last_change_log"] = str(logp)
    atomic_write_json(LOCK_PATH, lock)

    logger.info("[OK] APPLIED sha256=%s", after_sha)
    logger.info("[OK] CHANGE_LOG %s", logp)
    logger.info("[OK] LOCK_UPDATED %s", LOCK_PATH)
    return 0


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Initialize lock with current config hash")
    p_init.add_argument("--force", action="store_true",
                        help="Re-approve even though a lock already exists (writes a change log)")
    p_init.add_argument("--reason", default="", help="Required with --force: why re-approve")
    p_init.set_defaults(fn=cmd_init)

    p_status = sub.add_parser("status", help="Show current vs approved hash")
    p_status.set_defaults(fn=cmd_status)

    p_set = sub.add_parser("set", help="Apply patch via lock (backup + log + update approved hash)")
    p_set.add_argument("--patch-file", default=None, help="JSON file with top-level keys to merge")
    p_set.add_argument("--set", action="append", help="Set dotted key: a.b.c=value (value is JSON if possible)")
    p_set.add_argument("--unset", action="append",
                       help="Remove dotted key: a.b.c  (2026-09-12 신설. 죽은 설정 처분용)")
    p_set.set_defaults(fn=cmd_set)

    args = ap.parse_args()
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(main())

