# -*- coding: utf-8 -*-
"""
cleanup_improved.py  (v2.0 - 2026-02-24)

湲곗〈 cleanup_1_data.py ?鍮?異붽? 泥섎━:
  1) 肄붾뱶議곌컖 ?뚯씪/?붾젆?좊━ ??젣 (0諛붿씠??+ 鍮꾩젙???뚯씪紐?
  2) 鍮꾪몴以 ?뺤옣??諛깆뾽 泥섎━ (.asofforce_, .BROKEN_, .broken_, ??
  3) '???대뜑' ??'_krx_clean' ?대쫫 蹂寃?
  4) Windows 寃쎈줈紐??뚯씪 泥섎━
  5) D:/?놁뼱??濡쒖뺄 _bak/<ts>/ 濡??꾩뭅?대툕 媛??
  6) .bak_before_restore, .bak_A_*, .bak_C*_, .bak_H*_ ?⑦꽩 ?ы븿

?ㅽ뻾:
  python cleanup_improved.py                  # DRY (湲곕낯): 怨꾪쉷留?異쒕젰
  python cleanup_improved.py DOIT             # ?ㅼ젣 ?ㅽ뻾 (?대룞/??젣/?대쫫蹂寃?
  python cleanup_improved.py DOIT --no-logs   # 2_Logs 泥섎━ ?쒖쇅
  python cleanup_improved.py DOIT --dest D:/1_Data_Archive
"""
from __future__ import annotations

import os
import sys
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parent  # E:\1_Data
TS = datetime.now().strftime("%Y%m%d_%H%M%S")

# ?꾩뭅?대툕 紐⑹쟻吏: D:\ ?놁쑝硫?濡쒖뺄 _bak/ ?ъ슜
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

# ??????????????????????????????????????????????
# 蹂댄샇 紐⑸줉
# ??????????????????????????????????????????????
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

# ??????????????????????????????????????????????
# ?뺢퇋???⑦꽩
# ??????????????????????????????????????????????

# 湲곗〈 .bak_YYYYMMDD ?⑦꽩
BAK_PAT = re.compile(r".*\.bak_?\d{8}.*$", re.IGNORECASE)

# ?뺤옣??諛깆뾽 ?⑦꽩 (.bak_A_, .bak_C*_, .bak_H*_, .bak_before_*, .broken_, .BROKEN_)
BAK_EXT_PAT = re.compile(
    r".*\.(bak_[A-Za-z]|bak_before|broken_|BROKEN_|asofforce_|cleandiag_|"
    r"dateparsefix_|FINALFIX_|indentfix_|loadfix_|nokfix_|parseddiag_|"
    r"pickfix_|rebak_|loadfix_|fixsyntax_)",
    re.IGNORECASE
)

# 肄붾뱶議곌컖 ?뚯씪紐??⑦꽩 (Python ?덉빟???쒗쁽????낇엺????
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

# 肄붾뱶議곌컖 ?붾젆?좊━紐??⑦꽩
_CODE_FRAG_DIRS = {
    "(report['meta'].get('latest_date')",
    "'')",
    "'').replace('-'",
    "mx",
    "None",
    "or",
    "pq.ParquetFile(str(p)).metadata",
}

# Windows 寃쎈줈紐??뚯씪 (?뚯씪紐낆뿉 Windows 寃쎈줈媛 ?듭㎏濡??ㅼ뼱媛?寃?
_WIN_PATH_FILES = {
    "C:UsersjjtopAppDataLocalTempexcel_content.txt",
}

# Windows ?섍꼍蹂?섎챸 ?뚯씪
_ENV_VAR_FILES = {
    "%BAKCFG%",
}

# 鍮??뚯씪 + ?대쫫??Python ?쒗쁽??臾몄옄 ?ы븿
_CODE_CHARS_PAT = re.compile(r"[\[\]()'=.]")


def _is_code_fragment_file(p: Path) -> bool:
    """0諛붿씠?몄씠怨??대쫫??肄붾뱶議곌컖???뚯씪 ?먮퀎."""
    if p.name in _CODE_FRAG_NAMES:
        return True
    if p.name in _WIN_PATH_FILES:
        return True
    if p.name in _ENV_VAR_FILES:
        return True
    # 0諛붿씠?몄씠怨??대쫫??Python ?쒗쁽???⑦꽩 ?ы븿
    try:
        if p.stat().st_size == 0 and _CODE_CHARS_PAT.search(p.name):
            return True
    except OSError:
        pass
    return False


def _is_code_fragment_dir(p: Path) -> bool:
    """?대쫫??肄붾뱶議곌컖???붾젆?좊━ ?먮퀎."""
    return p.name in _CODE_FRAG_DIRS


# ??????????????????????????????????????????????
# ?≪뀡 ???
# ??????????????????????????????????????????????
@dataclass
class Action:
    kind: str          # "move" | "delete" | "rename"
    src: Path
    dst: Path | None   # rename/move: ??? delete: None
    reason: str


# ??????????????????????????????????????????????
# 蹂댄샇 ?щ? ?뺤씤
# ??????????????????????????????????????????????
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
    # ?뺥솗???뚯씪 蹂댄샇
    rel_s = "/".join(parts)
    if rel_s in PROTECT_FILES or parts[-1] in PROTECT_FILES:
        return True
    return False


# ??????????????????????????????????????????????
# ?뚮옖 ?섎┰
# ??????????????????????????????????????????????
def plan_actions() -> list[Action]:
    actions: list[Action] = []

    for p in sorted(BASE_DIR.iterdir()):
        # ?쒖뒪???붾젆?좊━ 嫄대꼫?
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

        # ?? 2) 肄붾뱶議곌컖 ?붾젆?좊━ ??젣 ???????????????????
        if p.is_dir() and _is_code_fragment_dir(p):
            # ?붾젆?좊━ ?????뚯씪???덉쑝硫??대룞, 鍮꾩뼱?덉쑝硫???젣
            children = list(p.iterdir())
            if not children:
                actions.append(Action(kind="delete", src=p, dst=None,
                                      reason="code_fragment_empty_directory"))
            else:
                actions.append(Action(kind="move", src=p,
                                      dst=DEST_DIR / "garbage_dirs" / _safe_name(name),
                                      reason="肄붾뱶議곌컖_?붾젆?좊━_鍮꾩뼱?덉??딆쓬"))
            continue

        if p.is_file():
            # ?? 3) 肄붾뱶議곌컖 ?뚯씪 ??젣 ??????????????????????
            if _is_code_fragment_file(p):
                actions.append(Action(kind="delete", src=p, dst=None,
                                      reason="肄붾뱶議곌컖_?뚯씪"))
                continue

            # ?? 4) 鍮꾪몴以 ?뺤옣??諛깆뾽 ?대룞 ?????????????????
            if BAK_EXT_PAT.match(name):
                actions.append(Action(kind="move", src=p,
                                      dst=DEST_DIR / "backups_ext" / name,
                                      reason="鍮꾪몴以?뺤옣??諛깆뾽"))
                continue

            # ?? 5) ?쒖? .bak_YYYYMMDD 諛깆뾽 ?대룞 ????????????
            if BAK_PAT.match(name):
                actions.append(Action(kind="move", src=p,
                                      dst=DEST_DIR / "backups" / name,
                                      reason="?쒖?_bak_諛깆뾽"))
                continue

    # ?? 6) 2_Logs 30???댁쟾 ?뚯씪 ?대룞 (--no-logs ??嫄대꼫?) ????
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
                                          reason="濡쒓렇_30??珥덇낵"))

    # 以묐났 ?쒓굅
    seen: set[str] = set()
    deduped: list[Action] = []
    for a in actions:
        k = str(a.src).lower()
        if k not in seen:
            seen.add(k)
            deduped.append(a)
    return deduped


def _safe_name(name: str) -> str:
    """?뚯씪?쒖뒪?쒖뿉???ъ슜 遺덇??ν븳 臾몄옄瑜?_濡?移섑솚."""
    return re.sub(r'[<>:"/\\|?*\x00-\x1f\']', "_", name)[:80]


# ??????????????????????????????????????????????
# ?ㅽ뻾
# ??????????????????????????????????????????????
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
        print(f"  [WARN] rename ??곸씠 ?대? 議댁옱: {dst}")
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
            print(f"  [ERR] {a.kind} ?ㅽ뙣: {a.src}  err={e}")
    return counts


# ??????????????????????????????????????????????
# 硫붿씤
# ??????????????????????????????????????????????
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

    # 醫낅쪟蹂?異쒕젰
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
    print(f"[CLEANUP v2] 珥?{total}媛????(??젣:{len(by_kind.get('delete',[]))}, "
          f"?대쫫蹂寃?{len(by_kind.get('rename',[]))}, "
          f"?대룞:{len(by_kind.get('move',[]))})")

    if mode == "DRY":
        print("[CLEANUP v2] DRY 紐⑤뱶 - ?ㅼ젣 蹂寃??놁쓬.")
        print("[CLEANUP v2] ?ㅽ뻾?섎젮硫? python cleanup_improved.py DOIT")
        return 0

    # DOIT 紐⑤뱶
    try:
        DEST_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[CLEANUP v2] FATAL: dest ?앹꽦 ?ㅽ뙣: {DEST_DIR}  err={e}")
        return 3

    counts = execute(actions)

    # 寃곌낵 ???
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

    print(f"\n[CLEANUP v2] ?꾨즺: ?대룞={counts['move']}, ??젣={counts['delete']}, "
          f"?대쫫蹂寃?{counts['rename']}, ?ㅻ쪟={counts['error']}")
    print(f"[CLEANUP v2] 寃곌낵 ??? {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

