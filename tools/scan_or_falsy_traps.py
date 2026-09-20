# -*- coding: utf-8 -*-
"""`.get(키, 기본값) or 기본값` 함정을 **설정값과 대조해서** 찾는다.

## 왜 이 절차인가

`x.get("k", D) or D` 는 설정에 **0 이 들어 있으면 그 0 을 삼키고 D 를 낸다.**
2026-09-10 에 이것 때문에 생산 손익이 왕복 1.400% 를 청구하고 있었다
(설정은 0.400% 였다. BROKEN_WINDOW_REGISTER **C7**).

**코드만 보면 못 가른다.** 그날 실측: 68키 83곳이 나왔는데 대부분 무해했다
(`max_hold_days` 가 0 일 리 없다). **함정은 설정에 실제로 0 이 있을 때만 문다.**

그래서 이 도구는 두 단계다:
```
1. 코드에서 `.get("키", 0아닌기본값) or` 를 전부 뽑는다
2. **살아 있는 설정 파일들에서 그 키의 실제 값을 찾아 대조**한다
   -> 값이 0 인 키만 진짜 후보다
```
그리고 남은 후보도 **어느 절(section)을 읽는지** 사람이 확인해야 한다.
그날 2건 중 1건이 위양성이었다 - 0 이 든 절과 읽는 절이 달랐다.

## 스캔 범위 — 디렉터리부터 센다

이름 기반 검색은 세 번 실패했다. 특히 `paper_engine/` 이 **패키지 디렉터리**라
`E:\\1_Data\\*.py` 만 보면 14모듈이 통째로 빠진다
(`[[feedback_scan_scope_before_claiming_all]]`).

## 산출물

```
2_Logs/or_falsy_trap_scan_latest.json    이름에 날짜 없음
```

[2026-09-10] 신설. PLANS (322)(327)
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import io
import json
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "2_Logs" / "or_falsy_trap_scan_latest.json"

CODE_DIRS = ["*.py", "tools/*.py", "utils/*.py", "paper_engine/*.py", "checkfile/*.py"]
CFG_GLOBS = ["paper/paper_engine_config.json", "config/*.json", "12_Risk_Controlled/*.json"]

# 이미 조사해서 설명이 붙은 것들. **새 건과 갈라 보이게** 한다.
#   여기 없는 것이 나오면 그건 아직 아무도 안 본 것이다.
KNOWN = {
    ("pricing_engine.py", "fee_pct"):
        "docstring 이 C7 당시 옛 코드를 인용한 것. 살아 있는 코드가 아니다 (2026-09-10 확인)",
    (r"tools\verify_cost_and_ladder_invariants.py", "fee_pct"):
        "결함을 일부러 재현하는 검증 코드(GUARD-1). 의도된 것이다",
    (r"tools\validate_surge_intraday_execution.py", "high_rejection_min_drawdown_pct"):
        "위양성. 0 이 든 절은 surge_exit_policy/reversal_exit 인데 "
        "이 코드는 intraday_reversal_exit(0.025)를 읽는다 (L185, 2026-09-10 확인)",
}


PAT = re.compile(r'\.get\(\s*["\']([^"\']+)["\']\s*,\s*([0-9][0-9._e+-]*)\s*\)\s*or\s+')


def _walk(o, key_set, found, path=""):
    if isinstance(o, dict):
        for k, v in o.items():
            if k in key_set and isinstance(v, (int, float)) and not isinstance(v, bool):
                found.setdefault(k, []).append({"path": path + "/" + str(k), "value": v})
            _walk(v, key_set, found, path + "/" + str(k))
    elif isinstance(o, list):
        for i, x in enumerate(o):
            _walk(x, key_set, found, path + "[%d]" % i)


def main() -> int:
    ap = argparse.ArgumentParser(description="or-falsy 함정 스캔 (읽기 전용)")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()

    files = []
    for d in CODE_DIRS:
        files += glob.glob(str(ROOT / d))
    files = sorted(set(f for f in files if "backup" not in os.path.basename(f).lower()))

    sites = {}
    for f in files:
        try:
            lines = io.open(f, encoding="utf-8", errors="replace").read().splitlines()
        except Exception:
            continue
        for i, l in enumerate(lines, 1):
            t = l.strip()
            if t.startswith("#"):
                continue
            for m in PAT.finditer(t):
                k, d = m.group(1), m.group(2)
                try:
                    if float(d) == 0.0:      # 기본값이 0 이면 함정이 무해하다(0 or 0 == 0)
                        continue
                except Exception:
                    pass
                sites.setdefault(k, []).append({
                    "file": os.path.relpath(f, ROOT), "line": i,
                    "default": d, "code": t[:120],
                })

    cfg_files = []
    for g in CFG_GLOBS:
        cfg_files += glob.glob(str(ROOT / g))
    found = {}
    for c in sorted(set(cfg_files)):
        try:
            _walk(json.load(io.open(c, encoding="utf-8-sig")), set(sites), found,
                  os.path.basename(c))
        except Exception:
            continue

    biting = {}
    for k, occ in found.items():
        zeros = [o for o in occ if float(o["value"]) == 0.0]
        if zeros:
            biting[k] = {"config_zero_at": zeros, "code_sites": sites[k]}

    # 설명된 것과 새 것을 가른다
    unexplained = {}
    for _k, _v in biting.items():
        _new = [st for st in _v["code_sites"] if (st["file"], _k) not in KNOWN]
        if _new:
            unexplained[_k] = {"config_zero_at": _v["config_zero_at"], "code_sites": _new}

    payload = {
        "scanned_at": dt.datetime.now().isoformat(timespec="seconds"),
        "code_dirs": CODE_DIRS,
        "files_scanned": len(files),
        "keys_in_code": len(sites),
        "sites_in_code": sum(len(v) for v in sites.values()),
        "keys_found_in_config": len(found),
        "keys_with_config_zero": sorted(biting),
        "biting": biting,
        "unexplained": unexplained,
        "unexplained_n": sum(len(v["code_sites"]) for v in unexplained.values()),
        "known_explained": {("%s | %s" % (f, k)): why for (f, k), why in KNOWN.items()},
        "note": "설정에 0 이 있는 키만 진짜 후보다. 그 다음 **어느 절을 읽는지** 사람이 확인할 것.",
    }
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if not a.quiet:
        print("파일 %d개 / 코드 후보 %d키 %d곳 / 설정에서 값을 찾은 키 %d개"
              % (len(files), len(sites), payload["sites_in_code"], len(found)))
        print()
        if not biting:
            print("설정값이 0 인 키: **없음** - 지금 무는 함정이 없어요")
        else:
            print("설정값이 0 인 키:")
            for k, v in biting.items():
                print("  %s" % k)
                for z in v["config_zero_at"]:
                    print("      설정 %s = %r" % (z["path"], z["value"]))
                for st in v["code_sites"]:
                    tag = KNOWN.get((st["file"], k))
                    print("      코드 %s:%d  기본값 %s%s"
                          % (st["file"], st["line"], st["default"],
                             ("   [설명됨] " + tag) if tag else "   <-- **미조사**"))
        print()
        if payload["unexplained_n"] == 0:
            print("**미조사 건 0.** 위 항목은 전부 2026-09-10 에 확인해 설명이 붙어 있어요.")
        else:
            print("**미조사 %d건.** 각 건마다 '읽는 절' 이 그 0 이 든 절인지 확인하세요 - 위양성이 흔합니다."
                  % payload["unexplained_n"])
        print()
        print("저장: %s" % OUT.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
