# -*- coding: utf-8 -*-
"""종목코드 -> 시장(KOSPI/KOSDAQ) 마스터를 만든다.

왜 필요한가.
KRX 일별 clean parquet 의 `market` 컬럼은 2026-04-17 까지 UNKNOWN 이 0% 였는데
2026-05-06 부터 41% 로 무너져 2026-08-20 에는 51.8%(2,583행 중 1,337행)가 UNKNOWN 이다.
원인은 데이터 소실이 아니라 수집 경로다 - `krx_update_clean_incremental.py` 의 보충 수집이
가져온 종목에 시장을 `"UNKNOWN"` 리터럴로 박아 넣는다.

과거 파일에는 값이 온전히 남아 있다. 그것들을 모으면 오늘의 UNKNOWN 1,337건이
**100% 해결된다**(실측, 미해결 0건). 이 스크립트는 그 마스터를 산출물로 굳힌다.

산출물: `2_Logs/market_master_latest.json`
  {"generated_at": ..., "codes": {"005690": "KOSPI", ...}, "counts": {...}, "sources": [...]}

읽기 전용이다. 다른 산출물을 고치지 않는다.
상세: .agent/PLANS.md 2026-08-21 (11)
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
OUT_PATH = LOGS / "market_master_latest.json"

KST = timezone(timedelta(hours=9))
VALID_MARKETS = ("KOSPI", "KOSDAQ")

# 오래된 것부터 읽어 최신 값이 덮어쓰게 한다(이전/이관 상장 반영).
SOURCE_GLOBS = (
    "_krx_seed_full/*.parquet",
    "_krx_manual/*.parquet",
    "krx_daily_archive/*.parquet",
)


def _now_iso() -> str:
    return datetime.now(tz=KST).isoformat(timespec="seconds")


def _file_ymd(path: Path) -> str:
    found = re.findall(r"(20\d{6})", path.name)
    return max(found) if found else ""


def _source_files() -> List[Path]:
    files: List[Path] = []
    for pattern in SOURCE_GLOBS:
        for p in ROOT.glob(pattern):
            if not p.is_file():
                continue
            parts = {x.lower() for x in p.parts}
            if parts & {"backup", "tmp", "_pytest_tmp"}:
                continue
            files.append(p)
    return sorted(files, key=_file_ymd)


def build_master() -> Tuple[Dict[str, str], List[str]]:
    master: Dict[str, str] = {}
    used: List[str] = []
    for path in _source_files():
        try:
            df = pd.read_parquet(path, columns=["code", "market"])
        except Exception:
            continue  # market 컬럼이 없는 파일은 조용히 건너뛴다
        if df.empty:
            continue
        mkt = df["market"].astype(str).str.upper().str.strip()
        keep = df[mkt.isin(VALID_MARKETS)]
        if keep.empty:
            continue
        used.append(path.name)
        codes = keep["code"].astype(str).str.zfill(6)
        markets = keep["market"].astype(str).str.upper().str.strip()
        for code, market in zip(codes, markets):
            # 우선주/신형우선주는 숫자가 아니다 (00104K, 37550L, 0001A0 ...).
            # isdigit() 로 거르면 이런 종목 43건이 통째로 빠진다.
            if len(code) == 6 and code.isalnum():
                master[code] = market
    return master, used


def load_master(path: Path = OUT_PATH) -> Dict[str, str]:
    """소비자용 로더. 파일이 없거나 깨졌으면 빈 dict 를 준다(호출부를 깨지 않는다)."""
    try:
        if not path.exists():
            return {}
        doc = json.loads(path.read_text(encoding="utf-8-sig"))
        codes = doc.get("codes")
        if not isinstance(codes, dict):
            return {}
        return {
            str(k).zfill(6): str(v).upper()
            for k, v in codes.items()
            if str(v).upper() in VALID_MARKETS
        }
    except Exception:
        return {}


def main() -> int:
    master, used = build_master()
    if not master:
        print("[MARKET_MASTER] no source rows found; nothing written")
        return 1
    counts = {m: sum(1 for v in master.values() if v == m) for m in VALID_MARKETS}
    doc = {
        "generated_at": _now_iso(),
        "total_codes": len(master),
        "counts": counts,
        "source_file_count": len(used),
        "source_files_tail": used[-5:],
        "codes": dict(sorted(master.items())),
    }
    LOGS.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"[MARKET_MASTER] codes={len(master)} {counts} sources={len(used)} -> {OUT_PATH.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
