# -*- coding: utf-8 -*-
"""원장 파일이 **기록인지 재계산인지** 스스로 밝히게 한다.

## 왜 필요한가 (2026-09-10)

`paper/trades.csv` 와 `paper/trades_calc.csv` 는 모양이 비슷하고 이름도 비슷한데
성격이 완전히 다르다.

```
trades.csv       그때 장부에 찍힌 값     = **기록(record)**
trades_calc.csv  오늘 설정으로 전체 재계산 = **재계산(recomputed)**
                 631행 전부 fee 0.0 / slip 0.001 / tax 0.002 로 동일하다.
                 2025-12 거래도 그렇다 - 즉 과거 사실이 아니다
```

**어느 쪽인지 파일에 적혀 있지 않다.** 그래서 같은 거래가 한쪽은 -1.78%,
다른 쪽은 +0.03% 로 **부호까지 다른데** 그것이 모순으로 보이지 않았다.
(408건 매칭 중 405건이 0.1%p 넘게 다르고 중앙 차이 +2.69%p)

오늘 나 자신이 이것 때문에 두 번 틀렸다 —
"비용 컬럼이 없어 정량화 불가" 라 했다가, "trades_calc 가 권위 원장" 이라 했다가.
**둘 다 파일이 스스로 말하지 않아서 생긴 오독이다.**

## 어떻게

CSV 스키마를 건드리지 않는다(검증기·소비자가 깨진다).
같은 이름의 **사이드카 JSON** 을 옆에 둔다.

```
paper/trades.csv        -> paper/trades.csv.kind.json
paper/trades_calc.csv   -> paper/trades_calc.csv.kind.json
```

[2026-09-10] 신설. PLANS (334)
"""
from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SPEC = {
    "paper/trades.csv": {
        "kind": "record",
        "meaning": "그때 장부에 찍힌 값. 체결 시점의 엔진이 계산한 결과다",
        "cost_breakdown": False,
        "caution": "비용 분해 컬럼이 없다. pnl_pct 안에 그때의 비용이 녹아 있고 "
                   "얼마였는지는 이 파일만으로 알 수 없다. "
                   "2_Logs/cost_profile_history_ledger.jsonl 의 그날 프로파일과 함께 읽을 것",
        "authority_for": ["실제로 무엇이 언제 체결·청산됐는가"],
        "not_authority_for": ["비용이 얼마였는가"],
    },
    "paper/trades_calc.csv": {
        "kind": "recomputed",
        "meaning": "**오늘 설정으로 전체를 다시 계산한 값.** 과거 사실이 아니다",
        "cost_breakdown": True,
        "caution": "631행 전부 동일한 fee/slip/tax 를 쓴다(2025-12 거래 포함). "
                   "즉 '지금 비용으로 다시 재면 얼마였을까' 이지 "
                   "'그때 얼마였나' 가 아니다. trades.csv 와 값이 다른 것은 정상이며, "
                   "그 차이가 곧 비용 모델 변화(또는 결함)의 크기다",
        "authority_for": ["현행 비용 가정 하의 반사실 수익률"],
        "not_authority_for": ["그때 실제로 적용된 비용", "그때의 장부 손익"],
    },
}


def _sha8(p: Path):
    try:
        return hashlib.sha256(p.read_bytes()).hexdigest()[:8]
    except Exception:
        return None


def main() -> int:
    now = dt.datetime.now().isoformat(timespec="seconds")
    n = 0
    for rel, spec in SPEC.items():
        src = ROOT / rel
        out = ROOT / (rel + ".kind.json")
        payload = dict(spec)
        payload.update({
            "file": rel,
            "written_at": now,
            "exists": src.exists(),
            "sha8": _sha8(src),
            "mtime": (dt.datetime.fromtimestamp(src.stat().st_mtime).isoformat(timespec="seconds")
                      if src.exists() else None),
            "written_by": "tools/write_ledger_kind_manifest.py",
        })
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[KIND] %-26s -> %s  (%s)" % (rel, out.name, spec["kind"]))
        n += 1
    print("%d개 기록. 이 사이드카는 CSV 스키마를 건드리지 않는다." % n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
