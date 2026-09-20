# -*- coding: utf-8 -*-
"""탐색 라운드를 시도 원장(research_trial_ledger.json)에 한 줄 추가한다.

왜 필요한가
    DSR(deflated Sharpe)의 n_trials 는 "몇 번 시도했는가"로 샤프를 깎는 장치다.
    2026-08-30 이전에는 n_trials = len(param_grid) = 4 였고, 실제 시도는 수백이었다.
    그 상태의 "DSR PASS" 는 아무 보증도 아니었다(고치니 0.5928 -> 0.0055 로 뒤집혔다).
    원장을 갱신하지 않으면 그 상태로 되돌아간다.

규칙
    탐색/격자/HPO 라운드를 돌린 쪽이 그 라운드의 마지막 단계로 추가한다.
    숫자는 줄이지 않는다. 이 값은 "기록으로 방어 가능한 하한" 이다.

사용
    python tools/add_research_trial.py --n 20 --source "구성일치 기준선 재측정 20조합" \
        --ref "PLANS (142) 1절"
    python tools/add_research_trial.py --show
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "2_Logs" / "research_trial_ledger.json"


def _load() -> dict:
    if not LEDGER.exists():
        return {"_note": "DSR n_trials 누적 시도 원장", "entries": []}
    try:
        return json.loads(LEDGER.read_text(encoding="utf-8"))
    except Exception as exc:
        print("[ERR] 원장을 읽지 못했다: %s: %s" % (type(exc).__name__, exc))
        raise SystemExit(2)


def _total(obj: dict) -> int:
    return sum(int(e.get("n") or 0) for e in (obj.get("entries") or []) if isinstance(e, dict))


def main() -> int:
    ap = argparse.ArgumentParser(description="시도 원장에 라운드 추가")
    ap.add_argument("--n", type=int, help="이 라운드에서 훑은 조합/시도 수")
    ap.add_argument("--source", default="", help="무엇을 훑었는지 (사람이 읽는 설명)")
    ap.add_argument("--ref", default="", help="근거 위치 (PLANS 번호, 메모리 이름 등)")
    ap.add_argument("--when", default="", help="YYYY-MM-DD 또는 YYYYMMDD (기본: 오늘)")
    ap.add_argument("--show", action="store_true", help="현재 원장만 출력")
    ap.add_argument("--force", action="store_true", help="같은 항목이 있어도 추가")
    args = ap.parse_args()

    obj = _load()
    entries = obj.get("entries") or []

    if args.show or args.n is None:
        print("원장: %s" % LEDGER)
        print("갱신: %s   항목 %d건   누적 n_trials = %d" % (obj.get("updated", "-"), len(entries), _total(obj)))
        for e in entries:
            print("  %5d  %-40s  %s  (%s)" % (int(e.get("n") or 0), str(e.get("source"))[:40],
                                              e.get("when", "-"), e.get("ref", "-")))
        if args.n is None and not args.show:
            print("\n[USAGE] --n 과 --source 를 주면 추가한다. --show 는 조회만.")
            return 2
        return 0

    if args.n <= 0:
        print("[ERR] --n 은 1 이상이어야 한다")
        return 2
    if not args.source.strip():
        print("[ERR] --source 는 비울 수 없다. 나중에 이 줄을 읽는 사람이 무엇을 셌는지 알아야 한다")
        return 2

    # [2026-08-30] 원장은 "2026-08-29" 형식이고 --when 은 "20260829" 로 들어올 수 있다.
    #   포맷이 다르면 중복 가드가 조용히 통과한다(첫 시험에서 실제로 통과했다).
    #   저장은 YYYY-MM-DD 로 통일하고, 비교는 숫자만 뽑아서 한다.
    raw_when = (args.when or dt.datetime.now().strftime("%Y-%m-%d")).strip()
    digits = re.sub(r"[^0-9]", "", raw_when)
    when = ("%s-%s-%s" % (digits[0:4], digits[4:6], digits[6:8])) if len(digits) == 8 else raw_when
    new = {"n": int(args.n), "when": when, "source": args.source.strip(), "ref": args.ref.strip()}

    def _key(v):
        return re.sub(r"[^0-9]", "", str(v or ""))

    dup = [e for e in entries if isinstance(e, dict)
           and str(e.get("source", "")).strip() == new["source"]
           and _key(e.get("when")) == _key(when)]
    if dup and not args.force:
        print("[SKIP] 같은 날짜·같은 설명의 항목이 이미 있다. 중복 계산을 막는다.")
        print("       기존: %s" % json.dumps(dup[0], ensure_ascii=False))
        print("       정말 추가하려면 --force")
        return 3

    before = _total(obj)
    entries.append(new)
    obj["entries"] = entries
    obj["updated"] = dt.datetime.now().strftime("%Y-%m-%d")

    tmp = LEDGER.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(str(tmp), str(LEDGER))

    after = _total(obj)
    print("[OK] 추가: %s" % json.dumps(new, ensure_ascii=False))
    print("     누적 n_trials %d -> %d  (항목 %d건)" % (before, after, len(entries)))
    print("     다음 검증 실행부터 DSR 이 이 값을 쓴다.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
