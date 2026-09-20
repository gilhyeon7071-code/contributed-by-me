# -*- coding: utf-8 -*-
"""2026-08-20 운영 변경 9건이 **지금도 그대로인지** 매번 다시 잰다.

[2026-09-12] 미결 대장 C9. PLANS (151) 이후 3주간 "후보 발생일 대기" 로 묶여 있던 항목.

왜 도구인가
  9건은 전부 "적용했다" 로 끝났고 검증은 후보 발생일을 기다렸다. 그 사이 되돌아가도
  아무도 모른다. 이 시스템이 반복해서 겪은 실패가 **조용한 원복**이다.
  그래서 한 번 판정하고 끝내지 않고 **매번 다시 재는 형태**로 만든다.

판정
  PASS    실효값·코드가 변경 후 상태와 일치
  FAIL    되돌아갔다
  PENDING 아직 관측 못 함 (거래일 산출물 필요) - **PASS 로 흡수하지 않는다**

    python tools/verify_20260820_changes.py
    python tools/verify_20260820_changes.py --json
"""
from __future__ import annotations

import argparse
import glob
import io
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
_NL = chr(10)


def _cfg() -> Dict[str, Any]:
    from paper_engine.config import load_config
    return load_config()


def _dig(d: Any, *keys: str) -> Any:
    for k in keys:
        if not isinstance(d, dict) or k not in d:
            return None
        d = d[k]
    return d


def _src(rel: str) -> str:
    p = ROOT / rel
    if not p.is_file():
        return ""
    return io.open(str(p), encoding="utf-8", errors="replace").read()


def _stable() -> Dict[str, Any]:
    p = ROOT / "12_Risk_Controlled" / "stable_params_v41_1.json"
    if not p.is_file():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def _captures() -> List[Dict[str, Any]]:
    out = []
    for f in sorted(glob.glob(str(ROOT / "2_Logs" / "pending_entry_status_capture_*.json")))[-40:]:
        try:
            out.append(json.load(io.open(f, encoding="utf-8")))
        except Exception:
            continue
    return out


def checks() -> List[Dict[str, str]]:
    c = _cfg()
    st = _stable()
    caps = _captures()
    r: List[Dict[str, str]] = []

    def add(n: int, name: str, verdict: str, ev: str) -> None:
        r.append({"n": str(n), "name": name, "verdict": verdict, "evidence": ev})

    # 1) intraday_momentum_recheck.block_v_accel_min 1 -> 0
    #    close_auction 에도 같은 이름의 키가 있으나 **그건 다른 게이트**이고 변경 대상이 아니었다
    v = _dig(c, "normal_realtime_gap_policy", "intraday_momentum_recheck", "block_v_accel_min")
    add(1, "block_v_accel_min 1->0", "PASS" if v == 0 else "FAIL",
        "실효값=%r (close_auction 쪽 %r 은 별개 게이트, 변경 대상 아님)"
        % (v, _dig(c, "normal_realtime_gap_policy", "close_auction", "block_v_accel_min")))

    # 1b) E2E - 차단이 이동하지 않았나. (56) 이 "약 22%" 로 예고한 지점
    rows = [x for d in caps for x in (d.get("entry_decision_rows") or [])]
    moved = [x for x in rows if "REDUCED_MIN_QTY_BLOCK" in str(x.get("reason", ""))
             and str(x.get("qty_initial")) == "1"]
    src_e = _src("paper_engine/entry.py")
    guarded = "if int(qty) < int(old_qty_mom)" in src_e
    if not rows:
        add("1b", "E2E: 차단 이동 여부", "PENDING", "캡처에 진입 판정 행이 없다")
    elif guarded:
        # **코드에 가드가 있다는 것과 "통과하더라" 는 다르다.**
        #   수리 후 캡처에서 qty_initial=1 행이 차단되지 않고 지나간 것을 봐야 PASS 다.
        #   수리 시각(2026-09-12 19:14) 이후 캡처가 나올 때까지는 PENDING.
        post = [x for d in caps if str(d.get("generated_at", "")) > "2026-09-12T19:14"
                for x in (d.get("entry_decision_rows") or []) if str(x.get("qty_initial")) == "1"]
        passed = [x for x in post if "REDUCED_MIN_QTY_BLOCK" not in str(x.get("reason", ""))]
        if not post:
            add("1b", "E2E: 차단 이동 여부", "PENDING",
                "가드는 들어갔다(2026-09-12). 수리 후 qty_initial=1 관측이 아직 0행 - "
                "다음 거래일 2026-09-14. 과거 차단 %d행은 수리 전 기록이다" % len(moved))
        elif passed:
            add("1b", "E2E: 차단 이동 여부", "PASS",
                "수리 후 qty_initial=1 행 %d건 중 %d건이 최소수량으로 차단되지 않았다"
                % (len(post), len(passed)))
        else:
            add("1b", "E2E: 차단 이동 여부", "FAIL",
                "수리 후에도 qty_initial=1 행 %d건이 전부 차단됐다 - 다른 바닥이 있다" % len(post))
    else:
        add("1b", "E2E: 차단 이동 여부", "FAIL",
            "qty_initial=1 인데 최소수량으로 차단된 행 %d건 - 수리가 원복됐다" % len(moved))

    # 2) certified / operational 분리
    g = _src("utils/stable_params_gate.py")
    ok2 = ("certified" in g) and ("operational" in g)
    add(2, "certified/operational 분리", "PASS" if ok2 else "FAIL",
        "utils/stable_params_gate.py certified=%d operational=%d"
        % (g.count("certified"), g.count("operational")))

    # 3) state.py 이중 구현 -> 정식 게이트 위임
    s = _src("paper_engine/state.py")
    ok3 = "from utils.stable_params_gate import evaluate_stable_params" in s
    add(3, "state.py 정식 게이트 위임", "PASS" if ok3 else "FAIL",
        "위임 import %s / 사본 잔재(min_mean_pf 직접 판정) %s"
        % ("있음" if ok3 else "**없음**", "없음" if s.count("min_mean_pf") <= 3 else "의심"))

    # 4) require_macd_golden 1.0 -> 0.0
    v = st.get("require_macd_golden")
    add(4, "require_macd_golden 1.0->0.0", "PASS" if v == 0.0 else "FAIL", "stable_params=%r" % v)

    # 5) max_open_to_entry_chase_pct 0.05 -> 0.28
    v = _dig(c, "split_entry", "max_open_to_entry_chase_pct")
    add(5, "max_open_to_entry_chase_pct 0.05->0.28", "PASS" if v == 0.28 else "FAIL", "실효값=%r" % v)

    # 6) news_topic_execution_policy 명시적 비활성
    v = _dig(c, "news_topic_execution_policy", "enabled")
    add(6, "news_topic_execution_policy enabled=false", "PASS" if v is False else "FAIL",
        "실효값=%r (키가 없으면 코드 기본값 True 로 후보를 지운다)" % v)

    # 7) medium_news_adjustment 가산 -> 승산
    m = _src("tools/final_score_merge_daily.py")
    ok7 = "(1.0 + _mn)" in m or "(1.0 + mn)" in m
    add(7, "medium_news 가산->승산", "PASS" if ok7 else "FAIL",
        "final_score = (base + exec_lob) x (1 + medium_news) 형태 %s" % ("확인" if ok7 else "**미확인**"))

    # 8) final_score_base 8축 -> 2축 (tech 0.75 + fund 0.25)
    ok8 = '_tech * _w_tech + _fund * _w_fund' in m
    dflt = 'w_tech_score", 0.75' in m
    declared = ("w_tech_score" in st)
    add(8, "final_score_base 8축->2축", "PASS" if ok8 else "FAIL",
        "산식 %s / 가중 출처=%s" % ("2축 확인" if ok8 else "**미확인**",
        "stable_params 선언" if declared else "코드 기본값 0.75/0.25 (선언 없음)" if dflt else "불명"))

    # 9) close_auction.max_day_range_pct 0.07 -> 0.30
    #    [2026-09-12] **값이 맞는 것과 작동하는 것은 다르다.** 이 게이트를 부르는 자리는
    #    `use_intraday_realtime_entry` 의 else 가지 하나뿐인데 운영은 항상 realtime 이다.
    #    캡처 86행에 CLOSE_AUCTION 사유 0건. 값만 보고 PASS 를 내면 거짓 PASS 다.
    v = _dig(c, "normal_realtime_gap_policy", "close_auction", "max_day_range_pct")
    reached = any("CLOSE_AUCTION" in str(x.get("reason", "")) for x in rows)
    if v != 0.3:
        add(9, "close_auction.max_day_range_pct 0.07->0.30", "FAIL", "실효값=%r" % v)
    elif reached:
        add(9, "close_auction.max_day_range_pct 0.07->0.30", "PASS",
            "실효값=0.3, 경로 도달 확인")
    else:
        add(9, "close_auction.max_day_range_pct 0.07->0.30", "PENDING",
            "실효값=0.3 이나 **경로가 죽어 있다** - 캡처 %d행에 CLOSE_AUCTION 사유 0건. "
            "운영은 use_intraday_realtime_entry 가지로만 간다. 종가매매를 살릴지는 결정 대기 "
            "(PLANS 374)" % len(rows))
    return r


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    res = checks()
    if args.json:
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0 if not any(x["verdict"] == "FAIL" for x in res) else 1
    n_pass = sum(1 for x in res if x["verdict"] == "PASS")
    n_fail = sum(1 for x in res if x["verdict"] == "FAIL")
    n_pend = sum(1 for x in res if x["verdict"] == "PENDING")
    print("=" * 74)
    print(" 2026-08-20 운영 변경 재검증   PASS %d / FAIL %d / PENDING %d" % (n_pass, n_fail, n_pend))
    print("=" * 74)
    for x in res:
        mark = {"PASS": "O", "FAIL": "X", "PENDING": "-"}[x["verdict"]]
        print("  [%s] %-3s %-42s %s" % (mark, x["n"], x["name"][:42], x["verdict"]))
        print("        %s" % x["evidence"][:120])
    if n_pend:
        print(_NL + "  * PENDING 은 PASS 가 아니다. 관측 산출물이 아직 없다는 뜻이다")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
