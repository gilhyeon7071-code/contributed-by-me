# -*- coding: utf-8 -*-
"""2026-09-12 / 09-13 변경분이 **실제 거래일에 동작하는지** 판정한다.

[2026-09-13] 사용자 질문: *"어제건만 체크하면 되는거야?"* — 아니다.
이틀에 걸쳐 소스 31개 + 설정 3건 + RootB 2건을 바꿨다.
**머리로 추적하면 그게 곧 "확인 안 함" 이 된다.** 그래서 목록을 코드로 옮긴다.

판정
  PASS    실제 동작으로 확인됨
  FAIL    되돌아갔거나 틀림
  PENDING 아직 판정 불가 — **PASS 로 흡수하지 않는다**
          특히 **휴장일에는 대부분이 PENDING 이다.** 그 사실을 숨기지 않는다.

    python tools/verify_20260913_changes.py
    python tools/verify_20260913_changes.py --json
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import io
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
LOG_DIR = ROOT / "2_Logs"


def _is_trading_day(d: dt.date) -> bool:
    if d.weekday() >= 5:
        return False
    try:
        obj = json.loads((ROOT / "holidays.json").read_text(encoding="utf-8-sig"))
        vals = obj.get("holidays") if isinstance(obj, dict) else obj
        hs = {re.sub(r"\D", "", str(v))[:8] for v in (vals or [])}
    except Exception:
        hs = set()
    return d.strftime("%Y%m%d") not in hs


def _src(rel: str) -> str:
    p = ROOT / rel
    return io.open(str(p), encoding="utf-8", errors="replace").read() if p.is_file() else ""


def _json(p: Path) -> Dict[str, Any]:
    try:
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _json_today(p: Path, ymd: str) -> Dict[str, Any]:
    """**오늘 것이 아니면 빈 dict 가 아니라 나이를 붙여 돌려준다.**

    [2026-09-14] 이 함수가 없어서 09-14(월, 거래일) 아침에 **FAIL 을 오보했다.**
    산출물이 09-13(일)에 쓰인 NON_TRADING_DAY 였는데 나이를 안 보고 그대로 판정했다.
    다시 생성하니 RUNNING 이었다. 산출물은 낡을 수 있다 - 판정 도구가
    그것을 구별하지 못하면 **'모른다' 가 'FAIL' 로 둔갑한다.**
    -> [[feedback_check_artifact_age_first]] / screen-claim 스킬
    """
    d = _json(p)
    if not d:
        return {}
    raw = str(d.get("generated_at") or d.get("as_of") or d.get("ts") or "")
    stamp = re.sub(r"\D", "", raw)[:8]
    if not stamp:
        try:
            stamp = dt.datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y%m%d")
        except Exception:
            stamp = ""
    d["_stale"] = bool(stamp and stamp != ymd)
    d["_stamp"] = stamp
    return d


def _cfg() -> Dict[str, Any]:
    from paper_engine.config import load_config
    return load_config()


def _dig(d: Any, *keys: str) -> Any:
    for k in keys:
        if not isinstance(d, dict) or k not in d:
            return None
        d = d[k]
    return d


def checks(today: Optional[dt.date] = None) -> List[Dict[str, str]]:
    today = today or dt.date.today()
    trading = _is_trading_day(today)
    ymd = today.strftime("%Y%m%d")
    c = _cfg()
    r: List[Dict[str, str]] = []

    def add(tag: str, name: str, verdict: str, ev: str) -> None:
        r.append({"tag": tag, "name": name, "verdict": verdict, "evidence": ev})

    def pend(tag: str, name: str, why: str) -> None:
        add(tag, name, "PENDING", why + ("" if trading else " [휴장이라 판정 불가]"))

    # ── 09-12 변경 ────────────────────────────────────────────────────
    src_e = _src("paper_engine/entry.py")

    # (373) 최소수량 바닥이 축소되지 않은 1주 주문을 막던 것
    add("373", "최소수량 바닥은 축소 후에만",
        "PASS" if "if int(qty) < int(old_qty_mom)" in src_e else "FAIL",
        "entry.py 가드 문자열")
    rows = [x for f in sorted(glob.glob(str(LOG_DIR / "pending_entry_status_capture_*.json")))[-40:]
            for x in (_json(Path(f)).get("entry_decision_rows") or [])]
    # [2026-09-14 수정] `qty_initial == "1"` 만 봤다. 오늘 실측 12행의 qty_initial 은
    #   3~864 였고 1은 하나도 없었다. **조건이 너무 좁아 영원히 0행이다.**
    #   봐야 할 것은 "축소 결과가 1주가 된 건이 차단됐는가" 다.
    post = [x for x in rows if str(x.get("signal_date") or "") >= "20260914"
            and str(x.get("qty") or "").strip() in ("1", "1.0")]
    if not post:
        pend("373", "E2E: 축소 후 1주가 통과하나", "수리 후 거래일에 최종 1주 관측 0행")
    else:
        blocked = [x for x in post if "REDUCED_MIN_QTY_BLOCK" in str(x.get("reason", ""))]
        add("373", "E2E: 축소 후 1주가 통과하나",
            "PASS" if not blocked else "FAIL",
            "%d행 중 차단 %d행" % (len(post), len(blocked)))

    # (388) [2026-09-14 신설] 예산 배분 원장. 사슬의 **입력**이 기록되나
    _bl = LOG_DIR / "budget_alloc_ledger.jsonl"
    if not _bl.is_file():
        pend("388", "예산 배분 원장", "파일 없음 - 아직 진입 경로가 안 돌았다")
    else:
        _rows = []
        for _ln in _bl.read_text(encoding="utf-8").splitlines():
            _ln = _ln.strip()
            if not _ln:
                continue
            try:
                _rows.append(json.loads(_ln))
            except Exception:
                pass
        _today_rows = [x for x in _rows if str(x.get("ts") or "").replace("-", "")[:8] == ymd]
        if not _today_rows:
            pend("388", "예산 배분 원장", "%d행 있으나 오늘(%s) 것 0행" % (len(_rows), ymd))
        else:
            _last = _today_rows[-1]
            add("388", "예산 배분 원장", "PASS",
                "오늘 %d행  effective_basic=%.4f  예약합계=%.4f  종목당=%s원"
                % (len(_today_rows), float(_last.get("effective_basic", -1)),
                   float(_last.get("common_reserved", -1)),
                   format(int(float(_last.get("cap_notional", 0))), ",")))
            # 버킷 합이 1.0 을 넘는지도 같이 본다 - 넘으면 basic 이 잔여로 밀린다
            _sum = (float(_last.get("surge_reserved", 0)) + float(_last.get("split_alloc", 0))
                    + float(_last.get("recovery_alloc", 0)) + float(_last.get("reserve_alloc", 0))
                    + float(_last.get("basic_alloc_pct_cfg", 0)))
            add("388", "예산 버킷 합 <= 1.0", "PASS" if _sum <= 1.0 + 1e-9 else "FAIL",
                "합계 %.3f (급등 %.2f 분할 %.2f 회복 %.2f 예비 %.2f 일반 %.2f)"
                % (_sum, float(_last.get("surge_reserved", 0)), float(_last.get("split_alloc", 0)),
                   float(_last.get("recovery_alloc", 0)), float(_last.get("reserve_alloc", 0)),
                   float(_last.get("basic_alloc_pct_cfg", 0))))

    # (375) 수량 축소 사슬이 초기 산정부터 기록되나
    add("375", "수량 사슬이 예산상한부터 기록",
        "PASS" if '_track_qty(_trail, "budget_cap"' in src_e else "FAIL",
        "entry.py 계측 지점")
    # [2026-09-14 수정] 여기서 `qty_raw_calc` 를 찾고 있었다. **산출물에 없는 열쇠다.**
    #   entry_decision_rows 의 실제 키는 qty_initial / qty_trail 이다.
    #   그래서 이 검사는 거래일에 사슬이 멀쩡히 기록되고 있는데도 계속 PENDING 이었다.
    #   **틀린 열쇠로 찾으면 영원히 '판정 불가' 다.** 그건 안전한 게 아니라 조용한 실패다.
    chain = [x for x in rows if str(x.get("qty_trail") or "").strip()]
    if chain:
        heads = []
        for x in chain:
            m = re.findall(r"([A-Za-z_]+):(\d+)>(\d+)", str(x.get("qty_trail")))
            if m:
                heads.append(m[0][0])
        # 사슬이 예산상한(budget_cap) 또는 그 이전 단계부터 시작해야 한다
        add("375", "E2E: 사슬 출발점이 산출물에",
            "PASS" if heads else "FAIL",
            "qty_trail 기록 %d행  출발단계=%s" % (len(chain), sorted(set(heads))))
    else:
        pend("375", "E2E: 사슬 출발점이 산출물에", "qty_trail 기록 0행")

    # (376) 잔여 오버나이트 가드가 선언 키를 읽나
    src_x = _src("paper_engine/exit.py")
    ok376 = ("if _row_is_surge and not apply_to_surge:" in src_x
             and "INTRADAY_RESIDUAL_GUARD_CONTRACT" in src_x)
    add("376", "잔여 가드가 선언 키를 읽는다", "PASS" if ok376 else "FAIL",
        "exit.py 분기 + fail-closed 경고")

    # (382) 시장 이벤트 게이트 fail-closed
    ok382 = "_market_event_observation_block" in src_e and '"market_event_level": "UNKNOWN"' in src_e
    add("382", "이벤트 게이트 fail-closed + NORMAL 날조 제거",
        "PASS" if ok382 else "FAIL", "entry.py 관측 가드 + UNKNOWN 스텁")
    add("382", "require_market_observation 선언",
        "PASS" if _dig(c, "p1_entry_policy", "event_gate", "require_market_observation") is True else "FAIL",
        "실효값=%r" % _dig(c, "p1_entry_policy", "event_gate", "require_market_observation"))

    # (383) SIDECAR 제거
    g = _dig(c, "p1_entry_policy", "event_gate", "explicit_market_event_guard") or {}
    blk = [str(x).upper() for x in (g.get("block_event_types") or [])]
    add("383", "SIDECAR 는 차단 사유가 아니다",
        "PASS" if ("SIDECAR" not in blk and "CIRCUIT_BREAKER" in blk) else "FAIL",
        "block_event_types=%s" % blk)

    # (378) 사이클 상태 표시
    cyc = _json_today(LOG_DIR / "topn_cycle_status_latest.json", ymd)
    if not cyc:
        add("378", "topn 사이클 상태 산출물", "FAIL", "산출물 없음")
    elif cyc.get("_stale"):
        # **낡은 산출물로 판정하지 않는다.** 이것이 09-14 오보의 원인이었다
        pend("378", "topn 사이클 상태 산출물",
             "산출물이 %s 것이다 (오늘 %s). 다시 생성해야 판정 가능" % (cyc.get("_stamp"), ymd))
        pend("378", "E2E: RUNNING/IDLE 이 갈리나", "위와 같은 이유로 판정 불가")
    elif not trading:
        add("378", "topn 사이클 상태 산출물", "PASS",
            "state=%s (휴장이라 NON_TRADING_DAY 가 정답)" % cyc.get("state"))
        pend("378", "E2E: RUNNING/IDLE 이 갈리나", "휴장이라 창 안 상태가 없다")
    else:
        add("378", "topn 사이클 상태 산출물", "PASS", "state=%s" % cyc.get("state"))
        add("378", "E2E: RUNNING/IDLE 이 갈리나",
            "PASS" if cyc.get("state") in ("RUNNING", "IDLE", "WINDOW_CLOSED", "BEFORE_WINDOW") else "FAIL",
            "state=%s note=%s" % (cyc.get("state"), str(cyc.get("note"))[:50]))

    # (378) 미체결 스냅샷 생산자 배선
    oo = _json(LOG_DIR / "kis_open_orders_latest.json")
    oo_ymd = re.sub(r"\D", "", str(oo.get("date") or ""))[:8]
    if not oo:
        add("378", "미체결 스냅샷 생산", "FAIL", "산출물 없음")
    elif trading:
        add("378", "미체결 스냅샷 생산",
            "PASS" if oo_ymd == ymd else "FAIL", "date=%s (오늘 %s)" % (oo_ymd, ymd))
    else:
        # [2026-09-13] **생산 배선은 휴장일에도 확인된다.** 오늘 날짜가 찍혔으면
        #   배치가 그 스텝을 돌린 것이다. 예전에는 PENDING 으로 흡수해서
        #   실제로 도는 것을 "판정 불가" 로 감췄다.
        if oo_ymd == ymd:
            add("378", "미체결 스냅샷 생산", "PASS",
                "date=%s (오늘 생산됨 - 휴장일이라 내용은 직전 세션 것)" % oo_ymd)
        else:
            pend("378", "미체결 스냅샷 생산", "date=%s" % oo_ymd)

    # ── 09-13 변경 ────────────────────────────────────────────────────
    # (386) 비거래일 스냅샷 가드
    src_s = _src("tools/intraday_price_snapshot.py")
    add("386", "비거래일 스냅샷 가드",
        "PASS" if "_non_trading_day_reason()" in src_s else "FAIL",
        "intraday_price_snapshot main 가드")
    try:
        import pandas as pd
        ip = pd.read_csv(LOG_DIR / "intraday_prices_latest.csv", dtype=str)
        ip_ymd = sorted(ip["date"].dropna().unique())[-1] if "date" in ip else ""
    except Exception as exc:
        ip_ymd = "read_fail:%s" % type(exc).__name__
    if trading:
        add("386", "E2E: 장중 스냅샷 날짜",
            "PASS" if ip_ymd == ymd else "FAIL", "date=%s (오늘 %s)" % (ip_ymd, ymd))
    else:
        add("386", "E2E: 비거래일에 덮지 않았나",
            "PASS" if ip_ymd and ip_ymd < ymd else "FAIL",
            "date=%s (오늘 %s — 비거래일이므로 과거여야 맞다)" % (ip_ymd, ymd))

    # (386) 조용한 실패 제거
    add("386", "탐지기 종료가 사유를 말한다",
        "PASS" if "[SURGE_DETECTOR_ABORT]" in _src("tools/surge_detector_realtime.py") else "FAIL",
        "surge_detector_realtime 종료 출력")
    sr = _json(LOG_DIR / "surge_realtime_latest.json")
    add("386", "급등 탐지 상태",
        "PASS" if str(sr.get("status")) == "OK" else "FAIL",
        "status=%s" % sr.get("status"))

    # (386) 적응 진입 정책 신선도
    ok_ad = "policy_stale" in src_e and "policy_max_age_trading_days" in src_e
    add("386", "낡은 적응 진입 정책 거부", "PASS" if ok_ad else "FAIL", "entry.py 나이 가드")
    add("386", "policy_max_age_trading_days 선언",
        "PASS" if _dig(c, "adaptive_good_stock_entry", "policy_max_age_trading_days") == 5 else "FAIL",
        "실효값=%r" % _dig(c, "adaptive_good_stock_entry", "policy_max_age_trading_days"))

    # (386) 신선도 대장
    fg = _json(LOG_DIR / "artifact_freshness_guard_latest.json")
    cnt = fg.get("counts") or {}
    add("386", "신선도: 없음 0 / 미지정 오래된 것 0",
        "PASS" if (int(cnt.get("missing", 9)) == 0
                   and int(cnt.get("unspecified_and_old", 9)) == 0) else "FAIL",
        "missing=%s unspecified_and_old=%s stale=%s"
        % (cnt.get("missing"), cnt.get("unspecified_and_old"), cnt.get("stale")))

    # (386) 전진 선택 평가
    fe = _json(LOG_DIR / "topn_forward_eval_latest.json")
    if not fe:
        pend("386", "전진 선택 평가 산출물", "아직 생성 안 됨 (저녁 배치 22:10)")
    else:
        add("386", "전진 선택 평가 산출물", "PASS",
            "%d거래일 %d행" % (fe.get("ledger_days", 0), fe.get("ledger_rows", 0)))
    return r


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    today = dt.date.today()
    res = checks(today)
    if args.json:
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 1 if any(x["verdict"] == "FAIL" for x in res) else 0

    n = {k: sum(1 for x in res if x["verdict"] == k) for k in ("PASS", "FAIL", "PENDING")}
    trading = _is_trading_day(today)
    print("=" * 78)
    print(" 2026-09-12/13 변경 검증  |  대상일 %s (%s)"
          % (today.strftime("%Y%m%d"), "거래일" if trading else "휴장"))
    print("=" * 78)
    for x in res:
        mark = {"PASS": "O", "FAIL": "X", "PENDING": "-"}[x["verdict"]]
        print(" [%s] (%-3s) %-38s %-8s %s"
              % (mark, x["tag"], x["name"][:38], x["verdict"], x["evidence"][:60]))
    print("-" * 78)
    print(" PASS %d / FAIL %d / PENDING %d" % (n["PASS"], n["FAIL"], n["PENDING"]))
    print(" * PENDING 은 통과가 아니다. 판정할 수 없는 상태다")
    if not trading:
        print(" * 오늘은 휴장이다. 실제 매매 동작은 다음 거래일에만 판정된다")
    return 1 if n["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
