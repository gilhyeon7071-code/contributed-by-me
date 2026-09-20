# -*- coding: utf-8 -*-
"""2026-09-11 운영 변경 6건의 검증을 **기계가** 판정한다.

PLANS (358)(359)(361)(362)(363). 관측 전용 — 아무것도 바꾸지 않는다.

왜 필요한가
  2026-09-11 에 로그를 손으로 읽어 판정했고 그 과정에서 다섯 번 틀렸다.
  09-14(월)에는 세 검증이 같은 날 겹치고 시간대가 다르다(09:00~10:00 / 장중 / 16시 이후).
  손으로 읽으면 또 섞는다. 그리고 미리 적어둔 검증 지점은 **읽는 사람이 없으면 안 일어난다.**

판정은 셋뿐이다
  PASS     기대대로다
  FAIL     기대와 다르다
  PENDING  아직 판정할 수 없다 (휴장 / 해당 시각 전 / 산출물 없음)
  **PENDING 을 PASS 로 흡수하지 않는다.** 안 돈 검사와 통과한 검사는 다르다.

    python tools/verify_20260914_changes.py
    python tools/verify_20260914_changes.py --date 20260914
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER = ROOT / "paper"

CRLF = chr(13) + chr(10)
CAP_KRW = 868176          # (362) max_gross/daily_new 0.01 x 실효자본
TOPN_CASH_FLOOR = 2470000  # (362) topn 재매수용으로 남겨야 할 최소 예수금


def _read_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return None


def _is_trading_day(ymd: str) -> bool:
    try:
        hol = set(json.loads((ROOT / "holidays.json").read_text(encoding="utf-8-sig")).get("holidays") or [])
    except Exception:
        hol = set()
    d = dt.datetime.strptime(ymd, "%Y%m%d").date()
    return d.weekday() < 5 and ymd not in hol


def _batch_text() -> str:
    p = LOG_DIR / "run_paper_daily_last.txt"
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def check_step_order(txt: str) -> Dict[str, Any]:
    """(358) [7/9] 진입이 [6.96/9] 검증보다 **앞**에 오는가."""
    m_entry = re.search(r"\[(\d{2}:\d{2}:\d{2})\] \[7/9\] paper_engine main START", txt)
    m_btval = re.search(r"\[(\d{2}:\d{2}:\d{2})\] \[6\.96/9\] run_backtest_validation_real\.bat START", txt)
    if not m_entry or not m_btval:
        return {"verdict": "PENDING", "detail": "배치 로그에 두 스텝이 아직 없다"}
    ok = m_entry.start() < m_btval.start()
    return {"verdict": "PASS" if ok else "FAIL",
            "detail": "진입 %s / 검증 %s" % (m_entry.group(1), m_btval.group(1)),
            "entry_at": m_entry.group(1)}


def check_freshness_precheck(txt: str) -> Dict[str, Any]:
    """(359) 진입 전 신선도 검사가 rc=0 으로 통과했는가."""
    m = re.search(r"\[6\.999/9\] btval_freshness_precheck END rc=(\d+)", txt)
    if not m:
        return {"verdict": "PENDING", "detail": "검사 기록 없음"}
    rc = int(m.group(1))
    return {"verdict": "PASS" if rc == 0 else "FAIL", "detail": "rc=%d" % rc}


def check_account_basis() -> Dict[str, Any]:
    """(363) 엔진이 브로커 실측을 자본 기준으로 쓰는가."""
    d = _read_json(LOG_DIR / "paper_ddm_status_latest.json")
    if not d:
        return {"verdict": "PENDING", "detail": "paper_ddm_status 없음"}
    ab = ((d.get("metric_details") or {}).get("account_basis")) or {}
    sel = str(ab.get("account_basis_selected") or "")
    return {"verdict": "PASS" if sel == "broker" else "FAIL",
            "detail": "selected=%s equity=%s (생성 %s)" % (sel, ab.get("equity_est"), d.get("generated_at"))}


def check_dispatch(ymd: str) -> Dict[str, Any]:
    """(362) 발주가 DRY_RUN 이 아니라 실제로 나갔는가."""
    d = _read_json(LOG_DIR / ("kis_order_dispatch_%s_mock.json" % ymd))
    if not d:
        return {"verdict": "PENDING", "detail": "그날 dispatch 산출물 없음"}
    counts = d.get("counts") or {}
    applied = bool(d.get("apply"))
    if not applied:
        return {"verdict": "FAIL", "detail": "apply=False counts=%s (스위치가 안 먹었다)" % counts}
    if int(counts.get("DRY_RUN", 0) or 0) > 0 and not any(
            int(v or 0) > 0 for k, v in counts.items() if k != "DRY_RUN"):
        return {"verdict": "FAIL", "detail": "apply=True 인데 전부 DRY_RUN: %s" % counts}
    return {"verdict": "PASS", "detail": "apply=True counts=%s" % counts}


def check_exposure_cap(ymd: str) -> Dict[str, Any]:
    """(362) 그날 신규 진입 총액이 상한을 넘지 않았는가."""
    f = PAPER / "fills.csv"
    if not f.is_file():
        return {"verdict": "PENDING", "detail": "fills.csv 없음"}
    total = 0.0
    n = 0
    with f.open("r", encoding="utf-8", newline="") as fh:
        for r in csv.reader(fh):
            if len(r) < 5 or not str(r[0]).startswith(ymd):
                continue
            if str(r[2]).upper() != "BUY":
                continue
            try:
                total += float(r[4]) * int(float(r[3]))
                n += 1
            except (TypeError, ValueError):
                continue
    if n == 0:
        return {"verdict": "PENDING", "detail": "그날 매수 0건"}
    return {"verdict": "PASS" if total <= CAP_KRW else "FAIL",
            "detail": "%d건 합계 %,.0f원 / 상한 %,.0f원".replace(",", "") % (n, total, CAP_KRW)}


def check_morning_window(ymd: str) -> Dict[str, Any]:
    """(358 효과) 장중 루프가 배치 락으로 몇 사이클이나 건너뛰었는가. 09-11 은 94회.

    [2026-09-12 수리] 휴장일에 **0회를 PASS 로 냈다.** 주말에는 루프가 weekend 분기로
    빠져 락 검사에 도달조차 하지 않는다. 0 은 "고쳐졌다" 가 아니라 "측정이 안 됐다" 다.
    이 파일 스스로 "PENDING 을 PASS 로 흡수하지 않는다" 고 써놓고 그것을 어겼다.
    """
    if not _is_trading_day(ymd):
        return {"verdict": "PENDING",
                "detail": "휴장 - 루프가 weekend 분기라 락 검사에 도달하지 않는다"}
    p = LOG_DIR / "run_intraday_paper_last.txt"
    try:
        txt = p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return {"verdict": "PENDING", "detail": "루프 로그 없음"}
    hits = re.findall(r"%s-%s-%s (\d{2}:\d{2}:\d{2})[^\n]{0,120}run_paper_daily\.lock"
                      % (ymd[:4], ymd[4:6], ymd[6:8]), txt)
    if not hits:
        return {"verdict": "PASS", "detail": "락 건너뜀 0회 (09-11 은 94회)"}
    return {"verdict": "FAIL" if len(hits) > 20 else "PASS",
            "detail": "락 건너뜀 %d회 (%s ~ %s), 09-11 은 94회" % (len(hits), hits[0], hits[-1])}


def check_round_interference() -> Dict[str, Any]:
    """(362) topn 라운드 여력이 남아 있는가."""
    d = _read_json(LOG_DIR / "broker_ledger_reconcile_latest.json")
    if not d:
        return {"verdict": "PENDING", "detail": "대조 산출물 없음"}
    cash = ((d.get("sources") or {}).get("broker") or {}).get("cash")
    if cash is None:
        return {"verdict": "PENDING", "detail": "예수금 미수신"}
    return {"verdict": "PASS" if float(cash) >= TOPN_CASH_FLOOR else "FAIL",
            "detail": "예수금 %.0f원 / 하한 %.0f원" % (float(cash), TOPN_CASH_FLOOR)}


def check_batch_window(txt: str, ymd: str) -> Dict[str, Any]:
    """(358) 배치가 **시계상** 장중을 얼마나 점유하는가.

    [2026-09-12] (358) 은 순서를 바꿨을 뿐 **창을 비우지 않았다.**
    진입은 개장 전으로 당겨졌지만 락은 배치 전체 수명 동안 잡혀 있고,
    그동안 장중 루프의 **발주 스텝이 안 돈다**(intraday_paper_loop.py:2705).
    09-11 은 08:36~10:58 로 개장 후 118분을 먹었다.
    """
    # [2026-09-13] **휴장 게이트를 뺐다.** 배치는 휴장일에도 08:30 에 시작하므로
    #   시계상 점유는 그대로 잴 수 있다. 오히려 휴장일은 자료가 적어 **하한**이다 -
    #   휴장일에 09:05 에 끝나면 거래일엔 그보다 늦게 끝난다.
    #   예전에는 여기서 PENDING 을 내서, 오늘 35분을 실측했는데도 "판정 불가" 로 나왔다.
    # [2026-09-13] **잘린 로그로 점유를 재면 안 된다.**
    #   실측 사고: 배치가 메모리로 죽어 로그가 재실행분으로 덮였고,
    #   그 조각(09:07~09:08)을 재서 "개장 후 8분 점유 PASS" 를 냈다.
    #   진짜 그날 배치는 08:30~09:05 (35분)였다. 완결 표지가 없으면 판정하지 않는다.
    if "WRAPPER_EXIT" not in txt:
        return {"verdict": "PENDING",
                "detail": "배치 로그에 WRAPPER_EXIT 가 없다 - 중단된 실행이라 점유를 못 잰다"}
    marks = re.findall(r"^" + re.escape("[") + r"(\d{2}:\d{2}:\d{2})" + re.escape("]"), txt, re.M)
    if len(marks) < 2:
        return {"verdict": "PENDING", "detail": "배치 로그 부족"}
    start, end = marks[0], marks[-1]

    def _mins(t: str) -> int:
        h, m, _s = t.split(":")
        return int(h) * 60 + int(m)

    over = _mins(end) - (9 * 60)
    if over <= 0:
        return {"verdict": "PASS", "detail": "%s~%s, 개장 전 종료" % (start, end)}
    _note = "" if _is_trading_day(ymd) else " [휴장 실측 - 거래일엔 이보다 길다]"
    return {"verdict": "FAIL" if over > 20 else "PASS",
            "detail": "%s~%s, 개장 후 %d분 점유 (09-11 은 118분)%s" % (start, end, over, _note)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=dt.datetime.now().strftime("%Y%m%d"))
    args = ap.parse_args()
    ymd = args.date.strip()
    trading = _is_trading_day(ymd)
    txt = _batch_text()

    checks: List[tuple] = [
        ("(358) 배치 순서: 진입이 검증보다 앞", check_step_order(txt), False),
        ("(359) 진입 전 신선도 검사 rc=0", check_freshness_precheck(txt), False),
        ("(363) 자본 기준 = 브로커 실측", check_account_basis(), False),
        ("(362) 발주가 실제로 나갔나", check_dispatch(ymd), True),
        ("(362) 신규 진입 총액 <= 상한", check_exposure_cap(ymd), True),
        ("(358) 아침 창 회복 (락 건너뜀)", check_morning_window(ymd), True),
        ("(362) 라운드 여력 유지", check_round_interference(), False),
        ("(358) 배치가 개장 후 몇 분 점유", check_batch_window(txt, ymd), False),
    ]

    print("=" * 74)
    print(" 2026-09-11 운영변경 검증  |  대상일 %s (%s)" % (ymd, "거래일" if trading else "휴장"))
    print("=" * 74)
    counts: Dict[str, int] = {}
    for name, res, needs_trading in checks:
        v = res["verdict"]
        if needs_trading and not trading and v == "PENDING":
            v = "PENDING"
            res["detail"] = (res.get("detail") or "") + " [휴장이라 판정 불가]"
        counts[v] = counts.get(v, 0) + 1
        mark = {"PASS": "O", "FAIL": "X", "PENDING": "-"}.get(v, "?")
        print(" [%s] %-34s %-8s %s" % (mark, name, v, res.get("detail") or ""))
    print("-" * 74)
    print(" PASS %d / FAIL %d / PENDING %d" % (counts.get("PASS", 0), counts.get("FAIL", 0), counts.get("PENDING", 0)))
    if counts.get("PENDING"):
        print(" * PENDING 은 통과가 아니다. 판정할 수 없는 상태다")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
