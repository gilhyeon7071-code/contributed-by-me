# -*- coding: utf-8 -*-
"""RD_20260901_topn 1단계 - 하네스 전용 발주 래퍼

왜 래퍼인가
  kis_order_dispatch_from_exec.py 는 산출물 경로가 PAPER_DIR / LOG_DIR 상수에 박혀 있다.
  그 상수를 통째로 돌리면 **리스크 게이트 가드도 같이 못 읽게 된다**
  (_load_orderflow_guard / _load_production_risk_guard / _load_risk_gate_guard 가 LOG_DIR 를 읽는다).
  가드가 조용히 무력화되는 것은 이 시스템에서 반복된 결함 유형이라 그 길은 쓰지 않는다.

그래서 하는 일
  1) v41.1 이 만들어 둔 동명 산출물 2개를 백업한다
  2) 디스패처를 원래대로 실행한다 (가드 전부 살아 있음)
  3) 새로 생긴 산출물을 2_Logs/topn/ 으로 옮긴다
  4) v41.1 산출물을 되돌린다
  결과: 두 시스템의 산출물이 섞이지 않고, 가드는 그대로 작동한다

안전장치
  --apply 는 --confirm TOPN_APPLY 를 추가로 요구한다 (E2E 러너와 같은 방식).
  기본은 dry-run 이다.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "2_Logs" / "topn"
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"


def artifacts(d: str, mode: str) -> list[Path]:
    return [
        PAPER_DIR / ("orders_%s_broker_submit_%s.csv" % (d, mode)),
        LOG_DIR / ("kis_order_dispatch_%s_%s.json" % (d, mode)),
    ]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="", help="YYYYMMDD. 비우면 최신 orders_*_exec.xlsx 의 날짜")
    ap.add_argument("--mock", default="true", choices=["auto", "true", "false"])
    ap.add_argument("--order-type", default="limit", choices=["market", "limit"])
    ap.add_argument("--apply", action="store_true", help="실제 발주. --confirm 필요")
    ap.add_argument("--confirm", default="", help="--apply 시 TOPN_APPLY 를 넣어야 한다")
    ap.add_argument("--allow-offhours", action="store_true")
    ap.add_argument("--allow-non-today", action="store_true")
    args = ap.parse_args()

    if args.apply and args.confirm.strip() != "TOPN_APPLY":
        print("[STOP] --apply 는 --confirm TOPN_APPLY 를 요구한다")
        return 2
    if args.apply and args.mock != "true":
        print("[STOP] 1단계는 모의계좌에서만 발주한다. --mock true 로만 --apply 가능하다")
        return 2

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    d = args.date.strip()
    if not d:
        cands = sorted(OUT_DIR.glob("orders_*_exec.xlsx"))
        if not cands:
            print("[STOP] orders_*_exec.xlsx 가 없다. topn_build_orders.py 를 먼저 돌려라")
            return 2
        d = cands[-1].stem.split("_")[1]
        print("[DATE] 최신 주문 파일에서 %s 를 잡았다" % d, flush=True)
    mode = "mock" if args.mock == "true" else ("prod" if args.mock == "false" else "auto")

    orders = OUT_DIR / ("orders_%s_exec.xlsx" % d)
    if not orders.exists():
        print("[STOP] %s 가 없다. topn_build_orders.py 를 먼저 돌려라" % orders)
        return 2

    # 1) 백업 - v41.1 이 같은 이름에 써 둔 것을 보존한다
    saved = {}
    for p in artifacts(d, mode):
        if p.exists():
            b = p.with_suffix(p.suffix + ".topn_bak")
            shutil.copy2(p, b)
            saved[p] = b
            print("[BAK] %s" % p.name, flush=True)

    # 1-b) **우리 직전 submit log 를 원래 자리에 되돌려 놓는다** [2026-09-03]
    #   디스패처는 submit log 로 중복 발주를 막는다(`_load_done_keys`).
    #   그런데 이 래퍼가 매 실행마다 그 파일을 topn/ 으로 옮기고 원본을 지운다.
    #   -> 장중에 반복 시도하는 구조에서는 **매 주기마다 같은 주문이 다시 나간다.**
    #   한 번만 돌던 동안에는 드러나지 않았다. 되돌려 놓아야 dedup 이 산다.
    # [2026-09-08] **`not p.exists()` 조건이 덫이었다.**
    #   그 자리에 파일이 하나라도 있으면 복원을 건너뛰고, 디스패처는 그 파일에서
    #   done_keys 를 읽는다. 우리 이력이 없으니 dedup 이 **조용히 죽는다.**
    #   실측 2026-09-08: 12:57 에 다른 실행이 그 경로에 헤더만 있는 파일을 남겼고
    #   13:05 부터 [DEDUP] 줄이 사라지면서 003230 이 12:41 / 13:05 / 13:15 세 번 발주돼
    #   의도 12주 대신 **27주(슬롯 예산의 2.09배)** 가 체결됐다.
    #   v41.1 이 같은 경로에 정상적으로 쓰는 날에도 똑같이 터진다 - 이미 놓여 있던 덫이다.
    #   -> 존재 여부로 갈리지 말고 **합친다.** 우리 이력이 항상 dedup 입력에 들어가야 한다.
    #   csv 가 아닌 산출물(json)은 기존대로 "없을 때만 복원" 이다.
    restored_ours = []
    for p in artifacts(d, mode):
        ours = OUT_DIR / ("topn_" + p.name)
        if not ours.exists():
            continue
        if not p.exists():
            shutil.copy2(ours, p)
            restored_ours.append(p.name)
            continue
        if p.suffix.lower() != ".csv":
            continue
        try:
            import pandas as _pd
            a = _pd.read_csv(ours, dtype=str)
            b = _pd.read_csv(p, dtype=str)
            merged = _pd.concat([a, b], ignore_index=True).drop_duplicates()
            if len(merged) > len(b):
                merged.to_csv(p, index=False, encoding="utf-8-sig")
                restored_ours.append("%s(+%d행 병합)" % (p.name, len(merged) - len(b)))
        except Exception as e:  # noqa: BLE001
            # 병합 실패를 조용히 넘기면 dedup 이 다시 죽는다. 크게 적는다.
            print("[DEDUP][WARN] %s 병합 실패 (%s: %s). **중복 발주 위험**"
                  % (p.name, type(e).__name__, e), flush=True)
    if restored_ours:
        print("[DEDUP] 직전 발주 이력 복원: %s" % ", ".join(restored_ours), flush=True)

    # 2) 실행 - 가드 전부 살아 있는 원래 경로
    cmd = [sys.executable, str(ROOT / "tools" / "kis_order_dispatch_from_exec.py"),
           "--date", d, "--orders-path", str(orders),
           "--mock", args.mock, "--order-type", args.order_type]
    if args.apply:
        cmd.append("--apply")
    if args.allow_offhours:
        cmd.append("--allow-offhours")
    if args.allow_non_today:
        cmd.append("--allow-non-today")
    print("[RUN] %s" % " ".join(cmd[1:]), flush=True)
    r = subprocess.run(cmd, cwd=str(ROOT))
    rc = int(r.returncode)

    # 3) 산출물 이동  4) 원복
    moved = []
    for p in artifacts(d, mode):
        if p.exists():
            dest = OUT_DIR / ("topn_" + p.name)
            shutil.copy2(p, dest)
            moved.append(dest.name)
            p.unlink()
    for p, b in saved.items():
        shutil.move(str(b), str(p))
        print("[RESTORE] %s" % p.name, flush=True)

    print("[OUT] %s -> %s" % (", ".join(moved) if moved else "(산출물 없음)", OUT_DIR), flush=True)

    # [2026-09-03] 폭락 가드에 전부 막힌 것은 **실패가 아니라 정상 대기**다.
    #   risk_gate_guard(risk_off)는 모의계좌에서도 우회되지 않는다(설계). 시장이 실제로
    #   폭락 중이면 안 사는 것이 맞다. 그런데 디스패처는 그 상태를 rc=2 로 돌려주므로
    #   그대로 배선하면 **매일 실패 경보가 쌓이고 진짜 실패가 묻힌다.**
    #   이 저장소 규약상 rc=3 은 "대기, 실패 아님"이고 run_tool_with_alert.bat 이
    #   [WAIT] 로 처리한다. 전량 차단이면 3 으로 낮춘다. **일부라도 나갔으면 원래 rc 를 쓴다.**
    # [2026-09-03 실측] `moved` 확인이 없으면 **이전 실행이 남긴 낡은 submit log** 를 읽고
    #   엉뚱한 사유로 대기 판정을 내린다. 실제로 날짜 불일치(apply blocked for non-today)로
    #   산출물이 0건이었는데 "리스크 게이트 대기"로 오분류했다.
    #   -> 이번 실행이 산출물을 실제로 냈을 때만 판정한다. 그 외 실패는 그대로 실패다.
    if args.apply and rc != 0 and moved:
        try:
            import csv as _csv
            sub = OUT_DIR / ("topn_orders_%s_broker_submit_%s.csv" % (d, mode))
            if sub.exists():
                with sub.open("r", encoding="utf-8-sig", newline="") as fh:
                    rows = [r for r in _csv.DictReader(fh)]
                mine = [r for r in rows if str(r.get("note", "")).startswith("RD_TOPN_STAGE1")]
                if mine:
                    blocked = [r for r in mine
                               if str(r.get("dispatch_status", "")) == "PRECHECK_RISK_GATE_HARD_BLOCK"]
                    if len(blocked) == len(mine):
                        reason = str(mine[0].get("precheck_msg", "") or "risk_gate")
                        print("[WAIT] 전량 %d건이 리스크 게이트에 막혔다 (%s). 실패가 아니라 대기다"
                              % (len(mine), reason), flush=True)
                        rc = 3
        except Exception as e:
            print("[WARN] 대기 판정 실패, 원래 rc 를 유지한다: %s" % e, flush=True)

    print("[MODE] apply=%s mock=%s  rc=%d" % (args.apply, args.mock, rc), flush=True)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
