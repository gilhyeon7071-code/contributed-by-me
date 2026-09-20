# -*- coding: utf-8 -*-
"""KRX 애프터마켓(2026-09-14 개설, 16:00~20:00) 첫날 관측기.

docs/references/AFTERMARKET_20260914_OBSERVATION.md 의 1번 항목을 수집한다.

무엇을 묻나
  **일봉 종가·거래량의 정의가 바뀌는가.**
  value = 종가 x 거래량 으로 유동성 게이트를 건다. 거래량 정의가 바뀌면
  그날부터 value_floor_20e 같은 임계값의 **의미가 달라진다.**

어떻게
  같은 종목을 두 시점에 조회해 비교한다.
    T1  15:30~16:00  정규장 마감 직후
    T2  20:10 이후   애프터마켓 종료 후
  두 값이 같으면 일봉은 정규장만 반영하는 것이고, 다르면 애프터마켓이 섞인 것이다.

설계 원칙
  - **필드를 미리 고르지 않는다.** KIS 응답 output 을 통째로 저장한다.
    무엇이 바뀔지 모르는 상태에서 고르면 놓친다.
  - 원장은 날짜 없는 append-only JSONL (결정에 쓰는 산출물이다)
  - 조회 실패는 0 으로 채우지 않고 error 로 남긴다

    python tools/aftermarket_probe.py --label T1        # 수집
    python tools/aftermarket_probe.py --label T2
    python tools/aftermarket_probe.py --compare         # T1 vs T2 대조
    python tools/aftermarket_probe.py --label T1 --out-dir <경로>   # 격리 실행
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"
LOG_DIR = ROOT / "2_Logs"
LEDGER_NAME = "aftermarket_probe_ledger.jsonl"      # 날짜 없는 append-only

# 보유 6종목 + 대형주 2. 보유분은 우리 손익에 직접 걸리고,
# 대형주는 애프터마켓 거래가 실제로 붙는지 보기 위한 대조군이다.
DEFAULT_CODES = ["000370", "001210", "003230", "012210", "017900", "377450",
                 "005930", "000660"]

# 이 필드들이 **바뀌는지**가 관심사다. 저장은 output 전체를 하고, 요약만 이걸 쓴다.
WATCH = ["stck_prpr", "acml_vol", "acml_tr_pbmn", "stck_oprc", "stck_hgpr", "stck_lwpr",
         "prdy_vrss", "prdy_ctrt", "stck_sdpr"]


FLOW_DIR = ROOT / "_cache" / "investor_flow"
FLOW_SUM_COLS = ["indiv_qty", "forgn_qty", "inst_qty", "indiv_amt", "forgn_amt", "inst_amt"]


def flow_snapshot(target_ymd: str) -> Dict[str, Any]:
    """수급 원장에 **그날 행이 어떻게 적혀 있는지** 찍는다.

    [2026-09-12] 애프터마켓 관측 2번(동결 라운드 RD_20260831_flow_h10).
    VIBE_Investor_Flow_Daily 가 16:30 에 돈다 - 09-14 부터는 **애프터마켓 장중**이다.
    그 시각에 찍힌 09-14 수급이 최종값인지, 다음날 재수집에서 바뀌는지가 질문이다.
    T1/T2 로는 못 잡는다(16:30 수집이 T1 이후다). **날짜를 건너 비교**해야 한다.

    같은 target_ymd 를 09-14 저녁과 09-15 저녁에 각각 찍어 값이 바뀌면
    16:30 수집은 확정 전 스냅샷이고, 동결 라운드의 원자료 정의가 흔들린다.
    """
    out: Dict[str, Any] = {"target_ymd": target_ymd}
    try:
        import pandas as pd
        files = sorted(FLOW_DIR.glob("investor_flow_*.parquet"))
        if not files:
            out["status"] = "NO_FILE"
            return out
        ym = target_ymd[:6]
        cand = [f for f in files if ym in f.name] or files[-1:]
        df = pd.read_parquet(cand[-1])
        if "date" not in df.columns:
            out["status"] = "NO_DATE_COL"
            return out
        sub = df[df["date"].astype(str) == str(target_ymd)]
        out["source"] = cand[-1].name
        out["rows"] = int(len(sub))
        if len(sub) == 0:
            out["status"] = "NO_ROWS"       # 아직 수집 전이면 정상이다. 0 을 값으로 쓰지 않는다
            return out
        out["status"] = "OK"
        out["codes"] = int(sub["code"].nunique()) if "code" in sub.columns else None
        if "fetched_at" in sub.columns:
            out["fetched_at_max"] = str(sub["fetched_at"].astype(str).max())
        # [2026-09-12] 이 컬럼들은 **문자열(object)이고 빈 값 '' 가 섞여 있다.
        #   astype("float64") 는 전부 예외로 죽는다. to_numeric(coerce) 로 바꾸되
        #   **숫자로 못 읽은 개수를 같이 남긴다** - 조용히 버리면 합계가 거짓이 된다.
        sums, nonnum = {}, {}
        for c in FLOW_SUM_COLS:
            if c not in sub.columns:
                continue
            v = pd.to_numeric(sub[c], errors="coerce")
            sums[c] = float(v.sum())
            nonnum[c] = int(v.isna().sum())
        out["sums"] = sums
        out["nonnumeric_counts"] = nonnum
    except Exception as exc:
        out["status"] = "ERROR"
        out["error"] = "%s: %s" % (type(exc).__name__, exc)
    return out


def collect(codes: List[str], label: str, mock: bool, flow_ymd: str = "") -> Dict[str, Any]:
    sys.path.insert(0, str(TOOLS_DIR))
    from kis_order_client import KISOrderClient  # type: ignore

    now = dt.datetime.now()
    client = KISOrderClient.from_env(mock=mock)
    rows: List[Dict[str, Any]] = []
    for c in codes:
        try:
            r = client.inquire_price(code=c)
            rows.append({"code": c, "ok": True, "output": r.get("output") or {}})
        except Exception as exc:
            rows.append({"code": c, "ok": False, "error": "%s: %s" % (type(exc).__name__, exc)})
    return {
        "ts": now.strftime("%Y-%m-%dT%H:%M:%S"),
        "ymd": now.strftime("%Y%m%d"),
        "hhmm": now.strftime("%H%M"),
        "label": label,
        "mode": "mock" if mock else "prod",
        "codes": len(codes),
        "ok_count": sum(1 for r in rows if r["ok"]),
        "rows": rows,
        "flow": flow_snapshot(flow_ymd or now.strftime("%Y%m%d")),
    }


def compare(ledger: Path, ymd: str) -> int:
    if not ledger.is_file():
        print("[CMP] 원장 없음: %s" % ledger)
        return 2
    recs = []
    with ledger.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if str(d.get("ymd")) == ymd:
                recs.append(d)
    by_label: Dict[str, Dict[str, Any]] = {}
    for d in recs:
        by_label[str(d.get("label"))] = d          # 같은 라벨이 여럿이면 마지막 것
    if "T1" not in by_label or "T2" not in by_label:
        print("[CMP] %s 에 필요한 라벨이 없다. 보유: %s" % (ymd, sorted(by_label)))
        return 3

    t1, t2 = by_label["T1"], by_label["T2"]
    m1 = {r["code"]: (r.get("output") or {}) for r in t1["rows"] if r.get("ok")}
    m2 = {r["code"]: (r.get("output") or {}) for r in t2["rows"] if r.get("ok")}
    print("=" * 78)
    print(" 애프터마켓 대조  %s   T1 %s  ->  T2 %s" % (ymd, t1.get("hhmm"), t2.get("hhmm")))
    print("=" * 78)
    changed_any = False
    for c in sorted(set(m1) & set(m2)):
        diffs = []
        for k in WATCH:
            a, b = str(m1[c].get(k, "")), str(m2[c].get(k, ""))
            if a != b:
                diffs.append("%s %s->%s" % (k, a, b))
        if diffs:
            changed_any = True
            print("  %s  %s" % (c, " | ".join(diffs)))
        else:
            print("  %s  변화 없음" % c)
    missing = sorted((set(m1) | set(m2)) - (set(m1) & set(m2)))
    if missing:
        print("  [조회 실패로 대조 불가] %s" % ", ".join(missing))
    print("-" * 78)
    if changed_any:
        print(" 판정: **애프터마켓 체결이 일봉 누적값에 섞인다.**")
        print("       -> value(종가x거래량) 임계값의 의미가 09-14 부터 달라진다")
        print("       -> 유동성 게이트(value_floor_20e 등)와 동결 라운드를 재검토해야 한다")
    else:
        print(" 판정: 일봉 누적값은 정규장만 반영한다 (애프터마켓 미반영)")
        print("       -> 게이트 임계값은 그대로 유효하다")
    return 0


def compare_flow(ledger: Path, target_ymd: str) -> int:
    """같은 날짜의 수급 행이 **기록 시점 사이에 바뀌었는가.**

    16:30 수집이 09-14 부터 애프터마켓 장중이다. 그때 찍힌 값이 확정 전이면
    다음날 재수집에서 바뀐다. 바뀌면 동결 라운드 RD_20260831_flow_h10 의
    원자료 정의가 09-14 전후로 갈린다.
    """
    if not ledger.is_file():
        print("[FLOW] 원장 없음: %s" % ledger)
        return 2
    snaps = []
    with ledger.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            f = d.get("flow") or {}
            if str(f.get("target_ymd")) == target_ymd:
                snaps.append((d.get("ts"), d.get("label"), f))
    if len(snaps) < 2:
        print("[FLOW] %s 스냅샷이 %d건이다. 최소 2건(다른 날) 필요" % (target_ymd, len(snaps)))
        for ts, lb, f in snaps:
            print("   %s %s status=%s rows=%s" % (ts, lb, f.get("status"), f.get("rows")))
        return 3

    print("=" * 78)
    print(" 수급 원장 대조  대상일 %s   스냅샷 %d건" % (target_ymd, len(snaps)))
    print("=" * 78)
    for ts, lb, f in snaps:
        print("  %s %-6s status=%-8s rows=%-6s codes=%-5s fetched_at_max=%s"
              % (ts, lb, f.get("status"), f.get("rows"), f.get("codes"), f.get("fetched_at_max")))
    ok = [(ts, lb, f) for ts, lb, f in snaps if f.get("status") == "OK"]
    if len(ok) < 2:
        print("-" * 78)
        print(" 판정 불가: OK 스냅샷이 2건 미만이다 (아직 수집 전일 수 있다)")
        return 3
    first, last = ok[0][2], ok[-1][2]
    diffs = []
    if first.get("rows") != last.get("rows"):
        diffs.append("rows %s->%s" % (first.get("rows"), last.get("rows")))
    for k in FLOW_SUM_COLS:
        a, b = (first.get("sums") or {}).get(k), (last.get("sums") or {}).get(k)
        if a is not None and b is not None and a != b:
            diffs.append("%s %.0f->%.0f" % (k, a, b))
    print("-" * 78)
    if diffs:
        print(" 변화: %s" % " | ".join(diffs))
        print(" 판정: **16:30 수집은 확정 전 스냅샷이다.**")
        print("       -> 동결 라운드 RD_20260831_flow_h10 의 원자료가 09-14 전후로 갈린다")
        print("       -> VIBE_Investor_Flow_Daily 를 애프터마켓 종료 후로 옮겨야 한다")
    else:
        print(" 변화 없음")
        print(" 판정: 16:30 수집값이 최종값과 같다 -> 시간표를 옮길 필요가 없다")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="", help="T1(정규장 직후) 또는 T2(애프터마켓 후)")
    ap.add_argument("--codes", default="", help="쉼표 구분. 비우면 기본 8종목")
    ap.add_argument("--prod", action="store_true", help="실계좌 자격증명 사용 (기본 모의)")
    ap.add_argument("--compare", action="store_true")
    ap.add_argument("--date", default=dt.datetime.now().strftime("%Y%m%d"))
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    ap.add_argument("--flow-date", default="", help="수급 스냅샷 대상일 (기본: 오늘)")
    ap.add_argument("--compare-flow", action="store_true",
                    help="--date 의 수급 행이 기록 간에 바뀌었는지 대조")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ledger = out_dir / LEDGER_NAME          # **out_dir 를 따른다**

    if args.compare_flow:
        return compare_flow(ledger, args.date.strip())
    if args.compare:
        return compare(ledger, args.date.strip())

    label = args.label.strip()
    if not label:
        hhmm = int(dt.datetime.now().strftime("%H%M"))
        label = "T1" if hhmm < 1800 else "T2"
        print("[LABEL] 시각으로 추정: %s" % label)

    codes = [c.strip().zfill(6) for c in args.codes.split(",") if c.strip()] or DEFAULT_CODES
    rec = collect(codes, label, mock=not args.prod, flow_ymd=args.flow_date.strip())
    with ledger.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + chr(10))

    print("[PROBE] %s %s  라벨=%s  조회 %d/%d 성공  -> %s"
          % (rec["ymd"], rec["hhmm"], label, rec["ok_count"], rec["codes"], ledger))
    for r in rec["rows"]:
        if not r["ok"]:
            print("   [FAIL] %s %s" % (r["code"], r.get("error")))
            continue
        o = r["output"]
        print("   %s 현재가 %s 누적거래량 %s 누적거래대금 %s"
              % (r["code"], o.get("stck_prpr"), o.get("acml_vol"), o.get("acml_tr_pbmn")))
    if rec["ok_count"] == 0:
        print("[WARN] 전부 실패했다. 수집으로 세지 말 것")
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
