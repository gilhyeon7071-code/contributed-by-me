# -*- coding: utf-8 -*-
"""종목별 투자자 순매수(개인·외국인·기관)를 받아 이력으로 쌓는다.

2026-08-28 신규. 배경은 `.agent/PLANS.md` (136)~(139).

## 왜 만드나

오늘 하루 94개 조합을 훑었는데 **일별 종가·거래대금으로 만든 축은 전부 지수에 졌다**
(최고 샤프 0.53 vs KOSPI 0.61). 그 층에 남은 것이 없다는 결론이고, 남은 길은 재료를 바꾸는 것이다.

가격에 없는 축 중 가장 유망한 것이 **투자자별 수급**이다 - 일별·전 종목이라 검정력이 나오고,
한국 시장 특유의 효과로 보고돼 왔다. 그런데 **역사 데이터를 구할 수 없다**:

```
pykrx                  차단됨. KRX 정보데이터시스템이 프로그램 접근을 막는다
                       (tools/build_fundamental_from_krx_manual.py 헤더 참조)
KRX 웹                 수동 다운로드만 가능. 11년치는 비현실적
KIS FHKST01010900      **된다. 단 한 번에 최근 30거래일뿐이고 시작일 지정 불가**
```

그래서 이 배치가 하는 일은 단순하다 - **매일 30일치를 받아 겹치는 부분을 병합해 쌓는다.**
하루만 돌려도 30일치가 생기고, 그 뒤로 매일 1일씩 는다. 1년이면 250일 x 유니버스 규모의
관측이 모이고, 그때 비로소 수급 축을 오늘 vol60 을 쟀던 방식으로 검정할 수 있다.

**오늘의 답은 아니다. 1년 뒤에 이 벽이 없어지게 하는 것이 목적이다.**

## 설계

- `KISOrderClient` 는 **인증에만** 쓴다. 주문 클라이언트에 메서드를 추가하지 않는다
  (그 파일은 실주문 경로다. `tools/fetch_index_daily.py` 와 같은 방침)
- 월별 parquet 샤드에 쌓고 `(date, code)` 중복은 최신 수집분을 남긴다 - 재실행이 안전하다
- 종목 100개마다 중간 저장한다. 중간에 죽어도 그때까지가 남는다
- 이미 오늘 받은 종목은 건너뛴다(`--resume`, 기본 켜짐)

    python tools/fetch_investor_flow.py                 # 유동성 하한 이상 전 종목
    python tools/fetch_investor_flow.py --limit 50      # 시험용
    python tools/fetch_investor_flow.py --min-value 0   # 전 상장종목
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.kis_order_client import KISOrderClient      # noqa: E402

OUTDIR = ROOT / "_cache" / "investor_flow"
STATUS = ROOT / "2_Logs" / "investor_flow_status_latest.json"
TR_ID = "FHKST01010900"
PATH = "/uapi/domestic-stock/v1/quotations/inquire-investor"

# API 응답 키 -> 저장 컬럼
FIELDS = {
    "stck_bsop_date": "date",
    "stck_clpr": "close",
    "prsn_ntby_qty": "indiv_qty",
    "frgn_ntby_qty": "forgn_qty",
    "orgn_ntby_qty": "inst_qty",
    "prsn_ntby_tr_pbmn": "indiv_amt",
    "frgn_ntby_tr_pbmn": "forgn_amt",
    "orgn_ntby_tr_pbmn": "inst_amt",
}


def universe(min_value: float, limit: int) -> list[str]:
    """최근 아카이브 마지막 거래일에서 거래대금 하한을 넘는 종목."""
    files = sorted(glob.glob(str(ROOT / "krx_daily_archive" / "krx_daily_*_clean.parquet")))
    if not files:
        raise RuntimeError("krx_daily_archive 에서 읽을 파일이 없다")
    frames = []
    for f in files[-6:]:                       # 최근 샤드만 본다(가볍게)
        try:
            frames.append(pd.read_parquet(f, columns=["date", "code", "value"]))
        except Exception:
            continue
    d = pd.concat(frames, ignore_index=True)
    d["date"] = d["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    d["code"] = d["code"].astype(str).str.zfill(6)
    last = d["date"].max()
    day = d[d["date"] == last]
    if min_value > 0:
        day = day[pd.to_numeric(day["value"], errors="coerce").fillna(0) >= min_value]
    codes = sorted(day["code"].unique())
    print("[UNIV] 기준일 %s  종목 %d개 (min_value=%.0f)" % (last, len(codes), min_value))
    return codes[:limit] if limit else codes


def fetch_one(cli: KISOrderClient, code: str) -> list[dict]:
    params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
    headers = cli._auth_headers(tr_id=TR_ID)
    body, _ = cli._request_json("GET", PATH, headers=headers, params=params)
    rt = str(body.get("rt_cd", "")).strip()
    if rt and rt != "0":
        raise RuntimeError("rt_cd=%s msg1=%s" % (rt, body.get("msg1")))
    now = dt.datetime.now().isoformat(timespec="seconds")
    out = []
    for r in body.get("output") or []:
        ymd = str(r.get("stck_bsop_date") or "").strip()
        if not ymd:
            continue
        row = {"code": code, "fetched_at": now}
        for k, v in FIELDS.items():
            row[v] = str(r.get(k) or "").strip()
        out.append(row)
    return out


def save(rows: list[dict]) -> int:
    """월별 샤드에 병합. (date, code) 중복은 최신 수집분을 남긴다."""
    if not rows:
        return 0
    OUTDIR.mkdir(parents=True, exist_ok=True)
    new = pd.DataFrame(rows)
    new["ym"] = new["date"].str[:6]
    total = 0
    for ym, part in new.groupby("ym"):
        p = OUTDIR / ("investor_flow_%s.parquet" % ym)
        part = part.drop(columns=["ym"])
        if p.exists():
            old = pd.read_parquet(p)
            part = pd.concat([old, part], ignore_index=True)
        part = part.sort_values("fetched_at").drop_duplicates(subset=["date", "code"], keep="last")
        part.to_parquet(p, index=False)
        total += len(part)
    return total


def already_today() -> set[str]:
    """오늘 이미 받은 종목(재실행 시 건너뛰기용)."""
    today = dt.date.today().isoformat()
    done = set()
    for p in sorted(OUTDIR.glob("investor_flow_*.parquet")):
        try:
            df = pd.read_parquet(p, columns=["code", "fetched_at"])
        except Exception:
            continue
        done |= set(df.loc[df["fetched_at"].astype(str).str[:10] == today, "code"].unique())
    return done


def main() -> int:
    ap = argparse.ArgumentParser(description="종목별 투자자 순매수 수집")
    ap.add_argument("--min-value", type=float, default=1e9, help="유니버스 거래대금 하한. 0=전 상장종목")
    ap.add_argument("--limit", type=int, default=0, help="종목 수 상한(시험용). 0=제한없음")
    ap.add_argument("--no-resume", action="store_true", help="오늘 받은 것도 다시 받는다")
    ap.add_argument("--chunk", type=int, default=100, help="중간 저장 간격")
    args = ap.parse_args()

    codes = universe(args.min_value, args.limit)
    skip = set() if args.no_resume else already_today()
    todo = [c for c in codes if c not in skip]
    print("[PLAN] 대상 %d  이미받음 %d  실행 %d" % (len(codes), len(codes) - len(todo), len(todo)))
    if not todo:
        print("[DONE] 오늘 받을 것이 없다.")
        return 0

    cli = KISOrderClient.from_env(mock=True)   # 조회 전용. 주문 안 한다
    t0 = time.time()
    buf, ok, fail, rows_total = [], 0, 0, 0
    errs: dict[str, int] = {}
    for i, code in enumerate(todo, 1):
        try:
            got = fetch_one(cli, code)
            buf.extend(got)
            ok += 1
        except Exception as e:
            fail += 1
            k = type(e).__name__
            errs[k] = errs.get(k, 0) + 1
        if len(buf) >= args.chunk * 25 or i == len(todo):
            rows_total = save(buf)
            buf = []
            print("  %d/%d  ok=%d fail=%d  누적 %s행  %.0fs"
                  % (i, len(todo), ok, fail, format(rows_total, ","), time.time() - t0))

    files = sorted(OUTDIR.glob("investor_flow_*.parquet"))
    tot = sum(len(pd.read_parquet(p, columns=["code"])) for p in files)
    span = ""
    if files:
        a = pd.read_parquet(files[0], columns=["date"])["date"].min()
        b = pd.read_parquet(files[-1], columns=["date"])["date"].max()
        span = "%s ~ %s" % (a, b)
    st = {"ts": dt.datetime.now().isoformat(timespec="seconds"), "codes": len(todo),
          "ok": ok, "fail": fail, "errors": errs, "rows_total": tot, "span": span,
          "elapsed_sec": round(time.time() - t0, 1), "shards": len(files)}
    STATUS.parent.mkdir(parents=True, exist_ok=True)
    STATUS.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[DONE] ok=%d fail=%d  누적 %s행  기간 %s  %.0fs"
          % (ok, fail, format(tot, ","), span, time.time() - t0))
    if errs:
        print("       오류: %s" % errs)
    # 절반 이상 실패면 배치 실패로 알린다
    return 1 if ok == 0 or fail > ok else 0


if __name__ == "__main__":
    sys.exit(main())
