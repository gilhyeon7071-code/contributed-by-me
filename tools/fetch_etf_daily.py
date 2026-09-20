# -*- coding: utf-8 -*-
"""지수 ETF 일봉 수집 (KIS FHKST03010100).

왜 필요한가
    2026-08-31 확인: `krx_daily_archive` 는 보통주만 담아 **ETF 가 가격 패널에 한 종목도 없다.**
    94조합·Track B 재측정이 모두 "지수를 이기지 못한다"로 수렴했는데,
    정작 지수를 사겠다고 결정해도 값을 붙일 수 없다 - 백테스트도 포지션 산정도 원장도 막힌다.
    KIS 조회는 정상 동작하므로(rt_cd=0) 여기서 받아 채운다.

설계
    `KISOrderClient` 는 **인증에만** 쓴다(fetch_investor_flow.py 와 같은 방침).
    한 번 호출에 100행 상한이므로 과거는 날짜창을 뒤로 밀며 페이지네이션한다.
    저장은 월별 샤드 + (date,code) 중복 제거로 **누적**한다. 재실행 안전.

한계 (반드시 함께 인용)
    - `FID_ORG_ADJ_PRC=0` 으로 받는다. ETF 분배금은 가격에 반영되지 않으므로
      총수익(total return)이 아니라 **가격수익**이다. 지수와 비교할 때 이 차이를 감안할 것
    - 장중에 돌리면 그 날 행은 미완성(시가=고가=저가=종가)이다. 장 마감 후 재수집하면 덮인다

사용
    python tools/fetch_etf_daily.py                       증분(최근 100거래일)
    python tools/fetch_etf_daily.py --backfill-from 20150101   과거 소급
    python tools/fetch_etf_daily.py --show
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.kis_order_client import KISOrderClient      # noqa: E402

OUTDIR = ROOT / "_cache" / "etf_daily"
STATUS = ROOT / "2_Logs" / "etf_daily_status_latest.json"
TR_ID = "FHKST03010100"
PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"

# 기본 대상. 지수 노출을 만들 수 있는 최소 집합.
DEFAULT_ETFS = {
    "069500": "KODEX 200",
    "226490": "KODEX 코스피",
    "102110": "TIGER 200",
    "229200": "KODEX 코스닥150",
    "232080": "TIGER 코스닥150",
}

FIELDS = {
    "stck_bsop_date": "date",
    "stck_oprc": "open",
    "stck_hgpr": "high",
    "stck_lwpr": "low",
    "stck_clpr": "close",
    "acml_vol": "volume",
    "acml_tr_pbmn": "value",
}


def fetch_window(cli: KISOrderClient, code: str, d1: str, d2: str) -> list[dict]:
    headers = cli._auth_headers(tr_id=TR_ID)
    params = {
        "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
        "FID_INPUT_DATE_1": d1, "FID_INPUT_DATE_2": d2,
        "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0",
    }
    body, _ = cli._request_json("GET", PATH, headers=headers, params=params)
    rt = str(body.get("rt_cd", "")).strip()
    if rt and rt != "0":
        raise RuntimeError("rt_cd=%s msg1=%s" % (rt, body.get("msg1")))
    now = dt.datetime.now().isoformat(timespec="seconds")
    out = []
    for r in body.get("output2") or []:
        ymd = str(r.get("stck_bsop_date") or "").strip()
        if not ymd:
            continue
        row = {"code": code, "fetched_at": now}
        for k, v in FIELDS.items():
            row[v] = str(r.get(k) or "").strip()
        out.append(row)
    return out


def save(rows: list[dict]) -> int:
    if not rows:
        return 0
    OUTDIR.mkdir(parents=True, exist_ok=True)
    new = pd.DataFrame(rows)
    new["ym"] = new["date"].astype(str).str[:6]
    total = 0
    for ym, part in new.groupby("ym"):
        p = OUTDIR / ("etf_daily_%s.parquet" % ym)
        part = part.drop(columns=["ym"])
        if p.exists():
            part = pd.concat([pd.read_parquet(p), part], ignore_index=True)
        part = part.sort_values("fetched_at").drop_duplicates(subset=["date", "code"], keep="last")
        part.to_parquet(p, index=False)
        total += len(part)
    return total


def _summary() -> dict:
    files = sorted(OUTDIR.glob("etf_daily_*.parquet"))
    if not files:
        return {"rows_total": 0, "span": "", "shards": 0, "per_code": {}}
    d = pd.concat([pd.read_parquet(f, columns=["date", "code"]) for f in files], ignore_index=True)
    return {"rows_total": int(len(d)), "span": "%s ~ %s" % (d["date"].min(), d["date"].max()),
            "shards": len(files),
            "per_code": {c: int(n) for c, n in d.groupby("code").size().items()}}


def main() -> int:
    ap = argparse.ArgumentParser(description="지수 ETF 일봉 수집")
    ap.add_argument("--codes", default="", help="쉼표구분. 비우면 기본 집합")
    ap.add_argument("--backfill-from", default="", help="YYYYMMDD 까지 과거로 소급")
    ap.add_argument("--show", action="store_true", help="누적 현황만 출력")
    args = ap.parse_args()

    if args.show:
        print(json.dumps(_summary(), ensure_ascii=False, indent=2))
        return 0

    codes = [c.strip() for c in args.codes.split(",") if c.strip()] or list(DEFAULT_ETFS)
    today = dt.date.today().strftime("%Y%m%d")
    cli = KISOrderClient.from_env(mock=True)   # 조회 전용. 주문 안 한다
    t0 = time.time()
    ok = fail = 0
    errs: dict[str, int] = {}

    for code in codes:
        name = DEFAULT_ETFS.get(code, code)
        end = today
        got_code = 0
        while True:
            try:
                rows = fetch_window(cli, code, args.backfill_from or "20000101", end)
            except Exception as e:
                fail += 1
                errs[type(e).__name__] = errs.get(type(e).__name__, 0) + 1
                break
            if not rows:
                break
            save(rows)
            got_code += len(rows)
            ok += 1
            oldest = min(r["date"] for r in rows)
            if not args.backfill_from or oldest <= args.backfill_from or len(rows) < 100:
                break
            # 한 칸 더 과거로. 가장 오래된 날의 하루 전까지.
            end = (dt.datetime.strptime(oldest, "%Y%m%d") - dt.timedelta(days=1)).strftime("%Y%m%d")
            time.sleep(0.2)
        print("  %-8s %-16s %d행 수집  (~%s)" % (code, name, got_code, end))

    s = _summary()
    st = {"ts": dt.datetime.now().isoformat(timespec="seconds"), "codes": len(codes),
          "calls_ok": ok, "calls_fail": fail, "errors": errs,
          "adj_price_mode": "FID_ORG_ADJ_PRC=0 (분배금 미반영, 가격수익)",
          "elapsed_sec": round(time.time() - t0, 1), **s}
    STATUS.parent.mkdir(parents=True, exist_ok=True)
    STATUS.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[DONE] 호출 ok=%d fail=%d  누적 %s행  기간 %s  %.0fs"
          % (ok, fail, format(s["rows_total"], ","), s["span"], time.time() - t0))
    if errs:
        print("       오류: %s" % errs)
    return 1 if (fail and ok == 0) else 0


if __name__ == "__main__":
    sys.exit(main())
