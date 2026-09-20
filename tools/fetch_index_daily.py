# -*- coding: utf-8 -*-
"""국내 지수 일별 시세를 받아 이력으로 쌓는다.

2026-08-26 신규. 사용자가 "코스피 6868.65" 라고 했는데 **내가 그게 몇 %인지 확인할 수
없었다.** 시스템에 코스피 지수가 없다.

```
Buffett-WS-Index-Intraday   2026-07-09 rc=-1 실패 후 Disabled (사유 미확인, PLANS 에 열린 항목)
실시간 피드                   코스닥(002001)만 흘러온다
_cache / 2_Logs              코스피 지수 이력 파일 없음
```

성과를 지수와 대보려면 이게 있어야 한다. WS(실시간)는 07-09 이후 고장 원인이
규명 안 됐으므로 **REST 일별 조회**로 간다 - 하루 한 번이면 충분하고 훨씬 단순하다.

`KISOrderClient` 는 **인증에만** 쓴다. 주문 클라이언트에 메서드를 추가하지 않는다 -
그 파일은 실주문 경로다.

    python tools/fetch_index_daily.py            # 최근 30일 갱신
    python tools/fetch_index_daily.py --days 400 # 초기 적재
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.kis_order_client import KISOrderClient      # noqa: E402

OUT = ROOT / "2_Logs" / "index_daily_history.csv"
FIELDS = ["date", "index_code", "index_name", "close", "open", "high", "low", "volume", "fetched_at"]

# 국내 업종/지수 코드. KIS 는 지수에 시장구분 "U" 를 쓴다.
# [2026-08-27 정정] 2001 은 KOSDAQ 이 아니라 **KOSPI200** 이다.
#   API 의 hts_kor_isnm 으로 확인: 0001=종합 / 1001=KOSDAQ / 2001=KOSPI200
#   / 2203=KSQ150 / 4001=KRX100.
#   08-26~27 에 내가 "KOSDAQ" 이라고 보고한 값은 전부 KOSPI200 이었다.
INDEXES = {
    "0001": "KOSPI",
    "1001": "KOSDAQ",
    "2001": "KOSPI200",
}


def fetch(client: KISOrderClient, code: str, d1: str, d2: str) -> list[dict]:
    params = {
        "FID_COND_MRKT_DIV_CODE": "U",
        "FID_INPUT_ISCD": code,
        "FID_INPUT_DATE_1": d1,
        "FID_INPUT_DATE_2": d2,
        "FID_PERIOD_DIV_CODE": "D",
    }
    headers = client._auth_headers(tr_id="FHKUP03500100")
    body, _ = client._request_json(
        "GET",
        "/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice",
        headers=headers,
        params=params,
    )
    rt = str(body.get("rt_cd", "")).strip()
    if rt and rt != "0":
        raise RuntimeError("지수 조회 거부 rt_cd=%s msg1=%s" % (rt, body.get("msg1")))
    rows = body.get("output2") or []
    now = dt.datetime.now().isoformat(timespec="seconds")
    out = []
    for r in rows:
        ymd = str(r.get("stck_bsop_date") or "").strip()
        px = str(r.get("bstp_nmix_prpr") or "").strip()
        if not ymd or not px:
            continue
        out.append({
            "date": ymd, "index_code": code, "index_name": INDEXES.get(code, code),
            "close": px,
            "open": str(r.get("bstp_nmix_oprc") or ""),
            "high": str(r.get("bstp_nmix_hgpr") or ""),
            "low": str(r.get("bstp_nmix_lwpr") or ""),
            "volume": str(r.get("acml_vol") or ""),
            "fetched_at": now,
        })
    return out


def merge(new_rows: list[dict]) -> int:
    old = {}
    if OUT.exists():
        with OUT.open(encoding="utf-8-sig") as fh:
            for r in csv.DictReader(fh):
                old[(r["date"], r["index_code"])] = r
    added = 0
    for r in new_rows:
        k = (r["date"], r["index_code"])
        if k not in old:
            added += 1
        old[k] = r            # 최신 조회로 덮는다(정정 반영)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        for k in sorted(old):
            w.writerow({c: old[k].get(c, "") for c in FIELDS})
    return added


def main() -> int:
    ap = argparse.ArgumentParser(description="국내 지수 일별 시세 적재")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--codes", default=",".join(INDEXES))
    ap.add_argument("--page-days", type=int, default=70,
                    help="한 번에 요청할 달력일 수. KIS 가 호출당 50행 상한이라 70일이면 대략 채워진다")
    ap.add_argument("--max-calls", type=int, default=80, help="지수당 최대 호출 수(안전장치)")
    args = ap.parse_args()

    d2 = dt.date.today()
    d1 = d2 - dt.timedelta(days=args.days)
    client = KISOrderClient.from_env(mock=False)

    # [2026-08-27] 페이지네이션. KIS 지수 일별 조회는 **호출당 최대 50행**이라
    #   --days 400 을 줘도 50행만 왔다(2026-08-26 실측). 11.4년을 채우려면 뒤에서부터
    #   창을 옮겨가며 여러 번 부른다. 같은 날짜가 겹쳐 오면 merge 가 덮으므로 안전하다.
    allrows: list[dict] = []
    for code in [c.strip() for c in args.codes.split(",") if c.strip()]:
        got: list[dict] = []
        cur_end = d2
        calls = 0
        while cur_end > d1 and calls < args.max_calls:
            cur_start = max(d1, cur_end - dt.timedelta(days=args.page_days))
            rows = fetch(client, code, cur_start.strftime("%Y%m%d"), cur_end.strftime("%Y%m%d"))
            calls += 1
            if not rows:
                break
            got += rows
            oldest = min(r["date"] for r in rows)
            nxt = dt.datetime.strptime(oldest, "%Y%m%d").date() - dt.timedelta(days=1)
            if nxt >= cur_end:      # 더 이상 뒤로 못 가면 멈춘다
                break
            cur_end = nxt
        uniq = {r["date"]: r for r in got}
        got = [uniq[k] for k in sorted(uniq)]
        print("[INDEX] %-6s %s  %d행 (%d회 호출)  %s ~ %s"
              % (INDEXES.get(code, code), code, len(got), calls,
                 got[0]["date"] if got else "-", got[-1]["date"] if got else "-"))
        allrows += got

    added = merge(allrows)
    print("  wrote %s  (신규 %d행)" % (OUT, added))

    # 최근 2거래일 변화를 바로 보여준다
    with OUT.open(encoding="utf-8-sig") as fh:
        data = list(csv.DictReader(fh))
    for code, name in INDEXES.items():
        s = [r for r in data if r["index_code"] == code]
        if len(s) >= 2:
            a, b = float(s[-2]["close"]), float(s[-1]["close"])
            print("  %-6s %s %.2f -> %s %.2f   %+.2f%%"
                  % (name, s[-2]["date"], a, s[-1]["date"], b, 100 * (b / a - 1)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
