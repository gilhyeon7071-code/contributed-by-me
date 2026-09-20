"""H1 수집기 4 — 사건 종목의 **수정주가** 일별 시세를 받아 쌓는다. 읽기 전용 조회, 주문 없음.

    python -m paper.strategies.kospi_mcap_quarterly_v2.src.fetch_adjusted_prices --events-dir <폴더> [--pre 15 --post 30]

왜 필요한가 (2026-09-18 실측)
  `krx_daily_archive` 는 **원주가와 수정주가가 섞여 있다.** 비비안(002070) 2026-07-29 종가가
  파일에 따라 8,350원(원주가)과 4,180원(수정주가)으로 나온다. 한 계열 안에서 기준이 바뀌면
  없는 −50% 가 생긴다. 그래서 사건 분석용 가격은 **한 곳에서 한 기준으로** 받는다.

어디서 (2026-09-18 세 출처 대조 결과 **pykrx 가 기본**)
  pykrx  `stock.get_market_ohlcv(..., adjusted=True)` — KRX 자료, 빠르다(종목당 <1초)
  한투   `/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice` (`FID_ORG_ADJ_PRC=0`=수정주가)
         — **모의 서버 값이 최근 주에 조금씩 다르다**(002020 09-14 24,900 vs 24,800). 종목당 10~180초로 느리다
  대조:  pykrx = krx_daily_archive 완전 일치, 한투만 최근 주 6일 어긋남 → 한투는 대조용으로만

기록: `prices_adj.parquet` (code·date 로 중복 제거, 나중에 다시 받으면 최신으로 갱신)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
TR_ID = "FHKST03010100"
API_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
FIELDS = {"stck_bsop_date": "date", "stck_oprc": "open", "stck_hgpr": "high", "stck_lwpr": "low",
          "stck_clpr": "close", "acml_vol": "volume"}
CHUNK_DAYS = 120


def fetch_window_pykrx(code: str, d1: str, d2: str) -> List[Dict[str, Any]]:
    """KRX 수정주가. 실패하면 예외를 올린다 — 빈 목록으로 조용히 넘어가지 않는다."""
    from pykrx import stock as _stock
    df = _stock.get_market_ohlcv(d1, d2, code, adjusted=True)
    if df is None or len(df) == 0:
        return []
    out = []
    for idx, r in df.iterrows():
        out.append({"code": code, "date": idx.strftime("%Y%m%d"), "open": float(r["시가"]), "high": float(r["고가"]),
                    "low": float(r["저가"]), "close": float(r["종가"]), "volume": float(r["거래량"])})
    return [r for r in out if r["close"] > 0]


def fetch_window(client: Any, code: str, d1: str, d2: str) -> List[Dict[str, Any]]:
    headers = client._auth_headers(tr_id=TR_ID)
    body, _ = client._request_json("GET", API_PATH, headers=headers, params={
        "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": d1,
        "FID_INPUT_DATE_2": d2, "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"})
    rt = str(body.get("rt_cd", "")).strip()
    if rt and rt != "0":
        raise RuntimeError(f"rt_cd={rt} msg1={body.get('msg1')}")
    rows = []
    for r in body.get("output2") or []:
        if not str(r.get("stck_bsop_date") or "").strip():
            continue
        row = {"code": code}
        for k, v in FIELDS.items():
            raw = str(r.get(k) or "").strip()
            row[v] = float(raw) if raw not in ("", "-") else None
        row["date"] = str(r.get("stck_bsop_date")).strip()
        rows.append(row)
    return rows


def _chunks(d1: str, d2: str) -> List[Tuple[str, str]]:
    a = datetime.strptime(d1, "%Y%m%d")
    b = datetime.strptime(d2, "%Y%m%d")
    out = []
    while a <= b:
        end = min(a + timedelta(days=CHUNK_DAYS), b)
        out.append((a.strftime("%Y%m%d"), end.strftime("%Y%m%d")))
        a = end + timedelta(days=1)
    return out


def _save(out_path: Path, frames: List[pd.DataFrame], now: datetime) -> int:
    if not frames:
        return 0
    old = pd.read_parquet(out_path) if Path(out_path).exists() else None
    new = pd.concat(frames, ignore_index=True)
    new["fetched_at"] = now.isoformat(timespec="seconds")
    merged = pd.concat([old, new], ignore_index=True) if old is not None and len(old) else new
    merged = merged.sort_values("fetched_at").drop_duplicates(["code", "date"], keep="last")
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(out_path, index=False)
    return int(len(merged))


def already_covered(out_path: Path, d1: str, d2: str, min_days: int = 5) -> set:
    """이미 그 구간 시세가 있는 종목은 건너뛴다 — 중간에 끊겨도 이어서 받게."""
    if not Path(out_path).exists():
        return set()
    df = pd.read_parquet(out_path, columns=["code", "date"])
    df = df[(df["date"] >= d1) & (df["date"] <= d2)]
    cnt = df.groupby("code")["date"].nunique()
    return set(cnt[cnt >= min_days].index.astype(str))


def run(client: Any, codes: Iterable[str], d1: str, d2: str, out_path: Path, now: datetime,
        sleep: float = 0.15, save_every: int = 20, resume: bool = True, source: str = "pykrx") -> Dict[str, Any]:
    """느린 서버에서 끊겨도 진도가 남게 **20종목마다 저장**하고, 다시 돌리면 남은 것만 받는다."""
    out_path = Path(out_path)
    todo = sorted(set(codes))
    done = already_covered(out_path, d1, d2) if resume else set()
    todo = [c for c in todo if c not in done]
    res = {"run_at": now.isoformat(timespec="seconds"), "skipped_existing": len(done), "codes": 0, "rows": 0,
           "failed": [], "status": "OK"}
    frames: List[pd.DataFrame] = []
    for i, code in enumerate(todo, 1):
        res["codes"] += 1
        for a, b in _chunks(d1, d2):
            try:
                rows = fetch_window_pykrx(code, a, b) if source == "pykrx" else fetch_window(client, code, a, b)
            except Exception as e:
                res["failed"].append({"code": code, "range": [a, b], "error": f"{type(e).__name__}:{e}"})
                continue
            if rows:
                frames.append(pd.DataFrame(rows))
                res["rows"] += len(rows)
            time.sleep(sleep)
        if i % save_every == 0:
            res["stored_rows"] = _save(out_path, frames, now)
            frames = []
    res["stored_rows"] = _save(out_path, frames, now) or res.get("stored_rows", 0)
    if res["failed"]:
        res["status"] = "WARN"
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-dir", required=True, type=Path)
    ap.add_argument("--types", default="BUYBACK_DIRECT,BUYBACK_TRUST,BUYBACK_DISPOSAL,RIGHTS_OFFERING")
    ap.add_argument("--source", choices=["pykrx", "kis"], default="pykrx")
    ap.add_argument("--out-name", default="prices_adj.parquet")
    ap.add_argument("--codes-file", type=Path, help="control_set.json 처럼 종목 목록이 든 파일(사건 대신 이 목록을 받는다)")
    ap.add_argument("--codes-key", default="controls")
    ap.add_argument("--pre-days", type=int, default=20)
    ap.add_argument("--post-days", type=int, default=45)
    ap.add_argument("--events-file", default="h1_events.jsonl")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    events = [json.loads(x) for x in (args.events_dir / args.events_file).read_text(encoding="utf-8").splitlines() if x.strip()]
    types = {t.strip() for t in args.types.split(",") if t.strip()}
    events = [e for e in events if e["event_type"] in types and e["stock_code"]]
    if not events:
        print(json.dumps({"status": "STOP", "reason": "NO_EVENTS"}, ensure_ascii=False))
        return 3
    # 날짜 구간은 사건 기준으로 잡는다(대조군도 같은 구간이어야 비교가 된다)
    d1 = (datetime.strptime(min(e["rcept_dt"] for e in events), "%Y%m%d") - timedelta(days=args.pre_days)).strftime("%Y%m%d")
    d2 = (datetime.strptime(max(e["rcept_dt"] for e in events), "%Y%m%d") + timedelta(days=args.post_days)).strftime("%Y%m%d")
    if args.codes_file:
        codes = json.loads(args.codes_file.read_text(encoding="utf-8"))[args.codes_key]
    else:
        codes = [e["stock_code"] for e in events]
    client = None
    if args.source == "kis":
        from paper.strategies.kospi_mcap_quarterly_v2.src.kis_adapter import make_client
        client = make_client()
    res = run(client, codes, d1, d2, args.events_dir / args.out_name, datetime.now(),
              sleep=0.0 if args.source == "pykrx" else 0.15, source=args.source)
    print(json.dumps({**res, "range": [d1, d2], "failed": res["failed"][:5],
                      "failed_count": len(res["failed"])}, ensure_ascii=False, indent=2))
    return 0 if res["status"] == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
