# -*- coding: utf-8 -*-
"""KIS 기간별매매손익 조회 -> 응답 원본 저장 + 알려진 정답으로 필드 매핑 자동 탐색.

왜 필요한가 (2026-08-22, PLANS 56):
  이 시스템의 비용 상수는 실매매를 해 본 적이 없어서 추측으로 박혔고, 두 곳이 서로 다르다.
      paper_engine_config.json   수수료 0.004%  거래세 0.15%
      paper_fills_ledger.csv     수수료 0.02%   거래세 0.20%
  실계좌 왕복 1건(매수 71,950 / 매도 72,350 / 실현손익 256 / 제세금 144)으로 역산하니
  **수수료 0% / 제세금 0.20%** 였다. 즉 둘 다 틀렸고 방향도 반대였다.

  일별주문체결조회(TTTC0081R)는 수량과 가격만 준다. 청구액을 주는 것은 이 엔드포인트뿐이다.

이 도구가 하는 일
  1. 조회해서 **응답 원본을 그대로** 저장한다 (가공하지 않는다)
  2. `--expect` 로 준 알려진 값들이 응답의 어느 키에 있는지 **자동으로 찾는다**
     KIS 필드명을 추측해서 박지 않기 위해서다. 정답을 아는 거래가 fixture 역할을 한다
  3. 찾은 매핑을 보고서로 남긴다. 배선은 그걸 사람이 확인한 뒤에 한다

읽기 전용이다. 주문을 내지 않는다.

사용
  python tools/kis_probe_period_trade_profit.py --start 20260801 --end 20260822
  python tools/kis_probe_period_trade_profit.py --start ... --end ... --expect 144 256 71950 72350
  python tools/kis_probe_period_trade_profit.py --start ... --end ... --mock     (모의계좌로 시도)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

OUT_DIR = ROOT / "2_Logs"


def _num(value: Any):
    txt = str(value if value is not None else "").replace(",", "").strip()
    if not txt:
        return None
    try:
        return float(txt)
    except Exception:
        return None


def _find_values(rows: List[Dict[str, Any]], summary: Dict[str, Any], targets: List[float]) -> Dict[str, Any]:
    """알려진 값이 어느 키에 들어 있는지 전수 탐색한다."""
    hits: Dict[str, List[Dict[str, Any]]] = {str(t): [] for t in targets}
    blocks = [("output1[%d]" % i, r) for i, r in enumerate(rows)]
    if summary:
        blocks.append(("output2", summary))
    for label, block in blocks:
        if not isinstance(block, dict):
            continue
        for key, raw in block.items():
            val = _num(raw)
            if val is None:
                continue
            for t in targets:
                if abs(val - float(t)) < 1e-9:
                    hits[str(t)].append({"where": label, "key": key, "raw": raw})
    return hits


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYYMMDD")
    ap.add_argument("--end", required=True, help="YYYYMMDD")
    ap.add_argument("--pdno", default="", help="종목코드(선택). 비우면 전체")
    ap.add_argument("--mock", action="store_true", help="모의계좌로 시도(대개 미지원)")
    ap.add_argument(
        "--expect",
        nargs="*",
        type=float,
        default=[144.0, 256.0, 71950.0, 72350.0],
        help="알려진 정답 값들. 응답에서 이 숫자가 있는 키를 찾는다",
    )
    args = ap.parse_args()

    from kis_order_client import KISOrderClient  # noqa: E402

    client = KISOrderClient.from_env(mock=bool(args.mock))
    print(f"[MODE] mock={bool(args.mock)}  기간 {args.start} ~ {args.end}  종목={args.pdno or '전체'}")

    try:
        rsp = client.inquire_period_trade_profit(
            start_ymd=args.start, end_ymd=args.end, pdno=args.pdno
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] {type(exc).__name__}: {exc}")
        return 2

    rows = rsp.get("rows") or []
    summary = rsp.get("summary") or {}
    print(f"[OK] tr_id={rsp.get('tr_id')}  rows={len(rows)}  pages={rsp.get('pages')}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = OUT_DIR / f"kis_period_trade_profit_raw_{ts}.json"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(json.dumps(rsp, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[RAW] {raw_path}")

    if rows:
        print("\n=== output1 첫 행의 전체 키 (가공 없음)")
        for key, val in rows[0].items():
            print(f"   {key:24s} = {val}")
    if summary:
        print("\n=== output2 (합계)")
        for key, val in summary.items():
            print(f"   {key:24s} = {val}")

    print("\n=== 알려진 값 역탐색")
    hits = _find_values(rows, summary, args.expect)
    for target, found in hits.items():
        if found:
            for f in found:
                print(f"   {target:>12s} -> {f['where']}.{f['key']}  (raw={f['raw']})")
        else:
            print(f"   {target:>12s} -> 못 찾음")

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mock": bool(args.mock),
        "start_ymd": args.start,
        "end_ymd": args.end,
        "tr_id": rsp.get("tr_id"),
        "row_count": len(rows),
        "output1_keys": sorted(rows[0].keys()) if rows else [],
        "output2_keys": sorted(summary.keys()) if summary else [],
        "expected_value_hits": hits,
        "raw_path": str(raw_path),
        "note": "필드 매핑은 이 보고서를 사람이 확인한 뒤에 배선할 것. 추측 금지.",
    }
    rep_path = OUT_DIR / "kis_period_trade_profit_probe_latest.json"
    rep_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[REPORT] {rep_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
