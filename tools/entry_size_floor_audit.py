# -*- coding: utf-8 -*-
"""진입 수량이 **최종적으로 얼마나 작아졌는지**를 한 곳에서 본다. 관측 전용.

[2026-09-11] PLANS (360).
029460 실측: 139주 -> 18(종목당예산) -> 5(분할1차 0.3) -> 2(모멘텀) -> 1(가중)
            = 기저의 0.72%, 계좌의 0.046%.

축소기마다 자기 이유를 로그에 남긴다. 그런데 **곱을 보는 지점이 없다.**
개별로는 다 타당한 규칙이 겹쳐서 표본이 안 되는 크기를 만든다.
이 도구는 그 곱과 최종 비중만 계산해 원장에 남긴다.

**매매를 바꾸지 않는다.** rc 는 항상 0 이다 (--strict 를 줬을 때만 예외).

    python tools/entry_size_floor_audit.py                    # 오늘
    python tools/entry_size_floor_audit.py --since 20260901   # 기간
    python tools/entry_size_floor_audit.py --floor-pct 0.5 --strict   # 바닥 미만이면 rc=3
    python tools/entry_size_floor_audit.py --out-dir <경로>   # 격리 실행
"""
from __future__ import annotations

import argparse
import csv
import glob
import io
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
FILLS = PAPER / "fills.csv"

# [주의] shrink_ratio 는 **부분 축소만** 잰다.
#   note 의 qty_initial 은 이미 BUDGET_BASIC_CAP 과 split_entry 1차를 지난 뒤의 값이다.
#   (029460 실측: calc_qty 139 -> cap 18 -> split 5 = qty_initial, 그 뒤 5 -> 1)
#   전체 곱을 보려면 배치 로그의 [BUDGET_BASIC_CAP] 줄과 함께 봐야 한다.
#   **pct_of_capital 은 부분이 아니다 - 최종 비중 그 자체이므로 판정은 이 값으로 한다.**
LEDGER_NAME = "entry_size_ledger.csv"          # 날짜 없는 append-only
LATEST_NAME = "entry_size_floor_audit_latest.json"
_NL = chr(10)
ROW_HEADER = ("ts,code,qty_initial,qty_final,price,notional_krw,"
              "shrink_ratio,pct_of_capital,capital_basis,capital_krw,trail" + _NL)
ROW_FMT = "%s,%s,%d,%d,%.2f,%.0f,%.6f,%.6f,%s,%.0f,%s" + _NL


def _note_field(note: str, key: str) -> Optional[str]:
    for part in str(note or "").split(";"):
        p = part.strip()
        if p.startswith(key + "="):
            return p[len(key) + 1:]
    return None


def resolve_capital() -> Dict[str, Any]:
    """자본 기준을 고른다. **브로커 실측이 있으면 그쪽을 쓴다.**

    엔진이 쓰는 effective_capital_total 은 trades_csv 기반이라 실제와 어긋난다
    (2026-09-11 실측 86,817,634 vs 브로커 103,002,391). 둘 다 싣고 브로커를 우선한다.
    """
    out: Dict[str, Any] = {"basis": "unknown", "capital": 0.0, "engine_capital": None, "broker_capital": None}
    try:
        d = json.loads((LOG_DIR / "pending_entry_status_latest.json").read_text(encoding="utf-8-sig"))
        cb = ((d.get("max_positions_meta") or {}).get("capital_basis")) or {}
        v = cb.get("effective_capital_total")
        if v:
            out["engine_capital"] = float(v)
    except Exception:
        pass
    try:
        d = json.loads((LOG_DIR / "broker_account_basis_latest.json").read_text(encoding="utf-8-sig"))
        if str(d.get("status")) == "PASS" and d.get("equity_est"):
            out["broker_capital"] = float(d["equity_est"])
    except Exception:
        pass
    if out["broker_capital"]:
        out["basis"], out["capital"] = "broker_account_equity", out["broker_capital"]
    elif out["engine_capital"]:
        out["basis"], out["capital"] = "engine_effective_capital_total", out["engine_capital"]
    return out


def collect(since: str) -> List[Dict[str, Any]]:
    if not FILLS.is_file():
        return []
    rows: List[Dict[str, Any]] = []
    with FILLS.open("r", encoding="utf-8", newline="") as fh:
        for r in csv.reader(fh):
            if len(r) < 7:
                continue
            ts, code, side, qty, px, _oid, note = r[0], r[1], r[2], r[3], r[4], r[5], r[6]
            if str(side).upper() != "BUY":
                continue
            ymd = str(ts)[:8]
            if not ymd.isdigit() or ymd < since:
                continue
            try:
                q_fin = int(float(qty))
                price = float(px)
            except (TypeError, ValueError):
                continue
            q_init_raw = _note_field(note, "qty_initial")
            try:
                q_init = int(float(q_init_raw)) if q_init_raw else q_fin
            except (TypeError, ValueError):
                q_init = q_fin
            rows.append({
                "ts": ts, "code": str(code).zfill(6),
                "qty_initial": q_init, "qty_final": q_fin, "price": price,
                "notional": price * q_fin,
                "trail": (_note_field(note, "qty_trail") or "").replace(",", " "),
            })
    return rows


ROUNDTRIP_COST_PCT = 0.00358   # 왕복. 권위 원장 trades_calc.csv (사용자 승인). 여기서 **가정하지 않고 표기만** 한다


def collect_roundtrips(since: str) -> List[Dict[str, Any]]:
    """SELL 을 entry_order_id 로 BUY 에 붙여 왕복을 만든다.

    [2026-09-11] 이 도구는 원래 BUY 만 봤다. 그날 청산 3건이 나오면서
    "얼마나 작게 샀나" 만으로는 ②수익률 의 재료가 안 된다는 것이 드러났다.

    비용은 **심지 않는다.** gross 와 net(왕복 0.358%) 을 나란히 낸다.
    부분 매도는 팔린 수량만 왕복으로 센다 (남은 수량은 미청산).
    """
    if not FILLS.is_file():
        return []
    buys: Dict[str, Dict[str, Any]] = {}
    sells: List[Dict[str, Any]] = []
    with FILLS.open("r", encoding="utf-8", newline="") as fh:
        for r in csv.reader(fh):
            if len(r) < 7:
                continue
            ts, code, side, qty, px, oid, note = r[0], r[1], r[2], r[3], r[4], r[5], r[6]
            ymd = str(ts)[:8]
            if not ymd.isdigit() or ymd < since:
                continue
            try:
                q, price = int(float(qty)), float(px)
            except (TypeError, ValueError):
                continue
            side_u = str(side).upper()
            if side_u == "BUY":
                buys[str(oid)] = {"ts": ts, "code": _z6(code), "qty": q, "price": price}
            elif side_u == "SELL":
                eoid = _note_field(note, "entry_order_id") or ""
                sells.append({"ts": ts, "code": _z6(code), "qty": q, "price": price,
                              "entry_order_id": eoid,
                              "exit_reason": _note_field(note, "exit_reason") or ""})
    out: List[Dict[str, Any]] = []
    for sv in sells:
        b = buys.get(sv["entry_order_id"])
        if not b:
            out.append({**sv, "matched": False})
            continue
        gross = (sv["price"] - b["price"]) / b["price"] if b["price"] > 0 else 0.0
        out.append({
            "code": sv["code"], "matched": True,
            "buy_ts": b["ts"], "sell_ts": sv["ts"],
            "buy_price": b["price"], "sell_price": sv["price"],
            "qty": sv["qty"], "exit_reason": sv["exit_reason"],
            "gross_ret": gross, "net_ret": gross - ROUNDTRIP_COST_PCT,
            "gross_pnl_krw": (sv["price"] - b["price"]) * sv["qty"],
            "net_pnl_krw": (sv["price"] - b["price"]) * sv["qty"] - b["price"] * sv["qty"] * ROUNDTRIP_COST_PCT,
        })
    return out


def collect_qty_chain(since: str) -> Dict[str, Any]:
    """수량 축소 **사슬**을 캡처에서 모은다.

    [2026-09-12] C20. 축소기는 서로 독립적으로 "안전하게" 줄이는데
    **결합 결과를 계산하는 주체가 없다.** 이 함수가 그 곱을 낸다.

    사슬은 `entry.py` 의 `_qty_trail` 이고 2026-09-12 부터 초기 산정
    (예산상한·분할·적응)까지 포함한다. 그 전 캡처는 사슬이 짧다 - 장치가
    붕괴가 끝난 자리에서 시작했기 때문이다. **짧은 사슬을 "축소 없음"으로 읽지 말 것.**
    """
    import collections
    rows: List[Dict[str, Any]] = []
    for f in sorted(glob.glob(str(LOG_DIR / "pending_entry_status_capture_*.json"))):
        if since and since not in Path(f).name:
            continue
        try:
            d = json.load(io.open(f, encoding="utf-8"))
        except Exception:
            continue
        for r in (d.get("entry_decision_rows") or []):
            rows.append(r)
    seen = {}
    for r in rows:                       # 같은 종목이 사이클마다 반복된다. 종목 단위로 접는다
        seen[(str(r.get("code")), str(r.get("reason"))[:40])] = r
    stage_hits = collections.Counter()
    stage_mult: Dict[str, List[float]] = collections.defaultdict(list)
    detail: List[Dict[str, Any]] = []
    for r in seen.values():
        trail = str(r.get("qty_trail", "") or "")
        raw = r.get("qty_raw_calc", "")
        steps = []
        for part in [x for x in trail.split("|") if x]:
            try:
                name, ba = part.split(":", 1)
                before, after = ba.split(">", 1)
                b, a = int(before), int(after)
            except Exception:
                continue
            steps.append({"stage": name, "before": b, "after": a,
                          "mult": (float(a) / float(b)) if b else None})
            stage_hits[name] += 1
            if b:
                stage_mult[name].append(float(a) / float(b))
        if not steps and not str(raw):
            continue
        first = int(raw) if str(raw).strip().isdigit() else (steps[0]["before"] if steps else None)
        last = steps[-1]["after"] if steps else first
        detail.append({
            "code": r.get("code"), "reason": str(r.get("reason"))[:44],
            "qty_raw_calc": first, "qty_final": last,
            "product": (float(last) / float(first)) if first else None,
            "steps": steps,
        })
    return {
        "rows_seen": len(seen),
        "stage_hits": dict(stage_hits),
        "stage_mult_median": {k: round(sorted(v)[len(v) // 2], 4) for k, v in stage_mult.items() if v},
        "detail": detail,
    }


def _z6(v: Any) -> str:
    return str(v).strip().zfill(6)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="", help="YYYYMMDD (기본: 오늘)")
    ap.add_argument("--floor-pct", type=float, default=0.5,
                    help="최종 비중 바닥 (계좌 %%). 기본 0.5%%")
    ap.add_argument("--strict", action="store_true", help="바닥 미만이 있으면 rc=3")
    ap.add_argument("--out-dir", default=str(LOG_DIR))
    ap.add_argument("--fills-path", default="", help="다른 fills.csv (백업 대조·시험용)")
    ap.add_argument("--chain", action="store_true",
                    help="수량 축소 사슬(_qty_trail)의 **곱**을 낸다 (C20)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ledger = out_dir / LEDGER_NAME          # **out_dir 를 따른다** (2026-09-11 학습)

    since = args.since.strip()

    if args.chain:
        # 사슬 조회는 날짜로 좁히지 않는다. 표본이 적으면 아무것도 안 보인다
        ch = collect_qty_chain(since)
        print("=" * 74)
        print(" 수량 축소 사슬의 곱   종목 %d   %s" % (ch["rows_seen"], since or "전체 캡처"))
        print("=" * 74)
        if not ch["detail"]:
            print("  사슬 기록이 없다. 2026-09-12 이전 캡처는 초기 산정 구간이 계측되지")
            print("  않았다 - **짧은 사슬을 '축소 없음' 으로 읽지 말 것**")
        for d in ch["detail"]:
            prod = ("x%.3f" % d["product"]) if d["product"] is not None else "-"
            print("  %-8s %-7s -> %-7s %-9s %s"
                  % (d["code"], d["qty_raw_calc"], d["qty_final"], prod, d["reason"]))
            for st in d["steps"]:
                mu = ("x%.3f" % st["mult"]) if st["mult"] is not None else "-"
                print("        %-22s %6d -> %-6d %s" % (st["stage"], st["before"], st["after"], mu))
        if ch["stage_mult_median"]:
            print("-" * 74)
            print("  단계별 중앙 배수 (발동 횟수)")
            for k, v in sorted(ch["stage_mult_median"].items(), key=lambda x: x[1]):
                print("    %-24s x%-8.3f  %d회" % (k, v, ch["stage_hits"].get(k, 0)))
        return 0

    if not since:
        import datetime as dt
        since = dt.datetime.now().strftime("%Y%m%d")

    global FILLS
    if args.fills_path.strip():
        FILLS = Path(args.fills_path.strip())
        print("[FILLS] 원본 지정: %s" % FILLS)

    cap = resolve_capital()
    rows = collect(since)
    trips = collect_roundtrips(since)
    if not rows:
        print("[SIZE] since=%s BUY 체결 0건" % since)

    below: List[Dict[str, Any]] = []
    new = not ledger.exists()
    with ledger.open("a", encoding="utf-8", newline="") as fh:
        if new:
            fh.write(ROW_HEADER)
        for r in rows:
            shrink = (r["qty_final"] / r["qty_initial"]) if r["qty_initial"] > 0 else 0.0
            pct = (r["notional"] / cap["capital"] * 100.0) if cap["capital"] > 0 else 0.0
            r["shrink_ratio"], r["pct_of_capital"] = shrink, pct
            if cap["capital"] > 0 and pct < args.floor_pct:
                below.append(r)
            fh.write(ROW_FMT % (r["ts"], r["code"], r["qty_initial"], r["qty_final"],
                                r["price"], r["notional"], shrink, pct,
                                cap["basis"], cap["capital"], r["trail"]))
            flag = "  <- 바닥 미만" if (cap["capital"] > 0 and pct < args.floor_pct) else ""
            print("[SIZE] {ts} {code}  {qi}->{qf}주 (x{sh:.4f})  {nt:,.0f}원  계좌의 {pc:.4f}%{fl}".format(
                ts=r["ts"], code=r["code"], qi=r["qty_initial"], qf=r["qty_final"],
                sh=shrink, nt=r["notional"], pc=pct, fl=flag))

    matched = [t for t in trips if t.get("matched")]
    if trips:
        print("[왕복] %d건 (매칭 %d)" % (len(trips), len(matched)))
        for t in matched:
            print("  {code} {b:.0f}->{s:.0f} x{q}주  총 {g:+.2f}%  비용차감 {n:+.2f}%  {r}".format(
                code=t["code"], b=t["buy_price"], s=t["sell_price"], q=t["qty"],
                g=t["gross_ret"] * 100, n=t["net_ret"] * 100, r=t["exit_reason"]))
        unmatched = [t for t in trips if not t.get("matched")]
        if unmatched:
            print("  [미매칭] %d건 - entry_order_id 로 BUY 를 못 찾았다" % len(unmatched))
        if matched:
            wins = sum(1 for t in matched if t["net_ret"] > 0)
            print("  합계 총 {g:+,.0f}원 / 비용차감 {n:+,.0f}원 | 승 {w}/{a}".format(
                g=sum(t["gross_pnl_krw"] for t in matched),
                n=sum(t["net_pnl_krw"] for t in matched), w=wins, a=len(matched)))

    summary = {
        "generated_at": __import__("datetime").datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "since": since,
        "observe_only": True,
        "capital": cap,
        "floor_pct": args.floor_pct,
        "entries": len(rows),
        "below_floor": len(below),
        "below_floor_codes": [r["code"] for r in below],
        "median_pct_of_capital": (sorted(r["pct_of_capital"] for r in rows)[len(rows) // 2] if rows else None),
        "total_notional_krw": round(sum(r["notional"] for r in rows)),
        "total_pct_of_capital": (round(sum(r["notional"] for r in rows) / cap["capital"] * 100.0, 6)
                                 if cap["capital"] > 0 else None),
        "shrink_ratio_note": "qty_initial->final 만 잰다. BUDGET_BASIC_CAP/split 1차는 그 이전이다",
        "ledger": str(ledger),
        "roundtrips": len(trips),
        "roundtrips_matched": len(matched),
        "roundtrip_cost_pct": ROUNDTRIP_COST_PCT,
        "roundtrip_gross_pnl_krw": round(sum(t["gross_pnl_krw"] for t in matched)) if matched else None,
        "roundtrip_net_pnl_krw": round(sum(t["net_pnl_krw"] for t in matched)) if matched else None,
        "roundtrip_wins": sum(1 for t in matched if t["net_ret"] > 0) if matched else None,
        "roundtrip_detail": matched,
    }
    (out_dir / LATEST_NAME).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[SIZE] 자본기준=%s %.0f원 | 진입 %d건 | 바닥(%.2f%%) 미만 %d건"
          % (cap["basis"], cap["capital"], len(rows), args.floor_pct, len(below)))

    if args.strict and below:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
