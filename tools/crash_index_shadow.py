# -*- coding: utf-8 -*-
"""폭락 가드를 **진짜 지수**로 재계산한 그림자 판정 (O5-7, 2026-08-31 신설).

왜 있나
    p0_daily_check 의 폭락 가드는 1차 소스(pykrx 지수)가 비어(error_fetch_empty)
    로컬 프록시로 떨어진다. 그 프록시의 "지수" 는 파케이 파일 안 종목들의 **평균 종가**다.
    단일 파일에서 60일을 요구하는데 최근 자료는 1일치 파일로 저장되므로,
    최신 파일을 전부 건너뛰고 krx_daily_20260113_20260403_clean.parquet(60일)로 성공한다.
    => **2026-04-03 시장으로 오늘을 판정한다** (최신 거래일 20260828). .agent/PLANS.md (163)(164)

    합쳐서 고치려다 두 번 회귀했다((164)). 유니버스가 다른 파일을 이어붙이면
    평균 종가 수준이 튀어 가짜 폭락(-98%)이 나온다. 평균 종가로 지수를 흉내 내는 것이 원래 문제였다.

무엇을 하는가
    2_Logs/index_daily_history.csv (VIBE_Index_Daily_Fetch 가 매일 16:05 갱신, 최신 20260828)
    의 **진짜 지수**로 같은 산식(60일 max_dd / 당일 수익률)을 계산해
    현재 가드 판정과 나란히 기록한다.

무엇을 하지 않는가 - 중요
    **p0_daily_check.py 를 건드리지 않는다. 매매 판정을 바꾸지 않는다.**
    진짜 지수로 재면 60일 max_dd 가 -38~-41% 로 trigger_max_dd_pct(0.12) 를 크게 넘는다.
    즉 소스만 바꾸면 가드가 상시 발동해 시스템이 REDUCE 로 고정된다.
    그런데 그 0.12 는 근거 기록이 0인 값이다
    (docs/references/GATE_THRESHOLD_INVENTORY.md [가동] 미분류).
    **소스 교체와 문턱 재설정은 함께 결정해야 하므로 그때까지 관측만 한다.**

주의 - 지수 코드가 라벨과 다르다 (2026-08-31 실측)
    0001 -> KOSPI / 1001 -> KOSDAQ / 2001 -> 같은 파일에 KOSDAQ 과 KOSPI200 이 섞여 있다
    config 는 index_code=1001 을 index_market=KOSPI 로 쓴다. **불일치다.**
    이 도구는 이름으로도 찾고 코드로도 찾아 둘 다 기록한다.

읽기 전용. 2_Logs/index_daily_history.csv 는 RD_20260831_flow_h10 의 동결 기준선이므로
**절대 수정하지 않는다.**

사용   python tools/crash_index_shadow.py [--as-of YYYYMMDD] [--out DIR]
반환   0 정상 / 2 자료 부족 / 3 실행 실패
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

INDEX_CSV = ROOT / "2_Logs" / "index_daily_history.csv"
DEFAULT_OUT = ROOT / "2_Logs"


def _series(rows, code=None, name_contains=None):
    sel = []
    for r in rows:
        if code is not None and str(r.get("index_code", "")).strip() != str(code):
            continue
        if name_contains is not None and name_contains.upper() not in str(r.get("index_name", "")).upper():
            continue
        try:
            c = float(r.get("close"))
        except Exception:
            continue
        if c > 0:
            sel.append((str(r.get("date", "")).strip(), c, str(r.get("index_name", "")).strip()))
    sel.sort()
    return sel


def _metrics(sel, window):
    if len(sel) < 2:
        return None
    w = sel[-int(window):]
    cl = [c for _, c, _ in w]
    peak, mdd = cl[0], 0.0
    for c in cl:
        peak = max(peak, c)
        mdd = min(mdd, c / peak - 1.0)
    return {
        "date_min": w[0][0], "date_max": w[-1][0],
        "names": sorted({n for _, _, n in w}),
        "used_rows": len(w),
        "close_last": cl[-1],
        "max_dd": mdd,
        "day_ret": cl[-1] / cl[-2] - 1.0 if len(cl) >= 2 else 0.0,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", default=dt.datetime.now().strftime("%Y%m%d"))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    a = ap.parse_args()

    if not INDEX_CSV.exists():
        print("[FAIL] 지수 자료가 없다: %s" % INDEX_CSV)
        return 2
    rows = [r for r in csv.DictReader(io.open(INDEX_CSV, encoding="utf-8-sig"))
            if str(r.get("date", "")).strip() <= a.as_of]
    if not rows:
        print("[FAIL] as_of 이하 행이 없다")
        return 2

    from paper_engine.config import load_config
    cfg = load_config()
    crash = cfg.get("crash_risk_off") or {}
    window = int(crash.get("window_days", 60) or 60)
    trig_dd = float(crash.get("trigger_max_dd_pct", 0.12) or 0.12)
    trig_day = float(crash.get("trigger_day_ret_pct", 0.05) or 0.05)
    cfg_code = str(crash.get("index_code") or "").strip()
    cfg_name = str(crash.get("index_name_contains") or "").strip()

    out = {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "as_of": a.as_of,
        "source": str(INDEX_CSV),
        "window_days": window,
        "limits": {"trigger_max_dd_pct": trig_dd, "trigger_day_ret_pct": trig_day},
        "config": {"index_code": cfg_code, "index_name_contains": cfg_name},
        "note": ("관측 전용. p0_daily_check 의 매매 판정을 바꾸지 않는다. "
                 "소스 교체는 trigger 문턱 재설정과 함께 결정해야 한다."),
        "by_code": {},
        "by_config": {},
    }

    for code in sorted({str(r.get("index_code", "")).strip() for r in rows if str(r.get("index_code", "")).strip()}):
        m = _metrics(_series(rows, code=code), window)
        if m:
            m["would_trigger"] = bool(abs(m["max_dd"]) >= trig_dd or abs(m["day_ret"]) >= trig_day)
            out["by_code"][code] = m

    m_code = out["by_code"].get(cfg_code)
    m_name = _metrics(_series(rows, name_contains=cfg_name), window) if cfg_name else None
    if m_name:
        m_name["would_trigger"] = bool(abs(m_name["max_dd"]) >= trig_dd or abs(m_name["day_ret"]) >= trig_day)
    out["by_config"] = {"by_index_code": m_code, "by_index_name_contains": m_name}
    out["code_label_mismatch"] = bool(
        m_code and cfg_name and not any(cfg_name.upper() in n.upper() for n in m_code.get("names", []))
    )

    out_dir = Path(a.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    for p in (out_dir / ("crash_index_shadow_%s.json" % a.as_of),
              out_dir / "crash_index_shadow_latest.json"):
        io.open(p, "w", encoding="utf-8").write(json.dumps(out, ensure_ascii=False, indent=1, default=str))

    print("=== 진짜 지수로 재계산한 폭락 지표 (관측 전용) ===")
    print("  as_of %s / window %d일 / 한계 max_dd %.2f, day_ret %.2f" % (a.as_of, window, trig_dd, trig_day))
    for code, m in out["by_code"].items():
        print("  %-6s %-16s %s~%s  max_dd %+.4f  day_ret %+.4f  -> %s"
              % (code, ",".join(m["names"])[:16], m["date_min"], m["date_max"],
                 m["max_dd"], m["day_ret"], "발동" if m["would_trigger"] else "미발동"))
    if out["code_label_mismatch"]:
        print("  [MISMATCH] config index_code=%s 의 실제 이름이 index_name_contains=%s 와 다르다"
              % (cfg_code, cfg_name))
    print("  saved: %s" % (out_dir / "crash_index_shadow_latest.json"))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        print("[FAIL] %s" % exc)
        raise SystemExit(3)
