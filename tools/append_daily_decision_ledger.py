# -*- coding: utf-8 -*-
"""하루의 **결정 상태**를 한 줄로 요약해 날짜 없는 append-only 원장에 쌓는다.

## 왜 필요한가

`tools/log_cleanup_30d.py` 가 `.+_YYYYMMDD(_HHMMSS)?\\.(json|csv|log|txt)` 를 30일 뒤 지운다.
날짜가 이름에 없는 것만 살아남는다. 실측(2026-09-10): `2_Logs` 파일 106,911개,
날짜 붙은 가족 166종, 결정 관련 가족들이 **전부 2026-08-12/19 부터만** 남아 있다.

이 구조가 조사를 두 번 막았다:
```
2026-09-09  optimize_if_due 발동 횟수를 못 셈 -> 다중검정 시도 수가 거짓이 된다
2026-09-10  거래대금 손상 구간(2025-12~2026-02)에 유동성 필터가 무엇을 걸렀는지 못 봄
```

**무차별 보존은 답이 아니다.** 2026-09-09 에 config 백업이 34,540개 쌓인 것을 정리했다.
그래서 **하루 한 줄**로 줄여 쌓는다. 1년이면 250줄, 10년이면 2,500줄이다.

## 무엇을 남기나

"그날 시스템이 무엇을 보고 무엇을 걸렀나" 를 나중에 답할 수 있는 최소한:
```
p0_daily_check            flags / open_positions / market_regime / risk_off
liquidity_filter_daily    status / candidates_before,after / min_trading_value
survivorship_daily        status / candidates_before,after / open_positions
backtest_validation_checklist  pass_n / fail_n / not_evaluable_n
trading_stage_validation  overall
kill_switch_validation    score 요약
패널                      거래가능 종목 수(20억) - 자료 손상 조기 검출용
```

## 멱등이다

같은 날짜가 이미 있으면 다시 쓰지 않는다. `--force` 로만 덮어쓴다.

[2026-09-10] 신설.
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import io
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG = ROOT / "2_Logs"
LEDGER = LOG / "decision_history_ledger.jsonl"


def _latest(pattern: str, ymd: str) -> dict:
    """그날 파일 중 가장 늦은 것. 없으면 빈 dict."""
    fs = sorted(glob.glob(str(LOG / (pattern % ymd))))
    if not fs:
        return {}
    try:
        return json.load(io.open(fs[-1], encoding="utf-8-sig"))
    except Exception:
        return {}


def _num(v, default=None):
    try:
        return int(v)
    except Exception:
        try:
            return float(v)
        except Exception:
            return default


def build_row(ymd: str) -> dict:
    p0 = _latest("p0_daily_check_%s_*.json", ymd)
    liq = _latest("liquidity_filter_daily_%s_*.json", ymd)
    sur = _latest("survivorship_daily_%s_*.json", ymd)
    chk = _latest("backtest_validation_checklist_%s_*.json", ymd)
    tsv = _latest("trading_stage_validation_%s_*.json", ymd)

    row = {
        "ymd": ymd,
        "written_at": dt.datetime.now().isoformat(timespec="seconds"),
        "p0": {
            "flags": p0.get("flags"),
            "open_positions": p0.get("open_positions"),
            "market_regime": p0.get("market_regime"),
            "risk_off": (p0.get("risk_off") or {}).get("enabled")
            if isinstance(p0.get("risk_off"), dict) else p0.get("risk_off"),
        } if p0 else None,
        "liquidity": {
            "status": liq.get("status"),
            "before": _num(liq.get("candidates_before")),
            "after": _num(liq.get("candidates_after")),
            "min_value_krw": _num(liq.get("min_trading_value_krw")),
        } if liq else None,
        "survivorship": {
            "status": sur.get("status"),
            "before": _num(sur.get("candidates_before")),
            "after": _num(sur.get("candidates_after")),
            "prices_date_max": sur.get("prices_date_max"),
        } if sur else None,
        "validation": {
            "pass_n": chk.get("pass_n"), "fail_n": chk.get("fail_n"),
            "not_evaluable_n": chk.get("not_evaluable_n"),
            "passed": chk.get("passed"),
        } if chk else None,
        "entry_switch": entry_switch_state(ymd) or None,
        "candidates": candidates_state(ymd) or None,
        "ladder": ladder_state(ymd) or None,
        "trading_stage": (tsv.get("overall") if isinstance(tsv.get("overall"), (str, int, float))
                          else (tsv.get("overall") or {}).get("verdict")
                          if isinstance(tsv.get("overall"), dict) else None),
    }
    return row


def candidates_state(ymd: str) -> dict:
    """그날 후보 생성이 **몇 단계까지 완화해서** 몇 개를 냈나.

    [2026-09-10 추가] 오늘 완화 단계 이력을 재려다 16일치밖에 못 구했다.
    candidates_v41_1_YYYYMMDD.csv 는 이름에 날짜가 있어 30일 뒤 지워진다.
    "L0 는 한 번도 안 쓰인다" 같은 사실이 매번 재발견 대상이 되면 안 된다.
    """
    import glob as _g
    fs = sorted(_g.glob(str(LOG / ("candidates_v41_1_%s.csv" % ymd))))
    if not fs:
        return {}
    try:
        import pandas as pd
        d = pd.read_csv(fs[-1])
    except Exception as exc:
        return {"error": "%s: %s" % (type(exc).__name__, exc)}
    out = {"n": int(len(d))}
    for col in ("relax_level", "candidate_origin", "market_regime"):
        if col in d.columns:
            vc = d[col].dropna().astype(str).value_counts()
            out[col] = {str(k): int(v) for k, v in vc.items()}
    # [2026-09-10] `natural_pass` 는 쓰지 않는다.
    #   generate_candidates_v41_1.py:1887 에서 **SECTOR_PREFILTER_UNION 폴백 행에만**
    #   False 로 박힌다. 게이트 통과분에는 컬럼 자체가 없어 concat 후 NaN 이 된다.
    #   fillna(False) 로 세면 매일 0 이 나오고, 그건 기아의 증거가 아니라 집계 오류다.
    #   쓸 수 있는 지표는 candidate_origin 이다 - 빈 값이 게이트 통과분이다.
    if "candidate_origin" in d.columns:
        fb = int(d["candidate_origin"].astype(str).eq("SECTOR_PREFILTER_UNION").sum())
        out["gate_pass_n"] = int(len(d)) - fb
        out["fallback_n"] = fb
    if "observe_only" in d.columns:
        # fillna(False).astype(bool) 은 object 컬럼에서 FutureWarning 을 낸다.
        # 참값만 세면 되므로 eq(True) 로 충분하다(결측은 False 취급).
        out["observe_only_n"] = int(d["observe_only"].eq(True).sum())
    return out


def entry_switch_state(ymd: str) -> dict:
    """진입 스위치(PAPER_EXIT_ONLY)가 그날 **켜져 있었나**.

    [2026-09-10 신설] 배관시험 개시와 함께.
      이 스위치가 켜져 있으면 후보가 몇 개든 max_new=0 으로 잠긴다.
      그런데 그 상태가 이력에 없어서, 나중에 "진입 0" 을 보고
      **정책인지 결함인지** 구별할 수 없었다. 판정 기준이므로 남긴다.

    declared 와 runtime 을 갈라 적는다:
      declared  런처 bat 이 선언한 기본값. 파일을 읽는다
      runtime   그날 돈 프로세스가 보고한 값. p1_entry_gate_status_latest.json
    둘이 다르면 **bat 은 고쳤는데 프로세스가 옛 환경을 들고 도는 것**이다.
    """
    import re as _re

    out: dict = {}

    declared = {}
    for name in ("run_paper_daily.bat", "run_intraday_paper.bat"):
        f = ROOT / name
        if not f.exists():
            continue
        try:
            txt = io.open(f, encoding="utf-8-sig", errors="replace").read()
        except Exception:
            continue
        m = _re.search(r'set "PAPER_EXIT_ONLY=([^"]*)"', txt)
        declared[name] = (m.group(1).strip() if m else None)
    out["declared"] = declared or None

    # 강제 지점(paper_engine.py:440)과 같은 규칙
    on = {"1", "true", "yes", "on"}
    out["declared_on"] = {k: (str(v).strip().lower() in on) for k, v in declared.items()} or None

    fp = LOG / "p1_entry_gate_status_latest.json"
    if fp.exists():
        try:
            d = json.load(io.open(fp, encoding="utf-8-sig"))
        except Exception as exc:
            out["runtime"] = {"error": "%s: %s" % (type(exc).__name__, exc)}
            d = None
        if d is not None:
            # 이 파일은 **덮어써진다**. 대상일이 아니면 채우지 않는다.
            mt = dt.datetime.fromtimestamp(fp.stat().st_mtime).strftime("%Y%m%d")
            if mt != ymd:
                out["runtime"] = {"skipped": "artifact_is_for_%s_not_%s" % (mt, ymd)}
            else:
                out["runtime"] = {
                    "exit_only_mode": d.get("exit_only_mode"),
                    "max_new_after": d.get("max_new_after"),
                    "max_new_final_expected": d.get("max_new_final_expected"),
                    "entry_candidates_after": d.get("entry_candidates_after"),
                    "written_at_hhmm": dt.datetime.fromtimestamp(fp.stat().st_mtime).strftime("%H:%M"),
                }

    rt = (out.get("runtime") or {}).get("exit_only_mode")
    dec = out.get("declared_on") or {}
    if rt is not None and dec:
        # 런처 중 하나라도 선언값과 다르면 표시한다
        out["divergence"] = any(bool(v) != bool(rt) for v in dec.values())
    return out


def ladder_state(ymd: str) -> dict:
    """그날 사다리가 **어디서 멈췄고 무엇이 막았나**.

    [2026-09-10 신설] C 수리.
      generate_candidates_v41_1 은 단계마다 `_diag_counts` 를 만들어
      `attempts` 로 `candidates_latest_meta.json` 에 넣는다. 자료는 이미 있었다.
      **그런데 그 파일은 매일 덮어써져 이력이 남지 않는다.**
      그래서 "0건이 판단의 결과인가 소진의 결과인가", "무엇이 막았나" 를
      하루만 지나면 답할 수 없었다.

    남기는 것:
      chosen_level   어느 단계에서 멈췄나 (NONE = L0~L9 를 다 써도 0건)
      levels_tried   내려간 자취
      binding_gate   그 단계에서 **가장 적게 통과시킨** 게이트 = 구속 조건
      diag           게이트별 통과수 원자료

    주의: meta 는 **최신 1건**만 있다. 다른 날짜에 붙이면 거짓이 되므로
      meta 의 latest_date/as_of 가 대상일과 맞을 때만 채운다.
    """
    mp = LOG / "candidates_latest_meta.json"
    if not mp.exists():
        return {}
    try:
        m = json.load(io.open(mp, encoding="utf-8-sig"))
    except Exception as exc:
        return {"error": "%s: %s" % (type(exc).__name__, exc)}

    # 날짜 가드 - meta 는 덮어써지는 파일이다
    md = str(m.get("latest_date") or "").replace("-", "")
    ad = str(m.get("as_of") or "")[:10].replace("-", "")
    if ymd not in (md, ad):
        return {"skipped": "meta_is_for_%s_not_%s" % (md or ad or "?", ymd)}

    attempts = m.get("attempts") or []
    chosen = m.get("chosen_level")
    # chosen_level 에 해당하는 시도를 찾는다. 없으면 마지막(=가장 느슨한) 것
    pick = None
    for a in attempts:
        if str(a.get("level")) == str(chosen):
            pick = a
    if pick is None and attempts:
        pick = attempts[-1]
    diag = (pick or {}).get("diag") or {}

    binding = None
    if diag:
        cand = {k: v for k, v in diag.items()
                if k.endswith("_pass") and k != "all_pass" and isinstance(v, (int, float))}
        if cand:
            binding = min(cand, key=lambda k: cand[k])

    return {
        "chosen_level": chosen,
        "levels_tried": [str(a.get("level")) for a in attempts],
        "attempts_n": len(attempts),
        # NONE = 사다리를 다 써도 0건. 그 외 = 그 단계에서 멈춤
        "zero_kind": ("EXHAUSTED" if str(chosen).upper() == "NONE" else "STOPPED_AT_LEVEL"),
        "binding_gate": binding,
        "binding_pass_n": (diag.get(binding) if binding else None),
        "rows_today": diag.get("rows_today"),
        "all_pass": diag.get("all_pass"),
        "diag": diag,
    }


def panel_health(ymd: str) -> dict:
    """그날 거래가능 종목 수. 자료 손상(int32 오버플로 등)을 나중에 잡아내는 실마리."""
    try:
        import sys
        sys.path.insert(0, str(ROOT / "tools"))
        from load_merged_panel import load_merged
        d = load_merged()
        d = d[d["date"] == ymd]
        if d.empty:
            return {"rows": 0, "ge_2e9": None, "note": "패널에 그 날짜가 없어요"}
        import pandas as pd
        v = pd.to_numeric(d["value"], errors="coerce")
        return {"rows": int(len(d)), "ge_2e9": int((v >= 2e9).sum()),
                "value_na": int(v.isna().sum())}
    except Exception as exc:
        return {"error": "%s: %s" % (type(exc).__name__, exc)}


def main() -> int:
    ap = argparse.ArgumentParser(description="하루 결정 상태를 원장에 한 줄로")
    ap.add_argument("--ymd", default=None, help="기본은 오늘")
    ap.add_argument("--force", action="store_true", help="같은 날짜가 있어도 덧쓴다")
    ap.add_argument("--with-panel", action="store_true",
                    help="패널 건강(거래가능 종목 수)도 넣는다. 패널 로딩이 느리다")
    ap.add_argument("--backfill-days", type=int, default=0,
                    help="오늘부터 과거 N일치를 한 번에 채운다(남아 있는 파일 범위 안에서)")
    a = ap.parse_args()

    base = dt.date.today() if not a.ymd else dt.datetime.strptime(a.ymd, "%Y%m%d").date()
    days = [base - dt.timedelta(days=i) for i in range(max(1, a.backfill_days))]

    have = set()
    if LEDGER.exists():
        for line in io.open(LEDGER, encoding="utf-8"):
            try:
                have.add(json.loads(line).get("ymd"))
            except Exception:
                pass

    wrote = skipped = empty = 0
    for d0 in days:
        ymd = d0.strftime("%Y%m%d")
        if ymd in have and not a.force:
            skipped += 1
            continue
        row = build_row(ymd)
        if not any(row.get(k) for k in ("p0", "liquidity", "survivorship", "validation", "candidates", "ladder")):
            empty += 1
            continue
        if a.with_panel:
            row["panel"] = panel_health(ymd)
        with io.open(LEDGER, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + chr(10))
        wrote += 1

    print("[DECISION_LEDGER] 기록 %d / 건너뜀(이미 있음) %d / 자료없음 %d  -> %s"
          % (wrote, skipped, empty, LEDGER.name))
    if wrote:
        print("  이 파일은 **이름에 날짜가 없어** log_cleanup_30d 대상이 아니에요.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
