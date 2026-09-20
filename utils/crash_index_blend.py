# -*- coding: utf-8 -*-
"""폭락 가드용 **구성일치 혼합 지수** 시계열 (O5-8, 2026-08-31 신설).

왜 있나
    폭락 가드의 로컬 프록시는 파케이 파일 안 종목들의 **평균 종가**를 지수처럼 썼다.
    단일 파일에서 60일을 요구하는데 최근 자료는 1일치 파일이라, 최신 파일을 전부 건너뛰고
    2026-04-03 에서 끝나는 옛 아카이브로 성공했다 -> **4월 시장으로 오늘을 판정**했다.
    합쳐서 고치려니 파일마다 유니버스가 달라 수준이 튀어 가짜 폭락(-98%)이 났다.
    .agent/PLANS.md (163)(164)(165)(166)

무엇을 쓰는가
    2_Logs/index_daily_history.csv 의 **진짜 지수**.
    VIBE_Index_Daily_Fetch 가 매일 16:05 갱신한다(최신 20260828 확인).
    이 파일은 RD_20260831_flow_h10 의 동결 기준선이므로 **읽기만 한다.**

왜 혼합인가
    이 프로젝트의 원칙: "기준선은 실제 대안 + 스타일 일치 둘 다" (2026-08-28).
    실제 체결의 시장 구성이 KOSDAQ 53.6% / KOSPI 46.4% 이므로
    KOSPI 단독도 KOSDAQ 단독도 스타일이 어긋난다.
    부수로 config 의 `index_code=1001 + index_market=KOSPI` 불일치도 해소된다
    (1001 은 이 자료에서 KOSDAQ 이다). **단일 코드를 쓰지 않기 때문이다.**

주의 - 코드와 이름이 어긋난다 (2026-08-31 실측)
    0001 -> KOSPI / 1001 -> KOSDAQ / 2001 -> **같은 파일에 KOSDAQ 과 KOSPI200 이 섞여 있다**

    처음에는 이름으로 골랐다가 틀렸다. 2001 이 이력 일부에서 KOSDAQ 으로 라벨돼 있어
    1001 과 2001 이 한 계열로 섞였고 max_dd 가 -0.4829 로 나왔다(정답 -0.3700).
    **그래서 코드로 고르고, 그 코드에서 실제로 관측된 이름을 함께 기록해 불일치를 드러낸다.**
    [[feedback_reconstruct_arithmetic_not_labels]] - 라벨이 아니라 식별자로 고른다.

가중치
    기본 KOSPI 0.464 / KOSDAQ 0.536. 근거는 실측 체결 구성이며
    포트 구성이 바뀌면 갱신해야 한다(PLANS (166) E-4).
"""
from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
INDEX_CSV = ROOT / "2_Logs" / "index_daily_history.csv"

DEFAULT_WEIGHTS: Dict[str, float] = {"KOSPI": 0.464, "KOSDAQ": 0.536}
# 역할 -> index_code. **이름이 아니라 코드로 고른다** (위 주의 참조)
ROLE_CODE: Dict[str, str] = {"KOSPI": "0001", "KOSDAQ": "1001"}


def _load_by_code(as_of_ymd: str, csv_path: Optional[Path] = None):
    """index_code -> ({date8: close}, {관측된 index_name}) 를 돌려준다."""
    p = Path(csv_path or INDEX_CSV)
    out: Dict[str, Dict[str, float]] = {}
    names: Dict[str, set] = {}
    if not p.exists():
        return out, names
    with io.open(p, encoding="utf-8-sig", errors="ignore") as f:
        for r in csv.DictReader(f):
            d = str(r.get("date", "")).strip()
            if not d or d > str(as_of_ymd):
                continue
            code = str(r.get("index_code", "")).strip()
            if not code:
                continue
            try:
                c = float(r.get("close"))
            except Exception:
                continue
            if c > 0:
                out.setdefault(code, {})[d] = c
                names.setdefault(code, set()).add(str(r.get("index_name", "")).strip().upper())
    return out, names


def build_blend_series(as_of_ymd: str,
                       weights: Optional[Dict[str, float]] = None,
                       csv_path: Optional[Path] = None) -> Tuple[List[Tuple[str, float]], dict]:
    """구성일치 혼합 지수를 만든다.

    일별 **수익률**을 가중 평균해 지수화한다(레벨을 섞지 않는다 - 레벨을 섞으면
    (164)의 유니버스 점프와 같은 인위적 낙폭이 생긴다).
    반환 (series, info). 만들 수 없으면 ([], info) 이며 호출자는 기존 경로로 떨어져야 한다.
    """
    w = dict(weights or DEFAULT_WEIGHTS)
    tot = sum(abs(v) for v in w.values())
    if tot <= 0:
        return [], {"status": "error_bad_weights"}
    w = {k.upper(): v / tot for k, v in w.items()}

    by_code, names = _load_by_code(as_of_ymd, csv_path=csv_path)
    role_code = {k: ROLE_CODE.get(k) for k in w}
    missing = [k for k, c in role_code.items() if not c or c not in by_code]
    if missing:
        return [], {"status": "error_missing_index", "missing": missing,
                    "available_codes": sorted(by_code.keys())}

    dates = None
    for k in w:
        s = set(by_code[role_code[k]].keys())
        dates = s if dates is None else (dates & s)
    dates = sorted(dates or [])
    if len(dates) < 2:
        return [], {"status": "error_insufficient_dates", "rows": len(dates)}

    level = 100.0
    series: List[Tuple[str, float]] = [(dates[0], level)]
    for i in range(1, len(dates)):
        d, dp = dates[i], dates[i - 1]
        r = 0.0
        for k, wk in w.items():
            src = by_code[role_code[k]]
            prev = src[dp]
            r += wk * (src[d] / prev - 1.0) if prev > 0 else 0.0
        level *= (1.0 + r)
        series.append((d, level))
    # 코드에서 실제로 관측된 이름. 역할과 다르면 호출자가 알 수 있게 남긴다
    observed = {k: sorted(names.get(role_code[k], set())) for k in w}
    return series, {"status": "ok", "weights": w, "rows": len(series),
                    "date_min": dates[0], "date_max": dates[-1],
                    "role_code": role_code, "observed_names": observed,
                    "label_mismatch": {k: v for k, v in observed.items() if k not in v}}


def max_dd_and_day_ret(series: List[Tuple[str, float]], window: int = 60) -> Optional[dict]:
    """마지막 window 구간의 max_dd 와 당일 수익률."""
    if not series or len(series) < 2:
        return None
    w = series[-int(window):]
    cl = [c for _, c in w]
    peak, mdd = cl[0], 0.0
    for c in cl:
        peak = max(peak, c)
        mdd = min(mdd, c / peak - 1.0)
    # [2026-09-07] 현재 낙폭 = 창 안 고점 대비 **현재가**.
    #   max_dd 는 창 안 고점~저점이라 회복을 반영하지 않는다. 사건이 창에서
    #   밀려나야 풀리므로 해제가 시장이 아니라 달력으로 결정된다.
    #   2026-09-07 실측: max_dd -0.3700 인데 저점 대비 +26.87% 회복, regime=BULL.
    #   cur_dd 는 같은 날 -0.2006 이다. 이쪽이 "지금 폭락 중인가" 를 잰다.
    win_hi = max(cl)
    win_lo = min(cl)
    cur_dd = (cl[-1] / win_hi - 1.0) if win_hi > 0 else 0.0
    rebound = (cl[-1] / win_lo - 1.0) if win_lo > 0 else 0.0
    return {"date_min": w[0][0], "date_max": w[-1][0], "used_rows": len(w),
            "max_dd": mdd, "day_ret": cl[-1] / cl[-2] - 1.0,
            "cur_dd": cur_dd, "window_high": win_hi, "window_low": win_lo,
            "rebound_from_low": rebound}
