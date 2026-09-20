# -*- coding: utf-8 -*-
"""생산 코드가 읽는 `*_latest.*` 산출물이 낡거나 없으면 **소리를 낸다**.

2026-09-07 신규. 사용자 지적에서 출발했다 — "왜 매일 여기까지다 하고 다음날 문제가 나오나".
답은 이 시스템의 기본값이 **조용한 실패**라는 것이었다. 같은 형태를 셋 이상 실측했다.

```
orderflow_observer_state    status=NO_INPUT 41일    경보 0건   (PLANS 220)
kis_open_orders_latest      54일 낡음 status=PASS   화면은 "미체결 없음" 단정
paper/kis_fills_api_*.csv   5개월간 거의 헤더만     원장 대조가 빈 통 (PLANS 217)
```

**설계 원칙 넷.**

1. 등록부를 손으로 관리하지 않는다. 생산 진입점 코드를 스캔해 참조를 **자동 도출**한다.
   손 등록부는 소비처가 늘면 조용히 뒤처진다 — 그게 이 도구가 막으려는 실패 그 자체다.
2. 기준선은 **달력**이다. 산출물 자신의 날짜에서 기대치를 뽑으면 항상 통과한다.
   (memory: feedback_freshness_reference_must_be_calendar)
3. 나이 정책이 없는 항목은 PASS 가 아니라 **UNSPECIFIED** 다. 모르는 것을 통과시키지 않는다.
   (memory: feedback_absence_is_not_evidence)
4. **매매를 막지 않는다.** 기본 rc=0. 우선순위 ③ 이므로 관측이 목적이고 차단이 목적이 아니다.
   `--strict` 를 준 호출자만 rc=2 를 받는다.

    python tools/artifact_freshness_guard.py            # 관측 + 경보
    python tools/artifact_freshness_guard.py --no-alert # 조회만
    python tools/artifact_freshness_guard.py --strict   # 결정경로 위반 시 rc=2
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# 배치 콘솔이 cp949 라 em-dash 같은 문자에서 print 가 죽는다.
# 도구가 일을 다 끝내고 **출력에서** 죽으면 rc!=0 이 되어 성공이 실패로 기록된다.
def _force_utf8_stdout() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


_force_utf8_stdout()

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
TOOLS_DIR = ROOT / "tools"
VIBE = Path(r"E:\vibe\buffett")

# ── 생산 진입점 ────────────────────────────────────────────────────────────
# 여기 없는 파일의 참조는 이 도구가 못 본다. 진입점이 늘면 여기에 넣는다.
# role: decision = 매매 판단에 닿는다 / display = 화면·보고에만 닿는다
PROD_ENTRYPOINTS: List[Dict[str, str]] = [
    {"path": str(ROOT / "generate_candidates_v41_1.py"), "role": "decision"},
    {"path": str(ROOT / "paper_engine" / "entry.py"), "role": "decision"},
    {"path": str(ROOT / "p0_daily_check.py"), "role": "decision"},
    {"path": str(ROOT / "intraday_paper_loop.py"), "role": "decision"},
    {"path": str(TOOLS_DIR / "kis_order_dispatch_from_exec.py"), "role": "decision"},
    {"path": str(VIBE / "tools" / "build_dashboard_state.py"), "role": "display"},
    {"path": str(VIBE / "tools" / "build_dashboard_state_v2.py"), "role": "display"},
]

# [2026-09-20] **고정 감시 목록** — 코드의 `*_latest` 참조로는 안 잡히지만 멈추면 안 되는 것.
#   V2 일일 운용(저녁·아침·오후)이 조용히 멈추면 지금은 아무도 모른다. 예전 "아침 배치가 8거래일간
#   10초 만에 성공" 과 같은 형태의 구멍이다. 경로를 직접 적고 거래일 한도를 건다.
#   `only_if_exists` 는 아직 시작 전이라 없는 것(첫 재구성 뒤 생김) — 생기기 전에는 감시하지 않는다.
V2_STATE = ROOT / "paper" / "strategies" / "kospi_mcap_quarterly_v2" / "data" / "state"
FIXED_WATCH = [
    {"path": V2_STATE / "daily_log.jsonl", "role": "decision", "max_age_trading_days": 1,
     "note": "V2 일일 운용 세 작업의 기록. 하루라도 비면 예약이 죽은 것", "only_if_exists": False},
    {"path": V2_STATE / "next_action.json", "role": "decision", "max_age_trading_days": 1,
     "note": "저녁 작업이 정한 다음 거래일 할 일", "only_if_exists": True},
    {"path": V2_STATE / "nav_log.jsonl", "role": "decision", "max_age_trading_days": 1,
     "note": "전략 자산 기록. 래치 판정의 입력", "only_if_exists": True},
    {"path": V2_STATE / "current_target.json", "role": "decision", "max_age_trading_days": 70,
     "note": "현재 목표(분기마다 갱신). 70거래일이면 한 분기를 넘긴 것", "only_if_exists": True},
]

# 산출물이 놓일 수 있는 곳. 앞에서부터 먼저 찾은 것을 쓴다(소비자의 _first_existing 관례와 같다).
SEARCH_DIRS = [
    ROOT / "_cache",
    LOG_DIR,
    ROOT / "paper",
    VIBE / "runs",
    ROOT / "docs" / "research" / "integrity",
    # [2026-09-13] **거짓 MISSING 을 냈다.** replay_orders_latest.json 은
    #   VIBE 아래 data/orders 에 멀쩡히 있는데(09-12 09:08) 이 목록에 없어서
    #   "어느 탐색 경로에도 없다" 로 보고했다. 없는 것과 못 찾은 것은 다른 말이다.
    VIBE / "data" / "orders",
    VIBE / "data" / "ledger",
]

# ── 아무도 안 읽는 감시 산출물 ──────────────────────────────────────────
# 이 도구는 "생산 코드가 읽는 산출물" 만 본다. 그런데 감시 장치들의 산출물은
# 읽는 코드가 없어서 **감시 대상이 아니다.** 즉 감시가 조용히 멈춰도 모른다.
# 2026-09-07 실측: order_capability_canary 는 계좌가 막힌 것을 알아내는 유일한 경로인데
# 그 자신이 며칠 안 돌아도 아무도 알 수 없는 상태였다.
# [2026-09-13] **출력물을 입력으로 오분류하지 않는다.**
#   역할은 '이 이름이 어느 진입점 소스에 나오나' 로 정해진다. 그런데 도구가 **쓰는** 파일도
#   그 소스에 이름이 나오므로 decision 으로 잡힌다.
#   실측: kis_order_dispatch_slicing_preview_latest.json 은 발주 도구의 출력이고
#   읽는 곳이 없다. 그대로 두면 CRITICAL -> status_overall=FAIL 이 되어 **거짓 경보**가 된다.
#   매일 울리는 경보는 무시된다.
ROLE_OVERRIDE: Dict[str, str] = {
    "kis_order_dispatch_slicing_preview_latest.json": "display",
}

ALWAYS_WATCH: Dict[str, Dict[str, Any]] = {
    "order_capability_canary_latest.json": {"consumers": ["(감시장치)"], "role": "decision"},
    "broker_ledger_reconcile_latest.json": {"consumers": ["(감시장치)"], "role": "decision"},
    "round_preflight_daily_latest.json": {"consumers": ["(감시장치)"], "role": "display"},
    "panel_baseline_check_latest.json": {"consumers": ["(감시장치)"], "role": "display"},
    # [2026-09-12] 사이드카·서킷브레이커 판정의 사슬. 셋 다 감시 밖에 있었다.
    #   market_event_gate 는 **설정(events_file)으로** 참조돼 이름이 코드에 안 나온다 -
    #   그래서 참조 스캔으로는 영원히 안 잡힌다. 여기에 직접 올린다.
    #   이 둘이 낡으면 "이벤트 없음" 과 "관측 못 함" 이 구별되지 않는다.
    "market_event_gate_latest.json": {"consumers": ["paper_engine/entry.py(설정 경로)"], "role": "decision"},
    "market_anomaly_detector_status_latest.json": {"consumers": ["paper_engine/entry.py"], "role": "decision"},
    "kis_ws_index_latest.json": {"consumers": ["market_anomaly_detector.py"], "role": "decision"},
}

# ── 나이 정책 ──────────────────────────────────────────────────────────────
# 단위는 **거래일**이다. 주말·휴장으로 인한 정상 지연을 위반으로 세지 않기 위해서다.
# 근거 없는 값을 넣지 않는다. 모르는 항목은 여기 없고, 그러면 UNSPECIFIED 로 보고된다.
MAX_AGE_TRADING_DAYS: Dict[str, int] = {
    # 매 거래일 생산되는 것 (관측된 산출 주기 기준)
    "candidates_latest.csv": 1,
    "intraday_prices_latest.csv": 1,
    "orderflow_observer_state_latest.json": 1,
    "production_risk_playbook_latest.json": 1,
    "p1_entry_gate_status_latest.json": 1,
    "pending_entry_status_latest.json": 1,
    "pending_entry_signals_latest.csv": 1,
    "dashboard_state_latest.json": 1,
    "integrated_ops_snapshot_latest.json": 1,
    "account_equity_history_latest.json": 1,
    "krx_price_integrity_status_latest.json": 1,
    "kis_open_orders_latest.json": 1,
    "surge_realtime_latest.json": 1,
    "surge_lob_latest.csv": 1,
    "order_scoring_metrics_latest.json": 1,
    "intraday_loop_status_latest.json": 1,
    "intraday_watchdog_status_latest.json": 1,
    # [2026-09-12] 사이드카·서킷브레이커 판정의 입력과 산출. 감시 밖에 있었다.
    #   이 둘이 낡으면 "이벤트 없음" 과 "관측 못 함" 이 구별되지 않는다.
    #   진입 게이트는 2026-09-12 부터 fail-closed 지만, 낡고 있다는 사실 자체를 알아야 한다.
    "market_event_gate_latest.json": 1,
    "market_anomaly_detector_status_latest.json": 1,
    "kis_ws_index_latest.json": 1,
    # 느린 주기 (관측된 갱신 간격 기준)
    "dart_fundamental_latest.csv": 5,
    "pykrx_fundamental_latest.csv": 5,
    "krx_watchlist_latest.csv": 5,
    "market_master_latest.json": 5,
    "macro_signal_latest.json": 5,
    # [2026-09-13] 20 -> 250. **생산자가 없다**(전수 grep). 종목명 사전이라 갱신 주체가
    #   애초에 없었고, 한도 20일은 갱신을 전제한 값이라 매일 STALE 을 냈다.
    #   실측: 사전 4,037종목이 전진 원장 최근 282종목을 **결손 0건**으로 덮는다.
    #   완전히 빼지 않는 이유 - 신규 상장이 쌓이면 언젠가 이름이 빠진다.
    #   250거래일(약 1년)이면 그때는 걸린다. 결손이 생기면 그게 진짜 신호다.
    "code_name_cache_latest.json": 250,
    # [2026-09-13] 한도가 없어 낡아도 안 걸리던 것들. 성격을 나눠 정한다.
    #   adaptive_entry_condition_policy_design 은 **진입 수량을 바꾸는 입력**이라
    #   entry.py 가 직접 5거래일 가드를 갖는다. 여기서도 같이 본다.
    "adaptive_entry_condition_policy_design_latest.json": 5,
    #   아래는 표시 경로이거나 도구의 출력물이다. 매매를 안 바꾸지만
    #   낡은 채 화면에 나가면 잘못된 해석을 만든다. 20거래일(약 1달)로 둔다.
    "sector_system_diagnostics_latest.json": 20,
    "surge_event_money_pullback_combined_observation_layer_latest.json": 20,
    "ledger_live_fill_sync_latest.json": 20,
    "preopen_5min_check_latest.json": 20,
    "pending_entry_status_shadow_latest.json": 20,
    #   슬라이싱 미리보기는 발주 도구의 **출력**이다(입력이 아니다).
    #   86거래일 안 돌았다는 사실만 보이면 된다.
    "kis_order_dispatch_slicing_preview_latest.json": 20,
    # 감시 산출물. 전부 매 거래일 생산된다(카나리아는 평일 09:20, 나머지는 08:30 배치).
    # 신선도 가드가 배치에서 먼저 돌므로 전날 것을 본다 -> 1거래일이 정상이다.
    "order_capability_canary_latest.json": 1,
    "broker_ledger_reconcile_latest.json": 1,
    "round_preflight_daily_latest.json": 1,
    "panel_baseline_check_latest.json": 1,
}

# [2026-09-20] v41.1 을 청산 전용으로 바꾼 뒤(PLANS 520), 진입 입력은 **설계상** 갱신되지 않는다.
#   설계된 상태를 매일 경보하면 그 경보는 무시되고, 10-01 부터 나올 진짜 경보가 같이 묻힌다
#   (topn ROUND_CLOSED 와 같은 형태). 면제는 **증거가 최신일 때만** — 스위치가 풀리면 다시 STALE 로 돌아간다.
EXIT_ONLY_EXEMPT = {"adaptive_entry_condition_policy_design_latest.json"}
EXIT_ONLY_EVIDENCE = LOG_DIR / "intraday_loop_status_latest.json"
EXIT_ONLY_EVIDENCE_MAX_TD = 5

DEFAULT_ALERT_LEVEL = {"decision": "error", "display": "warn"}
REF_PAT = re.compile(r"([A-Za-z0-9_]*_latest)\.(json|csv|parquet|xlsx)")

# 주기 실측의 한계. 형제가 이보다 적으면 추정하지 않고 UNSPECIFIED 로 남긴다.
CADENCE_MIN_SAMPLES = 3
CADENCE_MAX_LIMIT_TD = 20
# 나이 정책이 없어도 이만큼 늙었으면 조용히 두지 않는다. 판정이 아니라 **주의 환기**다.
UNSPECIFIED_SUSPICION_TD = 10

# ── 날짜별 시리즈 (사전등록 RD_20260901_topn 산출물) ────────────────────────
# `*_latest.*` 가 아니라 `이름_YYYYMMDD.확장자` 로 매 거래일 쌓이는 산출물이다.
# 이것들이 조용히 멈추면 40거래일 뒤에 "판정 불가"가 된다 — A3~A5 의 재료이기 때문이다.
# 파일명의 ymd 축이 시리즈마다 다르다(저녁 생산분은 신호일, 아침 생산분은 집행일)이라
# "오늘 파일이 있어야 한다"로 짜면 오경보가 난다. 산출 시각을 실측해 기준일을 고른다.
DATED_SERIES_DIRS = [LOG_DIR / "topn"]
# [2026-09-19] 라운드가 종결되면 그 폴더의 산출물은 **설계된 대로** 멈춘다.
#   멈춘 것을 STALE 로 울리면 매일 울고, 진짜 경보가 그 속에 묻힌다.
#   종결은 폴더 안 ROUND_CLOSED.json 하나로 표시한다(파일이 진실, 코드는 그걸 읽을 뿐).
SERIES_CLOSED_MARKER = "ROUND_CLOSED.json"
SERIES_GRACE_HOURS = 3          # 산출 시각 중앙값 + 이만큼 지나야 "오늘 것"을 기대한다
SERIES_MIN_SAMPLES = 3          # 표본이 적으면 추정하지 않고 UNSPECIFIED

# 날짜가 안 붙는 롤링 산출물의 한도(거래일). 근거를 아는 것만 적는다.
# positions.csv 는 일부러 넣지 않았다 — 매 회차가 아니라 **변동이 있을 때만** 쓰인다
# (재대조는 오늘 11:25 에 돌았는데 파일은 09-03 자다). 한도를 넣으면 오경보가 된다.
ROLLING_MAX_AGE_TD: Dict[str, int] = {
    "forward_ledger.csv": 2,            # 저녁 회차가 매 거래일 갱신. 당일 저녁 전이라 +1
    "stage1_status_forward.json": 2,    # 같은 회차 산출
}


def _now() -> dt.datetime:
    return dt.datetime.now()


def _load_calendar():
    """휴장 달력. 없으면 None 을 돌려주고 호출부가 달력 없음을 **보고한다**(조용히 넘기지 않는다)."""
    try:
        sys.path.insert(0, str(ROOT))
        from holiday_manager import HolidayManager  # type: ignore

        return HolidayManager()
    except Exception as exc:  # pragma: no cover - 환경 문제
        print(f"[FRESH_GUARD] calendar_unavailable {type(exc).__name__}: {exc}")
        return None


def _trading_days_between(cal, older: dt.date, newer: dt.date) -> Optional[int]:
    """older 이후 newer 까지의 거래일 수. 달력이 없으면 None."""
    if cal is None or newer < older:
        return None
    n = 0
    cur = older
    guard = 0
    while cur < newer and guard < 400:
        cur = cur + dt.timedelta(days=1)
        guard += 1
        try:
            if cal.is_market_open(cur.strftime("%Y%m%d")):
                n += 1
        except Exception:
            return None
    return n


def collect_references() -> Dict[str, Dict[str, Any]]:
    """생산 진입점에서 `*_latest.*` 참조를 긁는다. 이름 -> {consumers, role}."""
    refs: Dict[str, Dict[str, Any]] = {}
    for ep in PROD_ENTRYPOINTS:
        p = Path(ep["path"])
        if not p.exists():
            # 진입점이 사라진 것도 조용하면 안 된다.
            refs.setdefault("__entrypoint_missing__", {"consumers": [], "role": "decision", "missing_entrypoints": []})
            refs["__entrypoint_missing__"]["missing_entrypoints"].append(str(p))
            continue
        src = p.read_text(encoding="utf-8", errors="replace")
        for m in REF_PAT.finditer(src):
            name = f"{m.group(1)}.{m.group(2)}"
            e = refs.setdefault(name, {"consumers": [], "role": "display"})
            if p.name not in e["consumers"]:
                e["consumers"].append(p.name)
            if ep["role"] == "decision":
                e["role"] = "decision"

    # 출력물을 입력으로 오분류한 것을 바로잡는다
    for name, role in ROLE_OVERRIDE.items():
        if name in refs:
            refs[name]["role"] = role

    # 읽는 코드가 없어도 반드시 본다.
    for name, meta in ALWAYS_WATCH.items():
        e = refs.setdefault(name, {"consumers": [], "role": meta["role"]})
        for c in meta["consumers"]:
            if c not in e["consumers"]:
                e["consumers"].append(c)
        if meta["role"] == "decision":
            e["role"] = "decision"
    return refs


def resolve(name: str) -> Optional[Path]:
    for d in SEARCH_DIRS:
        p = d / name
        if p.is_file():
            return p
    return None


def observed_cadence_td(name: str, path: Path, cal) -> Optional[Dict[str, Any]]:
    """날짜별 형제 파일(`stem_YYYYMMDD*.ext`)에서 **실측 산출 주기**를 뽑는다.

    한도를 내 추측으로 정하지 않기 위해서다. 기준점(now)은 여전히 달력이므로,
    산출이 완전히 멈춘 산출물은 형제들의 짧은 간격 때문에 오히려 더 빨리 걸린다.
    """
    stem = name.rsplit(".", 1)[0].removesuffix("_latest")
    ext = name.rsplit(".", 1)[1]
    if not stem:
        return None
    pat = re.compile(rf"^{re.escape(stem)}_(\d{{8}})(?:_\d+)?\.{re.escape(ext)}$")
    days = set()
    try:
        for sib in path.parent.iterdir():
            m = pat.match(sib.name)
            if m:
                try:
                    days.add(dt.datetime.strptime(m.group(1), "%Y%m%d").date())
                except ValueError:
                    continue
    except OSError:
        return None
    ds = sorted(days)[-30:]
    if len(ds) < CADENCE_MIN_SAMPLES:
        return None
    gaps = []
    for a, b in zip(ds, ds[1:]):
        g = _trading_days_between(cal, a, b)
        if g is not None and g > 0:
            gaps.append(g)
    if len(gaps) < CADENCE_MIN_SAMPLES - 1:
        return None
    gaps.sort()
    median = gaps[len(gaps) // 2] if len(gaps) % 2 else (gaps[len(gaps) // 2 - 1] + gaps[len(gaps) // 2]) / 2
    limit = min(CADENCE_MAX_LIMIT_TD, int(median) + 1)
    return {"limit": max(1, limit), "median_gap_td": median, "samples": len(ds),
            "last_dated": ds[-1].strftime("%Y%m%d")}


SERIES_PAT = re.compile(r"^(.*?)_?(\d{8})(_[A-Za-z0-9_]+)?(\.[A-Za-z0-9]+)$")


def _prev_trading_day(cal, d: dt.date) -> Optional[dt.date]:
    if cal is None:
        return None
    try:
        s = cal.previous_trading_day(d.strftime("%Y%m%d"), include_target=False)
        return dt.datetime.strptime(str(s), "%Y%m%d").date()
    except Exception:
        return None


def _series_reference_day(cal, typical_hour: int, now: dt.datetime) -> Optional[dt.date]:
    """이 시리즈에 대해 "이 날짜 파일은 있어야 한다"는 기준 거래일.

    산출 시각(실측 중앙값)에 유예를 더한 시각이 지나야 오늘 것을 기대한다.
    저녁 22시에 나오는 시리즈를 낮 12시에 재면서 오늘 것을 요구하면 매일 오경보다.
    """
    if cal is None:
        return None
    today = now.date()
    try:
        open_today = bool(cal.is_market_open(today.strftime("%Y%m%d")))
    except Exception:
        return None
    if open_today and now.hour >= min(23, typical_hour + SERIES_GRACE_HOURS):
        return today
    return _prev_trading_day(cal, today)


def evaluate_dated_series(cal) -> List[Dict[str, Any]]:
    """`이름_YYYYMMDD.확장자` 로 매 거래일 쌓이는 산출물이 멈췄는지 본다."""
    rows: List[Dict[str, Any]] = []
    now = _now()
    for d in DATED_SERIES_DIRS:
        if not d.is_dir():
            rows.append({"series": str(d), "kind": "series", "status": "MISSING",
                         "role": "decision", "consumers": ["(디렉터리)"],
                         "reason": "시리즈 디렉터리가 없다"})
            continue
        closed_marker = d / SERIES_CLOSED_MARKER
        if closed_marker.is_file():
            try:
                info = json.loads(closed_marker.read_text(encoding="utf-8"))
            except Exception:
                info = {}
            rows.append({"series": f"{d.name}/*", "kind": "series", "status": "CLOSED",
                         "role": "decision", "consumers": [info.get("round_id", "(라운드)")],
                         "reason": f"라운드 종결({info.get('closed_at', '?')}) — 갱신 멈춤이 설계된 상태"})
            continue
        buckets: Dict[str, List[Any]] = {}
        for p in d.iterdir():
            if not p.is_file():
                continue
            m = SERIES_PAT.match(p.name)
            if not m:
                continue
            key = f"{m.group(1)}{m.group(3) or ''}{m.group(4)}"
            try:
                ymd = dt.datetime.strptime(m.group(2), "%Y%m%d").date()
            except ValueError:
                continue
            buckets.setdefault(key, []).append((ymd, p))

        for key in sorted(buckets):
            items = sorted(buckets[key], key=lambda x: x[0])
            ymds = [y for y, _ in items]
            row: Dict[str, Any] = {
                "series": f"{d.name}/{key}",
                "kind": "series",
                "role": "decision",           # 사전등록 판정 재료다
                "consumers": ["RD_20260901_topn"],
                "samples": len(items),
                "latest_ymd": ymds[-1].strftime("%Y%m%d"),
                "latest_size": items[-1][1].stat().st_size,
            }
            if len(items) < SERIES_MIN_SAMPLES:
                row["status"] = "UNSPECIFIED"
                row["reason"] = f"표본 {len(items)}개 — 산출 주기를 실측할 수 없다"
                rows.append(row)
                continue

            hours = sorted(dt.datetime.fromtimestamp(p.stat().st_mtime).hour for _, p in items)
            typical_hour = hours[len(hours) // 2]
            row["typical_hour"] = typical_hour

            gaps = []
            for a, b in zip(ymds, ymds[1:]):
                g = _trading_days_between(cal, a, b)
                if g:
                    gaps.append(g)
            if not gaps:
                row["status"] = "NO_CALENDAR" if cal is None else "UNSPECIFIED"
                row["reason"] = "거래일 간격을 못 쟀다"
                rows.append(row)
                continue
            gaps.sort()
            limit = max(1, min(CADENCE_MAX_LIMIT_TD, gaps[len(gaps) // 2] + 1))
            row["limit_td"] = limit
            row["limit_source"] = "observed"

            ref = _series_reference_day(cal, typical_hour, now)
            if ref is None:
                row["status"] = "NO_CALENDAR"
                row["reason"] = "휴장 달력을 못 읽어 기준 거래일을 못 정한다"
                rows.append(row)
                continue
            row["reference_ymd"] = ref.strftime("%Y%m%d")
            lag = _trading_days_between(cal, ymds[-1], ref)
            lag = 0 if lag is None else lag
            row["lag_td"] = lag

            # 0바이트를 곧바로 결함으로 읽으면 안 된다. 장중 시리즈는 10분마다 덮어써서
            # 자르고 쓰는 순간에 0으로 관측된다(2026-09-07 실측: 12:15 에 0, 12:31 에 46,557).
            # 방금 손댄 파일은 "쓰는 중"으로 보고, 오래 0인 것만 결함으로 센다.
            latest_mtime = dt.datetime.fromtimestamp(items[-1][1].stat().st_mtime)
            write_age_sec = (now - latest_mtime).total_seconds()
            row["latest_mtime_age_sec"] = round(write_age_sec, 1)
            if row["latest_size"] == 0 and write_age_sec > 300:
                row["status"] = "STALE"
                row["reason"] = f"최신 파일({row['latest_ymd']})이 {int(write_age_sec)}초째 0바이트다"
            elif row["latest_size"] == 0:
                row["status"] = "FRESH"
                row["reason"] = "0바이트이나 방금 쓰인 파일이라 기록 중으로 본다"
            elif lag > limit:
                row["status"] = "STALE"
                row["reason"] = (f"최신 {row['latest_ymd']} 이 기준일 {row['reference_ymd']} 보다 "
                                 f"거래일 {lag}일 뒤처짐 > 한도 {limit}일")
            else:
                row["status"] = "FRESH"
                row["reason"] = ""
            rows.append(row)
    return rows


def evaluate_rolling(cal) -> List[Dict[str, Any]]:
    """날짜가 안 붙는 롤링 산출물. 한도를 아는 것만 잰다."""
    rows: List[Dict[str, Any]] = []
    now = _now()
    for d in DATED_SERIES_DIRS:
        if (d / SERIES_CLOSED_MARKER).is_file():
            continue          # 종결된 라운드 — 롤링 산출물도 같이 멈춘다(위 CLOSED 줄로 이미 보고했다)
        for name, limit in ROLLING_MAX_AGE_TD.items():
            p = d / name
            row: Dict[str, Any] = {"series": f"{d.name}/{name}", "kind": "rolling",
                                   "role": "decision", "consumers": ["RD_20260901_topn"],
                                   "limit_td": limit, "limit_source": "registry"}
            if not p.is_file():
                row["status"] = "MISSING"
                row["reason"] = "등록된 롤링 산출물이 없다"
                rows.append(row)
                continue
            mtime = dt.datetime.fromtimestamp(p.stat().st_mtime)
            age = _trading_days_between(cal, mtime.date(), now.date())
            row["mtime"] = mtime.strftime("%Y-%m-%d %H:%M:%S")
            row["age_trading_days"] = age
            if age is None:
                row["status"] = "NO_CALENDAR"
                row["reason"] = "거래일 나이를 못 쟀다"
            elif age > limit:
                row["status"] = "STALE"
                row["reason"] = f"거래일 {age}일 경과 > 한도 {limit}일"
            else:
                row["status"] = "FRESH"
                row["reason"] = ""
            rows.append(row)
    return rows


def evaluate(refs: Dict[str, Dict[str, Any]], cal) -> Dict[str, Any]:
    now = _now()
    today = now.date()
    rows: List[Dict[str, Any]] = []
    entrypoint_missing = refs.pop("__entrypoint_missing__", None)

    for name in sorted(refs):
        meta = refs[name]
        path = resolve(name)
        row: Dict[str, Any] = {
            "artifact": name,
            "role": meta["role"],
            "consumers": sorted(meta["consumers"]),
            "max_age_trading_days": MAX_AGE_TRADING_DAYS.get(name),
        }
        if path is None:
            row["status"] = "MISSING"
            row["path"] = None
            row["reason"] = "생산 코드가 참조하는데 어느 탐색 경로에도 없다"
            rows.append(row)
            continue

        mtime = dt.datetime.fromtimestamp(path.stat().st_mtime)
        row["path"] = str(path)
        row["mtime"] = mtime.strftime("%Y-%m-%d %H:%M:%S")
        row["age_days"] = round((now - mtime).total_seconds() / 86400.0, 2)
        row["age_trading_days"] = _trading_days_between(cal, mtime.date(), today)

        limit = row["max_age_trading_days"]
        row["limit_source"] = "registry" if limit is not None else None
        if limit is None:
            obs = observed_cadence_td(name, path, cal)
            if obs is not None:
                limit = obs["limit"]
                row["max_age_trading_days"] = limit
                row["limit_source"] = "observed"
                row["observed_cadence"] = obs

        if limit is None:
            row["status"] = "UNSPECIFIED"
            row["reason"] = "나이 정책이 없고 날짜별 형제도 부족해 주기를 실측할 수 없다 — 통과시키지 않는다"
        elif row["age_trading_days"] is None:
            row["status"] = "NO_CALENDAR"
            row["reason"] = "휴장 달력을 못 읽어 거래일 나이를 못 잰다"
        elif row["age_trading_days"] > limit:
            row["status"] = "STALE"
            row["reason"] = f"거래일 {row['age_trading_days']}일 경과 > 한도 {limit}일"
        else:
            row["status"] = "FRESH"
            row["reason"] = ""
        if row["status"] == "STALE" and name in EXIT_ONLY_EXEMPT:
            active, why = exit_only_state(cal, today)
            row["exit_only_check"] = why
            if active:
                row["status"] = "EXPECTED_STALE"
                row["reason"] = f"v41.1 청산 전용이라 진입 입력이 갱신되지 않는다(설계된 상태). {why}"
        rows.append(row)

    for w in FIXED_WATCH:                      # 고정 감시 목록
        pth = Path(w["path"])
        row = {"artifact": pth.name, "role": w["role"], "consumers": ["FIXED_WATCH"],
               "max_age_trading_days": w["max_age_trading_days"], "path": str(pth),
               "limit_source": "fixed", "note": w["note"]}
        if not pth.is_file():
            if w.get("only_if_exists"):
                continue                        # 아직 시작 전 — 없는 게 정상
            row.update(status="MISSING", reason=f"고정 감시 대상이 없다 — {w['note']}")
            rows.append(row)
            continue
        mtime = dt.datetime.fromtimestamp(pth.stat().st_mtime)
        row["mtime"] = mtime.strftime("%Y-%m-%d %H:%M:%S")
        row["age_days"] = round((now - mtime).total_seconds() / 86400.0, 2)
        row["age_trading_days"] = _trading_days_between(cal, mtime.date(), today)
        if row["age_trading_days"] is None:
            row.update(status="NO_CALENDAR", reason="휴장 달력을 못 읽어 거래일 나이를 못 잰다")
        elif row["age_trading_days"] > w["max_age_trading_days"]:
            row.update(status="STALE",
                       reason=f"거래일 {row['age_trading_days']}일 경과 > 한도 {w['max_age_trading_days']}일 — {w['note']}")
        else:
            row.update(status="FRESH", reason="")
        rows.append(row)

    series_rows = evaluate_dated_series(cal) + evaluate_rolling(cal)

    def pick(*st: str) -> List[Dict[str, Any]]:
        return [r for r in rows if r["status"] in st]

    def spick(*st: str) -> List[Dict[str, Any]]:
        return [r for r in series_rows if r["status"] in st]

    bad = pick("STALE", "MISSING", "NO_CALENDAR")
    unspec = pick("UNSPECIFIED")
    unspec_old = sorted(
        [r for r in unspec if (r.get("age_trading_days") or 0) >= UNSPECIFIED_SUSPICION_TD],
        key=lambda r: -(r.get("age_trading_days") or 0),
    )
    out: Dict[str, Any] = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "calendar": "holiday_manager" if cal is not None else "UNAVAILABLE",
        "counts": {
            "total": len(rows),
            "fresh": len(pick("FRESH")),
            "stale": len(pick("STALE")),
            "missing": len(pick("MISSING")),
            "unspecified": len(unspec),
            "unspecified_and_old": len(unspec_old),
            "no_calendar": len(pick("NO_CALENDAR")),
            "expected_stale": len(pick("EXPECTED_STALE")),
        },
        "expected_stale": pick("EXPECTED_STALE"),
        "series_counts": {
            "total": len(series_rows),
            "fresh": len(spick("FRESH")),
            "stale": len(spick("STALE")),
            "missing": len(spick("MISSING")),
            "unspecified": len(spick("UNSPECIFIED")),
            "no_calendar": len(spick("NO_CALENDAR")),
        },
        "series_violations": spick("STALE", "MISSING", "NO_CALENDAR"),
        "series_rows": series_rows,
        "decision_path_violations": [r for r in bad if r["role"] == "decision"],
        "display_path_violations": [r for r in bad if r["role"] == "display"],
        "unspecified": unspec,
        "unspecified_and_old": unspec_old,
        "rows": rows,
    }
    if entrypoint_missing:
        out["entrypoint_missing"] = entrypoint_missing["missing_entrypoints"]
    return out


def exit_only_state(cal, today) -> tuple:
    """v41.1 진입 스위치가 꺼져 있나. (면제해도 되나, 근거 한 줄).
    증거는 장중 루프 상태 파일의 실효 스위치다(런처 기본값이 아니라 **돌던 값**).
    증거가 없거나 낡으면 면제하지 않는다 — 모름을 통과로 바꾸지 않는다."""
    p = EXIT_ONLY_EVIDENCE
    if not p.is_file():
        return (False, "EXIT_ONLY_EVIDENCE_MISSING")
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        return (False, f"EXIT_ONLY_EVIDENCE_UNREADABLE:{type(exc).__name__}")
    age_td = _trading_days_between(cal, dt.datetime.fromtimestamp(p.stat().st_mtime).date(), today)
    sw = d.get("switches") or {}
    raw = str(((sw.get("raw") or {}).get("PAPER_EXIT_ONLY")) or "").strip()
    active = bool(sw.get("exit_only_mode")) or raw == "1"
    if age_td is None or age_td > EXIT_ONLY_EVIDENCE_MAX_TD:
        return (False, f"EXIT_ONLY_EVIDENCE_STALE:age_td={age_td}")
    if not active:
        return (False, f"EXIT_ONLY_OFF:exit_only_mode={sw.get('exit_only_mode')} raw={raw}")
    return (True, f"exit_only_mode={sw.get('exit_only_mode')} raw={raw} evidence_age_td={age_td}")


def build_alert_text(out: Dict[str, Any]) -> Optional[str]:
    dec = out["decision_path_violations"]
    dis = out["display_path_violations"]
    ser = out.get("series_violations") or []
    miss_ep = out.get("entrypoint_missing") or []
    if not dec and not dis and not ser and not miss_ep and not (out.get("unspecified_and_old") or []):
        return None
    lines = [f"[산출물 신선도] {out['generated_at']}"]
    if miss_ep:
        lines.append(f"진입점 파일 없음 {len(miss_ep)}건: " + ", ".join(Path(p).name for p in miss_ep))
    if ser:
        lines.append(f"▲ 사전등록 판정재료 {len(ser)}건 (RD_20260901_topn)")
        for r in ser[:8]:
            lines.append(f"  {r['series']} [{r['status']}] {r['reason']}")
    if dec:
        lines.append(f"■ 결정경로 {len(dec)}건")
        for r in dec[:8]:
            lines.append(f"  {r['artifact']} [{r['status']}] {r['reason']} <- {','.join(r['consumers'])}")
    if dis:
        lines.append(f"□ 표시경로 {len(dis)}건")
        for r in dis[:8]:
            lines.append(f"  {r['artifact']} [{r['status']}] {r['reason']}")
    for r in out.get("expected_stale") or []:   # 설계된 상태는 경보하지 않고 한 줄로 남긴다
        lines.append(f"◇ {r['artifact']} [설계된 상태] {r['reason']}")
    u = out["counts"]["unspecified"]
    if u:
        lines.append(f"· 나이정책 미지정 {u}건 (판정 보류. 통과 아님)")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-alert", action="store_true", help="경보를 보내지 않고 조회만 한다")
    ap.add_argument("--strict", action="store_true", help="결정경로 위반이 있으면 rc=2")
    ap.add_argument("--out-dir", default=str(LOG_DIR), help="산출 경로 재지정(시험용)")
    args = ap.parse_args()

    cal = _load_calendar()
    out = evaluate(collect_references(), cal)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = _now().strftime("%Y%m%d_%H%M%S")
    body = json.dumps(out, ensure_ascii=False, indent=2)
    (out_dir / f"artifact_freshness_guard_{ts}.json").write_text(body, encoding="utf-8")
    (out_dir / "artifact_freshness_guard_latest.json").write_text(body, encoding="utf-8")

    c = out["counts"]
    print(f"[FRESH_GUARD] total={c['total']} fresh={c['fresh']} stale={c['stale']} "
          f"missing={c['missing']} unspecified={c['unspecified']} no_calendar={c['no_calendar']}")
    sc = out["series_counts"]
    print(f"[FRESH_GUARD] 사전등록 시리즈 total={sc['total']} fresh={sc['fresh']} stale={sc['stale']} "
          f"missing={sc['missing']} unspecified={sc['unspecified']}")
    for r in out["series_rows"]:
        mark = "OK " if r["status"] == "FRESH" else "!! "
        extra = (f"latest={r.get('latest_ymd')} ref={r.get('reference_ymd')} lag={r.get('lag_td')} "
                 f"limit={r.get('limit_td')}" if r.get("kind") == "series"
                 else f"age_td={r.get('age_trading_days')} limit={r.get('limit_td')}")
        print(f"  {mark}[사전등록] {r['series']:<40} {r['status']:<12} {extra}")
    for r in out["decision_path_violations"]:
        print(f"  [결정] {r['artifact']:<52} {r['status']:<11} {r['reason']}")
    for r in out["display_path_violations"]:
        print(f"  [표시] {r['artifact']:<52} {r['status']:<11} {r['reason']}")
    for r in out.get("unspecified_and_old") or []:
        print(f"  [미지정·오래됨] {r['artifact']:<46} age_td={r.get('age_trading_days')} <- {','.join(r['consumers'])}")
    print(f"[FRESH_GUARD] 미지정 총 {out['counts']['unspecified']}건 "
          f"(그중 {UNSPECIFIED_SUSPICION_TD}거래일 이상 {len(out.get('unspecified_and_old') or [])}건)")

    text = build_alert_text(out)
    if text and not args.no_alert:
        try:
            sys.path.insert(0, str(TOOLS_DIR))
            from notify_channels import send_alert  # type: ignore

            level = "error" if out["decision_path_violations"] else "warn"
            res = send_alert(text, level=level, extra={"source": "artifact_freshness_guard"},
                             cooldown_sec=6 * 3600)
            # 경보 전송 실패가 조용하면 이 도구도 같은 병에 걸린다. 반드시 찍는다.
            print(f"[FRESH_GUARD] alert ok={res.get('ok')} suppressed={res.get('suppressed')} "
                  f"channels={res.get('channels')}")
        except Exception as exc:
            print(f"[FRESH_GUARD] alert_failed {type(exc).__name__}: {exc}")

    if args.strict and (out["decision_path_violations"] or out["series_violations"]):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
