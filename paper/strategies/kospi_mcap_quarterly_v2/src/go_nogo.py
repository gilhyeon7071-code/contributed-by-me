"""09-28 가부 판정을 사람의 문장이 아니라 파일 상태로 낸다 (2026-09-20 신설).

왜: 가부 9개는 되돌릴 수 없는 결정(10-01 발주)의 문이다. 그런데 지금은 내가 산문으로 보고하고
사용자가 그 산문을 판단한다. 2026-09-20 에 "246 passed, 전부 통과" 로 부분 실행을 전수로 보고한 적이 있다.
같은 형태가 가부에서 나오면 되돌릴 수 없다.

규칙 셋:
  1. **모름을 통과로 바꾸지 않는다.** 증거가 없으면 UNKNOWN 이고, UNKNOWN 은 '간다' 가 아니다
  2. 기준마다 **읽은 파일 경로**를 같이 낸다. 못 대면 그건 판정이 아니다
  3. 증거에 **나이 한도**를 둔다. 낡은 증거는 UNKNOWN 이다 (그때 통과였다는 뜻일 뿐)

명령으로 적을 수 없는 기준은 UNKNOWN 으로 남는다 — 그게 드러나는 것 자체가 이 도구의 소득이다.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path

V2 = Path(__file__).resolve().parents[1]
ROOT = V2.parents[2]                      # E:\1_Data
STATE = V2 / "data" / "state"
PLAN_RUNS = V2 / "data" / "plan_runs"
TEST_RUNS = V2 / "data" / "test_runs"

STRATEGY_CAPITAL = 60_000_000

PASS, FAIL, UNKNOWN = "PASS", "FAIL", "UNKNOWN"


class Result:
    def __init__(self, n, title):
        self.n, self.title = n, title
        self.status, self.detail, self.sources = UNKNOWN, "확인 안 함", []

    def set(self, status, detail, *sources):
        self.status, self.detail = status, detail
        self.sources = [str(s) for s in sources]
        return self


def _age_days(p: Path, now: dt.datetime) -> float:
    return (now - dt.datetime.fromtimestamp(p.stat().st_mtime)).total_seconds() / 86400


def _newest(d: Path, pattern: str):
    if not d.is_dir():
        return None
    xs = sorted(d.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return xs[0] if xs else None


def _load(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:                                   # 못 읽으면 통과가 아니라 모름이다
        return {"__unreadable__": str(e)}


# ---------------------------------------------------------------- 기준 1·2 계획 산출물
def c1_krx_reproduces(now, r):
    runs = sorted([d for d in PLAN_RUNS.iterdir() if d.is_dir()], reverse=True) if PLAN_RUNS.is_dir() else []
    if not runs:
        return r.set(UNKNOWN, f"계획 실행 폴더가 없다: {PLAN_RUNS}")
    run = runs[0]
    chk = _newest(run, "input_*_check.json")
    cnt = _newest(run, "universe_*_count.json")
    if not (chk and cnt):
        return r.set(UNKNOWN, f"{run.name}: input_check / universe_count 중 하나가 없다", run)
    a, b = _load(chk), _load(cnt)
    sa, sb = a.get("status"), b.get("status")
    if sa == "OK" and sb == "OK":
        return r.set(PASS, f"{run.name}: D1 입력검사 OK, D2 모집단 OK", chk, cnt)
    return r.set(FAIL, f"{run.name}: input_check={sa} universe_count={sb}", chk, cnt)


def c2_handcalc(now, r):
    runs = sorted([d for d in PLAN_RUNS.iterdir() if d.is_dir()], reverse=True) if PLAN_RUNS.is_dir() else []
    if not runs:
        return r.set(UNKNOWN, "계획 실행 폴더 없음")
    run = runs[0]
    summ = _newest(run, "target_*_summary.json")
    if not summ:
        return r.set(UNKNOWN, f"{run.name}: target summary 없음", run)
    s = _load(summ)
    hand = _newest(run, "handcalc_*.json") or _newest(V2 / "data" / "fixtures", "handcalc_*.json")
    if not hand:
        # 기계가 읽을 손계산 대조 기록이 없다. 사람이 맞다고 말한 것은 증거가 아니다
        return r.set(UNKNOWN, f"D7 status={s.get('status')} 이지만 **손계산 대조 기록 파일이 없다** "
                              f"(handcalc_*.json). 사람의 확인은 여기서 증거가 되지 않는다", summ)
    h = _load(hand)
    diffs = [k for k, v in h.get("expected", {}).items() if s.get(k) != v]
    return r.set(PASS if not diffs else FAIL,
                 "손계산과 일치" if not diffs else f"불일치 {diffs}", summ, hand)


# ---------------------------------------------------------------- 기준 3·6 실발주 시험
def _test_run_report(now):
    if not TEST_RUNS.is_dir():
        return None, None
    d = sorted([p for p in TEST_RUNS.iterdir() if p.is_dir()], reverse=True)
    if not d:
        return None, None
    rep = _newest(d[0], "*summary*.json") or _newest(d[0], "*.json")
    return d[0], rep


def c3_order_roundtrip(now, r):
    run, rep = _test_run_report(now)
    if not rep:
        return r.set(UNKNOWN, f"실발주 시험 산출물이 없다 (09-22 예정): {TEST_RUNS}")
    s = _load(rep)
    ok = s.get("order_no") and s.get("filled") and s.get("balance_returned")
    return r.set(PASS if ok else FAIL, f"{run.name}: 주문번호·체결·잔고 = {bool(ok)}", rep)


def c6_cancel_confirmed(now, r):
    run, rep = _test_run_report(now)
    if not rep:
        return r.set(UNKNOWN, "취소 시험 산출물 없음 (09-22 --cancel-test 예정)")
    s = _load(rep)
    v = s.get("cancel_confirmed_by_query")
    if v is None:
        return r.set(UNKNOWN, f"{run.name}: cancel_confirmed_by_query 키가 없다", rep)
    return r.set(PASS if v else FAIL, f"취소 조회 확인 = {v}", rep)


# ---------------------------------------------------------------- 기준 4·8(일부) 시험
def _pytest(expr, label):
    try:
        p = subprocess.run([sys.executable, "-m", "pytest", "-q", "-k", expr, "tests/"],
                           cwd=ROOT, capture_output=True, timeout=900)
        out = (p.stdout or b"").decode("utf-8", "replace")
        m = re.search(r"(\d+) passed", out)
        nf = re.search(r"(\d+) failed", out)
        if p.returncode == 0 and m:
            return PASS, f"{label}: {m.group(1)} passed"
        if nf:
            return FAIL, f"{label}: {nf.group(1)} failed"
        return UNKNOWN, f"{label}: rc={p.returncode}, 통과 수를 못 읽음"
    except subprocess.TimeoutExpired:
        return UNKNOWN, f"{label}: 시간 초과 — 통과로 읽지 않는다"


def c4_incident_fixture(now, r):
    st, detail = _pytest("incident or intent_vs_result or mismatch", "사고1(의도≠결과) fixture")
    return r.set(st, detail, "tests/")


# ---------------------------------------------------------------- 기준 5·7 계좌
def _account_report(now, max_age_days=3.0):
    cands = []
    for d in (V2 / "data", ROOT / "2_Logs", V2):
        f = _newest(d, "check_account_*.json")
        if f:
            cands.append(f)
    if not cands:
        return None, "계좌 점검 산출물(check_account_*.json)이 없다"
    f = max(cands, key=lambda p: p.stat().st_mtime)
    age = _age_days(f, now)
    if age > max_age_days:
        return None, f"계좌 점검이 {age:.1f}일 낡았다 ({f.name}) — 그때 값이지 지금 값이 아니다"
    return f, None


def c5_no_topn_holdings(now, r):
    f, why = _account_report(now)
    if not f:
        return r.set(UNKNOWN, why)
    s = _load(f)
    holdings = s.get("holdings")
    if holdings is None:
        return r.set(UNKNOWN, "holdings 키가 없다", f)
    return r.set(PASS if len(holdings) == 0 else FAIL, f"모의계좌 보유 {len(holdings)}종목", f)


def c7_buying_power(now, r):
    f, why = _account_report(now)
    if not f:
        return r.set(UNKNOWN, why)
    s = _load(f)
    v = s.get("nrcvb_buy_amt", s.get("account", {}).get("nrcvb_buy_amt"))
    if v is None:
        return r.set(UNKNOWN, "nrcvb_buy_amt 키가 없다", f)
    v = float(v)
    return r.set(PASS if v >= STRATEGY_CAPITAL else FAIL,
                 f"nrcvb_buy_amt={v:,.0f} vs 전략자본 {STRATEGY_CAPITAL:,}", f)


# ---------------------------------------------------------------- 기준 8 일일 운용
def c8_daily_ops(now, r):
    log = STATE / "daily_log.jsonl"
    if not log.exists():
        return r.set(UNKNOWN, f"일일 기록이 없다: {log}")
    rows = [json.loads(ln) for ln in log.read_text(encoding="utf-8").splitlines() if ln.strip()]
    jobs = {x.get("job") for x in rows}
    self_run = [x for x in rows if x.get("run_at")]
    missing = {"evening", "morning", "afternoon"} - jobs
    st_test, d_test = _pytest("daily_ops", "판정기 시험")
    parts = [f"기록 {len(rows)}건", f"직업 {sorted(jobs)}", d_test]
    if missing or st_test != PASS or not self_run:
        return r.set(UNKNOWN if missing else (FAIL if st_test == FAIL else UNKNOWN),
                     f"증거 4개 중 부족: 없는 작업={sorted(missing) or '없음'} / {' / '.join(parts)}", log)
    return r.set(PASS, " / ".join(parts), log)


# ---------------------------------------------------------------- 기준 9 v41.1 청산 전용
def c9_v41_exit_only(now, r):
    checks, srcs = [], []

    for name, pat in (("run_intraday_paper.bat", r'PAPER_EXIT_ONLY.*=.*"?1'),
                      ("run_paper_daily.bat", r'PAPER_EXIT_ONLY.*=.*"?1')):
        p = ROOT / name
        if not p.exists():
            checks.append((UNKNOWN, f"{name} 없음")); continue
        hit = re.search(pat, p.read_text(encoding="utf-8", errors="replace"))
        checks.append((PASS if hit else FAIL, f"{name} 기본값 1 {'있음' if hit else '없음'}"))
        srcs.append(p)

    try:                                                     # 환경변수가 기본값을 덮어쓰지 않았나
        ps = subprocess.run(["powershell", "-NoProfile", "-Command",
                             "[Environment]::GetEnvironmentVariable('PAPER_EXIT_ONLY','User');"
                             "[Environment]::GetEnvironmentVariable('PAPER_EXIT_ONLY','Machine')"],
                            capture_output=True, timeout=60)
        vals = [ln.strip() for ln in (ps.stdout or b"").decode("utf-8", "replace").splitlines() if ln.strip()]
        checks.append((PASS if not vals else FAIL, f"환경변수 {'비어 있음' if not vals else vals}"))
    except Exception as e:
        checks.append((UNKNOWN, f"환경변수 확인 실패: {e}"))
    if os.environ.get("PAPER_EXIT_ONLY") not in (None, "", "1"):
        checks.append((FAIL, f"현재 세션 PAPER_EXIT_ONLY={os.environ['PAPER_EXIT_ONLY']}"))

    st = ROOT / "2_Logs" / "intraday_loop_status_latest.json"
    if not st.exists():
        checks.append((UNKNOWN, "intraday_loop_status_latest.json 없음"))
    else:
        age = _age_days(st, now)
        s = _load(st)
        sw = s.get("switches", {})
        good = sw.get("exit_only_mode") is True and str(sw.get("raw", {}).get("PAPER_EXIT_ONLY")) == "1"
        if age > 5:
            checks.append((UNKNOWN, f"상태 파일이 {age:.1f}일 낡았다 — 지금 상태가 아니다"))
        else:
            checks.append((PASS if good else FAIL, f"exit_only_mode={sw.get('exit_only_mode')}"))
        srcs.append(st)

    ps_json = ROOT / "paper" / "paper_state.json"
    if not ps_json.exists():
        checks.append((UNKNOWN, "paper_state.json 없음"))
    else:
        n = len(_load(ps_json).get("open_positions", []) or [])
        checks.append((PASS if n == 0 else FAIL, f"v41.1 보유 {n}종목"))
        srcs.append(ps_json)

    sts = [c[0] for c in checks]
    overall = FAIL if FAIL in sts else (UNKNOWN if UNKNOWN in sts else PASS)
    return r.set(overall, " | ".join(f"{s}:{d}" for s, d in checks), *srcs)


CRITERIA = [
    (1, "KRX 3종으로 D1·D2 재현", c1_krx_reproduces),
    (2, "D6·D7 이 손계산과 일치", c2_handcalc),
    (3, "주문 1건 접수·체결·잔고", c3_order_roundtrip),
    (4, "사고1(의도≠결과) fixture 통과", c4_incident_fixture),
    (5, "모의계좌 topn 잔여 보유 0", c5_no_topn_holdings),
    (6, "취소 1건 조회로 확인", c6_cancel_confirmed),
    (7, "매수가능금액 ≥ 6,000만원", c7_buying_power),
    (8, "일일 운용 증거 4개", c8_daily_ops),
    (9, "v41.1 청산 전용 + 보유 0", c9_v41_exit_only),
]


def evaluate(now=None):
    now = now or dt.datetime.now()
    out = []
    for n, title, fn in CRITERIA:
        r = Result(n, title)
        try:
            fn(now, r)
        except Exception as e:                               # 터지면 통과가 아니라 모름이다
            r.set(UNKNOWN, f"검사 중 예외: {type(e).__name__}: {e}")
        out.append(r)
    return out


def render(results):
    lines = ["09-28 가부 판정 — 기준 9개", ""]
    for r in results:
        mark = {PASS: "PASS   ", FAIL: "FAIL   ", UNKNOWN: "UNKNOWN"}[r.status]
        lines.append(f"[{mark}] {r.n}. {r.title}")
        lines.append(f"          {r.detail}")
        for s in r.sources:
            lines.append(f"          <- {s}")
    n_pass = sum(1 for r in results if r.status == PASS)
    n_fail = sum(1 for r in results if r.status == FAIL)
    n_unk = sum(1 for r in results if r.status == UNKNOWN)
    lines += ["", f"PASS {n_pass} / FAIL {n_fail} / UNKNOWN {n_unk}"]
    verdict = "간다 (GO)" if n_pass == len(results) else "미룬다 (NO-GO) — 12-30 선정 / 2027-01-04 집행"
    lines += [f"판정: {verdict}",
              "  규칙: 전부 PASS 여야 간다. UNKNOWN 은 통과가 아니다(확인 안 된 것이다)."]
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description="09-28 가부 기준 9개를 파일 상태로 판정한다")
    ap.add_argument("--json", action="store_true", help="기계용 출력")
    a = ap.parse_args(argv)
    rs = evaluate()
    if a.json:
        print(json.dumps([{"n": r.n, "title": r.title, "status": r.status,
                           "detail": r.detail, "sources": r.sources} for r in rs],
                         ensure_ascii=False, indent=2))
    else:
        print(render(rs))
    return 0 if all(r.status == PASS for r in rs) else 1


if __name__ == "__main__":
    sys.exit(main())
