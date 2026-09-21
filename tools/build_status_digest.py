# -*- coding: utf-8 -*-
"""하루 한 번 시스템 상태를 요약해 사람에게 민다.

2026-08-26 신규. 사용자 지적에서 나왔다 - **"묻기 전에는 현재 상태를 알 수 없다."**
맞는 말이었다. 지금까지 만든 것들은 전부 파일에 쓰고 끝났고,
알림은 `error`/`critical` 만 텔레그램으로 간다. 즉 **정상이면 아무 소식이 없다.**

그러면 "조용하다" 와 "죽었다" 가 구분되지 않는다.
[[project_1data_alert_delivery_outage]] 가 정확히 그 모습이었다 -
3개월 18일간 45건이 미전송이었는데 조용해서 아무도 몰랐다.

**그래서 정상일 때도 보낸다.** 요약이 안 오면 그것 자체가 신호다.

## 상태를 어디서 읽나

예약작업 rc 가 아니라 **산출물의 나이**를 본다
([[feedback_check_artifact_age_first]]). 작업이 "성공" 으로 끝나도 파일을
안 만들었으면 소용없고, 파일이 신선하면 작업은 돈 것이다.

읽기 전용이다.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(r"E:\1_Data")
LOGS = ROOT / "2_Logs"
REB = LOGS / "rebalance"


def _age(p: Path) -> str:
    if not p.exists():
        return "없음"
    mins = (dt.datetime.now() - dt.datetime.fromtimestamp(p.stat().st_mtime)).total_seconds() / 60
    if mins < 90:
        return "%d분 전" % int(mins)
    if mins < 60 * 40:
        return "%.1f시간 전" % (mins / 60)
    return "%.1f일 전" % (mins / 60 / 24)


def _json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return None


def section_book() -> list[str]:
    out = []
    st = _json(REB / "rebal_state.json")
    if not st:
        return ["장부    아직 개시 전"]
    hist = st.get("history") or []
    cash = float(st.get("cash") or 0)
    npos = len(st.get("positions") or {})

    eq_rows = []
    f = REB / "rebal_equity.csv"
    if f.exists():
        with f.open(encoding="utf-8-sig") as fh:
            eq_rows = list(csv.DictReader(fh))
    if eq_rows:
        last = eq_rows[-1]
        equity = float(last["equity"])
        idx = float(last["equity_idx"])
        exc = float(last["excess_pp"])
        out.append("장부    자산 %s원 (%+.2f%%)  보유 %d종목  현금 %.1f%%"
                   % (format(int(equity), ","), 100 * (idx - 1), npos,
                      100 * cash / max(equity, 1)))
        out.append("        벤치마크 대비 %+.2f%%p   기준일 %s (거래일 %d)"
                   % (exc, last["date"], len(eq_rows)))
    else:
        out.append("장부    보유 %d종목  현금 %s원  (자산곡선 아직 없음)"
                   % (npos, format(int(cash), ",")))

    if hist:
        h = hist[-1]
        out.append("        마지막 체결 %s  %d건 체결 / %d건 건너뜀  비용 %s원"
                   % (h.get("exec_date"), h.get("filled", 0), h.get("skipped", 0),
                      format(int(h.get("fees") or 0), ",")))
    return out


def section_next_rebalance(step: int) -> list[str]:
    st = _json(REB / "rebal_state.json")
    if not st or not (st.get("history") or []):
        return []
    last = max(str(h.get("exec_date")) for h in st["history"])
    dates = sorted(Path(p).name.split("_")[2] for p in
                   glob.glob(str(ROOT / "krx_daily_archive" / "krx_daily_*_clean.parquet")))
    since = len([d for d in dates if d > last])
    left = max(0, step - since - 1)
    return ["        다음 리밸런싱 %d거래일 뒤 (마지막 %s 이후 %d일)" % (left, last, since)]


def section_loop() -> list[str]:
    hb = _json(LOGS / "run_intraday_paper.lock" / "heartbeat.json")
    if not hb:
        return ["루프    하트비트 없음 (장 시간 밖이면 정상)"]
    ok, tot = int(hb.get("steps_ok") or 0), int(hb.get("steps_total") or 0)
    mark = "정상" if (not tot or ok >= tot) else "스텝실패 %d/%d" % (ok, tot)
    lines = ["루프    cycle %s  %s  하트비트 %s"
             % (hb.get("cycle"), mark, _age(LOGS / "run_intraday_paper.lock" / "heartbeat.json"))]
    sd = _json(LOGS / "intraday_loop_status_latest.json")
    if sd:
        bad = [str(s.get("label")) for s in (sd.get("steps") or [])
               if isinstance(s, dict) and not s.get("ok")]
        if bad:
            lines.append("        실패 스텝: %s" % ", ".join(bad[:5]))
    return lines


def section_batches() -> list[str]:
    arc = sorted(glob.glob(str(ROOT / "krx_daily_archive" / "krx_daily_*_clean.parquet")))
    latest = Path(arc[-1]).name.split("_")[2] if arc else "없음"
    return [
        "배치    아침 %s   저녁장부 %s"
        % (_age(LOGS / "full_auto_hidden_last.txt"), _age(LOGS / "run_rebalance_daily_last.txt")),
        "        일봉 아카이브 최신 %s (%s)" % (latest, _age(Path(arc[-1])) if arc else "-"),
    ]


def section_measure() -> list[str]:
    f = LOGS / "measure" / "spread_passrate_history.csv"
    if not f.exists():
        return []
    with f.open(encoding="utf-8-sig") as fh:
        rows = [r for r in csv.DictReader(fh) if r.get("mode") == "full"]
    if not rows:
        return []
    last = rows[-1]
    return ["측정    스프레드 통과율 %.1f%% (%s, 전수 %d회차)"
            % (100 * float(last["passrate_weighted"]), last["ts"][:16].replace("T", " "), len(rows))]


def section_index() -> list[str]:
    """지수 대비. [2026-08-26] 그전까지 시스템에 코스피가 아예 없어서
    '시장 대비 어땠나' 를 답할 수 없었다(PLANS 120)."""
    f = LOGS / "index_daily_history.csv"
    if not f.exists():
        return []
    with f.open(encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))
    out = []
    # [2026-08-27 정정] 2001 은 KOSPI200 이다. KOSDAQ 은 1001.
    for code, name in (("0001", "KOSPI"), ("1001", "KOSDAQ"), ("2001", "KOSPI200")):
        s_ = sorted([r for r in rows if r["index_code"] == code], key=lambda r: r["date"])
        if len(s_) < 2:
            continue
        a, b_ = float(s_[-2]["close"]), float(s_[-1]["close"])
        out.append("        %-7s %s %.2f  (%+.2f%%)" % (name, s_[-1]["date"], b_, 100 * (b_ / a - 1)))
    return (["지수"] + out) if out else []


def section_v411() -> list[str]:
    """v41.1 페이퍼 장부. [2026-08-26] 장부가 둘인데 요약이 새 것만 보여주고 있었다.
    '내 시스템 전체가 얼마인가' 에 답이 안 됐다."""
    d = _json(LOGS / "account_equity_history_latest.json")
    if not d:
        return []
    lt = d.get("latest") or {}
    cap = float(lt.get("capital_total") or 0)
    real = float(lt.get("realized_total_krw") or 0)
    eq = float(lt.get("equity_est") or 0)
    out = ["v41.1   자산추정 %s원 (%+.2f%%)  누적실현 %s원"
           % (format(int(eq), ","), 100 * (eq / cap - 1) if cap else 0, format(int(real), ","))]
    # as_of 와 생성시각이 어긋나면 그것을 드러낸다(오늘 날짜로 옛 자료를 판정하는 패턴)
    asof, gen = str(lt.get("as_of_ymd") or ""), str(d.get("generated_at") or "")
    if asof and gen and asof != gen[:10].replace("-", ""):
        out.append("        [주의] 기준일 %s 인데 생성 %s - 최신 자료가 아니다" % (asof, gen[:10]))
    return out


def section_topn() -> list[str]:
    """RD_20260901_topn 1단계 하네스. [2026-09-03 신설]

    하네스는 매일 도는데 2_Logs/topn/ 을 읽는 곳이 0건이라 **화면에 전혀 안 나왔다.**
    보이지 않으면 멈춰 있어도 모른다. 한 줄이라도 매일 나오게 한다.
    """
    tp = LOGS / "topn"
    if not tp.exists():
        return []
    out = []

    # [2026-09-21] **종결된 라운드를 진행 중처럼 보이면 안 된다.**
    #   09-16 종결 / 09-17 보유 전량 청산인데 이 화면은 09-15 자 낡은 산출물을 읽어
    #   오늘 아침에도 "보유 6/6" 을 띄웠다. 브로커 보유 0 이 정본이다.
    #   같은 마커를 artifact_freshness_guard 와 broker_ledger_reconcile 은 09-19 에 읽게 고쳤는데
    #   **이 화면만 빠져 있었다** — 같은 결함의 세 번째 복사본.
    closed = _json(tp / "ROUND_CLOSED.json")
    if closed:
        out.append("하네스  [종결] %s  %s" % (closed.get("round_id", "?"),
                                              str(closed.get("why") or "")[:60]))
        out.append("        산출물은 더 갱신되지 않는다(설계된 상태). 브로커 보유 0 이 정본")
        return out

    # 후보 전진 - 이게 멈추면 하네스가 죽은 것이다
    led = tp / "forward_ledger.csv"
    last_sig, ndays = "", 0
    try:
        import pandas as pd
        c = pd.read_csv(led, dtype=str, encoding="utf-8-sig")
        ndays = c["date"].nunique()
        last_sig = str(c["date"].max())
    except Exception:
        pass

    # 최신 집행 상태
    ex = sorted(tp.glob("exec_*.json"))
    d = _json(ex[-1]) if ex else {}
    pos = d.get("A3_positions")
    mx = d.get("A3_max_pos")
    a4 = d.get("A4_fill_rate")
    a5 = d.get("A5_slippage_median")
    ds = str(d.get("dispatch_status") or "")

    head = "하네스  후보 %d일 누적 (최신 %s)" % (ndays, last_sig or "?")
    if d:
        head += "  보유 %s/%s" % (pos if pos is not None else "?", mx if mx is not None else "?")
    out.append(head)

    if d:
        a4s = ("%.1f%%" % (100 * a4)) if isinstance(a4, (int, float)) else "n/a"
        a5s = ("%+.4f%%" % (100 * a5)) if isinstance(a5, (int, float)) else "n/a"
        out.append("        %s  주문 %s건  체결률 %s  슬리피지 %s"
                   % (d.get("date", "?"), d.get("orders_submitted", "?"), a4s, a5s))
        if ds:
            out.append("        발주 %s" % ds[:70])

    # 배치가 실제로 돌았는지 - rc 와 나이
    for name, log in (("저녁", tp / "run_topn_evening.log"), ("장중", tp / "run_topn_intraday.log")):
        if not log.exists():
            out.append("        [주의] %s 로그 없음" % name)
            continue
        try:
            tail = log.read_text(encoding="utf-8", errors="replace").splitlines()
            ends = [l for l in tail if l.startswith("[END]")]
            out.append("        %s %s  (%s)" % (name, ends[-1].strip() if ends else "END 기록 없음", _age(log)))
        except Exception:
            out.append("        [주의] %s 로그를 읽지 못했다" % name)
    return out


def section_live_account() -> list[str]:
    f = LOGS / "kis_account_snapshot_latest.json"
    d = _json(f)
    if not d:
        return ["실계좌  스냅샷 없음"]
    sm = d.get("summary") or {}
    return ["실계좌  보유 %s종목  평가 %s원  스냅샷 %s"
            % (sm.get("positions", "?"), format(int(float(sm.get("total_notional_krw") or 0)), ","), _age(f))]


def section_research_ledger() -> list[str]:
    """시도 원장 신선도 + DSR 이 실제로 쓰는 n_trials.

    [2026-08-30] DSR(deflated Sharpe) 은 "몇 번 시도했는가" 로 샤프를 깎는다.
      그 값이 종전에 len(param_grid)=4 였고 실제 시도는 375+ 였다.
      고치니 DSR 0.5928(PASS) -> 0.0055(FAIL) 로 뒤집혔다.
      원장을 갱신하지 않으면 그 상태로 조용히 되돌아간다 - 그래서 매일 눈에 보이게 한다.
    """
    out: list[str] = []
    p = LOGS / "research_trial_ledger.json"
    if not p.exists():
        return ["검정", "        시도원장 파일 없음  [주의] DSR n_trials 가 param_grid 로 떨어진다"]
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        return ["검정", "        시도원장 읽기 실패 [주의] %s" % type(exc).__name__]

    entries = obj.get("entries") or []
    total = sum(int(e.get("n") or 0) for e in entries if isinstance(e, dict))
    updated = str(obj.get("updated") or "").strip()
    out.append("        시도원장 %d회 (%d건, 갱신 %s)" % (total, len(entries), updated or "-"))

    # 검증 리포트가 실제로 쓴 값 — 원장과 어긋나면 그것이 신호다
    try:
        rep = json.loads((LOGS / "backtest_validation_latest.json").read_text(encoding="utf-8"))
        for g in (rep.get("gate_results") or []):
            if g.get("name") != "deflated_sharpe_ratio":
                continue
            d = g.get("details") or {}
            used, srcname = d.get("n_trials"), str(d.get("n_trials_source") or "?")
            mark = "  [주의] 과소" if "UNDERSTATED" in srcname else ""
            try:
                dsr_s = "%.4f" % float(d.get("deflated_sharpe_ratio"))
            except Exception:
                dsr_s = str(d.get("deflated_sharpe_ratio"))
            out.append("        DSR      n_trials=%s (%s) dsr=%s%s"
                       % (used, srcname, dsr_s, mark))
            break
    except Exception:
        pass

    # [2026-08-31] R&D 승인 상태 + 목적 + 라운드. 기본값은 SUSPENDED 다.
    #   중단이 능동적 행동을 요구하면 중단은 일어나지 않는다 - 그래서 매일 보이게 한다.
    try:
        auth = json.loads((LOGS / "rd_authorization.json").read_text(encoding="utf-8"))
        st = str(auth.get("status") or "SUSPENDED").upper()
        until = str(auth.get("authorized_until") or "")
        expired = False
        if st == "AUTHORIZED" and until:
            try:
                expired = dt.datetime.strptime(until[:10], "%Y-%m-%d").date() < dt.date.today()
            except Exception:
                expired = True
        eff = "SUSPENDED" if (st != "AUTHORIZED" or expired) else "AUTHORIZED"
        mark = "" if eff == "AUTHORIZED" else "  [주의] 새 확증 라운드 금지"
        out.append("        R&D      %s%s%s" % (eff, ("  만료 %s" % until) if until else "", mark))
        obj = str(auth.get("current_objective") or "UNDECIDED")
        if obj == "UNDECIDED":
            out.append("        목적      UNDECIDED  [주의] 2026-07-27 중단조건 충족 후 미결정")
        else:
            out.append("        목적      %s" % obj)
    except Exception:
        out.append("        R&D      승인 파일 없음  [주의] SUSPENDED 로 취급")

    # 라운드 (사전등록·동결 상태)
    try:
        rdir = ROOT / "docs" / "research" / "rounds"
        fro = dra = 0
        for p2 in sorted(rdir.glob("RD_*")):
            if not p2.is_dir():
                continue
            if (p2 / "frozen.json").exists():
                fro += 1
            else:
                dra += 1
        if fro or dra:
            out.append("        라운드    동결 %d / 초안 %d" % (fro, dra))
    except Exception:
        pass

    # 원장보다 새 탐색 산출물이 있으면 "기록 안 한 라운드" 일 수 있다
    try:
        ud = dt.datetime.strptime(updated[:10], "%Y-%m-%d").date() if updated else None
    except Exception:
        ud = None
    if ud:
        newest, newest_d = None, None
        for f in glob.glob(str(LOGS / "design" / "*")):
            fp = Path(f)
            if not fp.is_file():
                continue
            d = dt.datetime.fromtimestamp(fp.stat().st_mtime).date()
            if newest_d is None or d > newest_d:
                newest, newest_d = fp.name, d
        if newest_d and newest_d > ud:
            out.append("        [주의] design 산출물이 원장보다 최신 (%s, %s) - 라운드 미기록 확인"
                       % (newest[:38], newest_d))
    return ["검정"] + out

def section_data_age() -> list[str]:
    """마스터 데이터 나이. 낡은 줄 모르고 분석에 쓰면 결론이 오염된다."""
    out = []
    # [2026-08-27] **생산이 실제로 읽는 파일**을 본다. 감사용 파일 나이는 소용없다.
    #   섹터   paper_engine/common.py:1009 _load_sector_db() -> _cache/sector_ssot.csv
    #   시총   generate_candidates_v41_1.py:_load_pykrx_fundamental_snapshot()
    #          -> _cache/pykrx_fundamental_latest.csv (market_cap/listed_shares)
    #   처음엔 krx_current_industry_master(_partial, 생산 산출물 아님)와
    #   krx_sector_master(감사용)를 감시하고 있었다 - 낡았어도 매매에 안 닿는 파일이었다.
    for label, pat, warn_days in (("시총/주식수(생산)", "pykrx_fundamental_latest.csv", 7),
                                  ("섹터 SSOT(생산)", "sector_ssot.csv", 90),
                                  ("DART 재무(생산)", "dart_fundamental_latest.csv", 3)):
        fs = sorted(glob.glob(str(ROOT / "_cache" / pat)))
        if not fs:
            out.append("        %-14s 파일 없음" % label)
            continue
        f = Path(fs[-1])
        days = (dt.datetime.now() - dt.datetime.fromtimestamp(f.stat().st_mtime)).days
        mark = "  <-- 낡음" if days > warn_days else ""
        out.append("        %-14s %s (%d일)%s" % (label, f.name[:44], days, mark))
    return (["자료"] + out) if out else []


def section_tasks() -> list[str]:
    """예약작업 실패 감시.

    [2026-08-26] 감사에서 나온 것 - **오늘만 6개 작업이 실패했는데 5개는 아무도 몰랐다.**
    내가 만든 작업 5개에 실패 알림을 붙였지만 시스템에는 작업이 37개다.
    37개를 일일이 감싸는 대신 **결과를 매일 한 번 훑는다.**
    Disabled 는 이미 꺼둔 것이므로 세지 않는다(따로 표시만 한다).
    """
    ps = (
        "Get-ScheduledTask | Where-Object { $_.TaskName -match '^(VIBE|Buffett|STOC)' } | "
        "ForEach-Object { $i = $_ | Get-ScheduledTaskInfo; "
        "[pscustomobject]@{n=$_.TaskName; s=$_.State.ToString(); "
        "rc=$i.LastTaskResult; t=$(if($i.LastRunTime){$i.LastRunTime.ToString('s')}else{''})} } | "
        "ConvertTo-Json -Compress"
    )
    try:
        r = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
                           capture_output=True, timeout=120)
        data = json.loads(r.stdout.decode("utf-8", "replace") or "[]")
    except Exception as e:
        return ["작업    상태 조회 실패: %s" % str(e)[:60]]
    if isinstance(data, dict):
        data = [data]

    RUNNING, NEVER = 267009, 267011
    cutoff = (dt.datetime.now() - dt.timedelta(hours=30)).isoformat(timespec="seconds")
    bad, disabled = [], 0
    for x in data:
        if str(x.get("s")) == "Disabled":
            disabled += 1
            continue
        rc = x.get("rc")
        if rc in (0, RUNNING, NEVER) or rc is None:
            continue
        if str(x.get("t") or "") < cutoff:      # 30시간 넘은 실패는 오래된 것
            continue
        bad.append((str(x.get("n")), rc, str(x.get("t"))[5:16].replace("T", " ")))
    out = ["작업    %d개 중 최근 실패 %d개 (Disabled %d개 제외)" % (len(data), len(bad), disabled)]
    for n, rc, t in sorted(bad, key=lambda z: z[2], reverse=True)[:8]:
        out.append("        실패: %-38s rc=%-6s %s" % (n[:38], rc, t))
    return out


def section_costs() -> list[str]:
    """[2026-09-21] 종전에는 `trade_costs.csv` 의 **마지막 행**(개별 체결 1건)을 "왕복" 이라 띄웠다.
    그 값 0.398% 는 68건 중 **최솟값**이었다(평균 0.720% / 중앙 0.576% / 최대 2.729%).
    모델값(모든 검증이 서는 상수)과 실현값(지나간 체결)이 같은 글자를 쓰고 있었다.
    게다가 '마지막 행' 은 '최근 청산' 이 아니다 — 파일 순서였다.
    단일 출처 `tools/cost_model.py` 가 **재현해서** 둘을 따로 낸다."""
    try:
        from cost_model import summary_lines
        return summary_lines()
    except Exception as exc:
        return ["비용    모름 — cost_model 실패: %s: %s" % (type(exc).__name__, exc)]


def main() -> int:
    ap = argparse.ArgumentParser(description="일일 상태 요약")
    ap.add_argument("--step", type=int, default=10, help="리밸런싱 간격(거래일)")
    ap.add_argument("--send", action="store_true", help="텔레그램으로 보낸다")
    ap.add_argument("--out", default=str(LOGS / "status_digest_latest.txt"))
    args = ap.parse_args()

    # [2026-08-26] 일련번호. "요약이 안 오면 그것이 신호" 라는 설계는
    #   사용자가 매일 왔는지 세고 있어야 성립한다. 번호를 붙이면 나중에 빠진 날을 찾을 수 있다.
    seq_f = LOGS / "status_digest_seq.json"
    seq = 0
    try:
        seq = int(json.loads(seq_f.read_text(encoding="utf-8")).get("seq") or 0)
    except Exception:
        pass
    if args.send:
        seq += 1
        try:
            seq_f.write_text(json.dumps({"seq": seq, "ts": dt.datetime.now().isoformat(timespec="seconds")}),
                             encoding="utf-8")
        except Exception:
            pass
    lines = ["[상태 #%d] %s" % (seq, dt.datetime.now().strftime("%Y-%m-%d %H:%M")), ""]
    lines += section_book()
    lines += section_next_rebalance(args.step)
    lines += section_loop()
    lines += section_batches()
    lines += section_v411()
    lines += section_topn()
    lines += section_live_account()
    lines += section_index()
    lines += section_measure()
    lines += section_research_ledger()
    lines += section_costs()
    lines += section_data_age()
    lines += section_tasks()

    # [2026-08-26] 경고 집계. 처음엔 "실패 스텝"/"없음" 만 셌는데
    #   "<-- 낡음", "[주의]" 를 놓쳤다. 요약이 이상을 표시해 놓고도 "경고 없음" 이라 적었다.
    _KEYS = ("실패 스텝", "[주의]", "<-- 낡음", "파일 없음", "스냅샷 없음", "        실패:")
    warn = [l for l in lines if any(k in l for k in _KEYS)]
    lines += ["", "경고    %s" % ("없음" if not warn else "%d건 (위 표시)" % len(warn))]
    text = "\n".join(lines)
    print(text)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(text, encoding="utf-8")

    if args.send:
        # [2026-09-04] **전송 결과를 확인한다.**
        #   예전에는 send_alert 의 반환을 버리고 예외만 안 나면 "[SENT]" 를 찍었다.
        #   send_alert 는 fail_silent=True 가 기본이라 실패해도 예외를 안 던지므로
        #   **텔레그램이 죽어도 [SENT] 가 찍히고 rc=0 으로 끝났다.**
        #   이 저장소는 경보 단절로 3개월 18일을 날린 이력이 있다. 그때도 증상이 같았다.
        #   "조용함 = 죽음" 을 구분하려고 만든 장치가 자기 실패는 조용히 넘기면 안 된다.
        try:
            sys.path.insert(0, str(ROOT / "tools"))
            from notify_channels import send_alert  # type: ignore
            # 정상일 때도 보내는 것이 요점이므로 채널을 명시한다.
            # cooldown 0: 내용이 같아도 매일 와야 "조용함 = 죽음" 을 구분할 수 있다.
            res = send_alert(text, level="info", channels="telegram,file", cooldown_sec=0.0)
        except Exception as e:
            print("\n[SEND_FAIL] 예외: %s" % e)
            return 1

        res = res if isinstance(res, dict) else {}
        rows = res.get("results") if isinstance(res.get("results"), list) else []
        oks, bads = [], []
        for r in rows:
            ch = str((r or {}).get("channel") or "?")
            if (r or {}).get("ok"):
                oks.append(ch)
            else:
                bads.append("%s(%s)" % (ch, str((r or {}).get("error") or "?")[:60]))
        if not rows:
            print("\n[SEND_FAIL] 전송 결과가 비어 있다 - 채널이 하나도 시도되지 않았다")
            return 1
        if oks:
            print("\n[SENT] %s" % ", ".join(oks))
        if bads:
            print("[SEND_PARTIAL_FAIL] %s" % " / ".join(bads))
        if not oks:
            print("[SEND_FAIL] 모든 채널 실패 - 이 다이제스트는 아무에게도 도달하지 않았다")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
