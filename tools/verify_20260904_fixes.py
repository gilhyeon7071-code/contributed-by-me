"""2026-09-04 수리 3건이 배치에서 실제로 반영됐는지 한 번에 확인한다.

읽기 전용. 어떤 산출물도 쓰지 않는다.
    py tools/verify_20260904_fixes.py

확인 대상
  (205) run_paper_daily [16.96/16] scan_json_encoding_utf8.ps1 타임아웃 -> PASS
  (209) 폭락 가드 as_of 가 마지막 매수일(20260824)이 아니라 가격 최신일을 따르는가
        + data_date_max / data_lag_days 가 찍히는가
  (210) 계좌 킬스위치가 두 원장을 대조하고 보수적인 쪽을 고르는가
        + account_basis_divergence 가 남는가
"""
from __future__ import annotations

import glob
import json
import os
from datetime import datetime
from pathlib import Path

ROOTA = Path(os.getenv("ROOTA", r"E:\1_Data"))
LOGS = ROOTA / "2_Logs"

OK, BAD, WARN = "[OK]  ", "[FAIL]", "[WARN]"
verdicts: list[tuple[str, str]] = []


def say(mark: str, label: str, detail: str = "") -> None:
    verdicts.append((mark, label))
    print("%s %s" % (mark, label))
    if detail:
        for ln in str(detail).splitlines():
            print("        " + ln)


def _latest(pattern: str):
    fs = sorted(glob.glob(str(LOGS / pattern)))
    return Path(fs[-1]) if fs else None


# ---------------------------------------------------------------- (205)
def check_scanner() -> None:
    print("\n=== (205) 인코딩 스캐너가 배치를 죽이지 않는가 ===")
    log = LOGS / "run_paper_daily_last.txt"
    if not log.exists():
        say(WARN, "run_paper_daily_last.txt 가 없다", str(log))
        return
    txt = log.read_text(encoding="utf-8", errors="replace")
    i = txt.rfind("[START] run_paper_daily.bat")
    blk = txt[i:] if i >= 0 else txt
    start = blk.splitlines()[0] if blk.splitlines() else ""
    print("        최근 실행: %s" % start.strip()[:80])

    st = LOGS / "json_encoding_scan_timeout_status_latest.json"
    if st.exists():
        j = json.loads(st.read_text(encoding="utf-8-sig"))
        ok = str(j.get("status")) == "PASS" and not j.get("timed_out")
        say(OK if ok else BAD,
            "스캐너 단계 status=%s timed_out=%s rc=%s"
            % (j.get("status"), j.get("timed_out"), j.get("returncode")),
            "생성 %s" % j.get("generated_at"))
    else:
        say(WARN, "json_encoding_scan_timeout_status_latest.json 이 없다")

    failed = "[FAILED]" in blk
    scanner_failed = "scan_json_encoding_utf8.ps1" in blk[blk.rfind("[FAILED]"):] if failed else False
    say(OK if not scanner_failed else BAD,
        "최근 배치 블록에 스캐너 원인 [FAILED] %s" % ("없음" if not scanner_failed else "있음"))


# ---------------------------------------------------------------- (209)(210)
def check_p0() -> None:
    print("\n=== (209)(210) p0_daily_check 반영 여부 ===")
    p = _latest("p0_daily_check_2*.json")
    if p is None:
        say(WARN, "p0_daily_check_*.json 이 없다")
        return
    age_h = (datetime.now() - datetime.fromtimestamp(p.stat().st_mtime)).total_seconds() / 3600
    print("        파일: %s  (%.1f시간 전)" % (p.name, age_h))
    d = json.loads(p.read_text(encoding="utf-8-sig"))

    # --- 폭락 가드 as_of
    cm = ((d.get("crash_risk_off") or {}).get("metrics") or {})
    asof = str(cm.get("as_of_ymd") or "")
    price_max = str((d.get("prices") or {}).get("date_max") or "")
    say(OK if (asof and asof == price_max) else BAD,
        "폭락 가드 as_of=%s / 가격 최신일=%s" % (asof or "-", price_max or "-"),
        "20260824(마지막 매수일)에 고정돼 있으면 수리가 반영되지 않은 것이다")

    # --- 신선도 계측
    has_dm = "data_date_max" in cm
    say(OK if has_dm else BAD,
        "신선도 계측 data_date_max=%s data_lag_days=%s (문턱 %s)"
        % (cm.get("data_date_max", "-"), cm.get("data_lag_days", "-"),
           ((d.get("crash_risk_off") or {}).get("limits") or {}).get("max_data_lag_days", "-")))

    lag = cm.get("data_lag_days")
    if isinstance(lag, (int, float)):
        say(OK if lag <= 7 else WARN, "지수 데이터 지연 %s일" % lag,
            "7일을 넘으면 fail-closed 로 매수가 막힌다(설계된 동작). 수집 배치를 볼 것")

    # --- 계좌 킬스위치 원장 대조
    ab = ((d.get("kill_switch") or {}).get("metrics") or {}).get("account_basis") or {}
    dv = ab.get("account_basis_divergence")
    say(OK if isinstance(dv, dict) else BAD,
        "계좌 원장 대조 기록 %s" % ("있음" if isinstance(dv, dict) else "없음"),
        "선택=%s" % ab.get("account_basis_selected", "-"))
    if isinstance(dv, dict):
        say(WARN if abs(float(dv.get("dd_gap_pct_points") or 0)) > 1.0 else OK,
            "두 원장 낙폭 차이 %.4f%%p / 자산 차이 %s원"
            % (float(dv.get("dd_gap_pct_points") or 0), dv.get("equity_gap_krw", "-")),
            "채택=%s (규칙 %s)" % (dv.get("chosen", "-"), dv.get("rule", "-")))

    ro = d.get("risk_off") or {}
    print("        risk_off=%s reasons=%s" % (ro.get("enabled"), ro.get("reasons")))


def check_batch() -> None:
    print("\n=== 배치 결과 ===")
    f = LOGS / "full_auto_hidden_last.txt"
    if not f.exists():
        say(WARN, "full_auto_hidden_last.txt 가 없다")
        return
    t = f.read_text(encoding="utf-8", errors="replace")
    fin = [ln for ln in t.splitlines() if "FINAL STATUS" in ln or "[AUTO]" in ln]
    for ln in fin[-3:]:
        print("        " + ln.strip()[:140])
    say(OK if "rc=0" in t.splitlines()[-1] else WARN,
        "마지막 줄: %s" % (t.splitlines()[-1].strip()[:80] if t.splitlines() else "-"))


def main() -> int:
    print("2026-09-04 수리 검증  (실행 %s)" % datetime.now().strftime("%Y-%m-%d %H:%M"))
    check_scanner()
    check_p0()
    check_batch()
    n_bad = sum(1 for m, _ in verdicts if m == BAD)
    n_warn = sum(1 for m, _ in verdicts if m == WARN)
    print("\n=== 요약 ===")
    print("  OK %d / WARN %d / FAIL %d" % (len(verdicts) - n_bad - n_warn, n_warn, n_bad))
    if n_bad:
        print("  FAIL 이 있으면 그 항목에서 멈추고 진단할 것. PLANS (205)(209)(210) 참조")
    return 1 if n_bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
