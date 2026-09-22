"""10-01 생방 경로 검증 **한 묶음** (2026-09-22 신설).

왜: 같은 일을 확인하는 데 손으로 여섯 가지를 따로 돌렸다. 그러면 두 가지가 생긴다.
  ① 빼먹는다 — 09-22 에 `craft_scan` 을 embed 로만 돌려 [4] 가 평소 호출에서
     한 번도 안 돌던 것을 못 봤다.
  ② "이번에 돌렸나" 가 애매해진다 — 지난 실행 결과를 지금 결과처럼 말하게 된다.

규칙은 가부 판정기와 같다. **UNKNOWN 은 통과가 아니다.**
못 돌린 항목은 PASS 로 접지 않고 UNKNOWN 으로 낸다 — 확인 못 한 것이지 괜찮은 게 아니다.

쓰는 법:
    python tools/verify_v2_battery.py              # 전부
    python tools/verify_v2_battery.py --quick      # 느린 것(pytest·단정문 반증) 뺀다
    python tools/verify_v2_battery.py --json out.json
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

ROOT = Path(__file__).resolve().parents[1]
V2 = ROOT / "paper" / "strategies" / "kospi_mcap_quarterly_v2"
VIBE = Path(r"E:\vibe\control_center_v2")
PY = sys.executable

PASS, FAIL, UNKNOWN = "PASS", "FAIL", "UNKNOWN"


class Item:
    def __init__(self, key: str, title: str, slow: bool = False):
        self.key, self.title, self.slow = key, title, slow
        self.status, self.detail, self.secs = UNKNOWN, "", 0.0

    def row(self) -> Dict[str, Any]:
        return {"key": self.key, "title": self.title, "status": self.status,
                "detail": self.detail, "secs": round(self.secs, 1)}


def _run(cmd: List[str], cwd: Path, timeout: int = 3000) -> tuple:
    """rc·출력을 같이 낸다. **타임아웃을 rc 0 으로 접지 않는다.**"""
    try:
        p = subprocess.run(cmd, cwd=str(cwd), capture_output=True, timeout=timeout,
                           encoding="utf-8", errors="replace")
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except subprocess.TimeoutExpired:
        return 124, "TIMEOUT %ds — 원인이 지워졌다. 따로 다시 돌린다" % timeout
    except OSError as exc:
        return -1, "%s: %s" % (type(exc).__name__, exc)


# ---------------------------------------------------------------- 항목들
def c_pytest(it: Item) -> None:
    rc, out = _run([PY, "-m", "pytest", "tests", "-q"], ROOT)
    m = re.search(r"(\d+) passed", out)
    fails = re.search(r"(\d+) failed", out)
    if rc == 0 and m and not fails:
        it.status, it.detail = PASS, "%s passed" % m.group(1)
    elif fails:
        it.status, it.detail = FAIL, "%s failed / %s passed" % (fails.group(1), m.group(1) if m else "?")
    else:
        it.status, it.detail = UNKNOWN, "rc=%s 결과를 못 읽었다: %s" % (rc, out.strip()[-120:])


def c_craft_scan(it: Item) -> None:
    """**스크립트 모드로 부른다.** embed 로 부르면 [4] 가 통과처럼 보인다(09-22 실측)."""
    rc, out = _run([PY, str(ROOT / "tools" / "craft_scan.py")], ROOT, 900)
    hits = re.findall(r"\[(\d)\][^\n]*?(\d+)건", out)
    total = sum(int(n) for _, n in hits)
    if rc == 0 and hits and total == 0:
        it.status, it.detail = PASS, "%d개 검사 전부 0건" % len(hits)
    elif hits:
        bad = [f"[{i}]{n}건" for i, n in hits if n != "0"]
        it.status, it.detail = FAIL, " ".join(bad) or ("rc=%s" % rc)
    else:
        it.status, it.detail = UNKNOWN, "검사 줄을 못 읽었다 rc=%s" % rc


def c_freshness(it: Item) -> None:
    rc, out = _run([PY, str(ROOT / "tools" / "artifact_freshness_guard.py")], ROOT, 900)
    m = re.search(r"total=(\d+) fresh=(\d+) stale=(\d+) missing=(\d+)", out)
    if not m:
        it.status, it.detail = UNKNOWN, "집계 줄을 못 읽었다 rc=%s" % rc
        return
    stale, missing = int(m.group(3)), int(m.group(4))
    it.status = PASS if (rc == 0 and stale == 0 and missing == 0) else FAIL
    it.detail = "total=%s fresh=%s stale=%s missing=%s" % m.groups()


def c_rehearsal(it: Item) -> None:
    out_dir = ROOT / "2_Logs" / "v2_verify_battery" / "rehearsal"
    rc, out = _run([PY, str(V2 / "src" / "morning_branch_rehearsal.py"),
                    "--out-dir", str(out_dir)], ROOT, 900)
    try:
        rec = json.loads(out[out.index("{"):])
    except (ValueError, json.JSONDecodeError):
        it.status, it.detail = UNKNOWN, "예행 결과를 못 읽었다 rc=%s" % rc
        return
    junk = [s["label"] for s in rec.get("scenarios", []) if s.get("report_note")]
    it.status = PASS if rec.get("verdict") == "PASS" else FAIL
    it.detail = "%s 분기 %d종" % (rec.get("verdict"), len(rec.get("scenarios", [])))
    if junk:                      # 버티긴 했지만 **말은 한다** — 원인 미상인 현상이다
        it.detail += " / stdout 잡소리: %s" % ", ".join(junk)


def c_go_nogo(it: Item) -> None:
    rc, out = _run([PY, str(V2 / "src" / "go_nogo.py")], ROOT, 900)
    m = re.search(r"PASS (\d+) / FAIL (\d+) / UNKNOWN (\d+)", out)
    if not m:
        it.status, it.detail = UNKNOWN, "판정 줄을 못 읽었다 rc=%s" % rc
        return
    p, f, u = (int(x) for x in m.groups())
    it.status = PASS if (rc == 0 and f == 0 and u == 0) else FAIL
    it.detail = "PASS %d / FAIL %d / UNKNOWN %d" % (p, f, u)


def c_boundary(it: Item) -> None:
    rc, out = _run([PY, str(ROOT / "tools" / "boundary_watch.py")], ROOT, 900)
    it.status = PASS if rc == 0 else FAIL
    it.detail = ("변화 없음" if "변화 없음" in out else out.strip().splitlines()[-1][:90]) if out else "rc=%s" % rc


def c_claims_fire(it: Item) -> None:
    """화면 단정문 가드가 **정말 우는지**. 원본은 안 건드린다(읽기 계층만 가로챈다)."""
    script = VIBE / "scripts" / "check_claims_fire.cjs"
    if not script.is_file():
        it.status, it.detail = UNKNOWN, "반증 도구가 없다: %s" % script
        return
    rc, out = _run(["node", str(script)], VIBE, 1800)
    m = re.search(r"전체 (\d+)건 중 안 우는 것 (\d+)건", out)
    if not m:
        it.status, it.detail = UNKNOWN, "결과 줄을 못 읽었다 rc=%s" % rc
        return
    total, silent = int(m.group(1)), int(m.group(2))
    it.status = PASS if silent == 0 else FAIL
    it.detail = "%d건 중 안 우는 것 %d건" % (total, silent)


def c_dashboard_types(it: Item) -> None:
    """단정문 가드는 `dataService.ts` 한 파일만 컴파일한다 — 나머지는 여기서 본다."""
    # [2026-09-22] `npx tsc` 는 셸 없이 못 부른다(윈도에서는 npx.cmd 다) — FileNotFoundError 가 났다.
    #   check_screen_claims.cjs 가 같은 함정을 겪고 주석에 적어뒀다: node 로 진입 스크립트를 직접 부른다.
    tsc = VIBE / "node_modules" / "typescript" / "bin" / "tsc"
    if not (VIBE / "tsconfig.json").is_file() or not tsc.is_file():
        it.status, it.detail = UNKNOWN, "tsconfig 나 tsc 를 못 찾았다 — 확인 못 함"
        return
    rc, out = _run(["node", str(tsc), "--noEmit", "-p", "tsconfig.json"], VIBE, 900)
    if rc < 0:                     # 실행 자체가 안 됐다 = 확인 못 한 것이지 고장이 아니다
        it.status, it.detail = UNKNOWN, "돌리지 못했다: %s" % out.strip()[:90]
        return
    it.status = PASS if rc == 0 else FAIL
    it.detail = "타입 오류 없음" if rc == 0 else out.strip().splitlines()[-1][:110]


def c_schedule(it: Item) -> None:
    """예약이 **있는지**가 아니라 상태·다음 실행·명령줄을 본다.
    'Ready' 는 명령줄이 옳다는 뜻이 아니고, 다음 실행 요일이 틀리면 검증일이 영원히 안 온다."""
    ps = (r"Get-ScheduledTask | Where-Object {$_.TaskName -match 'VIBE_V2'} | "
          r"ForEach-Object { $i=$_|Get-ScheduledTaskInfo; "
          r"'{0}|{1}|{2}|{3}' -f $_.TaskName,$_.State,$i.NextRunTime,$_.Actions[0].Arguments }")
    rc, out = _run(["powershell", "-NoProfile", "-Command", ps], ROOT, 300)
    rows = [l.strip() for l in out.splitlines() if l.strip().startswith("VIBE_V2")]
    if not rows:
        it.status, it.detail = UNKNOWN, "예약 작업을 못 읽었다 rc=%s" % rc
        return
    bad = []
    for r in rows:
        name, state, nxt, args = (r.split("|") + ["", "", ""])[:4]
        if state not in ("Ready", "Running"):
            bad.append("%s=%s" % (name, state))
        elif not nxt.strip():
            bad.append("%s=다음실행없음" % name)
        elif "run_v2_daily_ops.bat" not in args:
            bad.append("%s=명령줄이 다르다" % name)
    it.status = PASS if not bad else FAIL
    it.detail = ("%d개 전부 정상" % len(rows)) if not bad else " / ".join(bad)


def c_ledger(it: Item) -> None:
    """원장 vs 브로커. 10-01 전에는 보유 0이라 조용한 것이 정답이다."""
    tool = ROOT / "tools" / "broker_ledger_reconcile.py"
    if not tool.is_file():
        it.status, it.detail = UNKNOWN, "대조 도구가 없다"
        return
    # [2026-09-22] `--no-alert` 없이 부르면 **검증이 경보를 쏜다.** 확인하는 행위가
    #   사용자를 깨우면 안 되고, 경보 이력도 오염된다.
    rc, out = _run([PY, str(tool), "--no-alert"], ROOT, 900)
    m = re.search(r"status=(\w+)", out)
    st = m.group(1) if m else ""
    if rc != 0:
        it.status, it.detail = FAIL, (out.strip().splitlines() or ["rc=%s" % rc])[-1][:110]
    elif st in ("NO_BROKER", "UNKNOWN", ""):
        # **대조 불가는 통과가 아니다.** rc=0 이라고 접으면 브로커가 죽어도 초록이 뜬다
        it.status, it.detail = UNKNOWN, "대조 불가(status=%s) — 브로커 조회 실패" % (st or "?")
    else:
        it.status, it.detail = PASS, "status=%s" % st


def c_alert_channel(it: Item) -> None:
    """경보가 **닿을 수 있는 상태인지**. 메시지는 보내지 않는다(getMe 는 사용자에게 안 간다).

    [2026-09-22 실측] 09-21 13:18 에 토큰 파일이 BotFather **메시지 전문**으로 덮여
    (132자) 그때부터 텔레그램이 전부 404 였다. 파일 채널만 성공해 기록은 남고
    사용자에게는 안 갔다 — `경보 3.5개월 미도달` 과 같은 모양이다.
    그래서 '설정이 있나' 가 아니라 **'토큰이 지금 유효한가'** 를 묻는다.
    """
    import json as _j
    import re as _re
    import urllib.request as _u
    sys.path.insert(0, str(ROOT / "tools"))
    try:
        import notify_channels as _N
        tok = _N._secret_or_env("TELEGRAM_BOT_TOKEN", "telegram_bot_token.txt")
        cid = _N._secret_or_env("TELEGRAM_CHAT_ID", "telegram_chat_id.txt")
    except Exception as exc:
        it.status, it.detail = UNKNOWN, "통로를 못 읽었다: %s" % type(exc).__name__
        return
    if not tok or not cid:
        it.status, it.detail = FAIL, "토큰·채팅ID 가 없다 — 경보가 안 간다"
        return
    if not _re.fullmatch(r"\d{6,}:[A-Za-z0-9_-]{30,}", tok.strip()):
        it.status, it.detail = FAIL, "토큰 파일에 토큰 말고 다른 것이 섞였다(길이 %d)" % len(tok.strip())
        return
    try:
        with _u.urlopen("https://api.telegram.org/bot%s/getMe" % tok.strip(), timeout=10) as r:
            d = _j.loads(r.read().decode())
        it.status = PASS if d.get("ok") else FAIL
        it.detail = "봇 %s 유효" % (d.get("result") or {}).get("username") if d.get("ok") else "getMe ok=False"
    except Exception as exc:
        # 망 문제와 토큰 무효를 구분한다 — 401/404 는 토큰, 그 밖은 확인 못 함
        code = getattr(exc, "code", None)
        if code in (401, 404):
            it.status, it.detail = FAIL, "토큰이 무효다(HTTP %s)" % code
        else:
            it.status, it.detail = UNKNOWN, "확인 못 함: %s %s" % (type(exc).__name__, str(exc)[:50])


def c_cost(it: Item) -> None:
    """판정이 아니라 **표시**. 기준이 바뀌면 배터리에 보여야 한다."""
    rc, out = _run([PY, str(ROOT / "tools" / "cost_model.py")], ROOT, 300)
    line = next((l for l in out.splitlines() if "왕복(모델)" in l), "")
    it.status = PASS if (rc == 0 and line) else UNKNOWN
    it.detail = line.strip()[:110] or "비용 줄을 못 읽었다"


ITEMS: List[tuple] = [
    ("pytest", "시험 전수", True, c_pytest),
    ("craft_scan", "반복 실수 스캔(8검사)", False, c_craft_scan),
    ("freshness", "산출물 신선도", False, c_freshness),
    ("rehearsal", "아침 분기 예행", False, c_rehearsal),
    ("go_nogo", "가부 9개", False, c_go_nogo),
    ("boundary", "불변식 감시", False, c_boundary),
    ("claims_fire", "화면 단정문이 우는가", True, c_claims_fire),
    ("dash_types", "대시보드 타입체크", True, c_dashboard_types),
    ("schedule", "예약 작업 4개", False, c_schedule),
    ("alert", "경보 통로 유효", False, c_alert_channel),
    ("ledger", "원장-브로커 대조", False, c_ledger),
    ("cost", "왕복 비용(표시)", False, c_cost),
]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="V2 생방 경로 검증 한 묶음")
    ap.add_argument("--quick", action="store_true", help="느린 항목(시험 전수·단정문 반증·타입체크)을 뺀다")
    ap.add_argument("--only", help="쉼표로 구분한 key 만 돌린다")
    ap.add_argument("--json", type=Path, help="결과를 이 경로에 저장")
    a = ap.parse_args(argv)
    only = set(x.strip() for x in a.only.split(",")) if a.only else None

    print("=" * 74)
    print(" V2 검증 배터리   %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 74)
    rows: List[Dict[str, Any]] = []
    skipped: List[str] = []
    for key, title, slow, fn in ITEMS:
        it = Item(key, title, slow)
        if only and key not in only:
            continue
        if a.quick and slow:
            it.status, it.detail = UNKNOWN, "--quick 으로 건너뜀 — 확인 안 된 것이다"
            skipped.append(key)
        else:
            t0 = time.time()
            try:
                fn(it)
            except Exception as exc:                 # 터지면 통과가 아니다
                it.status, it.detail = UNKNOWN, "%s: %s" % (type(exc).__name__, exc)
            it.secs = time.time() - t0
        mark = {PASS: "OK  ", FAIL: "FAIL", UNKNOWN: "????"}[it.status]
        print("  %s %-22s %-8s %s" % (mark, it.title, "%.0fs" % it.secs, it.detail))
        rows.append(it.row())

    n_fail = sum(1 for r in rows if r["status"] == FAIL)
    n_unk = sum(1 for r in rows if r["status"] == UNKNOWN)
    verdict = PASS if (n_fail == 0 and n_unk == 0) else (FAIL if n_fail else UNKNOWN)
    print("-" * 74)
    print("  PASS %d / FAIL %d / UNKNOWN %d  ->  %s"
          % (len(rows) - n_fail - n_unk, n_fail, n_unk, verdict))
    print("  규칙: 전부 PASS 여야 통과다. **UNKNOWN 은 통과가 아니다**(확인 안 된 것이다).")
    if skipped:
        print("  --quick 으로 건너뛴 것: %s" % ", ".join(skipped))
    body = {"generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "verdict": verdict, "rows": rows, "skipped": skipped}
    out = a.json or (ROOT / "2_Logs" / "v2_verify_battery_latest.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")
    print("  기록: %s" % out)
    return 0 if verdict == PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
