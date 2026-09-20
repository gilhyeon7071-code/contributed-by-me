# -*- coding: utf-8 -*-
"""전송 실패를 실제로 잡는지 시험한다.

성공 경로만 보면 이 결함을 또 놓친다. 실제로 예전 코드는 성공 때만 확인됐고
실패해도 [SENT] 를 찍었다. 그래서 **실패를 주입**해 rc 와 출력을 본다.

토큰 파일은 건드리지 않는다. send_alert 를 가짜로 바꿔치기해 시험한다.
"""
import io
import subprocess
import sys
import types
from pathlib import Path

PY = "E:/1_Data/_runtime/python312-embed/python.exe"
FAILS = []


def run_with_fake(fake_result, label):
    """notify_channels.send_alert 를 가짜로 바꿔치기한 뒤 다이제스트를 --send 로 돌린다."""
    stub = f'''
import sys, json, types
sys.path.insert(0, "E:/1_Data/tools")
import notify_channels
notify_channels.send_alert = lambda *a, **k: {fake_result!r}
sys.argv = ["build_status_digest.py", "--send"]
import runpy
try:
    runpy.run_path("E:/1_Data/tools/build_status_digest.py", run_name="__main__")
except SystemExit as e:
    print("EXITCODE=%s" % (e.code if e.code is not None else 0))
'''
    p = Path("C:/Users/jjtop/AppData/Local/Temp/claude/C--Windows-System32/"
             "7540291e-b49c-4f8a-b251-27710d724518/scratchpad/_stub.py")
    p.write_text(stub, encoding="utf-8")
    r = subprocess.run([PY, str(p)], capture_output=True, text=True,
                       encoding="utf-8", errors="replace")
    out = (r.stdout or "") + (r.stderr or "")
    rc = None
    for line in out.splitlines():
        if line.startswith("EXITCODE="):
            rc = line.split("=", 1)[1].strip()
    tail = [l for l in out.splitlines() if l.startswith(("[SENT]", "[SEND_FAIL]", "[SEND_PARTIAL_FAIL]"))]
    print("   %-24s rc=%-4s %s" % (label, rc, " | ".join(tail) or "(전송 출력 없음)"))
    return rc, tail


print("=" * 78)
print("전송 실패를 잡는가 (토큰 미접촉, send_alert 를 가짜로 교체)")

# 1) 전 채널 실패 -> rc=1 이어야 한다
rc, tail = run_with_fake(
    {"ok": False, "results": [{"channel": "telegram", "ok": False, "error": "HTTP 500"},
                              {"channel": "file", "ok": False, "error": "disk"}]},
    "전 채널 실패")
if rc != "1" or not any("[SEND_FAIL]" in t for t in tail):
    FAILS.append("전 채널 실패인데 rc=%s / %s" % (rc, tail))

# 2) 텔레그램만 실패 -> rc=0 이되 PARTIAL 경고
rc, tail = run_with_fake(
    {"ok": False, "results": [{"channel": "telegram", "ok": False, "error": "timeout"},
                              {"channel": "file", "ok": True}]},
    "텔레그램만 실패")
if rc not in ("0", "None") or not any("PARTIAL" in t for t in tail):
    FAILS.append("부분 실패 처리 이상: rc=%s / %s" % (rc, tail))

# 3) 결과가 비어 있음 -> rc=1
rc, tail = run_with_fake({"ok": True, "results": []}, "결과 비어있음")
if rc != "1":
    FAILS.append("빈 결과인데 rc=%s" % rc)

# 4) 정상 -> rc=0, [SENT]
rc, tail = run_with_fake(
    {"ok": True, "results": [{"channel": "telegram", "ok": True},
                             {"channel": "file", "ok": True}]},
    "정상")
if rc not in ("0", "None") or not any("[SENT]" in t for t in tail):
    FAILS.append("정상인데 rc=%s / %s" % (rc, tail))

print()
print("=" * 78)
print("[요약]", "실패가 전부 잡힌다" if not FAILS else "**미검출 %d건**" % len(FAILS))
for f in FAILS:
    print("   " + f)
sys.exit(1 if FAILS else 0)
