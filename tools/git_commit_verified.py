"""커밋을 '내가 친 명령' 이 아니라 '저장소 상태' 로 판정한다 (2026-09-20 신설).

왜: 2026-09-20 하루에 같은 실패를 세 번 했다 —
  (1) parquet 126MB 가 추적 중인 걸 모르고 "대용량 제외" 라고 보고
  (2) 루트 스크립트 45개가 안 담겼는데 "담았다" 라고 보고 (장중 루프 포함)
  (3) 루트 변경분 30개가 안 담겼는데 "커밋 끝" 이라고 보고
셋 다 같은 자리: `{ git add ...; } | head` 형태가 시간 초과 124 를 **0 으로 바꿨고**
나는 그 0 을 성공으로 읽었다 (실측 재현: `{ echo x; timeout 1 sleep 5; } | head -3` -> $?=0).

그래서 이 도구는:
  - 파이프를 쓰지 않는다. add 를 **파일 하나씩** 돌리고 건별 rc 를 본다
  - 커밋 뒤 `git status --porcelain` 으로 **대상이 정말 깨끗해졌는지** 다시 잰다
  - 어긋나면 rc=1 로 크게 실패한다. 조용히 성공하지 않는다
"""
from __future__ import annotations

import argparse
import subprocess
import sys

GIT = ["git", "-c", "core.fsmonitor=false"]


def run(args, cwd, timeout=600):
    """rc 를 숨기지 않는다. 시간 초과는 시간 초과로 돌려준다."""
    try:
        # Windows 기본 인코딩(cp949)으로 읽으면 한글 커밋 메시지에서 터진다 (2026-09-20 실측)
        p = subprocess.run(GIT + args, cwd=cwd, capture_output=True, timeout=timeout)
        dec = lambda b: (b or b"").decode("utf-8", errors="replace")  # noqa: E731
        return p.returncode, dec(p.stdout), dec(p.stderr)
    except subprocess.TimeoutExpired:
        return 124, "", f"TIMEOUT after {timeout}s: git {' '.join(args)}"


def dirty_paths(cwd):
    rc, out, err = run(["status", "--porcelain"], cwd)
    if rc != 0:
        raise SystemExit(f"[FAIL] status rc={rc} {err.strip()}")
    return {ln[3:].strip().strip('"') for ln in out.splitlines() if ln.strip()}


def main(argv=None):
    ap = argparse.ArgumentParser(description="검증된 커밋 — 저장소 상태로 판정한다")
    ap.add_argument("-m", "--message", required=True)
    ap.add_argument("-C", "--cwd", default=".")
    ap.add_argument("paths", nargs="*", help="비우면 현재 변경/미추적 전부")
    a = ap.parse_args(argv)

    before = dirty_paths(a.cwd)
    targets = sorted(a.paths) if a.paths else sorted(before)
    if not targets:
        print("[SKIP] 담을 것이 없다")
        return 0

    staged, failed = [], []
    for f in targets:
        rc, _, err = run(["add", "--", f], a.cwd, timeout=120)
        (staged if rc == 0 else failed).append(f if rc == 0 else f"{f} (rc={rc} {err.strip()[:80]})")
    if failed:
        print(f"[FAIL] add 실패 {len(failed)}건 — 커밋하지 않는다")
        for f in failed:
            print(f"  {f}")
        return 1

    rc, out, err = run(["commit", "-m", a.message], a.cwd)
    if rc != 0:
        print(f"[FAIL] commit rc={rc}\n{out}{err}")
        return 1

    # ---- 여기부터가 본체: 명령이 아니라 상태를 잰다
    after = dirty_paths(a.cwd)
    left = sorted(set(targets) & after)
    rc2, head, _ = run(["show", "--stat", "--oneline", "HEAD"], a.cwd)
    n_in_commit = sum(1 for ln in head.splitlines() if " | " in ln)

    print(f"커밋에 담긴 파일 {n_in_commit}건 / 담으려던 파일 {len(targets)}건")
    if left:
        print(f"[FAIL] 커밋 뒤에도 남아 있는 대상 {len(left)}건 — '끝났다' 고 말하면 안 된다")
        for f in left[:20]:
            print(f"  {f}")
        return 1
    if n_in_commit != len(targets):
        print(f"[FAIL] 수가 맞지 않는다 (커밋 {n_in_commit} vs 대상 {len(targets)})")
        return 1
    print("[OK] 대상 전부가 커밋에 들어갔고 작업 트리에 남지 않았다")
    return 0


if __name__ == "__main__":
    sys.exit(main())
