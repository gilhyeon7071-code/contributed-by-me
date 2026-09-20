# -*- coding: utf-8 -*-
"""로그 파일을 자르기 전에 아카이브로 옮긴다.

2026-08-24: run_intraday_paper.bat 이 매 재시작마다 run_intraday_paper_last.txt 를
`>` 로 잘라내어, 그날 09:47~14:29 하드블록 구간의 증거가 통째로 사라졌다.
소비자(build_defense_signal_entry_policy_outcome_review.py 등)가 _last.txt 를
읽으므로 파일명은 그대로 두고, **자르기 직전에 기존 파일만 옮긴다.**

기록 실패가 실행을 막아서는 안 되므로 모든 예외를 삼키고 항상 0 을 반환한다.
"""
import sys, os, shutil, datetime as dt, glob

DEFAULT_KEEP_DAYS = 30

def main() -> int:
    try:
        if len(sys.argv) < 2:
            return 0
        src = sys.argv[1]
        keep_days = DEFAULT_KEEP_DAYS
        for i, a in enumerate(sys.argv):
            if a == "--keep-days" and i + 1 < len(sys.argv):
                try: keep_days = max(1, int(sys.argv[i + 1]))
                except Exception: pass
        if not os.path.isfile(src):
            return 0
        if os.path.getsize(src) == 0:
            return 0                                  # 빈 파일은 옮길 것이 없다
        root = os.path.dirname(os.path.abspath(src))
        arc = os.path.join(root, "_archive")
        os.makedirs(arc, exist_ok=True)
        stem, ext = os.path.splitext(os.path.basename(src))
        ts = dt.datetime.fromtimestamp(os.path.getmtime(src)).strftime("%Y%m%d_%H%M%S")
        dst = os.path.join(arc, "%s_%s%s" % (stem, ts, ext))
        n = 1
        while os.path.exists(dst):                    # 같은 초에 두 번이면 접미사
            dst = os.path.join(arc, "%s_%s_%d%s" % (stem, ts, n, ext))
            n += 1
        shutil.move(src, dst)
        print("[ROTATE] %s -> %s" % (os.path.basename(src), os.path.basename(dst)))
        cutoff = dt.datetime.now().timestamp() - keep_days * 86400
        removed = 0
        for f in glob.glob(os.path.join(arc, "%s_*%s" % (stem, ext))):
            try:
                if os.path.getmtime(f) < cutoff:
                    os.remove(f); removed += 1
            except Exception:
                pass
        if removed:
            print("[ROTATE] retention %dd: %d개 삭제" % (keep_days, removed))
        return 0
    except Exception as e:
        try: print("[ROTATE] 실패(무시하고 계속): %s" % e)
        except Exception: pass
        return 0

if __name__ == "__main__":
    sys.exit(main())
