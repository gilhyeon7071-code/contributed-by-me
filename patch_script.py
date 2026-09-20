import os
from pathlib import Path

path = Path('E:/1_Data/intraday_paper_loop.py')
content = path.read_text(encoding='utf-8')

run_replace = '''        if ok:
            logger.info("[%s] OK (%.1fs)", label, elapsed)
        else:
            err_msg = "\\n  ".join(err_tail)
            err_code = "ERR_UNKNOWN"
            if "KIS env/config failed" in err_msg or "KIS_APP_KEY" in err_msg:
                err_code = "ERR_KIS_AUTH_REQ"
            elif "timeout" in err_msg.lower() or "connection reset" in err_msg.lower():
                err_code = "ERR_KIS_ORDER_TIMEOUT"
            logger.warning("[%s] FAILED rc=%d (%.1fs) err_code=%s\\n  stderr: %s",
                           label, cp.returncode, elapsed, err_code, err_msg)
            (LOG_DIR / "paper_intraday_hard_blocked.flag").write_text(f"{err_code} in {label} threshold exceeded")
            logger.error("[%s] Hard blocking intraday loop. (PID=%s, Error=%s)", label, os.getpid(), err_code)'''

old_run_block = '''        if ok:
            logger.info("[%s] OK (%.1fs)", label, elapsed)
        else:
            logger.warning("[%s] FAILED rc=%d (%.1fs)\\n  stderr: %s",
                           label, cp.returncode, elapsed, "\\n  ".join(err_tail))'''

content = content.replace(old_run_block, run_replace)

loop_replace = '''    while True:
        if (LOG_DIR / "paper_intraday_hard_blocked.flag").exists():
            logger.error("[LOOP] paper_intraday_hard_blocked.flag exists. Sleeping to prevent crash loop.")
            import time
            time.sleep(60)
            continue
        now = dt.datetime.now(tz=KST)'''

old_loop_block = '''    while True:
        now = dt.datetime.now(tz=KST)'''

content = content.replace(old_loop_block, loop_replace)

path.write_text(content, encoding='utf-8')
print('Replacement complete.')
