import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
logger = logging.getLogger("kis_ws_multiplexer")

def main() -> int:
    ap = argparse.ArgumentParser(description="Multiplexer for KIS WebSocket to overcome 40-symbol limit")
    ap.add_argument("--max-codes", type=int, default=80)
    ap.add_argument("--channels", default="trade,hoga")
    ap.add_argument("--mock", default="auto")
    ap.add_argument("--duration-sec", type=int, default=0)
    ap.add_argument("--max-subscriptions", type=int, default=40)
    ap.add_argument("--notify-on-error", action="store_true")
    ap.add_argument("--worker-id-prefix", default="", help="Optional worker-id prefix, e.g. hoga -> hoga1")
    ap.add_argument("--extra-subscriptions", default="", help="Optional TR:code list passed to the first worker")
    ap.add_argument(
        "--include-index-worker",
        action="store_true",
        help="Also launch kis_ws_index_realtime.py from this multiplexer. Keep off when run_kis_ws_index_monitor.bat is scheduled separately.",
    )
    args = ap.parse_args()

    # Get codes using select_ws_codes.py
    cmd_select = [sys.executable, str(TOOLS_DIR / "select_ws_codes.py"), "--max-codes", str(args.max_codes)]
    try:
        output = subprocess.check_output(cmd_select, text=True).strip()
    except Exception as e:
        logger.error(f"Failed to run select_ws_codes.py: {e}")
        return 1

    codes = [c.strip() for c in output.split(",") if c.strip()]
    if not codes:
        logger.error("No codes selected for multiplexing.")
        return 2

    # Split into chunks of max_subscriptions
    chunks = [codes[i:i + args.max_subscriptions] for i in range(0, len(codes), args.max_subscriptions)]
    logger.info(f"Loaded {len(codes)} codes, split into {len(chunks)} workers.")

    processes = []
    
    for i, chunk in enumerate(chunks):
        worker_id = f"{args.worker_id_prefix}{i + 1}" if str(args.worker_id_prefix or "").strip() else str(i + 1)
        worker_codes = ",".join(chunk)
        
        cmd = [
            sys.executable, str(TOOLS_DIR / "kis_realtime_ws.py"),
            "--codes", worker_codes,
            "--channels", args.channels,
            "--mock", args.mock,
            "--duration-sec", str(args.duration_sec),
            "--max-subscriptions", str(args.max_subscriptions),
            "--worker-id", worker_id
        ]
        if args.notify_on_error:
            cmd.append("--notify-on-error")
        if args.extra_subscriptions and i == 0:
            cmd.extend(["--extra-subscriptions", args.extra_subscriptions])
            
        env = os.environ.copy()
        
        # For workers > 1, configure them to use alternative secrets
        if i > 0:
            env["KIS_APP_KEY_FILE_PROD"] = str(ROOT / f".secrets/kis_app_key_prod_{worker_id}.txt")
            env["KIS_APP_SECRET_FILE_PROD"] = str(ROOT / f".secrets/kis_app_secret_prod_{worker_id}.txt")
            env["KIS_ACCOUNT_NO_FILE_PROD"] = str(ROOT / f".secrets/kis_account_no_prod_{worker_id}.txt")
            
        logger.info(f"[Worker {worker_id}] Starting with {len(chunk)} codes.")
        p = subprocess.Popen(cmd, env=env)
        processes.append((worker_id, p))

    if args.include_index_worker:
        # Optional fallback for single-entry operation. The normal scheduler runs
        # run_kis_ws_index_monitor.bat separately, so defaulting this off avoids
        # duplicate approval-key sessions for the same appkey.
        index_cmd = [
            sys.executable, str(TOOLS_DIR / "kis_ws_index_realtime.py"),
            "--duration-sec", str(args.duration_sec),
            "--worker-id", "index"
        ]
        logger.info("[Worker index] Starting index TR worker (H0UPCNT0).")
        index_p = subprocess.Popen(index_cmd, env=os.environ.copy())
        processes.append(("index", index_p))
    else:
        logger.info("[Worker index] skipped; use run_kis_ws_index_monitor.bat or --include-index-worker")

    try:
        while True:
            all_done = True
            for wid, p in processes:
                if p.poll() is None:
                    all_done = False
                else:
                    logger.warning(f"[Worker {wid}] Exited with code {p.returncode}")
            
            if all_done:
                logger.info("All workers have exited.")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrupted. Terminating workers...")
        for wid, p in processes:
            p.terminate()
        for wid, p in processes:
            p.wait()

    return 0

if __name__ == "__main__":
    sys.exit(main())
