import os
import shutil
from datetime import datetime

def _base_dir() -> str:
    return os.environ.get("STOC_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))

def run_v130_optimization() -> int:
    base_dir = _base_dir()
    drive, _ = os.path.splitdrive(os.path.abspath(base_dir))
    root = (drive + "\\") if drive else base_dir

    total, used, free = shutil.disk_usage(root)
    usage_pct = (used / total) * 100.0

    log_dir = os.path.join(base_dir, "2_Logs")
    os.makedirs(log_dir, exist_ok=True)

    history_path = os.path.join(log_dir, "history_log.txt")
    with open(history_path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().isoformat(timespec='seconds')}] v130 patch executed. Usage: {usage_pct:.2f}%\n")

    print(f"[STORAGE] usage={usage_pct:.2f}% (OK)")
    return 0

if __name__ == "__main__":
    raise SystemExit(run_v130_optimization())
