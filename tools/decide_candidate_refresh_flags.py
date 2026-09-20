from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
import logging




logger = logging.getLogger(__name__)

def _log_print(*args, **kwargs):
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")
    sep = kwargs.get("sep", " ")
    try:
        msg = sep.join(str(a) for a in args)
    except Exception:
        msg = " ".join(str(a) for a in args)
    logger.info(msg)
def _read_asof(meta_path: Path) -> str:
    if not meta_path.exists():
        return ""
    try:
        data = json.loads(meta_path.read_text(encoding="utf-8"))
        return str(data.get("as_of_ymd", "")).strip()
    except Exception:
        return ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="E:/1_Data")
    args = parser.parse_args()

    root = Path(args.root)
    cache = root / "_cache"
    today = datetime.now().strftime("%Y%m%d")

    dart_asof = _read_asof(cache / "dart_fundamental_meta.json")
    watch_asof = _read_asof(cache / "krx_watchlist_meta.json")

    dart_refresh = "0" if dart_asof == today else "1"
    watch_refresh = "0" if watch_asof == today else "1"

    _log_print(f"FUND_DART_REFRESH={dart_refresh}")
    _log_print(f"FUND_KRX_WATCH_REFRESH={watch_refresh}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
