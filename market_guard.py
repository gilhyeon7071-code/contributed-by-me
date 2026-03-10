import sys
from datetime import datetime
from holiday_manager import HolidayManager

def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")
    hm = HolidayManager()
    r = hm.explain(target)
    if r.reason == "ERROR":
        print(f"[MARKET] {r.yyyymmdd} CLOSED (ERROR) {r.detail or ''}".strip())
        return 1
    if r.is_open:
        print(f"[MARKET] {r.yyyymmdd} OPEN")
        return 0
    print(f"[MARKET] {r.yyyymmdd} CLOSED ({r.reason})")
    return 2

if __name__ == "__main__":
    raise SystemExit(main())
