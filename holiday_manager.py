import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

@dataclass(frozen=True)
class MarketStatus:
    yyyymmdd: str
    is_open: bool
    reason: str  # OPEN / WEEKEND / HOLIDAY_CACHE / ERROR
    detail: Optional[str] = None

class HolidayManager:
    # Market open/close 판단:
    # 0) 주말: 토/일 = 휴장
    # 1) holidays.json 캐시: YYYYMMDD 리스트에 포함되면 휴장
    # 2) 그 외: 개장으로 간주 (KIS 휴장일 API 동기화는 별도 구현)

    def __init__(self, cache_path: Optional[str] = None):
        base_dir = os.environ.get("STOC_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))
        self.cache_path = cache_path or os.path.join(base_dir, "holidays.json")
        self.holidays = self._load_cache()

    def explain(self, target_date: str) -> MarketStatus:
        yyyymmdd = self._normalize_date(target_date)
        if not yyyymmdd:
            return MarketStatus(target_date, False, "ERROR", "Invalid date format")
        try:
            d = datetime.strptime(yyyymmdd, "%Y%m%d")
        except Exception as e:
            return MarketStatus(yyyymmdd, False, "ERROR", str(e))

        if d.weekday() >= 5:
            return MarketStatus(yyyymmdd, False, "WEEKEND")

        if yyyymmdd in self.holidays:
            return MarketStatus(yyyymmdd, False, "HOLIDAY_CACHE")

        return MarketStatus(yyyymmdd, True, "OPEN")

    def is_market_open(self, target_date: str) -> bool:
        return self.explain(target_date).is_open

    def previous_trading_day(self, target_date: str, *, include_target: bool = False, max_lookback_days: int = 31) -> str:
        yyyymmdd = self._normalize_date(target_date)
        if not yyyymmdd:
            return ""
        try:
            d = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        except Exception:
            return ""
        if not include_target:
            d -= timedelta(days=1)
        for _ in range(max(1, int(max_lookback_days))):
            y = d.strftime("%Y%m%d")
            if self.explain(y).is_open:
                return y
            d -= timedelta(days=1)
        return ""

    def next_trading_day(self, target_date: str, *, include_target: bool = False, max_lookahead_days: int = 31) -> str:
        yyyymmdd = self._normalize_date(target_date)
        if not yyyymmdd:
            return ""
        try:
            d = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        except Exception:
            return ""
        if not include_target:
            d += timedelta(days=1)
        for _ in range(max(1, int(max_lookahead_days))):
            y = d.strftime("%Y%m%d")
            if self.explain(y).is_open:
                return y
            d += timedelta(days=1)
        return ""

    def refresh_cache(self, start_date: str, end_date: str) -> List[str]:
        holidays, used_start, used_end = self._generate_krx_holidays(start_date, end_date)
        payload = {
            "source": "exchange_calendars.XKRX",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "start_date": used_start,
            "end_date": used_end,
            "holidays": holidays,
        }
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        self.holidays = list(holidays)
        return holidays

    def _load_cache(self) -> List[str]:
        if not os.path.exists(self.cache_path):
            return []
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                j = json.load(f)
            items = j.get("holidays", [])
            out = []
            for x in items:
                s = self._normalize_date(str(x))
                if s:
                    out.append(s)
            return sorted(list(set(out)))
        except Exception:
            return []

    @staticmethod
    def _generate_krx_holidays(start_date: str, end_date: str) -> tuple[List[str], str, str]:
        start_ymd = HolidayManager._normalize_date(start_date)
        end_ymd = HolidayManager._normalize_date(end_date)
        if not start_ymd or not end_ymd:
            raise ValueError("Invalid start/end date")
        if start_ymd > end_ymd:
            raise ValueError("start_date > end_date")

        import exchange_calendars as xc
        import pandas as pd

        cal = xc.get_calendar("XKRX")
        first_session = cal.first_session.tz_localize(None).normalize()
        last_session = cal.last_session.tz_localize(None).normalize()
        start = pd.Timestamp(datetime.strptime(start_ymd, "%Y%m%d").date())
        end = pd.Timestamp(datetime.strptime(end_ymd, "%Y%m%d").date())
        start = max(start, first_session)
        end = min(end, last_session)
        if start > end:
            raise ValueError("requested range is outside supported XKRX calendar range")
        weekdays = pd.bdate_range(start, end)
        sessions = cal.sessions_in_range(start, end).tz_localize(None).normalize()
        session_set = set(sessions)

        out = sorted(
            {
                d.strftime("%Y%m%d")
                for d in weekdays
                if d.normalize() not in session_set
            }
        )
        return out, start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

    @staticmethod
    def _normalize_date(s: str) -> str:
        s = s.strip()
        if not s:
            return ""
        s = s.replace("-", "")
        if len(s) == 8 and s.isdigit():
            return s
        return ""


def main() -> int:
    ap = argparse.ArgumentParser(description="KRX holiday cache helper")
    ap.add_argument("target_date", nargs="?", default="", help="YYYYMMDD to check")
    ap.add_argument("--cache-path", default="", help="holidays.json path")
    ap.add_argument("--refresh", action="store_true", help="Refresh holidays.json from exchange_calendars.XKRX")
    ap.add_argument("--start", default="", help="Refresh range start YYYYMMDD")
    ap.add_argument("--end", default="", help="Refresh range end YYYYMMDD")
    args = ap.parse_args()

    hm = HolidayManager(cache_path=(args.cache_path or None))
    today = datetime.now().strftime("%Y%m%d")

    if args.refresh:
        year = int(today[:4])
        start = args.start or f"{year - 1}0101"
        end = args.end or f"{year + 1}1231"
        try:
            holidays = hm.refresh_cache(start, end)
            with open(hm.cache_path, "r", encoding="utf-8") as f:
                cache_obj = json.load(f)
        except Exception as e:
            print(f"[HOLIDAY] REFRESH FAIL {type(e).__name__}: {e}")
            return 1
        print(
            f"[HOLIDAY] REFRESH OK cache={hm.cache_path} "
            f"start={cache_obj.get('start_date', HolidayManager._normalize_date(start))} "
            f"end={cache_obj.get('end_date', HolidayManager._normalize_date(end))} "
            f"count={len(holidays)}"
        )
        return 0

    target = args.target_date or today
    r = hm.explain(target)
    if r.reason == "ERROR":
        print(f"[HOLIDAY] {r.yyyymmdd} ERROR {r.detail or ''}".strip())
        return 1
    if r.is_open:
        print(f"[HOLIDAY] {r.yyyymmdd} OPEN")
        return 0
    print(f"[HOLIDAY] {r.yyyymmdd} CLOSED ({r.reason})")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
