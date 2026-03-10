import json
import os
from dataclasses import dataclass
from datetime import datetime
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
    def _normalize_date(s: str) -> str:
        s = s.strip()
        if not s:
            return ""
        s = s.replace("-", "")
        if len(s) == 8 and s.isdigit():
            return s
        return ""
