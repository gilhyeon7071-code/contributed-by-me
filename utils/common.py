# -*- coding: utf-8 -*-
"""
STOC 공통 유틸리티 모듈

여러 모듈에서 중복 사용되던 함수들을 통합.
- 날짜/시간 처리
- 파일 I/O
- 코드 정규화
- Parquet 최적화 읽기
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ============================================================================
# 상수 정의 (매직 넘버 제거)
# ============================================================================
MIN_UNIVERSE_SIZE = 2000          # KRX 유니버스 최소 종목 수
MAX_PROCESSED_SIGNALS = 20000     # 처리된 시그널 최대 보관 수
DEFAULT_TIMEOUT = (5, 30)         # requests 타임아웃 (connect, read)
DATE_FORMAT_YMD = "%Y%m%d"
DATE_FORMAT_YMDHMS = "%Y%m%d_%H%M%S"
DATE_FORMAT_ISO = "%Y-%m-%d %H:%M:%S"

# 로거 설정
logger = logging.getLogger("stoc")


# ============================================================================
# 날짜/시간 유틸리티
# ============================================================================
def now_tag() -> str:
    """현재 시각을 YYYYMMDD_HHMMSS 형식으로 반환."""
    return datetime.now().strftime(DATE_FORMAT_YMDHMS)


def now_ymd() -> str:
    """현재 날짜를 YYYYMMDD 형식으로 반환."""
    return datetime.now().strftime(DATE_FORMAT_YMD)


def now_iso() -> str:
    """현재 시각을 ISO 형식으로 반환."""
    return datetime.now().strftime(DATE_FORMAT_ISO)


def prev_weekday(base_date: datetime.date = None) -> str:
    """직전 평일(월-금)을 YYYYMMDD 형식으로 반환."""
    d = (base_date or datetime.now().date()) - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d.strftime(DATE_FORMAT_YMD)


def parse_yyyymmdd(s: Any) -> Optional[str]:
    """다양한 형식의 날짜를 YYYYMMDD 문자열로 정규화.

    지원 형식: datetime, 'YYYY-MM-DD', 'YYYYMMDD', int, float
    """
    if s is None:
        return None

    try:
        if pd.isna(s):
            return None
    except Exception:
        pass

    # datetime-like 객체
    try:
        ts = pd.to_datetime(s, errors='raise')
        if not pd.isna(ts):
            return ts.strftime(DATE_FORMAT_YMD)
    except Exception:
        pass

    # 문자열/정수
    try:
        st = str(s).strip()
        if st == '' or st.lower() == 'nan':
            return None
        # 8자리 숫자
        if len(st) == 8 and st.isdigit():
            return st
        # 하이픈 제거 후 재시도
        clean = re.sub(r'[^0-9]', '', st)[:8]
        if len(clean) == 8:
            return clean
        # pandas로 파싱
        ts = pd.to_datetime(st, errors='coerce')
        if ts is not None and not pd.isna(ts):
            return ts.strftime(DATE_FORMAT_YMD)
    except Exception:
        pass

    return None


def to_yyyymmdd(s: str) -> str:
    """날짜 문자열에서 숫자만 추출하여 YYYYMMDD 반환."""
    return re.sub(r'[^0-9]', '', str(s))[:8]


# ============================================================================
# 코드 정규화
# ============================================================================
def norm_code(x: Any) -> str:
    """종목코드를 6자리 문자열로 정규화.

    예: '5930' -> '005930', 'A005930' -> '005930'
    """
    try:
        s = str(x).strip()
    except Exception:
        return ""
    s = re.sub(r"\D", "", s)  # 숫자만 추출
    if not s:
        return ""
    if len(s) > 6:
        s = s[-6:]  # 뒤 6자리
    return s.zfill(6)


# ============================================================================
# JSON 파일 I/O
# ============================================================================
def read_json(path: Path) -> Optional[Dict[str, Any]]:
    """JSON 파일을 안전하게 읽기. 실패 시 None 반환."""
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception as e:
        logger.warning(f"JSON 읽기 실패: {path} - {type(e).__name__}: {e}")
        return None


def write_json(path: Path, data: Dict[str, Any], indent: int = 2) -> bool:
    """JSON 파일 저장. 성공 시 True 반환."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=indent),
            encoding='utf-8'
        )
        return True
    except Exception as e:
        logger.error(f"JSON 저장 실패: {path} - {type(e).__name__}: {e}")
        return False


# ============================================================================
# 파일 탐색
# ============================================================================
def latest_file(dir_path: Path, pattern: str) -> Optional[Path]:
    """디렉토리에서 패턴에 맞는 가장 최신 파일 반환."""
    try:
        files = sorted(dir_path.glob(pattern), key=lambda p: p.stat().st_mtime)
        return files[-1] if files else None
    except Exception as e:
        logger.warning(f"파일 탐색 실패: {dir_path}/{pattern} - {type(e).__name__}")
        return None


def find_parquets(root: Path, pattern: str = "*.parquet") -> List[Path]:
    """디렉토리 하위에서 Parquet 파일 목록 반환."""
    try:
        return sorted(root.rglob(pattern))
    except Exception as e:
        logger.warning(f"Parquet 탐색 실패: {root} - {type(e).__name__}")
        return []


# ============================================================================
# Parquet 최적화 읽기 (핵심 개선)
# ============================================================================
def read_parquet_schema(path: Path) -> Tuple[List[str], Optional[str]]:
    """Parquet 스키마(컬럼 목록)만 빠르게 읽기.

    전체 데이터를 읽지 않고 메타데이터만 읽어서 컬럼명 반환.

    Returns:
        (컬럼 목록, date 컬럼 타입) 튜플
    """
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(str(path))
        cols = list(pf.schema_arrow.names)
        date_type = None
        if "date" in cols:
            try:
                date_type = str(pf.schema_arrow.field("date").type)
            except Exception:
                pass
        return cols, date_type
    except Exception as e:
        logger.debug(f"PyArrow 스키마 읽기 실패, pandas fallback: {e}")
        try:
            # Fallback: pandas로 0행만 읽기
            df = pd.read_parquet(path, columns=None)
            cols = list(df.columns)
            date_type = str(df["date"].dtype) if "date" in df.columns else None
            return cols, date_type
        except Exception:
            return [], None


def read_parquet_date_max(path: Path, col: str = "date") -> Optional[str]:
    """Parquet 파일의 date 컬럼 최대값만 빠르게 읽기.

    1. 먼저 통계(statistics)에서 max 추출 시도
    2. 실패 시 마지막 1행만 읽어서 추출
    """
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(str(path))
        names = pf.schema_arrow.names
        if col not in names:
            return None

        idx = names.index(col)
        md = pf.metadata
        mx = None

        # Row group 통계에서 max 추출
        for rg in range(md.num_row_groups):
            st = md.row_group(rg).column(idx).statistics
            if st is None or st.max is None:
                continue
            v = st.max
            if hasattr(v, "strftime"):
                v = v.strftime(DATE_FORMAT_YMD)
            else:
                v = to_yyyymmdd(str(v))
            mx = v if (mx is None or v > mx) else mx

        if mx:
            return mx
    except Exception:
        pass

    # Fallback: 마지막 1행만 읽기
    try:
        df = pd.read_parquet(str(path), columns=[col]).tail(1)
        if df is not None and not df.empty:
            return parse_yyyymmdd(df[col].iloc[0])
    except Exception:
        pass

    return None


def read_parquet_optimized(
    path: Path,
    columns: Optional[List[str]] = None,
    filters: Optional[List] = None
) -> Optional[pd.DataFrame]:
    """Parquet 파일 최적화 읽기.

    - 필요한 컬럼만 읽기
    - 필터 조건 적용 (pushdown)
    """
    try:
        return pd.read_parquet(
            path,
            columns=columns,
            filters=filters,
            engine="pyarrow"
        )
    except Exception as e:
        logger.warning(f"Parquet 읽기 실패: {path} - {type(e).__name__}: {e}")
        return None


# ============================================================================
# CSV I/O
# ============================================================================
def read_csv_safe(path: Path, **kwargs) -> Optional[pd.DataFrame]:
    """여러 인코딩으로 CSV 읽기 시도."""
    if not path.exists() or path.stat().st_size == 0:
        return None

    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except Exception:
            continue

    # 최후의 시도: pandas 기본
    try:
        return pd.read_csv(path, **kwargs)
    except Exception as e:
        logger.warning(f"CSV 읽기 실패: {path} - {type(e).__name__}")
        return None


# ============================================================================
# 컬럼 매핑 (OHLCV)
# ============================================================================
KOREAN_COL_MAP = {
    "시가": "open",
    "고가": "high",
    "저가": "low",
    "종가": "close",
    "거래량": "volume",
    "등락률": "change_rate",
}

OHLC_ALIASES = {
    "date": ["date", "dt", "trade_date", "yyyymmdd", "ymd"],
    "code": ["code", "ticker", "symbol"],
    "open": ["open", "시가"],
    "high": ["high", "고가"],
    "low": ["low", "저가"],
    "close": ["close", "종가"],
    "volume": ["volume", "거래량"],
    "name": ["name", "종목명"],
}


def infer_ohlc_columns(columns: List[str]) -> Optional[Dict[str, str]]:
    """컬럼 목록에서 OHLC 컬럼 매핑 추론.

    Returns:
        {'date': 'date', 'code': 'ticker', ...} 형태의 매핑
        필수 컬럼이 없으면 None
    """
    lc = {c.lower(): c for c in columns}
    result = {}

    for target, aliases in OHLC_ALIASES.items():
        for alias in aliases:
            if alias.lower() in lc:
                result[target] = lc[alias.lower()]
                break

    # 필수 컬럼 확인
    required = ["date", "code", "open", "high", "low", "close"]
    if not all(k in result for k in required):
        return None

    return result
