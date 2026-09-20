"""D1 입력 데이터 — KRX 수동 다운로드 3종 + KOSPI200 종가를 읽고, 붙이고, 검사한다.

판단(모집단·순위)은 하지 않는다. spec/D1_input_data.md 참조.
멈춤은 사고가 아니다. 멈췄다는 사실이 기록되지 않는 것이 사고다.
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

PRICE_COLUMNS = ["종목코드", "종목명", "시장구분", "종가", "시가총액", "상장주식수"]
BASIC_COLUMNS = ["단축코드", "시장구분", "증권구분", "주식종류", "상장일"]
FLAG_COLUMNS = ["종목코드", "매매거래정지", "정리매매 종목", "관리종목"]
BLOCK_FLAGS = {"매매거래정지": "trading_halt", "정리매매 종목": "liquidation", "관리종목": "administrative"}
CODE_RE = re.compile(r"^[0-9A-Z]{6}$")


def read_krx_csv(path: Path) -> pd.DataFrame:
    """KRX CSV 는 CP949. 실패하면 UTF-8-SIG 로 한 번 더. 모든 열은 문자열로 읽는다."""
    last_err: Optional[Exception] = None
    for enc in ("cp949", "utf-8-sig"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str, keep_default_na=False)
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            last_err = e
    raise ValueError(f"CSV 를 읽을 수 없음: {path} ({last_err})")


def classify_krx_file(df: pd.DataFrame) -> Optional[str]:
    """열 모양으로 파일 종류를 판별한다. KRX 다운로드 이름은 data_XXXX_YYYYMMDD 로 비슷하다."""
    cols = set(df.columns)
    if set(FLAG_COLUMNS) <= cols:
        return "flags"
    if set(BASIC_COLUMNS) <= cols:
        return "basic"
    if set(PRICE_COLUMNS) <= cols:
        return "price"
    return None


def read_zone_identifier(path: Path) -> Dict[str, str]:
    """윈도우가 다운로드 파일에 붙이는 출처 기록. 이름을 바꿔 복사하면 사라진다."""
    try:
        text = Path(str(path) + ":Zone.Identifier").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {}
    out: Dict[str, str] = {}
    for line in text.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _to_int64(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(cleaned, errors="coerce").astype("Int64")


def check_receipt(modified_at: datetime, selection_date: str, min_hhmm: str) -> Optional[str]:
    """받은 날짜가 선정일이고 min_hhmm 이후인지. 아니면 사유 문자열."""
    if modified_at.strftime("%Y%m%d") != selection_date:
        return f"INPUT_NOT_SELECTION_DATE:{modified_at:%Y%m%d}!={selection_date}"
    if modified_at.strftime("%H%M") < min_hhmm:
        return f"INPUT_BEFORE_CLOSE:{modified_at:%H%M}<{min_hhmm}"
    return None


def _rows_check(name: str, n: int, prev: Optional[int], th: Dict[str, Any], reasons: List[str]) -> None:
    lo, hi = th[f"{name}_rows_abs_min"], th[f"{name}_rows_abs_max"]
    if not (lo <= n <= hi):
        reasons.append(f"INPUT_ROWCOUNT_OUT_OF_RANGE:{name}={n} not in [{lo},{hi}]")
    if prev:
        change = abs(n - prev) / prev
        if change > th["rows_rel_change_max"]:
            reasons.append(f"INPUT_ROWCOUNT_JUMP:{name}={n} prev={prev} change={change:.3f}")


def build_input(
    price: pd.DataFrame,
    basic: pd.DataFrame,
    flags: pd.DataFrame,
    *,
    thresholds: Dict[str, Any],
    previous_rows: Optional[Dict[str, int]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """세 파일을 종목코드로 붙인다. 반환: (입력표, 검사결과). 검사결과 status 는 OK 또는 STOP."""
    reasons: List[str] = []
    previous_rows = previous_rows or {}
    for name, df, cols in (("price", price, PRICE_COLUMNS), ("basic", basic, BASIC_COLUMNS), ("flags", flags, FLAG_COLUMNS)):
        missing = [c for c in cols if c not in df.columns]
        if missing:
            reasons.append(f"INPUT_SCHEMA_CHANGED:{name}:missing={missing}")
    if reasons:
        return pd.DataFrame(), {"status": "STOP", "reasons": reasons}

    rows = {"price": len(price), "basic": len(basic), "flags": len(flags)}
    for name, n in rows.items():
        _rows_check(name, n, previous_rows.get(name), thresholds, reasons)

    # 가드는 정규화 전 원본에 건다 — 빈 값을 zfill 하면 000000 이 된다
    for name, df, key in (("price", price, "종목코드"), ("basic", basic, "단축코드"), ("flags", flags, "종목코드")):
        raw = df[key].astype(str)
        bad = raw[~raw.str.fullmatch(CODE_RE)]
        if len(bad):
            reasons.append(f"INPUT_CODE_INVALID:{name}:{len(bad)}:{list(bad.head(3))}")

    p = price[PRICE_COLUMNS].rename(columns={"종목코드": "code", "종목명": "name", "시장구분": "market",
                                             "종가": "close", "시가총액": "market_cap", "상장주식수": "listed_shares"})
    for c in ("close", "market_cap", "listed_shares"):
        p[c] = _to_int64(p[c])
    b = basic[BASIC_COLUMNS].rename(columns={"단축코드": "code", "시장구분": "market_basic", "증권구분": "security_group",
                                             "주식종류": "share_type", "상장일": "listed_date"})
    f = flags[FLAG_COLUMNS].rename(columns={"종목코드": "code", **BLOCK_FLAGS})

    out = p.merge(b, on="code", how="left", indicator="_basic").merge(f, on="code", how="left", indicator="_flags")
    out["basic_joined"] = out["_basic"].eq("both")
    out["flags_joined"] = out["_flags"].eq("both")
    out = out.drop(columns=["_basic", "_flags"])

    expected = out["close"] * out["listed_shares"]
    out["mcap_check"] = "OK"
    out.loc[out["market_cap"].isna() | out["close"].isna() | out["listed_shares"].isna(), "mcap_check"] = "MISSING"
    out.loc[out["mcap_check"].eq("OK") & expected.ne(out["market_cap"]), "mcap_check"] = "MISMATCH"

    # 시총 상위 N (KOSPI) 안의 결함은 선정 결과를 바꾸므로 멈춘다.
    # 순위를 검사 대상 값(시가총액)으로만 매기면 망가진 값이 상위에서 빠져 스스로를 숨긴다(2026-09-17 시험에서 발견).
    # 그래서 시가총액 순위와 종가×상장주식수 순위의 합집합을 검사한다.
    top_n = int(thresholds["top_check_n"])
    kospi = out[out["market"].eq("KOSPI")].copy()
    kospi["_expected_mcap"] = kospi["close"] * kospi["listed_shares"]
    top_by_file = kospi.sort_values("market_cap", ascending=False, na_position="first").head(top_n)
    top_by_calc = kospi.sort_values("_expected_mcap", ascending=False, na_position="first").head(top_n)
    top = kospi.loc[top_by_file.index.union(top_by_calc.index)]
    bad_price = top[(top["close"].fillna(0) <= 0) | (top["market_cap"].fillna(0) <= 0)]
    if len(bad_price):
        reasons.append(f"INPUT_PRICE_INVALID_IN_TOP{top_n}:{list(bad_price['code'])}")
    unjoined = top[~(top["basic_joined"] & top["flags_joined"])]
    if len(unjoined):
        reasons.append(f"INPUT_UNJOINED_IN_TOP{top_n}:{list(unjoined['code'])}")
    mismatch = top[top["mcap_check"].ne("OK")]
    if len(mismatch):
        reasons.append(f"INPUT_MCAP_CHECK_IN_TOP{top_n}:{list(mismatch['code'])}")

    check = {
        "status": "STOP" if reasons else "OK",
        "reasons": reasons,
        "rows": rows,
        "unjoined_basic": int((~out["basic_joined"]).sum()),
        "unjoined_flags": int((~out["flags_joined"]).sum()),
        "basic_not_in_price": int(len(set(b["code"]) - set(p["code"]))),
        "mcap_check_counts": out["mcap_check"].value_counts().to_dict(),
    }
    return out, check


def load_kospi200(index_csv: Path, selection_date: str, *, index_code: str, window: int) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """선정일 포함 최근 window 개 유효 종가. 선정일 행이 없으면 멈춘다."""
    reasons: List[str] = []
    df = pd.read_csv(index_csv, dtype=str, keep_default_na=False)
    x = df[df["index_code"].eq(index_code)].copy()
    x["close_num"] = pd.to_numeric(x["close"], errors="coerce")
    x = x[x["date"].le(selection_date) & x["close_num"].gt(0)].sort_values("date")
    x = x.drop_duplicates(subset=["date"], keep="last")
    if x.empty or x["date"].iloc[-1] != selection_date:
        last = x["date"].iloc[-1] if len(x) else None
        reasons.append(f"INDEX_NOT_READY:last={last} selection={selection_date}")
    tail = x.tail(window)
    if len(tail) < window:
        reasons.append(f"INDEX_WINDOW_SHORT:{len(tail)}<{window}")
    fetched = tail["fetched_at"].iloc[-1] if ("fetched_at" in tail.columns and len(tail)) else None
    series = tail[["date", "close_num"]].rename(columns={"close_num": "close"}).reset_index(drop=True)
    return series, {"status": "STOP" if reasons else "OK", "reasons": reasons, "rows": int(len(tail)),
                    "last_date": tail["date"].iloc[-1] if len(tail) else None, "fetched_at": fetched}
