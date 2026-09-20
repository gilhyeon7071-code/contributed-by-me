from __future__ import annotations

from typing import Any, Iterable

import numpy as np
import pandas as pd

CONTRACT_VERSION = "PRICE_HISTORY_INTEGRITY_V1"
DEFAULT_MAX_ABS_SESSION_RETURN = 1.0
SEGMENT_COL = "price_history_segment"
KEY_COL = "price_history_key"
SESSION_COL = "price_session_index"


def apply_price_history_contract(
    df: pd.DataFrame,
    *,
    max_abs_session_return: float = DEFAULT_MAX_ABS_SESSION_RETURN,
    max_gap_sessions: int = 0,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Validate rows and split history at missing or abnormal market sessions.

    ``max_gap_sessions`` tolerates short runs of missing sessions (e.g. a 1-2
    day trading halt) without breaking the segment, so long as the return
    across the gap does not also trip ``max_abs_session_return``. Gaps longer
    than ``max_gap_sessions`` always break the segment, same as a gap of any
    length did before this parameter existed (default 0 preserves that exact
    behavior).
    """
    required = {"date", "code", "open", "high", "low", "close"}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"price history contract missing columns: {missing}")

    work = df.copy()
    rows_in = int(len(work))
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["code"] = work["code"].astype(str).str.strip().str.zfill(6)
    numeric_cols = [c for c in ("open", "high", "low", "close", "volume", "value") if c in work.columns]
    for col in numeric_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    valid = work["date"].notna() & work["code"].str.upper().str.fullmatch(r"[0-9A-Z]{6}", na=False)
    for col in ("open", "high", "low", "close"):
        valid &= np.isfinite(work[col]) & work[col].gt(0)
    valid &= work["high"].ge(work[["open", "close", "low"]].max(axis=1))
    valid &= work["low"].le(work[["open", "close", "high"]].min(axis=1))
    if "volume" in work.columns:
        valid &= work["volume"].isna() | (np.isfinite(work["volume"]) & work["volume"].ge(0))
    if "value" in work.columns:
        valid &= np.isfinite(work["value"]) & work["value"].gt(0)
        # [2026-09-10] **날짜 단위 가드.** 행 단위 `value > 0` 만으로는 부족하다.
        #   `value` 는 원천 값이 아니라 `종가 x 거래량` 계산이고, 과거에 int32 곱셈으로
        #   2^31 에서 감긴 적이 있다(2025-12-26~2026-02-24, 24거래일).
        #   감겨서 **음수**가 된 행은 위 줄이 막지만, **양수 작은 값**이 된 행은 통과한다 -
        #   삼성전자 5조가 +14억으로 보이면 "유동성 낮은 종목" 이 될 뿐 검출되지 않는다.
        #   실측 피해: 거래가능 유니버스가 ~800 -> 33~66 종목으로 붕괴하고
        #   그 구성이 사실상 난수가 됐다(감긴 값은 2^32 나머지). 그 집합에서 53건이 진입했다.
        #   거래대금 음수는 물리적으로 불가능하므로 **그날 자료 전체를 믿지 않는다.**
        neg_days = work.loc[np.isfinite(work["value"]) & work["value"].lt(0), "date"].unique()
        if len(neg_days):
            valid &= ~work["date"].isin(neg_days)

    invalid_cols = [c for c in ("date", "code", "open", "high", "low", "close", "volume", "value") if c in work.columns]
    invalid = work.loc[~valid, invalid_cols].copy()
    work = work.loc[valid].copy()
    if work.empty:
        raise ValueError("price history contract rejected every row")

    work = work.sort_values(["code", "date"], kind="mergesort").reset_index(drop=True)
    sessions = pd.Index(sorted(work["date"].dropna().unique()))
    session_map = pd.Series(np.arange(len(sessions), dtype=np.int64), index=sessions)
    work[SESSION_COL] = work["date"].map(session_map).astype("int64")

    by_code = work.groupby("code", sort=False)
    prev_session = by_code[SESSION_COL].shift(1)
    prev_close = by_code["close"].shift(1)
    nonfirst = prev_session.notna()
    gap_len = work[SESSION_COL].sub(prev_session).sub(1).clip(lower=0)
    session_gap = nonfirst & gap_len.gt(0)
    hard_gap = session_gap & gap_len.gt(int(max_gap_sessions))
    soft_gap = session_gap & ~hard_gap
    session_ret = work["close"].div(prev_close).sub(1.0)
    extreme = nonfirst & ~hard_gap & session_ret.abs().gt(float(max_abs_session_return))
    segment_start = (~nonfirst) | hard_gap | extreme
    work[SEGMENT_COL] = segment_start.groupby(work["code"], sort=False).cumsum().astype("int64")
    work[KEY_COL] = work["code"] + ":" + work[SEGMENT_COL].astype(str)
    work["price_history_gap_sessions"] = gap_len.where(nonfirst, 0).astype("int64")

    audit = {
        "contract_version": CONTRACT_VERSION,
        "max_abs_session_return": float(max_abs_session_return),
        "max_gap_sessions": int(max_gap_sessions),
        "rows_in": rows_in,
        "rows_out": int(len(work)),
        "invalid_rows": int((~valid).sum()),
        "session_gap_breaks": int(hard_gap.sum()),
        "soft_gap_bridges": int((soft_gap & ~extreme).sum()),
        "extreme_return_breaks": int(extreme.sum()),
        "segments": int(work[KEY_COL].nunique()),
        "codes": int(work["code"].nunique()),
        "trading_sessions": int(len(sessions)),
        "date_min": str(work["date"].min().date()),
        "date_max": str(work["date"].max().date()),
        "invalid_examples": invalid.head(10).astype(str).to_dict("records"),
        "extreme_examples": work.loc[extreme, ["date", "code", "close"]]
        .assign(previous_close=prev_close.loc[extreme].to_numpy(), session_return=session_ret.loc[extreme].to_numpy())
        .head(20).astype(str).to_dict("records"),
    }
    audit["log_line"] = (
        f"contract={CONTRACT_VERSION} rows_in={rows_in} rows_out={len(work)} "
        f"invalid_rows={audit['invalid_rows']} gap_breaks={audit['session_gap_breaks']} "
        f"soft_gap_bridges={audit['soft_gap_bridges']} max_gap_sessions={audit['max_gap_sessions']} "
        f"extreme_breaks={audit['extreme_return_breaks']} segments={audit['segments']}"
    )
    work.attrs["price_history_integrity"] = audit
    return work, audit


def add_exact_session_forward_returns(
    df: pd.DataFrame,
    horizons: Iterable[int] = (1, 2, 5),
    *,
    close_col: str = "close",
) -> pd.DataFrame:
    """정확히 h 세션 뒤 행에 대해서만 전방 수익률을 붙인다.

    [2026-09-08 복원] 이 함수는 2026-07-21 에 모듈에서 **사라졌다.** 백업이 없어
      구현 원본을 되찾을 수 없었고, 계약(테스트 단언 + 형제 함수 docstring)으로 재구성했다.
      사라진 뒤 49일간 아래 셋이 import 단계에서 죽어 있었다.
        tools/build_new_method_path_expectancy_audit.py
        tools/indicator_factor_diagnostic.py
        tests/test_price_history_contract.py   (수집 실패 -> 회귀 그물이 끊겼다)
      PLANS 2026-09-08 (257).

    ``add_trade_count_forward_returns`` 와의 차이 (형제 docstring 이 명시한 계약)
      trade_count  : 같은 구간의 **h번째 다음 관측 행**. 달력상 몇 세션 뒤든 상관없다
      exact_session: 그 행이 **정확히 h 세션 뒤**일 때만 값을 남긴다. 아니면 NaN
    간격이 있는 자리에서 두 함수는 갈린다. 지표를 만들 때 "h일 뒤 수익률" 이라고 말하려면
    이쪽이 맞다 - 저쪽은 간격을 건너뛴 값을 h일로 부르게 된다.

    구간(SEGMENT_COL)은 이미 ``apply_price_history_contract`` 가 나눠 뒀으므로
    구간을 넘어가는 참조는 grouping 단계에서 이미 차단된다. 여기서는 그 위에
    **세션 거리 == h** 조건을 더한다.
    """
    if KEY_COL not in df.columns:
        raise ValueError(f"{KEY_COL} missing; apply price history contract first")
    out = df.copy()
    grouped = out.groupby(KEY_COL, sort=False)
    for raw_h in horizons:
        h = int(raw_h)
        if h <= 0:
            raise ValueError(f"horizon must be positive: {h}")
        future_close = grouped[close_col].shift(-h)
        future_session = grouped[SESSION_COL].shift(-h)
        gap = future_session.sub(out[SESSION_COL])
        # 정확히 h 세션 뒤인 행만 남긴다. 간격을 건너뛴 행은 버린다.
        exact = future_close.notna() & gap.eq(h)
        out[f"fwd_ret_{h}d"] = future_close.div(out[close_col]).sub(1.0).where(exact)
        out[f"fwd_date_{h}d"] = grouped["date"].shift(-h).where(exact)
    return out


def add_trade_count_forward_returns(
    df: pd.DataFrame,
    horizons: Iterable[int] = (1, 2, 5),
    *,
    close_col: str = "close",
) -> pd.DataFrame:
    """Add forward returns to the Nth next *observed* row in the same segment.

    Unlike ``add_exact_session_forward_returns``, this does not require the
    Nth next row to land exactly N calendar sessions later, so it stays
    populated across a soft-bridged gap (see ``max_gap_sessions`` on
    ``apply_price_history_contract``). ``fwd_gap_sessions_{h}d`` records how
    many calendar sessions actually elapsed, so callers can tell a normal
    N-session-later fill (value == h) from one that crossed a bridged gap
    (value > h).
    """
    if KEY_COL not in df.columns:
        raise ValueError(f"{KEY_COL} missing; apply price history contract first")
    out = df.copy()
    grouped = out.groupby(KEY_COL, sort=False)
    for raw_h in horizons:
        h = int(raw_h)
        if h <= 0:
            raise ValueError(f"horizon must be positive: {h}")
        future_close = grouped[close_col].shift(-h)
        future_session = grouped[SESSION_COL].shift(-h)
        has_future = future_close.notna()
        out[f"fwd_ret_{h}d"] = future_close.div(out[close_col]).sub(1.0).where(has_future)
        out[f"fwd_date_{h}d"] = grouped["date"].shift(-h).where(has_future)
        out[f"fwd_gap_sessions_{h}d"] = future_session.sub(out[SESSION_COL]).where(has_future)
    return out
