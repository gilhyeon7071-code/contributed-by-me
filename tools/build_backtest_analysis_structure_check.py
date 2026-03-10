from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"

PASS = "PASS"
FAIL = "FAIL"
NE = "NOT_EVALUABLE"


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _safe_float(v: Any, default: float = np.nan) -> float:
    try:
        out = float(v)
        if math.isfinite(out):
            return out
        return default
    except Exception:
        return default


def _fmt(v: Any, digits: int = 4) -> str:
    f = _safe_float(v, np.nan)
    if pd.isna(f):
        return "-"
    return f"{f:.{digits}g}"


def _load_fw_module() -> Any:
    p = ROOT / "tools" / "backtest_validation_framework.py"
    spec = importlib.util.spec_from_file_location("btval_fw", str(p))
    if spec is None or spec.loader is None:
        raise ImportError(f"failed to load framework: {p}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_market(path: Path, date_col: str = "date") -> pd.DataFrame:
    df = pd.read_csv(path)
    dcol = date_col if date_col in df.columns else "date"
    if dcol not in df.columns:
        raise ValueError(f"date column not found: {date_col}")
    df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
    df = df.dropna(subset=[dcol]).set_index(dcol).sort_index()
    return df


def _streak_max(mask: pd.Series) -> int:
    m = mask.fillna(False).astype(bool).to_numpy(dtype=bool)
    best = 0
    cur = 0
    for x in m:
        if x:
            cur += 1
            if cur > best:
                best = cur
        else:
            cur = 0
    return int(best)


def _holding_stats(position: pd.Series) -> Dict[str, float]:
    p = pd.to_numeric(position, errors="coerce").fillna(0.0).to_numpy(dtype=float)
    lengths: List[int] = []
    cur = 0
    for v in p:
        if abs(v) > 1e-12:
            cur += 1
        else:
            if cur > 0:
                lengths.append(cur)
                cur = 0
    if cur > 0:
        lengths.append(cur)

    if not lengths:
        return {"n_holds": 0, "median_hold_days": np.nan, "mean_hold_days": np.nan, "max_hold_days": np.nan}

    arr = np.asarray(lengths, dtype=float)
    return {
        "n_holds": int(arr.size),
        "median_hold_days": float(np.median(arr)),
        "mean_hold_days": float(np.mean(arr)),
        "max_hold_days": float(np.max(arr)),
    }


def _simulate_extra_cost(fw: Any, returns: pd.Series, turnover: pd.Series, extra_bps: float) -> float:
    d = returns.fillna(0.0) - turnover.fillna(0.0) * (extra_bps / 10000.0)
    return _safe_float(fw.annualized_return(d), np.nan)


def _gate_map(checklist: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    items = checklist.get("items", []) or []
    return {str(x.get("name")): x for x in items}


def _gate_status(gates: Dict[str, Dict[str, Any]], name: str) -> str:
    g = gates.get(name)
    return str(g.get("status", NE)) if g else NE


def _gate_metric(gates: Dict[str, Dict[str, Any]], name: str) -> str:
    g = gates.get(name)
    return str(g.get("metric", "-")) if g else "-"


def _eval_criteria(
    checklist: Dict[str, Any],
    report: Dict[str, Any],
    market: pd.DataFrame,
    sig: Optional[pd.DataFrame],
    bt: Optional[Any],
    panel: Optional[pd.DataFrame] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[Dict[str, str]]]:
    gates = _gate_map(checklist)
    integ = ((report.get("artifacts") or {}).get("integration") or {})
    max_mdd_limit = -abs(_safe_float(integ.get("max_tolerable_mdd", 0.30), 0.30))

    analysis_df = panel if (panel is not None and len(panel)) else market
    cols = {str(c).lower() for c in analysis_df.columns}
    has_vix = any(c in {str(c).lower() for c in market.columns} for c in ["vix", "vkospi", "vol_index"])
    has_volume = "volume" in cols
    symbol_col = next((c for c in analysis_df.columns if str(c).lower() in {"symbol", "ticker", "code", "stock_code"}), None)
    sector_col = next((c for c in analysis_df.columns if str(c).lower() in {"sector", "industry", "gics", "krx_sector"}), None)
    cap_col = next((c for c in analysis_df.columns if str(c).lower() in {"market_cap", "시가총액"}), None)
    has_symbol_panel = bool(symbol_col and analysis_df[symbol_col].nunique(dropna=True) > 1)

    diffs = market.index.to_series().diff().dropna()
    med_diff_days = float(diffs.dt.total_seconds().median() / 86400.0) if len(diffs) else np.nan
    has_intraday = bool(pd.notna(med_diff_days) and med_diff_days < 1.0)

    returns = pd.Series(dtype=float)
    equity = pd.Series(dtype=float)
    turnover = pd.Series(dtype=float)
    trade_count = 0
    hold_stats = {"n_holds": 0, "median_hold_days": np.nan, "mean_hold_days": np.nan, "max_hold_days": np.nan}
    if bt is not None and sig is not None:
        returns = pd.to_numeric(bt.returns, errors="coerce").dropna()
        equity = pd.to_numeric(bt.equity, errors="coerce").dropna()
        pos = pd.to_numeric(sig.get("position", sig.get("signal", 0.0)), errors="coerce").reindex(market.index).fillna(0.0)
        turnover = pos.diff().abs().fillna(pos.abs())
        trade_count = int((turnover > 0).sum())
        hold_stats = _holding_stats(pos)

    mret = pd.Series(dtype=float)
    corr_bm = np.nan
    beta_bm = np.nan
    if len(returns) and "close" in market.columns:
        mret = pd.to_numeric(market["close"], errors="coerce").pct_change().reindex(returns.index)
        mret = mret.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        if len(mret) == len(returns):
            corr_bm = returns.corr(mret)
            var_m = float(mret.var(ddof=1)) if len(mret) > 1 else np.nan
            if pd.notna(var_m) and var_m > 0:
                beta_bm = float(np.cov(returns, mret, ddof=1)[0, 1] / var_m)

    roll126_med = np.nan
    if len(returns) >= 126:
        roll126 = (1.0 + returns).rolling(126).apply(np.prod, raw=True) - 1.0
        roll126_med = _safe_float(roll126.dropna().median(), np.nan)

    month_pos_ratio = np.nan
    quarter_pos_ratio = np.nan
    if len(returns) >= 60:
        mon = returns.groupby([returns.index.year, returns.index.month]).apply(lambda x: (1.0 + x).prod() - 1.0)
        if len(mon):
            month_pos_ratio = _safe_float((mon > 0).mean(), np.nan)
        qtr = returns.groupby([returns.index.year, returns.index.quarter]).apply(lambda x: (1.0 + x).prod() - 1.0)
        if len(qtr):
            quarter_pos_ratio = _safe_float((qtr > 0).mean(), np.nan)

    dd_longest_days = np.nan
    mdd = np.nan
    if len(equity):
        peak = equity.cummax()
        under = equity < peak
        dd_longest_days = float(_streak_max(under))
        mdd = _safe_float((equity / peak - 1.0).min(), np.nan)

    high_vol_ann = np.nan
    if len(returns) and len(mret):
        vol20 = mret.rolling(20).std()
        q1 = vol20.quantile(0.33)
        q2 = vol20.quantile(0.66)
        bucket = pd.cut(vol20, bins=[-np.inf, q1, q2, np.inf], labels=["LOW", "MID", "HIGH"])
        part = pd.DataFrame({"r": returns, "b": bucket}).dropna()
        hv = part.loc[part["b"] == "HIGH", "r"]
        if len(hv) > 10:
            high_vol_ann = _safe_float((1.0 + hv).prod() ** (252.0 / len(hv)) - 1.0, np.nan)

    ar_10 = np.nan
    ar_30 = np.nan
    ar_50 = np.nan
    if len(returns) and len(turnover):
        fw = _load_fw_module()
        ar_10 = _simulate_extra_cost(fw, returns, turnover.reindex(returns.index).fillna(0.0), 10.0)
        ar_30 = _simulate_extra_cost(fw, returns, turnover.reindex(returns.index).fillna(0.0), 30.0)
        ar_50 = _simulate_extra_cost(fw, returns, turnover.reindex(returns.index).fillna(0.0), 50.0)

    years_covered = int(len(set(market.index.year))) if len(market) else 0
    years_min = int(market.index.min().year) if len(market) else 9999
    has_crash_history = years_min <= 2009

    macro_files = list(LOG_DIR.glob("*rate*latest*.csv")) + list(LOG_DIR.glob("*macro*latest*.csv"))
    has_rate_series = len(macro_files) > 0
    fundamental_files = list(LOG_DIR.glob("*fundamental*latest*.csv")) + list(LOG_DIR.glob("*fundamental*latest*.json"))
    has_market_cap = bool(cap_col and pd.to_numeric(analysis_df[cap_col], errors="coerce").notna().any())
    has_fundamental = (len(fundamental_files) > 0) or has_market_cap

    calibrated = bool(((integ.get("cost_calibration") or {}).get("calibrated", False)))

    criteria: List[Dict[str, Any]] = []

    def add(priority: str, section: str, name: str, status: str, metric: str, reason: str, required: str, action: str) -> None:
        criteria.append(
            {
                "priority": priority,
                "section": section,
                "name": name,
                "status": status,
                "metric": metric,
                "reason": reason,
                "required_data": required,
                "action": action,
            }
        )

    add("필수", "시장환경", "국면(Regime)", _gate_status(gates, "market_regime_response"), _gate_metric(gates, "market_regime_response"), "기존 레짐 게이트 재사용", "market_ohlc", "레짐 FAIL 시 진입필터 강화")

    if pd.notna(roll126_med):
        add("필수", "수익률산출", "롤링(Rolling)", PASS if roll126_med > 0 else FAIL, f"rolling126_median={_fmt(roll126_med)}", "126일 롤링 수익률 중앙값", "strategy_returns", "롤링 수익률 음수면 전략 교정")
    else:
        add("필수", "수익률산출", "롤링(Rolling)", NE, "rolling126_median=-", "전략 수익률 시계열 부족", "strategy_returns>=126", "데이터 기간 확장")

    if pd.notna(high_vol_ann):
        add("필수", "시장환경", "변동성 구간(VIX/VKOSPI)", PASS if high_vol_ann > -0.20 else FAIL, f"high_vol_ann={_fmt(high_vol_ann)} has_vix_col={has_vix}", "고변동 구간 생존성 점검(실현변동 대체 포함)", "vix_or_realized_vol", "고변동 손실 시 포지션 축소")
    else:
        add("필수", "시장환경", "변동성 구간(VIX/VKOSPI)", NE, f"has_vix_col={has_vix}", "고변동 구간 분해 자료 부족", "vix/vkospi_or_longer_returns", "VKOSPI 또는 장기 수익률 확보")

    if pd.notna(mdd):
        add("필수", "리스크", "낙폭 분해", PASS if mdd >= max_mdd_limit else FAIL, f"mdd={_fmt(mdd)} longest_dd_days={_fmt(dd_longest_days)} limit={_fmt(max_mdd_limit)}", "최대낙폭/회복지연 점검", "equity_curve", "MDD 초과 시 리스크 한도 하향")
    else:
        add("필수", "리스크", "낙폭 분해", NE, "mdd=-", "에쿼티 곡선 없음", "equity_curve", "백테스트 산출물 보강")

    if int(hold_stats.get("n_holds", 0)) >= 5:
        med_hold = _safe_float(hold_stats.get("median_hold_days"), np.nan)
        add("필수", "거래특성", "보유 기간별", PASS if (pd.notna(med_hold) and 2 <= med_hold <= 80) else FAIL, f"n_holds={hold_stats.get('n_holds')} median_hold_days={_fmt(med_hold)}", "포지션 연속구간 기반 보유기간 추정", "position_series", "보유기간 편향 시 청산규칙 조정")
    else:
        add("필수", "거래특성", "보유 기간별", NE, f"n_holds={hold_stats.get('n_holds',0)}", "보유기간 산출 샘플 부족", "trade_or_position_log", "거래 샘플 확보")

    if trade_count >= 20:
        wr_trade = np.nan
        if bt is not None and isinstance(bt.trades, pd.DataFrame) and "return" in bt.trades.columns:
            wr_trade = _safe_float((pd.to_numeric(bt.trades["return"], errors="coerce") > 0).mean(), np.nan)
        add("필수", "거래특성", "진입/청산 조건별", PASS, f"trade_count={trade_count} trade_win_rate={_fmt(wr_trade)}", "거래 표본 존재(세부 시그널 라벨은 미지원)", "trade_log_with_signal_tags", "entry/exit reason 태깅 추가")
    else:
        add("필수", "거래특성", "진입/청산 조건별", FAIL if trade_count > 0 else NE, f"trade_count={trade_count}", "거래 샘플 부족", "trade_log", "거래 표본 확충")

    if has_symbol_panel and (sector_col is not None):
        add("필수", "종목특성", "시가총액/섹터별", PASS if has_fundamental else NE, f"symbols={int(analysis_df[symbol_col].nunique())} sector_col={sector_col}", "패널 데이터 존재", "symbol_panel+sector+marketcap", "시총 구간/섹터 성과 분해 실행")
    else:
        add("필수", "종목특성", "시가총액/섹터별", NE, f"has_symbol_panel={has_symbol_panel} sector_col={sector_col}", "단일시계열 또는 섹터 컬럼 부재", "symbol_level_panel_with_sector_marketcap", "종목 패널 데이터 결합")

    if len(turnover):
        annual_turn = _safe_float(turnover.mean() * 252.0, np.nan)
        add("필수", "비용실행", "회전율(Turnover)", PASS if (pd.notna(annual_turn) and annual_turn <= 120.0) else FAIL, f"annual_turnover_proxy={_fmt(annual_turn)}", "포지션변화 기반 회전율 프록시", "position_series", "회전율 과다 시 신호 필터 강화")
    else:
        add("필수", "비용실행", "회전율(Turnover)", NE, "annual_turnover_proxy=-", "포지션 시계열 없음", "position_series", "포지션 기록 보강")

    if pd.notna(quarter_pos_ratio) and years_covered >= 2:
        add("권장", "시간기반", "분기(Quarter)", PASS if quarter_pos_ratio >= 0.40 else FAIL, f"quarter_pos_ratio={_fmt(quarter_pos_ratio)} years={years_covered}", "분기별 승률 패턴", "strategy_returns", "실적시즌 필터 검토")
    else:
        add("권장", "시간기반", "분기(Quarter)", NE, f"years={years_covered}", "분기 패턴 계산 데이터 부족", "strategy_returns>=2y", "기간 확장")

    if pd.notna(month_pos_ratio) and years_covered >= 2:
        add("권장", "시간기반", "월(Month)", PASS if month_pos_ratio >= 0.45 else FAIL, f"month_pos_ratio={_fmt(month_pos_ratio)} years={years_covered}", "월별 승률 패턴", "strategy_returns", "월별 약세구간 회피 규칙")
    else:
        add("권장", "시간기반", "월(Month)", NE, f"years={years_covered}", "월별 패턴 계산 데이터 부족", "strategy_returns>=2y", "기간 확장")

    add("권장", "수익률산출", "연도별 추이", _gate_status(gates, "temporal_consistency"), _gate_metric(gates, "temporal_consistency"), "기존 연도 일관성 게이트 재사용", "strategy_returns", "연도별 저하구간 점검")

    add("권장", "메타", "아웃라이어 의존도", _gate_status(gates, "outlier_concentration"), _gate_metric(gates, "outlier_concentration"), "기존 아웃라이어 게이트 재사용", "trade_returns", "상위 거래 의존도 낮추기")

    if pd.notna(ar_30):
        add("권장", "비용실행", "슬리피지 민감도", PASS if ar_30 > 0 else FAIL, f"ann_ret_10bps={_fmt(ar_10)} ann_ret_30bps={_fmt(ar_30)} ann_ret_50bps={_fmt(ar_50)}", "추가 비용 시뮬레이션", "returns+turnover", "30bps 손익분기점 하회 시 체결개선")
    else:
        add("권장", "비용실행", "슬리피지 민감도", NE, "ann_ret_10/30/50=-", "수익률/회전율 자료 부족", "returns+turnover", "거래비용 로그 보강")

    if pd.notna(corr_bm):
        add("권장", "리스크", "상관관계", PASS if abs(corr_bm) < 0.90 else FAIL, f"corr_to_benchmark={_fmt(corr_bm)}", "전략-벤치마크 상관", "benchmark_returns", "상관 과다 시 분산 전략 추가")
    else:
        add("권장", "리스크", "상관관계", NE, "corr_to_benchmark=-", "벤치마크 상관 산출 불가", "benchmark_returns", "벤치마크 시계열 결합")

    if has_intraday and pd.notna(month_pos_ratio):
        add("선택", "시간기반", "요일/시간대", PASS, f"intraday={has_intraday}", "요일+시간대 분석 가능", "intraday_bars", "시간대별 진입/청산 최적화")
    elif pd.notna(month_pos_ratio):
        add("선택", "시간기반", "요일/시간대", NE, f"intraday={has_intraday}", "요일은 가능하나 시간대는 불가(일봉)", "intraday_bars", "분봉/틱 데이터 추가")
    else:
        add("선택", "시간기반", "요일/시간대", NE, "-", "수익률 시계열 부족", "intraday_or_daily_returns", "수익률 시계열 보강")

    if has_rate_series and len(returns):
        rate_status = NE
        rate_metric = f"rate_files={len(macro_files)}"
        rate_reason = "금리 레짐 결합 산출 실패"
        try:
            rp = sorted(macro_files, key=lambda x: x.stat().st_mtime)[-1]
            rraw = pd.read_csv(rp)
            dcol_r = next((c for c in rraw.columns if str(c).lower() in {"date", "ymd", "trade_date", "dt"}), None)
            rcol_r = next((c for c in rraw.columns if str(c).lower() in {"rate", "base_rate", "policy_rate", "korea_base_rate", "yield_3y", "3y", "금리", "기준금리"}), None)
            if dcol_r is not None and rcol_r is not None:
                rr = pd.DataFrame({
                    "date": pd.to_datetime(rraw[dcol_r], errors="coerce"),
                    "rate": pd.to_numeric(rraw[rcol_r], errors="coerce"),
                }).dropna().sort_values("date")
                ret_df = returns.rename("strategy_ret").reset_index()
                ret_df.columns = ["date", "strategy_ret"]
                ret_df["date"] = pd.to_datetime(ret_df["date"], errors="coerce")
                joined = pd.merge_asof(ret_df.sort_values("date"), rr, on="date", direction="backward").dropna()
                high = joined[joined["rate"] >= 2.5]["strategy_ret"]
                low = joined[joined["rate"] <= 1.0]["strategy_ret"]
                if len(high) >= 40 and len(low) >= 40:
                    ann_high = float((1.0 + high).prod() ** (252.0 / len(high)) - 1.0)
                    ann_low = float((1.0 + low).prod() ** (252.0 / len(low)) - 1.0)
                    spread = ann_high - ann_low
                    rate_status = PASS if ann_high > -0.20 else FAIL
                    rate_metric = f"high_rate_ann={_fmt(ann_high)} low_rate_ann={_fmt(ann_low)} spread={_fmt(spread)} n_high={len(high)} n_low={len(low)}"
                    rate_reason = "고금리/저금리 레짐별 연환산 성과 비교"
                else:
                    rate_status = NE
                    rate_metric = f"n_high={len(high)} n_low={len(low)}"
                    rate_reason = "금리 레짐 샘플 부족"
            else:
                rate_status = NE
                rate_metric = f"rate_files={len(macro_files)}"
                rate_reason = "금리 파일 컬럼(date/rate) 부재"
        except Exception as ex:
            rate_status = NE
            rate_metric = f"rate_files={len(macro_files)}"
            rate_reason = f"금리 결합 오류: {type(ex).__name__}"
        add("선택", "시장환경", "금리 환경", rate_status, rate_metric, rate_reason, "rate_series_joined", "금리 레짐별 성과 기준 유지/조정")
    elif has_rate_series:
        add("선택", "시장환경", "금리 환경", NE, f"rate_files={len(macro_files)}", "전략 수익률 부재", "strategy_returns+rate_series", "수익률 시계열 확보")
    else:
        add("선택", "시장환경", "금리 환경", NE, "rate_files=0", "금리 시계열 부재", "rate_series", "국채/기준금리 시계열 추가")

    if has_symbol_panel and has_volume and calibrated:
        add("선택", "비용실행", "용량 분석(Capacity)", PASS, f"symbol_panel={has_symbol_panel} volume={has_volume} calibrated={calibrated}", "용량 추정 입력 충족", "symbol_volume+fills", "AUM 단계별 충격비용 추정")
    else:
        add("선택", "비용실행", "용량 분석(Capacity)", NE, f"symbol_panel={has_symbol_panel} volume={has_volume} calibrated={calibrated}", "체결/유동성 입력 부족", "symbol_volume+fills", "체결로그/종목별 거래대금 수집")

    if pd.notna(beta_bm):
        add("선택", "리스크", "베타 노출", PASS if abs(beta_bm) <= 1.5 else FAIL, f"beta_to_benchmark={_fmt(beta_bm)}", "시장 베타 노출 추정", "benchmark_returns", "베타 과다 시 헤지/사이징 조정")
    else:
        add("선택", "리스크", "베타 노출", NE, "beta_to_benchmark=-", "베타 산출 불가", "benchmark_returns", "벤치마크 수익률 결합")

    summary = {
        "years_covered": years_covered,
        "start_date": str(market.index.min().date()) if len(market) else "-",
        "end_date": str(market.index.max().date()) if len(market) else "-",
        "has_intraday": has_intraday,
        "has_symbol_panel": has_symbol_panel,
        "has_sector": sector_col is not None,
        "has_volume": has_volume,
        "has_rate_series": has_rate_series,
        "has_fundamental": has_fundamental,
        "has_crash_history_2008": has_crash_history,
        "trade_count_proxy": trade_count,
        "median_hold_days": hold_stats.get("median_hold_days"),
    }

    req_map: Dict[str, Dict[str, str]] = {
        "intraday_bars": {"request": "분봉/틱 데이터(최소 1분봉)", "why": "요일/시간대 분석"},
        "symbol_level_panel_with_sector_marketcap": {"request": "종목별 OHLCV + 섹터 + 시가총액", "why": "시총/섹터/용량 분석"},
        "trade_log_with_signal_tags": {"request": "체결 로그에 entry/exit reason 태그", "why": "진입/청산 조건별 품질"},
        "trade_log": {"request": "주문/체결 로그(시간,가격,수량,수수료)", "why": "거래특성/비용 검증"},
        "rate_series": {"request": "금리 시계열(기준금리/국채금리)", "why": "금리 레짐 분석"},
        "benchmark_returns": {"request": "벤치마크 수익률 시계열(KOSPI/KOSDAQ)", "why": "상관/베타 분석"},
        "vix/vkospi_or_longer_returns": {"request": "VKOSPI 또는 장기 변동성 지표", "why": "고변동 생존 검증"},
        "strategy_returns>=2y": {"request": "전략 수익률 최소 2년 이상", "why": "월/분기 패턴 신뢰성"},
    }

    needed_keys = sorted({c["required_data"] for c in criteria if c["status"] == NE and c.get("required_data") in req_map})
    requests = [req_map[k] | {"key": k} for k in needed_keys]

    return criteria, summary, requests


def _priority_counts(criteria: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for p in ["필수", "권장", "선택"]:
        part = [c for c in criteria if c.get("priority") == p]
        out[p] = {
            "total": len(part),
            "pass_n": sum(1 for c in part if c.get("status") == PASS),
            "fail_n": sum(1 for c in part if c.get("status") == FAIL),
            "not_evaluable_n": sum(1 for c in part if c.get("status") == NE),
        }
    return out


def _render_md(payload: Dict[str, Any]) -> str:
    c = payload.get("criteria", [])
    pc = payload.get("priority_counts", {})
    lines: List[str] = []
    lines.append(f"# 백테스트 분석 구조 체크 ({payload.get('generated_at')})")
    lines.append("")
    lines.append(f"- 데이터 범위: {payload.get('data_summary',{}).get('start_date','-')} .. {payload.get('data_summary',{}).get('end_date','-')}")
    lines.append(f"- 운영판정: {payload.get('operation_judgment','-')}")
    lines.append("")
    lines.append("## 우선순위 요약")
    lines.append("")
    lines.append("| 우선순위 | total | pass | fail | not_evaluable |")
    lines.append("|---|---:|---:|---:|---:|")
    for p in ["필수", "권장", "선택"]:
        row = pc.get(p, {})
        lines.append(f"| {p} | {row.get('total',0)} | {row.get('pass_n',0)} | {row.get('fail_n',0)} | {row.get('not_evaluable_n',0)} |")

    lines.append("")
    lines.append("## 기준별 체크")
    lines.append("")
    lines.append("| 우선순위 | 분류 | 기준 | 상태 | 지표 | 이유 |")
    lines.append("|---|---|---|---|---|---|")
    for row in c:
        lines.append(
            f"| {row.get('priority')} | {row.get('section')} | {row.get('name')} | {row.get('status')} | {row.get('metric')} | {row.get('reason')} |"
        )

    lines.append("")
    lines.append("## 요청사항(데이터/연동)")
    req = payload.get("requests", [])
    if not req:
        lines.append("- 없음")
    else:
        for r in req:
            lines.append(f"- {r.get('request')}: {r.get('why')}")

    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Check backtest analysis structure against priority taxonomy")
    ap.add_argument("--checklist-json", default=str(LOG_DIR / "backtest_validation_checklist_latest.json"))
    ap.add_argument("--report-json", default=str(LOG_DIR / "backtest_validation_latest.json"))
    ap.add_argument("--market-csv", default=str(LOG_DIR / "backtest_market_ohlc_latest.csv"))
    ap.add_argument("--panel-csv", default=str(LOG_DIR / "backtest_symbol_panel_latest.csv"))
    ap.add_argument("--date-col", default="date")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--out-md", default="")
    args = ap.parse_args()

    checklist = _load_json(Path(args.checklist_json))
    report = _load_json(Path(args.report_json))
    if not checklist:
        raise FileNotFoundError(f"missing checklist: {args.checklist_json}")
    if not report:
        raise FileNotFoundError(f"missing report: {args.report_json}")

    market = _load_market(Path(args.market_csv), date_col=args.date_col)
    panel = None
    panel_path = Path(args.panel_csv)
    if panel_path.exists():
        try:
            panel = _load_market(panel_path, date_col=args.date_col)
        except Exception as ex:
            panel = None
            print(f"[BTSTRUCT][WARN] panel load failed: {type(ex).__name__}: {ex}")

    sig = None
    bt = None
    bt_context_error = ""
    try:
        fw = _load_fw_module()
        integ = ((report.get("artifacts") or {}).get("integration") or {})
        strategy_source = str(integ.get("strategy_source", ""))
        backtest_source = str(integ.get("backtest_source", ""))
        if strategy_source == "builtin" and backtest_source == "builtin":
            params = integ.get("params") or {"fast": 10, "slow": 100, "allow_short": True, "position_scale": 0.7}
            cm_raw = integ.get("cost_model") or {}
            cm = fw.CostModel(
                commission_bps=_safe_float(cm_raw.get("commission_bps", 2.0), 2.0),
                slippage_bps=_safe_float(cm_raw.get("slippage_bps", 3.0), 3.0),
                spread_bps=_safe_float(cm_raw.get("spread_bps", 2.0), 2.0),
            )
            sig = fw.sma_cross_strategy(market, params)
            bt = fw.reference_backtest(market, sig, params, cm)
    except Exception as ex:
        sig = None
        bt = None
        bt_context_error = str(ex)
        print(f"[BTSTRUCT][WARN] bt context disabled: {bt_context_error}")

    criteria, data_summary, requests = _eval_criteria(checklist, report, market, sig, bt, panel=panel)

    payload = {
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operation_judgment": checklist.get("operation_judgment", "-"),
        "priority_counts": _priority_counts(criteria),
        "data_summary": data_summary,
        "bt_context_error": bt_context_error,
        "criteria": criteria,
        "requests": requests,
    }

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = Path(args.out_json) if args.out_json else (LOG_DIR / f"backtest_analysis_structure_{stamp}.json")
    out_md = Path(args.out_md) if args.out_md else (LOG_DIR / f"backtest_analysis_structure_{stamp}.md")
    latest_json = LOG_DIR / "backtest_analysis_structure_latest.json"
    latest_md = LOG_DIR / "backtest_analysis_structure_latest.md"

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    md = _render_md(payload)
    out_md.write_text(md, encoding="utf-8-sig")
    latest_md.write_text(md, encoding="utf-8-sig")

    p = payload["priority_counts"]
    print(
        "[BTSTRUCT] "
        + f"필수(pass={p.get('필수',{}).get('pass_n',0)},fail={p.get('필수',{}).get('fail_n',0)},ne={p.get('필수',{}).get('not_evaluable_n',0)}) "
        + f"권장(pass={p.get('권장',{}).get('pass_n',0)},fail={p.get('권장',{}).get('fail_n',0)},ne={p.get('권장',{}).get('not_evaluable_n',0)}) "
        + f"선택(pass={p.get('선택',{}).get('pass_n',0)},fail={p.get('선택',{}).get('fail_n',0)},ne={p.get('선택',{}).get('not_evaluable_n',0)})"
    )
    print(f"[BTSTRUCT] json={out_json}")
    print(f"[BTSTRUCT] md={out_md}")
    print(f"[BTSTRUCT] latest_json={latest_json}")
    print(f"[BTSTRUCT] latest_md={latest_md}")

    has_fail = any(x.get("status") == FAIL for x in criteria)
    return 2 if has_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())





