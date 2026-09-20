# -*- coding: utf-8 -*-
"""
paper_pending_report.py

목적
- paper_state.json의 open_positions 중, entry_date 이후(> entry_date) 가격 데이터가 아직 없는 종목을
  [PENDING] 섹션으로 분리 출력합니다.
- "데이터가 없어 멈춘 것처럼 보이는 문제"를 시각적으로 해결하기 위한 리포트 보조 스크립트입니다.

입력
- paper/paper_state.json
- paper/prices/ohlcv_paper.parquet

출력
- 콘솔 출력
- 2_Logs/paper_pending_report_<YYYYMMDD_HHMMSS>.txt
- 2_Logs/paper_pending_report_<YYYYMMDD_HHMMSS>.json
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
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
def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _safe_read_json(path: Path) -> Dict[str, Any]:
    # paper_state.json은 utf-8로 읽혀야 하나, name 필드가 깨져도 JSON 파싱만 되면 진행 가능
    raw = path.read_text(encoding="utf-8", errors="replace")
    return json.loads(raw)


def _normalize_code(x: Any) -> str:
    s = "" if x is None else str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(6)


def _to_ymd_series(date_series: pd.Series) -> pd.Series:
    """
    parquet date 컬럼이 datetime/문자/정수 등 혼재 가능.
    - 우선 pd.to_datetime으로 파싱
    - 실패분은 숫자만 남긴 뒤 %Y%m%d로 재시도
    """
    y = pd.to_datetime(date_series, errors="coerce")
    y2 = pd.to_datetime(
        date_series.astype(str).str.replace(r"[^0-9]", "", regex=True),
        format="%Y%m%d",
        errors="coerce",
    )
    y = y.fillna(y2)
    return y.dt.strftime("%Y%m%d")


@dataclass
class PendingItem:
    code: str
    name: str
    entry_date: str
    entry_price: float
    stop_loss: float
    stop_price: float
    prices_date_max: str
    last_price_date_for_code: str
    reason: str


def main() -> int:
    base_dir = Path(__file__).resolve().parents[1]  # ...\tools -> base
    state_file = base_dir / "paper" / "paper_state.json"
    prices_file = base_dir / "paper" / "prices" / "ohlcv_paper.parquet"
    intraday_prices_file = base_dir / "2_Logs" / "intraday_prices_latest.csv"
    out_dir = base_dir / "2_Logs"
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = _now_ts()
    out_txt = out_dir / f"paper_pending_report_{ts}.txt"
    out_json = out_dir / f"paper_pending_report_{ts}.json"

    lines: List[str] = []
    report: Dict[str, Any] = {
        "ts": ts,
        "base_dir": str(base_dir),
        "state_file": str(state_file),
        "prices_file": str(prices_file),
        "intraday_prices_file": str(intraday_prices_file),
        "prices_date_max": None,
        "eod_prices_date_max": None,
        "intraday_prices_date_max": None,
        "intraday_price_codes": 0,
        "pending": [],
        "active": [],
        "carryover_queue_file": str(out_dir / "pending_entry_signals_latest.csv"),
        "carryover_queue": [],
        "notes": [],
    }

    if not state_file.exists():
        msg = f"[FATAL] STATE_FILE not found: {state_file}"
        _log_print(msg)
        out_txt.write_text(msg + "\n", encoding="utf-8")
        report["notes"].append(msg)
        out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return 2

    if not prices_file.exists():
        msg = f"[FATAL] PRICES_FILE not found: {prices_file}"
        _log_print(msg)
        out_txt.write_text(msg + "\n", encoding="utf-8")
        report["notes"].append(msg)
        out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return 2

    st = _safe_read_json(state_file)
    op = st.get("open_positions", []) or []
    if not isinstance(op, list):
        op = []

    # prices: code/date만 읽어서 빠르게 판단
    df = pd.read_parquet(prices_file, columns=["code", "date"])
    df["code"] = df["code"].apply(_normalize_code)
    df["ymd"] = _to_ymd_series(df["date"])
    eod_prices_date_max = str(df["ymd"].max())
    report["eod_prices_date_max"] = eod_prices_date_max
    if intraday_prices_file.exists():
        try:
            rdf = pd.read_csv(intraday_prices_file, usecols=["code", "date"], dtype=str)
            rdf["code"] = rdf["code"].apply(_normalize_code)
            rdf["ymd"] = rdf["date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
            rdf = rdf[(rdf["code"].str.len() == 6) & (rdf["ymd"].str.len() == 8)].copy()
            if not rdf.empty:
                report["intraday_prices_date_max"] = str(rdf["ymd"].max())
                report["intraday_price_codes"] = int(rdf["code"].nunique())
                df = pd.concat([df[["code", "ymd"]], rdf[["code", "ymd"]]], ignore_index=True)
                df = df.drop_duplicates(["code", "ymd"], keep="last")
                report["notes"].append(f"intraday_prices_merged_rows={len(rdf)}")
        except Exception as e:
            report["notes"].append(f"intraday_prices_read_failed: {type(e).__name__}: {e}")

    prices_date_max = str(df["ymd"].max())
    report["prices_date_max"] = prices_date_max

    # code별 보유 날짜 목록
    code_dates = (
        df.groupby("code")["ymd"]
        .apply(lambda s: sorted(set([x for x in s.tolist() if isinstance(x, str) and x.strip()])))
        .to_dict()
    )

    pending_items: List[PendingItem] = []
    active_items: List[Dict[str, Any]] = []
    carryover_items: List[Dict[str, Any]] = []

    carryover_file = out_dir / "pending_entry_signals_latest.csv"
    if carryover_file.exists():
        try:
            qdf = pd.read_csv(carryover_file)
            if len(qdf) > 0 and "carry_reason" in qdf.columns:
                qdf["carry_reason"] = qdf["carry_reason"].astype(str).str.strip().str.upper()
                qdf = qdf[qdf["carry_reason"].eq("NO_NEXT_DAY")].copy()
                if len(qdf) > 0:
                    if "code" in qdf.columns:
                        qdf["code"] = qdf["code"].apply(_normalize_code)
                    keep_cols = [
                        c
                        for c in ["signal_date", "code", "name", "carry_reason", "captured_at", "final_score"]
                        if c in qdf.columns
                    ]
                    carryover_items = qdf[keep_cols].to_dict(orient="records")
        except Exception as e:
            report["notes"].append(f"carryover_queue_read_failed: {type(e).__name__}: {e}")

    for x in op:
        if not isinstance(x, dict):
            continue
        code = _normalize_code(x.get("code"))
        name = str(x.get("name", "") or "")
        entry_date = str(x.get("entry_date", "") or "").strip()
        try:
            entry_price = float(x.get("entry_price", 0) or 0)
        except Exception:
            entry_price = 0.0
        try:
            stop_loss = float(x.get("stop_loss", -0.05) or -0.05)
        except Exception:
            stop_loss = -0.05

        stop_price = entry_price * (1.0 + stop_loss)

        dd = code_dates.get(code, [])
        last_for_code = dd[-1] if dd else ""
        after = [d for d in dd if d > entry_date] if entry_date else dd

        if not after:
            reason = "SAME_DAY_ENTRY_WAIT_NEXT_SESSION" if entry_date and entry_date == prices_date_max else "NO_PRICES_AFTER_ENTRY_DATE"
            pending_items.append(
                PendingItem(
                    code=code,
                    name=name,
                    entry_date=entry_date,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    stop_price=stop_price,
                    prices_date_max=prices_date_max,
                    last_price_date_for_code=last_for_code,
                    reason=reason,
                )
            )
        else:
            active_items.append(
                {
                    "code": code,
                    "name": name,
                    "entry_date": entry_date,
                    "prices_after_entry_count": len(after),
                    "first_after_entry": after[0],
                    "last_after_entry": after[-1],
                }
            )

    # 출력 구성
    lines.append("============================================================")
    lines.append("[PAPER_PENDING_REPORT]")
    lines.append(f"ts={ts}")
    lines.append(f"STATE_FILE={state_file} EXISTS={state_file.exists()}")
    lines.append(f"PRICES_FILE={prices_file} EXISTS={prices_file.exists()}")
    lines.append(f"prices_date_max={prices_date_max}")
    lines.append(f"open_positions_len={len(op)}")
    lines.append("============================================================")

    # PENDING 섹션
    lines.append("")
    lines.append("[PENDING] 진입 완료 / 평가 대기(다음 거래일 데이터 없음)")
    lines.append(f"pending_count={len(pending_items)}")
    if pending_items:
        lines.append("code name entry_date entry_price stop_loss stop_price last_price_date_for_code reason")
        for it in pending_items:
            # name이 깨져도 그대로 출력
            lines.append(
                f"{it.code} {it.name} {it.entry_date} {it.entry_price} {it.stop_loss} {round(it.stop_price,4)} {it.last_price_date_for_code} {it.reason}"
            )
    else:
        lines.append("(none)")

    # ACTIVE 섹션(참고)
    lines.append("")
    lines.append("[ACTIVE] 평가 가능(진입일 이후 데이터 존재)")
    lines.append(f"active_count={len(active_items)}")
    if active_items:
        lines.append("code name entry_date prices_after_entry_count first_after_entry last_after_entry")
        for a in active_items:
            lines.append(
                f"{a.get('code')} {a.get('name')} {a.get('entry_date')} {a.get('prices_after_entry_count')} {a.get('first_after_entry')} {a.get('last_after_entry')}"
            )
    else:
        lines.append("(none)")

    lines.append("")
    lines.append("[PENDING_QUEUE] NO_NEXT_DAY carryover queue")
    lines.append(f"carryover_queue_count={len(carryover_items)}")
    if carryover_items:
        lines.append("signal_date code name carry_reason captured_at final_score")
        for it in carryover_items:
            lines.append(
                f"{it.get('signal_date', '')} {it.get('code', '')} {it.get('name', '')} "
                f"{it.get('carry_reason', '')} {it.get('captured_at', '')} {it.get('final_score', '')}"
            )
    else:
        lines.append("(none)")

    text = "\n".join(lines) + "\n"
    _log_print(text)

    report["pending"] = [asdict(x) for x in pending_items]
    report["active"] = active_items
    report["carryover_queue"] = carryover_items
    allowed_wait_reasons = {"SAME_DAY_ENTRY_WAIT_NEXT_SESSION"}
    blocking_pending = []
    for item in report["pending"]:
        reason = str(item.get("reason") or "")
        entry_date = str(item.get("entry_date") or "")
        if reason in allowed_wait_reasons:
            continue
        if reason == "NO_PRICES_AFTER_ENTRY_DATE" and entry_date >= eod_prices_date_max:
            continue
        blocking_pending.append(item)
    report["verdict"] = {
        "ok": len(blocking_pending) == 0,
        "pending_total": len(report["pending"]),
        "pending_blocking_count": len(blocking_pending),
        "pending_allowed_wait_count": len(report["pending"]) - len(blocking_pending),
        "allowed_wait_reasons": sorted(allowed_wait_reasons | {"NO_PRICES_AFTER_ENTRY_DATE when entry_date>=eod_prices_date_max"}),
        "eod_prices_date_max": eod_prices_date_max,
        "intraday_prices_date_max": report.get("intraday_prices_date_max"),
        "reason": "ok" if len(blocking_pending) == 0 else "BLOCKING_PENDING_PRICE_GAP",
    }

    out_txt.write_text(text, encoding="utf-8")
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    _log_print(f"[OK] wrote: {out_txt}")
    _log_print(f"[OK] wrote: {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
