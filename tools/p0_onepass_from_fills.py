# -*- coding: utf-8 -*-
r"""
p0_onepass_from_fills.py

Purpose
- SSOT: fills.csv 기준으로 D(as_of) 1회 관통 산출물 생성
- outputs:
  - RootA: E:\1_Data\paper\orders_{D}_exec.xlsx   (fills 기반, STOP 포함; entry_blocked 표식 포함)
  - RootA: E:\1_Data\2_Logs\p0_live_vs_bt_core_{D}.json
  - RootA: E:\1_Data\2_Logs\p0_stop_report_{D}.json

Patch (2026-02-13)
- Strategy risk mitigation (Plan-only):
  - For BUY & non-stop rows, apply CAP per signal_date using candidates score (top N).
  - Mark blocked rows: entry_blocked=True, entry_block_reason="CAP_SIGNALDATE_TOP{N}_BY_SCORE"
  - Exclude blocked rows from core calculations.

Notes
- This does NOT block live broker execution. It only marks/excludes rows in this onepass pipeline (Plan-only).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


ROOT = Path(r"E:\1_Data")
FILLS = ROOT / "paper" / "fills.csv"
OUT_ORDERS_DIR = ROOT / "paper"
OUT_LOGS_DIR = ROOT / "2_Logs"


def _ymd_from_datetime(s: str) -> str:
    s = str(s)
    return s[:8]


def _z6(x: str) -> str:
    return str(x).zfill(6)


def _parse_signal_date(note: str) -> Optional[str]:
    m = re.search(r"signal_date=(\d{8})", str(note))
    return m.group(1) if m else None


def _is_stop_row(side_u: str, note: str) -> bool:
    # 현재 관측: SELL rows에 exit_reason=STOP / STOP_GAP
    side_u = str(side_u).upper().strip()
    note = str(note)
    if side_u != "SELL":
        return False
    return ("STOP" in note) or ("STOP_GAP" in note)


def _pick_candidates_path_for_signal_date(signal_date: str) -> Optional[Path]:
    r"""
    Prefer v41_1 daily snapshot if exists:
      E:\1_Data\2_Logs\candidates_v41_1_YYYYMMDD.csv
    """
    if not signal_date or not re.fullmatch(r"\d{8}", str(signal_date)):
        return None
    p = OUT_LOGS_DIR / f"candidates_v41_1_{signal_date}.csv"
    return p if p.exists() else None


def _load_top_codes_by_score(signal_date: str, top_n: int = 3) -> Tuple[Optional[set], str]:
    """
    Returns (top_codes_set, debug_msg).
    - top_codes are code6 strings
    - tie-breaker: value(desc) if present
    """
    p = _pick_candidates_path_for_signal_date(signal_date)
    if p is None:
        return None, f"candidates_missing_for_signal_date={signal_date}"

    try:
        c = pd.read_csv(p, dtype=str)
    except Exception as e:
        return None, f"candidates_read_error={p}: {e}"

    if "code" not in c.columns or "score" not in c.columns:
        return None, f"candidates_missing_cols(code/score) path={p}"

    c["code6"] = c["code"].astype(str).str.zfill(6)
    c["score_num"] = pd.to_numeric(c["score"], errors="coerce")

    sort_cols = ["score_num"]
    ascending = [False]
    if "value" in c.columns:
        c["value_num"] = pd.to_numeric(c["value"], errors="coerce")
        sort_cols.append("value_num")
        ascending.append(False)

    c = c.sort_values(sort_cols, ascending=ascending)
    top = c["code6"].dropna().astype(str).head(int(top_n)).tolist()
    return set(top), f"candidates_ok path={p} top_n={top_n} top_codes={top}"


def _detect_D_by_rule(df: pd.DataFrame) -> str:
    """
    D 규칙(사용자 규칙):
    ① 최신 BUY ymd
    ② BUY 없으면 최신 ymd(datetime 앞 8자리)
    """
    df = df.copy()
    df["ymd"] = df["datetime"].apply(_ymd_from_datetime)
    side_u = df["side"].astype(str).str.upper().str.strip()
    buy = df[side_u == "BUY"]
    if len(buy) > 0:
        return str(buy["ymd"].max())
    return str(df["ymd"].max())


def main(argv=None) -> int:
    # ---- load fills ----
    if not FILLS.exists():
        print(f"STOP fills_missing={FILLS}")
        return 2

    OUT_ORDERS_DIR.mkdir(parents=True, exist_ok=True)
    OUT_LOGS_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(FILLS, dtype=str)
    need = ["datetime", "code", "side", "qty", "price"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        print("STOP missing_cols=", missing)
        print("[INFO] cols=", df.columns.tolist())
        return 2

    D = _detect_D_by_rule(df)

    df["ymd"] = df["datetime"].apply(_ymd_from_datetime)
    asof = df[df["ymd"] == D].copy()

    if len(asof) == 0:
        outj = {
            "status": "NA",
            "as_of": D,
            "summary": {
                "source_fills": str(FILLS.name),
                "fills_rows_as_of": 0,
                "as_of": D,
                "exec_date": D,
            },
            "notes": ["as_of 필터 적용 후 유효행 0 → NA"],
        }
        outp = OUT_LOGS_DIR / f"p0_live_vs_bt_core_{D}.json"
        outp.write_text(json.dumps(outj, ensure_ascii=False, indent=2), encoding="utf-8")
        print("WROTE", outp, "status=NA")
        return 0

    # ---- normalize ----
    asof["code6"] = asof["code"].apply(_z6)
    asof["side_u"] = asof["side"].astype(str).str.upper().str.strip()
    asof["fill_qty"] = pd.to_numeric(asof["qty"], errors="coerce")
    asof["fill_price"] = pd.to_numeric(asof["price"], errors="coerce")
    asof["note"] = asof.get("note", "").astype(str)
    asof["signal_date"] = asof["note"].apply(_parse_signal_date)
    asof["is_stop"] = asof.apply(lambda r: _is_stop_row(r["side_u"], r["note"]), axis=1)

    # ---- write orders_exec (fills-based) ----
    out_orders = OUT_ORDERS_DIR / f"orders_{D}_exec.xlsx"
    out_df = pd.DataFrame(
        {
            "exec_date": D,
            "side": asof["side_u"],
            "code": asof["code6"],
            "fill_qty": asof["fill_qty"],
            "fill_price": asof["fill_price"],
            "signal_date": asof["signal_date"],
            "is_stop": asof["is_stop"],
            "note": asof["note"],
            "reason": "",
            "entry_blocked": False,
            "entry_block_reason": "",
        }
    )

    # ---- CAP policy (Plan-only marking) ----
    # Apply to BUY & non-stop rows only.
    CAP_TOP_N = 3
    cap_notes = []
    try:
        buy_mask = (out_df["side"].astype(str).str.upper().str.strip() == "BUY")
        nonstop_mask = (out_df["is_stop"] == False)
        entry_mask = buy_mask & nonstop_mask

        # group by signal_date
        for sd, idx in out_df[entry_mask].groupby(out_df.loc[entry_mask, "signal_date"]).groups.items():
            sd_str = str(sd) if sd is not None else ""
            if not re.fullmatch(r"\d{8}", sd_str):
                # signal_date가 없으면 캡 적용 불가(표식 없음)
                cap_notes.append(f"cap_skip_invalid_signal_date={sd}")
                continue

            top_set, msg = _load_top_codes_by_score(sd_str, top_n=CAP_TOP_N)
            cap_notes.append(msg)
            if top_set is None:
                # candidates 없으면 캡 적용 불가(표식 없음)
                continue

            # allow only top_set, block others
            codes = out_df.loc[idx, "code"].astype(str)
            blocked_idx = [i for i, c in zip(idx, codes) if c not in top_set]
            if blocked_idx:
                out_df.loc[blocked_idx, "entry_blocked"] = True
                out_df.loc[blocked_idx, "entry_block_reason"] = f"CAP_SIGNALDATE_TOP{CAP_TOP_N}_BY_SCORE"
    except Exception as e:
        cap_notes.append(f"cap_exception={e}")

    # write file
    out_df.to_excel(out_orders, index=False)
    print("WROTE", out_orders)

    # ---- STOP report ----
    stop_df = out_df[out_df["is_stop"] == True].copy()
    stopj = {
        "as_of": D,
        "stop_rows": int(len(stop_df)),
        "by_side": stop_df["side"].value_counts(dropna=False).to_dict(),
        "by_code": stop_df["code"].value_counts(dropna=False).to_dict(),
    }
    out_stop = OUT_LOGS_DIR / f"p0_stop_report_{D}.json"
    out_stop.write_text(json.dumps(stopj, ensure_ascii=False, indent=2), encoding="utf-8")
    print("WROTE", out_stop, "stop_rows=", stopj["stop_rows"])

    # ---- core = STOP 제외 + entry_blocked 제외 ----
    core = out_df[(out_df["is_stop"] == False) & (out_df["entry_blocked"] == False)].copy()
    core_rows = int(len(core))

    # Minimal core json (keep compatible shape)
    outj = {
        "status": "PASS",
        "as_of": D,
        "summary": {
            "source_fills": str(FILLS.name),
            "fills_rows_as_of": int(len(asof)),
            "exec_date": D,
            "core_rows": core_rows,
            "cap_top_n": CAP_TOP_N,
        },
        "cap": {
            "notes": cap_notes[:50],  # avoid huge
            "blocked_rows": int(out_df["entry_blocked"].sum()),
        },
    }
    outp = OUT_LOGS_DIR / f"p0_live_vs_bt_core_{D}.json"
    outp.write_text(json.dumps(outj, ensure_ascii=False, indent=2), encoding="utf-8")
    print("WROTE", outp, "status=PASS", "core_rows=", core_rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
