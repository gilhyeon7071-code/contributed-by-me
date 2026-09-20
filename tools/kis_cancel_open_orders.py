from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from kis_order_client import KISApiError, KISConfig, KISOrderClient
from order_scoring_engine import (
    OrderScoreConfig,
    OrderScoreFeature,
    decide_action,
    score_order,
)


ROOT = Path(__file__).resolve().parents[1]
PAPER_DIR = ROOT / "paper"
LOG_DIR = ROOT / "2_Logs"
RUNS_DIR = ROOT.parent / "vibe" / "buffett" / "runs"
logger = logging.getLogger("kis_cancel_open_orders")


def _api_error_fields(e: Exception) -> Dict[str, object]:
    return {
        "error_type": type(e).__name__,
        "error_code": str(getattr(e, "code", "") or ""),
        "error_category": str(getattr(e, "category", "") or ""),
        "status_code": getattr(e, "status_code", None),
        "path": str(getattr(e, "path", "") or ""),
        "message": str(e),
    }


def _norm_ymd(v: object) -> str:
    s = str(v or "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8]


def _to_int(v: object, default: int = 0) -> int:
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return int(default)


def _parse_hhmmss(v: object) -> Optional[tuple[int, int, int]]:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    if len(s) >= 6:
        s = s[:6]
    elif len(s) == 4:
        s = f"{s}00"
    else:
        return None
    hh = _to_int(s[:2], -1)
    mm = _to_int(s[2:4], -1)
    ss = _to_int(s[4:6], -1)
    if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59):
        return None
    return hh, mm, ss


def _to_float(v: object, default: float = 0.0) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return float(default)


def _extract_first_float(d: Dict[str, object], keys: List[str], default: float = 0.0) -> float:
    for k in keys:
        if k in d:
            vv = _to_float(d.get(k), default)
            if vv != default:
                return vv
    return float(default)


def _merge_scoring_metrics(path: Path, section_name: str, payload: Dict[str, object]) -> None:
    root: Dict[str, object] = {}
    if path.exists():
        try:
            prev = json.loads(path.read_text(encoding="utf-8-sig"))
            if isinstance(prev, dict):
                root = prev
        except Exception:
            root = {}
    root["generated_at"] = dt.datetime.now().isoformat(timespec="seconds")
    root[section_name] = payload
    try:
        dispatch_ok = bool((root.get("dispatch") or {}).get("new_orders_allowed", True)) if isinstance(root.get("dispatch"), dict) else True
        cancel_ok = bool((root.get("cancel") or {}).get("new_orders_allowed", True)) if isinstance(root.get("cancel"), dict) else True
        root["new_orders_allowed"] = bool(dispatch_ok and cancel_ok)
    except Exception:
        root["new_orders_allowed"] = bool(payload.get("new_orders_allowed", True))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(root, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _order_age_minutes(now_kst: dt.datetime, d: str, ord_tmd: object) -> Optional[float]:
    if str(d) != now_kst.strftime("%Y%m%d"):
        return 10_000.0
    hms = _parse_hhmmss(ord_tmd)
    if hms is None:
        return None
    hh, mm, ss = hms
    ts = now_kst.replace(hour=hh, minute=mm, second=ss, microsecond=0)
    if ts > now_kst:
        return 0.0
    return (now_kst - ts).total_seconds() / 60.0


def _norm_side(v: object) -> str:
    s = str(v or "").strip().upper()
    if s in {"02", "2", "BUY", "B", "매수"}:
        return "BUY"
    if s in {"01", "1", "SELL", "S", "매도"}:
        return "SELL"
    return ""


def _mode_label(args_mock: str, client: KISOrderClient) -> str:
    if args_mock == "true":
        return "mock"
    if args_mock == "false":
        return "prod"
    return "mock" if client.cfg.mock else "prod"


def _run_self_test() -> int:
    cfg = KISConfig(
        app_key="k",
        app_secret="s",
        cano="12345678",
        acnt_prdt_cd="01",
        mock=True,
    )
    client = KISOrderClient(cfg)

    sample_rows = [
        {
            "pdno": "005930",
            "odno": "1111",
            "ord_gno_brno": "00001",
            "ord_qty": "10",
            "tot_ccld_qty": "3",
            "ord_tmd": "091000",
            "sll_buy_dvsn_cd": "02",
        },
        {
            "pdno": "000660",
            "odno": "2222",
            "ord_gno_brno": "00001",
            "ord_qty": "5",
            "tot_ccld_qty": "5",
            "ord_tmd": "091500",
            "sll_buy_dvsn_cd": "01",
        },
    ]

    def _fake_inq(**_: object) -> Dict[str, object]:
        return {"ok": True, "tr_id": "VTTC0081R", "rows": sample_rows, "pages": 1}

    def _fake_auth(**_: object) -> Dict[str, str]:
        return {"tr_id": "VTTC0803U"}

    def _fake_req(*_: object, **__: object):
        return {"rt_cd": "0", "msg1": "OK", "output": {"ODNO": "3333", "KRX_FWDG_ORD_ORGNO": "00001"}}, {}

    client.inquire_daily_ccld = _fake_inq  # type: ignore[assignment]
    client._auth_headers = _fake_auth  # type: ignore[assignment]
    client._request_json = _fake_req  # type: ignore[assignment]

    open_rsp = client.inquire_open_orders(ymd="20260309")
    rows = open_rsp.get("rows", []) or []
    assert len(rows) == 1, f"expected 1 open row, got {len(rows)}"
    assert int(rows[0]["rmn_qty"]) == 7, f"expected rem=7, got {rows[0]['rmn_qty']}"

    c_rsp = client.cancel_order(org_order_no="1111", org_order_branch_no="00001", qty=7, cancel_all=False)
    assert bool(c_rsp.get("ok", False)), "cancel_order should return ok"
    assert c_rsp.get("payload", {}).get("RVSE_CNCL_DVSN_CD") == "02", "cancel payload mismatch"

    now = dt.datetime(2026, 3, 9, 10, 0, 0)
    age = _order_age_minutes(now, "20260309", "093000")
    assert age is not None and age >= 30, f"age calc failed: {age}"

    scfg = OrderScoreConfig()
    sfeat = OrderScoreFeature(
        side="BUY",
        age_min=3.0,
        remain_ratio=0.2,
        imbalance=0.4,
        spread_bps=4.0,
        venue_type="price_time",
    )
    score, pfill, _, _ = score_order(sfeat, scfg)
    action = decide_action(score, pfill, "price_time", scfg)
    assert action in {"HOLD", "CANCEL_REPOST"}, "scoring action should be valid"

    logger.info("[OK] self-test passed")
    return 0


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s %(name)s - %(message)s")

    ap = argparse.ArgumentParser(description="Cancel stale open orders from KIS daily order status")
    ap.add_argument("--date", default="", help="YYYYMMDD (default=today)")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--apply", action="store_true", help="Actually send cancel orders")
    ap.add_argument("--allow-non-today", action="store_true", help="Allow apply for non-today date")
    ap.add_argument("--min-age-minutes", type=int, default=10, help="Cancel only if order age >= this threshold")
    ap.add_argument("--max-cancels", type=int, default=0, help="0 means all")
    ap.add_argument("--sll-buy-dvsn-cd", default="00", help="00=all, 01=sell, 02=buy")
    ap.add_argument("--code", default="", help="Optional 6-digit code filter")
    ap.add_argument("--tr-id", default="", help="Optional override for cancel TR ID")
    ap.add_argument("--score-policy", dest="score_policy", action="store_true", default=True, help="Enable score-based cancel decision")
    ap.add_argument("--no-score-policy", dest="score_policy", action="store_false", help="Disable score-based cancel decision")
    ap.add_argument("--venue-type", default="price_time", choices=["price_time", "retail_pro_rata"])
    ap.add_argument("--fee-bps", type=float, default=2.0)
    ap.add_argument("--slippage-bps", type=float, default=3.0)
    ap.add_argument("--replace-penalty-bps", type=float, default=1.0)
    ap.add_argument("--pfill-cut-price-time", type=float, default=0.03)
    ap.add_argument("--pfill-cut-retail", type=float, default=0.10)
    ap.add_argument("--self-test", action="store_true", help="Run offline logic self-test")
    args = ap.parse_args()

    if args.self_test:
        return _run_self_test()

    today = dt.datetime.now().strftime("%Y%m%d")
    d = _norm_ymd(args.date) or today

    if args.apply and (not args.allow_non_today) and d != today:
        logger.error("[STOP] apply is blocked for non-today D=%s today=%s. Use --allow-non-today if intentional.", d, today)
        return 2

    mock_opt: Optional[bool]
    if args.mock == "auto":
        mock_opt = None
    else:
        mock_opt = args.mock == "true"

    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)

    score_cfg = OrderScoreConfig(
        fee_bps=float(args.fee_bps),
        slippage_bps=float(args.slippage_bps),
        replace_penalty_bps=float(args.replace_penalty_bps),
        pfill_cut_price_time=float(args.pfill_cut_price_time),
        pfill_cut_retail_pro_rata=float(args.pfill_cut_retail),
    )

    try:
        client = KISOrderClient.from_env(mock=mock_opt)
    except Exception as e:
        logger.error("[STOP] KIS env/config failed: %s", e)
        return 2

    mode = _mode_label(args.mock, client)
    out_csv = PAPER_DIR / f"orders_{d}_broker_cancel_{mode}.csv"
    out_json = LOG_DIR / f"kis_cancel_open_orders_{d}_{mode}.json"

    try:
        rsp = client.inquire_open_orders(
            ymd=d,
            sll_buy_dvsn_cd=str(args.sll_buy_dvsn_cd),
            pdno=str(args.code or "").strip(),
            max_pages=30,
        )
        open_rows = rsp.get("rows", []) or []
    except KISApiError as e:
        ef = _api_error_fields(e)
        logger.error(
            "[STOP] KIS API error type=%s code=%s category=%s status=%s path=%s msg=%s",
            ef.get("error_type", ""),
            ef.get("error_code", ""),
            ef.get("error_category", ""),
            ef.get("status_code", ""),
            ef.get("path", ""),
            ef.get("message", ""),
        )
        return 2
    except Exception as e:
        logger.error("[STOP] inquire_open_orders failed: %s", e)
        return 2

    try:
        from zoneinfo import ZoneInfo

        now_kst = dt.datetime.now(ZoneInfo("Asia/Seoul"))
    except Exception:
        now_kst = dt.datetime.utcnow() + dt.timedelta(hours=9)

    records: List[Dict[str, object]] = []

    for r in open_rows:
        rec = {
            "dispatch_ts": dt.datetime.now().isoformat(timespec="seconds"),
            "date": d,
            "code": str(r.get("code", "")).zfill(6),
            "side": _norm_side(r.get("side_raw", "")),
            "org_order_no": str(r.get("odno", "")),
            "org_order_branch_no": str(r.get("ord_gno_brno", "")),
            "ord_qty": _to_int(r.get("ord_qty", 0)),
            "filled_qty": _to_int(r.get("ccld_qty", 0)),
            "remain_qty": _to_int(r.get("rmn_qty", 0)),
            "ord_tmd": str(r.get("ord_tmd", "")),
            "age_min": None,
            "cancel_status": "",
            "ok": False,
            "rt_cd": "",
            "msg1": "",
            "cancel_ord_no": "",
            "cancel_org_no": "",
            "tr_id": "",
            "error": "",
            "error_code": "",
            "error_category": "",
            "error_status_code": "",
            "error_path": "",
            "apply": bool(args.apply),
            "mode": mode,
            "score_policy": bool(args.score_policy),
            "venue_type": str(args.venue_type),
            "order_score": "",
            "p_fill": "",
            "signed_markout": "",
            "estimated_tc": "",
            "score_action": "",
            "imbalance": "",
            "spread_bps": "",
        }

        age_min = _order_age_minutes(now_kst, d, rec["ord_tmd"])
        rec["age_min"] = age_min
        if age_min is not None and float(age_min) < float(args.min_age_minutes):
            rec["cancel_status"] = "SKIP_YOUNG_ORDER"
            records.append(rec)
            continue

        if rec["remain_qty"] <= 0:
            rec["cancel_status"] = "SKIP_NO_REMAIN"
            records.append(rec)
            continue

        if not rec["org_order_no"]:
            rec["cancel_status"] = "SKIP_NO_ORDER_NO"
            records.append(rec)
            continue

        if not rec["org_order_branch_no"]:
            rec["cancel_status"] = "SKIP_NO_BRANCH_NO"
            records.append(rec)
            continue

        if bool(args.score_policy):
            imbalance = 0.0
            spread_bps = 0.0
            try:
                hoga = client.inquire_hoga(str(rec["code"]))
                output = hoga.get("output", {}) if isinstance(hoga, dict) else {}
                ask = _extract_first_float(output, ["askp1", "askp_rsqn1_price", "ask_price1"], 0.0)
                bid = _extract_first_float(output, ["bidp1", "bidp_rsqn1_price", "bid_price1"], 0.0)
                ask_sz = _extract_first_float(output, ["askp_rsqn1", "ask_q1", "ask_qty1"], 0.0)
                bid_sz = _extract_first_float(output, ["bidp_rsqn1", "bid_q1", "bid_qty1"], 0.0)
                if (ask + bid) > 0:
                    mid = (ask + bid) / 2.0
                    if mid > 0 and ask > 0 and bid > 0:
                        spread_bps = max(0.0, ((ask - bid) / mid) * 10000.0)
                den = ask_sz + bid_sz
                if den > 0:
                    imbalance = (bid_sz - ask_sz) / den
            except Exception:
                pass

            ord_qty = max(1, int(rec["ord_qty"]))
            remain_ratio = max(0.0, min(1.0, float(rec["remain_qty"]) / float(ord_qty)))
            feat = OrderScoreFeature(
                side=str(rec["side"]),
                age_min=float(rec["age_min"] or 0.0),
                remain_ratio=remain_ratio,
                imbalance=float(imbalance),
                spread_bps=float(spread_bps),
                venue_type=str(args.venue_type),
            )
            sc, pf, mk, tc = score_order(feat, score_cfg)
            action = decide_action(sc, pf, str(args.venue_type), score_cfg)
            rec["order_score"] = round(float(sc), 8)
            rec["p_fill"] = round(float(pf), 6)
            rec["signed_markout"] = round(float(mk), 8)
            rec["estimated_tc"] = round(float(tc), 8)
            rec["score_action"] = action
            rec["imbalance"] = round(float(imbalance), 6)
            rec["spread_bps"] = round(float(spread_bps), 4)
            if action != "CANCEL_REPOST":
                rec["cancel_status"] = "SKIP_SCORE_HOLD"
                records.append(rec)
                continue

        if not args.apply:
            rec["cancel_status"] = "DRY_RUN"
            rec["ok"] = True
            rec["msg1"] = "dry-run only"
            records.append(rec)
            continue

        try:
            c_rsp = client.cancel_order(
                org_order_no=str(rec["org_order_no"]),
                org_order_branch_no=str(rec["org_order_branch_no"]),
                qty=int(rec["remain_qty"]),
                cancel_all=True,
                tr_id=str(args.tr_id or ""),
            )
            rec["ok"] = bool(c_rsp.get("ok", False))
            rec["rt_cd"] = str(c_rsp.get("rt_cd", ""))
            rec["msg1"] = str(c_rsp.get("msg1", ""))
            rec["cancel_ord_no"] = str(c_rsp.get("ord_no", ""))
            rec["cancel_org_no"] = str(c_rsp.get("org_no", ""))
            rec["tr_id"] = str(c_rsp.get("tr_id", ""))
            rec["cancel_status"] = "CANCEL_ACCEPTED" if rec["ok"] else "CANCEL_REJECTED"
            records.append(rec)
        except KISApiError as e:
            ef = _api_error_fields(e)
            rec["cancel_status"] = "ERROR"
            rec["error"] = str(ef.get("message", ""))
            rec["error_code"] = str(ef.get("error_code", ""))
            rec["error_category"] = str(ef.get("error_category", ""))
            rec["error_status_code"] = str(ef.get("status_code", ""))
            rec["error_path"] = str(ef.get("path", ""))
            records.append(rec)
        except Exception as e:
            rec["cancel_status"] = "ERROR"
            rec["error"] = f"unexpected: {e}"
            records.append(rec)

        if args.max_cancels > 0:
            done_cnt = len([x for x in records if str(x.get("cancel_status", "")).startswith("CANCEL_")])
            if done_cnt >= int(args.max_cancels):
                break

    new_df = pd.DataFrame(records)
    if out_csv.exists():
        try:
            old = pd.read_csv(out_csv, dtype=str)
            out_df = pd.concat([old, new_df], ignore_index=True)
        except Exception:
            out_df = new_df
    else:
        out_df = new_df

    out_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    summary = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "date": d,
        "today": today,
        "apply": bool(args.apply),
        "mode": mode,
        "min_age_minutes": int(args.min_age_minutes),
        "max_cancels": int(args.max_cancels),
        "sll_buy_dvsn_cd": str(args.sll_buy_dvsn_cd),
        "code": str(args.code or ""),
        "rows_open": int(len(open_rows)),
        "rows_new": int(len(new_df)),
        "counts": new_df.get("cancel_status", pd.Series(dtype=str)).value_counts(dropna=False).to_dict(),
        "score_policy": bool(args.score_policy),
        "venue_type": str(args.venue_type),
        "paths": {
            "cancel_log_csv": str(out_csv),
            "summary_json": str(out_json),
        },
    }
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    score_series = pd.to_numeric(new_df.get("order_score", pd.Series(dtype=float)), errors="coerce")
    reject_mask = new_df.get("cancel_status", pd.Series(dtype=str)).astype(str).isin(["CANCEL_REJECTED", "ERROR"])
    metrics = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "date": d,
        "score_policy": bool(args.score_policy),
        "venue_type": str(args.venue_type),
        "rows": int(len(new_df)),
        "cancel_rate": float((new_df.get("cancel_status", pd.Series(dtype=str)).astype(str).str.startswith("CANCEL_")).mean()) if len(new_df) else 0.0,
        "reject_rate": float(reject_mask.mean()) if len(new_df) else 0.0,
        "avg_score": float(score_series.mean()) if len(score_series.dropna()) else None,
        "avg_p_fill": float(pd.to_numeric(new_df.get("p_fill", pd.Series(dtype=float)), errors="coerce").mean()) if len(new_df) else None,
        "new_orders_allowed": bool((not args.apply) or (int(reject_mask.sum()) == 0)),
        "counts": summary["counts"],
    }
    metrics_path = RUNS_DIR / f"order_scoring_metrics_{d}.json"
    metrics_latest = RUNS_DIR / "order_scoring_metrics_latest.json"
    _merge_scoring_metrics(metrics_path, "cancel", metrics)
    _merge_scoring_metrics(metrics_latest, "cancel", metrics)

    reject_rows = new_df[reject_mask].copy()
    reject_pattern = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "date": d,
        "source": "cancel",
        "total_rejects": int(len(reject_rows)),
        "by_status": reject_rows.get("cancel_status", pd.Series(dtype=str)).value_counts(dropna=False).to_dict(),
        "by_error_code": reject_rows.get("error_code", pd.Series(dtype=str)).astype(str).value_counts(dropna=False).to_dict() if len(reject_rows) else {},
        "by_error_category": reject_rows.get("error_category", pd.Series(dtype=str)).astype(str).value_counts(dropna=False).to_dict() if len(reject_rows) else {},
        "avg_age_min": float(pd.to_numeric(reject_rows.get("age_min", pd.Series(dtype=float)), errors="coerce").mean()) if len(reject_rows) else None,
    }
    reject_path = LOG_DIR / "fix_reject_pattern_last.json"
    reject_path.write_text(json.dumps(reject_pattern, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("[OK] cancel_log=%s rows_new=%s", out_csv, len(new_df))
    logger.info("[OK] summary=%s", out_json)
    logger.info("[OK] scoring_metrics=%s", metrics_path)
    logger.info("[OK] reject_pattern=%s", reject_path)

    if args.apply and len(new_df):
        fail_n = int(new_df["cancel_status"].isin(["CANCEL_REJECTED", "ERROR"]).sum())
        if fail_n > 0:
            logger.error("[STOP] apply mode had failed cancels: %s", fail_n)
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

