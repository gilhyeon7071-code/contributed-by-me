from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

TOOLS_DIR = Path(__file__).resolve().parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

from kis_order_client import KISApiError, KISOrderClient

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
PAPER_DIR = ROOT / "paper"
KST = dt.timezone(dt.timedelta(hours=9))

def _now_kst() -> dt.datetime:
    return dt.datetime.now(KST)


def _norm_ymd(v: object) -> str:
    digits = "".join(ch for ch in str(v or "") if ch.isdigit())
    return digits[:8]


def _to_int(v: object, default: int = 0) -> int:
    try:
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return int(default)


def _to_float(v: object, default: float = 0.0) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return float(default)


def _norm_side(v: object) -> str:
    s = str(v or "").strip().upper()
    if s in {"02", "2", "BUY", "B", "매수"}:
        return "BUY"
    if s in {"01", "1", "SELL", "S", "매도"}:
        return "SELL"
    return ""


def _pick(raw: Dict[str, Any], keys: List[str], default: object = "") -> object:
    for key in keys:
        if key in raw and raw.get(key) not in (None, ""):
            return raw.get(key)
    return default


def _load_code_names() -> Dict[str, str]:
    path = LOG_DIR / "code_name_cache_latest.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(data, dict):
            return {str(k).zfill(6): str(v) for k, v in data.items() if str(v).strip()}
    except Exception:
        pass
    return {}


def _mode_label(args_mock: str, client: KISOrderClient) -> str:
    if args_mock == "true":
        return "mock"
    if args_mock == "false":
        return "prod"
    return "mock" if client.cfg.mock else "prod"


def _api_error_payload(e: Exception) -> Dict[str, object]:
    return {
        "error_type": type(e).__name__,
        "error_code": str(getattr(e, "code", "") or ""),
        "error_category": str(getattr(e, "category", "") or ""),
        "status_code": getattr(e, "status_code", None),
        "path": str(getattr(e, "path", "") or ""),
        "message": str(e),
    }


def _snapshot_rows(rows: List[Dict[str, Any]], *, ymd: str, name_map: Dict[str, str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        code = str(row.get("code") or _pick(raw, ["pdno", "PDNO", "code"], "")).strip().zfill(6)
        side = _norm_side(row.get("side_raw") or _pick(raw, ["sll_buy_dvsn_cd", "SLL_BUY_DVSN_CD", "side"], ""))
        order_price = _to_float(_pick(raw, ["ord_unpr", "ORD_UNPR", "order_price", "price"], 0), 0.0)
        order_time = str(row.get("ord_tmd") or _pick(raw, ["ord_tmd", "ORD_TMD", "order_time"], ""))
        out.append(
            {
                "time": f"{ymd}T{order_time}" if order_time else ymd,
                "exec_date": ymd,
                "code": code,
                "name": name_map.get(code, ""),
                "side": side,
                "qty": int(row.get("rmn_qty") or 0),
                "price": order_price,
                "status": "미체결",
                "order_no": str(row.get("odno", "")),
                "branch_no": str(row.get("ord_gno_brno", "")),
                "ord_qty": int(row.get("ord_qty") or 0),
                "filled_qty": int(row.get("ccld_qty") or 0),
                "remaining_qty": int(row.get("rmn_qty") or 0),
            }
        )
    return out


def build_snapshot(*, ymd: str, mock_arg: str, max_pages: int) -> Dict[str, Any]:
    generated_at = _now_kst().isoformat(timespec="seconds")
    mock_opt: Optional[bool]
    if mock_arg == "auto":
        mock_opt = None
    else:
        mock_opt = mock_arg == "true"

    payload: Dict[str, Any] = {
        "generated_at": generated_at,
        "date": ymd,
        "today": _now_kst().strftime("%Y%m%d"),
        "mode": mock_arg,
        "status": "UNKNOWN",
        "ok": False,
        "rows_open": 0,
        "rows_raw": 0,
        "pages": 0,
        "rows": [],
        "error": "",
        "read_only": True,
        "no_order_effect": True,
        "execution_allowed": False,
    }

    try:
        client = KISOrderClient.from_env(mock=mock_opt)
        payload["mode"] = _mode_label(mock_arg, client)
        rsp = client.inquire_open_orders(ymd=ymd, max_pages=max_pages)
        open_rows = rsp.get("rows", []) or []
        raw_rows = rsp.get("rows_raw", []) or []
        rows = _snapshot_rows(open_rows, ymd=ymd, name_map=_load_code_names())
        payload.update(
            {
                "status": "PASS",
                "ok": True,
                "tr_id": rsp.get("tr_id", ""),
                "rows_open": len(rows),
                "rows_raw": len(raw_rows),
                "pages": int(rsp.get("pages", 0) or 0),
                "rows": rows,
            }
        )
    except KISApiError as e:
        payload["status"] = "ERROR"
        payload["error"] = str(e)
        payload["api_error"] = _api_error_payload(e)
    except Exception as e:
        payload["status"] = "ERROR"
        payload["error"] = str(e)
    return payload


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Read-only KIS open-order snapshot")
    ap.add_argument("--date", default="", help="YYYYMMDD, default today KST")
    ap.add_argument("--mock", default="auto", choices=["auto", "true", "false"])
    ap.add_argument("--max-pages", type=int, default=30)
    ap.add_argument("--self-test", action="store_true")
    return ap.parse_args()


def _self_test() -> int:
    rows = _snapshot_rows(
        [
            {
                "code": "005930",
                "side_raw": "02",
                "odno": "1234",
                "ord_gno_brno": "00001",
                "ord_qty": 10,
                "ccld_qty": 3,
                "rmn_qty": 7,
                "ord_tmd": "091500",
                "raw": {"ord_unpr": "70000"},
            }
        ],
        ymd="20260715",
        name_map={"005930": "삼성전자"},
    )
    assert rows[0]["qty"] == 7
    assert rows[0]["status"] == "미체결"
    assert rows[0]["name"] == "삼성전자"
    print(json.dumps({"ok": True, "rows": rows}, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    args = _parse_args()
    if args.self_test:
        return _self_test()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PAPER_DIR.mkdir(parents=True, exist_ok=True)
    ymd = _norm_ymd(args.date) or _now_kst().strftime("%Y%m%d")
    payload = build_snapshot(ymd=ymd, mock_arg=str(args.mock), max_pages=int(args.max_pages))

    mode = str(payload.get("mode") or args.mock)
    dated = LOG_DIR / f"kis_open_orders_{ymd}_{mode}.json"
    latest = LOG_DIR / "kis_open_orders_latest.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    dated.write_text(text, encoding="utf-8-sig")
    latest.write_text(text, encoding="utf-8-sig")
    print(text)
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())