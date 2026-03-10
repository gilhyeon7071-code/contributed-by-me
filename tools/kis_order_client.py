from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests


PROD_BASE_URL = "https://openapi.koreainvestment.com:9443"
MOCK_BASE_URL = "https://openapivts.koreainvestment.com:29443"
ROOT = Path(__file__).resolve().parents[1]

class KISApiError(RuntimeError):
    """Raised when KIS API call fails."""


def _split_account_no(account_no: str) -> Tuple[str, str]:
    s = str(account_no or "").strip()
    if not s:
        raise ValueError("KIS_ACCOUNT_NO is required (ex: 12345678-01)")

    if "-" in s:
        left, right = s.split("-", 1)
    else:
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) == 8:
            # Accept plain 8-digit account and default product code to 01.
            left, right = digits, "01"
        elif len(digits) >= 10:
            left, right = digits[:8], digits[8:10]
        else:
            raise ValueError("KIS_ACCOUNT_NO must include 8-digit account + 2-digit product code")

    left = left.strip()
    right = right.strip()
    if len(left) != 8 or len(right) != 2 or (not left.isdigit()) or (not right.isdigit()):
        raise ValueError("Invalid account format. Use 12345678-01")
    return left, right


def _read_secret_direct_or_file(value: str, file_path: str) -> str:
    v = str(value or "").strip()
    if v:
        return v
    fp = str(file_path or "").strip()
    if not fp:
        return ""
    p = Path(fp)
    if not p.exists():
        raise ValueError(f"secret file not found: {p}")
    txt = p.read_text(encoding="utf-8-sig").strip()
    return txt


def _resolve_secrets_dir() -> Path:
    env_dir = str(os.getenv("KIS_SECRETS_DIR", "")).strip()
    if env_dir:
        return Path(env_dir).expanduser()
    return ROOT / ".secrets"


def _resolve_secret_file_path(env_file_path: str, default_name: str) -> str:
    fp = str(env_file_path or "").strip()
    if fp:
        return fp
    p = _resolve_secrets_dir() / str(default_name)
    return str(p) if p.exists() else ""


@dataclass
class KISConfig:
    app_key: str
    app_secret: str
    cano: str
    acnt_prdt_cd: str
    mock: bool = False
    timeout_sec: float = 10.0

    @property
    def base_url(self) -> str:
        return MOCK_BASE_URL if self.mock else PROD_BASE_URL


class KISOrderClient:
    """Minimal KIS REST client for domestic stock trading APIs."""

    def __init__(self, cfg: KISConfig):
        self.cfg = cfg
        self._token = ""
        self._token_expire_at = 0.0
        self._session = requests.Session()

    @classmethod
    def from_env(cls, mock: Optional[bool] = None) -> "KISOrderClient":
        app_key_file = _resolve_secret_file_path(os.getenv("KIS_APP_KEY_FILE", ""), "kis_app_key.txt")
        app_secret_file = _resolve_secret_file_path(os.getenv("KIS_APP_SECRET_FILE", ""), "kis_app_secret.txt")
        account_no_file = _resolve_secret_file_path(os.getenv("KIS_ACCOUNT_NO_FILE", ""), "kis_account_no.txt")

        app_key = _read_secret_direct_or_file(
            os.getenv("KIS_APP_KEY", ""),
            app_key_file,
        )
        app_secret = _read_secret_direct_or_file(
            os.getenv("KIS_APP_SECRET", ""),
            app_secret_file,
        )
        account_no = _read_secret_direct_or_file(
            os.getenv("KIS_ACCOUNT_NO", ""),
            account_no_file,
        )
        if not app_key or not app_secret or not account_no:
            raise ValueError(
                "KIS_APP_KEY(or KIS_APP_KEY_FILE), KIS_APP_SECRET(or KIS_APP_SECRET_FILE), "
                "KIS_ACCOUNT_NO(or KIS_ACCOUNT_NO_FILE) are required"
            )

        env_mock = str(os.getenv("KIS_MOCK", "0")).strip().lower() in {"1", "true", "y", "yes"}
        cano, acnt_prdt_cd = _split_account_no(account_no)
        cfg = KISConfig(
            app_key=app_key,
            app_secret=app_secret,
            cano=cano,
            acnt_prdt_cd=acnt_prdt_cd,
            mock=env_mock if mock is None else bool(mock),
        )
        return cls(cfg)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        headers: Dict[str, str],
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        url = f"{self.cfg.base_url}{path}"
        resp = self._session.request(
            method=method.upper(),
            url=url,
            headers=headers,
            json=payload,
            params=params,
            timeout=self.cfg.timeout_sec,
        )
        try:
            body = resp.json()
        except Exception:
            body = {"raw_text": resp.text}

        if resp.status_code >= 400:
            raise KISApiError(f"HTTP {resp.status_code} on {path}: {body}")

        hdrs = {str(k).lower(): str(v) for k, v in resp.headers.items()}
        return body, hdrs

    def _token_cache_path(self) -> Path:
        p = str(os.getenv("KIS_TOKEN_CACHE_FILE", "")).strip()
        if p:
            return Path(p).expanduser()
        suffix = "mock" if self.cfg.mock else "prod"
        return ROOT / "2_Logs" / f"kis_token_cache_{suffix}.json"

    def _app_key_hash(self) -> str:
        return hashlib.sha256(self.cfg.app_key.encode("utf-8")).hexdigest()

    def _load_cached_token(self) -> bool:
        p = self._token_cache_path()
        if not p.exists():
            return False
        try:
            obj = json.loads(p.read_text(encoding="utf-8-sig"))
            token = str(obj.get("access_token", "")).strip()
            expires_at = float(obj.get("expires_at", 0) or 0)
            base_url = str(obj.get("base_url", "")).strip()
            app_key_hash = str(obj.get("app_key_hash", "")).strip()
            if (not token) or (expires_at <= (time.time() + 30)):
                return False
            if base_url and base_url != self.cfg.base_url:
                return False
            if app_key_hash and app_key_hash != self._app_key_hash():
                return False
            self._token = token
            self._token_expire_at = expires_at
            return True
        except Exception:
            return False

    def _save_cached_token(self) -> None:
        p = self._token_cache_path()
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            obj = {
                "saved_at": int(time.time()),
                "base_url": self.cfg.base_url,
                "app_key_hash": self._app_key_hash(),
                "access_token": self._token,
                "expires_at": float(self._token_expire_at),
            }
            p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            # Cache failure should never block trading logic.
            pass

    def _issue_token(self) -> str:
        if self._load_cached_token():
            return self._token

        payload = {
            "grant_type": "client_credentials",
            "appkey": self.cfg.app_key,
            "appsecret": self.cfg.app_secret,
        }
        headers = {"content-type": "application/json"}
        body, _ = self._request_json("POST", "/oauth2/tokenP", headers=headers, payload=payload)
        token = str(body.get("access_token", "")).strip()
        if not token:
            raise KISApiError(f"Token response missing access_token: {body}")

        expires_in = int(body.get("expires_in", 60 * 60))
        self._token = token
        self._token_expire_at = time.time() + max(60, expires_in - 60)
        self._save_cached_token()
        return token

    def _ensure_token(self) -> str:
        if self._token and time.time() < self._token_expire_at:
            return self._token
        if self._load_cached_token():
            return self._token
        return self._issue_token()

    def issue_ws_approval_key(self) -> str:
        """Issue websocket approval key for real-time subscription."""
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.cfg.app_key,
            "secretkey": self.cfg.app_secret,
        }
        headers = {"content-type": "application/json"}
        body, _ = self._request_json("POST", "/oauth2/Approval", headers=headers, payload=payload)
        key = str(body.get("approval_key", "")).strip()
        if not key:
            raise KISApiError(f"Approval response missing approval_key: {body}")
        return key

    def _hashkey(self, payload: Dict[str, Any]) -> str:
        headers = {
            "content-type": "application/json",
            "appkey": self.cfg.app_key,
            "appsecret": self.cfg.app_secret,
        }
        body, _ = self._request_json("POST", "/uapi/hashkey", headers=headers, payload=payload)
        key = str(body.get("HASH", "")).strip()
        if not key:
            raise KISApiError(f"Hashkey response missing HASH: {body}")
        return key

    def _auth_headers(
        self,
        *,
        tr_id: str,
        payload_for_hash: Optional[Dict[str, Any]] = None,
        tr_cont: str = "",
    ) -> Dict[str, str]:
        token = self._ensure_token()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.cfg.app_key,
            "appsecret": self.cfg.app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }
        if tr_cont:
            headers["tr_cont"] = str(tr_cont)
        if payload_for_hash is not None:
            headers["hashkey"] = self._hashkey(payload_for_hash)
        return headers

    def place_order_cash(
        self,
        *,
        side: str,
        code: str,
        qty: int,
        order_type: str = "market",
        price: int = 0,
        exchange: str = "KRX",
    ) -> Dict[str, Any]:
        side_u = str(side).strip().upper()
        if side_u not in {"BUY", "SELL"}:
            raise ValueError(f"Unsupported side: {side}")
        if int(qty) <= 0:
            raise ValueError(f"qty must be > 0: {qty}")

        code_s = str(code).strip().zfill(6)
        if not code_s.isdigit():
            raise ValueError(f"Invalid stock code: {code}")

        order_type_l = str(order_type).strip().lower()
        if order_type_l == "market":
            ord_dvsn = "01"
            ord_unpr = "0"
        elif order_type_l == "limit":
            if int(price) <= 0:
                raise ValueError("limit order requires price > 0")
            ord_dvsn = "00"
            ord_unpr = str(int(price))
        else:
            raise ValueError(f"Unsupported order_type: {order_type}")

        if self.cfg.mock:
            tr_id = "VTTC0012U" if side_u == "BUY" else "VTTC0011U"
        else:
            tr_id = "TTTC0012U" if side_u == "BUY" else "TTTC0011U"

        payload = {
            "CANO": self.cfg.cano,
            "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
            "PDNO": code_s,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": ord_unpr,
            "EXCG_ID_DVSN_CD": str(exchange).strip().upper() or "KRX",
            "SLL_TYPE": "01" if side_u == "SELL" else "",
            "CNDT_PRIC": "",
        }

        headers = self._auth_headers(tr_id=tr_id, payload_for_hash=payload)
        body, _ = self._request_json(
            "POST",
            "/uapi/domestic-stock/v1/trading/order-cash",
            headers=headers,
            payload=payload,
        )

        rt_cd = str(body.get("rt_cd", ""))
        msg1 = str(body.get("msg1", ""))
        output = body.get("output", {}) or {}
        ord_no = str(output.get("ODNO", "") or output.get("odno", "")).strip()
        org_no = str(output.get("KRX_FWDG_ORD_ORGNO", "") or output.get("krx_fwdg_ord_orgno", "")).strip()

        return {
            "ok": rt_cd == "0",
            "rt_cd": rt_cd,
            "msg1": msg1,
            "ord_no": ord_no,
            "org_no": org_no,
            "tr_id": tr_id,
            "payload": payload,
            "raw": body,
        }

    def inquire_psbl_order(
        self,
        *,
        code: str,
        order_price: int,
        ord_dvsn: str = "01",
        cma_evlu_amt_icld_yn: str = "N",
        ovrs_icld_yn: str = "N",
    ) -> Dict[str, Any]:
        code_s = str(code).strip().zfill(6)
        if not code_s.isdigit():
            raise ValueError(f"Invalid stock code: {code}")

        tr_id = "VTTC8908R" if self.cfg.mock else "TTTC8908R"
        params = {
            "CANO": self.cfg.cano,
            "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
            "PDNO": code_s,
            "ORD_UNPR": str(max(0, int(order_price))),
            "ORD_DVSN": str(ord_dvsn),
            "CMA_EVLU_AMT_ICLD_YN": str(cma_evlu_amt_icld_yn),
            "OVRS_ICLD_YN": str(ovrs_icld_yn),
        }

        headers = self._auth_headers(tr_id=tr_id)
        body, _ = self._request_json(
            "GET",
            "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            headers=headers,
            params=params,
        )

        rt_cd = str(body.get("rt_cd", ""))
        msg1 = str(body.get("msg1", ""))
        if rt_cd != "0":
            raise KISApiError(f"inquire_psbl_order rejected rt_cd={rt_cd} msg1={msg1} body={body}")

        output = body.get("output", {}) or {}
        return {
            "ok": True,
            "tr_id": tr_id,
            "rt_cd": rt_cd,
            "msg1": msg1,
            "output": output,
            "raw": body,
        }

    def inquire_balance_positions(
        self,
        *,
        afhr_flpr_yn: str = "N",
        ofl_yn: str = "",
        inqr_dvsn: str = "01",
        unpr_dvsn: str = "01",
        fund_sttl_icld_yn: str = "N",
        fncg_amt_auto_rdpt_yn: str = "N",
        prcs_dvsn: str = "00",
        max_pages: int = 30,
    ) -> Dict[str, Any]:
        tr_id = "VTTC8434R" if self.cfg.mock else "TTTC8434R"

        pages = 0
        tr_cont = ""
        fk100 = ""
        nk100 = ""
        rows: list[Dict[str, Any]] = []
        output2: list[Dict[str, Any]] = []

        while True:
            pages += 1
            if pages > int(max_pages):
                break

            params = {
                "CANO": self.cfg.cano,
                "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
                "AFHR_FLPR_YN": str(afhr_flpr_yn),
                "OFL_YN": str(ofl_yn),
                "INQR_DVSN": str(inqr_dvsn),
                "UNPR_DVSN": str(unpr_dvsn),
                "FUND_STTL_ICLD_YN": str(fund_sttl_icld_yn),
                "FNCG_AMT_AUTO_RDPT_YN": str(fncg_amt_auto_rdpt_yn),
                "PRCS_DVSN": str(prcs_dvsn),
                "CTX_AREA_FK100": str(fk100),
                "CTX_AREA_NK100": str(nk100),
            }

            headers = self._auth_headers(tr_id=tr_id, tr_cont=tr_cont)
            body, hdrs = self._request_json(
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-balance",
                headers=headers,
                params=params,
            )

            rt_cd = str(body.get("rt_cd", ""))
            msg1 = str(body.get("msg1", ""))
            if rt_cd != "0":
                raise KISApiError(f"inquire_balance rejected rt_cd={rt_cd} msg1={msg1} body={body}")

            out1 = body.get("output1", []) or []
            out2 = body.get("output2", []) or []
            if isinstance(out1, dict):
                out1 = [out1]
            if isinstance(out2, dict):
                out2 = [out2]
            rows.extend(out1)
            output2.extend(out2)

            fk100 = str(body.get("ctx_area_fk100", "") or "")
            nk100 = str(body.get("ctx_area_nk100", "") or "")
            tr_cont_resp = str(hdrs.get("tr_cont", "") or "").upper()
            if tr_cont_resp not in {"M", "F"}:
                break

            tr_cont = "N"
            if not fk100 and not nk100:
                break

        return {
            "ok": True,
            "tr_id": tr_id,
            "rows": rows,
            "summary_rows": output2,
            "pages": pages,
        }

    def inquire_daily_ccld(
        self,
        *,
        start_ymd: str,
        end_ymd: str,
        pd_dv: str = "inner",
        sll_buy_dvsn_cd: str = "00",
        ccld_dvsn: str = "01",
        inqr_dvsn: str = "00",
        inqr_dvsn_3: str = "00",
        pdno: str = "",
        excg_id_dvsn_cd: str = "KRX",
        max_pages: int = 30,
    ) -> Dict[str, Any]:
        pd_dv_s = str(pd_dv).strip().lower()
        if pd_dv_s not in {"before", "inner"}:
            raise ValueError("pd_dv must be before or inner")

        if self.cfg.mock:
            tr_id = "VTSC9215R" if pd_dv_s == "before" else "VTTC0081R"
        else:
            tr_id = "CTSC9215R" if pd_dv_s == "before" else "TTTC0081R"

        all_rows: list[Dict[str, Any]] = []
        pages = 0
        fk100 = ""
        nk100 = ""
        tr_cont = ""

        while True:
            pages += 1
            if pages > int(max_pages):
                break

            params: Dict[str, Any] = {
                "CANO": self.cfg.cano,
                "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
                "INQR_STRT_DT": str(start_ymd),
                "INQR_END_DT": str(end_ymd),
                "SLL_BUY_DVSN_CD": str(sll_buy_dvsn_cd),
                "PDNO": str(pdno),
                "CCLD_DVSN": str(ccld_dvsn),
                "INQR_DVSN": str(inqr_dvsn),
                "INQR_DVSN_3": str(inqr_dvsn_3),
                "ORD_GNO_BRNO": "",
                "ODNO": "",
                "INQR_DVSN_1": "",
                "CTX_AREA_FK100": str(fk100),
                "CTX_AREA_NK100": str(nk100),
            }
            if excg_id_dvsn_cd:
                params["EXCG_ID_DVSN_CD"] = str(excg_id_dvsn_cd)

            headers = self._auth_headers(tr_id=tr_id, tr_cont=tr_cont)
            body, hdrs = self._request_json(
                "GET",
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
                headers=headers,
                params=params,
            )

            rt_cd = str(body.get("rt_cd", ""))
            msg1 = str(body.get("msg1", ""))
            if rt_cd != "0":
                raise KISApiError(f"inquire_daily_ccld rejected rt_cd={rt_cd} msg1={msg1} body={body}")

            out1 = body.get("output1", []) or []
            if isinstance(out1, dict):
                out1 = [out1]
            all_rows.extend(out1)

            fk100 = str(body.get("ctx_area_fk100", "") or "")
            nk100 = str(body.get("ctx_area_nk100", "") or "")
            tr_cont_resp = str(hdrs.get("tr_cont", "") or "").upper()
            if tr_cont_resp not in {"M", "F"}:
                break

            tr_cont = "N"
            if not fk100 and not nk100:
                break

        return {
            "ok": True,
            "tr_id": tr_id,
            "rows": all_rows,
            "pages": pages,
            "start_ymd": str(start_ymd),
            "end_ymd": str(end_ymd),
        }

    @staticmethod
    def _pick_first_int(row: Dict[str, Any], keys: list[str], default: int = 0) -> int:
        for k in keys:
            if k not in row:
                continue
            try:
                return int(float(str(row.get(k, "")).replace(",", "").strip()))
            except Exception:
                continue
        return int(default)

    def inquire_open_orders(
        self,
        *,
        ymd: str,
        sll_buy_dvsn_cd: str = "00",
        pdno: str = "",
        max_pages: int = 30,
    ) -> Dict[str, Any]:
        """Return open(unfilled) orders for a day by filtering inquire_daily_ccld rows."""
        rsp = self.inquire_daily_ccld(
            start_ymd=str(ymd),
            end_ymd=str(ymd),
            sll_buy_dvsn_cd=str(sll_buy_dvsn_cd),
            ccld_dvsn="00",
            inqr_dvsn="00",
            inqr_dvsn_3="00",
            pdno=str(pdno or ""),
            max_pages=int(max_pages),
        )
        rows = rsp.get("rows", []) or []
        out_rows: list[Dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            ord_qty = self._pick_first_int(r, ["ord_qty", "tot_ord_qty", "qty", "ORD_QTY"], default=0)
            ccld_qty = self._pick_first_int(r, ["tot_ccld_qty", "ccld_qty", "exec_qty", "CCLD_QTY"], default=0)
            rem_qty = self._pick_first_int(
                r,
                ["rmn_qty", "ord_psbl_qty", "open_qty", "unccld_qty", "UNCCLD_QTY"],
                default=max(0, int(ord_qty) - int(ccld_qty)),
            )
            if int(rem_qty) <= 0:
                continue
            code = str(r.get("pdno", "") or r.get("code", "")).strip().zfill(6)
            odno = str(r.get("odno", "") or r.get("ord_no", "") or r.get("order_no", "")).strip()
            orgno = str(r.get("ord_gno_brno", "") or r.get("order_branch_no", "") or r.get("orgn_odno", "")).strip()
            if not odno:
                continue
            out_rows.append(
                {
                    "code": code,
                    "odno": odno,
                    "ord_gno_brno": orgno,
                    "ord_qty": int(ord_qty),
                    "ccld_qty": int(ccld_qty),
                    "rmn_qty": int(rem_qty),
                    "ord_tmd": str(r.get("ord_tmd", "") or r.get("order_time", "") or ""),
                    "side_raw": str(r.get("sll_buy_dvsn_cd", "") or r.get("side", "")),
                    "raw": r,
                }
            )

        return {
            "ok": True,
            "tr_id": rsp.get("tr_id", ""),
            "ymd": str(ymd),
            "rows": out_rows,
            "rows_raw": rows,
            "pages": int(rsp.get("pages", 0) or 0),
        }

    def cancel_order(
        self,
        *,
        org_order_no: str,
        org_order_branch_no: str,
        qty: int = 0,
        cancel_all: bool = True,
        order_price: int = 0,
        order_dvsn: str = "00",
        tr_id: str = "",
    ) -> Dict[str, Any]:
        """Cancel a previously submitted cash order."""
        odno = str(org_order_no or "").strip()
        brno = str(org_order_branch_no or "").strip()
        if not odno:
            raise ValueError("org_order_no is required")
        if not brno:
            raise ValueError("org_order_branch_no is required")

        q = int(qty or 0)
        if q < 0:
            raise ValueError("qty must be >= 0")

        tr_id_use = str(tr_id or "").strip()
        if not tr_id_use:
            tr_id_use = "VTTC0803U" if self.cfg.mock else "TTTC0803U"

        payload = {
            "CANO": self.cfg.cano,
            "ACNT_PRDT_CD": self.cfg.acnt_prdt_cd,
            "KRX_FWDG_ORD_ORGNO": brno,
            "ORGN_ODNO": odno,
            "ORD_DVSN": str(order_dvsn or "00"),
            "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": str(q),
            "ORD_UNPR": str(max(0, int(order_price))),
            "QTY_ALL_ORD_YN": "Y" if cancel_all else "N",
        }
        headers = self._auth_headers(tr_id=tr_id_use, payload_for_hash=payload)
        body, _ = self._request_json(
            "POST",
            "/uapi/domestic-stock/v1/trading/order-rvsecncl",
            headers=headers,
            payload=payload,
        )
        rt_cd = str(body.get("rt_cd", ""))
        msg1 = str(body.get("msg1", ""))
        output = body.get("output", {}) or {}
        rvsecncl_no = str(output.get("ODNO", "") or output.get("odno", "")).strip()
        rvsecncl_org_no = str(output.get("KRX_FWDG_ORD_ORGNO", "") or output.get("krx_fwdg_ord_orgno", "")).strip()

        return {
            "ok": rt_cd == "0",
            "rt_cd": rt_cd,
            "msg1": msg1,
            "ord_no": rvsecncl_no,
            "org_no": rvsecncl_org_no,
            "tr_id": tr_id_use,
            "payload": payload,
            "raw": body,
        }

    def inquire_price(
        self,
        *,
        code: str,
        fid_cond_mrkt_div_code: str = "J",
        tr_id: str = "FHKST01010100",
    ) -> Dict[str, Any]:
        """Get current quote snapshot for domestic stock."""
        code_s = str(code).strip().zfill(6)
        if not code_s.isdigit() or len(code_s) != 6:
            raise ValueError(f"Invalid stock code: {code}")

        params = {
            "FID_COND_MRKT_DIV_CODE": str(fid_cond_mrkt_div_code or "J"),
            "FID_INPUT_ISCD": code_s,
        }
        headers = self._auth_headers(tr_id=str(tr_id or "FHKST01010100"))
        body, _ = self._request_json(
            "GET",
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=headers,
            params=params,
        )

        rt_cd = str(body.get("rt_cd", "")).strip()
        msg1 = str(body.get("msg1", "")).strip()
        if rt_cd and rt_cd != "0":
            raise KISApiError(f"inquire_price rejected rt_cd={rt_cd} msg1={msg1} body={body}")

        output = body.get("output", {}) or {}
        return {
            "ok": True,
            "rt_cd": rt_cd,
            "msg1": msg1,
            "tr_id": str(tr_id or "FHKST01010100"),
            "code": code_s,
            "output": output,
            "raw": body,
        }









