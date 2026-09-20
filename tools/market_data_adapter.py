from __future__ import annotations

from typing import Any, Dict, List, Protocol

from kis_order_client import KISOrderClient


class MarketDataAdapter(Protocol):
    def parse_ticker(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def parse_orderbook(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def parse_trades(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        ...


def _to_int(v: Any) -> int:
    try:
        return int(str(v or 0).replace(",", "") or 0)
    except Exception:
        return 0


class KISMarketDataAdapter:
    def fetch_ticker(self, client: KISOrderClient, code: str) -> Dict[str, Any]:
        return client.inquire_price(code=str(code).zfill(6))

    def fetch_orderbook(self, client: KISOrderClient, code: str) -> Dict[str, Any]:
        return client.inquire_hoga(code=str(code).zfill(6))

    def parse_ticker(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        out = payload.get("output", {}) if isinstance(payload, dict) else {}
        if not isinstance(out, dict):
            out = {}
        return {
            "current_price": _to_int(out.get("stck_prpr", 0)),
            "open": _to_int(out.get("stck_oprc", 0)),
            "high": _to_int(out.get("stck_hgpr", 0)),
            "low": _to_int(out.get("stck_lwpr", 0)),
            "volume": _to_int(out.get("acml_vol", 0)),
            "trading_value": _to_int(out.get("acml_tr_pbmn", 0)),
            "ask1": _to_int(out.get("askp1", 0)),
            "bid1": _to_int(out.get("bidp1", 0)),
            "askq1": _to_int(out.get("askp_rsqn1", 0)),
            "bidq1": _to_int(out.get("bidp_rsqn1", 0)),
        }

    def parse_orderbook(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        out = payload.get("output", {}) if isinstance(payload, dict) else {}
        if not isinstance(out, dict):
            out = {}
        return {
            "ask1": _to_int(out.get("askp1", 0)),
            "bid1": _to_int(out.get("bidp1", 0)),
            "askq1": _to_int(out.get("askp_rsqn1", 0)),
            "bidq1": _to_int(out.get("bidp_rsqn1", 0)),
        }

    def parse_trades(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        # KIS 호가/현재가 응답은 체결틱 목록을 항상 포함하지 않아 빈 리스트 반환을 기본으로 둔다.
        out = payload.get("output2", []) if isinstance(payload, dict) else []
        if not isinstance(out, list):
            return []
        rows: List[Dict[str, Any]] = []
        for row in out:
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "price": _to_int(row.get("stck_prpr", 0) or row.get("prpr", 0)),
                    "volume": _to_int(row.get("cntg_vol", 0) or row.get("acml_vol", 0)),
                    "time": str(row.get("stck_cntg_hour", "") or row.get("cntg_hour", "")).strip(),
                }
            )
        return rows
