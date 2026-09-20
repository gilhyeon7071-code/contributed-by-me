from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "2_Logs"
DEFAULT_SOURCE = LOGS / "kis_ws_market_event_latest.json"
DEFAULT_OUTPUT_DIR = LOGS


def _norm_ymd(value: Any) -> str:
    s = re.sub(r"[^0-9]", "", str(value or ""))
    return s[:8] if len(s) >= 8 else ""


def _today_ymd() -> str:
    return datetime.now().strftime("%Y%m%d")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _walk_values(obj: Any) -> Iterable[str]:
    if isinstance(obj, dict):
        for value in obj.values():
            yield from _walk_values(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk_values(value)
    else:
        text = str(obj or "").strip()
        if text:
            yield text


def _detect_flags(doc: Dict[str, Any]) -> Dict[str, Any]:
    texts: List[str] = [x.upper() for x in _walk_values(doc)]
    joined = " ".join(texts)
    sidecar_terms = ("SIDECAR", "SIDE CAR", "사이드카")
    circuit_terms = ("CIRCUIT_BREAKER", "CIRCUIT BREAKER", "CIRCUIT-BREAKER", "서킷브레이커", "서킷 브레이커")
    return {
        "sidecar": any(term in joined for term in sidecar_terms),
        "circuit_breaker": any(term in joined for term in circuit_terms),
        "matched_terms": {
            "sidecar": [term for term in sidecar_terms if term in joined],
            "circuit_breaker": [term for term in circuit_terms if term in joined],
        },
    }


def build_market_event_log(*, d: str, source: Path, output_dir: Path) -> Dict[str, Any]:
    ymd = _norm_ymd(d) or _today_ymd()
    source_doc = _read_json(source)
    flags = _detect_flags(source_doc) if source_doc else {
        "sidecar": False,
        "circuit_breaker": False,
        "matched_terms": {"sidecar": [], "circuit_breaker": []},
    }
    payload: Dict[str, Any] = {
        "D": ymd,
        "sidecar": bool(flags.get("sidecar", False)),
        "circuit_breaker": bool(flags.get("circuit_breaker", False)),
        "source": str(source),
        "source_exists": bool(source.exists()),
        "source_ts": str(source_doc.get("ts") or source_doc.get("generated_at") or ""),
        "source_event_type": str(source_doc.get("event_type") or ""),
        "matched_terms": flags.get("matched_terms") or {"sidecar": [], "circuit_breaker": []},
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "policy": {
            "diagnostic_only": True,
            "used_for_trading": False,
            "orders_modified": False,
            "fills_modified": False,
            "ledger_modified": False,
            "stats_modified": False,
            "gate_modified": False,
            "risk_lock_modified": False,
        },
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"market_event_log_{ymd}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    latest_path = output_dir / "market_event_log_latest.json"
    latest_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    payload["outputs"] = {"json": str(out_path), "latest_json": str(latest_path)}
    out_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build daily sidecar/circuit-breaker market event log from KIS WS latest event.")
    ap.add_argument("--D", "--date", dest="d", default=_today_ymd())
    ap.add_argument("--source", default=str(DEFAULT_SOURCE))
    ap.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = ap.parse_args()
    payload = build_market_event_log(d=args.d, source=Path(args.source), output_dir=Path(args.output_dir))
    print(
        f"[MARKET_EVENT_LOG] D={payload.get('D')} sidecar={payload.get('sidecar')} "
        f"circuit_breaker={payload.get('circuit_breaker')} output={payload.get('outputs', {}).get('json')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
