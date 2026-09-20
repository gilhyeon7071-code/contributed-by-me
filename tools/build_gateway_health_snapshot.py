from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import socket
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "2_Logs"
CFG_PATH = ROOT / "config" / "resilience_check.json"
KST = dt.timezone(dt.timedelta(hours=9))


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).astimezone(KST)


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            obj = json.loads(path.read_text(encoding=enc))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            continue
    return {}


def _cfg() -> Dict[str, Any]:
    return _read_json(CFG_PATH)


def _parse_host_port(raw: str) -> Dict[str, Any]:
    s = str(raw or "").strip()
    if not s:
        return {"host": "", "port": 443}
    if ":" in s:
        host, _, p = s.rpartition(":")
        try:
            return {"host": host.strip(), "port": int(p)}
        except Exception:
            return {"host": s, "port": 443}
    return {"host": s, "port": 443}


def _tcp_latency_ms(host: str, port: int, timeout_ms: int = 1500) -> Dict[str, Any]:
    if not host:
        return {"ok": False, "latency_ms": None, "error": "empty_host"}
    t0 = time.perf_counter()
    try:
        with socket.create_connection((host, int(port)), timeout=max(0.2, timeout_ms / 1000.0)):
            dt_ms = int((time.perf_counter() - t0) * 1000.0)
            return {"ok": True, "latency_ms": dt_ms, "method": "tcp"}
    except Exception as e:
        return {"ok": False, "latency_ms": None, "error": str(e), "method": "tcp"}


def _ping_latency_ms(host: str, timeout_ms: int = 1500) -> Dict[str, Any]:
    cmd = ["ping", "-n", "2", "-w", str(max(100, timeout_ms)), host]
    p = subprocess.run(cmd, capture_output=True, text=True)
    text = (p.stdout or "") + "\n" + (p.stderr or "")
    m = re.search(r"Average = (\d+)ms", text, flags=re.IGNORECASE)
    lat = int(m.group(1)) if m else None
    return {"ok": p.returncode == 0 and lat is not None, "latency_ms": lat, "returncode": p.returncode, "raw_tail": text[-400:], "method": "icmp"}


def _infer_health_from_logs() -> Dict[str, Any]:
    e2e = _read_json(LOG_DIR / "kis_intraday_e2e_latest.json")
    rt = _read_json(LOG_DIR / "kis_realtime_status_latest.json")
    hc = sorted(LOG_DIR.glob("kis_healthcheck_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    hc_latest = _read_json(hc[0]) if hc else {}
    return {
        "e2e_ok": bool(e2e.get("ok")),
        "rt_ok": bool((rt.get("health") or {}).get("ok")) if isinstance(rt.get("health"), dict) else bool(rt.get("ok")),
        "hc_ok": bool(hc_latest.get("ok")),
    }


def build() -> List[Dict[str, Any]]:
    cfg = _cfg()
    sec = cfg.get("gateway_probe", {}) if isinstance(cfg, dict) else {}
    timeout_ms = int(sec.get("timeout_ms", 1500))
    defaults = [
        {"name": "primary", "host": "openapivts.koreainvestment.com"},
        {"name": "secondary", "host": "openapi.koreainvestment.com"},
        {"name": "tertiary", "host": "8.8.8.8"},
    ]
    gateways = sec.get("gateways", defaults) if isinstance(sec, dict) else defaults
    signals = _infer_health_from_logs()
    rows: List[Dict[str, Any]] = []
    for g in gateways:
        if not isinstance(g, dict):
            continue
        name = str(g.get("name", "")).strip() or "unknown"
        host = str(g.get("host", "")).strip()
        if not host:
            continue
        hp = _parse_host_port(host)
        tcp = _tcp_latency_ms(hp["host"], hp["port"], timeout_ms=timeout_ms)
        probe = tcp if bool(tcp.get("ok")) else _ping_latency_ms(hp["host"], timeout_ms=timeout_ms)
        if (not bool(probe.get("ok"))) and "koreainvestment.com" in hp["host"].lower() and bool(signals["hc_ok"]):
            probe = {
                "ok": True,
                "latency_ms": None,
                "method": "kis_healthcheck",
                "fallback_from": probe,
            }
        ok = bool(probe.get("ok")) and (signals["e2e_ok"] or signals["rt_ok"] or signals["hc_ok"])
        rec = {
            "generated_at": _now().isoformat(),
            "name": name,
            "host": hp["host"],
            "port": hp["port"],
            "ok": ok,
            "latency_ms": probe.get("latency_ms"),
            "probe_method": probe.get("method"),
            "signals": signals,
            "probe_detail": probe,
        }
        ts = _now().strftime("%Y%m%d_%H%M%S")
        out_ts = LOG_DIR / f"gateway_health_{name}_{ts}.json"
        out_latest = LOG_DIR / f"gateway_health_{name}_latest.json"
        out_ts.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        out_latest.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        rows.append(rec)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Build gateway health snapshots")
    _ = ap.parse_args()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    rows = build()
    ok_n = sum(1 for r in rows if bool(r.get("ok")))
    print(f"[GW] total={len(rows)} ok={ok_n}")
    return 0 if rows else 2


if __name__ == "__main__":
    raise SystemExit(main())
