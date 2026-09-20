# -*- coding: utf-8 -*-
"""연구 라운드가 **실제로 읽는 패널 데이터**의 불변성을 감시한다.

2026-09-07 신규. RD_20260831_flow_h10 의 동결 목록을 실측한 결과:

```
동결됨       tools/load_merged_panel.py        (코드)
             tools/fetch_investor_flow.py      (코드)
             2_Logs/index_daily_history.csv    <- **라운드가 안 읽는다**
동결 안 됨    krx_daily_archive/*_clean.parquet <- load_merged_panel 이 읽는 실데이터
             Raw/krx_daily_20221001_20251224.parquet
```

즉 동결이 안 쓰는 파일을 지키고, 쓰는 파일은 안 지킨다.

**이 도구는 라운드 계약(registration.md / frozen.json)을 건드리지 않는다.**
등록표는 "동결 후 수정하지 않는다" 이므로 기준선 목록에 파일을 더할 수 없다.
대신 별도 무결성 기준선을 만들어 **감시만** 한다.

**계약: 파일은 한 번 쓰이면 안 바뀐다.**
```
새 파일 등장     정상 (전진 수집)
기존 파일 변경   FAIL  (과거 재생성 = 측정 기준선 이동)
기존 파일 소실   FAIL
```
전체 파일 sha256 을 쓴다. 파케이는 행 단위로 자라지 않고 파일 단위로 추가되기 때문이다.
(자라는 CSV 는 성격이 달라 round_preflight 의 프리픽스 해시를 쓴다.)

    python tools/panel_integrity_baseline.py --freeze
    python tools/panel_integrity_baseline.py --check
    python tools/panel_integrity_baseline.py --check --strict   # 변경 있으면 rc=2
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List


def _force_utf8_stdout() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


_force_utf8_stdout()

ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"
BASELINE_DIR = ROOT / "docs" / "research" / "integrity"

# load_merged_panel.py 가 실제로 읽는 것. 그 파일이 바뀌면 여기도 바꿔야 한다.
WATCH_GLOBS = [
    "krx_daily_archive/*_clean.parquet",
    "Raw/krx_daily_*.parquet",
]


def _now() -> dt.datetime:
    return dt.datetime.now()


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def scan() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for g in WATCH_GLOBS:
        for p in sorted(ROOT.glob(g)):
            if not p.is_file():
                continue
            rel = p.relative_to(ROOT).as_posix()
            st = p.stat()
            out[rel] = {"sha256": _sha(p), "size": st.st_size,
                        "mtime": dt.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")}
    return out


def cmd_freeze(name: str, note: str) -> int:
    files = scan()
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    p = BASELINE_DIR / f"{name}.json"
    prior = None
    if p.exists():
        try:
            prior = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            prior = None
    doc = {
        "name": name,
        "frozen_at": _now().strftime("%Y-%m-%d %H:%M:%S"),
        "note": note,
        "contract": "파일은 한 번 쓰이면 안 바뀐다. 새 파일 등장은 정상, 기존 파일 변경·소실은 FAIL",
        "scope": WATCH_GLOBS,
        "not_a_round_baseline": ("라운드 등록표(registration.md)와 frozen.json 은 건드리지 않는다. "
                                 "이것은 별도 감시 장치다."),
        "files": files,
    }
    if prior:
        doc["superseded"] = {"frozen_at": prior.get("frozen_at"), "file_count": len(prior.get("files") or {})}
    p.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[PANEL_FREEZE] {len(files)}개 파일 동결 -> {p}")
    if prior:
        print(f"               이전 기준선 덮어씀 (frozen_at={prior.get('frozen_at')}, "
              f"{len(prior.get('files') or {})}개)")
    return 0


def compare(baseline: Dict[str, Any]) -> Dict[str, Any]:
    old = baseline.get("files") or {}
    new = scan()
    changed: List[Dict[str, Any]] = []
    missing: List[str] = []
    added: List[str] = []
    for rel, meta in old.items():
        if rel not in new:
            missing.append(rel)
        elif new[rel]["sha256"] != meta.get("sha256"):
            changed.append({"file": rel,
                            "old_sha256": str(meta.get("sha256"))[:12],
                            "new_sha256": new[rel]["sha256"][:12],
                            "old_size": meta.get("size"), "new_size": new[rel]["size"],
                            "old_mtime": meta.get("mtime"), "new_mtime": new[rel]["mtime"]})
    for rel in new:
        if rel not in old:
            added.append(rel)
    return {"changed": changed, "missing": missing, "added": sorted(added),
            "baseline_count": len(old), "current_count": len(new)}


def cmd_check(name: str, alert: bool, strict: bool) -> int:
    p = BASELINE_DIR / f"{name}.json"
    if not p.exists():
        print(f"[PANEL_CHECK] 기준선 없음: {p}  -> 먼저 --freeze 할 것")
        return 2 if strict else 0
    baseline = json.loads(p.read_text(encoding="utf-8"))
    r = compare(baseline)
    r.update({"name": name, "checked_at": _now().strftime("%Y-%m-%d %H:%M:%S"),
              "frozen_at": baseline.get("frozen_at")})
    r["status"] = "FAIL" if (r["changed"] or r["missing"]) else "PASS"

    out = BASELINE_DIR / f"{name}_check_latest.json"
    out.write_text(json.dumps(r, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[PANEL_CHECK] {r['status']}  기준선 {r['baseline_count']}개 / 현재 {r['current_count']}개")
    print(f"              변경 {len(r['changed'])}  소실 {len(r['missing'])}  신규 {len(r['added'])} (신규는 정상)")
    for c in r["changed"][:10]:
        print(f"  [변경] {c['file']}")
        print(f"         sha {c['old_sha256']} -> {c['new_sha256']}  "
              f"size {c['old_size']} -> {c['new_size']}  mtime {c['old_mtime']} -> {c['new_mtime']}")
    for m in r["missing"][:10]:
        print(f"  [소실] {m}")

    if r["status"] == "FAIL" and alert:
        try:
            sys.path.insert(0, str(TOOLS_DIR))
            from notify_channels import send_alert  # type: ignore

            lines = [f"[패널 무결성] {r['checked_at']} FAIL (기준선 {r['frozen_at']})",
                     f"변경 {len(r['changed'])} / 소실 {len(r['missing'])} / 신규 {len(r['added'])}"]
            for c in r["changed"][:6]:
                lines.append(f"  변경 {c['file']} {c['old_sha256']}->{c['new_sha256']}")
            for m in r["missing"][:6]:
                lines.append(f"  소실 {m}")
            lines.append("연구 라운드가 읽는 데이터가 바뀌었다. 측정 기준선이 이동했을 수 있다.")
            res = send_alert("\n".join(lines), level="error",
                             extra={"source": "panel_integrity_baseline"}, cooldown_sec=6 * 3600)
            print(f"[PANEL_CHECK] alert ok={res.get('ok')} suppressed={res.get('suppressed')}")
        except Exception as exc:
            print(f"[PANEL_CHECK] alert_failed {type(exc).__name__}: {exc}")

    return 2 if (strict and r["status"] == "FAIL") else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", default="panel_baseline")
    ap.add_argument("--freeze", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--note", default="")
    ap.add_argument("--no-alert", action="store_true")
    ap.add_argument("--strict", action="store_true")
    args = ap.parse_args()

    if args.freeze == args.check:
        print("--freeze 또는 --check 중 하나를 지정할 것")
        return 2
    if args.freeze:
        return cmd_freeze(args.name, args.note)
    return cmd_check(args.name, alert=not args.no_alert, strict=args.strict)


if __name__ == "__main__":
    raise SystemExit(main())
